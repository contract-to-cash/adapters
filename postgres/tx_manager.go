package postgres

import (
	"context"
	"fmt"

	"github.com/contract-to-cash/core/application/tx"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ReposFactory builds a tx.Repos bound to the given Querier.
//
// The factory is injected rather than hard-coded so that callers can choose
// which concrete repository types compose their tx.Repos — for example, a
// test may want to swap one of the repositories for a fake.
//
// When RunInTx is called, the manager invokes the factory with a
// transaction-scoped Querier (the pgx.Tx). The returned repositories must use
// that Querier for their writes. The standard pattern is to capture the
// QuerierFromContext(ctx, pool) result inside each repository method, so the
// factory can simply construct a thin wrapper that remembers the pool and the
// pgx.Tx-bearing context.
type ReposFactory func(q Querier) tx.Repos

// PostgresTxManager implements tx.TxManager on top of a pgxpool.Pool.
//
// It supports nested RunInTx calls: if the incoming context already carries a
// pgx.Tx, the outer transaction is reused rather than opened again. This is a
// hard requirement from core.CreditNoteService.ReissueInvoice, which calls
// BillingService.GenerateInvoice inside its own RunInTx.
type PostgresTxManager struct {
	pool    *pgxpool.Pool
	factory ReposFactory
}

var _ tx.TxManager = (*PostgresTxManager)(nil)

// NewTxManager constructs a PostgresTxManager.
func NewTxManager(pool *pgxpool.Pool, factory ReposFactory) *PostgresTxManager {
	return &PostgresTxManager{pool: pool, factory: factory}
}

// RunInTx runs fn inside a database transaction.
//
// If ctx already carries a pgx.Tx (i.e. we're already inside an outer
// RunInTx), fn is invoked with the existing transaction instead of opening a
// new one. A commit/rollback is performed only by the outermost call, so
// nested semantics are "join the outer tx" rather than "open a savepoint".
//
// On fn error, the outermost call issues Rollback and returns the original
// error. A rollback failure is wrapped so the original error remains visible.
func (m *PostgresTxManager) RunInTx(ctx context.Context, fn func(ctx context.Context, repos tx.Repos) error) error {
	if existing, ok := TxFromContext(ctx); ok {
		// Nested call: reuse the outer pgx.Tx. We do NOT commit here; the
		// outermost call owns the lifetime.
		return fn(ctx, m.factory(existing))
	}

	pgxTx, err := m.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	txCtx := ContextWithTx(ctx, pgxTx)
	if err := fn(txCtx, m.factory(pgxTx)); err != nil {
		if rbErr := pgxTx.Rollback(ctx); rbErr != nil {
			return fmt.Errorf("rollback after error: %v (original: %w)", rbErr, err)
		}
		return err
	}

	if err := pgxTx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
