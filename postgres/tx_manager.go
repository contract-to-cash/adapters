package postgres

import (
	"context"
	"fmt"

	"github.com/contract-to-cash/core/application/tx"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ReposFactory builds a tx.Repos bound to the given Querier.
type ReposFactory func(q Querier) tx.Repos

// PostgresTxManager implements tx.TxManager on top of a pgxpool.Pool.
type PostgresTxManager struct {
	pool    *pgxpool.Pool
	factory ReposFactory
}

var _ tx.TxManager = (*PostgresTxManager)(nil)

// NewTxManager constructs a PostgresTxManager.
func NewTxManager(pool *pgxpool.Pool, factory ReposFactory) *PostgresTxManager {
	return &PostgresTxManager{pool: pool, factory: factory}
}

// RunInTx runs fn inside a database transaction. Nested calls reuse the
// outer transaction via context propagation.
func (m *PostgresTxManager) RunInTx(ctx context.Context, fn func(ctx context.Context, repos tx.Repos) error) error {
	if existing, ok := TxFromContext(ctx); ok {
		return fn(ctx, m.factory(existing))
	}

	pgxTx, err := m.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	txCtx := ContextWithTx(ctx, pgxTx)
	if err := fn(txCtx, m.factory(pgxTx)); err != nil {
		// Use a detached context for rollback so it succeeds even if
		// the original context has been cancelled or timed out.
		if rbErr := pgxTx.Rollback(context.WithoutCancel(ctx)); rbErr != nil {
			return fmt.Errorf("rollback after error: %v (original: %w)", rbErr, err)
		}
		return err
	}

	if err := pgxTx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
