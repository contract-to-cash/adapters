package postgres

import (
	"context"
	"fmt"

	"github.com/contract-to-cash/core/application/tx"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresTxManager implements tx.TxManager with support for nested transactions.
// When RunInTx detects an existing Tx in the context, it reuses the outer transaction
// instead of starting a new one — satisfying core's requirement for nested RunInTx calls
// (e.g. CreditNoteService.ReissueInvoice).
type PostgresTxManager struct {
	pool       *pgxpool.Pool
	eventStore *PostgresEventStore
	repoFactory func(q Querier, es *PostgresEventStore) tx.Repos
}

// NewTxManager creates a new PostgresTxManager.
// repoFactory is called to construct Repos bound to the transaction's Querier.
func NewTxManager(
	pool *pgxpool.Pool,
	eventStore *PostgresEventStore,
	repoFactory func(q Querier, es *PostgresEventStore) tx.Repos,
) *PostgresTxManager {
	return &PostgresTxManager{
		pool:        pool,
		eventStore:  eventStore,
		repoFactory: repoFactory,
	}
}

var _ tx.TxManager = (*PostgresTxManager)(nil)

// RunInTx executes fn within a database transaction.
// If the context already contains a Tx (from an outer RunInTx call), the existing
// transaction is reused. Otherwise, a new transaction is started.
func (m *PostgresTxManager) RunInTx(ctx context.Context, fn func(ctx context.Context, repos tx.Repos) error) error {
	// Nested transaction: reuse the existing Tx from context
	if existingTx, ok := TxFromContext(ctx); ok {
		txES := m.eventStore.WithTx(existingTx)
		repos := m.repoFactory(existingTx, txES)
		return fn(ctx, repos)
	}

	// New transaction
	pgxTx, err := m.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	txCtx := ContextWithTx(ctx, pgxTx)
	txES := m.eventStore.WithTx(pgxTx)
	repos := m.repoFactory(pgxTx, txES)

	if err := fn(txCtx, repos); err != nil {
		if rbErr := pgxTx.Rollback(ctx); rbErr != nil {
			return fmt.Errorf("rollback failed: %v (original error: %w)", rbErr, err)
		}
		return err
	}

	if err := pgxTx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}
