package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/contract-to-cash/core/application/tx"
)

// ReposFactory builds a tx.Repos bound to the given Querier.
type ReposFactory func(q Querier) tx.Repos

// MySQLTxManager implements tx.TxManager on top of a *sql.DB.
type MySQLTxManager struct {
	db      *sql.DB
	factory ReposFactory
}

var _ tx.TxManager = (*MySQLTxManager)(nil)

// NewTxManager constructs a MySQLTxManager.
func NewTxManager(db *sql.DB, factory ReposFactory) *MySQLTxManager {
	return &MySQLTxManager{db: db, factory: factory}
}

// RunInTx runs fn inside a database transaction. Nested calls reuse the
// outer transaction via context propagation.
func (m *MySQLTxManager) RunInTx(ctx context.Context, fn func(ctx context.Context, repos tx.Repos) error) error {
	if existing, ok := TxFromContext(ctx); ok {
		return fn(ctx, m.factory(existing))
	}

	sqlTx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	txCtx := ContextWithTx(ctx, sqlTx)
	if err := fn(txCtx, m.factory(sqlTx)); err != nil {
		if rbErr := sqlTx.Rollback(); rbErr != nil {
			return fmt.Errorf("rollback after error: %v (original: %w)", rbErr, err)
		}
		return err
	}

	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
