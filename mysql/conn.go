package mysql

import (
	"context"
	"database/sql"
)

// Querier is the common interface satisfied by both *sql.DB and *sql.Tx.
// It lets repositories and the event store run against either the pooled
// connection or an ambient transaction transparently.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

var (
	_ Querier = (*sql.DB)(nil)
	_ Querier = (*sql.Tx)(nil)
)

type txKey struct{}

// TxFromContext returns the *sql.Tx embedded in ctx, if any.
func TxFromContext(ctx context.Context) (*sql.Tx, bool) {
	tx, ok := ctx.Value(txKey{}).(*sql.Tx)
	return tx, ok
}

// ContextWithTx stores tx in the returned context so downstream repository
// calls run inside the same transaction.
func ContextWithTx(ctx context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

// querierFromContext returns the ambient *sql.Tx if present, otherwise db.
func querierFromContext(ctx context.Context, db *sql.DB) Querier {
	if tx, ok := TxFromContext(ctx); ok {
		return tx
	}
	return db
}
