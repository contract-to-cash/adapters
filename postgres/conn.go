// Package postgres provides a PostgreSQL reference implementation of the
// contract-to-cash/core repository and event store interfaces.
//
// The package is organized around the following pieces:
//
//   - conn.go        — connection pool, Querier abstraction, context-embedded Tx helpers
//   - tx_manager.go  — PostgresTxManager implementing tx.TxManager with nested RunInTx
//   - eventstore.go  — PostgresEventStore implementing eventstore.Store
//   - contract_repo.go, invoice_repo.go, ... — repository implementations
//   - contract_projector.go, invoice_projector.go — projection.Projector implementations
//
// All state-stored entities are persisted via the ToSnapshot / FromSnapshot
// API introduced in contract-to-cash/core#95.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Querier is the common interface satisfied by both *pgxpool.Pool and pgx.Tx.
// Repositories take a Querier rather than a specific type so that the same
// code path serves pool-backed reads and transaction-scoped writes.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

var (
	_ Querier = (*pgxpool.Pool)(nil)
	_ Querier = (pgx.Tx)(nil)
)

// txKey is the unexported context key used to smuggle a pgx.Tx through the
// context chain. Nested RunInTx calls inspect the context and reuse the outer
// transaction, which is how we implement "nested transaction" semantics
// required by services like CreditNoteService.ReissueInvoice.
type txKey struct{}

// TxFromContext returns the pgx.Tx embedded in ctx, if any.
func TxFromContext(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := ctx.Value(txKey{}).(pgx.Tx)
	return tx, ok
}

// ContextWithTx stores tx in the returned context so that downstream calls
// (including nested RunInTx) can discover and reuse it.
func ContextWithTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

// QuerierFromContext returns the transaction-scoped Querier if a pgx.Tx is
// present on the context, otherwise it falls back to the pool. This lets a
// repository method participate in an ambient transaction transparently.
func QuerierFromContext(ctx context.Context, pool *pgxpool.Pool) Querier {
	if tx, ok := TxFromContext(ctx); ok {
		return tx
	}
	return pool
}

// Config holds the minimal knobs needed to open a pgx connection pool.
// Advanced users can ignore this and call pgxpool.NewWithConfig directly.
type Config struct {
	Host     string
	Port     uint16
	Database string
	User     string
	Password string
	SSLMode  string
	MaxConns int32
	MinConns int32
}

// ConnString renders the PostgreSQL connection URI.
func (c Config) ConnString() string {
	sslMode := c.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.Database, sslMode,
	)
}

// NewPool opens a pgxpool.Pool configured from cfg and verifies the connection
// with a Ping. The caller owns the pool and must Close it.
func NewPool(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.ConnString())
	if err != nil {
		return nil, fmt.Errorf("parse pool config: %w", err)
	}
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		poolCfg.MinConns = cfg.MinConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return pool, nil
}
