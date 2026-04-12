// Package postgrestest provides test helpers for PostgreSQL integration tests.
package postgrestest

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/contract-to-cash/adapters/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultDSN = "postgres://adapters_test:adapters_test@localhost:5432/adapters_test?sslmode=disable"

// NewPool creates a pgxpool.Pool for testing. It applies all migrations after
// truncating every application table, so each test starts with a clean schema.
// The pool is closed automatically when the test finishes.
func NewPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("ADAPTERS_TEST_DSN")
	if dsn == "" {
		dsn = defaultDSN
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	applyMigrations(t, pool)
	truncateAll(t, pool)

	return pool
}

func applyMigrations(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	entries, err := postgres.Migrations.ReadDir("migrations")
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	ctx := context.Background()
	for _, entry := range entries {
		data, err := postgres.Migrations.ReadFile("migrations/" + entry.Name())
		if err != nil {
			t.Fatalf("read migration %s: %v", entry.Name(), err)
		}
		if _, err := pool.Exec(ctx, string(data)); err != nil {
			// Ignore "already exists" errors so tests can be re-run.
		}
	}
}

// truncateAll removes all data from application tables in dependency order.
func truncateAll(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	ctx := context.Background()
	tables := []string{
		"balance_refunds",
		"balance_applications",
		"balance_entries",
		"credit_notes",
		"payments",
		"invoice_history",
		"invoices",
		"usage_records",
		"prices",
		"products",
		"invoice_read_models",
		"contract_read_models",
		"projection_checkpoints",
		"snapshots",
		"events",
	}

	for _, table := range tables {
		if _, err := pool.Exec(ctx, fmt.Sprintf("TRUNCATE %s CASCADE", table)); err != nil {
			// Table may not exist yet on first run; ignore.
		}
	}

	// Reset the global_position sequence so tests see predictable values.
	if _, err := pool.Exec(ctx, "ALTER SEQUENCE IF EXISTS events_global_position_seq RESTART WITH 1"); err != nil {
		// Ignore if sequence doesn't exist.
	}
}
