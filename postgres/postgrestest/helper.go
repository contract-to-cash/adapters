// Package postgrestest provides test helpers for PostgreSQL integration tests.
package postgrestest

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/contract-to-cash/adapters/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultDSN = "postgres://adapters_test:adapters_test@localhost:5432/adapters_test?sslmode=disable"

// NewPool creates a pgxpool.Pool for testing. It applies all migrations after
// truncating every application table, so each test starts with a clean schema.
// The pool is closed automatically when the test finishes.
//
// Database selection: ADAPTERS_TEST_DSN, falling back to defaultDSN. When no
// database is reachable, the test is SKIPPED if the DSN was implicit (local
// development without postgres must not go red) but FAILED if
// ADAPTERS_TEST_DSN was set explicitly (CI must not silently skip).
func NewPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("ADAPTERS_TEST_DSN")
	explicitDSN := dsn != ""
	if dsn == "" {
		dsn = defaultDSN
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		if explicitDSN {
			t.Fatalf("create pool: %v", err)
		}
		t.Skipf("skipping postgres integration test: invalid default DSN (%v); set ADAPTERS_TEST_DSN to run", err)
	}
	t.Cleanup(pool.Close)

	if err := pool.Ping(ctx); err != nil {
		if explicitDSN {
			t.Fatalf("ping %s: %v", dsn, err)
		}
		t.Skipf("skipping postgres integration test: no database reachable at default DSN (%v); set ADAPTERS_TEST_DSN to run", err)
	}

	// Apply migrations through the production runner, which tracks applied
	// files in schema_migrations and therefore no longer needs to swallow
	// "already exists" errors on a persistent database: a re-run simply skips
	// files it has already recorded.
	if err := postgres.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	truncateAll(t, pool)

	return pool
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
		// Best-effort: a table may not exist yet on the very first run.
		_, _ = pool.Exec(ctx, fmt.Sprintf("TRUNCATE %s CASCADE", table))
	}

	// Reset the global_position sequence so tests see predictable values.
	// Best-effort: ignore if the sequence does not exist.
	_, _ = pool.Exec(ctx, "ALTER SEQUENCE IF EXISTS events_global_position_seq RESTART WITH 1")
}
