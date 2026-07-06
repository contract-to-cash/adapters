package postgres_test

import (
	"context"
	"strings"
	"testing"

	"github.com/contract-to-cash/adapters/postgres"
	"github.com/contract-to-cash/adapters/postgres/postgrestest"
)

// TestMigration009_DropsDeadBalanceIndexIdempotently checks the embedded 009
// migration (issue #39) drops the dead partial index and does so with an
// IF EXISTS guard, so a re-run against an already-migrated database is a no-op
// rather than an error. This is a content assertion and needs no database.
func TestMigration009_DropsDeadBalanceIndexIdempotently(t *testing.T) {
	data, err := postgres.Migrations.ReadFile("migrations/009_drop_dead_balance_index.up.sql")
	if err != nil {
		t.Fatalf("read migration 009: %v", err)
	}
	sql := string(data)

	// Ignore comment lines so a "DROP INDEX ..." mentioned in prose does not
	// satisfy the assertion; only executable statements count.
	var exec strings.Builder
	for _, line := range strings.Split(sql, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		exec.WriteString(line)
		exec.WriteString("\n")
	}
	stmt := strings.ToUpper(exec.String())

	if !strings.Contains(stmt, "DROP INDEX IF EXISTS") {
		t.Errorf("009 must DROP INDEX IF EXISTS for idempotency, got:\n%s", sql)
	}
	if !strings.Contains(stmt, "IDX_BALANCE_ENTRIES_AVAILABLE") {
		t.Errorf("009 must drop idx_balance_entries_available, got:\n%s", sql)
	}
}

// TestMigrate_Idempotent verifies the runner can be invoked repeatedly against
// an already-migrated database: every file is recorded in schema_migrations and
// a second run is a no-op (no "already exists" error, nothing re-applied).
func TestMigrate_Idempotent(t *testing.T) {
	pool := postgrestest.NewPool(t) // already runs Migrate once
	ctx := context.Background()

	// schema_migrations must list exactly the embedded files.
	entries, err := postgres.Migrations.ReadDir("migrations")
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}
	wantFiles := len(entries)

	var got int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM schema_migrations WHERE status = 'applied'`).Scan(&got); err != nil {
		t.Fatalf("count schema_migrations: %v", err)
	}
	if got != wantFiles {
		t.Fatalf("schema_migrations has %d applied rows, want %d", got, wantFiles)
	}

	// A second Migrate must succeed without touching anything.
	if err := postgres.Migrate(ctx, pool); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}

	var after int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM schema_migrations WHERE status = 'applied'`).Scan(&after); err != nil {
		t.Fatalf("re-count schema_migrations: %v", err)
	}
	if after != wantFiles {
		t.Fatalf("schema_migrations changed after re-run: %d != %d", after, wantFiles)
	}
}

// TestMigrate_DropsProjectionFKs verifies migration 008 removed the write-side
// foreign keys onto the contract_read_models projection table, so an invoice
// can be written for a contract whose read model has not been projected yet.
func TestMigrate_DropsProjectionFKs(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()

	for _, fk := range []string{"fk_invoices_contract", "fk_credit_notes_contract", "fk_usage_records_contract"} {
		var exists bool
		if err := pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM information_schema.table_constraints
			 WHERE constraint_name = $1 AND constraint_type = 'FOREIGN KEY')`, fk).Scan(&exists); err != nil {
			t.Fatalf("check constraint %s: %v", fk, err)
		}
		if exists {
			t.Errorf("foreign key %s still present; migration 008 should have dropped it", fk)
		}
	}
}
