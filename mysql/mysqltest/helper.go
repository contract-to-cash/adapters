// Package mysqltest provides test helpers for MySQL integration tests.
package mysqltest

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/contract-to-cash/adapters/mysql"
	driver "github.com/go-sql-driver/mysql"
)

// defaultDSN targets the mysql:8 service container used in CI. parseTime +
// loc=UTC keep DATETIME(6) round-trips in UTC. Migrations are applied through
// mysql.Migrate, which splits each file into single statements, so
// multiStatements is not required.
const defaultDSN = "adapters_test:adapters_test@tcp(localhost:3306)/adapters_test?parseTime=true&loc=UTC"

// NewDB opens a *sql.DB for testing. It applies all migrations through the
// production mysql.Migrate runner and then truncates every application table, so
// each test starts from a clean schema. The DB is closed automatically when the
// test finishes.
//
// Database selection: ADAPTERS_TEST_MYSQL_DSN, falling back to defaultDSN. When
// no database is reachable, the test is SKIPPED if the DSN was implicit (local
// development without MySQL must not go red) but FAILED if
// ADAPTERS_TEST_MYSQL_DSN was set explicitly (CI must not silently skip).
func NewDB(t *testing.T) *sql.DB {
	t.Helper()

	dsn := os.Getenv("ADAPTERS_TEST_MYSQL_DSN")
	explicitDSN := dsn != ""
	if dsn == "" {
		dsn = defaultDSN
	} else {
		dsn = normalizeDSN(dsn)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		if explicitDSN {
			t.Fatalf("open mysql: %v", err)
		}
		t.Skipf("skipping mysql integration test: invalid default DSN (%v); set ADAPTERS_TEST_MYSQL_DSN to run", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		if explicitDSN {
			t.Fatalf("ping %s: %v", dsn, err)
		}
		t.Skipf("skipping mysql integration test: no database reachable at default DSN (%v); set ADAPTERS_TEST_MYSQL_DSN to run", err)
	}

	// Apply migrations through the production runner, which tracks applied
	// files in schema_migrations and therefore no longer needs to swallow
	// "already exists" errors on a persistent database: a re-run simply skips
	// files it has already recorded.
	if err := mysql.Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	truncateAll(t, db)

	return db
}

// normalizeDSN forces parseTime + loc=UTC on a user-supplied DSN so DATETIME(6)
// round-trips stay deterministic in UTC, regardless of what the caller passed.
func normalizeDSN(dsn string) string {
	if cfg, err := driver.ParseDSN(dsn); err == nil {
		cfg.ParseTime = true
		cfg.Loc = time.UTC
		return cfg.FormatDSN()
	}
	return dsn
}

// tables lists application tables in child-to-parent (FK) order. Truncation
// runs with foreign_key_checks disabled on a single dedicated connection, so
// order is not strictly required, but keeping it makes intent clear.
var tables = []string{
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

// truncateAll empties every application table. foreign_key_checks is toggled on
// a single *sql.Conn so the SESSION setting applies to all the TRUNCATEs (a
// pooled *sql.DB could otherwise spread them across connections), and the
// connection is discarded afterwards so it never returns to the pool with
// checks left disabled.
func truncateAll(t *testing.T, db *sql.DB) {
	t.Helper()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire truncate conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, "SET SESSION foreign_key_checks = 0"); err != nil {
		t.Fatalf("disable fk checks: %v", err)
	}
	for _, table := range tables {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", table)); err != nil {
			// Table may not exist on first run; ignore and continue.
			continue
		}
	}
	// Restore before the connection returns to the pool. Asserted, so a clean
	// connection is guaranteed for reuse.
	if _, err := conn.ExecContext(ctx, "SET SESSION foreign_key_checks = 1"); err != nil {
		t.Fatalf("restore fk checks: %v", err)
	}
}
