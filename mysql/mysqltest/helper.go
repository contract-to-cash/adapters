// Package mysqltest provides test helpers for MySQL integration tests.
package mysqltest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/contract-to-cash/adapters/mysql"
	driver "github.com/go-sql-driver/mysql"
)

// defaultDSN targets the mysql:8 service container used in CI. multiStatements
// is enabled so a migration file (multiple DDL statements) can be applied in a
// single Exec; parseTime + loc=UTC keep DATETIME(6) round-trips in UTC.
const defaultDSN = "adapters_test:adapters_test@tcp(localhost:3306)/adapters_test?parseTime=true&loc=UTC&multiStatements=true"

// NewDB opens a *sql.DB for testing. It applies all migrations (ignoring
// already-exists errors so a persistent CI database can be reused) and then
// truncates every application table, so each test starts from a clean schema.
// The DB is closed automatically when the test finishes.
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
		dsn = ensureMultiStatements(dsn)
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

	applyMigrations(t, db)
	truncateAll(t, db)

	return db
}

// ensureMultiStatements appends multiStatements=true to a user-supplied DSN if
// absent; applying a multi-statement migration file otherwise fails with error
// 1064 under the MySQL driver's default (single-statement) mode.
func ensureMultiStatements(dsn string) string {
	if cfg, err := driver.ParseDSN(dsn); err == nil {
		cfg.MultiStatements = true
		if cfg.Params == nil {
			cfg.Params = map[string]string{}
		}
		// parseTime + UTC keep timestamp round-trips deterministic.
		cfg.ParseTime = true
		cfg.Loc = time.UTC
		return cfg.FormatDSN()
	}
	return dsn
}

func applyMigrations(t *testing.T, db *sql.DB) {
	t.Helper()

	entries, err := mysql.Migrations.ReadDir("migrations")
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	ctx := context.Background()
	for _, entry := range entries {
		data, err := mysql.Migrations.ReadFile("migrations/" + entry.Name())
		if err != nil {
			t.Fatalf("read migration %s: %v", entry.Name(), err)
		}
		if _, err := db.ExecContext(ctx, string(data)); err != nil {
			// Ignore "already exists" errors so tests can re-run against a
			// persistent database:
			//   1050 = table already exists
			//   1061 = duplicate key/index name
			//   1826 = duplicate foreign key constraint name
			// Any other error is a real failure.
			var me *driver.MySQLError
			if errors.As(err, &me) && (me.Number == 1050 || me.Number == 1061 || me.Number == 1826) {
				continue
			}
			t.Fatalf("apply migration %s: %v", entry.Name(), err)
		}
	}
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
