package mysql

import (
	"context"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/DATA-DOG/go-sqlmock"
)

func fakeMigrations() fstest.MapFS {
	return fstest.MapFS{
		"m/001_a.up.sql": {Data: []byte("CREATE TABLE a (id INT);")},
		"m/002_b.up.sql": {Data: []byte("CREATE TABLE b (id INT);\nCREATE INDEX ix ON b (id);")},
	}
}

func TestMigrateOn_AppliesPendingFilesInOrder(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS schema_migrations").
		WillReturnResult(sqlmock.NewResult(0, 0))
	// No rows applied yet.
	mock.ExpectQuery("SELECT filename, status FROM schema_migrations").
		WillReturnRows(sqlmock.NewRows([]string{"filename", "status"}))

	// 001: pending marker, one statement, applied marker.
	mock.ExpectExec("INSERT INTO schema_migrations").WithArgs("001_a.up.sql", migrationStatusPending).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("CREATE TABLE a").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("UPDATE schema_migrations SET status").WithArgs(migrationStatusApplied, "001_a.up.sql").
		WillReturnResult(sqlmock.NewResult(0, 1))

	// 002: pending marker, two statements, applied marker.
	mock.ExpectExec("INSERT INTO schema_migrations").WithArgs("002_b.up.sql", migrationStatusPending).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("CREATE TABLE b").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE INDEX ix ON b").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("UPDATE schema_migrations SET status").WithArgs(migrationStatusApplied, "002_b.up.sql").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := migrateOn(context.Background(), db, fakeMigrations(), "m"); err != nil {
		t.Fatalf("migrateOn: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestMigrateOn_SkipsAlreadyApplied(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS schema_migrations").
		WillReturnResult(sqlmock.NewResult(0, 0))
	// 001 already applied; only 002 should run.
	mock.ExpectQuery("SELECT filename, status FROM schema_migrations").
		WillReturnRows(sqlmock.NewRows([]string{"filename", "status"}).
			AddRow("001_a.up.sql", migrationStatusApplied))

	mock.ExpectExec("INSERT INTO schema_migrations").WithArgs("002_b.up.sql", migrationStatusPending).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("CREATE TABLE b").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE INDEX ix ON b").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("UPDATE schema_migrations SET status").WithArgs(migrationStatusApplied, "002_b.up.sql").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := migrateOn(context.Background(), db, fakeMigrations(), "m"); err != nil {
		t.Fatalf("migrateOn: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestMigrateOn_DetectsPartialApplication(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectExec("CREATE TABLE IF NOT EXISTS schema_migrations").
		WillReturnResult(sqlmock.NewResult(0, 0))
	// 001 left in 'pending' — a prior run failed midway.
	mock.ExpectQuery("SELECT filename, status FROM schema_migrations").
		WillReturnRows(sqlmock.NewRows([]string{"filename", "status"}).
			AddRow("001_a.up.sql", migrationStatusPending))

	err = migrateOn(context.Background(), db, fakeMigrations(), "m")
	if err == nil {
		t.Fatal("expected error for partially applied migration, got nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestSplitStatements(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{
			name: "simple two statements",
			in:   "CREATE TABLE a (id INT);\nCREATE INDEX ix ON a (id);",
			want: []string{"CREATE TABLE a (id INT)", "CREATE INDEX ix ON a (id)"},
		},
		{
			name: "line comment stripped",
			in:   "-- a comment\nCREATE TABLE a (id INT);",
			want: []string{"CREATE TABLE a (id INT)"},
		},
		{
			name: "semicolon inside string literal is not a delimiter",
			in:   "INSERT INTO t VALUES ('a;b');\nSELECT 1;",
			want: []string{"INSERT INTO t VALUES ('a;b')", "SELECT 1"},
		},
		{
			name: "trailing statement without semicolon",
			in:   "SELECT 1",
			want: []string{"SELECT 1"},
		},
		{
			name: "block comment stripped",
			in:   "/* header */ SELECT 1; SELECT 2;",
			want: []string{"SELECT 1", "SELECT 2"},
		},
		{
			name: "prepared-statement guard (005 shape)",
			in: "SET @stmt := IF(TRUE, 'ALTER TABLE t DROP INDEX i', 'SELECT 1');\n" +
				"PREPARE s FROM @stmt;\nEXECUTE s;\nDEALLOCATE PREPARE s;",
			want: []string{
				"SET @stmt := IF(TRUE, 'ALTER TABLE t DROP INDEX i', 'SELECT 1')",
				"PREPARE s FROM @stmt",
				"EXECUTE s",
				"DEALLOCATE PREPARE s",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := splitStatements(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("got %d statements %q, want %d %q", len(got), got, len(tc.want), tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("statement %d: got %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// TestMigration016_PaymentsGatewayTransactionIdIndex checks the embedded 016
// migration (issue #72) creates a plain (non-partial) index on
// payments.gateway_transaction_id, mirroring postgres migration 017. MySQL
// has no partial indexes, so this must NOT carry a WHERE predicate. This is a
// content assertion and needs no database.
func TestMigration016_PaymentsGatewayTransactionIdIndex(t *testing.T) {
	data, err := Migrations.ReadFile("migrations/016_payments_gateway_transaction_id_index.up.sql")
	if err != nil {
		t.Fatalf("read migration 016: %v", err)
	}
	sql := string(data)

	// Ignore comment lines so prose mentioning the statement does not satisfy
	// the assertion; only executable statements count.
	var exec strings.Builder
	for _, line := range strings.Split(sql, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		exec.WriteString(line)
		exec.WriteString("\n")
	}
	stmt := strings.ToUpper(exec.String())

	if !strings.Contains(stmt, "CREATE INDEX IDX_PAYMENTS_GATEWAY_TRANSACTION_ID") {
		t.Errorf("016 must create idx_payments_gateway_transaction_id, got:\n%s", sql)
	}
	if !strings.Contains(stmt, "ON PAYMENTS (GATEWAY_TRANSACTION_ID)") {
		t.Errorf("016 must index payments.gateway_transaction_id, got:\n%s", sql)
	}
	if strings.Contains(stmt, "WHERE") {
		t.Errorf("016 must not use a WHERE predicate (MySQL has no partial indexes), got:\n%s", sql)
	}
}

// TestSplitStatements_RealMigrations verifies every embedded migration file
// splits without a dangling empty statement and that each split statement is
// non-empty — a guard against a file whose form the splitter cannot handle.
func TestSplitStatements_RealMigrations(t *testing.T) {
	names, err := migrationFilenames(Migrations, migrationsDir)
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	if len(names) == 0 {
		t.Fatal("no embedded migrations found")
	}
	for _, name := range names {
		data, err := Migrations.ReadFile(migrationsDir + "/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		stmts := splitStatements(string(data))
		if len(stmts) == 0 {
			t.Errorf("%s: split into 0 statements", name)
		}
		for i, s := range stmts {
			if s == "" {
				t.Errorf("%s: statement %d is empty", name, i)
			}
		}
	}
}
