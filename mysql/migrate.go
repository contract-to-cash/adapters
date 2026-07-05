package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

// migrationsDir is the embed.FS sub-directory holding the *.sql files.
const migrationsDir = "migrations"

// createSchemaMigrationsSQL bootstraps the ledger recording which migration
// files have been applied. Unlike PostgreSQL, MySQL DDL auto-commits, so a
// migration file cannot be rolled back as a unit. The status column makes a
// half-applied file detectable: a row is written as 'pending' BEFORE the DDL
// runs and flipped to 'applied' only after every statement in the file
// succeeds. A row stuck at 'pending' therefore means the file failed midway and
// the database needs manual reconciliation before migrations can proceed.
const createSchemaMigrationsSQL = "CREATE TABLE IF NOT EXISTS schema_migrations (" +
	"filename VARCHAR(191) NOT NULL," +
	"status VARCHAR(32) NOT NULL," +
	"applied_at DATETIME(6) NOT NULL DEFAULT NOW(6)," +
	"PRIMARY KEY (filename)" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

const (
	migrationStatusPending = "pending"
	migrationStatusApplied = "applied"
)

// migrationExecutor is satisfied by *sql.Conn, *sql.DB, and *sql.Tx. Migrate
// pins a single *sql.Conn so session state (user variables set by e.g. the 005
// PREPARE/EXECUTE guards) survives between statements — a pooled *sql.DB could
// scatter consecutive statements across connections and lose it.
type migrationExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Migrate applies every embedded migration file not yet recorded as 'applied'
// in schema_migrations, in filename order. Each file's statements run on a
// single dedicated connection, guarded by a 'pending'→'applied' status marker
// so a mid-file failure (MySQL cannot roll DDL back) is detectable on the next
// run rather than silently skipped.
//
// This does NOT swallow "already exists" errors: a file is applied exactly once
// (tracked by schema_migrations) and any error from a not-yet-applied file is a
// real failure. Bring an existing, untracked database under management by
// seeding schema_migrations with the files it has already had applied.
func Migrate(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer func() { _ = conn.Close() }()
	return migrateOn(ctx, conn, Migrations, migrationsDir)
}

func migrateOn(ctx context.Context, exec migrationExecutor, fsys fs.FS, dir string) error {
	if _, err := exec.ExecContext(ctx, createSchemaMigrationsSQL); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied, err := loadAppliedMigrations(ctx, exec)
	if err != nil {
		return err
	}

	names, err := migrationFilenames(fsys, dir)
	if err != nil {
		return err
	}

	for _, name := range names {
		if applied[name] {
			continue
		}
		data, err := fs.ReadFile(fsys, dir+"/"+name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if err := applyMigrationFile(ctx, exec, name, string(data)); err != nil {
			return err
		}
	}
	return nil
}

func loadAppliedMigrations(ctx context.Context, exec migrationExecutor) (map[string]bool, error) {
	rows, err := exec.QueryContext(ctx, "SELECT filename, status FROM schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("load schema_migrations: %w", err)
	}
	defer func() { _ = rows.Close() }()

	applied := make(map[string]bool)
	for rows.Next() {
		var filename, status string
		if err := rows.Scan(&filename, &status); err != nil {
			return nil, fmt.Errorf("scan schema_migrations: %w", err)
		}
		if status != migrationStatusApplied {
			return nil, fmt.Errorf(
				"migration %s is in state %q (partially applied): manual reconciliation required before migrating",
				filename, status)
		}
		applied[filename] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate schema_migrations: %w", err)
	}
	return applied, nil
}

// applyMigrationFile records the file as 'pending', executes each statement in
// order, then flips it to 'applied'. The 'pending' marker is committed (MySQL
// auto-commits every statement) before the DDL, so a failure leaves the row at
// 'pending' for detection on the next run.
func applyMigrationFile(ctx context.Context, exec migrationExecutor, name, sql string) error {
	if _, err := exec.ExecContext(ctx,
		"INSERT INTO schema_migrations (filename, status, applied_at) VALUES (?, ?, NOW(6))",
		name, migrationStatusPending); err != nil {
		return fmt.Errorf("mark migration %s pending: %w", name, err)
	}

	for i, stmt := range splitStatements(sql) {
		if _, err := exec.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("apply migration %s (statement %d): %w", name, i+1, err)
		}
	}

	if _, err := exec.ExecContext(ctx,
		"UPDATE schema_migrations SET status = ?, applied_at = NOW(6) WHERE filename = ?",
		migrationStatusApplied, name); err != nil {
		return fmt.Errorf("mark migration %s applied: %w", name, err)
	}
	return nil
}

func migrationFilenames(fsys fs.FS, dir string) ([]string, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("read migrations dir: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, nil
}

// splitStatements breaks a migration file into individual statements on the
// top-level ';' delimiter. MySQL (via go-sql-driver without multiStatements)
// rejects multi-statement Exec, so files must be executed one statement at a
// time. Semicolons inside single-quoted strings, backtick-quoted identifiers,
// line comments (-- ... and # ...) and block comments (/* ... */) are ignored
// so the PREPARE/EXECUTE guards in 005 and default string literals split
// correctly. Comments are stripped from the emitted statements.
func splitStatements(sql string) []string {
	var (
		out        []string
		buf        strings.Builder
		inSingle   bool // inside '...'
		inBacktick bool // inside `...`
	)

	flush := func() {
		stmt := strings.TrimSpace(buf.String())
		if stmt != "" {
			out = append(out, stmt)
		}
		buf.Reset()
	}

	for i := 0; i < len(sql); i++ {
		c := sql[i]

		if inSingle {
			buf.WriteByte(c)
			switch {
			case c == '\\' && i+1 < len(sql): // backslash escape (MySQL default)
				i++
				buf.WriteByte(sql[i])
			case c == '\'' && i+1 < len(sql) && sql[i+1] == '\'': // doubled quote
				i++
				buf.WriteByte(sql[i])
			case c == '\'':
				inSingle = false
			}
			continue
		}
		if inBacktick {
			buf.WriteByte(c)
			if c == '`' {
				inBacktick = false
			}
			continue
		}

		// Not inside a string: check for comments.
		if c == '-' && i+1 < len(sql) && sql[i+1] == '-' &&
			(i+2 >= len(sql) || sql[i+2] == ' ' || sql[i+2] == '\t' || sql[i+2] == '\n' || sql[i+2] == '\r') {
			i = skipToLineEnd(sql, i)
			continue
		}
		if c == '#' {
			i = skipToLineEnd(sql, i)
			continue
		}
		if c == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			i = skipBlockComment(sql, i+2)
			continue
		}

		switch c {
		case '\'':
			inSingle = true
			buf.WriteByte(c)
		case '`':
			inBacktick = true
			buf.WriteByte(c)
		case ';':
			flush()
		default:
			buf.WriteByte(c)
		}
	}
	flush()
	return out
}

// skipToLineEnd returns the index of the newline that ends the line comment
// starting at i, or len-1 if the comment runs to EOF.
func skipToLineEnd(sql string, i int) int {
	for j := i; j < len(sql); j++ {
		if sql[j] == '\n' {
			return j
		}
	}
	return len(sql) - 1
}

// skipBlockComment returns the index of the '/' that closes the block comment
// whose body starts at i, or len-1 if it is unterminated.
func skipBlockComment(sql string, i int) int {
	for j := i; j+1 < len(sql); j++ {
		if sql[j] == '*' && sql[j+1] == '/' {
			return j + 1
		}
	}
	return len(sql) - 1
}
