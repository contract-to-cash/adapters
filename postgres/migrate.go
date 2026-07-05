package postgres

import (
	"context"
	"fmt"
	"io/fs"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

// migrationsDir is the embed.FS sub-directory holding the *.sql files.
const migrationsDir = "migrations"

// createSchemaMigrationsSQL bootstraps the ledger that records which migration
// files have been applied. status is 'applied' for a completed file; it exists
// only to mirror the mysql adapter's partial-apply detection (PostgreSQL DDL is
// transactional, so a postgres row is never left in any other state).
const createSchemaMigrationsSQL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename   TEXT        PRIMARY KEY,
    status     TEXT        NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`

// Migrate applies every embedded migration file that has not yet been recorded
// in schema_migrations, in filename order. Each file runs inside its own
// transaction together with the bookkeeping INSERT, so PostgreSQL's
// transactional DDL makes a file all-or-nothing: a failure rolls the whole file
// back and leaves no schema_migrations row, so a re-run retries it cleanly.
//
// Unlike the old test-only loop this does NOT swallow "already exists" errors:
// a file is applied exactly once (tracked by schema_migrations) and any error
// from a not-yet-applied file is a real failure. Bring an existing, untracked
// database under management by seeding schema_migrations with the files it has
// already had applied.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return migratePool(ctx, pool, Migrations, migrationsDir)
}

func migratePool(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS, dir string) error {
	if _, err := pool.Exec(ctx, createSchemaMigrationsSQL); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied, err := loadAppliedMigrations(ctx, pool)
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
		if err := applyMigrationFile(ctx, pool, name, string(data)); err != nil {
			return err
		}
	}
	return nil
}

func loadAppliedMigrations(ctx context.Context, pool *pgxpool.Pool) (map[string]bool, error) {
	rows, err := pool.Query(ctx, `SELECT filename, status FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("load schema_migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var filename, status string
		if err := rows.Scan(&filename, &status); err != nil {
			return nil, fmt.Errorf("scan schema_migrations: %w", err)
		}
		if status != "applied" {
			// Should never happen for postgres (transactional DDL), but guard
			// anyway so a hand-corrupted ledger surfaces instead of silently
			// re-running a half-applied file.
			return nil, fmt.Errorf("migration %s is in state %q, expected 'applied': manual intervention required", filename, status)
		}
		applied[filename] = true
	}
	return applied, rows.Err()
}

// applyMigrationFile runs one migration file and its bookkeeping INSERT in a
// single transaction. The file SQL is executed with no bind arguments so pgx
// uses the simple query protocol, which (unlike the extended protocol) accepts
// the multiple statements a migration file contains.
func applyMigrationFile(ctx context.Context, pool *pgxpool.Pool, name, sql string) (err error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", name, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	if _, err = tx.Exec(ctx, sql); err != nil {
		return fmt.Errorf("apply migration %s: %w", name, err)
	}
	if _, err = tx.Exec(ctx,
		`INSERT INTO schema_migrations (filename, status) VALUES ($1, 'applied')`, name); err != nil {
		return fmt.Errorf("record migration %s: %w", name, err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %s: %w", name, err)
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
