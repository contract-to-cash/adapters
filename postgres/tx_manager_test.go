package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/contract-to-cash/adapters/postgres"
	"github.com/contract-to-cash/adapters/postgres/postgrestest"
	"github.com/contract-to-cash/core/application/tx"
)

func TestTxManager_CommitOnSuccess(t *testing.T) {
	pool := postgrestest.NewPool(t)
	factory := func(q postgres.Querier) tx.Repos { return tx.Repos{} }
	mgr := postgres.NewTxManager(pool, factory)
	ctx := context.Background()

	err := mgr.RunInTx(ctx, func(ctx context.Context, repos tx.Repos) error {
		q := postgres.QuerierFromContext(ctx, pool)
		_, err := q.Exec(ctx,
			`INSERT INTO projection_checkpoints (projector_name, last_position) VALUES ('test-tx', 42)`)
		return err
	})
	if err != nil {
		t.Fatalf("RunInTx: %v", err)
	}

	// Verify committed.
	var pos int64
	err = pool.QueryRow(ctx, `SELECT last_position FROM projection_checkpoints WHERE projector_name = 'test-tx'`).Scan(&pos)
	if err != nil {
		t.Fatalf("query after commit: %v", err)
	}
	if pos != 42 {
		t.Errorf("pos = %d, want 42", pos)
	}
}

func TestTxManager_RollbackOnError(t *testing.T) {
	pool := postgrestest.NewPool(t)
	factory := func(q postgres.Querier) tx.Repos { return tx.Repos{} }
	mgr := postgres.NewTxManager(pool, factory)
	ctx := context.Background()

	testErr := errors.New("intentional")
	err := mgr.RunInTx(ctx, func(ctx context.Context, repos tx.Repos) error {
		q := postgres.QuerierFromContext(ctx, pool)
		_, _ = q.Exec(ctx,
			`INSERT INTO projection_checkpoints (projector_name, last_position) VALUES ('test-rollback', 99)`)
		return testErr
	})
	if !errors.Is(err, testErr) {
		t.Fatalf("expected testErr, got %v", err)
	}

	// Verify rolled back.
	var count int
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM projection_checkpoints WHERE projector_name = 'test-rollback'`).Scan(&count)
	if count != 0 {
		t.Errorf("expected 0 rows after rollback, got %d", count)
	}
}

func TestTxManager_NestedRunInTx(t *testing.T) {
	pool := postgrestest.NewPool(t)
	factory := func(q postgres.Querier) tx.Repos { return tx.Repos{} }
	mgr := postgres.NewTxManager(pool, factory)
	ctx := context.Background()

	err := mgr.RunInTx(ctx, func(outerCtx context.Context, _ tx.Repos) error {
		q := postgres.QuerierFromContext(outerCtx, pool)
		_, err := q.Exec(outerCtx,
			`INSERT INTO projection_checkpoints (projector_name, last_position) VALUES ('outer', 1)`)
		if err != nil {
			return err
		}

		// Nested RunInTx should reuse the outer transaction.
		return mgr.RunInTx(outerCtx, func(innerCtx context.Context, _ tx.Repos) error {
			q := postgres.QuerierFromContext(innerCtx, pool)
			_, err := q.Exec(innerCtx,
				`INSERT INTO projection_checkpoints (projector_name, last_position) VALUES ('inner', 2)`)
			return err
		})
	})
	if err != nil {
		t.Fatalf("RunInTx: %v", err)
	}

	// Both writes should be committed.
	var count int
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM projection_checkpoints`).Scan(&count)
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}
