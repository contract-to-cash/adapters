package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/contract-to-cash/core/application/tx"
)

func newTxManager(t *testing.T) (*MySQLTxManager, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	factory := func(q Querier) tx.Repos { return tx.Repos{} }
	return NewTxManager(db, factory), mock
}

func TestTxManager_CommitOnSuccess(t *testing.T) {
	mgr, mock := newTxManager(t)

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO projection_checkpoints`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := mgr.RunInTx(context.Background(), func(ctx context.Context, repos tx.Repos) error {
		q := querierFromContext(ctx, nil)
		_, err := q.ExecContext(ctx,
			`INSERT INTO projection_checkpoints (projector_name, last_position) VALUES ('test-tx', 42)`)
		return err
	})
	if err != nil {
		t.Fatalf("RunInTx: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestTxManager_RollbackOnError(t *testing.T) {
	mgr, mock := newTxManager(t)

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO projection_checkpoints`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectRollback()

	testErr := errors.New("intentional")
	err := mgr.RunInTx(context.Background(), func(ctx context.Context, repos tx.Repos) error {
		q := querierFromContext(ctx, nil)
		_, _ = q.ExecContext(ctx,
			`INSERT INTO projection_checkpoints (projector_name, last_position) VALUES ('test-rollback', 99)`)
		return testErr
	})
	if !errors.Is(err, testErr) {
		t.Fatalf("expected testErr, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// A nested RunInTx must reuse the outer transaction (no second Begin/Commit):
// both writes land on the single ambient tx.
func TestTxManager_NestedRunInTx(t *testing.T) {
	mgr, mock := newTxManager(t)

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO projection_checkpoints`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO projection_checkpoints`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := mgr.RunInTx(context.Background(), func(outerCtx context.Context, _ tx.Repos) error {
		q := querierFromContext(outerCtx, nil)
		if _, err := q.ExecContext(outerCtx,
			`INSERT INTO projection_checkpoints (projector_name, last_position) VALUES ('outer', 1)`); err != nil {
			return err
		}
		// Nested RunInTx should reuse the outer transaction.
		return mgr.RunInTx(outerCtx, func(innerCtx context.Context, _ tx.Repos) error {
			q := querierFromContext(innerCtx, nil)
			_, err := q.ExecContext(innerCtx,
				`INSERT INTO projection_checkpoints (projector_name, last_position) VALUES ('inner', 2)`)
			return err
		})
	})
	if err != nil {
		t.Fatalf("RunInTx: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// The nested closure receives the same ambient *sql.Tx as the outer one.
func TestTxManager_NestedReusesSameTx(t *testing.T) {
	mgr, mock := newTxManager(t)
	mock.ExpectBegin()
	mock.ExpectCommit()

	var outerTx, innerTx *sql.Tx
	err := mgr.RunInTx(context.Background(), func(outerCtx context.Context, _ tx.Repos) error {
		outerTx, _ = TxFromContext(outerCtx)
		return mgr.RunInTx(outerCtx, func(innerCtx context.Context, _ tx.Repos) error {
			innerTx, _ = TxFromContext(innerCtx)
			return nil
		})
	})
	if err != nil {
		t.Fatalf("RunInTx: %v", err)
	}
	if outerTx == nil || outerTx != innerTx {
		t.Errorf("nested tx not reused: outer=%p inner=%p", outerTx, innerTx)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
