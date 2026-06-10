package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/contract-to-cash/core/application/tx"
	"github.com/contract-to-cash/core/domain/balance"
	"github.com/contract-to-cash/core/domain/shared"
)

func newBalanceRepo(t *testing.T) (*MySQLBalanceRepository, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewBalanceRepository(db), mock
}

// newBalanceEntry builds a fresh entry (loadedVersion 0 -> INSERT path).
func newBalanceEntry(t *testing.T) *balance.BalanceEntry {
	t.Helper()
	return balance.NewBalanceEntry("acct-1", jpy(1000), balance.BalanceReason("proration"), fixedTime)
}

// loadedBalanceEntry builds an entry as if loaded from the DB at the given
// version (loadedVersion == version -> UPDATE path).
func loadedBalanceEntry(t *testing.T, version int) *balance.BalanceEntry {
	t.Helper()
	e, err := balance.FromSnapshot(balance.BalanceEntrySnapshot{
		ID:              "bal-1",
		AccountID:       "acct-1",
		OriginalAmount:  jpy(1000),
		RemainingAmount: jpy(1000),
		Reason:          balance.BalanceReason("proration"),
		CreatedAt:       fixedTime,
		Version:         version,
	})
	if err != nil {
		t.Fatalf("FromSnapshot: %v", err)
	}
	return e
}

func balanceFindRow() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "account_id", "original_amount", "remaining_amount", "currency",
		"reason", "source_type", "source_id", "description", "expires_at", "version", "created_at",
	}).AddRow(
		"bal-1", "acct-1", int64(1000), int64(1000), "JPY",
		"proration", "", "", "", nil, 0, fixedTime,
	)
}

func TestBalanceRepo_Save_Insert(t *testing.T) {
	repo, mock := newBalanceRepo(t)
	e := newBalanceEntry(t)
	s := e.ToSnapshot()

	mock.ExpectExec(`INSERT INTO balance_entries`).
		WithArgs(string(s.ID), "acct-1", int64(1000), int64(1000), "JPY",
			"proration", "", "", "", nil, s.Version, fixedTime).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Save(context.Background(), e); err != nil {
		t.Fatalf("Save insert: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestBalanceRepo_Save_Update(t *testing.T) {
	repo, mock := newBalanceRepo(t)
	e := loadedBalanceEntry(t, 3)
	s := e.ToSnapshot()

	mock.ExpectExec(`UPDATE balance_entries SET .* WHERE id = \? AND version = \?`).
		WithArgs(int64(1000), int64(1000), "JPY", "proration", "", "", "", nil,
			s.Version, "bal-1", 3).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.Save(context.Background(), e); err != nil {
		t.Fatalf("Save update: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// A version-guarded UPDATE that affects zero rows means another writer advanced
// the version: the repo must surface tx.ErrVersionConflict.
func TestBalanceRepo_Save_VersionConflict(t *testing.T) {
	repo, mock := newBalanceRepo(t)
	e := loadedBalanceEntry(t, 3)

	mock.ExpectExec(`UPDATE balance_entries SET .* WHERE id = \? AND version = \?`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := repo.Save(context.Background(), e)
	if !errors.Is(err, tx.ErrVersionConflict) {
		t.Fatalf("expected tx.ErrVersionConflict, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestBalanceRepo_FindByID_Found(t *testing.T) {
	repo, mock := newBalanceRepo(t)
	mock.ExpectQuery(`SELECT .* FROM balance_entries WHERE id = \?`).
		WithArgs("bal-1").
		WillReturnRows(balanceFindRow())

	got, err := repo.FindByID(context.Background(), "bal-1")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if got.ToSnapshot().RemainingAmount.Int64() != 1000 {
		t.Errorf("remaining = %d, want 1000", got.ToSnapshot().RemainingAmount.Int64())
	}
}

func TestBalanceRepo_FindByID_NotFound(t *testing.T) {
	repo, mock := newBalanceRepo(t)
	mock.ExpectQuery(`SELECT .* FROM balance_entries WHERE id = \?`).
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)

	_, err := repo.FindByID(context.Background(), "missing")
	var de *shared.DomainError
	if !errors.As(err, &de) || de.Code != shared.ErrCodeNotFound {
		t.Fatalf("expected not_found DomainError, got %v", err)
	}
}

func TestBalanceRepo_FindAvailable(t *testing.T) {
	repo, mock := newBalanceRepo(t)
	mock.ExpectQuery(`SELECT .* FROM balance_entries WHERE account_id = \? AND currency = \? AND remaining_amount > 0 AND \(expires_at IS NULL OR expires_at > NOW\(6\)\) ORDER BY created_at ASC`).
		WithArgs("acct-1", "JPY").
		WillReturnRows(balanceFindRow())

	got, err := repo.FindAvailable(context.Background(), "acct-1", "JPY")
	if err != nil {
		t.Fatalf("FindAvailable: %v", err)
	}
	if len(got) != 1 || got[0].ToSnapshot().ID != "bal-1" {
		t.Fatalf("unexpected result: %+v", got)
	}
}
