package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/contract-to-cash/core/application/tx"
	"github.com/contract-to-cash/core/domain/invoice"
	"github.com/contract-to-cash/core/domain/shared"
	driver "github.com/go-sql-driver/mysql"
)

func newCreditNoteRepo(t *testing.T) (*MySQLCreditNoteRepository, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewCreditNoteRepository(db), mock
}

func sampleCreditNote(t *testing.T) *invoice.CreditNote {
	t.Helper()
	cn, err := invoice.CreditNoteFromSnapshot(invoice.CreditNoteSnapshot{
		ID:         "cn-1",
		Number:     "CN-0001",
		InvoiceID:  "inv-1",
		AccountID:  "acct-1",
		ContractID: "contract-1",
		Status:     invoice.CreditNoteStatusIssued,
		Reason:     invoice.CreditNoteReasonOrderChange,
		Memo:       "partial credit",
		Items: []invoice.CreditNoteItemSnapshot{
			{InvoiceLineItemID: "li-1", Description: "refund", Amount: jpy(500), TaxAmount: jpy(50)},
		},
		Subtotal:     jpy(500),
		TaxAmount:    jpy(50),
		Total:        jpy(550),
		CreditAmount: jpy(550),
		RefundAmount: jpy(0),
		IssuedAt:     &fixedTime,
		CreatedAt:    fixedTime,
	})
	if err != nil {
		t.Fatalf("CreditNoteFromSnapshot: %v", err)
	}
	return cn
}

// creditNoteFindRow mirrors the selectCreditNoteSQL column order.
func creditNoteFindRow(t *testing.T) *sqlmock.Rows {
	t.Helper()
	js, err := json.Marshal(creditNoteJSONState{
		Items: []invoice.CreditNoteItemSnapshot{
			{InvoiceLineItemID: "li-1", Description: "refund", Amount: jpy(500), TaxAmount: jpy(50)},
		},
		Subtotal: jpy(500), TaxAmount: jpy(50), Total: jpy(550),
		CreditAmount: jpy(550), RefundAmount: jpy(0),
	})
	if err != nil {
		t.Fatalf("marshal json state: %v", err)
	}
	return sqlmock.NewRows([]string{
		"id", "number", "invoice_id", "account_id", "contract_id", "status", "reason", "memo",
		"items", "issued_at", "created_at", "lock_version",
	}).AddRow(
		"cn-1", "CN-0001", "inv-1", "acct-1", "contract-1", "issued", "order_change", "partial credit",
		js, fixedTime, fixedTime, int64(0),
	)
}

// A brand-new credit note INSERTs (lock_version carried directly). See issue #147.
func TestCreditNoteRepo_Save_InsertNew(t *testing.T) {
	repo, mock := newCreditNoteRepo(t)
	cn := sampleCreditNote(t)

	mock.ExpectExec(`INSERT INTO credit_notes`).
		WithArgs("cn-1", "CN-0001", "inv-1", "contract-1", "acct-1",
			"issued", "order_change", "partial credit",
			sqlmock.AnyArg(), int64(500), int64(50), int64(550),
			int64(550), int64(0), "JPY", fixedTime, int64(0)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Save(context.Background(), cn); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// An existing credit note (PRIMARY-key duplicate on INSERT) is routed to a
// version-guarded UPDATE. One changed row means the guard passed. See issue #147.
func TestCreditNoteRepo_Save_UpdateExisting(t *testing.T) {
	repo, mock := newCreditNoteRepo(t)
	cn := sampleCreditNote(t)

	mock.ExpectExec(`INSERT INTO credit_notes`).
		WillReturnError(&driver.MySQLError{Number: 1062, Message: "Duplicate entry 'cn-1' for key 'credit_notes.PRIMARY'"})
	mock.ExpectExec(`UPDATE credit_notes SET`).
		WithArgs("CN-0001", "inv-1", "contract-1", "acct-1",
			"issued", "order_change", "partial credit",
			sqlmock.AnyArg(), int64(500), int64(50), int64(550),
			int64(550), int64(0), "JPY", fixedTime, int64(0), "cn-1", int64(0)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.Save(context.Background(), cn); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// A concurrent writer already advanced lock_version: the guarded UPDATE changes
// zero rows and Save must return tx.ErrVersionConflict rather than silently
// succeeding (last-writer-wins). This is what stops a concurrent Apply-vs-Refund
// on the same issued note from both persisting. See issue #147.
func TestCreditNoteRepo_Save_VersionConflict(t *testing.T) {
	repo, mock := newCreditNoteRepo(t)
	cn := sampleCreditNote(t)

	mock.ExpectExec(`INSERT INTO credit_notes`).
		WillReturnError(&driver.MySQLError{Number: 1062, Message: "Duplicate entry 'cn-1' for key 'credit_notes.PRIMARY'"})
	mock.ExpectExec(`UPDATE credit_notes SET`).
		WithArgs("CN-0001", "inv-1", "contract-1", "acct-1",
			"issued", "order_change", "partial credit",
			sqlmock.AnyArg(), int64(500), int64(50), int64(550),
								int64(550), int64(0), "JPY", fixedTime, int64(0), "cn-1", int64(0)).
		WillReturnResult(sqlmock.NewResult(0, 0)) // zero rows changed: version guard missed

	if err := repo.Save(context.Background(), cn); !errors.Is(err, tx.ErrVersionConflict) {
		t.Fatalf("expected tx.ErrVersionConflict, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestCreditNoteRepo_FindByID_Found(t *testing.T) {
	repo, mock := newCreditNoteRepo(t)
	mock.ExpectQuery(`SELECT .* FROM credit_notes WHERE id = \?`).
		WithArgs("cn-1").
		WillReturnRows(creditNoteFindRow(t))

	got, err := repo.FindByID(context.Background(), "cn-1")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	s := got.ToSnapshot()
	if s.ID != "cn-1" || s.Total.Int64() != 550 {
		t.Errorf("unexpected credit note: %+v", s)
	}
	if len(s.Items) != 1 || s.Items[0].Amount.Int64() != 500 {
		t.Errorf("items not restored: %+v", s.Items)
	}
}

func TestCreditNoteRepo_FindByID_NotFound(t *testing.T) {
	repo, mock := newCreditNoteRepo(t)
	mock.ExpectQuery(`SELECT .* FROM credit_notes WHERE id = \?`).
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)

	_, err := repo.FindByID(context.Background(), "missing")
	var de *shared.DomainError
	if !errors.As(err, &de) || de.Code != shared.ErrCodeNotFound {
		t.Fatalf("expected not_found DomainError, got %v", err)
	}
}

func TestCreditNoteRepo_FindByInvoiceID(t *testing.T) {
	repo, mock := newCreditNoteRepo(t)
	mock.ExpectQuery(`SELECT .* FROM credit_notes WHERE invoice_id = \? ORDER BY created_at DESC`).
		WithArgs("inv-1").
		WillReturnRows(creditNoteFindRow(t))

	got, err := repo.FindByInvoiceID(context.Background(), "inv-1")
	if err != nil {
		t.Fatalf("FindByInvoiceID: %v", err)
	}
	if len(got) != 1 || got[0].ToSnapshot().ID != "cn-1" {
		t.Fatalf("unexpected result: %+v", got)
	}
}
