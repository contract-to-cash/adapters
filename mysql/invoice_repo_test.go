package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/contract-to-cash/core/domain/invoice"
	"github.com/contract-to-cash/core/domain/shared"
)

func newInvoiceRepo(t *testing.T) (*MySQLInvoiceRepository, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewInvoiceRepository(db), mock
}

func sampleInvoice(t *testing.T) *invoice.Invoice {
	t.Helper()
	inv, err := invoice.InvoiceFromSnapshot(invoice.InvoiceSnapshot{
		ID:             "inv-1",
		InvoiceNumber:  "INV-0001",
		AccountID:      "acct-1",
		ContractID:     "contract-1",
		Status:         invoice.InvoiceStatusIssued,
		Subtotal:       jpy(1000),
		TaxAmount:      jpy(100),
		DiscountAmount: jpy(0),
		Total:          jpy(1100),
		AppliedBalance: jpy(0),
		AmountDue:      jpy(1100),
		PaidAmount:     jpy(0),
		Balance:        jpy(1100),
		IssueDate:      fixedTime,
		DueDate:        fixedTime,
	})
	if err != nil {
		t.Fatalf("InvoiceFromSnapshot: %v", err)
	}
	return inv
}

// invoiceFindRow builds a result row for selectInvoiceSQL, embedding the money
// state in the line_items JSON column exactly as scanInvoiceSnapshot expects.
func invoiceFindRow(t *testing.T) *sqlmock.Rows {
	t.Helper()
	js, err := json.Marshal(invoiceJSONState{
		Subtotal: jpy(1000), TaxAmount: jpy(100), DiscountAmount: jpy(0),
		Total: jpy(1100), AppliedBalance: jpy(0), AmountDue: jpy(1100),
		PaidAmount: jpy(0), Balance: jpy(1100),
	})
	if err != nil {
		t.Fatalf("marshal json state: %v", err)
	}
	return sqlmock.NewRows([]string{
		"id", "invoice_number", "account_id", "contract_id", "status",
		"line_items", "subtotal", "tax_amount", "discount_amount", "total",
		"applied_balance", "amount_due", "paid_amount", "balance",
		"billing_period_from", "billing_period_to", "issue_date", "due_date",
		"paid_at", "payment_method_id", "allow_partial_pay",
		"original_invoice_id", "revision_of", "void_reason", "metadata",
	}).AddRow(
		"inv-1", "INV-0001", "acct-1", "contract-1", "issued",
		js, int64(1000), int64(100), int64(0), int64(1100),
		int64(0), int64(1100), int64(0), int64(1100),
		nil, nil, fixedTime, fixedTime,
		nil, nil, false,
		nil, nil, "", []byte(`{}`),
	)
}

func TestInvoiceRepo_Save_Upsert(t *testing.T) {
	repo, mock := newInvoiceRepo(t)
	inv := sampleInvoice(t)

	mock.ExpectExec(`INSERT INTO invoices .* ON DUPLICATE KEY UPDATE`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE invoice_history SET valid_to = NOW\(6\) WHERE id = \? AND valid_to IS NULL`).
		WithArgs("inv-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`INSERT INTO invoice_history`).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Save(context.Background(), inv); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestInvoiceRepo_FindByID_Found(t *testing.T) {
	repo, mock := newInvoiceRepo(t)
	mock.ExpectQuery(`SELECT .* FROM invoices WHERE id = \?`).
		WithArgs("inv-1").
		WillReturnRows(invoiceFindRow(t))

	got, err := repo.FindByID(context.Background(), "inv-1")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	s := got.ToSnapshot()
	if s.ID != "inv-1" || s.Total.Int64() != 1100 {
		t.Errorf("unexpected invoice: %+v", s)
	}
	if s.IssueDate.Location() != nil && s.IssueDate.UTC() != fixedTime {
		t.Errorf("issue date = %v, want %v", s.IssueDate, fixedTime)
	}
}

// Issue #12: inside an ambient transaction FindByID must take a row lock so
// concurrent core FinalizeInvoice calls serialize (loser reads the finalized
// row and is rejected). Verify the FOR UPDATE clause is emitted only in a tx.
func TestInvoiceRepo_FindByID_ForUpdateInTx(t *testing.T) {
	repo, mock := newInvoiceRepo(t)

	mock.ExpectBegin()
	sqlTx, err := repo.db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	ctx := ContextWithTx(context.Background(), sqlTx)

	mock.ExpectQuery(`SELECT .* FROM invoices WHERE id = \? FOR UPDATE`).
		WithArgs("inv-1").
		WillReturnRows(invoiceFindRow(t))

	got, err := repo.FindByID(ctx, "inv-1")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if got.ToSnapshot().ID != "inv-1" {
		t.Errorf("unexpected invoice: %+v", got.ToSnapshot())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Outside a transaction (pooled read) no row lock is taken: the query must end
// at the id predicate with no trailing FOR UPDATE.
func TestInvoiceRepo_FindByID_NoLockOutsideTx(t *testing.T) {
	repo, mock := newInvoiceRepo(t)
	mock.ExpectQuery(`WHERE id = \?$`).
		WithArgs("inv-1").
		WillReturnRows(invoiceFindRow(t))

	if _, err := repo.FindByID(context.Background(), "inv-1"); err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestInvoiceRepo_FindByID_NotFound(t *testing.T) {
	repo, mock := newInvoiceRepo(t)
	mock.ExpectQuery(`SELECT .* FROM invoices WHERE id = \?`).
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)

	_, err := repo.FindByID(context.Background(), "missing")
	var de *shared.DomainError
	if !errors.As(err, &de) || de.Code != shared.ErrCodeNotFound {
		t.Fatalf("expected not_found DomainError, got %v", err)
	}
}

func TestInvoiceRepo_FindByContractID(t *testing.T) {
	repo, mock := newInvoiceRepo(t)
	mock.ExpectQuery(`SELECT .* FROM invoices WHERE contract_id = \? ORDER BY created_at DESC`).
		WithArgs("contract-1").
		WillReturnRows(invoiceFindRow(t))

	got, err := repo.FindByContractID(context.Background(), "contract-1")
	if err != nil {
		t.Fatalf("FindByContractID: %v", err)
	}
	if len(got) != 1 || got[0].ToSnapshot().ID != "inv-1" {
		t.Fatalf("unexpected result: %+v", got)
	}
}

// TestInvoiceRepo_FindOverdue asserts the query mirrors the core in-memory
// reference predicate: overdue-marked invoices (any due_date) plus
// issued/finalized invoices past their due date.
func TestInvoiceRepo_FindOverdue(t *testing.T) {
	repo, mock := newInvoiceRepo(t)
	mock.ExpectQuery(`SELECT .* FROM invoices WHERE status = 'overdue' OR \(status IN \('issued', 'finalized'\) AND due_date < NOW\(6\)\) ORDER BY due_date ASC`).
		WillReturnRows(invoiceFindRow(t))

	got, err := repo.FindOverdue(context.Background())
	if err != nil {
		t.Fatalf("FindOverdue: %v", err)
	}
	if len(got) != 1 || got[0].ToSnapshot().ID != "inv-1" {
		t.Fatalf("unexpected result: %+v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestInvoiceRepo_FindUnpaidByContract asserts refunded invoices are excluded
// from the unpaid set (parity with the core in-memory reference).
func TestInvoiceRepo_FindUnpaidByContract(t *testing.T) {
	repo, mock := newInvoiceRepo(t)
	mock.ExpectQuery(`SELECT .* FROM invoices WHERE contract_id = \? AND status NOT IN \('paid', 'voided', 'refunded'\) ORDER BY due_date IS NULL, due_date ASC`).
		WithArgs("contract-1").
		WillReturnRows(invoiceFindRow(t))

	got, err := repo.FindUnpaidByContract(context.Background(), "contract-1")
	if err != nil {
		t.Fatalf("FindUnpaidByContract: %v", err)
	}
	if len(got) != 1 || got[0].ToSnapshot().ID != "inv-1" {
		t.Fatalf("unexpected result: %+v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestInvoiceRepo_FindByIDAsOf(t *testing.T) {
	repo, mock := newInvoiceRepo(t)
	snap, err := json.Marshal(sampleInvoice(t).ToSnapshot())
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	mock.ExpectQuery(`SELECT snapshot FROM invoice_history WHERE id = \? AND valid_from <= \? AND \(valid_to IS NULL OR valid_to > \?\) ORDER BY version DESC LIMIT 1`).
		WithArgs("inv-1", fixedTime.UTC(), fixedTime.UTC()).
		WillReturnRows(sqlmock.NewRows([]string{"snapshot"}).AddRow(snap))

	got, err := repo.FindByIDAsOf(context.Background(), "inv-1", fixedTime)
	if err != nil {
		t.Fatalf("FindByIDAsOf: %v", err)
	}
	if got.ToSnapshot().ID != "inv-1" {
		t.Errorf("unexpected temporal invoice: %+v", got.ToSnapshot())
	}
}

func TestInvoiceRepo_FindByIDAsOf_NotFound(t *testing.T) {
	repo, mock := newInvoiceRepo(t)
	mock.ExpectQuery(`SELECT snapshot FROM invoice_history`).
		WithArgs("inv-1", fixedTime.UTC(), fixedTime.UTC()).
		WillReturnError(sql.ErrNoRows)

	_, err := repo.FindByIDAsOf(context.Background(), "inv-1", fixedTime)
	var de *shared.DomainError
	if !errors.As(err, &de) || de.Code != shared.ErrCodeNotFound {
		t.Fatalf("expected not_found DomainError, got %v", err)
	}
}
