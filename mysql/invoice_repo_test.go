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
	return NewInvoiceRepository(db, shared.FixedClock{FixedTime: fixedTime}), mock
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

	// Without an ambient transaction, Save wraps its three writes in a local
	// transaction (issue #36) so a crash mid-sequence cannot corrupt
	// invoice_history. Expect Begin ... 3 statements ... Commit.
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO invoices .* ON DUPLICATE KEY UPDATE`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// Close and open the history rows at one shared timestamp (bound param),
	// not two separate NOW(6) evaluations.
	mock.ExpectExec(`UPDATE invoice_history SET valid_to = \? WHERE id = \? AND valid_to IS NULL`).
		WithArgs(sqlmock.AnyArg(), "inv-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`INSERT INTO invoice_history`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := repo.Save(context.Background(), inv); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Issue #39: the bitemporal history stamp comes from the injected clock, not
// wall-clock time.Now, so history windows are deterministic and testable with
// FixedClock. Both the close (UPDATE ... valid_to) and the open (INSERT ...
// valid_from) must be bound to the exact clock instant.
func TestInvoiceRepo_Save_StampsHistoryFromInjectedClock(t *testing.T) {
	repo, mock := newInvoiceRepo(t) // clock = FixedClock{fixedTime}
	inv := sampleInvoice(t)

	want := fixedTime.UTC()

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO invoices .* ON DUPLICATE KEY UPDATE`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// valid_to (close) bound to the clock instant, not sqlmock.AnyArg.
	mock.ExpectExec(`UPDATE invoice_history SET valid_to = \? WHERE id = \? AND valid_to IS NULL`).
		WithArgs(want, "inv-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	// valid_from (open) — 4th bound arg — bound to the same clock instant.
	mock.ExpectExec(`INSERT INTO invoice_history`).
		WithArgs("inv-1", "inv-1", sqlmock.AnyArg(), want).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := repo.Save(context.Background(), inv); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Issue #36: when an ambient transaction IS present, Save must join it and NOT
// open a nested transaction — the three writes run directly on the caller's tx,
// which owns begin/commit. sqlmock records the outer Begin/Commit driven by this
// test; Save itself must emit no additional Begin.
func TestInvoiceRepo_Save_JoinsAmbientTx(t *testing.T) {
	repo, mock := newInvoiceRepo(t)
	inv := sampleInvoice(t)

	mock.ExpectBegin()
	sqlTx, err := repo.db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	ctx := ContextWithTx(context.Background(), sqlTx)

	// No further ExpectBegin here: Save must reuse the ambient tx.
	mock.ExpectExec(`INSERT INTO invoices .* ON DUPLICATE KEY UPDATE`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE invoice_history SET valid_to = \? WHERE id = \? AND valid_to IS NULL`).
		WithArgs(sqlmock.AnyArg(), "inv-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`INSERT INTO invoice_history`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := repo.Save(ctx, inv); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := sqlTx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Issue #36: a failure partway through the three-statement sequence must roll
// the whole unit back — never leave the invoices row committed while the history
// rows are inconsistent. Here the second statement (closing the prior history
// row) fails; Save must Rollback (not Commit) and surface the error.
func TestInvoiceRepo_Save_RollbackOnMidSequenceFailure(t *testing.T) {
	repo, mock := newInvoiceRepo(t)
	inv := sampleInvoice(t)

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO invoices .* ON DUPLICATE KEY UPDATE`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE invoice_history SET valid_to = \? WHERE id = \? AND valid_to IS NULL`).
		WithArgs(sqlmock.AnyArg(), "inv-1").
		WillReturnError(errors.New("connection reset"))
	mock.ExpectRollback()

	err := repo.Save(context.Background(), inv)
	if err == nil {
		t.Fatal("expected error from mid-sequence failure, got nil")
	}
	// The third statement must never run and the tx must roll back, not commit.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations (rollback not performed?): %v", err)
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
