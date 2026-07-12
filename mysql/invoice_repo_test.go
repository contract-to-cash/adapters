package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/contract-to-cash/core/application/tx"
	"github.com/contract-to-cash/core/domain/invoice"
	"github.com/contract-to-cash/core/domain/shared"
	driver "github.com/go-sql-driver/mysql"
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

// loadedInvoiceAtVersion returns a draft invoice reconstructed as if loaded from
// persistence at the given version (so LoadedVersion()==loaded) and then
// finalized once, bumping Version() to loaded+1. This models the read → mutate →
// Save sequence the optimistic-lock guard protects.
func loadedInvoiceAtVersion(t *testing.T, loaded int) *invoice.Invoice {
	t.Helper()
	inv, err := invoice.InvoiceFromSnapshot(invoice.InvoiceSnapshot{
		ID:             "inv-1",
		InvoiceNumber:  "INV-0001",
		AccountID:      "acct-1",
		ContractID:     "contract-1",
		Status:         invoice.InvoiceStatusDraft,
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
		Version:        loaded,
	})
	if err != nil {
		t.Fatalf("InvoiceFromSnapshot: %v", err)
	}
	if err := inv.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
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
		"original_invoice_id", "revision_of", "void_reason", "metadata", "lock_version",
	}).AddRow(
		"inv-1", "INV-0001", "acct-1", "contract-1", "issued",
		js, int64(1000), int64(100), int64(0), int64(1100),
		int64(0), int64(1100), int64(0), int64(1100),
		nil, nil, fixedTime, fixedTime,
		nil, nil, false,
		nil, nil, "", []byte(`{}`), int64(2),
	)
}

// A brand-new invoice (LoadedVersion()==0 and no row yet) takes the INSERT path:
// the version-guarded UPDATE matches no row, the existence probe finds none, so
// Save INSERTs then writes the two history rows. Without an ambient transaction
// the whole unit is wrapped in a local tx (issue #36): Begin ... statements ...
// Commit.
func TestInvoiceRepo_Save_InsertsNewInvoice(t *testing.T) {
	repo, mock := newInvoiceRepo(t)
	inv := sampleInvoice(t)

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE invoices SET .* WHERE id = \? AND lock_version = \?`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT 1 FROM invoices WHERE id = \?`).
		WithArgs("inv-1").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(`INSERT INTO invoices`).
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

// An existing invoice at the loaded version takes the guarded-UPDATE path: the
// version-guarded UPDATE matches its row (1 affected) so no INSERT is attempted,
// then the history rows are written. Save must persist inv.Version() as the new
// loaded baseline.
func TestInvoiceRepo_Save_UpdatesExistingInvoice(t *testing.T) {
	repo, mock := newInvoiceRepo(t)
	inv := loadedInvoiceAtVersion(t, 3)

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE invoices SET .* WHERE id = \? AND lock_version = \?`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			int64(4), "inv-1", int64(3)). // new version 4, guard on loaded version 3
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE invoice_history SET valid_to = \? WHERE id = \? AND valid_to IS NULL`).
		WithArgs(sqlmock.AnyArg(), "inv-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO invoice_history`).
		WithArgs("inv-1", "inv-1", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := repo.Save(context.Background(), inv); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if got := inv.LoadedVersion(); got != 4 {
		t.Errorf("LoadedVersion after Save = %d, want 4 (persisted version becomes new baseline)", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Issue #30: two loads of the same invoice at the same version, both mutated,
// racing to Save. The first wins; the second's guarded UPDATE matches no row
// (version already advanced) and the existence probe FINDS the row, so Save
// returns tx.ErrVersionConflict rather than silently inserting or last-writer-wins.
func TestInvoiceRepo_Save_VersionConflict(t *testing.T) {
	repo, mock := newInvoiceRepo(t)
	inv := loadedInvoiceAtVersion(t, 3)

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE invoices SET .* WHERE id = \? AND lock_version = \?`).
		WillReturnResult(sqlmock.NewResult(0, 0)) // 0 rows: version moved on
	mock.ExpectQuery(`SELECT 1 FROM invoices WHERE id = \?`).
		WithArgs("inv-1").
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1)) // row exists
	mock.ExpectRollback()

	err := repo.Save(context.Background(), inv)
	if !errors.Is(err, tx.ErrVersionConflict) {
		t.Fatalf("expected tx.ErrVersionConflict, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Issue #45: an INSERT of a second non-voided, non-proration invoice for a
// (contract_id, billing_period) already taken violates the ux_invoice_period
// unique index (1062). Save must translate that to a shared.ErrCodeConflict
// DomainError with the inmemory reference's message.
func TestInvoiceRepo_Save_PeriodConflict(t *testing.T) {
	repo, mock := newInvoiceRepo(t)
	inv := sampleInvoice(t)

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE invoices SET .* WHERE id = \? AND lock_version = \?`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT 1 FROM invoices WHERE id = \?`).
		WithArgs("inv-1").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(`INSERT INTO invoices`).
		WillReturnError(&driver.MySQLError{
			Number:  1062,
			Message: "Duplicate entry 'contract-1|...' for key 'invoices.ux_invoice_period'",
		})
	mock.ExpectRollback()

	err := repo.Save(context.Background(), inv)
	var de *shared.DomainError
	if !errors.As(err, &de) || de.Code != shared.ErrCodeConflict {
		t.Fatalf("expected ErrCodeConflict DomainError, got %v", err)
	}
	if de.Message != "invoice already exists for this billing period" {
		t.Errorf("unexpected conflict message: %q", de.Message)
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
	mock.ExpectExec(`UPDATE invoices SET .* WHERE id = \? AND lock_version = \?`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT 1 FROM invoices WHERE id = \?`).
		WithArgs("inv-1").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(`INSERT INTO invoices`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// valid_to (close) bound to the clock instant, not sqlmock.AnyArg.
	mock.ExpectExec(`UPDATE invoice_history SET valid_to = \? WHERE id = \? AND valid_to IS NULL`).
		WithArgs(want, "inv-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	// valid_from (open) — 4th bound arg — bound to the same clock instant.
	mock.ExpectExec(`INSERT INTO invoice_history`).
		WithArgs("inv-1", sqlmock.AnyArg(), sqlmock.AnyArg(), want).
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
// open a nested transaction — the writes run directly on the caller's tx, which
// owns begin/commit. sqlmock records the outer Begin/Commit driven by this test;
// Save itself must emit no additional Begin.
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
	mock.ExpectExec(`UPDATE invoices SET .* WHERE id = \? AND lock_version = \?`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT 1 FROM invoices WHERE id = \?`).
		WithArgs("inv-1").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(`INSERT INTO invoices`).
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

// Issue #36: a failure partway through the write sequence must roll the whole
// unit back — never leave the invoices row committed while the history rows are
// inconsistent. Here the history-close statement fails; Save must Rollback (not
// Commit) and surface the error.
func TestInvoiceRepo_Save_RollbackOnMidSequenceFailure(t *testing.T) {
	repo, mock := newInvoiceRepo(t)
	inv := sampleInvoice(t)

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE invoices SET .* WHERE id = \? AND lock_version = \?`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT 1 FROM invoices WHERE id = \?`).
		WithArgs("inv-1").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(`INSERT INTO invoices`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE invoice_history SET valid_to = \? WHERE id = \? AND valid_to IS NULL`).
		WithArgs(sqlmock.AnyArg(), "inv-1").
		WillReturnError(errors.New("connection reset"))
	mock.ExpectRollback()

	err := repo.Save(context.Background(), inv)
	if err == nil {
		t.Fatal("expected error from mid-sequence failure, got nil")
	}
	// The history-insert must never run and the tx must roll back, not commit.
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
	// Issue #30: the invoices.version column is authoritative and restored onto
	// both Version() and LoadedVersion() so the next Save guards on it.
	if got.Version() != 2 || got.LoadedVersion() != 2 {
		t.Errorf("version/loadedVersion = %d/%d, want 2/2", got.Version(), got.LoadedVersion())
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

// FindByContractAndPeriod must filter on billing-period EQUALITY
// (billing_period_from / billing_period_to), NOT on issue_date. Matching on
// issue_date breaks arrears billing (a June invoice issued July 1 falls outside
// the June window) and can falsely return an unrelated invoice merely issued
// within the queried window. This asserts the query shape and bound arguments.
func TestInvoiceRepo_FindByContractAndPeriod_MatchesBillingPeriod(t *testing.T) {
	repo, mock := newInvoiceRepo(t)

	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	period, err := shared.NewDateRange(start, end)
	if err != nil {
		t.Fatalf("NewDateRange: %v", err)
	}

	mock.ExpectQuery(`SELECT .* FROM invoices WHERE contract_id = \? AND billing_period_from = \? AND billing_period_to = \? ORDER BY issue_date ASC`).
		WithArgs("c-1", start, end).
		WillReturnRows(invoiceFindRow(t))

	got, err := repo.FindByContractAndPeriod(context.Background(), "c-1", period)
	if err != nil {
		t.Fatalf("FindByContractAndPeriod: %v", err)
	}
	if len(got) != 1 || got[0].ToSnapshot().ID != "inv-1" {
		t.Fatalf("unexpected result: %+v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
