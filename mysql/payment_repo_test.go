package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/contract-to-cash/core/application/tx"
	"github.com/contract-to-cash/core/domain/payment"
	"github.com/contract-to-cash/core/domain/shared"
	driver "github.com/go-sql-driver/mysql"
)

func newPaymentRepo(t *testing.T) (*MySQLPaymentRepository, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewPaymentRepository(db), mock
}

func samplePayment(t *testing.T) *payment.Payment {
	t.Helper()
	p, err := payment.FromSnapshot(payment.PaymentSnapshot{
		ID:                   "pay-1",
		InvoiceID:            "inv-1",
		Amount:               jpy(1100),
		RefundedAmount:       jpy(0),
		Method:               payment.PaymentMethodCreditCard,
		Status:               payment.PaymentStatusCompleted,
		GatewayTransactionID: "txn-1",
		IdempotencyKey:       "idem-pay-1",
		ProcessedAt:          fixedTime,
	})
	if err != nil {
		t.Fatalf("FromSnapshot: %v", err)
	}
	return p
}

// paymentColumns is the SELECT column set (order matters for sqlmock rows).
var paymentColumns = []string{
	"id", "invoice_id", "idempotency_key", "amount", "refunded_amount", "currency",
	"status", "method", "gateway_transaction_id", "failure_reason", "processed_at", "metadata", "state", "lock_version",
}

func paymentFindRow() *sqlmock.Rows {
	return sqlmock.NewRows(paymentColumns).AddRow(
		"pay-1", "inv-1", "idem-pay-1", int64(1100), int64(0), "JPY",
		"completed", "credit_card", "txn-1", "", fixedTime, []byte(`{}`), nil, int64(0),
	)
}

// A new payment is a plain INSERT (no ON DUPLICATE KEY UPDATE, so an
// idempotency_key collision can no longer silently overwrite the winner —
// issue #35).
func TestPaymentRepo_Save_InsertNew(t *testing.T) {
	repo, mock := newPaymentRepo(t)
	p := samplePayment(t)

	idem := "idem-pay-1"
	mock.ExpectExec(`INSERT INTO payments`).
		WithArgs("pay-1", "inv-1", &idem, int64(1100), int64(0),
			"JPY", "completed", "credit_card", "txn-1", "", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), int64(0)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Save(context.Background(), p); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// A same-id re-save collides on the PRIMARY key; Save must fall through to a
// version-guarded UPDATE keyed on id AND lock_version (the 3DS Pending ->
// Completed upgrade path). The trailing args are the new lock_version (write)
// and the LoadedVersion() guard (core#190).
func TestPaymentRepo_Save_UpdateExisting(t *testing.T) {
	repo, mock := newPaymentRepo(t)
	p := samplePayment(t)

	idem := "idem-pay-1"
	mock.ExpectExec(`INSERT INTO payments`).
		WithArgs("pay-1", "inv-1", &idem, int64(1100), int64(0),
			"JPY", "completed", "credit_card", "txn-1", "", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), int64(0)).
		WillReturnError(&driver.MySQLError{Number: 1062, Message: "Duplicate entry 'pay-1' for key 'payments.PRIMARY'"})
	mock.ExpectExec(`UPDATE payments SET`).
		WithArgs("inv-1", &idem, int64(1100), int64(0), "JPY", "completed", "credit_card",
			"txn-1", "", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), int64(0), "pay-1", int64(0)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.Save(context.Background(), p); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// A same-id re-save whose version-guarded UPDATE matches zero rows means a
// concurrent writer already advanced lock_version: Save must report
// tx.ErrVersionConflict rather than silently succeeding. This is what stops two
// concurrent RecordRefund calls from both booking a refund (core#190).
func TestPaymentRepo_Save_VersionConflict(t *testing.T) {
	repo, mock := newPaymentRepo(t)
	p := samplePayment(t)

	idem := "idem-pay-1"
	mock.ExpectExec(`INSERT INTO payments`).
		WithArgs("pay-1", "inv-1", &idem, int64(1100), int64(0),
			"JPY", "completed", "credit_card", "txn-1", "", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), int64(0)).
		WillReturnError(&driver.MySQLError{Number: 1062, Message: "Duplicate entry 'pay-1' for key 'payments.PRIMARY'"})
	mock.ExpectExec(`UPDATE payments SET`).
		WithArgs("inv-1", &idem, int64(1100), int64(0), "JPY", "completed", "credit_card",
								"txn-1", "", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), int64(0), "pay-1", int64(0)).
		WillReturnResult(sqlmock.NewResult(0, 0)) // zero rows changed: version guard missed

	if err := repo.Save(context.Background(), p); !errors.Is(err, tx.ErrVersionConflict) {
		t.Fatalf("expected tx.ErrVersionConflict, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// A duplicate on the idempotency_key UNIQUE index (a DIFFERENT payment already
// owns the key) must translate to the core sentinel so PaymentService converges
// on the winner rather than firing saga compensation.
func TestPaymentRepo_Save_DuplicateIdempotencyKey(t *testing.T) {
	repo, mock := newPaymentRepo(t)
	p := samplePayment(t)

	idem := "idem-pay-1"
	mock.ExpectExec(`INSERT INTO payments`).
		WithArgs("pay-1", "inv-1", &idem, int64(1100), int64(0),
			"JPY", "completed", "credit_card", "txn-1", "", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), int64(0)).
		WillReturnError(&driver.MySQLError{Number: 1062, Message: "Duplicate entry 'idem-pay-1' for key 'payments.idempotency_key'"})
	// Best-effort lookup of the winner for operational debugging.
	mock.ExpectQuery(`SELECT id FROM payments WHERE idempotency_key = \?`).
		WithArgs("idem-pay-1").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("pay-winner"))

	err := repo.Save(context.Background(), p)
	if !errors.Is(err, payment.ErrDuplicateIdempotencyKey) {
		t.Fatalf("expected ErrDuplicateIdempotencyKey, got %v", err)
	}
	var dup *payment.DuplicateIdempotencyKeyError
	if !errors.As(err, &dup) {
		t.Fatalf("expected *DuplicateIdempotencyKeyError, got %v", err)
	}
	if dup.Key != "idem-pay-1" || dup.AttemptedID != "pay-1" || dup.ExistingID != "pay-winner" {
		t.Errorf("unexpected dup error: %+v", dup)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestPaymentRepo_FindByID_Found(t *testing.T) {
	repo, mock := newPaymentRepo(t)
	mock.ExpectQuery(`SELECT .* FROM payments WHERE id = \?`).
		WithArgs("pay-1").
		WillReturnRows(paymentFindRow())

	got, err := repo.FindByID(context.Background(), "pay-1")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	s := got.ToSnapshot()
	if s.ID != "pay-1" || s.Amount.Int64() != 1100 {
		t.Errorf("unexpected payment: %+v", s)
	}
	if s.IdempotencyKey != "idem-pay-1" {
		t.Errorf("idempotency key = %q", s.IdempotencyKey)
	}
}

func TestPaymentRepo_FindByID_NotFound(t *testing.T) {
	repo, mock := newPaymentRepo(t)
	mock.ExpectQuery(`SELECT .* FROM payments WHERE id = \?`).
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)

	_, err := repo.FindByID(context.Background(), "missing")
	var de *shared.DomainError
	if !errors.As(err, &de) || de.Code != shared.ErrCodeNotFound {
		t.Fatalf("expected not_found DomainError, got %v", err)
	}
}

// FindByIdempotencyKey returns (nil, nil) on miss per the repository contract.
func TestPaymentRepo_FindByIdempotencyKey_NotFoundReturnsNil(t *testing.T) {
	repo, mock := newPaymentRepo(t)
	mock.ExpectQuery(`SELECT .* FROM payments WHERE idempotency_key = \?`).
		WithArgs("nope").
		WillReturnError(sql.ErrNoRows)

	got, err := repo.FindByIdempotencyKey(context.Background(), "nope")
	if err != nil {
		t.Fatalf("FindByIdempotencyKey: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil payment, got %+v", got)
	}
}

func TestPaymentRepo_FindByInvoiceID(t *testing.T) {
	repo, mock := newPaymentRepo(t)
	mock.ExpectQuery(`SELECT .* FROM payments WHERE invoice_id = \? ORDER BY created_at DESC`).
		WithArgs("inv-1").
		WillReturnRows(paymentFindRow())

	got, err := repo.FindByInvoiceID(context.Background(), "inv-1")
	if err != nil {
		t.Fatalf("FindByInvoiceID: %v", err)
	}
	if len(got) != 1 || got[0].ToSnapshot().ID != "pay-1" {
		t.Fatalf("unexpected result: %+v", got)
	}
}
