package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/contract-to-cash/core/domain/payment"
	"github.com/contract-to-cash/core/domain/shared"
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

func paymentFindRow() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "invoice_id", "idempotency_key", "amount", "refunded_amount", "currency",
		"status", "method", "gateway_transaction_id", "failure_reason", "processed_at", "metadata",
	}).AddRow(
		"pay-1", "inv-1", "idem-pay-1", int64(1100), int64(0), "JPY",
		"completed", "credit_card", "txn-1", "", fixedTime, []byte(`{}`),
	)
}

func TestPaymentRepo_Save_Upsert(t *testing.T) {
	repo, mock := newPaymentRepo(t)
	p := samplePayment(t)

	idem := "idem-pay-1"
	mock.ExpectExec(`INSERT INTO payments .* ON DUPLICATE KEY UPDATE`).
		WithArgs("pay-1", "inv-1", &idem, int64(1100), int64(0),
			"JPY", "completed", "credit_card", "txn-1", "", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Save(context.Background(), p); err != nil {
		t.Fatalf("Save: %v", err)
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
