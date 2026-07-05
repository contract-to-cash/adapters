package mysql

import (
	"context"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/contract-to-cash/core/domain/balance"
	"github.com/contract-to-cash/core/domain/payment"
	"github.com/contract-to-cash/core/domain/pricing"
	"github.com/contract-to-cash/core/domain/shared"
)

// jpyRat builds a JPY Money from an exact rational (num/denom), e.g. ¥100.75.
func jpyRat(num, denom int64) shared.Money {
	return shared.NewMoney(big.NewRat(num, denom), shared.Currency("JPY"))
}

// Issue #11: a Money with a fractional part (¥100.75) must survive a
// save -> reload round trip without being truncated to ¥100. The precise value
// travels through the `state` JSON column; the BIGINT column is written lossily
// (int64(100)) but is not read back.

func TestBalanceRepo_MoneyRoundTrip_Fraction(t *testing.T) {
	frac := jpyRat(10075, 100) // ¥100.75

	// Save writes the exact amount into the state JSON while the BIGINT
	// columns receive the truncated int64(100).
	wantState, err := json.Marshal(balanceEntryJSONState{OriginalAmount: frac, RemainingAmount: frac})
	if err != nil {
		t.Fatalf("marshal want state: %v", err)
	}

	entry, err := balance.FromSnapshot(balance.BalanceEntrySnapshot{
		ID: "bal-frac", AccountID: "acct-1",
		OriginalAmount: frac, RemainingAmount: frac,
		Reason:    balance.BalanceReason("proration"),
		CreatedAt: fixedTime, Version: 0,
	})
	if err != nil {
		t.Fatalf("FromSnapshot: %v", err)
	}

	repo, mock := newBalanceRepo(t)
	mock.ExpectExec(`INSERT INTO balance_entries`).
		WithArgs("bal-frac", "acct-1", int64(100), int64(100), "JPY",
			"proration", "", "", "", nil, 0, fixedTime, wantState).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := repo.Save(context.Background(), entry); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}

	// Reload: the state JSON restores the exact ¥100.75.
	row := sqlmock.NewRows([]string{
		"id", "account_id", "original_amount", "remaining_amount", "currency",
		"reason", "source_type", "source_id", "description", "expires_at", "version", "created_at", "state",
	}).AddRow(
		"bal-frac", "acct-1", int64(100), int64(100), "JPY",
		"proration", "", "", "", nil, 0, fixedTime, wantState,
	)
	mock.ExpectQuery(`SELECT .* FROM balance_entries WHERE id = \?`).
		WithArgs("bal-frac").WillReturnRows(row)

	got, err := repo.FindByID(context.Background(), "bal-frac")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if got.RemainingAmount().Amount().Cmp(big.NewRat(10075, 100)) != 0 {
		t.Errorf("RemainingAmount = %s, want 403/4 (¥100.75)", got.RemainingAmount().Amount().RatString())
	}
	if got.OriginalAmount().Amount().Cmp(big.NewRat(10075, 100)) != 0 {
		t.Errorf("OriginalAmount = %s, want 403/4 (¥100.75)", got.OriginalAmount().Amount().RatString())
	}
}

// GetBalance must sum the precise remaining amounts, not the truncated BIGINT
// column: two ¥100.75 credits total ¥201.50, not ¥200.
func TestBalanceRepo_GetBalance_Fraction(t *testing.T) {
	frac := jpyRat(10075, 100)
	state, err := json.Marshal(balanceEntryJSONState{OriginalAmount: frac, RemainingAmount: frac})
	if err != nil {
		t.Fatalf("marshal state: %v", err)
	}

	rows := sqlmock.NewRows([]string{
		"id", "account_id", "original_amount", "remaining_amount", "currency",
		"reason", "source_type", "source_id", "description", "expires_at", "version", "created_at", "state",
	}).
		AddRow("bal-1", "acct-1", int64(100), int64(100), "JPY", "proration", "", "", "", nil, 0, fixedTime, state).
		AddRow("bal-2", "acct-1", int64(100), int64(100), "JPY", "proration", "", "", "", nil, 0, fixedTime, state)

	repo, mock := newBalanceRepo(t)
	mock.ExpectQuery(`SELECT .* FROM balance_entries WHERE account_id = \? AND currency = \? AND \(expires_at IS NULL OR expires_at > NOW\(6\)\)`).
		WithArgs("acct-1", "JPY").WillReturnRows(rows)

	total, err := repo.GetBalance(context.Background(), "acct-1", "JPY")
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if total.Amount().Cmp(big.NewRat(20150, 100)) != 0 {
		t.Errorf("GetBalance = %s, want 403/2 (¥201.50)", total.Amount().RatString())
	}
}

func TestPaymentRepo_MoneyRoundTrip_Fraction(t *testing.T) {
	amount := jpyRat(10075, 100)  // ¥100.75
	refunded := jpyRat(2550, 100) // ¥25.50
	state, err := json.Marshal(paymentJSONState{Amount: amount, RefundedAmount: refunded})
	if err != nil {
		t.Fatalf("marshal state: %v", err)
	}

	row := sqlmock.NewRows(paymentColumns).AddRow(
		"pay-frac", "inv-1", "idem-frac", int64(100), int64(25), "JPY",
		"completed", "credit_card", "txn-1", "", fixedTime, []byte(`{}`), state,
	)
	repo, mock := newPaymentRepo(t)
	mock.ExpectQuery(`SELECT .* FROM payments WHERE id = \?`).
		WithArgs("pay-frac").WillReturnRows(row)

	got, err := repo.FindByID(context.Background(), "pay-frac")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	s := got.ToSnapshot()
	if s.Amount.Amount().Cmp(big.NewRat(10075, 100)) != 0 {
		t.Errorf("Amount = %s, want 403/4 (¥100.75)", s.Amount.Amount().RatString())
	}
	if s.RefundedAmount.Amount().Cmp(big.NewRat(2550, 100)) != 0 {
		t.Errorf("RefundedAmount = %s, want 51/2 (¥25.50)", s.RefundedAmount.Amount().RatString())
	}

	// Sanity: the same value marshaled by the repo on Save carries the exact
	// rational, proving the write side is not lossy either.
	p, err := payment.FromSnapshot(payment.PaymentSnapshot{
		ID: "pay-frac", InvoiceID: "inv-1",
		Amount: amount, RefundedAmount: refunded,
		Method: payment.PaymentMethodCreditCard, Status: payment.PaymentStatusCompleted,
		GatewayTransactionID: "txn-1",
		IdempotencyKey:       "idem-frac", ProcessedAt: fixedTime,
	})
	if err != nil {
		t.Fatalf("FromSnapshot: %v", err)
	}
	idem := "idem-frac"
	mock.ExpectExec(`INSERT INTO payments .* ON DUPLICATE KEY UPDATE`).
		WithArgs("pay-frac", "inv-1", &idem, int64(100), int64(25),
			"JPY", "completed", "credit_card", "txn-1", "", sqlmock.AnyArg(), sqlmock.AnyArg(), state).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := repo.Save(context.Background(), p); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPriceRepo_MoneyRoundTrip_Fraction(t *testing.T) {
	amount := jpyRat(10075, 100) // ¥100.75
	state, err := json.Marshal(priceJSONState{Amount: amount})
	if err != nil {
		t.Fatalf("marshal state: %v", err)
	}

	row := sqlmock.NewRows([]string{"id", "product_id", "amount", "currency", "interval_data", "pricing_model", "status", "created_at", "state"}).
		AddRow("price-frac", "prod-1", int64(100), "JPY",
			[]byte(`{}`), []byte(`{"kind":"flat","flat":{"Price":{"amount":"403/4","currency":"JPY"}}}`), "active", fixedTime, state)

	repo, mock := newPriceRepo(t)
	mock.ExpectQuery(`SELECT .* FROM prices WHERE id = \?`).
		WithArgs("price-frac").WillReturnRows(row)

	got, err := repo.FindByID(context.Background(), "price-frac")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	s := got.ToSnapshot()
	if s.Amount.Amount().Cmp(big.NewRat(10075, 100)) != 0 {
		t.Errorf("Amount = %s, want 403/4 (¥100.75)", s.Amount.Amount().RatString())
	}

	// Save writes the exact amount to the state column.
	p, err := pricing.FromSnapshot(pricing.PriceSnapshot{
		ID: "price-frac", ProductID: "prod-1",
		Amount: amount, Currency: "JPY",
		Status:       pricing.PriceStatusActive,
		PricingModel: pricing.FlatPrice{Price: amount},
		CreatedAt:    fixedTime,
	})
	if err != nil {
		t.Fatalf("FromSnapshot: %v", err)
	}
	mock.ExpectExec(`INSERT INTO prices .* ON DUPLICATE KEY UPDATE`).
		WithArgs("price-frac", "prod-1", int64(100), "JPY", "",
			sqlmock.AnyArg(), sqlmock.AnyArg(), "active", state, fixedTime).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := repo.Save(context.Background(), p); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
