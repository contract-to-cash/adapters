package postgres_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/contract-to-cash/adapters/postgres"
	"github.com/contract-to-cash/adapters/postgres/postgrestest"
	"github.com/contract-to-cash/core/domain/balance"
	"github.com/contract-to-cash/core/domain/payment"
	"github.com/contract-to-cash/core/domain/pricing"
	"github.com/contract-to-cash/core/domain/product"
	"github.com/contract-to-cash/core/domain/shared"
)

// jpyRat builds a JPY Money from an exact rational (num/denom), e.g. ¥100.75.
func jpyRat(num, denom int64) shared.Money {
	return shared.NewMoney(big.NewRat(num, denom), shared.Currency("JPY"))
}

// Issue #11: fractional Money (e.g. proration credits like ¥100.75) must
// survive a save -> reload round trip without being truncated to ¥100. Before
// the `state` JSON column existed, these repositories persisted Money via the
// lossy BIGINT column (Money.Int64) and silently dropped the fraction.

func TestBalanceRepo_MoneyRoundTrip_Fraction(t *testing.T) {
	pool := postgrestest.NewPool(t)
	repo := postgres.NewBalanceRepository(pool)
	ctx := context.Background()

	frac := jpyRat(10075, 100) // ¥100.75
	snap := balance.BalanceEntrySnapshot{
		ID: "be-frac", AccountID: "acc-frac",
		OriginalAmount: frac, RemainingAmount: frac,
		Reason: balance.BalanceReasonProration, SourceType: balance.BalanceSourceTypeManual,
		SourceID: "prorate-1", Version: 0,
		CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
	entry, err := balance.FromSnapshot(snap)
	if err != nil {
		t.Fatalf("FromSnapshot: %v", err)
	}
	if err := repo.Save(ctx, entry); err != nil {
		t.Fatalf("Save: %v", err)
	}

	found, err := repo.FindByID(ctx, "be-frac")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if found.RemainingAmount().Amount().Cmp(big.NewRat(10075, 100)) != 0 {
		t.Errorf("RemainingAmount = %s, want 403/4 (¥100.75)", found.RemainingAmount().Amount().RatString())
	}
	if found.OriginalAmount().Amount().Cmp(big.NewRat(10075, 100)) != 0 {
		t.Errorf("OriginalAmount = %s, want 403/4 (¥100.75)", found.OriginalAmount().Amount().RatString())
	}
}

// GetBalance must sum precise remaining amounts: two ¥100.75 credits => ¥201.50.
func TestBalanceRepo_GetBalance_Fraction(t *testing.T) {
	pool := postgrestest.NewPool(t)
	repo := postgres.NewBalanceRepository(pool)
	ctx := context.Background()

	for _, id := range []string{"be-gb1", "be-gb2"} {
		snap := balance.BalanceEntrySnapshot{
			ID: shared.BalanceEntryID(id), AccountID: "acc-gbf",
			OriginalAmount: jpyRat(10075, 100), RemainingAmount: jpyRat(10075, 100),
			Reason: balance.BalanceReasonProration, SourceType: balance.BalanceSourceTypeManual,
			Version: 0, CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
		}
		entry, err := balance.FromSnapshot(snap)
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Save(ctx, entry); err != nil {
			t.Fatal(err)
		}
	}

	total, err := repo.GetBalance(ctx, "acc-gbf", "JPY")
	if err != nil {
		t.Fatal(err)
	}
	if total.Amount().Cmp(big.NewRat(20150, 100)) != 0 {
		t.Errorf("GetBalance = %s, want 403/2 (¥201.50)", total.Amount().RatString())
	}
}

// A sub-unit remainder (¥0.75) truncates to 0 in the BIGINT column; the old
// `remaining_amount > 0` predicate would have wrongly excluded it. FindAvailable
// must still return it.
func TestBalanceRepo_FindAvailable_SubUnitFraction(t *testing.T) {
	pool := postgrestest.NewPool(t)
	repo := postgres.NewBalanceRepository(pool)
	ctx := context.Background()

	snap := balance.BalanceEntrySnapshot{
		ID: "be-sub", AccountID: "acc-sub",
		OriginalAmount: jpyRat(10075, 100), RemainingAmount: jpyRat(75, 100), // ¥0.75 left
		Reason: balance.BalanceReasonProration, SourceType: balance.BalanceSourceTypeManual,
		Version: 0, CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
	entry, err := balance.FromSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, entry); err != nil {
		t.Fatal(err)
	}

	available, err := repo.FindAvailable(ctx, "acc-sub", "JPY")
	if err != nil {
		t.Fatal(err)
	}
	if len(available) != 1 {
		t.Fatalf("FindAvailable returned %d entries, want 1 (sub-unit credit must not be truncated away)", len(available))
	}
	if available[0].RemainingAmount().Amount().Cmp(big.NewRat(75, 100)) != 0 {
		t.Errorf("RemainingAmount = %s, want 3/4 (¥0.75)", available[0].RemainingAmount().Amount().RatString())
	}

	total, err := repo.GetBalance(ctx, "acc-sub", "JPY")
	if err != nil {
		t.Fatal(err)
	}
	if total.Amount().Cmp(big.NewRat(75, 100)) != 0 {
		t.Errorf("GetBalance = %s, want 3/4 (¥0.75)", total.Amount().RatString())
	}
}

func TestPaymentRepo_MoneyRoundTrip_Fraction(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()
	_, _ = pool.Exec(ctx, `INSERT INTO contract_read_models (id, account_id, status) VALUES ('c-payf', 'acc-payf', 'active')`)
	_, _ = pool.Exec(ctx, `INSERT INTO invoices (id, account_id, contract_id, status, subtotal, tax_amount, discount_amount, total, applied_balance, amount_due, paid_amount, balance) VALUES ('inv-payf', 'acc-payf', 'c-payf', 'issued', 0, 0, 0, 0, 0, 0, 0, 0)`)

	repo := postgres.NewPaymentRepository(pool)
	snap := payment.PaymentSnapshot{
		ID: "pay-frac", InvoiceID: "inv-payf",
		Amount: jpyRat(10075, 100), RefundedAmount: jpyRat(2550, 100), // ¥100.75 / ¥25.50
		Method: payment.PaymentMethodCreditCard, Status: payment.PaymentStatusCompleted,
		IdempotencyKey: "idem-frac", ProcessedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
	p, err := payment.FromSnapshot(snap)
	if err != nil {
		t.Fatalf("FromSnapshot: %v", err)
	}
	if err := repo.Save(ctx, p); err != nil {
		t.Fatalf("Save: %v", err)
	}

	found, err := repo.FindByID(ctx, "pay-frac")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	fs := found.ToSnapshot()
	if fs.Amount.Amount().Cmp(big.NewRat(10075, 100)) != 0 {
		t.Errorf("Amount = %s, want 403/4 (¥100.75)", fs.Amount.Amount().RatString())
	}
	if fs.RefundedAmount.Amount().Cmp(big.NewRat(2550, 100)) != 0 {
		t.Errorf("RefundedAmount = %s, want 51/2 (¥25.50)", fs.RefundedAmount.Amount().RatString())
	}
}

func TestPriceRepo_MoneyRoundTrip_Fraction(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()

	prodRepo := postgres.NewProductRepository(pool)
	prod, err := product.FromSnapshot(product.ProductSnapshot{
		ID: "prod-pricef", Name: "P", Status: product.ProductStatusActive,
		CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := prodRepo.Save(ctx, prod); err != nil {
		t.Fatal(err)
	}

	repo := postgres.NewPriceRepository(pool)
	amount := jpyRat(10075, 100) // ¥100.75
	snap := pricing.PriceSnapshot{
		ID: "price-frac", ProductID: "prod-pricef",
		Amount: amount, Currency: "JPY",
		Status:       pricing.PriceStatusActive,
		PricingModel: pricing.FlatPrice{Price: amount},
		CreatedAt:    time.Now().UTC().Truncate(time.Microsecond),
	}
	price, err := pricing.FromSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, price); err != nil {
		t.Fatalf("Save: %v", err)
	}

	found, err := repo.FindByID(ctx, "price-frac")
	if err != nil {
		t.Fatal(err)
	}
	if found.ToSnapshot().Amount.Amount().Cmp(big.NewRat(10075, 100)) != 0 {
		t.Errorf("Amount = %s, want 403/4 (¥100.75)", found.ToSnapshot().Amount.Amount().RatString())
	}
}
