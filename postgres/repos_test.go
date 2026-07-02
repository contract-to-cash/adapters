package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/contract-to-cash/adapters/postgres"
	"github.com/contract-to-cash/adapters/postgres/postgrestest"
	"github.com/contract-to-cash/core/application/tx"
	"github.com/contract-to-cash/core/domain/balance"
	"github.com/contract-to-cash/core/domain/contract"
	"github.com/contract-to-cash/core/domain/invoice"
	"github.com/contract-to-cash/core/domain/payment"
	"github.com/contract-to-cash/core/domain/pricing"
	"github.com/contract-to-cash/core/domain/product"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/contract-to-cash/core/domain/usage"
	"github.com/contract-to-cash/core/eventstore"
)

func jpy(amount int64) shared.Money {
	return shared.NewMoney(new(big.Rat).SetInt64(amount), "JPY")
}

// newDraftContract creates an aggregate with a valid ContractCreated event.
func newDraftContract(t *testing.T, id shared.ContractID, accountID shared.AccountID, priceID shared.PriceID, clock shared.Clock) *contract.ContractAggregate {
	t.Helper()
	agg := contract.NewContractAggregate(id, clock)
	err := agg.Create(contract.CreateContractCommand{
		IdempotencyKey: "idem-" + string(id),
		AccountID:      accountID,
		PriceID:        priceID,
		ContractType:   contract.ContractTypeSubscription,
		Interval:       pricing.Monthly(),
		Price:          jpy(1000),
	}, eventstore.EventMetadata{UserID: "test-user"})
	if err != nil {
		t.Fatalf("Create contract %s: %v", id, err)
	}
	return agg
}

// --- Contract Repository ---

func TestContractRepo_SaveAndFindByID(t *testing.T) {
	pool := postgrestest.NewPool(t)
	es := postgres.NewEventStore(pool)
	clock := shared.FixedClock{FixedTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)}
	repo := postgres.NewContractRepository(pool, es, clock)
	ctx := context.Background()

	agg := newDraftContract(t, "c-1", "acc-1", "price-1", clock)

	if err := repo.Save(ctx, agg); err != nil {
		t.Fatalf("Save: %v", err)
	}

	found, err := repo.FindByID(ctx, "c-1")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if found.ID() != "c-1" {
		t.Errorf("ID = %q, want c-1", found.ID())
	}
	if found.Status() != contract.ContractStatusDraft {
		t.Errorf("Status = %q, want draft", found.Status())
	}
}

func TestContractRepo_FindByID_NotFound(t *testing.T) {
	pool := postgrestest.NewPool(t)
	es := postgres.NewEventStore(pool)
	clock := shared.FixedClock{FixedTime: time.Now()}
	repo := postgres.NewContractRepository(pool, es, clock)
	ctx := context.Background()

	_, err := repo.FindByID(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error")
	}
	if !isDomainError(err, shared.ErrCodeNotFound) {
		t.Errorf("expected not_found, got: %v", err)
	}
}

func TestContractRepo_FindByAccountID(t *testing.T) {
	pool := postgrestest.NewPool(t)
	es := postgres.NewEventStore(pool)
	cp := postgres.NewCheckpointStore(pool)
	clock := shared.FixedClock{FixedTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)}
	repo := postgres.NewContractRepository(pool, es, clock)
	proj := postgres.NewContractProjector(pool, es, cp)
	ctx := context.Background()

	// Create and project a contract.
	agg := newDraftContract(t, "c-2", "acc-2", "price-2", clock)
	if err := repo.Save(ctx, agg); err != nil {
		t.Fatal(err)
	}

	// Project the events to populate the read model.
	events, _ := es.Load(ctx, "c-2")
	for _, e := range events {
		if err := proj.Project(ctx, e); err != nil {
			t.Fatal(err)
		}
	}

	found, err := repo.FindByAccountID(ctx, "acc-2")
	if err != nil {
		t.Fatalf("FindByAccountID: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("got %d contracts, want 1", len(found))
	}
	if found[0].ID() != "c-2" {
		t.Errorf("ID = %q, want c-2", found[0].ID())
	}
}

// --- Invoice Repository ---

func TestInvoiceRepo_SaveAndFindByID(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()

	// Insert a contract read model row so the FK is satisfied.
	_, err := pool.Exec(ctx,
		`INSERT INTO contract_read_models (id, account_id, status) VALUES ('c-inv', 'acc-inv', 'active')`)
	if err != nil {
		t.Fatal(err)
	}

	repo := postgres.NewInvoiceRepository(pool)

	snap := invoice.InvoiceSnapshot{
		ID: "inv-1", InvoiceNumber: "INV-001",
		AccountID: "acc-inv", ContractID: "c-inv",
		Status:   invoice.InvoiceStatusDraft,
		Subtotal: jpy(10000), TaxAmount: jpy(1000),
		DiscountAmount: jpy(0), Total: jpy(11000),
		AppliedBalance: jpy(0), AmountDue: jpy(11000),
		PaidAmount: jpy(0), Balance: jpy(0),
	}
	inv, err := invoice.InvoiceFromSnapshot(snap)
	if err != nil {
		t.Fatalf("InvoiceFromSnapshot: %v", err)
	}

	if err := repo.Save(ctx, inv); err != nil {
		t.Fatalf("Save: %v", err)
	}

	found, err := repo.FindByID(ctx, "inv-1")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	fs := found.ToSnapshot()
	if fs.InvoiceNumber != "INV-001" {
		t.Errorf("InvoiceNumber = %q, want INV-001", fs.InvoiceNumber)
	}
	if fs.Total.Int64() != 11000 {
		t.Errorf("Total = %d, want 11000", fs.Total.Int64())
	}
}

func TestInvoiceRepo_FindByID_NotFound(t *testing.T) {
	pool := postgrestest.NewPool(t)
	repo := postgres.NewInvoiceRepository(pool)
	_, err := repo.FindByID(context.Background(), "nonexistent")
	if !isDomainError(err, shared.ErrCodeNotFound) {
		t.Errorf("expected not_found, got: %v", err)
	}
}

func TestInvoiceRepo_FindByStatus(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()
	_, _ = pool.Exec(ctx, `INSERT INTO contract_read_models (id, account_id, status) VALUES ('c-st', 'acc-st', 'active')`)

	repo := postgres.NewInvoiceRepository(pool)
	for i, status := range []invoice.InvoiceStatus{invoice.InvoiceStatusDraft, invoice.InvoiceStatusDraft, invoice.InvoiceStatusIssued} {
		snap := invoice.InvoiceSnapshot{
			ID:        shared.InvoiceID(fmt.Sprintf("inv-%s-%d", status, i)),
			AccountID: "acc-st", ContractID: "c-st", Status: status,
			Subtotal: jpy(0), TaxAmount: jpy(0), DiscountAmount: jpy(0),
			Total: jpy(0), AppliedBalance: jpy(0), AmountDue: jpy(0),
			PaidAmount: jpy(0), Balance: jpy(0),
		}
		inv, err := invoice.InvoiceFromSnapshot(snap)
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Save(ctx, inv); err != nil {
			t.Fatal(err)
		}
	}

	drafts, err := repo.FindByStatus(ctx, invoice.InvoiceStatusDraft)
	if err != nil {
		t.Fatal(err)
	}
	if len(drafts) != 2 {
		t.Errorf("got %d drafts, want 2", len(drafts))
	}
}

// --- Payment Repository ---

func TestPaymentRepo_SaveAndFindByID(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()
	_, _ = pool.Exec(ctx, `INSERT INTO contract_read_models (id, account_id, status) VALUES ('c-pay', 'acc-pay', 'active')`)
	_, _ = pool.Exec(ctx, `INSERT INTO invoices (id, account_id, contract_id, status, subtotal, tax_amount, discount_amount, total, applied_balance, amount_due, paid_amount, balance) VALUES ('inv-pay', 'acc-pay', 'c-pay', 'issued', 0, 0, 0, 0, 0, 0, 0, 0)`)

	repo := postgres.NewPaymentRepository(pool)
	snap := payment.PaymentSnapshot{
		ID: "pay-1", InvoiceID: "inv-pay",
		Amount: jpy(5000), RefundedAmount: jpy(0),
		Method: payment.PaymentMethodCreditCard, Status: payment.PaymentStatusCompleted,
		IdempotencyKey: "idem-1", ProcessedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
	p, err := payment.FromSnapshot(snap)
	if err != nil {
		t.Fatalf("FromSnapshot: %v", err)
	}
	if err := repo.Save(ctx, p); err != nil {
		t.Fatalf("Save: %v", err)
	}

	found, err := repo.FindByID(ctx, "pay-1")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if found.ToSnapshot().Amount.Int64() != 5000 {
		t.Errorf("Amount = %d, want 5000", found.ToSnapshot().Amount.Int64())
	}
}

func TestPaymentRepo_FindByIdempotencyKey(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()
	_, _ = pool.Exec(ctx, `INSERT INTO contract_read_models (id, account_id, status) VALUES ('c-idem', 'acc-idem', 'active')`)
	_, _ = pool.Exec(ctx, `INSERT INTO invoices (id, account_id, contract_id, status, subtotal, tax_amount, discount_amount, total, applied_balance, amount_due, paid_amount, balance) VALUES ('inv-idem', 'acc-idem', 'c-idem', 'issued', 0, 0, 0, 0, 0, 0, 0, 0)`)

	repo := postgres.NewPaymentRepository(pool)
	snap := payment.PaymentSnapshot{
		ID: "pay-idem", InvoiceID: "inv-idem",
		Amount: jpy(1000), RefundedAmount: jpy(0),
		Method: payment.PaymentMethodCreditCard, Status: payment.PaymentStatusCompleted,
		IdempotencyKey: "unique-key-1", ProcessedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
	p, err := payment.FromSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, p); err != nil {
		t.Fatal(err)
	}

	found, err := repo.FindByIdempotencyKey(ctx, "unique-key-1")
	if err != nil {
		t.Fatal(err)
	}
	if found == nil || found.ToSnapshot().ID != "pay-idem" {
		t.Errorf("expected pay-idem, got %v", found)
	}

	// Non-existent key returns nil, nil.
	notFound, err := repo.FindByIdempotencyKey(ctx, "missing")
	if err != nil {
		t.Fatal(err)
	}
	if notFound != nil {
		t.Error("expected nil for missing key")
	}
}

// --- Balance Repository ---

func TestBalanceRepo_SaveAndFindByID(t *testing.T) {
	pool := postgrestest.NewPool(t)
	repo := postgres.NewBalanceRepository(pool)
	ctx := context.Background()

	// Version 0 means the entry has never been persisted: FromSnapshot sets
	// loadedVersion = Version = 0, which routes Save through the INSERT path.
	snap := balance.BalanceEntrySnapshot{
		ID: "be-1", AccountID: "acc-bal",
		OriginalAmount: jpy(10000), RemainingAmount: jpy(10000),
		Reason: balance.BalanceReasonManualAdjustment, SourceType: balance.BalanceSourceTypeManual,
		SourceID: "manual-1", Description: "test credit",
		Version: 0, CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
	entry, err := balance.FromSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, entry); err != nil {
		t.Fatalf("Save: %v", err)
	}

	found, err := repo.FindByID(ctx, "be-1")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if found.ToSnapshot().OriginalAmount.Int64() != 10000 {
		t.Errorf("OriginalAmount = %d, want 10000", found.ToSnapshot().OriginalAmount.Int64())
	}
}

func TestBalanceRepo_OptimisticLock(t *testing.T) {
	pool := postgrestest.NewPool(t)
	repo := postgres.NewBalanceRepository(pool)
	ctx := context.Background()

	// Insert with version=1.
	_, _ = pool.Exec(ctx,
		`INSERT INTO balance_entries (id, account_id, original_amount, remaining_amount, currency,
			reason, source_type, source_id, description, version, created_at)
		 VALUES ('be-lock', 'acc-lock', 10000, 10000, 'JPY', 'credit_grant', 'manual', 'm1', 'test', 1, NOW())`)

	entry1, _ := repo.FindByID(ctx, "be-lock")
	entry2, _ := repo.FindByID(ctx, "be-lock")

	// Simulate a version bump on the first entry by directly updating the DB.
	// This mimics another process having saved the entry, advancing version to 2.
	_, _ = pool.Exec(ctx, `UPDATE balance_entries SET version = 2 WHERE id = 'be-lock'`)

	// entry1 loaded with version=1, but DB is now at version=2.
	// Save should fail with ErrVersionConflict because WHERE version=1 matches 0 rows.
	err := repo.Save(ctx, entry1)
	if !errors.Is(err, tx.ErrVersionConflict) {
		t.Errorf("expected ErrVersionConflict for entry1, got: %v", err)
	}

	// entry2 also loaded with version=1, same conflict.
	err = repo.Save(ctx, entry2)
	if !errors.Is(err, tx.ErrVersionConflict) {
		t.Errorf("expected ErrVersionConflict for entry2, got: %v", err)
	}
}

func TestBalanceRepo_GetBalance(t *testing.T) {
	pool := postgrestest.NewPool(t)
	repo := postgres.NewBalanceRepository(pool)
	ctx := context.Background()

	_, _ = pool.Exec(ctx,
		`INSERT INTO balance_entries (id, account_id, original_amount, remaining_amount, currency, reason, source_type, source_id, description, version, created_at)
		 VALUES ('be-b1', 'acc-gb', 5000, 3000, 'JPY', 'credit_grant', 'manual', 'm1', '', 1, NOW()),
		        ('be-b2', 'acc-gb', 2000, 2000, 'JPY', 'overpayment', 'invoice', 'inv1', '', 1, NOW())`)

	total, err := repo.GetBalance(ctx, "acc-gb", "JPY")
	if err != nil {
		t.Fatal(err)
	}
	if total.Int64() != 5000 {
		t.Errorf("balance = %d, want 5000", total.Int64())
	}
}

// --- Usage Repository ---

func TestUsageRepo_RecordAndGetSummary(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()
	_, _ = pool.Exec(ctx, `INSERT INTO contract_read_models (id, account_id, status) VALUES ('c-usage', 'acc-usage', 'active')`)

	repo := postgres.NewUsageRepository(pool)
	now := time.Now().UTC().Truncate(time.Microsecond)

	rec, err := usage.NewUsageRecord("ur-1", "c-usage", "api_calls", 100, now, "idem-ur-1")
	if err != nil {
		t.Fatalf("NewUsageRecord: %v", err)
	}
	if err := repo.Record(ctx, rec); err != nil {
		t.Fatalf("Record: %v", err)
	}

	// Idempotent: recording again should not error.
	if err := repo.Record(ctx, rec); err != nil {
		t.Fatalf("duplicate Record: %v", err)
	}

	period, err := shared.NewDateRange(now.Add(-time.Hour), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	summary, err := repo.GetSummary(ctx, "c-usage", "api_calls", period)
	if err != nil {
		t.Fatal(err)
	}
	if summary.TotalUsage != 100 {
		t.Errorf("TotalUsage = %d, want 100", summary.TotalUsage)
	}
}

// --- Product Repository ---

func TestProductRepo_SaveAndFindByID(t *testing.T) {
	pool := postgrestest.NewPool(t)
	repo := postgres.NewProductRepository(pool)
	ctx := context.Background()

	snap := product.ProductSnapshot{
		ID: "prod-1", Name: "Test Product", Description: "A test",
		Status:    product.ProductStatusActive,
		CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
		Features:  []product.Feature{{Name: "feature-1", Included: true}},
	}
	prod, err := product.FromSnapshot(snap)
	if err != nil {
		t.Fatalf("FromSnapshot: %v", err)
	}
	if err := repo.Save(ctx, prod); err != nil {
		t.Fatalf("Save: %v", err)
	}

	found, err := repo.FindByID(ctx, "prod-1")
	if err != nil {
		t.Fatal(err)
	}
	if found.ToSnapshot().Name != "Test Product" {
		t.Errorf("Name = %q, want Test Product", found.ToSnapshot().Name)
	}
}

// --- Price Repository ---

func TestPriceRepo_SaveAndFindByID(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()

	// Product must exist for FK.
	prodRepo := postgres.NewProductRepository(pool)
	prod, err := product.FromSnapshot(product.ProductSnapshot{
		ID: "prod-price", Name: "Product", Status: product.ProductStatusActive,
		CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := prodRepo.Save(ctx, prod); err != nil {
		t.Fatal(err)
	}

	repo := postgres.NewPriceRepository(pool)
	snap := pricing.PriceSnapshot{
		ID: "price-1", ProductID: "prod-price",
		Amount: jpy(1000), Currency: "JPY",
		Status:       pricing.PriceStatusActive,
		PricingModel: pricing.FlatPrice{Price: jpy(1000)},
		CreatedAt:    time.Now().UTC().Truncate(time.Microsecond),
	}
	price, err := pricing.FromSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, price); err != nil {
		t.Fatalf("Save: %v", err)
	}

	found, err := repo.FindByID(ctx, "price-1")
	if err != nil {
		t.Fatal(err)
	}
	fs := found.ToSnapshot()
	if fs.Amount.Int64() != 1000 {
		t.Errorf("Amount = %d, want 1000", fs.Amount.Int64())
	}
	if fs.PricingModel == nil {
		t.Error("PricingModel is nil")
	} else if _, ok := fs.PricingModel.(pricing.FlatPrice); !ok {
		t.Errorf("PricingModel type = %T, want pricing.FlatPrice", fs.PricingModel)
	}
}

func TestPriceRepo_FindActiveByProductID(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()

	prodRepo := postgres.NewProductRepository(pool)
	prod, err := product.FromSnapshot(product.ProductSnapshot{
		ID: "prod-active", Name: "P", Status: product.ProductStatusActive,
		CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := prodRepo.Save(ctx, prod); err != nil {
		t.Fatal(err)
	}

	repo := postgres.NewPriceRepository(pool)
	for i, s := range []pricing.PriceStatus{pricing.PriceStatusActive, pricing.PriceStatusActive, pricing.PriceStatusArchived} {
		snap := pricing.PriceSnapshot{
			ID:        shared.PriceID(fmt.Sprintf("pr-%s-%d", s, i)),
			ProductID: "prod-active", Amount: jpy(500), Currency: "JPY",
			Status: s, CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
		}
		price, err := pricing.FromSnapshot(snap)
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Save(ctx, price); err != nil {
			t.Fatal(err)
		}
	}

	active, err := repo.FindActiveByProductID(ctx, "prod-active")
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 2 {
		t.Errorf("got %d active prices, want 2", len(active))
	}
}

// --- Contract Projector ---

func TestContractProjector_Rebuild(t *testing.T) {
	pool := postgrestest.NewPool(t)
	es := postgres.NewEventStore(pool)
	cp := postgres.NewCheckpointStore(pool)
	clock := shared.FixedClock{FixedTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)}
	repo := postgres.NewContractRepository(pool, es, clock)
	proj := postgres.NewContractProjector(pool, es, cp)
	ctx := context.Background()

	// Create two contracts.
	for _, id := range []string{"c-r1", "c-r2"} {
		agg := newDraftContract(t, shared.ContractID(id), shared.AccountID("acc-"+id), shared.PriceID("p-"+id), clock)
		if err := repo.Save(ctx, agg); err != nil {
			t.Fatal(err)
		}
	}

	// Rebuild should populate the read model.
	if err := proj.Rebuild(ctx, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}

	// Verify read model has both contracts.
	found, err := repo.FindByAccountID(ctx, "acc-c-r1")
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 {
		t.Errorf("got %d contracts for acc-c-r1, want 1", len(found))
	}

	// Verify checkpoint was saved.
	pos, err := cp.Load(ctx, postgres.ContractProjectorName)
	if err != nil {
		t.Fatal(err)
	}
	if pos == 0 {
		t.Error("checkpoint should be > 0 after rebuild")
	}
}
