package postgres_test

import (
	"context"
	"encoding/json"
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

// newDraftContractKey is like newDraftContract but lets the caller choose the
// idempotency key so two aggregates can deliberately collide.
func newDraftContractKey(t *testing.T, id shared.ContractID, accountID shared.AccountID, priceID shared.PriceID, key string, clock shared.Clock) *contract.ContractAggregate {
	t.Helper()
	agg := contract.NewContractAggregate(id, clock)
	err := agg.Create(contract.CreateContractCommand{
		IdempotencyKey: key,
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

// Issue #30: option-1 optimistic locking. Two loads observe the same version,
// both finalize, and race to Save. The first wins; the second is rejected with
// tx.ErrVersionConflict (not last-writer-wins), so FinalizeInvoice would retry
// the loser, re-read the finalized row, and be rejected by Finalize(). The
// winner's state is durably persisted and the version advances.
func TestInvoiceRepo_Save_OptimisticLockConflict(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx,
		`INSERT INTO contract_read_models (id, account_id, status) VALUES ('c-ol', 'acc-ol', 'active')`); err != nil {
		t.Fatal(err)
	}
	repo := postgres.NewInvoiceRepository(pool)

	base, err := invoice.InvoiceFromSnapshot(invoice.InvoiceSnapshot{
		ID: "inv-ol", InvoiceNumber: "INV-OL", AccountID: "acc-ol", ContractID: "c-ol",
		Status: invoice.InvoiceStatusDraft, Subtotal: jpy(1000), TaxAmount: jpy(0),
		DiscountAmount: jpy(0), Total: jpy(1000), AppliedBalance: jpy(0),
		AmountDue: jpy(1000), PaidAmount: jpy(0), Balance: jpy(1000),
	})
	if err != nil {
		t.Fatalf("build base: %v", err)
	}
	if err := repo.Save(ctx, base); err != nil {
		t.Fatalf("seed Save: %v", err)
	}

	a, err := repo.FindByID(ctx, "inv-ol")
	if err != nil {
		t.Fatalf("load a: %v", err)
	}
	b, err := repo.FindByID(ctx, "inv-ol")
	if err != nil {
		t.Fatalf("load b: %v", err)
	}
	if err := a.Finalize(); err != nil {
		t.Fatalf("finalize a: %v", err)
	}
	if err := b.Finalize(); err != nil {
		t.Fatalf("finalize b: %v", err)
	}

	if err := repo.Save(ctx, a); err != nil {
		t.Fatalf("first Save (winner): %v", err)
	}
	if err := repo.Save(ctx, b); !errors.Is(err, tx.ErrVersionConflict) {
		t.Fatalf("second Save: expected tx.ErrVersionConflict, got %v", err)
	}

	reloaded, err := repo.FindByID(ctx, "inv-ol")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if reloaded.Status() != invoice.InvoiceStatusFinalized {
		t.Errorf("status = %s, want finalized", reloaded.Status())
	}
	if reloaded.Version() != 1 {
		t.Errorf("persisted version = %d, want 1", reloaded.Version())
	}
	// The retry path (re-load the finalized row, mutate, Save) must still work,
	// proving the guard does not wedge a legitimate follow-up write. History
	// stays consistent: the as-of read at the reload returns the finalized state.
	if err := reloaded.VoidWithReason("superseded"); err != nil {
		t.Fatalf("void reloaded: %v", err)
	}
	if err := repo.Save(ctx, reloaded); err != nil {
		t.Fatalf("retry Save after reload: %v", err)
	}
	asOf, err := repo.FindByIDAsOf(ctx, "inv-ol", time.Now())
	if err != nil {
		t.Fatalf("FindByIDAsOf: %v", err)
	}
	if asOf.Status() != invoice.InvoiceStatusVoided {
		t.Errorf("as-of status = %s, want voided", asOf.Status())
	}
}

// Issue #45: the per-(contract_id, billing_period) partial unique index. A second
// DISTINCT non-voided, non-proration invoice for the same period is rejected with
// ErrCodeConflict, while proration invoices, zero-period invoices, and a
// void-and-recreate replacement all coexist (the exemptions in the Save godoc /
// inmemory reference).
func TestInvoiceRepo_Save_PeriodUniqueness(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx,
		`INSERT INTO contract_read_models (id, account_id, status) VALUES ('c-pu', 'acc-pu', 'active')`); err != nil {
		t.Fatal(err)
	}
	repo := postgres.NewInvoiceRepository(pool)

	period, err := shared.NewDateRange(
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("period: %v", err)
	}

	mk := func(id string, status invoice.InvoiceStatus, p shared.DateRange, meta map[string]string) *invoice.Invoice {
		t.Helper()
		inv, err := invoice.InvoiceFromSnapshot(invoice.InvoiceSnapshot{
			ID: shared.InvoiceID(id), AccountID: "acc-pu", ContractID: "c-pu",
			Status: status, Subtotal: jpy(1000), TaxAmount: jpy(0), DiscountAmount: jpy(0),
			Total: jpy(1000), AppliedBalance: jpy(0), AmountDue: jpy(1000),
			PaidAmount: jpy(0), Balance: jpy(1000), BillingPeriod: p, Metadata: meta,
		})
		if err != nil {
			t.Fatalf("build %s: %v", id, err)
		}
		return inv
	}

	if err := repo.Save(ctx, mk("inv-p1", invoice.InvoiceStatusFinalized, period, nil)); err != nil {
		t.Fatalf("first period invoice: %v", err)
	}
	// Second distinct regular invoice for the same period -> conflict.
	if err := repo.Save(ctx, mk("inv-p2", invoice.InvoiceStatusFinalized, period, nil)); !isDomainError(err, shared.ErrCodeConflict) {
		t.Fatalf("second period invoice: expected ErrCodeConflict, got %v", err)
	}
	// Proration invoice for the same period coexists (exempt).
	if err := repo.Save(ctx, mk("inv-pro", invoice.InvoiceStatusFinalized, period,
		map[string]string{invoice.MetadataKeyInvoiceType: invoice.InvoiceTypeProration})); err != nil {
		t.Fatalf("proration invoice should coexist: %v", err)
	}
	// Zero-period invoices are unconstrained.
	if err := repo.Save(ctx, mk("inv-z1", invoice.InvoiceStatusFinalized, shared.DateRange{}, nil)); err != nil {
		t.Fatalf("zero-period invoice 1: %v", err)
	}
	if err := repo.Save(ctx, mk("inv-z2", invoice.InvoiceStatusFinalized, shared.DateRange{}, nil)); err != nil {
		t.Fatalf("zero-period invoice 2: %v", err)
	}

	// Void-and-recreate (RegenerateInvoice shape): void the original, then a
	// regeneration replacement for the SAME period succeeds because the voided
	// original is exempt.
	first, err := repo.FindByID(ctx, "inv-p1")
	if err != nil {
		t.Fatalf("load inv-p1: %v", err)
	}
	if err := first.VoidWithReason("superseded"); err != nil {
		t.Fatalf("void inv-p1: %v", err)
	}
	if err := repo.Save(ctx, first); err != nil {
		t.Fatalf("save voided inv-p1: %v", err)
	}
	if err := repo.Save(ctx, mk("inv-p1r", invoice.InvoiceStatusFinalized, period,
		map[string]string{invoice.MetadataKeyInvoiceType: invoice.InvoiceTypeRegeneration})); err != nil {
		t.Fatalf("regeneration replacement for same period after void: %v", err)
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

// TestInvoiceRepo_FindOverdue verifies parity with the core in-memory
// reference (infrastructure/inmemory/invoice_repository.go): overdue-marked
// invoices are returned regardless of due_date, issued/finalized invoices past
// their due date are returned, and everything else is excluded.
func TestInvoiceRepo_FindOverdue(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()
	_, _ = pool.Exec(ctx, `INSERT INTO contract_read_models (id, account_id, status) VALUES ('c-od', 'acc-od', 'active')`)

	repo := postgres.NewInvoiceRepository(pool)

	past := time.Now().Add(-48 * time.Hour)
	future := time.Now().Add(48 * time.Hour)

	saveInvoice := func(id string, status invoice.InvoiceStatus, due time.Time) {
		t.Helper()
		snap := invoice.InvoiceSnapshot{
			ID: shared.InvoiceID(id), AccountID: "acc-od", ContractID: "c-od",
			Status:   status,
			Subtotal: jpy(0), TaxAmount: jpy(0), DiscountAmount: jpy(0),
			Total: jpy(0), AppliedBalance: jpy(0), AmountDue: jpy(0),
			PaidAmount: jpy(0), Balance: jpy(0),
			DueDate: due,
		}
		inv, err := invoice.InvoiceFromSnapshot(snap)
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Save(ctx, inv); err != nil {
			t.Fatal(err)
		}
	}

	saveInvoice("od-issued-past", invoice.InvoiceStatusIssued, past)       // (b) included
	saveInvoice("od-finalized-past", invoice.InvoiceStatusFinalized, past) // (b) included
	saveInvoice("od-overdue-future", invoice.InvoiceStatusOverdue, future) // (a) included, due_date irrelevant
	saveInvoice("od-issued-future", invoice.InvoiceStatusIssued, future)   // excluded: not yet due
	saveInvoice("od-paid-past", invoice.InvoiceStatusPaid, past)           // excluded: paid
	saveInvoice("od-draft-past", invoice.InvoiceStatusDraft, past)         // excluded: draft

	got, err := repo.FindOverdue(ctx)
	if err != nil {
		t.Fatalf("FindOverdue: %v", err)
	}

	want := map[string]bool{
		"od-issued-past":    true,
		"od-finalized-past": true,
		"od-overdue-future": true,
	}
	gotIDs := map[string]bool{}
	for _, inv := range got {
		gotIDs[string(inv.ToSnapshot().ID)] = true
	}
	if len(gotIDs) != len(want) {
		t.Fatalf("FindOverdue returned %v, want exactly %v", gotIDs, want)
	}
	for id := range want {
		if !gotIDs[id] {
			t.Errorf("FindOverdue missing expected invoice %s", id)
		}
	}
}

// TestInvoiceRepo_FindUnpaidByContract_ExcludesRefunded verifies refunded
// invoices are not treated as unpaid (parity with the core in-memory
// reference), while draft/finalized/issued/overdue/partial_paid are.
func TestInvoiceRepo_FindUnpaidByContract_ExcludesRefunded(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()
	_, _ = pool.Exec(ctx, `INSERT INTO contract_read_models (id, account_id, status) VALUES ('c-up', 'acc-up', 'active')`)

	repo := postgres.NewInvoiceRepository(pool)

	saveInvoice := func(id string, status invoice.InvoiceStatus) {
		t.Helper()
		snap := invoice.InvoiceSnapshot{
			ID: shared.InvoiceID(id), AccountID: "acc-up", ContractID: "c-up",
			Status:   status,
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

	saveInvoice("up-issued", invoice.InvoiceStatusIssued)       // unpaid -> included
	saveInvoice("up-overdue", invoice.InvoiceStatusOverdue)     // unpaid -> included
	saveInvoice("up-partial", invoice.InvoiceStatusPartialPaid) // unpaid -> included
	saveInvoice("up-paid", invoice.InvoiceStatusPaid)           // excluded
	saveInvoice("up-voided", invoice.InvoiceStatusVoided)       // excluded
	saveInvoice("up-refunded", invoice.InvoiceStatusRefunded)   // excluded (the bug)

	got, err := repo.FindUnpaidByContract(ctx, "c-up")
	if err != nil {
		t.Fatalf("FindUnpaidByContract: %v", err)
	}

	want := map[string]bool{"up-issued": true, "up-overdue": true, "up-partial": true}
	gotIDs := map[string]bool{}
	for _, inv := range got {
		gotIDs[string(inv.ToSnapshot().ID)] = true
	}
	if gotIDs["up-refunded"] {
		t.Errorf("FindUnpaidByContract returned refunded invoice up-refunded")
	}
	if len(gotIDs) != len(want) {
		t.Fatalf("FindUnpaidByContract returned %v, want exactly %v", gotIDs, want)
	}
	for id := range want {
		if !gotIDs[id] {
			t.Errorf("FindUnpaidByContract missing expected invoice %s", id)
		}
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

// Two payments with the SAME idempotency_key but DIFFERENT ids: the loser's
// INSERT trips the payments_idempotency_key_key UNIQUE constraint (23505). Save
// MUST translate the raw pgconn error to payment.ErrDuplicateIdempotencyKey
// (issue #35 / core#97) — otherwise PaymentService routes the loser through
// saga compensation and refunds the winner's legitimate charge. A same-id
// re-save must still upsert in place.
func TestPaymentRepo_DuplicateIdempotencyKey(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()
	_, _ = pool.Exec(ctx, `INSERT INTO contract_read_models (id, account_id, status) VALUES ('c-pay-dup', 'acc-pay-dup', 'active')`)
	_, _ = pool.Exec(ctx, `INSERT INTO invoices (id, account_id, contract_id, status, subtotal, tax_amount, discount_amount, total, applied_balance, amount_due, paid_amount, balance) VALUES ('inv-pay-dup', 'acc-pay-dup', 'c-pay-dup', 'issued', 0, 0, 0, 0, 0, 0, 0, 0)`)

	repo := postgres.NewPaymentRepository(pool)

	winner := mustPayment(t, "pay-win", "inv-pay-dup", "idem-collide", payment.PaymentStatusCompleted, 5000)
	if err := repo.Save(ctx, winner); err != nil {
		t.Fatalf("Save winner: %v", err)
	}

	// Same idempotency_key, DIFFERENT id: must surface the core sentinel.
	loser := mustPayment(t, "pay-lose", "inv-pay-dup", "idem-collide", payment.PaymentStatusFailed, 9999)
	err := repo.Save(ctx, loser)
	if !errors.Is(err, payment.ErrDuplicateIdempotencyKey) {
		t.Fatalf("expected ErrDuplicateIdempotencyKey, got %v", err)
	}
	var dup *payment.DuplicateIdempotencyKeyError
	if !errors.As(err, &dup) {
		t.Fatalf("expected *DuplicateIdempotencyKeyError, got %v", err)
	}
	if dup.Key != "idem-collide" || dup.AttemptedID != "pay-lose" || dup.ExistingID != "pay-win" {
		t.Errorf("unexpected dup error: %+v", dup)
	}

	// The winner's row must be intact.
	got, err := repo.FindByID(ctx, "pay-win")
	if err != nil {
		t.Fatalf("FindByID winner: %v", err)
	}
	ws := got.ToSnapshot()
	if ws.Status != payment.PaymentStatusCompleted || ws.Amount.Int64() != 5000 {
		t.Errorf("winner corrupted: status=%q amount=%d (want completed/5000)", ws.Status, ws.Amount.Int64())
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM payments WHERE idempotency_key = 'idem-collide'`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 payment for the key, got %d", count)
	}

	// Same id (the 3DS upgrade path): re-saving pay-win must upsert in place.
	upgraded := mustPayment(t, "pay-win", "inv-pay-dup", "idem-collide", payment.PaymentStatusRefunded, 5000)
	if err := repo.Save(ctx, upgraded); err != nil {
		t.Fatalf("same-id re-save should upsert, got: %v", err)
	}
	reloaded, err := repo.FindByID(ctx, "pay-win")
	if err != nil {
		t.Fatalf("FindByID after update: %v", err)
	}
	if reloaded.ToSnapshot().Status != payment.PaymentStatusRefunded {
		t.Errorf("status = %q, want refunded", reloaded.ToSnapshot().Status)
	}
}

func mustPayment(t *testing.T, id, invoiceID, idempotencyKey string, status payment.PaymentStatus, amount int64) *payment.Payment {
	t.Helper()
	p, err := payment.FromSnapshot(payment.PaymentSnapshot{
		ID:             shared.PaymentID(id),
		InvoiceID:      shared.InvoiceID(invoiceID),
		Amount:         jpy(amount),
		RefundedAmount: jpy(0),
		Method:         payment.PaymentMethodCreditCard,
		Status:         status,
		IdempotencyKey: idempotencyKey,
		ProcessedAt:    time.Now().UTC().Truncate(time.Microsecond),
	})
	if err != nil {
		t.Fatalf("payment.FromSnapshot: %v", err)
	}
	return p
}

// Contract creation is idempotent at the storage layer: migration 010's partial
// unique index over the contract.created event's idempotency_key rejects a
// SECOND creation reusing a key, translated by the event store to an
// ErrCodeConflict DomainError (per contract.Repository.Save's godoc). Distinct
// keys are unaffected; historical events with no key are exempt.
func TestContractRepo_DuplicateIdempotencyKey(t *testing.T) {
	pool := postgrestest.NewPool(t)
	es := postgres.NewEventStore(pool)
	clock := shared.FixedClock{FixedTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)}
	repo := postgres.NewContractRepository(pool, es, clock)
	ctx := context.Background()

	first := newDraftContractKey(t, "c-idem-1", "acc-idem", "price-1", "dup-key", clock)
	if err := repo.Save(ctx, first); err != nil {
		t.Fatalf("Save first: %v", err)
	}

	// Same idempotency key, different contract id -> at-most-once creation.
	second := newDraftContractKey(t, "c-idem-2", "acc-idem", "price-1", "dup-key", clock)
	err := repo.Save(ctx, second)
	var de *shared.DomainError
	if !errors.As(err, &de) || de.Code != shared.ErrCodeConflict {
		t.Fatalf("expected conflict DomainError on duplicate idempotency key, got %v", err)
	}
	// Only the winner persisted.
	var streams int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(DISTINCT stream_id) FROM events WHERE data->>'idempotency_key' = 'dup-key'`).Scan(&streams); err != nil {
		t.Fatalf("count: %v", err)
	}
	if streams != 1 {
		t.Errorf("expected 1 contract for the key, got %d", streams)
	}

	// A different key is fine.
	third := newDraftContractKey(t, "c-idem-3", "acc-idem", "price-1", "other-key", clock)
	if err := repo.Save(ctx, third); err != nil {
		t.Fatalf("Save with distinct key: %v", err)
	}

	// Historical events with an absent idempotency_key are EXEMPT: two raw
	// contract.created events without the key must coexist (the partial index
	// excludes coalesce(data->>'idempotency_key','') = '').
	for _, id := range []string{"c-legacy-1", "c-legacy-2"} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO events (id, stream_id, type, version, schema_version, data, metadata, occurred_at)
			 VALUES ($1, $1, 'contract.created', 1, 2, $2, '{}', NOW())`,
			id, fmt.Sprintf(`{"contract_id":%q,"account_id":"acc-legacy"}`, id)); err != nil {
			t.Fatalf("insert legacy event %s: %v", id, err)
		}
	}
}

// The event store round-trips whatever SchemaVersion core stamps on an event —
// the append/load path is agnostic to it (ContractCreatedEvent is v3 since
// core#159). It must store and return v3 unchanged, never coercing to 1.
func TestEventStore_SchemaVersionRoundTrip(t *testing.T) {
	pool := postgrestest.NewPool(t)
	store := postgres.NewEventStore(pool)
	ctx := context.Background()

	ev := eventstore.Event{
		ID: "sv-1", Type: contract.EventTypeContractCreated, SchemaVersion: 3,
		Data:       json.RawMessage(`{"contract_id":"c-sv","account_id":"a","idempotency_key":"sv-key"}`),
		OccurredAt: time.Now().UTC(),
	}
	if err := store.Append(ctx, "c-sv", []eventstore.Event{ev}, 0); err != nil {
		t.Fatalf("Append: %v", err)
	}
	loaded, err := store.Load(ctx, "c-sv")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 1 || loaded[0].SchemaVersion != 3 {
		t.Fatalf("expected 1 event at SchemaVersion 3, got %+v", loaded)
	}
}

// --- Balance Repository ---

func TestBalanceRepo_FindExpired(t *testing.T) {
	pool := postgrestest.NewPool(t)
	repo := postgres.NewBalanceRepository(pool)
	ctx := context.Background()

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	asOf := base.Add(30 * 24 * time.Hour)
	past := base.Add(24 * time.Hour)   // expired before asOf
	future := asOf.Add(24 * time.Hour) // not yet expired

	mk := func(id string, remaining int64, expires *time.Time, off time.Duration) {
		snap := balance.BalanceEntrySnapshot{
			ID: shared.BalanceEntryID(id), AccountID: "acc-exp",
			OriginalAmount: jpy(1000), RemainingAmount: jpy(remaining),
			Reason: balance.BalanceReasonManualAdjustment, SourceType: balance.BalanceSourceTypeManual,
			ExpiresAt: expires, Version: 0, CreatedAt: base.Add(off),
		}
		e, err := balance.FromSnapshot(snap)
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Save(ctx, e); err != nil {
			t.Fatalf("Save %s: %v", id, err)
		}
	}
	mk("exp-old", 500, &past, time.Hour)      // expired + remaining -> included (first)
	mk("exp-new", 700, &past, 2*time.Hour)    // expired + remaining -> included (second)
	mk("exp-consumed", 0, &past, 3*time.Hour) // expired but fully consumed -> excluded
	mk("live", 900, &future, 4*time.Hour)     // not expired -> excluded
	mk("no-expiry", 900, nil, 5*time.Hour)    // no expiry -> excluded

	got, err := repo.FindExpired(ctx, asOf)
	if err != nil {
		t.Fatalf("FindExpired: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 expired entries with remaining, got %d: %+v", len(got), got)
	}
	if got[0].ToSnapshot().ID != "exp-old" || got[1].ToSnapshot().ID != "exp-new" {
		t.Errorf("expected FIFO order [exp-old exp-new], got [%s %s]",
			got[0].ToSnapshot().ID, got[1].ToSnapshot().ID)
	}
}

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

	// A second record reusing the same idempotency key (even with a different id)
	// must surface as the core's DomainError(duplicate_request), matching the
	// in-memory reference implementation — not be silently swallowed (issue #38).
	dup, err := usage.NewUsageRecord("ur-2", "c-usage", "api_calls", 200, now, "idem-ur-1")
	if err != nil {
		t.Fatalf("NewUsageRecord dup: %v", err)
	}
	err = repo.Record(ctx, dup)
	if err == nil {
		t.Fatal("expected duplicate idempotency key to return an error, got nil")
	}
	var de *shared.DomainError
	if !errors.As(err, &de) {
		t.Fatalf("expected *shared.DomainError, got %T: %v", err, err)
	}
	if de.Code != shared.ErrCodeDuplicateRequest {
		t.Errorf("Code = %q, want %q", de.Code, shared.ErrCodeDuplicateRequest)
	}

	// Records without an idempotency key are never deduplicated (NULLs are
	// distinct in Postgres), matching inmemory.
	for _, id := range []shared.UsageRecordID{"ur-nokey-1", "ur-nokey-2"} {
		rk, err := usage.NewUsageRecord(id, "c-usage", "api_calls", 5, now, "")
		if err != nil {
			t.Fatalf("NewUsageRecord %s: %v", id, err)
		}
		if err := repo.Record(ctx, rk); err != nil {
			t.Fatalf("Record %s (no key): %v", id, err)
		}
	}

	period, err := shared.NewDateRange(now.Add(-time.Hour), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	summary, err := repo.GetSummary(ctx, "c-usage", "api_calls", period)
	if err != nil {
		t.Fatal(err)
	}
	// 100 (rec) + 5 + 5 (keyless); the duplicate-key write was rejected.
	if summary.TotalUsage != 110 {
		t.Errorf("TotalUsage = %d, want 110", summary.TotalUsage)
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
