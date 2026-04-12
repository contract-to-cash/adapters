package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/contract-to-cash/adapters/postgres"
	"github.com/contract-to-cash/adapters/postgres/postgrestest"
	"github.com/contract-to-cash/core/application/tx"
	"github.com/contract-to-cash/core/domain/invoice"
	"github.com/contract-to-cash/core/eventstore"
)

// --- #1: version conflict must return tx.ErrVersionConflict ---

func TestEventStore_VersionConflictIsTxErrVersionConflict(t *testing.T) {
	pool := postgrestest.NewPool(t)
	store := postgres.NewEventStore(pool)
	ctx := context.Background()

	event1 := []eventstore.Event{{
		ID: "v1-evt-1", Type: "test.event", SchemaVersion: 1,
		Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
	}}
	if err := store.Append(ctx, "stream-v1", event1, 0); err != nil {
		t.Fatal(err)
	}

	// Append at same version should conflict and return tx.ErrVersionConflict
	// (not just a shared.DomainError), so that tx.RetryOnConflict retries.
	event2 := []eventstore.Event{{
		ID: "v1-evt-2", Type: "test.event", SchemaVersion: 1,
		Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
	}}
	err := store.Append(ctx, "stream-v1", event2, 0)
	if err == nil {
		t.Fatal("expected version conflict")
	}
	if !errors.Is(err, tx.ErrVersionConflict) {
		t.Errorf("expected errors.Is(err, tx.ErrVersionConflict), got: %v", err)
	}
}

// --- #1b: RetryOnConflict actually retries on version conflict ---

func TestEventStore_RetryOnConflictRetries(t *testing.T) {
	pool := postgrestest.NewPool(t)
	store := postgres.NewEventStore(pool)
	ctx := context.Background()

	// First seed one event.
	seed := []eventstore.Event{{
		ID: "r-seed", Type: "test.event", SchemaVersion: 1,
		Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
	}}
	if err := store.Append(ctx, "stream-retry", seed, 0); err != nil {
		t.Fatal(err)
	}

	attempts := 0
	expectedVersion := 0 // intentionally stale: first attempt will conflict
	err := tx.RetryOnConflict(3, func() error {
		attempts++
		evt := []eventstore.Event{{
			ID:         fmt.Sprintf("r-try-%d", attempts),
			Type:       "test.event",
			SchemaVersion: 1,
			Data:       json.RawMessage(`{}`),
			OccurredAt: time.Now().UTC(),
		}}
		appendErr := store.Append(ctx, "stream-retry", evt, expectedVersion)
		if appendErr != nil {
			expectedVersion = 1 // correct version for next retry
			return appendErr
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RetryOnConflict: %v", err)
	}
	if attempts < 2 {
		t.Errorf("expected at least 2 attempts (retry), got %d", attempts)
	}
}

// --- #4: invoice .cancelled should produce status 'cancelled', not 'voided' ---

func TestInvoiceProjector_CancelledDistinctFromVoided(t *testing.T) {
	pool := postgrestest.NewPool(t)
	es := postgres.NewEventStore(pool)
	cp := postgres.NewCheckpointStore(pool)
	proj := postgres.NewInvoiceProjector(pool, es, cp)
	ctx := context.Background()

	// Seed an invoice read model row.
	_, _ = pool.Exec(ctx,
		`INSERT INTO invoice_read_models (id, contract_id, account_id, status, total, currency)
		 VALUES ('inv-can', 'c-can', 'acc-can', 'issued', 0, 'JPY')`)

	cancelledEvt := eventstore.Event{
		ID: "evt-inv-can", StreamID: "inv-can", Type: "invoice.cancelled",
		Version: 2, SchemaVersion: 1,
		Data:       json.RawMessage(`{}`),
		OccurredAt: time.Now().UTC(),
	}
	if err := proj.Project(ctx, cancelledEvt); err != nil {
		t.Fatal(err)
	}

	var status string
	_ = pool.QueryRow(ctx, `SELECT status FROM invoice_read_models WHERE id = 'inv-can'`).Scan(&status)
	if status != "cancelled" {
		t.Errorf("status = %q, want 'cancelled' (distinct from 'voided')", status)
	}
}

// --- #8: billingFrom/billingTo fallback when JSON lacks billing_period ---

func TestInvoiceRepo_BillingPeriodFallbackFromColumns(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()
	_, _ = pool.Exec(ctx, `INSERT INTO contract_read_models (id, account_id, status) VALUES ('c-bp', 'acc-bp', 'active')`)

	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	// Insert an invoice row directly with billing_period columns set but
	// with snapshot_state that has NO billing_period key. The scan should
	// fall back to the billing_period_from/to columns.
	_, err := pool.Exec(ctx,
		`INSERT INTO invoices (
			id, invoice_number, account_id, contract_id, status,
			subtotal, tax_amount, discount_amount, total,
			applied_balance, amount_due, paid_amount, balance,
			currency, billing_period_from, billing_period_to,
			line_items, snapshot_state, metadata, version
		) VALUES (
			'inv-bp', '', 'acc-bp', 'c-bp', 'draft',
			0, 0, 0, 0, 0, 0, 0, 0,
			'JPY', $1, $2,
			'[]', '{}', '{}', 1
		)`, from, to)
	if err != nil {
		t.Fatal(err)
	}

	repo := postgres.NewInvoiceRepository(pool)
	inv, err := repo.FindByID(ctx, "inv-bp")
	if err != nil {
		t.Fatal(err)
	}
	s := inv.ToSnapshot()
	if s.BillingPeriod.IsZero() {
		t.Error("BillingPeriod should not be zero when columns have values")
	}
	if !s.BillingPeriod.Start().Equal(from) {
		t.Errorf("BillingPeriod.Start = %v, want %v", s.BillingPeriod.Start(), from)
	}
	if !s.BillingPeriod.End().Equal(to) {
		t.Errorf("BillingPeriod.End = %v, want %v", s.BillingPeriod.End(), to)
	}
}

// --- #5: schema misnomer — line_items JSONB should only contain line items ---
// This test documents the expected schema: line_items column holds only
// the array of line items, not the full snapshot state.

func TestInvoiceRepo_LineItemsColumnHoldsOnlyLineItems(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()
	_, _ = pool.Exec(ctx, `INSERT INTO contract_read_models (id, account_id, status) VALUES ('c-li', 'acc-li', 'active')`)

	repo := postgres.NewInvoiceRepository(pool)
	snap := invoice.InvoiceSnapshot{
		ID: "inv-li", AccountID: "acc-li", ContractID: "c-li",
		Status: invoice.StatusDraft,
		Subtotal: jpy(1000), TaxAmount: jpy(100), DiscountAmount: jpy(0),
		Total: jpy(1100), AppliedBalance: jpy(0), AmountDue: jpy(1100),
		PaidAmount: jpy(0), Balance: jpy(0),
		LineItems: []invoice.LineItemSnapshot{
			{ID: "li-1", Description: "Item 1", Quantity: 1, UnitPrice: jpy(1000), Amount: jpy(1000)},
		},
	}
	if err := repo.Save(ctx, invoice.NewInvoice(snap)); err != nil {
		t.Fatal(err)
	}

	// Read the raw line_items JSONB column. It should be a JSON ARRAY
	// (the line items), not an OBJECT containing multiple snapshot fields.
	var lineItemsRaw json.RawMessage
	err := pool.QueryRow(ctx, `SELECT line_items FROM invoices WHERE id = 'inv-li'`).Scan(&lineItemsRaw)
	if err != nil {
		t.Fatal(err)
	}

	trimmed := strings.TrimSpace(string(lineItemsRaw))
	if !strings.HasPrefix(trimmed, "[") {
		t.Errorf("line_items should be a JSON array, got: %s", trimmed)
	}
}
