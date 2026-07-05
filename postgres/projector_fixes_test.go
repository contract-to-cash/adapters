package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/contract-to-cash/adapters/postgres"
	"github.com/contract-to-cash/adapters/postgres/postgrestest"
	"github.com/contract-to-cash/core/domain/contract"
	"github.com/contract-to-cash/core/eventstore"
)

// seedContract creates a contract read-model row via a created event.
func seedContract(t *testing.T, ctx context.Context, es *postgres.PostgresEventStore, proj *postgres.ContractProjector, id, priceID string, occurred time.Time) {
	t.Helper()
	createEvt := eventstore.Event{
		ID: "evt-" + id + "-create", StreamID: id, Type: contract.EventTypeContractCreated,
		Version: 1, SchemaVersion: 1,
		Data:       json.RawMessage(`{"account_id":"acc-` + id + `","price_id":"` + priceID + `"}`),
		OccurredAt: occurred,
	}
	if err := es.Append(ctx, id, []eventstore.Event{createEvt}, 0); err != nil {
		t.Fatal(err)
	}
	if err := proj.Project(ctx, createEvt); err != nil {
		t.Fatal(err)
	}
}

// --- Issue #14 Fix 1: trial_ended honors the converted flag ---

func TestContractProjector_TrialEnded_NotConverted_IsCancelled(t *testing.T) {
	pool := postgrestest.NewPool(t)
	es := postgres.NewEventStore(pool)
	cp := postgres.NewCheckpointStore(pool)
	proj := postgres.NewContractProjector(pool, es, cp)
	ctx := context.Background()

	seedContract(t, ctx, es, proj, "c-trial-nc", "p-1", time.Now().UTC())

	trialEnd := eventstore.Event{
		ID: "evt-trial-nc", StreamID: "c-trial-nc", Type: contract.EventTypeTrialEnded,
		Version: 2, SchemaVersion: 1,
		Data:       json.RawMessage(`{"converted":false}`),
		OccurredAt: time.Now().UTC(),
	}
	if err := es.Append(ctx, "c-trial-nc", []eventstore.Event{trialEnd}, 1); err != nil {
		t.Fatal(err)
	}
	if err := proj.Project(ctx, trialEnd); err != nil {
		t.Fatal(err)
	}

	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM contract_read_models WHERE id = 'c-trial-nc'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "cancelled" {
		t.Errorf("status = %q, want 'cancelled'", status)
	}
}

func TestContractProjector_TrialEnded_Converted_IsActive(t *testing.T) {
	pool := postgrestest.NewPool(t)
	es := postgres.NewEventStore(pool)
	cp := postgres.NewCheckpointStore(pool)
	proj := postgres.NewContractProjector(pool, es, cp)
	ctx := context.Background()

	seedContract(t, ctx, es, proj, "c-trial-c", "p-1", time.Now().UTC())

	trialEnd := eventstore.Event{
		ID: "evt-trial-c", StreamID: "c-trial-c", Type: contract.EventTypeTrialEnded,
		Version: 2, SchemaVersion: 1,
		Data:       json.RawMessage(`{"converted":true}`),
		OccurredAt: time.Now().UTC(),
	}
	if err := es.Append(ctx, "c-trial-c", []eventstore.Event{trialEnd}, 1); err != nil {
		t.Fatal(err)
	}
	if err := proj.Project(ctx, trialEnd); err != nil {
		t.Fatal(err)
	}

	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM contract_read_models WHERE id = 'c-trial-c'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "active" {
		t.Errorf("status = %q, want 'active'", status)
	}
}

// --- Issue #14 Fix 2: invoice total parsed from core Money JSON ---

func TestInvoiceProjector_Created_ParsesMoneyTotal(t *testing.T) {
	pool := postgrestest.NewPool(t)
	es := postgres.NewEventStore(pool)
	cp := postgres.NewCheckpointStore(pool)
	proj := postgres.NewInvoiceProjector(pool, es, cp)
	ctx := context.Background()

	evt := eventstore.Event{
		ID: "evt-inv-money", StreamID: "inv-money", Type: "invoice.created",
		Version: 1, SchemaVersion: 1,
		Data:       json.RawMessage(`{"contract_id":"c-m","account_id":"acc-m","status":"draft","total":{"amount":"11000/1","currency":"JPY"}}`),
		OccurredAt: time.Now().UTC(),
	}
	if err := es.Append(ctx, "inv-money", []eventstore.Event{evt}, 0); err != nil {
		t.Fatal(err)
	}
	if err := proj.Project(ctx, evt); err != nil {
		t.Fatal(err)
	}

	var total int64
	var currency string
	if err := pool.QueryRow(ctx, `SELECT total, currency FROM invoice_read_models WHERE id = 'inv-money'`).Scan(&total, &currency); err != nil {
		t.Fatal(err)
	}
	if total != 11000 {
		t.Errorf("total = %d, want 11000", total)
	}
	if currency != "JPY" {
		t.Errorf("currency = %q, want JPY", currency)
	}
}

// --- Issue #14 Fix 3: PriceChanged updates price_id ---

func TestContractProjector_PriceChanged_UpdatesPriceID(t *testing.T) {
	pool := postgrestest.NewPool(t)
	es := postgres.NewEventStore(pool)
	cp := postgres.NewCheckpointStore(pool)
	proj := postgres.NewContractProjector(pool, es, cp)
	ctx := context.Background()

	seedContract(t, ctx, es, proj, "c-price", "price-old", time.Now().UTC())

	changed := eventstore.Event{
		ID: "evt-price", StreamID: "c-price", Type: contract.EventTypePriceChanged,
		Version: 2, SchemaVersion: 1,
		Data:       json.RawMessage(`{"old_price_id":"price-old","new_price_id":"price-new"}`),
		OccurredAt: time.Now().UTC(),
	}
	if err := es.Append(ctx, "c-price", []eventstore.Event{changed}, 1); err != nil {
		t.Fatal(err)
	}
	if err := proj.Project(ctx, changed); err != nil {
		t.Fatal(err)
	}

	var priceID string
	if err := pool.QueryRow(ctx, `SELECT price_id FROM contract_read_models WHERE id = 'c-price'`).Scan(&priceID); err != nil {
		t.Fatal(err)
	}
	if priceID != "price-new" {
		t.Errorf("price_id = %q, want 'price-new'", priceID)
	}
}

// --- Issue #14 Fix 4: Rebuild(until) leaves skipped events for incremental catch-up ---

func TestContractProjector_Rebuild_CheckpointStopsAtUntil(t *testing.T) {
	pool := postgrestest.NewPool(t)
	es := postgres.NewEventStore(pool)
	cp := postgres.NewCheckpointStore(pool)
	proj := postgres.NewContractProjector(pool, es, cp)
	ctx := context.Background()

	until := time.Now().UTC()

	// Event 1 (created) is before `until`; event 2 (suspended) is after it.
	createEvt := eventstore.Event{
		ID: "evt-rb-create", StreamID: "c-rb", Type: contract.EventTypeContractCreated,
		Version: 1, SchemaVersion: 1,
		Data:       json.RawMessage(`{"account_id":"acc-rb","price_id":"p-rb"}`),
		OccurredAt: until.Add(-time.Hour),
	}
	suspendEvt := eventstore.Event{
		ID: "evt-rb-suspend", StreamID: "c-rb", Type: contract.EventTypeContractSuspended,
		Version: 2, SchemaVersion: 1,
		Data:       json.RawMessage(`{}`),
		OccurredAt: until.Add(time.Hour),
	}
	if err := es.Append(ctx, "c-rb", []eventstore.Event{createEvt, suspendEvt}, 0); err != nil {
		t.Fatal(err)
	}

	// Discover the global positions the store assigned.
	all, err := es.LoadAll(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	var createdPos, suspendPos int64
	for _, e := range all {
		switch e.ID {
		case "evt-rb-create":
			createdPos = e.GlobalPosition
		case "evt-rb-suspend":
			suspendPos = e.GlobalPosition
		}
	}
	if createdPos == 0 || suspendPos == 0 || suspendPos <= createdPos {
		t.Fatalf("unexpected positions: created=%d suspend=%d", createdPos, suspendPos)
	}

	if err := proj.Rebuild(ctx, until); err != nil {
		t.Fatal(err)
	}

	// The suspended event was after `until`, so the read model is still 'draft'.
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM contract_read_models WHERE id = 'c-rb'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "draft" {
		t.Errorf("post-rebuild status = %q, want 'draft'", status)
	}

	// The checkpoint must sit at the last *projected* event (created), not the
	// scanned-but-skipped suspended event. Otherwise the skipped event is lost.
	pos, err := cp.Load(ctx, postgres.ContractProjectorName)
	if err != nil {
		t.Fatal(err)
	}
	if pos != createdPos {
		t.Fatalf("checkpoint = %d, want %d (created event); skipped events would be lost", pos, createdPos)
	}

	// Simulate the incremental projector resuming from the checkpoint: it must
	// pick up the previously skipped suspended event.
	remaining, err := es.LoadAll(ctx, pos, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range remaining {
		if err := proj.Project(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM contract_read_models WHERE id = 'c-rb'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "suspended" {
		t.Errorf("post-incremental status = %q, want 'suspended'", status)
	}
}
