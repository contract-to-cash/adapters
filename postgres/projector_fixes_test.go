package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/contract-to-cash/adapters/postgres"
	"github.com/contract-to-cash/adapters/postgres/postgrestest"
	"github.com/contract-to-cash/core/domain/contract"
	"github.com/contract-to-cash/core/domain/pricing"
	"github.com/contract-to-cash/core/domain/shared"
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

// --- Dunning: past_due / recovered transitions are materialized ---

// A payment-failure dunning transition (core MarkPastDue) must land the read
// model in 'past_due' so status-filtered queries (e.g. FindDueForRenewal's
// status = 'active') stop selecting the contract.
func TestContractProjector_PastDue_IsPastDue(t *testing.T) {
	pool := postgrestest.NewPool(t)
	es := postgres.NewEventStore(pool)
	cp := postgres.NewCheckpointStore(pool)
	proj := postgres.NewContractProjector(pool, es, cp)
	ctx := context.Background()

	seedContract(t, ctx, es, proj, "c-pastdue", "p-1", time.Now().UTC())

	pastDue := eventstore.Event{
		ID: "evt-pastdue", StreamID: "c-pastdue", Type: contract.EventTypeContractPastDue,
		Version: 2, SchemaVersion: 1,
		Data:       json.RawMessage(`{"contract_id":"c-pastdue","reason":"payment_failed","marked_at":"2026-06-01T12:00:00Z"}`),
		OccurredAt: time.Now().UTC(),
	}
	if err := es.Append(ctx, "c-pastdue", []eventstore.Event{pastDue}, 1); err != nil {
		t.Fatal(err)
	}
	if err := proj.Project(ctx, pastDue); err != nil {
		t.Fatal(err)
	}

	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM contract_read_models WHERE id = 'c-pastdue'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "past_due" {
		t.Errorf("status = %q, want 'past_due'", status)
	}
}

// A recovery transition (core RecoverFromPastDue) must return the read model
// to 'active', mirroring the resumed case.
func TestContractProjector_Recovered_IsActive(t *testing.T) {
	pool := postgrestest.NewPool(t)
	es := postgres.NewEventStore(pool)
	cp := postgres.NewCheckpointStore(pool)
	proj := postgres.NewContractProjector(pool, es, cp)
	ctx := context.Background()

	seedContract(t, ctx, es, proj, "c-recovered", "p-1", time.Now().UTC())

	pastDue := eventstore.Event{
		ID: "evt-rec-pastdue", StreamID: "c-recovered", Type: contract.EventTypeContractPastDue,
		Version: 2, SchemaVersion: 1,
		Data:       json.RawMessage(`{"contract_id":"c-recovered","reason":"payment_failed","marked_at":"2026-06-01T12:00:00Z"}`),
		OccurredAt: time.Now().UTC(),
	}
	recovered := eventstore.Event{
		ID: "evt-recovered", StreamID: "c-recovered", Type: contract.EventTypeContractRecovered,
		Version: 3, SchemaVersion: 1,
		Data:       json.RawMessage(`{"contract_id":"c-recovered","recovered_at":"2026-06-02T12:00:00Z"}`),
		OccurredAt: time.Now().UTC(),
	}
	if err := es.Append(ctx, "c-recovered", []eventstore.Event{pastDue, recovered}, 1); err != nil {
		t.Fatal(err)
	}
	for _, e := range []eventstore.Event{pastDue, recovered} {
		if err := proj.Project(ctx, e); err != nil {
			t.Fatal(err)
		}
	}

	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM contract_read_models WHERE id = 'c-recovered'`).Scan(&status); err != nil {
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

// --- Core #218: zero-interval one_time contracts must not be due for renewal ---

// activateContract runs Create+Activate through the real aggregate (so the
// event payloads exactly match core's ContractCreatedEvent/ContractActivatedEvent
// schemas), saves via the repo, and projects the resulting events into
// contract_read_models.
func activateContract(t *testing.T, ctx context.Context, repo *postgres.PostgresContractRepository, es *postgres.PostgresEventStore, proj *postgres.ContractProjector, id shared.ContractID, contractType contract.ContractType, interval pricing.BillingInterval, clock shared.Clock) {
	t.Helper()
	agg := contract.NewContractAggregate(id, clock)
	if err := agg.Create(contract.CreateContractCommand{
		IdempotencyKey: "idem-" + string(id),
		AccountID:      shared.AccountID("acc-" + string(id)),
		PriceID:        shared.PriceID("price-" + string(id)),
		ContractType:   contractType,
		Interval:       interval,
		Price:          jpy(1000),
	}, eventstore.EventMetadata{UserID: "test-user"}); err != nil {
		t.Fatalf("Create %s: %v", id, err)
	}
	if err := agg.Activate(eventstore.EventMetadata{UserID: "test-user"}); err != nil {
		t.Fatalf("Activate %s: %v", id, err)
	}
	if err := repo.Save(ctx, agg); err != nil {
		t.Fatalf("Save %s: %v", id, err)
	}
	events, err := es.Load(ctx, string(id))
	if err != nil {
		t.Fatalf("Load events %s: %v", id, err)
	}
	for _, e := range events {
		if err := proj.Project(ctx, e); err != nil {
			t.Fatalf("Project %s: %v", id, err)
		}
	}
}

// A zero-interval one_time contract (core issue #218) activates with a zero
// CurrentPeriod. The event's current_period marshals as
// {"start":"0001-01-01T00:00:00Z","end":"0001-01-01T00:00:00Z"} (never
// null/omitted — see core domain/shared/datetime.go DateRange.MarshalJSON), so
// the projector's parseTime must treat that year-0001 timestamp as "no value"
// and leave renewal_date/end_date NULL. Otherwise FindDueForRenewal would
// wrongly return a one_time contract that has no recurring billing.
func TestContractRepo_FindDueForRenewal_ExcludesZeroIntervalOneTime(t *testing.T) {
	pool := postgrestest.NewPool(t)
	es := postgres.NewEventStore(pool)
	cp := postgres.NewCheckpointStore(pool)
	clock := shared.FixedClock{FixedTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)}
	repo := postgres.NewContractRepository(pool, es, clock)
	proj := postgres.NewContractProjector(pool, es, cp)
	ctx := context.Background()

	activateContract(t, ctx, repo, es, proj, "c-onetime-zero", contract.ContractTypeOneTime, pricing.BillingInterval{}, clock)

	future := clock.Now().Add(365 * 24 * time.Hour)
	due, err := repo.FindDueForRenewal(ctx, future, 0)
	if err != nil {
		t.Fatalf("FindDueForRenewal: %v", err)
	}
	for _, c := range due {
		if c.ID() == "c-onetime-zero" {
			t.Fatalf("zero-interval one_time contract %s wrongly returned as due for renewal", c.ID())
		}
	}

	var renewalDate, endDate any
	if err := pool.QueryRow(ctx, `SELECT renewal_date, end_date FROM contract_read_models WHERE id = 'c-onetime-zero'`).
		Scan(&renewalDate, &endDate); err != nil {
		t.Fatal(err)
	}
	if renewalDate != nil {
		t.Errorf("renewal_date = %v, want NULL", renewalDate)
	}
	if endDate != nil {
		t.Errorf("end_date = %v, want NULL", endDate)
	}
}

// FindExpiring must likewise exclude a zero-interval one_time contract (core
// issue #218). Its predicate is `end_date IS NOT NULL AND end_date < $1`, so
// the same parseTime zero-time guard (which leaves end_date NULL for a
// zero-interval activation) keeps such a contract out of the expiry sweep — a
// one_time contract with no billing period never "expires" on a schedule.
func TestContractRepo_FindExpiring_ExcludesZeroIntervalOneTime(t *testing.T) {
	pool := postgrestest.NewPool(t)
	es := postgres.NewEventStore(pool)
	cp := postgres.NewCheckpointStore(pool)
	clock := shared.FixedClock{FixedTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)}
	repo := postgres.NewContractRepository(pool, es, clock)
	proj := postgres.NewContractProjector(pool, es, cp)
	ctx := context.Background()

	activateContract(t, ctx, repo, es, proj, "c-onetime-zero-exp", contract.ContractTypeOneTime, pricing.BillingInterval{}, clock)

	future := clock.Now().Add(365 * 24 * time.Hour)
	expiring, err := repo.FindExpiring(ctx, future)
	if err != nil {
		t.Fatalf("FindExpiring: %v", err)
	}
	for _, c := range expiring {
		if c.ID() == "c-onetime-zero-exp" {
			t.Fatalf("zero-interval one_time contract %s wrongly returned as expiring", c.ID())
		}
	}
}

// A normal subscription activation (real, non-zero interval) must still be
// returned by FindDueForRenewal once its renewal date is on/before asOf — no
// regression from the parseTime zero-time guard.
func TestContractRepo_FindDueForRenewal_IncludesNormalSubscription(t *testing.T) {
	pool := postgrestest.NewPool(t)
	es := postgres.NewEventStore(pool)
	cp := postgres.NewCheckpointStore(pool)
	clock := shared.FixedClock{FixedTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)}
	repo := postgres.NewContractRepository(pool, es, clock)
	proj := postgres.NewContractProjector(pool, es, cp)
	ctx := context.Background()

	activateContract(t, ctx, repo, es, proj, "c-sub-normal", contract.ContractTypeSubscription, pricing.Monthly(), clock)

	future := clock.Now().Add(60 * 24 * time.Hour)
	due, err := repo.FindDueForRenewal(ctx, future, 0)
	if err != nil {
		t.Fatalf("FindDueForRenewal: %v", err)
	}
	var found bool
	for _, c := range due {
		if c.ID() == "c-sub-normal" {
			found = true
		}
	}
	if !found {
		t.Errorf("normal subscription contract not returned as due for renewal")
	}

	var renewalDate, endDate any
	if err := pool.QueryRow(ctx, `SELECT renewal_date, end_date FROM contract_read_models WHERE id = 'c-sub-normal'`).
		Scan(&renewalDate, &endDate); err != nil {
		t.Fatal(err)
	}
	if renewalDate == nil {
		t.Error("renewal_date = NULL, want set")
	}
	if endDate == nil {
		t.Error("end_date = NULL, want set")
	}
}

// --- Issue #63: the projector must run events through the core upcaster
// chain before projecting, just like aggregate rehydration (LoadFromHistory)
// does. Both regression tests below feed a LEGACY (pre-upcast) payload
// straight into Project and assert the persisted contract_read_models.data
// reflects the MIGRATED shape.

// A legacy (SchemaVersion 1) contract.created payload carries billing_cycle
// instead of interval -- the schema contract.ContractCreatedEventUpcaster
// migrates (core/domain/contract/upcaster.go). The projector must persist the
// upcasted payload (with "interval" populated from "billing_cycle") into
// contract_read_models.data, not the verbatim legacy JSON.
func TestContractProjector_Created_LegacyBillingCycle_UpcastsInterval(t *testing.T) {
	pool := postgrestest.NewPool(t)
	es := postgres.NewEventStore(pool)
	cp := postgres.NewCheckpointStore(pool)
	proj := postgres.NewContractProjector(pool, es, cp)
	ctx := context.Background()

	createEvt := eventstore.Event{
		ID: "evt-legacy-create", StreamID: "c-legacy-billing-cycle", Type: contract.EventTypeContractCreated,
		Version: 1, SchemaVersion: 1,
		Data: json.RawMessage(`{"contract_id":"c-legacy-billing-cycle","account_id":"acc-legacy",
			"price_id":"price-legacy","billing_cycle":"yearly","contract_type":"subscription",
			"created_at":"2026-01-01T00:00:00Z"}`),
		OccurredAt: time.Now().UTC(),
	}
	if err := es.Append(ctx, "c-legacy-billing-cycle", []eventstore.Event{createEvt}, 0); err != nil {
		t.Fatal(err)
	}
	if err := proj.Project(ctx, createEvt); err != nil {
		t.Fatalf("Project legacy created: %v", err)
	}

	var unit string
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT data->'interval'->>'unit', (data->'interval'->>'count')::int
		 FROM contract_read_models WHERE id = 'c-legacy-billing-cycle'`,
	).Scan(&unit, &count); err != nil {
		t.Fatalf("query projected interval: %v", err)
	}
	if unit != "year" || count != 1 {
		t.Errorf("data.interval = {unit:%q count:%d}, want {unit:\"year\" count:1} (recovered from legacy billing_cycle)", unit, count)
	}

	var priceID string
	if err := pool.QueryRow(ctx, `SELECT price_id FROM contract_read_models WHERE id = 'c-legacy-billing-cycle'`).Scan(&priceID); err != nil {
		t.Fatal(err)
	}
	if priceID != "price-legacy" {
		t.Errorf("price_id = %q, want 'price-legacy'", priceID)
	}
}

// A legacy (SchemaVersion 1) price.changed payload carries only the old/new
// Money pair, with no new_price_id field at all -- the schema
// contract.PriceChangedEventUpcaster adds (defaulting it to "" when absent).
// The projector's COALESCE(NULLIF($1, empty string)) guard must still leave the read
// model's existing price_id untouched, and the persisted data blob must carry
// the upcasted policy/old_price_id/new_price_id fields the legacy payload
// never had.
func TestContractProjector_PriceChanged_LegacyV1_NoNewPriceID_LeavesPriceIDUnchanged(t *testing.T) {
	pool := postgrestest.NewPool(t)
	es := postgres.NewEventStore(pool)
	cp := postgres.NewCheckpointStore(pool)
	proj := postgres.NewContractProjector(pool, es, cp)
	ctx := context.Background()

	seedContract(t, ctx, es, proj, "c-price-legacy", "price-original", time.Now().UTC())

	changed := eventstore.Event{
		ID: "evt-price-legacy", StreamID: "c-price-legacy", Type: contract.EventTypePriceChanged,
		Version: 2, SchemaVersion: 1,
		Data: json.RawMessage(`{"contract_id":"c-price-legacy",
			"old_price":{"amount":"1000/1","currency":"JPY"},
			"new_price":{"amount":"2000/1","currency":"JPY"},
			"changed_at":"2026-01-01T00:00:00Z","effective_at":"2026-01-01T00:00:00Z"}`),
		OccurredAt: time.Now().UTC(),
	}
	if err := es.Append(ctx, "c-price-legacy", []eventstore.Event{changed}, 1); err != nil {
		t.Fatal(err)
	}
	if err := proj.Project(ctx, changed); err != nil {
		t.Fatalf("Project legacy price_changed: %v", err)
	}

	var priceID string
	if err := pool.QueryRow(ctx, `SELECT price_id FROM contract_read_models WHERE id = 'c-price-legacy'`).Scan(&priceID); err != nil {
		t.Fatal(err)
	}
	if priceID != "price-original" {
		t.Errorf("price_id = %q, want unchanged 'price-original' (legacy payload upcasts new_price_id to empty string)", priceID)
	}

	var policy string
	var hasOldID, hasNewID bool
	if err := pool.QueryRow(ctx,
		`SELECT data->>'policy', data ? 'old_price_id', data ? 'new_price_id'
		 FROM contract_read_models WHERE id = 'c-price-legacy'`,
	).Scan(&policy, &hasOldID, &hasNewID); err != nil {
		t.Fatalf("query projected data: %v", err)
	}
	if policy != "immediate" {
		t.Errorf("data.policy = %q, want 'immediate' (upcaster default for legacy events)", policy)
	}
	if !hasOldID || !hasNewID {
		t.Errorf("data missing upcasted old_price_id/new_price_id fields: hasOldID=%v hasNewID=%v", hasOldID, hasNewID)
	}
}
