package mysql_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/contract-to-cash/adapters/mysql"
	"github.com/contract-to-cash/adapters/mysql/mysqltest"
	"github.com/contract-to-cash/core/domain/invoice"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/contract-to-cash/core/domain/usage"
	"github.com/contract-to-cash/core/eventstore"
)

// These tests run against a real MySQL 8 instance. They are SKIPPED when no
// database is reachable via the implicit default DSN and FAIL when
// ADAPTERS_TEST_MYSQL_DSN is set explicitly but unreachable (see
// mysqltest.NewDB). CI provides a mysql:8 service container.

var integrationClock = shared.FixedClock{FixedTime: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)}

func isDomainError(err error, code shared.ErrorCode) bool {
	var de *shared.DomainError
	return errors.As(err, &de) && de.Code == code
}

func jpy(amount int64) shared.Money {
	return shared.NewMoney(big.NewRat(amount, 1), "JPY")
}

// insertContractReadModel seeds a minimal parent row so FK-constrained inserts
// (invoices, usage_records, ...) succeed.
func insertContractReadModel(t *testing.T, db *sql.DB, id, accountID string) {
	t.Helper()
	_, err := db.ExecContext(context.Background(),
		`INSERT INTO contract_read_models (id, account_id, status, data) VALUES (?, ?, 'active', '{}')`,
		id, accountID)
	if err != nil {
		t.Fatalf("seed contract_read_models: %v", err)
	}
}

func TestIntegration_EventStore_AppendAndLoad(t *testing.T) {
	db := mysqltest.NewDB(t)
	store := mysql.New(db, integrationClock)
	ctx := context.Background()

	events := []eventstore.Event{
		{ID: "evt-1", Type: "test.created", SchemaVersion: 1, Data: json.RawMessage(`{"name":"test"}`), OccurredAt: time.Now().UTC().Truncate(time.Microsecond)},
		{ID: "evt-2", Type: "test.updated", SchemaVersion: 1, Data: json.RawMessage(`{"name":"updated"}`), OccurredAt: time.Now().UTC().Truncate(time.Microsecond)},
	}
	if err := store.Append(ctx, "stream-1", events, 0); err != nil {
		t.Fatalf("Append: %v", err)
	}

	loaded, err := store.Load(ctx, "stream-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("got %d events, want 2", len(loaded))
	}
	if loaded[0].Version != 1 || loaded[1].Version != 2 {
		t.Errorf("versions = %d,%d want 1,2", loaded[0].Version, loaded[1].Version)
	}
	if loaded[0].GlobalPosition == 0 || loaded[1].GlobalPosition <= loaded[0].GlobalPosition {
		t.Errorf("global positions not strictly increasing: %d,%d", loaded[0].GlobalPosition, loaded[1].GlobalPosition)
	}
}

func TestIntegration_EventStore_AppendVersionConflict(t *testing.T) {
	db := mysqltest.NewDB(t)
	store := mysql.New(db, integrationClock)
	ctx := context.Background()

	first := []eventstore.Event{{ID: "vc-1", Type: "test.created", SchemaVersion: 1, Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC()}}
	if err := store.Append(ctx, "stream-vc", first, 0); err != nil {
		t.Fatalf("first Append: %v", err)
	}
	second := []eventstore.Event{{ID: "vc-2", Type: "test.created", SchemaVersion: 1, Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC()}}
	err := store.Append(ctx, "stream-vc", second, 0) // stale expectedVersion
	if !isDomainError(err, shared.ErrCodeVersionConflict) {
		t.Fatalf("expected version_conflict, got %v", err)
	}
}

// The core regression for issue #17: a duplicate event ID (uq_event_id, error
// 1062) must surface as a non-retryable conflict, NOT version_conflict. This is
// exactly the case sqlmock could not catch, since real MySQL produces the 1062
// with its key name in the message.
func TestIntegration_EventStore_DuplicateEventIDIsNotVersionConflict(t *testing.T) {
	db := mysqltest.NewDB(t)
	store := mysql.New(db, integrationClock)
	ctx := context.Background()

	ev := eventstore.Event{ID: "dup-id", Type: "test.created", SchemaVersion: 1, Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC()}
	if err := store.Append(ctx, "stream-dup-a", []eventstore.Event{ev}, 0); err != nil {
		t.Fatalf("first Append: %v", err)
	}

	// Same ID on a DIFFERENT stream: COUNT precheck passes (new stream is at 0)
	// but the INSERT trips uq_event_id.
	dup := eventstore.Event{ID: "dup-id", Type: "test.created", SchemaVersion: 1, Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC()}
	err := store.Append(ctx, "stream-dup-b", []eventstore.Event{dup}, 0)
	if err == nil {
		t.Fatal("expected error from duplicate event ID, got nil")
	}
	if isDomainError(err, shared.ErrCodeVersionConflict) {
		t.Fatalf("duplicate event ID must NOT be version_conflict: %v", err)
	}
	if !isDomainError(err, shared.ErrCodeConflict) {
		t.Fatalf("expected conflict DomainError, got %v", err)
	}

	// The failed append must not have persisted anything on stream-dup-b.
	loaded, err := store.Load(ctx, "stream-dup-b")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 0 {
		t.Errorf("expected 0 events on stream-dup-b, got %d", len(loaded))
	}
}

func TestIntegration_EventStore_SnapshotRoundTrip(t *testing.T) {
	db := mysqltest.NewDB(t)
	store := mysql.New(db, integrationClock)
	ctx := context.Background()

	asOf := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.SaveSnapshot(ctx, eventstore.Snapshot{
		StreamID: "stream-snap", Version: 5, State: json.RawMessage(`{"status":"active"}`), AsOf: asOf,
	}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	loaded, err := store.LoadSnapshot(ctx, "stream-snap")
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if loaded == nil || loaded.Version != 5 {
		t.Fatalf("unexpected snapshot: %+v", loaded)
	}
	var state map[string]string
	if err := json.Unmarshal(loaded.State, &state); err != nil {
		t.Fatalf("unmarshal state: %v", err)
	}
	if state["status"] != "active" {
		t.Errorf("state[status] = %q, want active", state["status"])
	}
}

// Rebuild empties and repopulates contract_read_models. Migration 008 removed
// every FK referencing contract_read_models, so Rebuild no longer suspends
// foreign_key_checks (issue #29): the truncate/reload runs as an ordinary
// transaction and needs no dedicated-connection restore dance. This test asserts
// Rebuild succeeds, clears the projection, and — crucially — that it does NOT
// leave foreign_key_checks disabled on pooled connections (a regression the old
// toggle code guarded against). Because there is no longer an
// invoices→contract_read_models FK, an orphan invoice is legitimately accepted
// by design; the connection-poisoning regression is instead detected via a
// self-referential FK on invoice_history (invoice_history.id → invoices.id),
// which is untouched by 008 and must still be enforced afterwards.
func TestIntegration_ContractProjector_Rebuild(t *testing.T) {
	db := mysqltest.NewDB(t)
	es := mysql.New(db, integrationClock)
	cp := mysql.NewCheckpointStore(db)
	proj := mysql.NewContractProjector(db, es, cp)
	ctx := context.Background()

	// Seed a read-model row that Rebuild will clear.
	insertContractReadModel(t, db, "c-rebuild", "acc-rebuild")

	if err := proj.Rebuild(ctx, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}

	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM contract_read_models`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 read models after rebuild, got %d", count)
	}

	// Post-008 an orphan invoice (no matching contract_read_models row) is
	// allowed: the write→projection FK was intentionally dropped so async
	// projection lag cannot reject legitimate writes.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO invoices (id, account_id, contract_id, status, subtotal, tax_amount, discount_amount, total, applied_balance, amount_due, paid_amount, balance, currency, void_reason, line_items, metadata)
		 VALUES ('inv-orphan', 'acc-x', 'missing-contract', 'draft', 0,0,0,0,0,0,0,0,'JPY','', '[]','{}')`); err != nil {
		t.Errorf("orphan invoice insert should be allowed after migration 008 dropped the projection FK: %v", err)
	}

	// A still-live FK (invoice_history.id → invoices.id) must remain enforced on
	// pooled connections: inserting a history row for a non-existent invoice must
	// be rejected. If Rebuild had left foreign_key_checks disabled on a pooled
	// connection, this violation would be silently accepted.
	_, err := db.ExecContext(ctx,
		`INSERT INTO invoice_history (id, version, snapshot, valid_from)
		 VALUES ('inv-missing', 1, '{}', NOW(6))`)
	if err == nil {
		t.Error("expected FK violation for orphan invoice_history row (foreign_key_checks left disabled?)")
	}
}

// Round-trip through the invoice repository, and assert the bitemporal history
// close/open share one instant (issue #17 NIT): after a re-save, the closed
// row's valid_to must exactly equal the reopened row's valid_from, leaving no
// gap for FindByIDAsOf to fall through.
func TestIntegration_InvoiceRepo_SaveFindAndHistoryContiguity(t *testing.T) {
	db := mysqltest.NewDB(t)
	repo := mysql.NewInvoiceRepository(db)
	ctx := context.Background()

	insertContractReadModel(t, db, "c-inv", "acc-inv")

	inv := mustInvoice(t, invoice.InvoiceStatusIssued, 1000)
	if err := repo.Save(ctx, inv); err != nil {
		t.Fatalf("Save v1: %v", err)
	}

	found, err := repo.FindByID(ctx, "inv-int")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if found.ToSnapshot().Total.Int64() != 1100 {
		t.Errorf("Total = %d, want 1100", found.ToSnapshot().Total.Int64())
	}

	// Re-save (new version) to close the first history row and open a second.
	inv2 := mustInvoice(t, invoice.InvoiceStatusPaid, 1000)
	if err := repo.Save(ctx, inv2); err != nil {
		t.Fatalf("Save v2: %v", err)
	}

	var closedValidTo, openValidFrom time.Time
	if err := db.QueryRowContext(ctx,
		`SELECT valid_to FROM invoice_history WHERE id = 'inv-int' AND valid_to IS NOT NULL ORDER BY version DESC LIMIT 1`).Scan(&closedValidTo); err != nil {
		t.Fatalf("read closed valid_to: %v", err)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT valid_from FROM invoice_history WHERE id = 'inv-int' AND valid_to IS NULL`).Scan(&openValidFrom); err != nil {
		t.Fatalf("read open valid_from: %v", err)
	}
	if !closedValidTo.Equal(openValidFrom) {
		t.Errorf("history gap: closed valid_to %v != open valid_from %v", closedValidTo, openValidFrom)
	}

	// FindByIDAsOf exactly at the boundary must resolve to the latest revision.
	asOf, err := repo.FindByIDAsOf(ctx, "inv-int", openValidFrom)
	if err != nil {
		t.Fatalf("FindByIDAsOf at boundary: %v", err)
	}
	if asOf.ToSnapshot().Status != invoice.InvoiceStatusPaid {
		t.Errorf("as-of status = %q, want paid", asOf.ToSnapshot().Status)
	}
}

func TestIntegration_UsageRepo_IdempotencyKeySemantics(t *testing.T) {
	db := mysqltest.NewDB(t)
	repo := mysql.NewUsageRepository(db)
	ctx := context.Background()

	insertContractReadModel(t, db, "c-usage", "acc-usage")

	rec := mustUsageRecord(t, "ur-int-1", "idem-int")
	if err := repo.Record(ctx, rec); err != nil {
		t.Fatalf("Record: %v", err)
	}

	// Same idempotency_key, DIFFERENT id: idempotent no-op (must NOT error).
	dupKey := mustUsageRecord(t, "ur-int-2", "idem-int")
	if err := repo.Record(ctx, dupKey); err != nil {
		t.Fatalf("duplicate idempotency_key should be a no-op, got: %v", err)
	}

	// Same id (PRIMARY KEY collision): a real fault that must surface.
	dupID := mustUsageRecord(t, "ur-int-1", "idem-different")
	if err := repo.Record(ctx, dupID); err == nil {
		t.Fatal("duplicate id must surface as an error, got nil")
	}

	// Exactly one row must have persisted (only ur-int-1).
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM usage_records WHERE contract_id = 'c-usage'`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 usage record, got %d", count)
	}
}

func mustInvoice(t *testing.T, status invoice.InvoiceStatus, subtotal int64) *invoice.Invoice {
	t.Helper()
	inv, err := invoice.InvoiceFromSnapshot(invoice.InvoiceSnapshot{
		ID: "inv-int", InvoiceNumber: "INV-INT", AccountID: "acc-inv", ContractID: "c-inv",
		Status:         status,
		Subtotal:       jpy(subtotal),
		TaxAmount:      jpy(100),
		DiscountAmount: jpy(0),
		Total:          jpy(subtotal + 100),
		AppliedBalance: jpy(0),
		AmountDue:      jpy(subtotal + 100),
		PaidAmount:     jpy(0),
		Balance:        jpy(subtotal + 100),
	})
	if err != nil {
		t.Fatalf("InvoiceFromSnapshot: %v", err)
	}
	return inv
}

func mustUsageRecord(t *testing.T, id, idempotencyKey string) *usage.UsageRecord {
	t.Helper()
	rec, err := usage.FromSnapshot(usage.UsageRecordSnapshot{
		ID:             shared.UsageRecordID(id),
		ContractID:     "c-usage",
		MetricName:     "api_calls",
		Quantity:       100,
		Timestamp:      integrationClock.Now(),
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		t.Fatalf("usage.FromSnapshot: %v", err)
	}
	return rec
}
