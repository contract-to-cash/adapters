package mysql_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/contract-to-cash/adapters/mysql"
	"github.com/contract-to-cash/adapters/mysql/mysqltest"
	"github.com/contract-to-cash/core/application/tx"
	"github.com/contract-to-cash/core/domain/balance"
	"github.com/contract-to-cash/core/domain/invoice"
	"github.com/contract-to-cash/core/domain/payment"
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

// A contract.created event reusing an idempotency key trips migration 009's
// ux_contract_idempotency_key UNIQUE index and must surface as ErrCodeConflict
// (a creation conflict), NOT version_conflict. Distinct keys are unaffected, and
// historical events with no key are exempt (the generated column is NULL, and a
// UNIQUE index permits multiple NULLs). This is the case sqlmock cannot catch:
// real MySQL evaluates the STORED generated column and produces the 1062.
func TestIntegration_EventStore_ContractIdempotencyKeyUniqueness(t *testing.T) {
	db := mysqltest.NewDB(t)
	store := mysql.New(db, integrationClock)
	ctx := context.Background()

	created := func(key string) json.RawMessage {
		return json.RawMessage(fmt.Sprintf(`{"contract_id":"x","account_id":"a","idempotency_key":%q}`, key))
	}

	// First contract.created with a non-empty key succeeds.
	ev1 := eventstore.Event{ID: "idem-a", Type: "contract.created", SchemaVersion: 3, Data: created("dup-key"), OccurredAt: time.Now().UTC()}
	if err := store.Append(ctx, "c-idem-a", []eventstore.Event{ev1}, 0); err != nil {
		t.Fatalf("first Append: %v", err)
	}

	// A DIFFERENT stream reusing the same key is rejected as a conflict.
	ev2 := eventstore.Event{ID: "idem-b", Type: "contract.created", SchemaVersion: 3, Data: created("dup-key"), OccurredAt: time.Now().UTC()}
	err := store.Append(ctx, "c-idem-b", []eventstore.Event{ev2}, 0)
	if isDomainError(err, shared.ErrCodeVersionConflict) {
		t.Fatalf("idempotency conflict must NOT be version_conflict: %v", err)
	}
	if !isDomainError(err, shared.ErrCodeConflict) {
		t.Fatalf("expected conflict DomainError, got %v", err)
	}

	// A distinct key is fine.
	ev3 := eventstore.Event{ID: "idem-c", Type: "contract.created", SchemaVersion: 3, Data: created("other-key"), OccurredAt: time.Now().UTC()}
	if err := store.Append(ctx, "c-idem-c", []eventstore.Event{ev3}, 0); err != nil {
		t.Fatalf("distinct key Append: %v", err)
	}

	// Historical events with NO idempotency_key are exempt: two coexist.
	for _, s := range []struct{ id, stream string }{{"legacy-a", "c-legacy-a"}, {"legacy-b", "c-legacy-b"}} {
		ev := eventstore.Event{ID: s.id, Type: "contract.created", SchemaVersion: 2, Data: json.RawMessage(`{"contract_id":"x","account_id":"a"}`), OccurredAt: time.Now().UTC()}
		if err := store.Append(ctx, s.stream, []eventstore.Event{ev}, 0); err != nil {
			t.Fatalf("legacy Append %s: %v", s.id, err)
		}
	}
}

// FindExpired returns non-consumed entries whose expiry has passed as of asOf,
// ordered FIFO, feeding the core BalanceExpirationProcessor. Mirrors the
// inmemory reference: fully-consumed and not-yet-expired and no-expiry entries
// are excluded.
func TestIntegration_BalanceRepo_FindExpired(t *testing.T) {
	db := mysqltest.NewDB(t)
	repo := mysql.NewBalanceRepository(db)
	ctx := context.Background()

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	asOf := base.Add(30 * 24 * time.Hour)
	past := base.Add(24 * time.Hour)
	future := asOf.Add(24 * time.Hour)

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
	mk("exp-old", 500, &past, time.Hour)
	mk("exp-new", 700, &past, 2*time.Hour)
	mk("exp-consumed", 0, &past, 3*time.Hour)
	mk("live", 900, &future, 4*time.Hour)
	mk("no-expiry", 900, nil, 5*time.Hour)

	got, err := repo.FindExpired(ctx, asOf, 0) // 0 = unbounded
	if err != nil {
		t.Fatalf("FindExpired: %v", err)
	}
	if len(got) != 2 || got[0].ToSnapshot().ID != "exp-old" || got[1].ToSnapshot().ID != "exp-new" {
		t.Fatalf("expected [exp-old exp-new], got %+v", got)
	}

	// limit bounds the result to the oldest-created eligible entry (core#197).
	limited, err := repo.FindExpired(ctx, asOf, 1)
	if err != nil {
		t.Fatalf("FindExpired(limit=1): %v", err)
	}
	if len(limited) != 1 || limited[0].ToSnapshot().ID != "exp-old" {
		t.Fatalf("expected [exp-old] with limit=1, got %+v", limited)
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
	repo := mysql.NewInvoiceRepository(db, integrationClock)
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

// Issue #36 (parity with postgres fixes_test.go
// TestInvoiceRepo_ConcurrentFinalize_OneLoserRejected): a real-DB race, not just
// an sqlmock SQL-text assertion. Two transactions race to finalize the same
// draft invoice, reproducing core's BillingService.FinalizeInvoice (read ->
// Finalize -> Save inside one tx). Because FindByID issues SELECT ... FOR UPDATE
// inside a tx, the loser blocks until the winner commits, then reads the
// already-finalized row and is rejected by Finalize() with
// invalid_state_transition. Exactly one may win — the substrate guarantee that
// lets core fire OnInvoiceIssuedHook at most once per invoice. Under InnoDB
// REPEATABLE READ the locking read is what serializes the two finalizers.
func TestIntegration_InvoiceRepo_ConcurrentFinalize_OneLoserRejected(t *testing.T) {
	db := mysqltest.NewDB(t)
	repo := mysql.NewInvoiceRepository(db, integrationClock)
	ctx := context.Background()

	insertContractReadModel(t, db, "c-fin", "acc-fin")

	draft, err := invoice.InvoiceFromSnapshot(invoice.InvoiceSnapshot{
		ID: "inv-fin", InvoiceNumber: "INV-FIN",
		AccountID: "acc-fin", ContractID: "c-fin",
		Status:   invoice.InvoiceStatusDraft,
		Subtotal: jpy(10000), TaxAmount: jpy(1000),
		DiscountAmount: jpy(0), Total: jpy(11000),
		AppliedBalance: jpy(0), AmountDue: jpy(11000),
		PaidAmount: jpy(0), Balance: jpy(0),
	})
	if err != nil {
		t.Fatalf("InvoiceFromSnapshot: %v", err)
	}
	if err := repo.Save(ctx, draft); err != nil {
		t.Fatalf("Save draft: %v", err)
	}

	// finalizeInTx reproduces FinalizeInvoice's tx body against the adapter.
	// afterRead (if non-nil) runs after FindByID but before Finalize/Save, so the
	// caller can hold an open transaction while the other racer reads.
	finalizeInTx := func(afterRead func()) error {
		sqlTx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		txCtx := mysql.ContextWithTx(ctx, sqlTx)
		loaded, err := repo.FindByID(txCtx, "inv-fin") // SELECT ... FOR UPDATE
		if err != nil {
			_ = sqlTx.Rollback()
			return err
		}
		if afterRead != nil {
			afterRead()
		}
		if err := loaded.Finalize(); err != nil {
			_ = sqlTx.Rollback()
			return err
		}
		if err := repo.Save(txCtx, loaded); err != nil {
			_ = sqlTx.Rollback()
			return err
		}
		return sqlTx.Commit()
	}

	// Deterministically force the race: goroutine A reads (taking the row lock),
	// signals aRead, then lingers before writing. Goroutine B waits for aRead,
	// then runs its full tx. Under the FOR UPDATE lock, B's FindByID blocks until
	// A commits and then sees the finalized row (rejected). Without the lock, B
	// would read the still-draft row and both would finalize.
	var (
		wg    sync.WaitGroup
		aRead = make(chan struct{})
		errs  [2]error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		errs[0] = finalizeInTx(func() {
			close(aRead)
			time.Sleep(300 * time.Millisecond)
		})
	}()
	go func() {
		defer wg.Done()
		<-aRead
		errs[1] = finalizeInTx(nil)
	}()
	wg.Wait()

	successes, rejected := 0, 0
	for _, e := range errs {
		switch {
		case e == nil:
			successes++
		case isDomainError(e, shared.ErrCodeInvalidStateTransition):
			rejected++
		default:
			t.Fatalf("unexpected finalize error: %v", e)
		}
	}
	if successes != 1 || rejected != 1 {
		t.Fatalf("expected exactly one success and one invalid_state_transition, got %d success / %d rejected", successes, rejected)
	}

	// The persisted invoice must be finalized exactly once, and its history must
	// remain consistent: exactly one open row (valid_to IS NULL).
	final, err := repo.FindByID(ctx, "inv-fin")
	if err != nil {
		t.Fatalf("FindByID after race: %v", err)
	}
	if final.ToSnapshot().Status != invoice.InvoiceStatusFinalized {
		t.Errorf("final status = %q, want finalized", final.ToSnapshot().Status)
	}
	var openRows int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM invoice_history WHERE id = 'inv-fin' AND valid_to IS NULL`).Scan(&openRows); err != nil {
		t.Fatalf("count open history rows: %v", err)
	}
	if openRows != 1 {
		t.Errorf("expected exactly 1 open invoice_history row, got %d", openRows)
	}
}

// Issue #30: option-1 optimistic locking against real MySQL. Two loads observe
// the same lock_version, both finalize, and Save sequentially. The first wins;
// the second's guarded UPDATE matches no row and the existence probe finds the
// row, so Save returns tx.ErrVersionConflict (not last-writer-wins). The winner's
// state is durably persisted; the retry path (re-load, mutate, Save) still works
// and invoice_history stays consistent.
func TestIntegration_InvoiceRepo_OptimisticLockConflict(t *testing.T) {
	db := mysqltest.NewDB(t)
	repo := mysql.NewInvoiceRepository(db, integrationClock)
	ctx := context.Background()

	insertContractReadModel(t, db, "c-ol", "acc-ol")

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
	if reloaded.ToSnapshot().Status != invoice.InvoiceStatusFinalized {
		t.Errorf("status = %s, want finalized", reloaded.ToSnapshot().Status)
	}
	if reloaded.Version() != 1 {
		t.Errorf("persisted lock_version = %d, want 1", reloaded.Version())
	}
	// The retry path must still succeed (the guard does not wedge a legitimate
	// follow-up write), and history stays consistent: exactly one open row.
	if err := reloaded.VoidWithReason("superseded"); err != nil {
		t.Fatalf("void reloaded: %v", err)
	}
	if err := repo.Save(ctx, reloaded); err != nil {
		t.Fatalf("retry Save after reload: %v", err)
	}
	var openRows int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM invoice_history WHERE id = 'inv-ol' AND valid_to IS NULL`).Scan(&openRows); err != nil {
		t.Fatalf("count open history rows: %v", err)
	}
	if openRows != 1 {
		t.Errorf("expected exactly 1 open invoice_history row, got %d", openRows)
	}
}

// Issue #45: the per-(contract_id, billing_period) uniqueness backstop against
// real MySQL, enforced by the generated period_uniq_key column + ux_invoice_period
// unique index (migration 011). A second distinct non-voided, non-proration
// invoice for the same period is rejected with ErrCodeConflict, while proration,
// zero-period, and void-and-recreate replacements all coexist.
func TestIntegration_InvoiceRepo_PeriodUniqueness(t *testing.T) {
	db := mysqltest.NewDB(t)
	repo := mysql.NewInvoiceRepository(db, integrationClock)
	ctx := context.Background()

	insertContractReadModel(t, db, "c-pu", "acc-pu")

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
	if err := repo.Save(ctx, mk("inv-p2", invoice.InvoiceStatusFinalized, period, nil)); !isDomainError(err, shared.ErrCodeConflict) {
		t.Fatalf("second period invoice: expected ErrCodeConflict, got %v", err)
	}
	if err := repo.Save(ctx, mk("inv-pro", invoice.InvoiceStatusFinalized, period,
		map[string]string{invoice.MetadataKeyInvoiceType: invoice.InvoiceTypeProration})); err != nil {
		t.Fatalf("proration invoice should coexist: %v", err)
	}
	if err := repo.Save(ctx, mk("inv-z1", invoice.InvoiceStatusFinalized, shared.DateRange{}, nil)); err != nil {
		t.Fatalf("zero-period invoice 1: %v", err)
	}
	if err := repo.Save(ctx, mk("inv-z2", invoice.InvoiceStatusFinalized, shared.DateRange{}, nil)); err != nil {
		t.Fatalf("zero-period invoice 2: %v", err)
	}

	// Void-and-recreate: void the original, then a regeneration replacement for the
	// SAME period succeeds because the voided original drops out of the index.
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

// Issue #36: a non-tx Save must be atomic and leave invoice_history consistent —
// after each Save there is exactly one open history row (valid_to IS NULL) and
// every prior version is closed. This exercises the Save-local transaction that
// wraps the three writes when no ambient transaction is present.
func TestIntegration_InvoiceRepo_NonTxSaveHistoryConsistent(t *testing.T) {
	db := mysqltest.NewDB(t)
	repo := mysql.NewInvoiceRepository(db, integrationClock)
	ctx := context.Background()

	insertContractReadModel(t, db, "c-inv", "acc-inv")

	// Three sequential non-tx saves (draft -> issued -> paid).
	for _, st := range []invoice.InvoiceStatus{
		invoice.InvoiceStatusDraft, invoice.InvoiceStatusIssued, invoice.InvoiceStatusPaid,
	} {
		if err := repo.Save(ctx, mustInvoice(t, st, 1000)); err != nil {
			t.Fatalf("Save %s: %v", st, err)
		}
	}

	// Exactly one open history row must remain.
	var openRows int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM invoice_history WHERE id = 'inv-int' AND valid_to IS NULL`).Scan(&openRows); err != nil {
		t.Fatalf("count open history rows: %v", err)
	}
	if openRows != 1 {
		t.Fatalf("expected exactly 1 open invoice_history row after non-tx saves, got %d", openRows)
	}

	// The open row must reflect the latest state.
	current, err := repo.FindByID(ctx, "inv-int")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if current.ToSnapshot().Status != invoice.InvoiceStatusPaid {
		t.Errorf("current status = %q, want paid", current.ToSnapshot().Status)
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

	// Same idempotency_key, DIFFERENT id: must surface as the core's
	// DomainError(duplicate_request), matching the in-memory reference
	// implementation (issue #38) — not a silent no-op.
	dupKey := mustUsageRecord(t, "ur-int-2", "idem-int")
	err := repo.Record(ctx, dupKey)
	if err == nil {
		t.Fatal("expected duplicate idempotency_key to return an error, got nil")
	}
	if !isDomainError(err, shared.ErrCodeDuplicateRequest) {
		t.Errorf("expected DomainError(%s), got %T: %v", shared.ErrCodeDuplicateRequest, err, err)
	}

	// Same id (PRIMARY KEY collision): a real fault that must surface — as a
	// plain wrapped error, NOT the duplicate_request sentinel.
	dupID := mustUsageRecord(t, "ur-int-1", "idem-different")
	err = repo.Record(ctx, dupID)
	if err == nil {
		t.Fatal("duplicate id must surface as an error, got nil")
	}
	if isDomainError(err, shared.ErrCodeDuplicateRequest) {
		t.Errorf("duplicate id must not be reported as duplicate_request, got: %v", err)
	}

	// Records without an idempotency key are never deduplicated (NULLs are
	// distinct), matching inmemory.
	for _, id := range []string{"ur-int-nokey-1", "ur-int-nokey-2"} {
		rk := mustUsageRecord(t, id, "")
		if err := repo.Record(ctx, rk); err != nil {
			t.Fatalf("Record %s (no key): %v", id, err)
		}
	}

	// Exactly three rows must have persisted (ur-int-1 + the two keyless ones);
	// both duplicate writes were rejected.
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM usage_records WHERE contract_id = 'c-usage'`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 usage records, got %d", count)
	}
}

// Two payments with the SAME idempotency_key but DIFFERENT ids must not corrupt
// each other: the loser's INSERT trips the idempotency_key UNIQUE index and Save
// must translate it to payment.ErrDuplicateIdempotencyKey (issue #35 / core#97),
// leaving the winner's row untouched. A same-id re-save must still update in
// place.
func TestIntegration_PaymentRepo_DuplicateIdempotencyKey(t *testing.T) {
	db := mysqltest.NewDB(t)
	repo := mysql.NewPaymentRepository(db)
	ctx := context.Background()

	insertContractReadModel(t, db, "c-pay-idem", "acc-pay-idem")
	if _, err := db.ExecContext(ctx,
		`INSERT INTO invoices (id, account_id, contract_id, status, subtotal, tax_amount, discount_amount, total, applied_balance, amount_due, paid_amount, balance, currency, void_reason, line_items, metadata)
		 VALUES ('inv-pay-idem', 'acc-pay-idem', 'c-pay-idem', 'issued', 0,0,0,0,0,0,0,0,'JPY','', '[]','{}')`); err != nil {
		t.Fatalf("seed invoice: %v", err)
	}

	winner := mustPayment(t, "pay-win", "inv-pay-idem", "idem-collide", payment.PaymentStatusCompleted, 5000)
	if err := repo.Save(ctx, winner); err != nil {
		t.Fatalf("Save winner: %v", err)
	}

	// Same idempotency_key, DIFFERENT id: must surface the core sentinel.
	loser := mustPayment(t, "pay-lose", "inv-pay-idem", "idem-collide", payment.PaymentStatusFailed, 9999)
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

	// The winner's row must be intact — NOT overwritten with the loser's fields.
	got, err := repo.FindByID(ctx, "pay-win")
	if err != nil {
		t.Fatalf("FindByID winner: %v", err)
	}
	ws := got.ToSnapshot()
	if ws.Status != payment.PaymentStatusCompleted || ws.Amount.Int64() != 5000 {
		t.Errorf("winner corrupted: status=%q amount=%d (want completed/5000)", ws.Status, ws.Amount.Int64())
	}

	// Exactly one row must own the key.
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM payments WHERE idempotency_key = 'idem-collide'`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 payment for the key, got %d", count)
	}

	// Same id (the 3DS upgrade path): re-saving pay-win must update in place.
	upgraded := mustPayment(t, "pay-win", "inv-pay-idem", "idem-collide", payment.PaymentStatusRefunded, 5000)
	if err := repo.Save(ctx, upgraded); err != nil {
		t.Fatalf("same-id re-save should update, got: %v", err)
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
		ProcessedAt:    integrationClock.Now(),
	})
	if err != nil {
		t.Fatalf("payment.FromSnapshot: %v", err)
	}
	return p
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
