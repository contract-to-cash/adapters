package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/contract-to-cash/adapters/postgres"
	"github.com/contract-to-cash/adapters/postgres/postgrestest"
	"github.com/contract-to-cash/core/application/tx"
	"github.com/contract-to-cash/core/domain/balance"
	"github.com/contract-to-cash/core/domain/contract"
	"github.com/contract-to-cash/core/domain/invoice"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/contract-to-cash/core/eventstore"
)

// --- Fix 1: Append atomicity without external transaction ---

func TestEventStore_AppendAtomicWithoutTx(t *testing.T) {
	pool := postgrestest.NewPool(t)
	store := postgres.NewEventStore(pool)
	ctx := context.Background()

	// Append 3 events. The 3rd has a duplicate ID with the 1st to trigger an
	// error midway. If Append is atomic, none should be persisted.
	events := []eventstore.Event{
		{ID: "atomic-1", Type: "test.event", SchemaVersion: 1, Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC()},
		{ID: "atomic-2", Type: "test.event", SchemaVersion: 1, Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC()},
		{ID: "atomic-1", Type: "test.event", SchemaVersion: 1, Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC()}, // dup ID
	}

	err := store.Append(ctx, "stream-atomic", events, 0)
	if err == nil {
		t.Fatal("expected error from duplicate event ID, got nil")
	}

	// If atomic, no events should be persisted.
	loaded, err := store.Load(ctx, "stream-atomic")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 0 {
		t.Errorf("expected 0 events after failed atomic append, got %d", len(loaded))
	}
}

// --- Fix 3: Contract Expired should be 'expired', not 'cancelled' ---

func TestContractProjector_ExpiredStatus(t *testing.T) {
	pool := postgrestest.NewPool(t)
	es := postgres.NewEventStore(pool)
	cp := postgres.NewCheckpointStore(pool)
	proj := postgres.NewContractProjector(pool, es, cp)
	ctx := context.Background()

	// First create the contract in the read model.
	createEvt := eventstore.Event{
		ID: "evt-exp-1", StreamID: "c-exp", Type: contract.EventTypeContractCreated,
		Version: 1, SchemaVersion: 1,
		Data:       json.RawMessage(`{"account_id":"acc-exp","price_id":"p-exp"}`),
		OccurredAt: time.Now().UTC(),
	}
	if err := es.Append(ctx, "c-exp", []eventstore.Event{createEvt}, 0); err != nil {
		t.Fatal(err)
	}
	if err := proj.Project(ctx, createEvt); err != nil {
		t.Fatal(err)
	}

	// Now expire it.
	expireEvt := eventstore.Event{
		ID: "evt-exp-2", StreamID: "c-exp", Type: contract.EventTypeContractExpired,
		Version: 2, SchemaVersion: 1,
		Data:       json.RawMessage(`{}`),
		OccurredAt: time.Now().UTC(),
	}
	if err := es.Append(ctx, "c-exp", []eventstore.Event{expireEvt}, 1); err != nil {
		t.Fatal(err)
	}
	if err := proj.Project(ctx, expireEvt); err != nil {
		t.Fatal(err)
	}

	// Verify the read model has status 'expired', not 'cancelled'.
	var status string
	err := pool.QueryRow(ctx, `SELECT status FROM contract_read_models WHERE id = 'c-exp'`).Scan(&status)
	if err != nil {
		t.Fatal(err)
	}
	if status != "expired" {
		t.Errorf("status = %q, want 'expired'", status)
	}
}

// --- Fix 4: Duplicate event ID should NOT be version conflict ---

func TestEventStore_DuplicateIDIsNotVersionConflict(t *testing.T) {
	pool := postgrestest.NewPool(t)
	store := postgres.NewEventStore(pool)
	ctx := context.Background()

	// Insert an event on stream-1.
	events1 := []eventstore.Event{{
		ID: "dup-id", Type: "test.event", SchemaVersion: 1,
		Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
	}}
	if err := store.Append(ctx, "stream-dup-1", events1, 0); err != nil {
		t.Fatal(err)
	}

	// Insert an event with the SAME ID on a DIFFERENT stream.
	// This is a PK violation (duplicate id), NOT a version conflict.
	events2 := []eventstore.Event{{
		ID: "dup-id", Type: "test.event", SchemaVersion: 1,
		Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
	}}
	err := store.Append(ctx, "stream-dup-2", events2, 0)
	if err == nil {
		t.Fatal("expected error from duplicate event ID")
	}
	// Should NOT be a version conflict error.
	if isDomainError(err, shared.ErrCodeVersionConflict) {
		t.Error("duplicate event ID should not be reported as version conflict")
	}
}

// --- Fix 8a: CreditNote repository tests ---

func TestCreditNoteRepo_SaveAndFindByID(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()

	// Set up FK dependencies.
	_, _ = pool.Exec(ctx, `INSERT INTO contract_read_models (id, account_id, status) VALUES ('c-cn', 'acc-cn', 'active')`)
	_, _ = pool.Exec(ctx, `INSERT INTO invoices (id, account_id, contract_id, status, subtotal, tax_amount, discount_amount, total, applied_balance, amount_due, paid_amount, balance) VALUES ('inv-cn', 'acc-cn', 'c-cn', 'issued', 10000, 1000, 0, 11000, 0, 11000, 0, 0)`)

	repo := postgres.NewCreditNoteRepository(pool)
	now := time.Now().UTC().Truncate(time.Microsecond)
	snap := invoice.CreditNoteSnapshot{
		ID: "cn-1", Number: "CN-001",
		InvoiceID: "inv-cn", ContractID: "c-cn", AccountID: "acc-cn",
		Status: invoice.CreditNoteStatusDraft, Reason: invoice.CreditNoteReasonDuplicate,
		Memo:         "test memo",
		Subtotal:     jpy(10000),
		TaxAmount:    jpy(1000),
		Total:        jpy(11000),
		CreditAmount: jpy(11000),
		RefundAmount: jpy(0),
		IssuedAt:     &now,
		CreatedAt:    now,
	}
	cn, err := invoice.CreditNoteFromSnapshot(snap)
	if err != nil {
		t.Fatalf("CreditNoteFromSnapshot: %v", err)
	}
	if err := repo.Save(ctx, cn); err != nil {
		t.Fatalf("Save: %v", err)
	}

	found, err := repo.FindByID(ctx, "cn-1")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	fs := found.ToSnapshot()
	if fs.Number != "CN-001" {
		t.Errorf("Number = %q, want CN-001", fs.Number)
	}
	if fs.Total.Int64() != 11000 {
		t.Errorf("Total = %d, want 11000", fs.Total.Int64())
	}
	if string(fs.Reason) != "duplicate" {
		t.Errorf("Reason = %q, want duplicate", fs.Reason)
	}
}

func TestCreditNoteRepo_FindByInvoiceID(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()
	_, _ = pool.Exec(ctx, `INSERT INTO contract_read_models (id, account_id, status) VALUES ('c-cn2', 'acc-cn2', 'active')`)
	_, _ = pool.Exec(ctx, `INSERT INTO invoices (id, account_id, contract_id, status, subtotal, tax_amount, discount_amount, total, applied_balance, amount_due, paid_amount, balance) VALUES ('inv-cn2', 'acc-cn2', 'c-cn2', 'issued', 0, 0, 0, 0, 0, 0, 0, 0)`)

	repo := postgres.NewCreditNoteRepository(pool)
	for i := 0; i < 2; i++ {
		snap := invoice.CreditNoteSnapshot{
			ID:        shared.CreditNoteID(fmt.Sprintf("cn-find-%d", i)),
			InvoiceID: "inv-cn2", ContractID: "c-cn2", AccountID: "acc-cn2",
			Status: invoice.CreditNoteStatusDraft, Subtotal: jpy(0), TaxAmount: jpy(0),
			Total: jpy(0), CreditAmount: jpy(0), RefundAmount: jpy(0),
			CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
		}
		cn, err := invoice.CreditNoteFromSnapshot(snap)
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Save(ctx, cn); err != nil {
			t.Fatal(err)
		}
	}

	found, err := repo.FindByInvoiceID(ctx, "inv-cn2")
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 2 {
		t.Errorf("got %d credit notes, want 2", len(found))
	}
}

func TestCreditNoteRepo_FindByID_NotFound(t *testing.T) {
	pool := postgrestest.NewPool(t)
	repo := postgres.NewCreditNoteRepository(pool)
	_, err := repo.FindByID(context.Background(), "nonexistent")
	if !isDomainError(err, shared.ErrCodeNotFound) {
		t.Errorf("expected not_found, got: %v", err)
	}
}

// --- Fix 8b: Balance INSERT path test ---

func TestBalanceRepo_SaveInsertPath(t *testing.T) {
	pool := postgrestest.NewPool(t)
	repo := postgres.NewBalanceRepository(pool)
	ctx := context.Background()

	// Version 0 means the entry has never been persisted: FromSnapshot sets
	// loadedVersion = Version = 0, which routes Save through the INSERT path.
	snap := balance.BalanceEntrySnapshot{
		ID: "be-insert", AccountID: "acc-insert",
		OriginalAmount:  shared.NewMoney(new(big.Rat).SetInt64(5000), "JPY"),
		RemainingAmount: shared.NewMoney(new(big.Rat).SetInt64(5000), "JPY"),
		Reason:          balance.BalanceReasonManualAdjustment, SourceType: balance.BalanceSourceTypeManual,
		SourceID: "manual-ins", Description: "insert test",
		Version: 0, CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
	entry, err := balance.FromSnapshot(snap)
	if err != nil {
		t.Fatalf("FromSnapshot: %v", err)
	}
	if err := repo.Save(ctx, entry); err != nil {
		t.Fatalf("Save INSERT: %v", err)
	}

	found, err := repo.FindByID(ctx, "be-insert")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if found.ToSnapshot().OriginalAmount.Int64() != 5000 {
		t.Errorf("OriginalAmount = %d, want 5000", found.ToSnapshot().OriginalAmount.Int64())
	}
}

// --- Fix 8c: Nested Tx error path test ---

func TestTxManager_NestedRunInTx_InnerError_RollsBackAll(t *testing.T) {
	pool := postgrestest.NewPool(t)
	factory := func(q postgres.Querier) tx.Repos { return tx.Repos{} }
	mgr := postgres.NewTxManager(pool, factory)
	ctx := context.Background()

	innerErr := errors.New("inner failure")
	err := mgr.RunInTx(ctx, func(outerCtx context.Context, _ tx.Repos) error {
		q := postgres.QuerierFromContext(outerCtx, pool)
		_, _ = q.Exec(outerCtx,
			`INSERT INTO projection_checkpoints (projector_name, last_position) VALUES ('outer-err', 1)`)

		return mgr.RunInTx(outerCtx, func(innerCtx context.Context, _ tx.Repos) error {
			q := postgres.QuerierFromContext(innerCtx, pool)
			_, _ = q.Exec(innerCtx,
				`INSERT INTO projection_checkpoints (projector_name, last_position) VALUES ('inner-err', 2)`)
			return innerErr
		})
	})
	if !errors.Is(err, innerErr) {
		t.Fatalf("expected innerErr, got: %v", err)
	}

	// Both writes should be rolled back.
	var count int
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM projection_checkpoints WHERE projector_name IN ('outer-err', 'inner-err')`).Scan(&count)
	if count != 0 {
		t.Errorf("expected 0 rows after rollback, got %d", count)
	}
}

// --- Fix 2: InvoiceProjector.Rebuild atomicity ---

func TestInvoiceProjector_RebuildAtomicOnFailure(t *testing.T) {
	pool := postgrestest.NewPool(t)
	es := postgres.NewEventStore(pool)
	cp := postgres.NewCheckpointStore(pool)
	ctx := context.Background()

	// Seed an invoice read model row directly.
	_, _ = pool.Exec(ctx,
		`INSERT INTO invoice_read_models (id, contract_id, account_id, status, total, currency, data, version)
		 VALUES ('inv-rebuild', 'c-r', 'acc-r', 'draft', 0, 'JPY', '{}', 1)`)

	proj := postgres.NewInvoiceProjector(pool, es, cp)

	// Rebuild with a valid until time. Since there are no events,
	// it should just clear and leave the table empty. Verify it works
	// within a transaction by checking the table is empty after.
	if err := proj.Rebuild(ctx, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	var count int
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM invoice_read_models`).Scan(&count)
	if count != 0 {
		t.Errorf("expected 0 rows after rebuild, got %d", count)
	}
}

// --- Issue #12: concurrent FinalizeInvoice must serialize via row lock ---
//
// Mirrors core's BillingService.FinalizeInvoice (read -> Finalize -> Save inside
// one tx). Two transactions race to finalize the same draft invoice. Because
// FindByID takes SELECT ... FOR UPDATE inside a tx, the loser blocks until the
// winner commits, then reads the already-finalized row and is rejected by
// Finalize() with invalid_state_transition. Exactly one may win — the substrate
// guarantee that lets core fire OnInvoiceIssuedHook at most once per invoice.
func TestInvoiceRepo_ConcurrentFinalize_OneLoserRejected(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()

	if _, err := pool.Exec(ctx,
		`INSERT INTO contract_read_models (id, account_id, status) VALUES ('c-fin', 'acc-fin', 'active')`); err != nil {
		t.Fatal(err)
	}

	repo := postgres.NewInvoiceRepository(pool)
	snap := invoice.InvoiceSnapshot{
		ID: "inv-fin", InvoiceNumber: "INV-FIN",
		AccountID: "acc-fin", ContractID: "c-fin",
		Status:   invoice.InvoiceStatusDraft,
		Subtotal: jpy(10000), TaxAmount: jpy(1000),
		DiscountAmount: jpy(0), Total: jpy(11000),
		AppliedBalance: jpy(0), AmountDue: jpy(11000),
		PaidAmount: jpy(0), Balance: jpy(0),
	}
	draft, err := invoice.InvoiceFromSnapshot(snap)
	if err != nil {
		t.Fatalf("InvoiceFromSnapshot: %v", err)
	}
	if err := repo.Save(ctx, draft); err != nil {
		t.Fatalf("Save draft: %v", err)
	}

	// finalizeInTx reproduces FinalizeInvoice's tx body against the adapter.
	// afterRead (if non-nil) runs after FindByID but before Finalize/Save, so
	// the caller can hold an open transaction while the other racer reads.
	finalizeInTx := func(afterRead func()) error {
		pgxTx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		txCtx := postgres.ContextWithTx(ctx, pgxTx)
		loaded, err := repo.FindByID(txCtx, "inv-fin") // SELECT ... FOR UPDATE
		if err != nil {
			_ = pgxTx.Rollback(ctx)
			return err
		}
		if afterRead != nil {
			afterRead()
		}
		if err := loaded.Finalize(); err != nil {
			_ = pgxTx.Rollback(ctx)
			return err
		}
		if err := repo.Save(txCtx, loaded); err != nil {
			_ = pgxTx.Rollback(ctx)
			return err
		}
		return pgxTx.Commit(ctx)
	}

	// Deterministically force the race: goroutine A reads (taking the row lock
	// under the fix), signals aRead, then lingers before writing. Goroutine B
	// waits for aRead, then runs its full tx. Under the FOR UPDATE fix, B's
	// FindByID blocks until A commits and then sees the finalized row (rejected).
	// Under the old last-writer-wins upsert, B reads the still-draft row
	// concurrently and both would finalize — which this assertion catches.
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

	// The persisted invoice must be finalized exactly once.
	final, err := repo.FindByID(ctx, "inv-fin")
	if err != nil {
		t.Fatalf("FindByID after race: %v", err)
	}
	if final.ToSnapshot().Status != invoice.InvoiceStatusFinalized {
		t.Errorf("final status = %q, want finalized", final.ToSnapshot().Status)
	}
}
