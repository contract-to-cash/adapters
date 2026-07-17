package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/contract-to-cash/adapters/postgres"
	"github.com/contract-to-cash/adapters/postgres/postgrestest"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/contract-to-cash/core/eventstore"
)

func TestEventStore_AppendAndLoad(t *testing.T) {
	pool := postgrestest.NewPool(t)
	store := postgres.NewEventStore(pool)
	ctx := context.Background()

	events := []eventstore.Event{
		{
			ID:            "evt-1",
			Type:          "test.created",
			Version:       1,
			SchemaVersion: 1,
			Data:          json.RawMessage(`{"name":"test"}`),
			OccurredAt:    time.Now().UTC().Truncate(time.Microsecond),
		},
		{
			ID:            "evt-2",
			Type:          "test.updated",
			Version:       2,
			SchemaVersion: 1,
			Data:          json.RawMessage(`{"name":"updated"}`),
			OccurredAt:    time.Now().UTC().Truncate(time.Microsecond),
		},
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
	if loaded[0].Version != 1 {
		t.Errorf("event[0].Version = %d, want 1", loaded[0].Version)
	}
	if loaded[1].Version != 2 {
		t.Errorf("event[1].Version = %d, want 2", loaded[1].Version)
	}
	if loaded[0].GlobalPosition == 0 {
		t.Error("event[0].GlobalPosition should be > 0")
	}
	if loaded[1].GlobalPosition <= loaded[0].GlobalPosition {
		t.Error("GlobalPosition should be strictly increasing")
	}
}

func TestEventStore_AppendVersionConflict(t *testing.T) {
	pool := postgrestest.NewPool(t)
	store := postgres.NewEventStore(pool)
	ctx := context.Background()

	event1 := []eventstore.Event{{
		ID: "evt-1", Type: "test.created", Version: 1, SchemaVersion: 1,
		Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
	}}
	if err := store.Append(ctx, "stream-1", event1, 0); err != nil {
		t.Fatalf("first Append: %v", err)
	}

	// Attempt to append at the same version should conflict (the version
	// conflict check runs before the contiguity check, so the stamped Version
	// here is irrelevant to what's being asserted).
	event2 := []eventstore.Event{{
		ID: "evt-2", Type: "test.created", Version: 1, SchemaVersion: 1,
		Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
	}}
	err := store.Append(ctx, "stream-1", event2, 0)
	if err == nil {
		t.Fatal("expected version conflict error, got nil")
	}
	if !isDomainError(err, shared.ErrCodeVersionConflict) {
		t.Errorf("expected ErrCodeVersionConflict, got: %v", err)
	}
}

// An expectedVersion AHEAD of the stream must conflict, not silently insert
// events past a gap in the version sequence. Regression test for the case
// where a stream at version 1 accepted an append with expectedVersion=10
// (inserting version 11) because only the UNIQUE constraint guarded appends.
func TestEventStore_AppendVersionConflict_ExpectedVersionAhead(t *testing.T) {
	pool := postgrestest.NewPool(t)
	store := postgres.NewEventStore(pool)
	ctx := context.Background()

	event1 := []eventstore.Event{{
		ID: "evt-ahead-1", Type: "test.created", Version: 1, SchemaVersion: 1,
		Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
	}}
	if err := store.Append(ctx, "stream-ahead", event1, 0); err != nil {
		t.Fatalf("first Append: %v", err)
	}

	// Stream is at version 1; claiming it is at 10 must be a version
	// conflict, not a successful insert of version 11. The version conflict
	// check runs before the contiguity check, so the stamped Version here
	// (11, contiguous with the claimed expectedVersion=10) is irrelevant to
	// what's being asserted.
	event2 := []eventstore.Event{{
		ID: "evt-ahead-2", Type: "test.updated", Version: 11, SchemaVersion: 1,
		Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
	}}
	err := store.Append(ctx, "stream-ahead", event2, 10)
	if err == nil {
		t.Fatal("expected version conflict error, got nil")
	}
	if !isDomainError(err, shared.ErrCodeVersionConflict) {
		t.Errorf("expected ErrCodeVersionConflict, got: %v", err)
	}

	// Nothing may have been inserted past the gap.
	events, err := store.Load(ctx, "stream-ahead")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(events) != 1 || events[0].Version != 1 {
		t.Errorf("stream must still contain exactly version 1, got %d events", len(events))
	}
}

// Append must reject a caller-stamped Version that is stale (does not equal
// expectedVersion+i+1) rather than silently renumbering it, matching the
// in-memory reference store (infrastructure/inmemory/event_store.go).
func TestEventStore_Append_RejectsStaleStampedVersion(t *testing.T) {
	pool := postgrestest.NewPool(t)
	store := postgres.NewEventStore(pool)
	ctx := context.Background()

	event1 := []eventstore.Event{{
		ID: "evt-stale-1", Type: "test.created", Version: 1, SchemaVersion: 1,
		Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
	}}
	if err := store.Append(ctx, "stream-stale", event1, 0); err != nil {
		t.Fatalf("first Append: %v", err)
	}

	// Stream is now at version 1 (expectedVersion=1 is correct), but the event
	// carries a stale Version=99 (e.g. built against a since-superseded
	// aggregate). This must be rejected, not persisted as version 2.
	event2 := []eventstore.Event{{
		ID: "evt-stale-2", Type: "test.updated", Version: 99, SchemaVersion: 1,
		Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
	}}
	err := store.Append(ctx, "stream-stale", event2, 1)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if !isDomainError(err, shared.ErrCodeValidation) {
		t.Errorf("expected ErrCodeValidation, got: %v", err)
	}

	events, err := store.Load(ctx, "stream-stale")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(events) != 1 {
		t.Errorf("rejected event must not be persisted, got %d events", len(events))
	}
}

// A gap in the middle of a batch (event 0 correctly stamped, event 1 not)
// must reject the WHOLE batch, leaving nothing persisted — a partial insert
// would leave a version hole in the append-only log.
func TestEventStore_Append_RejectsVersionGapMidBatch(t *testing.T) {
	pool := postgrestest.NewPool(t)
	store := postgres.NewEventStore(pool)
	ctx := context.Background()

	events := []eventstore.Event{
		{ID: "evt-gap-1", Type: "test.created", Version: 1, SchemaVersion: 1, Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC()},
		{ID: "evt-gap-2", Type: "test.updated", Version: 3, SchemaVersion: 1, Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC()},
	}
	err := store.Append(ctx, "stream-gap", events, 0)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if !isDomainError(err, shared.ErrCodeValidation) {
		t.Errorf("expected ErrCodeValidation, got: %v", err)
	}

	loaded, loadErr := store.Load(ctx, "stream-gap")
	if loadErr != nil {
		t.Fatalf("Load: %v", loadErr)
	}
	if len(loaded) != 0 {
		t.Errorf("no events from a rejected batch may be persisted, got %d", len(loaded))
	}
}

// A zero (unstamped) Event.Version — e.g. a caller that forgot to stamp
// Version at all — must be rejected rather than silently accepted as
// expectedVersion+i+1.
func TestEventStore_Append_RejectsZeroVersion(t *testing.T) {
	pool := postgrestest.NewPool(t)
	store := postgres.NewEventStore(pool)
	ctx := context.Background()

	events := []eventstore.Event{{
		ID: "evt-zero-1", Type: "test.created", SchemaVersion: 1,
		Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
		// Version intentionally left at its zero value.
	}}
	err := store.Append(ctx, "stream-zero", events, 0)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if !isDomainError(err, shared.ErrCodeValidation) {
		t.Errorf("expected ErrCodeValidation, got: %v", err)
	}
}

// Correctly-stamped, contiguous versions across a multi-event batch are
// accepted and persisted as given.
func TestEventStore_Append_AcceptsContiguousStampedVersions(t *testing.T) {
	pool := postgrestest.NewPool(t)
	store := postgres.NewEventStore(pool)
	ctx := context.Background()

	events := []eventstore.Event{
		{ID: "evt-ok-1", Type: "test.created", Version: 1, SchemaVersion: 1, Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC()},
		{ID: "evt-ok-2", Type: "test.updated", Version: 2, SchemaVersion: 1, Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC()},
	}
	if err := store.Append(ctx, "stream-ok", events, 0); err != nil {
		t.Fatalf("Append: %v", err)
	}

	loaded, err := store.Load(ctx, "stream-ok")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 2 || loaded[0].Version != 1 || loaded[1].Version != 2 {
		t.Fatalf("unexpected loaded events: %+v", loaded)
	}
}

func TestEventStore_LoadUntilVersion(t *testing.T) {
	pool := postgrestest.NewPool(t)
	store := postgres.NewEventStore(pool)
	ctx := context.Background()

	events := make([]eventstore.Event, 5)
	for i := range events {
		events[i] = eventstore.Event{
			ID: "evt-" + string(rune('a'+i)), Type: "test.event", Version: i + 1, SchemaVersion: 1,
			Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
		}
	}
	if err := store.Append(ctx, "stream-1", events, 0); err != nil {
		t.Fatalf("Append: %v", err)
	}

	loaded, err := store.LoadUntilVersion(ctx, "stream-1", 3)
	if err != nil {
		t.Fatalf("LoadUntilVersion: %v", err)
	}
	if len(loaded) != 3 {
		t.Fatalf("got %d events, want 3", len(loaded))
	}
}

func TestEventStore_LoadAll(t *testing.T) {
	pool := postgrestest.NewPool(t)
	store := postgres.NewEventStore(pool)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		events := []eventstore.Event{{
			ID: "evt-" + string(rune('a'+i)), Type: "test.event", Version: 1, SchemaVersion: 1,
			Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
		}}
		if err := store.Append(ctx, "stream-"+string(rune('a'+i)), events, 0); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	// Load all from position 0 (inclusive of everything).
	all, err := store.LoadAll(ctx, 0, 0)
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("got %d events, want 3", len(all))
	}

	// fromPosition is exclusive.
	partial, err := store.LoadAll(ctx, all[0].GlobalPosition, 0)
	if err != nil {
		t.Fatalf("LoadAll partial: %v", err)
	}
	if len(partial) != 2 {
		t.Fatalf("got %d events, want 2", len(partial))
	}

	// With limit.
	limited, err := store.LoadAll(ctx, 0, 1)
	if err != nil {
		t.Fatalf("LoadAll limited: %v", err)
	}
	if len(limited) != 1 {
		t.Fatalf("got %d events, want 1", len(limited))
	}
}

func TestEventStore_Snapshot(t *testing.T) {
	pool := postgrestest.NewPool(t)
	store := postgres.NewEventStore(pool)
	ctx := context.Background()

	asOf := time.Now().UTC().Truncate(time.Microsecond)
	snap := eventstore.Snapshot{
		StreamID: "stream-1",
		Version:  5,
		State:    json.RawMessage(`{"status":"active"}`),
		AsOf:     asOf,
	}

	if err := store.SaveSnapshot(ctx, snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	loaded, err := store.LoadSnapshot(ctx, "stream-1")
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if loaded == nil {
		t.Fatal("LoadSnapshot returned nil")
	}
	if loaded.Version != 5 {
		t.Errorf("Version = %d, want 5", loaded.Version)
	}
	var state map[string]string
	if err := json.Unmarshal(loaded.State, &state); err != nil {
		t.Fatalf("unmarshal state: %v", err)
	}
	if state["status"] != "active" {
		t.Errorf("state[status] = %q, want active", state["status"])
	}

	// LoadSnapshot for non-existent stream returns nil, nil.
	missing, err := store.LoadSnapshot(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("LoadSnapshot nonexistent: %v", err)
	}
	if missing != nil {
		t.Error("expected nil for nonexistent stream")
	}
}

// LoadSnapshotBefore must cut off on the snapshot creation time (CreatedAt),
// not the as_of time — parity with core's infrastructure/inmemory reference
// implementation (snapshots[i].CreatedAt.Before(before)).
func TestEventStore_SnapshotBefore(t *testing.T) {
	pool := postgrestest.NewPool(t)
	store := postgres.NewEventStore(pool)
	ctx := context.Background()

	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	// v1: created at t1. Its as_of is deliberately far in the FUTURE so the
	// test fails if the implementation filters on as_of instead of created_at.
	if err := store.SaveSnapshot(ctx, eventstore.Snapshot{
		StreamID: "s1", Version: 1, State: json.RawMessage(`{"v":1}`),
		AsOf: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), CreatedAt: t1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveSnapshot(ctx, eventstore.Snapshot{
		StreamID: "s1", Version: 5, State: json.RawMessage(`{"v":5}`),
		AsOf: t2, CreatedAt: t2,
	}); err != nil {
		t.Fatal(err)
	}

	// Before t2 should return v1 (created strictly before t2; v5 is excluded
	// because CreatedAt < before is strict).
	snap, err := store.LoadSnapshotBefore(ctx, "s1", t2)
	if err != nil {
		t.Fatal(err)
	}
	if snap == nil || snap.Version != 1 {
		t.Errorf("expected snapshot v1, got %v", snap)
	}

	// After both creation times, the LATEST-created snapshot wins.
	snap, err = store.LoadSnapshotBefore(ctx, "s1", t2.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if snap == nil || snap.Version != 5 {
		t.Errorf("expected snapshot v5, got %v", snap)
	}

	// Before t1 should return nil (strict inequality: created_at == before is excluded).
	snap, err = store.LoadSnapshotBefore(ctx, "s1", t1)
	if err != nil {
		t.Fatal(err)
	}
	if snap != nil {
		t.Errorf("expected nil, got %v", snap)
	}
}

// LoadRange must be a half-open interval: from <= occurred_at < to — parity
// with core's infrastructure/inmemory reference implementation.
func TestEventStore_LoadRange_HalfOpenInterval(t *testing.T) {
	pool := postgrestest.NewPool(t)
	store := postgres.NewEventStore(pool)
	ctx := context.Background()

	from := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	mid := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)

	events := []eventstore.Event{
		{ID: "evt-before", Type: "test.event", Version: 1, SchemaVersion: 1, Data: json.RawMessage(`{}`), OccurredAt: from.Add(-time.Second)},
		{ID: "evt-at-from", Type: "test.event", Version: 2, SchemaVersion: 1, Data: json.RawMessage(`{}`), OccurredAt: from},
		{ID: "evt-mid", Type: "test.event", Version: 3, SchemaVersion: 1, Data: json.RawMessage(`{}`), OccurredAt: mid},
		{ID: "evt-at-to", Type: "test.event", Version: 4, SchemaVersion: 1, Data: json.RawMessage(`{}`), OccurredAt: to},
	}
	if err := store.Append(ctx, "stream-range", events, 0); err != nil {
		t.Fatalf("Append: %v", err)
	}

	loaded, err := store.LoadRange(ctx, "stream-range", from, to)
	if err != nil {
		t.Fatalf("LoadRange: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("got %d events, want 2 (from is inclusive, to is exclusive)", len(loaded))
	}
	if loaded[0].ID != "evt-at-from" || loaded[1].ID != "evt-mid" {
		t.Errorf("unexpected events in range: %q, %q", loaded[0].ID, loaded[1].ID)
	}
}

func TestEventStore_Subscribe(t *testing.T) {
	pool := postgrestest.NewPool(t)
	store := postgres.NewEventStore(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Subscribe from position 0.
	ch, err := store.Subscribe(ctx, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Give subscriber time to start listening.
	time.Sleep(200 * time.Millisecond)

	// Append an event.
	events := []eventstore.Event{{
		ID: "evt-sub-1", Type: "test.event", Version: 1, SchemaVersion: 1,
		Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
	}}
	if err := store.Append(ctx, "stream-sub", events, 0); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Should receive the event.
	select {
	case evt := <-ch:
		if evt.ID != "evt-sub-1" {
			t.Errorf("got event ID %q, want evt-sub-1", evt.ID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for subscribed event")
	}
}

// Issue #60: concurrent appends must serialize so global positions become
// visible in commit order. global_position (BIGSERIAL) is assigned at INSERT but
// only visible at COMMIT, and commits are not ordered by position; without
// serialization a subscriber that read a higher position could permanently miss
// a lower one committed afterwards. Append now takes pg_advisory_xact_lock, so an
// append holding an open transaction blocks every other append until it commits.
//
// This holds an uncommitted append in tx A and asserts a concurrent append B
// cannot proceed until A commits, and that A's event (positioned first, under the
// lock) sorts before B's in LoadAll — i.e. commit order == global-position order.
func TestEventStore_ConcurrentAppends_SerializeGlobalPosition(t *testing.T) {
	pool := postgrestest.NewPool(t)
	ctx := context.Background()
	store := postgres.NewEventStore(pool)

	evt := func(id, stream string) []eventstore.Event {
		return []eventstore.Event{{
			ID: id, Type: "test.event", Version: 1, SchemaVersion: 1,
			Data: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
		}}
	}

	// Tx A appends inside an ambient transaction and stays open: it holds the
	// advisory append lock until it commits.
	txA, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx A: %v", err)
	}
	ctxA := postgres.ContextWithTx(ctx, txA)
	if err := store.Append(ctxA, "stream-A", evt("evt-a", "stream-A"), 0); err != nil {
		_ = txA.Rollback(ctx)
		t.Fatalf("append A: %v", err)
	}

	// Tx B is a self-managed append run concurrently; it must block on the
	// append lock until A commits.
	bDone := make(chan error, 1)
	go func() { bDone <- store.Append(ctx, "stream-B", evt("evt-b", "stream-B"), 0) }()

	select {
	case err := <-bDone:
		_ = txA.Rollback(ctx)
		t.Fatalf("append B completed before A committed (appends not serialized): %v", err)
	case <-time.After(300 * time.Millisecond):
		// Expected: B is blocked on the advisory lock held by A.
	}

	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("commit A: %v", err)
	}

	select {
	case err := <-bDone:
		if err != nil {
			t.Fatalf("append B after A committed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("append B did not complete after A committed")
	}

	// Commit order (A then B) must equal global-position order.
	all, err := store.LoadAll(ctx, 0, 0)
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("want 2 events, got %d", len(all))
	}
	if all[0].StreamID != "stream-A" || all[1].StreamID != "stream-B" {
		t.Fatalf("global-position order = [%s, %s], want [stream-A, stream-B]",
			all[0].StreamID, all[1].StreamID)
	}
	if all[1].GlobalPosition <= all[0].GlobalPosition {
		t.Fatalf("positions not strictly increasing in commit order: %d then %d",
			all[0].GlobalPosition, all[1].GlobalPosition)
	}
}
