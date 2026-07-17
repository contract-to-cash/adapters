package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/contract-to-cash/core/eventstore"
	driver "github.com/go-sql-driver/mysql"
)

func eventRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "stream_id", "type", "version", "schema_version",
		"data", "metadata", "occurred_at", "recorded_at", "global_position",
	}).AddRow("e1", "contract-1", "contract.created", 1, 1,
		[]byte(`{}`), []byte(`{}`), fixedTime, fixedTime, int64(1))
}

var fixedTime = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

// compile-time conformance check.
var _ eventstore.Store = (*EventStore)(nil)

func newTestStore(t *testing.T) (*EventStore, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return New(db, shared.FixedClock{FixedTime: fixedTime}), mock
}

func sampleEvent(id, stream string, version int) eventstore.Event {
	return eventstore.Event{
		ID:            id,
		StreamID:      stream,
		Type:          "contract.created",
		Version:       version,
		SchemaVersion: 1,
		Data:          []byte(`{"name":"acme"}`),
		Metadata:      eventstore.EventMetadata{UserID: "user-1"},
		OccurredAt:    fixedTime,
	}
}

func assertVersionConflict(t *testing.T, err error) {
	t.Helper()
	var de *shared.DomainError
	if !errors.As(err, &de) || de.Code != shared.ErrCodeVersionConflict {
		t.Fatalf("expected version_conflict DomainError, got %v", err)
	}
}

func TestEventStore_Append_Success(t *testing.T) {
	store, mock := newTestStore(t)
	events := []eventstore.Event{sampleEvent("e1", "contract-1", 1)}

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM event_append_lock WHERE id = 1 FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM events WHERE stream_id = \?`).
		WithArgs("contract-1").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock.ExpectExec(`INSERT INTO events`).
		WithArgs("e1", "contract-1", "contract.created", 1, 1,
			[]byte(`{"name":"acme"}`), sqlmock.AnyArg(), fixedTime, fixedTime).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := store.Append(context.Background(), "contract-1", events, 0); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// When an ambient transaction is supplied via ContextWithTx, Append must run
// the COUNT + INSERT directly on that tx and issue NO outer Begin/Commit — the
// caller owns the transaction boundary.
func TestEventStore_Append_UsesAmbientTx(t *testing.T) {
	store, mock := newTestStore(t)
	events := []eventstore.Event{sampleEvent("e1", "contract-1", 1)}

	// The caller's transaction. sqlmock matches the Begin/Commit to this one;
	// the store itself must not Begin/Commit again.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM event_append_lock WHERE id = 1 FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM events WHERE stream_id = \?`).
		WithArgs("contract-1").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock.ExpectExec(`INSERT INTO events`).
		WithArgs("e1", "contract-1", "contract.created", 1, 1,
			[]byte(`{"name":"acme"}`), sqlmock.AnyArg(), fixedTime, fixedTime).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	ctx := ContextWithTx(context.Background(), tx)

	if err := store.Append(ctx, "contract-1", events, 0); err != nil {
		t.Fatalf("Append on ambient tx: %v", err)
	}
	// The store must NOT have committed; the caller does.
	if err := tx.Commit(); err != nil {
		t.Fatalf("caller Commit: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestEventStore_Append_VersionConflict(t *testing.T) {
	store, mock := newTestStore(t)
	events := []eventstore.Event{sampleEvent("e9", "contract-1", 6)}

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM event_append_lock WHERE id = 1 FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM events WHERE stream_id = \?`).
		WithArgs("contract-1").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(1)) // current=1, expected=5
	mock.ExpectRollback()

	err := store.Append(context.Background(), "contract-1", events, 5)
	assertVersionConflict(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestEventStore_Append_DuplicateKeyMapsToVersionConflict(t *testing.T) {
	store, mock := newTestStore(t)
	events := []eventstore.Event{sampleEvent("e1", "contract-1", 1)}

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM event_append_lock WHERE id = 1 FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM events WHERE stream_id = \?`).
		WithArgs("contract-1").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	// A concurrent append won the (stream_id, version) race: the INSERT trips
	// the uq_stream_version UNIQUE constraint with MySQL error 1062. MySQL
	// names the offending key in the message, which is how we classify it.
	mock.ExpectExec(`INSERT INTO events`).
		WillReturnError(&driver.MySQLError{
			Number:  1062,
			Message: "Duplicate entry 'contract-1-1' for key 'events.uq_stream_version'",
		})
	mock.ExpectRollback()

	err := store.Append(context.Background(), "contract-1", events, 0)
	assertVersionConflict(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// A 1062 on uq_event_id means the same event ID was written twice — an
// infrastructure/caller bug, not a losable optimistic race. It must surface as
// a non-retryable conflict, never as version_conflict (regression for the bug
// where every 1062 was blindly mapped to version_conflict).
func TestEventStore_Append_DuplicateEventIDIsNotVersionConflict(t *testing.T) {
	store, mock := newTestStore(t)
	events := []eventstore.Event{sampleEvent("e1", "contract-1", 1)}

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM event_append_lock WHERE id = 1 FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM events WHERE stream_id = \?`).
		WithArgs("contract-1").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock.ExpectExec(`INSERT INTO events`).
		WillReturnError(&driver.MySQLError{
			Number:  1062,
			Message: "Duplicate entry 'e1' for key 'events.uq_event_id'",
		})
	mock.ExpectRollback()

	err := store.Append(context.Background(), "contract-1", events, 0)

	var de *shared.DomainError
	if !errors.As(err, &de) {
		t.Fatalf("expected *shared.DomainError, got %v", err)
	}
	if de.Code == shared.ErrCodeVersionConflict {
		t.Fatalf("duplicate event ID must not be version_conflict, got %v", err)
	}
	if de.Code != shared.ErrCodeConflict {
		t.Fatalf("expected conflict DomainError, got code %q (%v)", de.Code, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// A contract.created event reusing an idempotency key trips the
// ux_contract_idempotency_key UNIQUE index (migration 009), reported by MySQL as
// 1062 with the index name in the message. It must map to ErrCodeConflict (a
// creation conflict), NOT ErrCodeVersionConflict — a retry can never succeed, so
// the caller must look up the existing contract instead.
func TestEventStore_Append_ContractIdempotencyConflictMapsToConflict(t *testing.T) {
	store, mock := newTestStore(t)
	events := []eventstore.Event{sampleEvent("e1", "contract-1", 1)}

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM event_append_lock WHERE id = 1 FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM events WHERE stream_id = \?`).
		WithArgs("contract-1").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock.ExpectExec(`INSERT INTO events`).
		WillReturnError(&driver.MySQLError{
			Number:  1062,
			Message: "Duplicate entry 'dup-key' for key 'events.ux_contract_idempotency_key'",
		})
	mock.ExpectRollback()

	err := store.Append(context.Background(), "contract-1", events, 0)
	var de *shared.DomainError
	if !errors.As(err, &de) || de.Code != shared.ErrCodeConflict {
		t.Fatalf("expected conflict DomainError, got %v", err)
	}
	if de.Code == shared.ErrCodeVersionConflict {
		t.Fatal("idempotency conflict must NOT be a version conflict")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestEventStore_LoadUntilVersion(t *testing.T) {
	store, mock := newTestStore(t)
	mock.ExpectQuery(`SELECT .* FROM events WHERE stream_id = \? AND version <= \? ORDER BY version ASC`).
		WithArgs("contract-1", 5).
		WillReturnRows(eventRows())

	got, err := store.LoadUntilVersion(context.Background(), "contract-1", 5)
	if err != nil {
		t.Fatalf("LoadUntilVersion: %v", err)
	}
	if len(got) != 1 || got[0].ID != "e1" {
		t.Fatalf("unexpected result: %+v", got)
	}
}

func TestEventStore_LoadUntil(t *testing.T) {
	store, mock := newTestStore(t)
	mock.ExpectQuery(`SELECT .* FROM events WHERE stream_id = \? AND occurred_at <= \? ORDER BY version ASC`).
		WithArgs("contract-1", fixedTime).
		WillReturnRows(eventRows())

	got, err := store.LoadUntil(context.Background(), "contract-1", fixedTime)
	if err != nil {
		t.Fatalf("LoadUntil: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("unexpected result: %+v", got)
	}
}

func TestEventStore_LoadRange(t *testing.T) {
	store, mock := newTestStore(t)
	to := fixedTime.Add(24 * time.Hour)
	mock.ExpectQuery(`SELECT .* FROM events WHERE stream_id = \? AND occurred_at >= \? AND occurred_at < \? ORDER BY version ASC`).
		WithArgs("contract-1", fixedTime, to).
		WillReturnRows(eventRows())

	got, err := store.LoadRange(context.Background(), "contract-1", fixedTime, to)
	if err != nil {
		t.Fatalf("LoadRange: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("unexpected result: %+v", got)
	}
}

func TestEventStore_LoadSnapshotBefore(t *testing.T) {
	store, mock := newTestStore(t)
	before := fixedTime.Add(time.Hour)
	rows := sqlmock.NewRows([]string{"stream_id", "version", "state", "as_of", "created_at"}).
		AddRow("contract-1", 7, []byte(`{"s":1}`), fixedTime, fixedTime)
	mock.ExpectQuery(`SELECT .* FROM snapshots WHERE stream_id = \? AND created_at < \? ORDER BY created_at DESC, version DESC LIMIT 1`).
		WithArgs("contract-1", before).
		WillReturnRows(rows)

	snap, err := store.LoadSnapshotBefore(context.Background(), "contract-1", before)
	if err != nil {
		t.Fatalf("LoadSnapshotBefore: %v", err)
	}
	if snap == nil || snap.Version != 7 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
}

// assertValidationError requires err to be a *shared.DomainError with code
// validation_error, mirroring the in-memory reference store's contiguity
// check (infrastructure/inmemory/event_store.go).
func assertValidationError(t *testing.T, err error) *shared.DomainError {
	t.Helper()
	var de *shared.DomainError
	if !errors.As(err, &de) || de.Code != shared.ErrCodeValidation {
		t.Fatalf("expected validation_error DomainError, got %v", err)
	}
	return de
}

// The stored version must equal the caller-stamped Event.Version, which in
// turn must be contiguous with expectedVersion (expected+i+1) — matching the
// in-memory reference store and the postgres adapter. Correctly-stamped,
// contiguous events across a multi-event batch are accepted and persisted
// as-is (StreamID is still taken from the Append argument, not the event).
func TestEventStore_Append_AcceptsContiguousStampedVersions(t *testing.T) {
	store, mock := newTestStore(t)

	e1 := sampleEvent("e6", "wrong-stream", 6)
	e2 := sampleEvent("e7", "wrong-stream", 7)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM event_append_lock WHERE id = 1 FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM events WHERE stream_id = \?`).
		WithArgs("contract-1").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(5))
	mock.ExpectExec(`INSERT INTO events`).
		WithArgs("e6", "contract-1", "contract.created", 6, 1,
			[]byte(`{"name":"acme"}`), sqlmock.AnyArg(), fixedTime, fixedTime).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO events`).
		WithArgs("e7", "contract-1", "contract.created", 7, 1,
			[]byte(`{"name":"acme"}`), sqlmock.AnyArg(), fixedTime, fixedTime).
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectCommit()

	if err := store.Append(context.Background(), "contract-1", []eventstore.Event{e1, e2}, 5); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// A caller-stamped Version that is stale (built against an older aggregate
// version than expectedVersion reflects) must be rejected outright, not
// silently renumbered — mirrors the in-memory reference. No INSERT may run.
func TestEventStore_Append_RejectsStaleStampedVersion(t *testing.T) {
	store, mock := newTestStore(t)

	// expectedVersion=5 means the first event must be stamped 6; this one
	// carries a stale 99 (e.g. computed against a since-superseded aggregate).
	e1 := sampleEvent("e6", "contract-1", 99)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM event_append_lock WHERE id = 1 FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM events WHERE stream_id = \?`).
		WithArgs("contract-1").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(5))
	mock.ExpectRollback()

	err := store.Append(context.Background(), "contract-1", []eventstore.Event{e1}, 5)
	de := assertValidationError(t, err)
	if de.Message == "" {
		t.Error("expected a non-empty validation message")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// A gap in the middle of a batch (event 0 correctly stamped, event 1 not)
// must reject the WHOLE batch before any INSERT runs — a partial insert would
// leave a version hole in the append-only log.
func TestEventStore_Append_RejectsVersionGapMidBatch(t *testing.T) {
	store, mock := newTestStore(t)

	// expectedVersion=5: e1 correctly stamped 6, e2 should be 7 but skips to 9.
	e1 := sampleEvent("e6", "contract-1", 6)
	e2 := sampleEvent("e7", "contract-1", 9)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM event_append_lock WHERE id = 1 FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM events WHERE stream_id = \?`).
		WithArgs("contract-1").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(5))
	mock.ExpectRollback()

	err := store.Append(context.Background(), "contract-1", []eventstore.Event{e1, e2}, 5)
	_ = assertValidationError(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// A zero (unstamped) Event.Version — e.g. a caller that forgot to stamp
// Version at all — must be rejected rather than silently accepted as version
// expectedVersion+i+1.
func TestEventStore_Append_RejectsZeroVersion(t *testing.T) {
	store, mock := newTestStore(t)

	e1 := sampleEvent("e1", "contract-1", 0)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM event_append_lock WHERE id = 1 FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM events WHERE stream_id = \?`).
		WithArgs("contract-1").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock.ExpectRollback()

	err := store.Append(context.Background(), "contract-1", []eventstore.Event{e1}, 0)
	_ = assertValidationError(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestEventStore_Append_EmptyIsNoop(t *testing.T) {
	store, mock := newTestStore(t)
	// No DB interaction expected at all.
	if err := store.Append(context.Background(), "contract-1", nil, 0); err != nil {
		t.Fatalf("Append empty: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unexpected DB calls: %v", err)
	}
}

func TestEventStore_Load(t *testing.T) {
	store, mock := newTestStore(t)
	rows := sqlmock.NewRows([]string{
		"id", "stream_id", "type", "version", "schema_version",
		"data", "metadata", "occurred_at", "recorded_at", "global_position",
	}).
		AddRow("e1", "contract-1", "contract.created", 1, 1,
			[]byte(`{"name":"acme"}`), []byte(`{"user_id":"user-1"}`), fixedTime, fixedTime, int64(10)).
		AddRow("e2", "contract-1", "contract.activated", 2, 1,
			[]byte(`{}`), []byte(`{"user_id":"user-2"}`), fixedTime, fixedTime, int64(11))

	mock.ExpectQuery(`SELECT .* FROM events WHERE stream_id = \? ORDER BY version ASC`).
		WithArgs("contract-1").
		WillReturnRows(rows)

	got, err := store.Load(context.Background(), "contract-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 events, got %d", len(got))
	}
	if got[0].ID != "e1" || got[1].Version != 2 {
		t.Errorf("unexpected events: %+v", got)
	}
	if got[0].Metadata.UserID != "user-1" {
		t.Errorf("metadata not decoded: %+v", got[0].Metadata)
	}
	if got[1].GlobalPosition != 11 {
		t.Errorf("global position not scanned: %d", got[1].GlobalPosition)
	}
}

// W1: a real go-sql-driver without parseTime=true hands DATETIME columns back as
// raw bytes. The adapter must scan those into UTC time.Time, not error out.
func TestEventStore_Load_ScansTextualDatetime(t *testing.T) {
	store, mock := newTestStore(t)
	rows := sqlmock.NewRows([]string{
		"id", "stream_id", "type", "version", "schema_version",
		"data", "metadata", "occurred_at", "recorded_at", "global_position",
	}).AddRow("e1", "contract-1", "contract.created", 1, 1,
		[]byte(`{}`), []byte(`{}`),
		[]byte("2026-06-01 12:00:00.000000"), []byte("2026-06-01 12:00:00.000000"), int64(1))
	mock.ExpectQuery(`SELECT .* FROM events WHERE stream_id = \? ORDER BY version ASC`).
		WithArgs("contract-1").
		WillReturnRows(rows)

	got, err := store.Load(context.Background(), "contract-1")
	if err != nil {
		t.Fatalf("Load with textual datetime: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(got))
	}
	if !got[0].OccurredAt.Equal(fixedTime) {
		t.Errorf("occurred_at not parsed to UTC: got %v want %v", got[0].OccurredAt, fixedTime)
	}
	if got[0].OccurredAt.Location() != time.UTC {
		t.Errorf("occurred_at not in UTC: %v", got[0].OccurredAt.Location())
	}
}

// W2: an event with empty Data must not violate the JSON NOT NULL column; the
// adapter normalizes empty payloads to an empty JSON object.
func TestEventStore_Append_NormalizesEmptyData(t *testing.T) {
	store, mock := newTestStore(t)
	ev := sampleEvent("e1", "contract-1", 1)
	ev.Data = nil

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM event_append_lock WHERE id = 1 FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM events WHERE stream_id = \?`).
		WithArgs("contract-1").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock.ExpectExec(`INSERT INTO events`).
		WithArgs("e1", "contract-1", "contract.created", 1, 1,
			[]byte("{}"), sqlmock.AnyArg(), fixedTime, fixedTime).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := store.Append(context.Background(), "contract-1", []eventstore.Event{ev}, 0); err != nil {
		t.Fatalf("Append empty data: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestEventStore_SaveSnapshot_NormalizesEmptyState(t *testing.T) {
	store, mock := newTestStore(t)
	snap := eventstore.Snapshot{
		StreamID:  "contract-1",
		Version:   1,
		State:     nil,
		AsOf:      fixedTime,
		CreatedAt: fixedTime,
	}
	mock.ExpectExec(`INSERT INTO snapshots`).
		WithArgs("contract-1", 1, []byte("{}"), fixedTime, fixedTime).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := store.SaveSnapshot(context.Background(), snap); err != nil {
		t.Fatalf("SaveSnapshot empty state: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestEventStore_LoadAll_WithLimit(t *testing.T) {
	store, mock := newTestStore(t)
	rows := sqlmock.NewRows([]string{
		"id", "stream_id", "type", "version", "schema_version",
		"data", "metadata", "occurred_at", "recorded_at", "global_position",
	}).AddRow("e2", "c1", "t", 2, 1, []byte(`{}`), []byte(`{}`), fixedTime, fixedTime, int64(5))

	mock.ExpectQuery(`SELECT .* FROM events WHERE global_position > \? ORDER BY global_position ASC LIMIT \?`).
		WithArgs(int64(4), 1).
		WillReturnRows(rows)

	got, err := store.LoadAll(context.Background(), 4, 1)
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if len(got) != 1 || got[0].GlobalPosition != 5 {
		t.Fatalf("unexpected LoadAll result: %+v", got)
	}
}

func TestEventStore_SaveSnapshot(t *testing.T) {
	store, mock := newTestStore(t)
	snap := eventstore.Snapshot{
		StreamID:  "contract-1",
		Version:   42,
		State:     []byte(`{"status":"active"}`),
		AsOf:      fixedTime,
		CreatedAt: fixedTime,
	}
	mock.ExpectExec(`INSERT INTO snapshots`).
		WithArgs("contract-1", 42, []byte(`{"status":"active"}`), fixedTime, fixedTime).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := store.SaveSnapshot(context.Background(), snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// A zero CreatedAt must be stamped with the store clock before writing —
// LoadSnapshotBefore filters on created_at, so persisting the zero value
// would hide the snapshot from all temporal queries (and diverge from the
// postgres adapter's COALESCE(..., NOW()) fallback).
func TestEventStore_SaveSnapshot_ZeroCreatedAtUsesClock(t *testing.T) {
	store, mock := newTestStore(t)
	snap := eventstore.Snapshot{
		StreamID: "contract-1",
		Version:  42,
		State:    []byte(`{"status":"active"}`),
		AsOf:     fixedTime,
		// CreatedAt intentionally left zero.
	}
	mock.ExpectExec(`INSERT INTO snapshots`).
		WithArgs("contract-1", 42, []byte(`{"status":"active"}`), fixedTime, fixedTime).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := store.SaveSnapshot(context.Background(), snap); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestEventStore_LoadSnapshot_Found(t *testing.T) {
	store, mock := newTestStore(t)
	rows := sqlmock.NewRows([]string{"stream_id", "version", "state", "as_of", "created_at"}).
		AddRow("contract-1", 42, []byte(`{"status":"active"}`), fixedTime, fixedTime)
	mock.ExpectQuery(`SELECT .* FROM snapshots WHERE stream_id = \? ORDER BY version DESC LIMIT 1`).
		WithArgs("contract-1").
		WillReturnRows(rows)

	snap, err := store.LoadSnapshot(context.Background(), "contract-1")
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if snap == nil || snap.Version != 42 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
}

func TestEventStore_Subscribe_DeliversThenClosesOnCancel(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// A 1h poll interval guarantees exactly one drain happens before the
	// goroutine parks on the ticker, making the test deterministic.
	store := New(db, shared.FixedClock{FixedTime: fixedTime}, WithPollInterval(time.Hour))

	row := sqlmock.NewRows([]string{
		"id", "stream_id", "type", "version", "schema_version",
		"data", "metadata", "occurred_at", "recorded_at", "global_position",
	}).AddRow("e1", "c1", "contract.created", 1, 1, []byte(`{}`), []byte(`{}`), fixedTime, fixedTime, int64(7))
	mock.ExpectQuery(`SELECT .* FROM events WHERE global_position > \? ORDER BY global_position ASC LIMIT \?`).
		WithArgs(int64(0), 256).
		WillReturnRows(row)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := store.Subscribe(ctx, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case e, ok := <-ch:
		if !ok || e.GlobalPosition != 7 {
			t.Fatalf("expected delivered event at position 7, got ok=%v ev=%+v", ok, e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for subscribed event")
	}

	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to be closed after cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for channel close")
	}
}

func TestEventStore_LoadSnapshot_NoneReturnsNil(t *testing.T) {
	store, mock := newTestStore(t)
	mock.ExpectQuery(`SELECT .* FROM snapshots WHERE stream_id = \? ORDER BY version DESC LIMIT 1`).
		WithArgs("contract-1").
		WillReturnError(sql.ErrNoRows)

	snap, err := store.LoadSnapshot(context.Background(), "contract-1")
	if err != nil {
		t.Fatalf("LoadSnapshot none: %v", err)
	}
	if snap != nil {
		t.Fatalf("expected nil snapshot, got %+v", snap)
	}
}

// subErrCollector is a concurrency-safe WithSubscriptionErrorHandler sink
// (mirrors postgres's errCollector in subscription_reconnect_test.go).
type subErrCollector struct {
	mu   sync.Mutex
	errs []error
}

func (c *subErrCollector) handle(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, err)
}

func (c *subErrCollector) snapshot() []error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]error(nil), c.errs...)
}

// A failed poll must be reported through WithSubscriptionErrorHandler AND must
// not kill the loop: the next poll runs and its events are still delivered.
func TestEventStore_Subscribe_ReportsPollErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	reports := &subErrCollector{}
	store := New(db, shared.FixedClock{FixedTime: fixedTime},
		WithPollInterval(20*time.Millisecond),
		WithSubscriptionErrorHandler(reports.handle))

	// sqlmock matches expectations in order by default; make it explicit so
	// the failing poll is guaranteed to precede the successful one.
	mock.MatchExpectationsInOrder(true)
	mock.ExpectQuery(`SELECT .* FROM events WHERE global_position > \? ORDER BY global_position ASC LIMIT \?`).
		WithArgs(int64(0), 256).
		WillReturnError(errors.New("boom"))
	row := sqlmock.NewRows([]string{
		"id", "stream_id", "type", "version", "schema_version",
		"data", "metadata", "occurred_at", "recorded_at", "global_position",
	}).AddRow("e1", "c1", "contract.created", 1, 1, []byte(`{}`), []byte(`{}`), fixedTime, fixedTime, int64(7))
	mock.ExpectQuery(`SELECT .* FROM events WHERE global_position > \? ORDER BY global_position ASC LIMIT \?`).
		WithArgs(int64(0), 256).
		WillReturnRows(row)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := store.Subscribe(ctx, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// The loop survived the failed first poll: the second poll's event arrives.
	select {
	case e, ok := <-ch:
		if !ok || e.GlobalPosition != 7 {
			t.Fatalf("expected delivered event at position 7, got ok=%v ev=%+v", ok, e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for subscribed event after a failed poll")
	}

	// Expectations are ordered, so by the time the event was delivered the
	// failing poll has already been reported.
	var found bool
	for _, rerr := range reports.snapshot() {
		if strings.Contains(rerr.Error(), "boom") {
			found = true
			if !strings.Contains(rerr.Error(), "subscribe: event store: load all") {
				t.Errorf("reported error %q is not wrapped as \"subscribe: event store: load all\"", rerr)
			}
		}
	}
	if !found {
		t.Fatalf("expected the failed poll to be reported (containing \"boom\"), got %v", reports.snapshot())
	}

	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to be closed after cancel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for channel close")
	}
}

// Context cancellation is a normal shutdown: the channel closes and nothing is
// reported to the subscription error handler.
func TestEventStore_Subscribe_CancelDoesNotReportError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	reports := &subErrCollector{}
	// A 1h poll interval and a 1-event batch (< pollBatchSize, so no
	// full-batch continue) mean the single poll has provably completed once
	// the event is received: no LoadAll is in flight at or after cancel, and
	// the goroutine's only remaining transitions are parking on the ticker
	// select and observing ctx.Done — neither can report an error.
	store := New(db, shared.FixedClock{FixedTime: fixedTime},
		WithPollInterval(time.Hour),
		WithSubscriptionErrorHandler(reports.handle))

	row := sqlmock.NewRows([]string{
		"id", "stream_id", "type", "version", "schema_version",
		"data", "metadata", "occurred_at", "recorded_at", "global_position",
	}).AddRow("e1", "c1", "contract.created", 1, 1, []byte(`{}`), []byte(`{}`), fixedTime, fixedTime, int64(7))
	mock.ExpectQuery(`SELECT .* FROM events WHERE global_position > \? ORDER BY global_position ASC LIMIT \?`).
		WithArgs(int64(0), 256).
		WillReturnRows(row)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := store.Subscribe(ctx, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Receiving the event proves the single poll completed successfully.
	select {
	case e, ok := <-ch:
		if !ok || e.GlobalPosition != 7 {
			t.Fatalf("expected delivered event at position 7, got ok=%v ev=%+v", ok, e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for subscribed event")
	}

	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to be closed after cancel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for channel close")
	}

	for _, rerr := range reports.snapshot() {
		if errors.Is(rerr, context.Canceled) {
			t.Errorf("context cancellation must not be reported, got %v", rerr)
		}
	}
	if got := reports.snapshot(); len(got) != 0 {
		t.Errorf("expected no reported errors on clean shutdown, got %v", got)
	}
}

func TestReportSubErr_FiltersContextCancellation(t *testing.T) {
	var got []error
	s := &EventStore{onSubErr: func(err error) { got = append(got, err) }}

	// Normal-shutdown errors must not be reported.
	s.reportSubErr(nil)
	s.reportSubErr(context.Canceled)
	s.reportSubErr(context.DeadlineExceeded)
	s.reportSubErr(fmt.Errorf("wrap: %w", context.Canceled))
	s.reportSubErr(fmt.Errorf("wrap: %w", context.DeadlineExceeded))
	if len(got) != 0 {
		t.Fatalf("context/nil errors should not be reported, got %v", got)
	}

	// A genuine failure is forwarded exactly once.
	real := errors.New("load all failed")
	s.reportSubErr(real)
	if len(got) != 1 || !errors.Is(got[0], real) {
		t.Fatalf("expected real error to be reported once, got %v", got)
	}
}

func TestReportSubErr_NilHandlerIsSafe(t *testing.T) {
	s := &EventStore{} // no handler
	s.reportSubErr(errors.New("boom"))
}
