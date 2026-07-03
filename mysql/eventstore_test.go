package mysql

import (
	"context"
	"database/sql"
	"errors"
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
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM events WHERE stream_id = \?`).
		WithArgs("contract-1").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	// A concurrent append won the (stream_id, version) race: the INSERT trips
	// the UNIQUE constraint with MySQL error 1062.
	mock.ExpectExec(`INSERT INTO events`).
		WillReturnError(&driver.MySQLError{Number: 1062, Message: "Duplicate entry"})
	mock.ExpectRollback()

	err := store.Append(context.Background(), "contract-1", events, 0)
	assertVersionConflict(t, err)
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

// The stored version must be derived from expectedVersion (expected+i+1) and
// the stored stream id from the streamID argument — never from the
// caller-populated Event.Version / Event.StreamID fields. A caller passing
// stale or inconsistent values must not be able to break the version sequence
// (parity with the postgres adapter, whose INSERT derives both server-side).
func TestEventStore_Append_DerivesVersionFromExpectedVersion(t *testing.T) {
	store, mock := newTestStore(t)

	// Caller-supplied Version (99) and StreamID ("wrong-stream") disagree with
	// the Append arguments; the derived values must win.
	e1 := sampleEvent("e6", "wrong-stream", 99)
	e2 := sampleEvent("e7", "wrong-stream", 42)

	mock.ExpectBegin()
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
