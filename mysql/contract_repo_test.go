package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/contract-to-cash/core/domain/contract"
	"github.com/contract-to-cash/core/domain/pricing"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/contract-to-cash/core/eventstore"
)

func newContractRepo(t *testing.T) (*MySQLContractRepository, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	clock := shared.FixedClock{FixedTime: fixedTime}
	es := New(db, clock)
	return NewContractRepository(db, es, clock), mock
}

// newDraftContract builds a draft contract with exactly one uncommitted
// "contract.created" event.
func newDraftContract(t *testing.T, id string) *contract.ContractAggregate {
	t.Helper()
	agg := contract.NewContractAggregate(shared.ContractID(id), shared.FixedClock{FixedTime: fixedTime})
	err := agg.Create(contract.CreateContractCommand{
		IdempotencyKey: "idem-" + id,
		AccountID:      "acct-1",
		PriceID:        "price-1",
		Price:          jpy(1000),
		ContractType:   contract.ContractTypeSubscription,
		Interval:       pricing.Monthly(),
	}, eventstore.EventMetadata{UserID: "user-1"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return agg
}

// newDraftContractAt is like newDraftContract but lets the caller pin the
// aggregate's clock, so the produced "contract.created" event's OccurredAt is
// deterministic (needed to test asOf event-horizon boundaries).
func newDraftContractAt(t *testing.T, id string, clockTime time.Time) *contract.ContractAggregate {
	t.Helper()
	agg := contract.NewContractAggregate(shared.ContractID(id), shared.FixedClock{FixedTime: clockTime})
	err := agg.Create(contract.CreateContractCommand{
		IdempotencyKey: "idem-" + id,
		AccountID:      "acct-1",
		PriceID:        "price-1",
		Price:          jpy(1000),
		ContractType:   contract.ContractTypeSubscription,
		Interval:       pricing.Monthly(),
	}, eventstore.EventMetadata{UserID: "user-1"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return agg
}

// activateAt loads a fresh aggregate from the given history at clockTime and
// activates it, returning the resulting aggregate (version = len(history)+1,
// status Active) so its uncommitted "contract.activated" event and
// MarshalSnapshot output can be extracted.
func activateAt(t *testing.T, id string, history []eventstore.Event, clockTime time.Time) *contract.ContractAggregate {
	t.Helper()
	agg := contract.NewContractAggregate(shared.ContractID(id), shared.FixedClock{FixedTime: clockTime})
	if err := agg.LoadFromHistory(history); err != nil {
		t.Fatalf("LoadFromHistory: %v", err)
	}
	if err := agg.Activate(eventstore.EventMetadata{UserID: "user-1"}); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	return agg
}

// eventRow appends a row built from a real eventstore.Event (as produced by
// RaiseEvent), so Version/Type/Data/OccurredAt match production exactly.
func eventRow(rows *sqlmock.Rows, e eventstore.Event) *sqlmock.Rows {
	return rows.AddRow(e.ID, e.StreamID, string(e.Type), e.Version, e.SchemaVersion,
		[]byte(e.Data), []byte(`{}`), e.OccurredAt, e.OccurredAt, int64(e.Version))
}

// createdEventData returns the JSON payload of the aggregate's created event,
// so test event rows round-trip exactly through LoadFromHistory.
func createdEventData(t *testing.T, agg *contract.ContractAggregate) []byte {
	t.Helper()
	evs := agg.UncommittedEvents()
	if len(evs) != 1 {
		t.Fatalf("expected 1 uncommitted event, got %d", len(evs))
	}
	return evs[0].Data
}

// Save appends the aggregate's uncommitted events through the event store.
func TestContractRepo_Save_AppendsEvents(t *testing.T) {
	repo, mock := newContractRepo(t)
	agg := newDraftContract(t, "c-1")

	// Event store Append with no ambient tx: Begin, append-lock, COUNT check,
	// INSERT, Commit. The FOR UPDATE on event_append_lock serializes appends so
	// global positions commit in order (issue #60).
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM event_append_lock WHERE id = 1 FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM events WHERE stream_id = \?`).
		WithArgs("c-1").
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock.ExpectExec(`INSERT INTO events`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := repo.Save(context.Background(), agg); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if agg.Version() != 1 {
		t.Errorf("version after save = %d, want 1", agg.Version())
	}
	if len(agg.UncommittedEvents()) != 0 {
		t.Errorf("expected uncommitted events cleared, got %d", len(agg.UncommittedEvents()))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Save with no uncommitted events is a no-op.
func TestContractRepo_Save_NoEvents(t *testing.T) {
	repo, mock := newContractRepo(t)
	agg := contract.NewContractAggregate("c-empty", shared.FixedClock{FixedTime: fixedTime})

	if err := repo.Save(context.Background(), agg); err != nil {
		t.Fatalf("Save no-op: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expected no DB interaction: %v", err)
	}
}

// FindByID with no snapshot and no events returns a not_found DomainError.
func TestContractRepo_FindByID_NotFound(t *testing.T) {
	repo, mock := newContractRepo(t)

	mock.ExpectQuery(`SELECT .* FROM snapshots WHERE stream_id = \? ORDER BY version DESC`).
		WithArgs("missing").
		WillReturnRows(sqlmock.NewRows([]string{"stream_id", "version", "state", "as_of", "created_at"}))
	mock.ExpectQuery(`SELECT .* FROM events WHERE stream_id = \? AND version > \?`).
		WithArgs("missing", 0).
		WillReturnRows(emptyEventRows())

	_, err := repo.FindByID(context.Background(), "missing")
	var de *shared.DomainError
	if !errors.As(err, &de) || de.Code != shared.ErrCodeNotFound {
		t.Fatalf("expected not_found DomainError, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// FindByID rebuilds the aggregate from replayed events when no snapshot exists.
func TestContractRepo_FindByID_FromEvents(t *testing.T) {
	repo, mock := newContractRepo(t)
	data := createdEventData(t, newDraftContract(t, "c-1"))

	mock.ExpectQuery(`SELECT .* FROM snapshots WHERE stream_id = \? ORDER BY version DESC`).
		WithArgs("c-1").
		WillReturnRows(sqlmock.NewRows([]string{"stream_id", "version", "state", "as_of", "created_at"}))
	mock.ExpectQuery(`SELECT .* FROM events WHERE stream_id = \? AND version > \?`).
		WithArgs("c-1", 0).
		WillReturnRows(eventRowsWithData("c-1", data))

	agg, err := repo.FindByID(context.Background(), "c-1")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if agg.ID() != "c-1" {
		t.Errorf("id = %s, want c-1", agg.ID())
	}
	if agg.AccountID() != "acct-1" {
		t.Errorf("account = %s, want acct-1", agg.AccountID())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// FindExpiring queries the read model and rehydrates each contract by ID.
func TestContractRepo_FindExpiring(t *testing.T) {
	repo, mock := newContractRepo(t)
	data := createdEventData(t, newDraftContract(t, "c-1"))

	mock.ExpectQuery(`SELECT id FROM contract_read_models\s+WHERE end_date IS NOT NULL AND end_date < \? AND status = 'active'`).
		WithArgs(fixedTime).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("c-1"))

	mock.ExpectQuery(`SELECT .* FROM snapshots WHERE stream_id = \? ORDER BY version DESC`).
		WithArgs("c-1").
		WillReturnRows(sqlmock.NewRows([]string{"stream_id", "version", "state", "as_of", "created_at"}))
	mock.ExpectQuery(`SELECT .* FROM events WHERE stream_id = \? AND version > \?`).
		WithArgs("c-1", 0).
		WillReturnRows(eventRowsWithData("c-1", data))

	got, err := repo.FindExpiring(context.Background(), fixedTime)
	if err != nil {
		t.Fatalf("FindExpiring: %v", err)
	}
	if len(got) != 1 || got[0].ID() != "c-1" {
		t.Fatalf("unexpected result: %+v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// FindByIDAsOf must discard a snapshot whose Version exceeds the highest
// event version within the asOf horizon (core review W7). With a
// skewed/injected clock an event can be recorded before the snapshot yet
// stamped with an OccurredAt after asOf; using such a snapshot would leak
// post-asOf state into the reconstruction. Here the snapshot (version 2,
// Active) is created_at before asOf but covers the Activate event, which
// occurred after asOf — the repository must fall back to a full replay from
// version 0 and return the pre-Activate (Draft) state.
func TestContractRepo_FindByIDAsOf_DiscardsSnapshotBeyondHorizon(t *testing.T) {
	repo, mock := newContractRepo(t)
	id := "c-w7"

	t1 := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)

	created := newDraftContractAt(t, id, t1)
	createdEvt := created.UncommittedEvents()[0]

	activated := activateAt(t, id, []eventstore.Event{createdEvt}, t3)
	snapState, err := activated.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}

	// LoadUntil(asOf) only returns events that OCCURRED at or before asOf: the
	// Activate event (occurred at t3) is excluded.
	mock.ExpectQuery(`SELECT .* FROM events WHERE stream_id = \? AND occurred_at <= \? ORDER BY version ASC`).
		WithArgs(id, asOf).
		WillReturnRows(eventRow(emptyEventRows(), createdEvt))

	// The snapshot is version 2 (Active) but CreatedAt is backdated to t1
	// (before asOf), simulating a skewed/injected clock (core review W7).
	mock.ExpectQuery(`SELECT .* FROM snapshots WHERE stream_id = \? AND created_at < \? ORDER BY created_at DESC, version DESC LIMIT 1`).
		WithArgs(id, asOf).
		WillReturnRows(sqlmock.NewRows([]string{"stream_id", "version", "state", "as_of", "created_at"}).
			AddRow(id, 2, snapState, t3, t1))

	agg, err := repo.FindByIDAsOf(context.Background(), shared.ContractID(id), asOf)
	if err != nil {
		t.Fatalf("FindByIDAsOf: %v", err)
	}
	if agg.Status() != contract.ContractStatusDraft {
		t.Errorf("status = %s, want draft (snapshot beyond the asOf horizon must be discarded)", agg.Status())
	}
	if agg.Version() != 1 {
		t.Errorf("version = %d, want 1 (full replay from version 0)", agg.Version())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// FindByIDAsOf must still use the snapshot fast path when it does not cover
// any post-asOf event: only events after the snapshot version are replayed.
func TestContractRepo_FindByIDAsOf_UsesSnapshotWithinHorizon(t *testing.T) {
	repo, mock := newContractRepo(t)
	id := "c-w7-ok"

	t1 := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC)
	snapCreatedAt := time.Date(2026, 1, 1, 1, 30, 0, 0, time.UTC)
	asOf := time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)

	created := newDraftContractAt(t, id, t1)
	createdEvt := created.UncommittedEvents()[0]

	activated := activateAt(t, id, []eventstore.Event{createdEvt}, t2)
	activatedEvt := activated.UncommittedEvents()[0]

	// Snapshot at version 1 (Draft, matching state right after Create).
	snapState, err := created.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}

	rows := eventRow(emptyEventRows(), createdEvt)
	rows = eventRow(rows, activatedEvt)
	mock.ExpectQuery(`SELECT .* FROM events WHERE stream_id = \? AND occurred_at <= \? ORDER BY version ASC`).
		WithArgs(id, asOf).
		WillReturnRows(rows)

	mock.ExpectQuery(`SELECT .* FROM snapshots WHERE stream_id = \? AND created_at < \? ORDER BY created_at DESC, version DESC LIMIT 1`).
		WithArgs(id, asOf).
		WillReturnRows(sqlmock.NewRows([]string{"stream_id", "version", "state", "as_of", "created_at"}).
			AddRow(id, 1, snapState, t1, snapCreatedAt))

	agg, err := repo.FindByIDAsOf(context.Background(), shared.ContractID(id), asOf)
	if err != nil {
		t.Fatalf("FindByIDAsOf: %v", err)
	}
	if agg.Status() != contract.ContractStatusActive {
		t.Errorf("status = %s, want active", agg.Status())
	}
	if agg.Version() != 2 {
		t.Errorf("version = %d, want 2 (snapshot v1 + replayed Activate event)", agg.Version())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestContractRepo_FindByIDAsOf_NotFoundWhenSnapshotDiscardedAndNoEventsWithinHorizon
// pins a hidden win of the W7 guard (issue #62): once a snapshot beyond the
// asOf horizon is discarded, the not-found check
// (!useSnapshot && len(relevant) == 0) means a skew-backdated snapshot can no
// longer "resurrect" a contract before its first event. Here the contract's
// only event (Create) occurs AFTER asOf, but a snapshot at version 1 was
// saved with CreatedAt backdated to before asOf — without the guard,
// LoadSnapshotBefore(asOf) would return it and FindByIDAsOf would incorrectly
// report the contract as existing (Draft) at asOf. With the guard, the
// snapshot is discarded (its version 1 exceeds maxVersionAsOf 0, since no
// events occurred at/before asOf) and, with zero relevant events either,
// FindByIDAsOf correctly returns ErrCodeNotFound.
func TestContractRepo_FindByIDAsOf_NotFoundWhenSnapshotDiscardedAndNoEventsWithinHorizon(t *testing.T) {
	repo, mock := newContractRepo(t)
	id := "c-w7-nf"

	asOf := time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC)
	snapCreatedAt := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC) // before asOf
	tCreate := time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)       // after asOf

	// The contract's only event (Create) occurs AFTER asOf.
	created := newDraftContractAt(t, id, tCreate)
	snapState, err := created.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}

	// LoadUntil(asOf) returns no events: the only event occurred after asOf.
	mock.ExpectQuery(`SELECT .* FROM events WHERE stream_id = \? AND occurred_at <= \? ORDER BY version ASC`).
		WithArgs(id, asOf).
		WillReturnRows(emptyEventRows())

	// A skew-backdated snapshot at version 1, CreatedAt before asOf even
	// though the only event it could reflect occurred after asOf. Its version
	// (1) exceeds maxVersionAsOf (0), so the guard must discard it.
	mock.ExpectQuery(`SELECT .* FROM snapshots WHERE stream_id = \? AND created_at < \? ORDER BY created_at DESC, version DESC LIMIT 1`).
		WithArgs(id, asOf).
		WillReturnRows(sqlmock.NewRows([]string{"stream_id", "version", "state", "as_of", "created_at"}).
			AddRow(id, 1, snapState, tCreate, snapCreatedAt))

	_, err = repo.FindByIDAsOf(context.Background(), shared.ContractID(id), asOf)
	var de *shared.DomainError
	if !errors.As(err, &de) || de.Code != shared.ErrCodeNotFound {
		t.Fatalf("expected not_found DomainError, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func emptyEventRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "stream_id", "type", "version", "schema_version",
		"data", "metadata", "occurred_at", "recorded_at", "global_position",
	})
}

func eventRowsWithData(streamID string, data []byte) *sqlmock.Rows {
	return emptyEventRows().AddRow("e1", streamID, "contract.created", 1, 1,
		data, []byte(`{}`), fixedTime, fixedTime, int64(1))
}
