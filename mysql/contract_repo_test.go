package mysql

import (
	"context"
	"errors"
	"testing"

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
		AccountID:    "acct-1",
		PriceID:      "price-1",
		Price:        jpy(1000),
		ContractType: contract.ContractTypeSubscription,
		Interval:     pricing.Monthly(),
	}, eventstore.EventMetadata{UserID: "user-1"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return agg
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

	// Event store Append with no ambient tx: Begin, COUNT check, INSERT, Commit.
	mock.ExpectBegin()
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
