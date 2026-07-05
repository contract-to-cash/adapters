package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/contract-to-cash/core/eventstore"
)

func newContractProjector(t *testing.T) (*ContractProjector, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	es := New(db, shared.FixedClock{FixedTime: fixedTime})
	cp := NewCheckpointStore(db)
	return NewContractProjector(db, es, cp), mock
}

func TestContractProjector_Project_Created(t *testing.T) {
	proj, mock := newContractProjector(t)

	ev := eventstore.Event{
		StreamID: "contract-1",
		Type:     "contract.created",
		Version:  1,
		Data:     []byte(`{"account_id":"acct-1","price_id":"price-1","created_at":"2026-06-01T12:00:00Z"}`),
	}

	mock.ExpectExec(`INSERT INTO contract_read_models`).
		WithArgs("contract-1", "acct-1", sqlmock.AnyArg(), "price-1", sqlmock.AnyArg(), 1).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := proj.Project(context.Background(), ev); err != nil {
		t.Fatalf("Project created: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestContractProjector_Project_Suspended(t *testing.T) {
	proj, mock := newContractProjector(t)

	ev := eventstore.Event{
		StreamID: "contract-1",
		Type:     "contract.suspended",
		Version:  3,
		Data:     []byte(`{}`),
	}

	mock.ExpectExec(`UPDATE contract_read_models SET status = 'suspended'`).
		WithArgs(sqlmock.AnyArg(), 3, "contract-1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := proj.Project(context.Background(), ev); err != nil {
		t.Fatalf("Project suspended: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// A trial that ends without converting must land the read model in 'cancelled'
// (matching the aggregate), not 'active'.
func TestContractProjector_Project_TrialEnded_NotConverted(t *testing.T) {
	proj, mock := newContractProjector(t)

	ev := eventstore.Event{
		StreamID: "contract-1",
		Type:     "contract.trial_ended",
		Version:  4,
		Data:     []byte(`{"converted":false}`),
	}

	mock.ExpectExec(`UPDATE contract_read_models\s+SET status = \?, trial_end_date = NULL`).
		WithArgs("cancelled", sqlmock.AnyArg(), 4, "contract-1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := proj.Project(context.Background(), ev); err != nil {
		t.Fatalf("Project trial_ended: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// A converted trial transitions the read model to 'active'.
func TestContractProjector_Project_TrialEnded_Converted(t *testing.T) {
	proj, mock := newContractProjector(t)

	ev := eventstore.Event{
		StreamID: "contract-1",
		Type:     "contract.trial_ended",
		Version:  4,
		Data:     []byte(`{"converted":true}`),
	}

	mock.ExpectExec(`UPDATE contract_read_models\s+SET status = \?, trial_end_date = NULL`).
		WithArgs("active", sqlmock.AnyArg(), 4, "contract-1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := proj.Project(context.Background(), ev); err != nil {
		t.Fatalf("Project trial_ended: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// An immediate price change updates the read model's price_id.
func TestContractProjector_Project_PriceChanged(t *testing.T) {
	proj, mock := newContractProjector(t)

	ev := eventstore.Event{
		StreamID: "contract-1",
		Type:     "contract.price_changed",
		Version:  5,
		Data:     []byte(`{"old_price_id":"price-1","new_price_id":"price-2"}`),
	}

	mock.ExpectExec(`UPDATE contract_read_models\s+SET price_id = COALESCE\(NULLIF\(\?, ''\), price_id\)`).
		WithArgs("price-2", sqlmock.AnyArg(), 5, "contract-1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := proj.Project(context.Background(), ev); err != nil {
		t.Fatalf("Project price_changed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Rebuild(until) must checkpoint at the position of the last event it actually
// projected and stop scanning at the first event past `until`. Skipped events
// must stay behind the checkpoint so a later incremental Project reprocesses
// them; scanning past the first skip would be unsafe under clock skew (a
// projected pre-`until` event at a higher position would advance the checkpoint
// past the skipped one, losing it forever).
func TestContractProjector_Rebuild_CheckpointStopsAtUntil(t *testing.T) {
	proj, mock := newContractProjector(t)

	until := fixedTime
	cols := []string{
		"id", "stream_id", "type", "version", "schema_version",
		"data", "metadata", "occurred_at", "recorded_at", "global_position",
	}

	mock.ExpectBegin()
	mock.ExpectExec(`SET SESSION foreign_key_checks = 0`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM contract_read_models`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM projection_checkpoints`).WillReturnResult(sqlmock.NewResult(0, 1))

	// Single batch: gp=1 is on/before `until` (projected); gp=2 is after `until`
	// and stops the scan; gp=3 is a backdated pre-`until` event (clock skew)
	// that must NOT be projected now — it stays after the checkpoint for the
	// next incremental run.
	rows := sqlmock.NewRows(cols).
		AddRow("evt-1", "contract-1", "contract.created", 1, 1,
			[]byte(`{"account_id":"a","price_id":"p","created_at":"2026-06-01T00:00:00Z"}`),
			[]byte(``), until.Add(-time.Hour), until.Add(-time.Hour), int64(1)).
		AddRow("evt-2", "contract-1", "contract.suspended", 2, 1,
			[]byte(`{}`), []byte(``), until.Add(time.Hour), until.Add(time.Hour), int64(2)).
		AddRow("evt-3", "contract-1", "contract.resumed", 3, 1,
			[]byte(`{}`), []byte(``), until.Add(-time.Minute), until.Add(-time.Minute), int64(3))
	mock.ExpectQuery(`SELECT .* FROM events WHERE global_position > \?`).
		WithArgs(int64(0), 1000).WillReturnRows(rows)

	// Only the projected event (created) hits the read model. The scan breaks
	// at evt-2, so no further LoadAll batch is issued and evt-3 is not applied.
	mock.ExpectExec(`INSERT INTO contract_read_models`).
		WithArgs("contract-1", "a", sqlmock.AnyArg(), "p", sqlmock.AnyArg(), 1).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Checkpoint saved at gp=1 (the last projected event), NOT gp=2 or gp=3.
	mock.ExpectExec(`INSERT INTO projection_checkpoints`).
		WithArgs(ContractProjectorName, int64(1), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(`SET SESSION foreign_key_checks = 1`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	if err := proj.Rebuild(context.Background(), until); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// Unrelated event types are ignored (no DB write).
func TestContractProjector_Project_IgnoresUnrelated(t *testing.T) {
	proj, mock := newContractProjector(t)

	ev := eventstore.Event{StreamID: "inv-1", Type: "invoice.created", Version: 1, Data: []byte(`{}`)}
	if err := proj.Project(context.Background(), ev); err != nil {
		t.Fatalf("Project unrelated: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expected no DB interaction: %v", err)
	}
}

// Rebuild must disable foreign key checks around the truncate/reload and
// re-enable them afterwards (the MySQL stand-in for postgres SET CONSTRAINTS
// ALL DEFERRED).
func TestContractProjector_Rebuild_TogglesForeignKeyChecks(t *testing.T) {
	proj, mock := newContractProjector(t)

	mock.ExpectBegin()
	mock.ExpectExec(`SET SESSION foreign_key_checks = 0`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM contract_read_models`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM projection_checkpoints`).WillReturnResult(sqlmock.NewResult(0, 1))
	// No events: LoadAll returns empty.
	mock.ExpectQuery(`SELECT .* FROM events WHERE global_position > \?`).
		WithArgs(int64(0), 1000).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "stream_id", "type", "version", "schema_version",
			"data", "metadata", "occurred_at", "recorded_at", "global_position",
		}))
	mock.ExpectExec(`SET SESSION foreign_key_checks = 1`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	if err := proj.Rebuild(context.Background(), time.Now()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
