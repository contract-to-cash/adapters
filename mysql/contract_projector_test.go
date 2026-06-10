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
