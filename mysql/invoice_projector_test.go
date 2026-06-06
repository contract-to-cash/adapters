package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/contract-to-cash/core/eventstore"
)

func newInvoiceProjector(t *testing.T) (*InvoiceProjector, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	es := New(db, shared.FixedClock{FixedTime: fixedTime})
	cp := NewCheckpointStore(db)
	return NewInvoiceProjector(db, es, cp), mock
}

func TestInvoiceProjector_Project_Created(t *testing.T) {
	proj, mock := newInvoiceProjector(t)

	ev := eventstore.Event{
		StreamID: "inv-1",
		Type:     "invoice.created",
		Version:  1,
		Data:     []byte(`{"contract_id":"c-1","account_id":"acct-1","status":"draft","currency":"JPY","total":1500}`),
	}

	mock.ExpectExec(`INSERT INTO invoice_read_models`).
		WithArgs("inv-1", "c-1", "acct-1", "draft", int64(1500), "JPY", sqlmock.AnyArg(), 1).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := proj.Project(context.Background(), ev); err != nil {
		t.Fatalf("Project created: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestInvoiceProjector_Project_Paid(t *testing.T) {
	proj, mock := newInvoiceProjector(t)

	ev := eventstore.Event{StreamID: "inv-1", Type: "invoice.paid", Version: 2, Data: []byte(`{}`)}

	mock.ExpectExec(`UPDATE invoice_read_models SET status = 'paid'`).
		WithArgs(sqlmock.AnyArg(), 2, "inv-1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := proj.Project(context.Background(), ev); err != nil {
		t.Fatalf("Project paid: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestInvoiceProjector_Project_IgnoresNonInvoice(t *testing.T) {
	proj, mock := newInvoiceProjector(t)

	ev := eventstore.Event{StreamID: "contract-1", Type: "contract.created", Version: 1, Data: []byte(`{}`)}
	if err := proj.Project(context.Background(), ev); err != nil {
		t.Fatalf("Project non-invoice: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expected no DB interaction: %v", err)
	}
}

// Unlike the contract projector, the invoice rebuild does not toggle FK checks
// (mirrors postgres, where only the contract projector defers constraints).
func TestInvoiceProjector_Rebuild_NoForeignKeyToggle(t *testing.T) {
	proj, mock := newInvoiceProjector(t)

	mock.ExpectBegin()
	mock.ExpectExec(`DELETE FROM invoice_read_models`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM projection_checkpoints`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT .* FROM events WHERE global_position > \?`).
		WithArgs(int64(0), 1000).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "stream_id", "type", "version", "schema_version",
			"data", "metadata", "occurred_at", "recorded_at", "global_position",
		}))
	mock.ExpectCommit()

	if err := proj.Rebuild(context.Background(), time.Now()); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
