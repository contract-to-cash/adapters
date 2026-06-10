package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/contract-to-cash/core/domain/usage"
)

func newUsageRepo(t *testing.T) (*MySQLUsageRepository, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewUsageRepository(db), mock
}

func sampleUsageRecord(t *testing.T) *usage.UsageRecord {
	t.Helper()
	rec, err := usage.FromSnapshot(usage.UsageRecordSnapshot{
		ID:             "ur-1",
		ContractID:     "contract-1",
		MetricName:     "api_calls",
		Quantity:       100,
		Timestamp:      fixedTime,
		IdempotencyKey: "idem-ur-1",
	})
	if err != nil {
		t.Fatalf("FromSnapshot: %v", err)
	}
	return rec
}

func TestUsageRepo_Record_Idempotent(t *testing.T) {
	repo, mock := newUsageRepo(t)
	rec := sampleUsageRecord(t)

	idem := "idem-ur-1"
	mock.ExpectExec(`INSERT INTO usage_records .* ON DUPLICATE KEY UPDATE id = id`).
		WithArgs("ur-1", "contract-1", "api_calls", int64(100), fixedTime, &idem, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Record(context.Background(), rec); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestUsageRepo_GetSummary(t *testing.T) {
	repo, mock := newUsageRepo(t)
	period, err := shared.NewDateRange(fixedTime, fixedTime.Add(24*time.Hour))
	if err != nil {
		t.Fatalf("NewDateRange: %v", err)
	}

	mock.ExpectQuery(`SELECT COALESCE\(SUM\(quantity\), 0\) FROM usage_records WHERE contract_id = \? AND metric = \? AND timestamp >= \? AND timestamp < \?`).
		WithArgs("contract-1", "api_calls", period.Start().UTC(), period.End().UTC()).
		WillReturnRows(sqlmock.NewRows([]string{"total"}).AddRow(int64(250)))

	summary, err := repo.GetSummary(context.Background(), "contract-1", "api_calls", period)
	if err != nil {
		t.Fatalf("GetSummary: %v", err)
	}
	if summary.TotalUsage != 250 {
		t.Errorf("TotalUsage = %d, want 250", summary.TotalUsage)
	}
}

func TestUsageRepo_GetRecords(t *testing.T) {
	repo, mock := newUsageRepo(t)
	to := fixedTime.Add(24 * time.Hour)
	rows := sqlmock.NewRows([]string{"id", "contract_id", "metric", "quantity", "timestamp", "idempotency_key", "metadata"}).
		AddRow("ur-1", "contract-1", "api_calls", int64(100), fixedTime, "idem-ur-1", []byte(`{"src":"sdk"}`)).
		AddRow("ur-2", "contract-1", "api_calls", int64(50), fixedTime, nil, []byte(`{}`))

	mock.ExpectQuery(`SELECT id, contract_id, metric, quantity, timestamp, idempotency_key, metadata FROM usage_records WHERE contract_id = \? AND metric = \? AND timestamp >= \? AND timestamp < \? ORDER BY timestamp ASC`).
		WithArgs("contract-1", "api_calls", fixedTime, to).
		WillReturnRows(rows)

	got, err := repo.GetRecords(context.Background(), "contract-1", "api_calls", fixedTime, to)
	if err != nil {
		t.Fatalf("GetRecords: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 records, got %d", len(got))
	}
	if got[0].ToSnapshot().IdempotencyKey != "idem-ur-1" {
		t.Errorf("record 0 idempotency key = %q", got[0].ToSnapshot().IdempotencyKey)
	}
	if got[1].ToSnapshot().IdempotencyKey != "" {
		t.Errorf("record 1 idempotency key should be empty, got %q", got[1].ToSnapshot().IdempotencyKey)
	}
}
