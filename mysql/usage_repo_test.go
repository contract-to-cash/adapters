package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/contract-to-cash/core/domain/usage"
	driver "github.com/go-sql-driver/mysql"
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

func TestUsageRepo_Record(t *testing.T) {
	repo, mock := newUsageRepo(t)
	rec := sampleUsageRecord(t)

	idem := "idem-ur-1"
	mock.ExpectExec(`INSERT INTO usage_records`).
		WithArgs("ur-1", "contract-1", "api_calls", int64(100), fixedTime, &idem, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Record(context.Background(), rec); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// A duplicate idempotency_key must surface as the core's
// DomainError(duplicate_request), matching the in-memory reference
// implementation integrators test against (issue #38). It is NOT a silent no-op:
// a caller who wants ignore-duplicates semantics can check the error code, but a
// caller who needs the signal cannot recover it from a silent success.
func TestUsageRepo_Record_DuplicateIdempotencyKeyReturnsDuplicateRequest(t *testing.T) {
	repo, mock := newUsageRepo(t)
	rec := sampleUsageRecord(t)

	mock.ExpectExec(`INSERT INTO usage_records`).
		WillReturnError(&driver.MySQLError{
			Number:  1062,
			Message: "Duplicate entry 'idem-ur-1' for key 'usage_records.idempotency_key'",
		})

	err := repo.Record(context.Background(), rec)
	if err == nil {
		t.Fatal("expected duplicate idempotency key to return an error, got nil")
	}
	var de *shared.DomainError
	if !errors.As(err, &de) {
		t.Fatalf("expected *shared.DomainError, got %T: %v", err, err)
	}
	if de.Code != shared.ErrCodeDuplicateRequest {
		t.Errorf("Code = %q, want %q", de.Code, shared.ErrCodeDuplicateRequest)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// A duplicate PRIMARY KEY (id) is NOT idempotent — it signals a real bug and
// must surface rather than be silently swallowed.
func TestUsageRepo_Record_DuplicateIDSurfaces(t *testing.T) {
	repo, mock := newUsageRepo(t)
	rec := sampleUsageRecord(t)

	mock.ExpectExec(`INSERT INTO usage_records`).
		WillReturnError(&driver.MySQLError{
			Number:  1062,
			Message: "Duplicate entry 'ur-1' for key 'usage_records.PRIMARY'",
		})

	if err := repo.Record(context.Background(), rec); err == nil {
		t.Fatal("expected duplicate id to surface as an error, got nil")
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
