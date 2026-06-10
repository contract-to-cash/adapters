package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/domain/shared"
	"github.com/contract-to-cash/core/domain/usage"
)

// MySQLUsageRepository is a MySQL-backed usage.Repository.
type MySQLUsageRepository struct {
	db *sql.DB
}

var _ usage.Repository = (*MySQLUsageRepository)(nil)

// NewUsageRepository constructs a usage repository over an existing *sql.DB.
func NewUsageRepository(db *sql.DB) *MySQLUsageRepository {
	return &MySQLUsageRepository{db: db}
}

func (r *MySQLUsageRepository) q(ctx context.Context) Querier {
	return querierFromContext(ctx, r.db)
}

func (r *MySQLUsageRepository) Record(ctx context.Context, rec *usage.UsageRecord) error {
	s := rec.ToSnapshot()
	metadata, err := json.Marshal(s.Metadata)
	if err != nil {
		return fmt.Errorf("marshal usage metadata: %w", err)
	}

	var idempotencyKey *string
	if s.IdempotencyKey != "" {
		idempotencyKey = &s.IdempotencyKey
	}

	// Idempotent insert: a duplicate idempotency_key is a no-op (the existing
	// row wins), mirroring postgres' ON CONFLICT (idempotency_key) DO NOTHING.
	_, err = r.q(ctx).ExecContext(ctx,
		`INSERT INTO usage_records (id, contract_id, metric, quantity, timestamp, idempotency_key, metadata)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE id = id`,
		string(s.ID), string(s.ContractID), string(s.MetricName),
		s.Quantity, s.Timestamp.UTC(), idempotencyKey, metadata)
	if err != nil {
		return fmt.Errorf("record usage: %w", err)
	}
	return nil
}

func (r *MySQLUsageRepository) GetSummary(ctx context.Context, contractID shared.ContractID, metric shared.MetricName, period shared.DateRange) (*usage.UsageSummary, error) {
	var total int64
	err := r.q(ctx).QueryRowContext(ctx,
		`SELECT COALESCE(SUM(quantity), 0) FROM usage_records
		 WHERE contract_id = ? AND metric = ? AND timestamp >= ? AND timestamp < ?`,
		string(contractID), string(metric), period.Start().UTC(), period.End().UTC()).Scan(&total)
	if err != nil {
		return nil, fmt.Errorf("get usage summary: %w", err)
	}
	return &usage.UsageSummary{
		ContractID: contractID, MetricName: metric, Period: period, TotalUsage: total,
	}, nil
}

func (r *MySQLUsageRepository) GetRecords(ctx context.Context, contractID shared.ContractID, metric shared.MetricName, from, to time.Time) ([]*usage.UsageRecord, error) {
	rows, err := r.q(ctx).QueryContext(ctx,
		`SELECT id, contract_id, metric, quantity, timestamp, idempotency_key, metadata
		 FROM usage_records
		 WHERE contract_id = ? AND metric = ? AND timestamp >= ? AND timestamp < ?
		 ORDER BY timestamp ASC`,
		string(contractID), string(metric), from.UTC(), to.UTC())
	if err != nil {
		return nil, fmt.Errorf("get usage records: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var result []*usage.UsageRecord
	for rows.Next() {
		var (
			id, cID, metricStr string
			quantity           int64
			ts                 utcTime
			idempotencyKey     sql.NullString
			metadataRaw        json.RawMessage
		)
		if err := rows.Scan(&id, &cID, &metricStr, &quantity, &ts, &idempotencyKey, &metadataRaw); err != nil {
			return nil, fmt.Errorf("scan usage record: %w", err)
		}
		var metadata map[string]string
		if len(metadataRaw) > 0 {
			_ = json.Unmarshal(metadataRaw, &metadata)
		}
		snap := usage.UsageRecordSnapshot{
			ID: shared.UsageRecordID(id), ContractID: shared.ContractID(cID),
			MetricName: shared.MetricName(metricStr), Quantity: quantity,
			Timestamp: ts.Time, Metadata: metadata,
		}
		if idempotencyKey.Valid {
			snap.IdempotencyKey = idempotencyKey.String
		}
		rec, err := usage.FromSnapshot(snap)
		if err != nil {
			return nil, fmt.Errorf("reconstruct usage record: %w", err)
		}
		result = append(result, rec)
	}
	return result, rows.Err()
}
