package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/domain/shared"
	"github.com/contract-to-cash/core/domain/usage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresUsageRepository implements usage.Repository.
// Record is idempotent via idempotency_key (ON CONFLICT DO NOTHING).
type PostgresUsageRepository struct {
	pool *pgxpool.Pool
}

var _ usage.Repository = (*PostgresUsageRepository)(nil)

func NewUsageRepository(pool *pgxpool.Pool) *PostgresUsageRepository {
	return &PostgresUsageRepository{pool: pool}
}

func (r *PostgresUsageRepository) q(ctx context.Context) Querier {
	return QuerierFromContext(ctx, r.pool)
}

// Record idempotently records a usage event. Duplicate idempotency keys are ignored.
func (r *PostgresUsageRepository) Record(ctx context.Context, rec *usage.UsageRecord) error {
	q := r.q(ctx)

	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal usage record: %w", err)
	}

	_, err = q.Exec(ctx,
		`INSERT INTO usage_records (id, contract_id, metric, quantity, timestamp, idempotency_key, data)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (idempotency_key) DO NOTHING`,
		string(rec.ID()), string(rec.ContractID()), string(rec.Metric()),
		rec.Quantity(), rec.Timestamp(), rec.IdempotencyKey(), data,
	)
	if err != nil {
		return fmt.Errorf("record usage: %w", err)
	}
	return nil
}

// GetSummary returns an aggregated usage summary for a contract+metric within a date range.
func (r *PostgresUsageRepository) GetSummary(ctx context.Context, contractID shared.ContractID, metric shared.MetricName, period shared.DateRange) (*usage.UsageSummary, error) {
	q := r.q(ctx)

	var totalQuantity int64
	var count int64
	err := q.QueryRow(ctx,
		`SELECT COALESCE(SUM(quantity), 0), COUNT(*)
		 FROM usage_records
		 WHERE contract_id = $1 AND metric = $2 AND timestamp >= $3 AND timestamp < $4`,
		string(contractID), string(metric), period.From, period.To,
	).Scan(&totalQuantity, &count)
	if err != nil {
		if err == pgx.ErrNoRows {
			return usage.NewSummary(contractID, metric, period, 0, 0), nil
		}
		return nil, fmt.Errorf("get usage summary: %w", err)
	}

	return usage.NewSummary(contractID, metric, period, totalQuantity, count), nil
}

// GetRecords returns individual usage records for a contract+metric within a time range.
func (r *PostgresUsageRepository) GetRecords(ctx context.Context, contractID shared.ContractID, metric shared.MetricName, from, to time.Time) ([]*usage.UsageRecord, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM usage_records
		 WHERE contract_id = $1 AND metric = $2 AND timestamp >= $3 AND timestamp < $4
		 ORDER BY timestamp ASC`,
		string(contractID), string(metric), from, to,
	)
	if err != nil {
		return nil, fmt.Errorf("get usage records: %w", err)
	}
	defer rows.Close()

	var records []*usage.UsageRecord
	for rows.Next() {
		var data json.RawMessage
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("scan usage record: %w", err)
		}
		rec, err := usage.Unmarshal(data)
		if err != nil {
			return nil, fmt.Errorf("unmarshal usage record: %w", err)
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return records, nil
}
