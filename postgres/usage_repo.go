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
//
// Record writes are idempotent via the unique idempotency_key column:
// duplicate keys are silently ignored via ON CONFLICT DO NOTHING.
type PostgresUsageRepository struct {
	pool *pgxpool.Pool
}

var _ usage.Repository = (*PostgresUsageRepository)(nil)

// NewUsageRepository creates a new PostgresUsageRepository.
func NewUsageRepository(pool *pgxpool.Pool) *PostgresUsageRepository {
	return &PostgresUsageRepository{pool: pool}
}

func (r *PostgresUsageRepository) q(ctx context.Context) Querier {
	return QuerierFromContext(ctx, r.pool)
}

// Record idempotently records a usage event.
func (r *PostgresUsageRepository) Record(ctx context.Context, rec *usage.UsageRecord) error {
	s := rec.ToSnapshot()

	metadata, err := json.Marshal(s.Metadata)
	if err != nil {
		return fmt.Errorf("marshal usage metadata: %w", err)
	}

	var idempotencyKey *string
	if s.IdempotencyKey != "" {
		v := s.IdempotencyKey
		idempotencyKey = &v
	}

	_, err = r.q(ctx).Exec(ctx,
		`INSERT INTO usage_records (id, contract_id, metric, quantity, timestamp, idempotency_key, metadata)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (idempotency_key) DO NOTHING`,
		string(s.ID), string(s.ContractID), string(s.MetricName),
		s.Quantity, s.Timestamp, idempotencyKey, metadata,
	)
	if err != nil {
		return fmt.Errorf("record usage: %w", err)
	}
	return nil
}

// GetSummary returns an aggregated usage summary.
func (r *PostgresUsageRepository) GetSummary(ctx context.Context, contractID shared.ContractID, metric shared.MetricName, period shared.DateRange) (*usage.UsageSummary, error) {
	var total int64
	err := r.q(ctx).QueryRow(ctx,
		`SELECT COALESCE(SUM(quantity), 0)
		 FROM usage_records
		 WHERE contract_id = $1 AND metric = $2 AND timestamp >= $3 AND timestamp < $4`,
		string(contractID), string(metric), period.Start(), period.End(),
	).Scan(&total)
	if err != nil {
		return nil, fmt.Errorf("get usage summary: %w", err)
	}
	return &usage.UsageSummary{
		ContractID: contractID,
		MetricName: metric,
		Period:     period,
		TotalUsage: total,
	}, nil
}

// GetRecords returns individual usage records within a time range.
func (r *PostgresUsageRepository) GetRecords(ctx context.Context, contractID shared.ContractID, metric shared.MetricName, from, to time.Time) ([]*usage.UsageRecord, error) {
	rows, err := r.q(ctx).Query(ctx,
		`SELECT id, contract_id, metric, quantity, timestamp, idempotency_key, metadata
		 FROM usage_records
		 WHERE contract_id = $1 AND metric = $2 AND timestamp >= $3 AND timestamp < $4
		 ORDER BY timestamp ASC`,
		string(contractID), string(metric), from, to,
	)
	if err != nil {
		return nil, fmt.Errorf("get usage records: %w", err)
	}
	defer rows.Close()

	var result []*usage.UsageRecord
	for rows.Next() {
		var (
			id, cID, metricStr string
			quantity           int64
			ts                 time.Time
			idempotencyKey     *string
			metadataRaw        json.RawMessage
		)
		if err := rows.Scan(&id, &cID, &metricStr, &quantity, &ts, &idempotencyKey, &metadataRaw); err != nil {
			return nil, fmt.Errorf("scan usage record: %w", err)
		}

		var metadata map[string]string
		if len(metadataRaw) > 0 {
			if err := json.Unmarshal(metadataRaw, &metadata); err != nil {
				return nil, fmt.Errorf("unmarshal usage metadata: %w", err)
			}
		}

		snap := usage.UsageRecordSnapshot{
			ID:         shared.UsageRecordID(id),
			ContractID: shared.ContractID(cID),
			MetricName: shared.MetricName(metricStr),
			Quantity:   quantity,
			Timestamp:  ts,
			Metadata:   metadata,
		}
		if idempotencyKey != nil {
			snap.IdempotencyKey = *idempotencyKey
		}
		rec, err := usage.FromSnapshot(snap)
		if err != nil {
			return nil, fmt.Errorf("reconstruct usage record: %w", err)
		}
		result = append(result, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return result, nil
}

// Ensure pgx is referenced so the import is used if we later need pgx.ErrNoRows.
var _ = pgx.ErrNoRows
