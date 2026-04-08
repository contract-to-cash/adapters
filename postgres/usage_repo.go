package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/domain/usage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresUsageRepository implements usage.Repository.
// Read-only in the billing flow; Record is idempotent via idempotency_key.
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
		`INSERT INTO usage_records (id, subscription_id, meter_id, quantity, timestamp, idempotency_key, data)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (idempotency_key) DO NOTHING`,
		rec.ID(), rec.SubscriptionID(), rec.MeterID(), rec.Quantity(),
		rec.Timestamp(), rec.IdempotencyKey(), data,
	)
	if err != nil {
		return fmt.Errorf("record usage: %w", err)
	}
	return nil
}

func (r *PostgresUsageRepository) FindByID(ctx context.Context, id string) (*usage.UsageRecord, error) {
	q := r.q(ctx)

	var data json.RawMessage
	err := q.QueryRow(ctx,
		`SELECT data FROM usage_records WHERE id = $1`, id,
	).Scan(&data)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, usage.ErrNotFound
		}
		return nil, fmt.Errorf("find usage record: %w", err)
	}

	return usage.Unmarshal(data)
}

func (r *PostgresUsageRepository) FindBySubscriptionID(ctx context.Context, subscriptionID string, from, to time.Time) ([]*usage.UsageRecord, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM usage_records
		 WHERE subscription_id = $1 AND timestamp >= $2 AND timestamp < $3
		 ORDER BY timestamp ASC`,
		subscriptionID, from, to,
	)
	if err != nil {
		return nil, fmt.Errorf("find usage by subscription: %w", err)
	}
	defer rows.Close()

	return scanUsageRecords(rows)
}

func (r *PostgresUsageRepository) FindByMeterID(ctx context.Context, meterID string, from, to time.Time) ([]*usage.UsageRecord, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM usage_records
		 WHERE meter_id = $1 AND timestamp >= $2 AND timestamp < $3
		 ORDER BY timestamp ASC`,
		meterID, from, to,
	)
	if err != nil {
		return nil, fmt.Errorf("find usage by meter: %w", err)
	}
	defer rows.Close()

	return scanUsageRecords(rows)
}

func scanUsageRecords(rows pgx.Rows) ([]*usage.UsageRecord, error) {
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
