package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/domain/shared"
	"github.com/contract-to-cash/core/domain/usage"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

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

func (r *PostgresUsageRepository) Record(ctx context.Context, rec *usage.UsageRecord) error {
	s := rec.ToSnapshot()
	metadata, err := json.Marshal(s.Metadata)
	if err != nil {
		return fmt.Errorf("marshal usage metadata: %w", err)
	}

	var idempotencyKey *string
	if s.IdempotencyKey != "" {
		idempotencyKey = &s.IdempotencyKey
	}

	// A duplicate idempotency_key must surface as the core's
	// DomainError(duplicate_request) — matching the reference in-memory
	// implementation (core/infrastructure/inmemory/usage_repository.go) that
	// integrators test against — rather than being silently swallowed with
	// `ON CONFLICT DO NOTHING` (issue #38). A caller who wants ignore-duplicates
	// semantics can errors.As the DomainError and check its Code; a caller who
	// needs the signal cannot recover it from a silent success. A NULL
	// idempotency_key never conflicts (Postgres treats NULLs as distinct), so
	// records without a key are inserted unconditionally, matching inmemory.
	_, err = r.q(ctx).Exec(ctx,
		`INSERT INTO usage_records (id, contract_id, metric, quantity, timestamp, idempotency_key, metadata)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		string(s.ID), string(s.ContractID), string(s.MetricName),
		s.Quantity, s.Timestamp, idempotencyKey, metadata)
	if err != nil {
		if isUsageIdempotencyKeyConflict(err) {
			return shared.NewDomainError(shared.ErrCodeDuplicateRequest,
				fmt.Sprintf("duplicate usage record with idempotency key %s", s.IdempotencyKey))
		}
		return fmt.Errorf("record usage: %w", err)
	}
	return nil
}

// usageRecordsIdempotencyKeyConstraint is the auto-generated name of the UNIQUE
// constraint on usage_records.idempotency_key (Postgres names a column-level
// UNIQUE constraint <table>_<column>_key). Matching on the constraint name — not
// the bare 23505 code — keeps an unrelated UNIQUE violation from being
// misreported as a duplicate idempotency key (mirrors isIdempotencyKeyConflict
// in payment_repo.go and isVersionConflict in eventstore.go).
const usageRecordsIdempotencyKeyConstraint = "usage_records_idempotency_key_key"

// isUsageIdempotencyKeyConflict reports whether err is a unique-violation
// (23505) specifically on the usage_records idempotency_key constraint.
func isUsageIdempotencyKeyConflict(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgUniqueViolation &&
			pgErr.ConstraintName == usageRecordsIdempotencyKeyConstraint
	}
	return false
}

func (r *PostgresUsageRepository) GetSummary(ctx context.Context, contractID shared.ContractID, metric shared.MetricName, period shared.DateRange) (*usage.UsageSummary, error) {
	var total int64
	err := r.q(ctx).QueryRow(ctx,
		`SELECT COALESCE(SUM(quantity), 0) FROM usage_records
		 WHERE contract_id = $1 AND metric = $2 AND timestamp >= $3 AND timestamp < $4`,
		string(contractID), string(metric), period.Start(), period.End()).Scan(&total)
	if err != nil {
		return nil, fmt.Errorf("get usage summary: %w", err)
	}
	return &usage.UsageSummary{
		ContractID: contractID, MetricName: metric, Period: period, TotalUsage: total,
	}, nil
}

func (r *PostgresUsageRepository) GetRecords(ctx context.Context, contractID shared.ContractID, metric shared.MetricName, from, to time.Time) ([]*usage.UsageRecord, error) {
	rows, err := r.q(ctx).Query(ctx,
		`SELECT id, contract_id, metric, quantity, timestamp, idempotency_key, metadata
		 FROM usage_records
		 WHERE contract_id = $1 AND metric = $2 AND timestamp >= $3 AND timestamp < $4
		 ORDER BY timestamp ASC`,
		string(contractID), string(metric), from, to)
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
			_ = json.Unmarshal(metadataRaw, &metadata)
		}
		snap := usage.UsageRecordSnapshot{
			ID: shared.UsageRecordID(id), ContractID: shared.ContractID(cID),
			MetricName: shared.MetricName(metricStr), Quantity: quantity,
			Timestamp: ts, Metadata: metadata,
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
	return result, rows.Err()
}
