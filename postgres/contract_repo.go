package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/domain/contract"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/contract-to-cash/core/eventstore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresContractRepository implements contract.Repository using the
// event store plus the contract_read_models projection table.
//
// Save appends the aggregate's uncommitted events to the event store and
// updates its version. FindByID restores from snapshot + event replay.
// Read-model queries (FindByAccountID, FindExpiring, ...) hit the
// projection table to get IDs, then rehydrate each aggregate from events.
//
// The adapter owns a shared.Clock because NewContractAggregate requires one
// — the clock is threaded through to newly constructed aggregates when
// rebuilding them from snapshots + events. In production, pass
// shared.SystemClock{}; in tests, pass shared.FixedClock{}.
type PostgresContractRepository struct {
	pool       *pgxpool.Pool
	eventStore *PostgresEventStore
	clock      shared.Clock
}

var _ contract.Repository = (*PostgresContractRepository)(nil)

// NewContractRepository creates a new PostgresContractRepository.
func NewContractRepository(pool *pgxpool.Pool, es *PostgresEventStore, clock shared.Clock) *PostgresContractRepository {
	return &PostgresContractRepository{pool: pool, eventStore: es, clock: clock}
}

func (r *PostgresContractRepository) q(ctx context.Context) Querier {
	return QuerierFromContext(ctx, r.pool)
}

// Save persists the aggregate's uncommitted events via the event store and
// updates the committed version on success.
//
// The expectedVersion for the append is the aggregate's current persisted
// version (i.e. the version before the uncommitted events). Core's
// BaseAggregate.Version() returns exactly that. On successful append we
// advance Version by len(events) — the same pattern as the in-memory
// reference implementation.
func (r *PostgresContractRepository) Save(ctx context.Context, aggregate *contract.ContractAggregate) error {
	events := aggregate.UncommittedEvents()
	if len(events) == 0 {
		return nil
	}

	expectedVersion := aggregate.Version()
	if err := r.eventStore.Append(ctx, aggregate.ID(), events, expectedVersion); err != nil {
		return err
	}

	aggregate.SetVersion(expectedVersion + len(events))
	aggregate.ClearUncommittedEvents()
	return nil
}

// FindByID restores a contract aggregate from the latest snapshot + events.
func (r *PostgresContractRepository) FindByID(ctx context.Context, id shared.ContractID) (*contract.ContractAggregate, error) {
	streamID := string(id)

	snap, err := r.eventStore.LoadSnapshot(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("load snapshot: %w", err)
	}

	agg := contract.NewContractAggregate(id, r.clock)

	if snap != nil {
		if err := agg.LoadFromSnapshot(*snap); err != nil {
			return nil, fmt.Errorf("restore from snapshot: %w", err)
		}
	}

	// Load events strictly after the snapshot version.
	events, err := r.loadEventsAfter(ctx, streamID, agg.Version())
	if err != nil {
		return nil, fmt.Errorf("load events: %w", err)
	}

	if snap == nil && len(events) == 0 {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound,
			fmt.Sprintf("contract %s not found", id))
	}

	if len(events) > 0 {
		if err := agg.LoadFromHistory(events); err != nil {
			return nil, fmt.Errorf("load from history: %w", err)
		}
	}

	return agg, nil
}

// loadEventsAfter returns events with version > afterVersion for the stream.
// Implemented on the repository rather than the event store because it is an
// internal optimization — the public Store interface only exposes
// LoadUntilVersion / LoadUntil / LoadRange / Load, none of which express
// "strictly greater than".
func (r *PostgresContractRepository) loadEventsAfter(ctx context.Context, streamID string, afterVersion int) ([]eventstore.Event, error) {
	rows, err := r.q(ctx).Query(ctx,
		`SELECT id, stream_id, type, version, schema_version, data, metadata, occurred_at, recorded_at, global_position
		 FROM events WHERE stream_id = $1 AND version > $2 ORDER BY version ASC`,
		streamID, afterVersion,
	)
	if err != nil {
		return nil, fmt.Errorf("query events after version: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

// FindByAccountID returns contracts for the given account using the read model.
func (r *PostgresContractRepository) FindByAccountID(ctx context.Context, accountID shared.AccountID) ([]*contract.ContractAggregate, error) {
	return r.findManyFromReadModel(ctx,
		`SELECT id FROM contract_read_models WHERE account_id = $1`,
		string(accountID),
	)
}

// FindExpiring returns active contracts whose end_date is before `before`.
func (r *PostgresContractRepository) FindExpiring(ctx context.Context, before time.Time) ([]*contract.ContractAggregate, error) {
	return r.findManyFromReadModel(ctx,
		`SELECT id FROM contract_read_models
		 WHERE end_date IS NOT NULL AND end_date < $1 AND status = 'active'`,
		before,
	)
}

// FindTrialsEndingSoon returns trialing contracts whose trial_end_date is before `before`.
func (r *PostgresContractRepository) FindTrialsEndingSoon(ctx context.Context, before time.Time) ([]*contract.ContractAggregate, error) {
	return r.findManyFromReadModel(ctx,
		`SELECT id FROM contract_read_models
		 WHERE trial_end_date IS NOT NULL AND trial_end_date < $1 AND status = 'trialing'`,
		before,
	)
}

// FindByIDAsOf restores a contract as it existed at the given point in time.
func (r *PostgresContractRepository) FindByIDAsOf(ctx context.Context, id shared.ContractID, asOf time.Time) (*contract.ContractAggregate, error) {
	streamID := string(id)

	snap, err := r.eventStore.LoadSnapshotBefore(ctx, streamID, asOf)
	if err != nil {
		return nil, fmt.Errorf("load snapshot before: %w", err)
	}

	agg := contract.NewContractAggregate(id, r.clock)

	if snap != nil {
		if err := agg.LoadFromSnapshot(*snap); err != nil {
			return nil, fmt.Errorf("restore from snapshot: %w", err)
		}
	}

	// LoadUntil returns every event with occurred_at <= asOf. We still
	// need to filter out events at or before the snapshot version.
	events, err := r.eventStore.LoadUntil(ctx, streamID, asOf)
	if err != nil {
		return nil, fmt.Errorf("load events until: %w", err)
	}

	fromVersion := agg.Version()
	relevant := make([]eventstore.Event, 0, len(events))
	for _, e := range events {
		if e.Version > fromVersion {
			relevant = append(relevant, e)
		}
	}

	if snap == nil && len(relevant) == 0 {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound,
			fmt.Sprintf("contract %s not found as of %s", id, asOf))
	}

	if len(relevant) > 0 {
		if err := agg.LoadFromHistory(relevant); err != nil {
			return nil, fmt.Errorf("load from history: %w", err)
		}
	}

	return agg, nil
}

// FindDueForRenewal returns active contracts whose current period ends on or before asOf.
func (r *PostgresContractRepository) FindDueForRenewal(ctx context.Context, asOf time.Time) ([]*contract.ContractAggregate, error) {
	return r.findManyFromReadModel(ctx,
		`SELECT id FROM contract_read_models
		 WHERE renewal_date IS NOT NULL AND renewal_date <= $1 AND status = 'active'`,
		asOf,
	)
}

// findManyFromReadModel runs an ID-only query against contract_read_models
// and rehydrates each aggregate via the event store. This is intentionally
// straightforward (N+1 loads) — the reference adapter prioritizes
// readability over batching optimizations.
func (r *PostgresContractRepository) findManyFromReadModel(ctx context.Context, sql string, args ...any) ([]*contract.ContractAggregate, error) {
	rows, err := r.q(ctx).Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query contract read model: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan contract id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	result := make([]*contract.ContractAggregate, 0, len(ids))
	for _, id := range ids {
		agg, err := r.FindByID(ctx, shared.ContractID(id))
		if err != nil {
			// Missing aggregate is treated as a data-integrity issue: the
			// read model says it exists but the event store disagrees.
			// Surface rather than silently skip.
			return nil, fmt.Errorf("rehydrate contract %s: %w", id, err)
		}
		result = append(result, agg)
	}
	return result, nil
}

// loadContractSnapshotState is intentionally unused in repository code but
// kept close by as a reference for what the contract snapshot JSON looks
// like — projectors that need to decode parts of it can borrow this type.
// It is not exported because it mirrors a private core type.
func loadContractSnapshotState(data []byte) (map[string]any, error) {
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// silence pgx import linter when only used transitively.
var _ = pgx.ErrNoRows

// silence unused-function linter for loadContractSnapshotState (kept for docs).
var _ = loadContractSnapshotState

// silence errors import if none of the above use it directly.
var _ = errors.Is
