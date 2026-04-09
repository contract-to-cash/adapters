package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/domain/contract"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/contract-to-cash/core/eventstore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresContractRepository implements contract.Repository using Event Sourcing.
// Save persists uncommitted events via EventStore.Append.
// FindByID restores the aggregate from snapshots + event replay.
// Read-model queries (FindExpiring, etc.) query the projection table, then
// restore full aggregates from the EventStore.
type PostgresContractRepository struct {
	pool       *pgxpool.Pool
	eventStore *PostgresEventStore
}

var _ contract.Repository = (*PostgresContractRepository)(nil)

func NewContractRepository(pool *pgxpool.Pool, es *PostgresEventStore) *PostgresContractRepository {
	return &PostgresContractRepository{pool: pool, eventStore: es}
}

// Save persists uncommitted domain events for the aggregate.
func (r *PostgresContractRepository) Save(ctx context.Context, aggregate *contract.ContractAggregate) error {
	changes := aggregate.UncommittedEvents()
	if len(changes) == 0 {
		return nil
	}

	es := r.txEventStore(ctx)
	streamID := contractStreamID(aggregate.ID())

	if err := es.Append(ctx, streamID, changes, aggregate.Version()-len(changes)); err != nil {
		return fmt.Errorf("append contract events: %w", err)
	}

	aggregate.ClearUncommittedEvents()
	return nil
}

// FindByID restores a ContractAggregate from snapshot + event replay.
func (r *PostgresContractRepository) FindByID(ctx context.Context, id shared.ContractID) (*contract.ContractAggregate, error) {
	es := r.txEventStore(ctx)
	streamID := contractStreamID(string(id))

	snap, err := es.LoadSnapshot(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("load snapshot: %w", err)
	}

	agg := contract.NewEmptyAggregate(id)

	if snap != nil {
		if err := agg.LoadFromSnapshot(*snap); err != nil {
			return nil, fmt.Errorf("restore from snapshot: %w", err)
		}
	}

	// Load only events after the snapshot version to avoid over-fetching.
	fromVersion := agg.Version()
	events, err := es.loadFromVersion(ctx, streamID, fromVersion)
	if err != nil {
		return nil, fmt.Errorf("load events: %w", err)
	}

	if snap == nil && len(events) == 0 {
		return nil, contract.ErrNotFound
	}

	if len(events) > 0 {
		if err := agg.LoadFromHistory(events); err != nil {
			return nil, fmt.Errorf("load from history: %w", err)
		}
	}

	return agg, nil
}

// FindByAccountID finds all contracts for an account.
func (r *PostgresContractRepository) FindByAccountID(ctx context.Context, accountID shared.AccountID) ([]*contract.ContractAggregate, error) {
	q := QuerierFromContext(ctx, r.pool)

	rows, err := q.Query(ctx,
		`SELECT id FROM contract_read_models WHERE account_id = $1`,
		string(accountID),
	)
	if err != nil {
		return nil, fmt.Errorf("query contracts by account: %w", err)
	}
	defer rows.Close()

	return r.loadAggregatesByRows(ctx, rows)
}

// FindExpiring finds contracts expiring before the given time.
func (r *PostgresContractRepository) FindExpiring(ctx context.Context, before time.Time) ([]*contract.ContractAggregate, error) {
	q := QuerierFromContext(ctx, r.pool)

	rows, err := q.Query(ctx,
		`SELECT id FROM contract_read_models
		 WHERE end_date IS NOT NULL AND end_date < $1 AND status = 'active'`,
		before,
	)
	if err != nil {
		return nil, fmt.Errorf("query expiring contracts: %w", err)
	}
	defer rows.Close()

	return r.loadAggregatesByRows(ctx, rows)
}

// FindTrialsEndingSoon finds contracts with trials ending before the given time.
func (r *PostgresContractRepository) FindTrialsEndingSoon(ctx context.Context, before time.Time) ([]*contract.ContractAggregate, error) {
	q := QuerierFromContext(ctx, r.pool)

	rows, err := q.Query(ctx,
		`SELECT id FROM contract_read_models
		 WHERE trial_end_date IS NOT NULL AND trial_end_date < $1 AND status = 'trial'`,
		before,
	)
	if err != nil {
		return nil, fmt.Errorf("query trials ending soon: %w", err)
	}
	defer rows.Close()

	return r.loadAggregatesByRows(ctx, rows)
}

// FindByIDAsOf restores a ContractAggregate as it was at the given point in time.
func (r *PostgresContractRepository) FindByIDAsOf(ctx context.Context, id shared.ContractID, asOf time.Time) (*contract.ContractAggregate, error) {
	es := r.txEventStore(ctx)
	streamID := contractStreamID(string(id))

	// Load snapshot before asOf
	snap, err := es.LoadSnapshotBefore(ctx, streamID, asOf)
	if err != nil {
		return nil, fmt.Errorf("load snapshot before: %w", err)
	}

	agg := contract.NewEmptyAggregate(id)

	if snap != nil {
		if err := agg.LoadFromSnapshot(*snap); err != nil {
			return nil, fmt.Errorf("restore from snapshot: %w", err)
		}
	}

	// Load events after snapshot version, up to asOf.
	// LoadUntil filters by occurred_at in SQL; we additionally filter by version
	// to skip events already applied from the snapshot.
	events, err := es.LoadUntil(ctx, streamID, asOf)
	if err != nil {
		return nil, fmt.Errorf("load events until: %w", err)
	}

	fromVersion := agg.Version()
	relevant := events[:0]
	for _, evt := range events {
		if evt.Version > fromVersion {
			relevant = append(relevant, evt)
		}
	}

	if snap == nil && len(relevant) == 0 {
		return nil, contract.ErrNotFound
	}

	if len(relevant) > 0 {
		if err := agg.LoadFromHistory(relevant); err != nil {
			return nil, fmt.Errorf("load from history: %w", err)
		}
	}

	return agg, nil
}

// FindDueForRenewal finds contracts due for renewal as of the given time.
func (r *PostgresContractRepository) FindDueForRenewal(ctx context.Context, asOf time.Time) ([]*contract.ContractAggregate, error) {
	q := QuerierFromContext(ctx, r.pool)

	rows, err := q.Query(ctx,
		`SELECT id FROM contract_read_models
		 WHERE renewal_date IS NOT NULL AND renewal_date <= $1 AND status = 'active'`,
		asOf,
	)
	if err != nil {
		return nil, fmt.Errorf("query renewal contracts: %w", err)
	}
	defer rows.Close()

	return r.loadAggregatesByRows(ctx, rows)
}

// loadAggregatesByRows takes rows containing IDs, and restores each aggregate from EventStore.
func (r *PostgresContractRepository) loadAggregatesByRows(ctx context.Context, rows pgx.Rows) ([]*contract.ContractAggregate, error) {
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

	aggregates := make([]*contract.ContractAggregate, 0, len(ids))
	for _, id := range ids {
		agg, err := r.FindByID(ctx, shared.ContractID(id))
		if err != nil {
			return nil, fmt.Errorf("find contract %s: %w", id, err)
		}
		aggregates = append(aggregates, agg)
	}
	return aggregates, nil
}

func (r *PostgresContractRepository) txEventStore(ctx context.Context) *PostgresEventStore {
	if tx, ok := TxFromContext(ctx); ok {
		return r.eventStore.WithTx(tx)
	}
	return r.eventStore
}

func contractStreamID(id string) string {
	return "contract-" + id
}
