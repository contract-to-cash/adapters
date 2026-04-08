package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/domain/contract"
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
func (r *PostgresContractRepository) Save(ctx context.Context, aggregate *contract.Aggregate) error {
	changes := aggregate.Changes()
	if len(changes) == 0 {
		return nil
	}

	es := r.txEventStore(ctx)
	streamID := contractStreamID(aggregate.ID())

	if err := es.Append(ctx, streamID, changes, aggregate.Version()-len(changes)); err != nil {
		return fmt.Errorf("append contract events: %w", err)
	}

	aggregate.ClearChanges()
	return nil
}

// FindByID restores a ContractAggregate from snapshot + event replay.
func (r *PostgresContractRepository) FindByID(ctx context.Context, id string) (*contract.Aggregate, error) {
	es := r.txEventStore(ctx)
	streamID := contractStreamID(id)

	// Try loading snapshot first
	snap, err := es.LoadSnapshot(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("load snapshot: %w", err)
	}

	var agg *contract.Aggregate
	fromVersion := 0

	if snap != nil {
		state, ok := snap.State.(json.RawMessage)
		if !ok {
			stateBytes, err := json.Marshal(snap.State)
			if err != nil {
				return nil, fmt.Errorf("marshal snapshot state: %w", err)
			}
			state = stateBytes
		}
		agg, err = contract.RestoreFromSnapshot(state, snap.Version)
		if err != nil {
			return nil, fmt.Errorf("restore from snapshot: %w", err)
		}
		fromVersion = snap.Version
	}

	// Replay events after snapshot
	events, err := es.LoadFrom(ctx, streamID, fromVersion)
	if err != nil {
		return nil, fmt.Errorf("load events: %w", err)
	}

	if agg == nil && len(events) == 0 {
		return nil, contract.ErrNotFound
	}

	if agg == nil {
		agg = contract.NewEmptyAggregate(id)
	}

	for _, evt := range events {
		if err := agg.Apply(evt); err != nil {
			return nil, fmt.Errorf("apply event %s: %w", evt.ID, err)
		}
	}

	return agg, nil
}

// FindExpiring finds contracts expiring before the given time.
// Queries the read model, then restores full aggregates from EventStore.
func (r *PostgresContractRepository) FindExpiring(ctx context.Context, before time.Time) ([]*contract.Aggregate, error) {
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

// FindDueForRenewal finds contracts due for renewal before the given time.
func (r *PostgresContractRepository) FindDueForRenewal(ctx context.Context, before time.Time) ([]*contract.Aggregate, error) {
	q := QuerierFromContext(ctx, r.pool)

	rows, err := q.Query(ctx,
		`SELECT id FROM contract_read_models
		 WHERE renewal_date IS NOT NULL AND renewal_date < $1 AND status = 'active'`,
		before,
	)
	if err != nil {
		return nil, fmt.Errorf("query renewal contracts: %w", err)
	}
	defer rows.Close()

	return r.loadAggregatesByRows(ctx, rows)
}

// FindByAccountID finds all contracts for an account.
func (r *PostgresContractRepository) FindByAccountID(ctx context.Context, accountID string) ([]*contract.Aggregate, error) {
	q := QuerierFromContext(ctx, r.pool)

	rows, err := q.Query(ctx,
		`SELECT id FROM contract_read_models WHERE account_id = $1`,
		accountID,
	)
	if err != nil {
		return nil, fmt.Errorf("query contracts by account: %w", err)
	}
	defer rows.Close()

	return r.loadAggregatesByRows(ctx, rows)
}

// loadAggregatesByRows takes rows containing IDs, and restores each aggregate from EventStore.
func (r *PostgresContractRepository) loadAggregatesByRows(ctx context.Context, rows pgx.Rows) ([]*contract.Aggregate, error) {
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

	aggregates := make([]*contract.Aggregate, 0, len(ids))
	for _, id := range ids {
		agg, err := r.FindByID(ctx, id)
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
