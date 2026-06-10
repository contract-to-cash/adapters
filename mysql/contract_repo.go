package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/domain/contract"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/contract-to-cash/core/eventstore"
)

// MySQLContractRepository implements contract.Repository using the event store
// plus the contract_read_models projection table.
type MySQLContractRepository struct {
	db         *sql.DB
	eventStore *EventStore
	clock      shared.Clock
}

var _ contract.Repository = (*MySQLContractRepository)(nil)

// NewContractRepository constructs a contract repository over the event store
// and the contract_read_models read model.
func NewContractRepository(db *sql.DB, es *EventStore, clock shared.Clock) *MySQLContractRepository {
	return &MySQLContractRepository{db: db, eventStore: es, clock: clock}
}

func (r *MySQLContractRepository) q(ctx context.Context) Querier {
	return querierFromContext(ctx, r.db)
}

func (r *MySQLContractRepository) Save(ctx context.Context, aggregate *contract.ContractAggregate) error {
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

func (r *MySQLContractRepository) FindByID(ctx context.Context, id shared.ContractID) (*contract.ContractAggregate, error) {
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

func (r *MySQLContractRepository) loadEventsAfter(ctx context.Context, streamID string, afterVersion int) ([]eventstore.Event, error) {
	rows, err := r.q(ctx).QueryContext(ctx,
		"SELECT "+eventColumns+" FROM events WHERE stream_id = ? AND version > ? ORDER BY version ASC",
		streamID, afterVersion)
	if err != nil {
		return nil, fmt.Errorf("query events after version: %w", err)
	}
	return scanEvents(rows)
}

func (r *MySQLContractRepository) FindByAccountID(ctx context.Context, accountID shared.AccountID) ([]*contract.ContractAggregate, error) {
	return r.findManyFromReadModel(ctx,
		`SELECT id FROM contract_read_models WHERE account_id = ?`, string(accountID))
}

func (r *MySQLContractRepository) FindExpiring(ctx context.Context, before time.Time) ([]*contract.ContractAggregate, error) {
	return r.findManyFromReadModel(ctx,
		`SELECT id FROM contract_read_models
		 WHERE end_date IS NOT NULL AND end_date < ? AND status = 'active'`, before.UTC())
}

func (r *MySQLContractRepository) FindTrialsEndingSoon(ctx context.Context, before time.Time) ([]*contract.ContractAggregate, error) {
	return r.findManyFromReadModel(ctx,
		`SELECT id FROM contract_read_models
		 WHERE trial_end_date IS NOT NULL AND trial_end_date < ? AND status = 'trialing'`, before.UTC())
}

func (r *MySQLContractRepository) FindByIDAsOf(ctx context.Context, id shared.ContractID, asOf time.Time) (*contract.ContractAggregate, error) {
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

func (r *MySQLContractRepository) FindDueForRenewal(ctx context.Context, asOf time.Time) ([]*contract.ContractAggregate, error) {
	return r.findManyFromReadModel(ctx,
		`SELECT id FROM contract_read_models
		 WHERE renewal_date IS NOT NULL AND renewal_date <= ? AND status = 'active'`, asOf.UTC())
}

func (r *MySQLContractRepository) findManyFromReadModel(ctx context.Context, query string, args ...any) ([]*contract.ContractAggregate, error) {
	rows, err := r.q(ctx).QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query contract read model: %w", err)
	}
	defer func() { _ = rows.Close() }()

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
			return nil, fmt.Errorf("rehydrate contract %s: %w", id, err)
		}
		result = append(result, agg)
	}
	return result, nil
}
