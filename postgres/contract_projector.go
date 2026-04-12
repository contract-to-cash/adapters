package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/application/projection"
	"github.com/contract-to-cash/core/domain/contract"
	"github.com/contract-to-cash/core/eventstore"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ContractProjectorName is the checkpoint name used by ContractProjector.
const ContractProjectorName = "contract"

// ContractProjector maintains the contract_read_models projection table.
//
// It implements application/projection.Projector. Project handles a single
// event; Rebuild replays all events up to a timestamp inside a transaction
// with DEFERRABLE constraints so the projection rebuild can DELETE and
// re-insert without violating FKs pointing at contract_read_models.
type ContractProjector struct {
	pool       *pgxpool.Pool
	eventStore *PostgresEventStore
	checkpoint *CheckpointStore
}

var _ projection.Projector = (*ContractProjector)(nil)

// NewContractProjector creates a new ContractProjector.
func NewContractProjector(pool *pgxpool.Pool, es *PostgresEventStore, cp *CheckpointStore) *ContractProjector {
	return &ContractProjector{pool: pool, eventStore: es, checkpoint: cp}
}

// Project applies a single event to contract_read_models if it belongs to
// the contract domain. Non-contract events are ignored.
func (p *ContractProjector) Project(ctx context.Context, event eventstore.Event) error {
	switch event.Type {
	case contract.EventTypeContractCreated,
		contract.EventTypeContractActivated,
		contract.EventTypeContractSuspended,
		contract.EventTypeContractResumed,
		contract.EventTypeContractCancelled,
		contract.EventTypeContractRenewed,
		contract.EventTypeContractExpired,
		contract.EventTypeTrialStarted,
		contract.EventTypeTrialEnded,
		contract.EventTypePriceChanged,
		contract.EventTypePriceChangeScheduled,
		contract.EventTypePriceChangeUnscheduled,
		contract.EventTypeCancellationScheduled,
		contract.EventTypeCancellationUnscheduled,
		contract.EventTypePaymentMethodChanged:
	default:
		return nil
	}
	return p.applyEvent(ctx, event)
}

// Rebuild drops contract_read_models and re-replays all events up to
// `until`. Runs inside a transaction with DEFERRABLE constraints so FKs from
// other tables (invoices, credit_notes, usage_records) to
// contract_read_models remain valid at COMMIT.
func (p *ContractProjector) Rebuild(ctx context.Context, until time.Time) error {
	// If we're already inside an outer transaction we can reuse it; otherwise
	// open one here. SET CONSTRAINTS ALL DEFERRED is a no-op outside a tx.
	_, inTx := TxFromContext(ctx)
	if !inTx {
		tx, err := p.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin rebuild tx: %w", err)
		}
		txCtx := ContextWithTx(ctx, tx)
		if err := p.rebuildInTx(txCtx, until); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit rebuild tx: %w", err)
		}
		return nil
	}
	return p.rebuildInTx(ctx, until)
}

func (p *ContractProjector) rebuildInTx(ctx context.Context, until time.Time) error {
	q := QuerierFromContext(ctx, p.pool)

	if _, err := q.Exec(ctx, `SET CONSTRAINTS ALL DEFERRED`); err != nil {
		return fmt.Errorf("defer constraints: %w", err)
	}
	if _, err := q.Exec(ctx, `DELETE FROM contract_read_models`); err != nil {
		return fmt.Errorf("delete contract read models: %w", err)
	}
	if err := p.checkpoint.Reset(ctx, ContractProjectorName); err != nil {
		return fmt.Errorf("reset checkpoint: %w", err)
	}

	// Replay every event up to `until` in global_position order.
	var fromPosition int64
	var lastPosition int64
	const batch = 1000
	for {
		events, err := p.eventStore.LoadAll(ctx, fromPosition, batch)
		if err != nil {
			return fmt.Errorf("load events: %w", err)
		}
		if len(events) == 0 {
			break
		}

		for _, e := range events {
			if !e.OccurredAt.After(until) {
				if err := p.Project(ctx, e); err != nil {
					return fmt.Errorf("project event %s: %w", e.ID, err)
				}
			}
			lastPosition = e.GlobalPosition
		}

		newPos := events[len(events)-1].GlobalPosition
		if newPos == fromPosition {
			return fmt.Errorf("LoadAll did not advance global position (stuck at %d)", fromPosition)
		}
		fromPosition = newPos
	}

	if lastPosition > 0 {
		if err := p.checkpoint.Save(ctx, ContractProjectorName, lastPosition); err != nil {
			return fmt.Errorf("save checkpoint: %w", err)
		}
	}
	return nil
}

// applyEvent mutates contract_read_models in response to a single event.
//
// The projector does minimal bookkeeping: it updates `status`, critical
// date fields (start_date, end_date, renewal_date, trial_end_date) and
// version, and persists the raw event payload in `data` so that queries
// can inspect additional event-specific fields as needed. This keeps the
// projector code short and lets us defer schema decisions about optional
// fields until a query actually needs them.
func (p *ContractProjector) applyEvent(ctx context.Context, event eventstore.Event) error {
	q := QuerierFromContext(ctx, p.pool)

	contractID := event.StreamID

	var data map[string]any
	if len(event.Data) > 0 {
		if err := json.Unmarshal(event.Data, &data); err != nil {
			return fmt.Errorf("unmarshal event data: %w", err)
		}
	}

	raw := event.Data

	switch event.Type {
	case contract.EventTypeContractCreated:
		accountID, _ := data["account_id"].(string)
		priceID, _ := data["price_id"].(string)
		createdAt, _ := data["created_at"].(string)
		_, err := q.Exec(ctx,
			`INSERT INTO contract_read_models (id, account_id, status, start_date, price_id, data, version, updated_at)
			 VALUES ($1, $2, 'draft', $3, $4, $5, $6, NOW())
			 ON CONFLICT (id) DO UPDATE SET
			   account_id = EXCLUDED.account_id,
			   status     = 'draft',
			   start_date = EXCLUDED.start_date,
			   price_id   = EXCLUDED.price_id,
			   data       = EXCLUDED.data,
			   version    = EXCLUDED.version,
			   updated_at = NOW()`,
			contractID, accountID, parseTime(createdAt), priceID, raw, event.Version,
		)
		return err

	case contract.EventTypeContractActivated:
		var endDate *time.Time
		if periodRaw, ok := data["current_period"].(map[string]any); ok {
			if endStr, ok := periodRaw["end"].(string); ok {
				if t := parseTime(endStr); t != nil {
					endDate = t
				}
			}
		}
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models
			 SET status = 'active', end_date = COALESCE($1, end_date), renewal_date = COALESCE($1, renewal_date),
			     data = $2, version = $3, updated_at = NOW()
			 WHERE id = $4`,
			endDate, raw, event.Version, contractID,
		)
		return err

	case contract.EventTypeContractSuspended:
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models SET status = 'suspended', data = $1, version = $2, updated_at = NOW() WHERE id = $3`,
			raw, event.Version, contractID,
		)
		return err

	case contract.EventTypeContractResumed:
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models SET status = 'active', data = $1, version = $2, updated_at = NOW() WHERE id = $3`,
			raw, event.Version, contractID,
		)
		return err

	case contract.EventTypeContractCancelled, contract.EventTypeContractExpired:
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models SET status = 'cancelled', data = $1, version = $2, updated_at = NOW() WHERE id = $3`,
			raw, event.Version, contractID,
		)
		return err

	case contract.EventTypeTrialStarted:
		var trialEnd *time.Time
		if tc, ok := data["trial_config"].(map[string]any); ok {
			if s, ok := tc["trial_end_date"].(string); ok {
				trialEnd = parseTime(s)
			}
		}
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models
			 SET status = 'trialing', trial_end_date = $1, data = $2, version = $3, updated_at = NOW()
			 WHERE id = $4`,
			trialEnd, raw, event.Version, contractID,
		)
		return err

	case contract.EventTypeTrialEnded:
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models
			 SET status = 'active', trial_end_date = NULL, data = $1, version = $2, updated_at = NOW()
			 WHERE id = $3`,
			raw, event.Version, contractID,
		)
		return err

	case contract.EventTypeContractRenewed:
		var newEnd *time.Time
		if periodRaw, ok := data["new_period"].(map[string]any); ok {
			if s, ok := periodRaw["end"].(string); ok {
				newEnd = parseTime(s)
			}
		}
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models
			 SET end_date = COALESCE($1, end_date), renewal_date = COALESCE($1, renewal_date),
			     data = $2, version = $3, updated_at = NOW()
			 WHERE id = $4`,
			newEnd, raw, event.Version, contractID,
		)
		return err

	default:
		// Other events are recorded as raw data without touching dedicated
		// columns — the projection is intentionally thin.
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models SET data = $1, version = $2, updated_at = NOW() WHERE id = $3`,
			raw, event.Version, contractID,
		)
		return err
	}
}

// parseTime parses an RFC3339 timestamp into a *time.Time. Returns nil for
// empty or malformed inputs.
func parseTime(s string) *time.Time {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil
	}
	return &t
}
