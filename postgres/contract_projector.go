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

const ContractProjectorName = "contract"

// ContractProjector maintains the contract_read_models projection table.
type ContractProjector struct {
	pool       *pgxpool.Pool
	eventStore *PostgresEventStore
	checkpoint *CheckpointStore
}

var _ projection.Projector = (*ContractProjector)(nil)

func NewContractProjector(pool *pgxpool.Pool, es *PostgresEventStore, cp *CheckpointStore) *ContractProjector {
	return &ContractProjector{pool: pool, eventStore: es, checkpoint: cp}
}

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

func (p *ContractProjector) Rebuild(ctx context.Context, until time.Time) error {
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
				// Only advance the checkpoint for events actually projected.
				// Events after `until` are skipped here and must remain
				// unprocessed so a later incremental Project (resuming from this
				// checkpoint) picks them up. Tracking the last *scanned* position
				// instead would leave those skipped events permanently unprojected.
				lastPosition = e.GlobalPosition
			}
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
			   account_id = EXCLUDED.account_id, status = 'draft', start_date = EXCLUDED.start_date,
			   price_id = EXCLUDED.price_id, data = EXCLUDED.data, version = EXCLUDED.version, updated_at = NOW()`,
			contractID, accountID, parseTime(createdAt), priceID, raw, event.Version)
		return err

	case contract.EventTypeContractActivated:
		var endDate *time.Time
		if periodRaw, ok := data["current_period"].(map[string]any); ok {
			if endStr, ok := periodRaw["end"].(string); ok {
				endDate = parseTime(endStr)
			}
		}
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models
			 SET status = 'active', end_date = COALESCE($1, end_date), renewal_date = COALESCE($1, renewal_date),
			     data = $2, version = $3, updated_at = NOW()
			 WHERE id = $4`, endDate, raw, event.Version, contractID)
		return err

	case contract.EventTypeContractSuspended:
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models SET status = 'suspended', data = $1, version = $2, updated_at = NOW() WHERE id = $3`,
			raw, event.Version, contractID)
		return err

	case contract.EventTypeContractResumed:
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models SET status = 'active', data = $1, version = $2, updated_at = NOW() WHERE id = $3`,
			raw, event.Version, contractID)
		return err

	case contract.EventTypeContractCancelled:
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models SET status = 'cancelled', data = $1, version = $2, updated_at = NOW() WHERE id = $3`,
			raw, event.Version, contractID)
		return err

	case contract.EventTypeContractExpired:
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models SET status = 'expired', data = $1, version = $2, updated_at = NOW() WHERE id = $3`,
			raw, event.Version, contractID)
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
			 WHERE id = $4`, trialEnd, raw, event.Version, contractID)
		return err

	case contract.EventTypeTrialEnded:
		// The aggregate transitions to Active only when the trial converted;
		// otherwise it is Cancelled (core domain/contract/aggregate.go). Reading
		// the converted flag keeps the read model from showing "ghost active"
		// rows for trials that lapsed without converting.
		status := "cancelled"
		if converted, _ := data["converted"].(bool); converted {
			status = "active"
		}
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models
			 SET status = $1, trial_end_date = NULL, data = $2, version = $3, updated_at = NOW()
			 WHERE id = $4`, status, raw, event.Version, contractID)
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
			 WHERE id = $4`, newEnd, raw, event.Version, contractID)
		return err

	case contract.EventTypePriceChanged:
		// An immediate price change must move the read model's price_id to the
		// new price so price-filtered queries stay accurate. NULLIF guards
		// against clobbering the existing price_id with an empty payload value.
		newPriceID, _ := data["new_price_id"].(string)
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models
			 SET price_id = COALESCE(NULLIF($1, ''), price_id), data = $2, version = $3, updated_at = NOW()
			 WHERE id = $4`, newPriceID, raw, event.Version, contractID)
		return err

	default:
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models SET data = $1, version = $2, updated_at = NOW() WHERE id = $3`,
			raw, event.Version, contractID)
		return err
	}
}

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
