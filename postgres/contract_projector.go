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

// contractUpcasterChain runs every contract event through the same core
// upcaster chain the aggregate rehydration path uses (contract.ContractAggregate.
// LoadFromHistory in core/domain/contract/aggregate.go), so the projection read
// model never has to hand-roll a second implementation of a core schema
// migration (issue #63). Upcast is idempotent and a no-op for event types /
// versions no registered upcaster claims, so running it unconditionally over
// every contract event here is safe.
var contractUpcasterChain = contract.NewContractUpcasterChain()

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
		contract.EventTypeContractPastDue,
		contract.EventTypeContractRecovered,
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

	// No foreign key references contract_read_models anymore: migration 008
	// dropped the write→projection FKs (invoices/credit_notes/usage_records →
	// contract_read_models) that 003 had added as DEFERRABLE INITIALLY IMMEDIATE.
	// The DELETE + replay below can no longer transiently violate a constraint,
	// so the former `SET CONSTRAINTS ALL DEFERRED` is unnecessary. See issue #29.
	if _, err := q.Exec(ctx, `DELETE FROM contract_read_models`); err != nil {
		return fmt.Errorf("delete contract read models: %w", err)
	}
	if err := p.checkpoint.Reset(ctx, ContractProjectorName); err != nil {
		return fmt.Errorf("reset checkpoint: %w", err)
	}

	var fromPosition int64
	var lastPosition int64
	const batch = 1000
scan:
	for {
		events, err := p.eventStore.LoadAll(ctx, fromPosition, batch)
		if err != nil {
			return fmt.Errorf("load events: %w", err)
		}
		if len(events) == 0 {
			break
		}
		for _, e := range events {
			// Stop the scan at the first event past `until`; the checkpoint
			// stays at the last event actually projected. Continuing the scan
			// while skipping would be unsafe when occurred_at and
			// global_position disagree (clock skew / backdated events): a
			// projected pre-`until` event at a higher position would advance
			// the checkpoint past the skipped one, and the incremental
			// Project (which resumes after the checkpoint) would never see it.
			// Breaking is safe for the same reason — any pre-`until` events
			// left unscanned are still after the checkpoint, so the next
			// incremental run picks them up.
			if e.OccurredAt.After(until) {
				break scan
			}
			if err := p.Project(ctx, e); err != nil {
				return fmt.Errorf("project event %s: %w", e.ID, err)
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

func (p *ContractProjector) applyEvent(ctx context.Context, event eventstore.Event) error {
	q := QuerierFromContext(ctx, p.pool)

	// Upcast before decoding so the projection reads the same schema the
	// aggregate would after replay (issue #63) -- e.g. a legacy v1
	// contract.created payload with billing_cycle instead of interval, or a
	// v1 price.changed missing new_price_id, arrives here already migrated.
	upcasted, err := contractUpcasterChain.Upcast(event)
	if err != nil {
		return fmt.Errorf("upcast contract event: %w", err)
	}
	// Everything below must see the migrated event: UpcasterChain.Upcast
	// returns a full eventstore.Event (not just Data), and a future core
	// upcaster is free to mutate any field of it (e.g. rename an event type
	// on migration). Reassigning here means the switch, StreamID, and every
	// other event.* read automatically track that, instead of a future edit
	// accidentally reaching back into the pre-upcast event.
	event = upcasted
	contractID := event.StreamID

	var data map[string]any
	if len(event.Data) > 0 {
		if err := json.Unmarshal(event.Data, &data); err != nil {
			return fmt.Errorf("unmarshal event data: %w", err)
		}
	}
	// raw is persisted into contract_read_models.data, a read-model column
	// (not the append-only event log, which upcasting never touches -- see
	// eventstore.go/eventstore.Append). Storing the upcasted payload keeps the
	// read model's JSON blob self-consistent with the aggregate's view of the
	// same event, so any consumer reading contract_read_models.data directly
	// (rather than replaying the stream) also sees the migrated shape.
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

	case contract.EventTypeContractPastDue:
		// Dunning: the aggregate moved Active -> PastDue (core MarkPastDue).
		// Materializing 'past_due' keeps status-filtered queries honest —
		// notably FindDueForRenewal (status = 'active'), which must stop
		// selecting a contract whose renewal would be rejected by core anyway.
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models SET status = 'past_due', data = $1, version = $2, updated_at = NOW() WHERE id = $3`,
			raw, event.Version, contractID)
		return err

	case contract.EventTypeContractRecovered:
		// Recovery: PastDue -> Active after a successful payment (core
		// RecoverFromPastDue).
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
		// contractUpcasterChain (above) already guarantees new_price_id is
		// PRESENT on a legacy v1 payload (PriceChangedEventUpcaster fills it
		// with "" when absent), but a v1 event that never had a PriceID -- only
		// an old/new Money pair -- legitimately upcasts to an empty string, so
		// the NULLIF/COALESCE fallback to the existing price_id stays required.
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
	if t.IsZero() {
		return nil
	}
	return &t
}
