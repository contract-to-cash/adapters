package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/application/projection"
	"github.com/contract-to-cash/core/domain/contract"
	"github.com/contract-to-cash/core/eventstore"
)

// ContractProjectorName is the checkpoint key for the contract projector.
const ContractProjectorName = "contract"

// ContractProjector maintains the contract_read_models projection table.
type ContractProjector struct {
	db         *sql.DB
	eventStore *EventStore
	checkpoint *CheckpointStore
}

var _ projection.Projector = (*ContractProjector)(nil)

// NewContractProjector constructs a ContractProjector.
func NewContractProjector(db *sql.DB, es *EventStore, cp *CheckpointStore) *ContractProjector {
	return &ContractProjector{db: db, eventStore: es, checkpoint: cp}
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
	if _, inTx := TxFromContext(ctx); !inTx {
		sqlTx, err := p.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin rebuild tx: %w", err)
		}
		txCtx := ContextWithTx(ctx, sqlTx)
		if err := p.rebuildInTx(txCtx, until); err != nil {
			_ = sqlTx.Rollback()
			return err
		}
		if err := sqlTx.Commit(); err != nil {
			return fmt.Errorf("commit rebuild tx: %w", err)
		}
		return nil
	}
	return p.rebuildInTx(ctx, until)
}

func (p *ContractProjector) rebuildInTx(ctx context.Context, until time.Time) error {
	q := querierFromContext(ctx, p.db)

	// MySQL has no deferrable constraints (postgres uses SET CONSTRAINTS ALL
	// DEFERRED here). The equivalent is to suspend foreign-key checks for the
	// truncate/reload so any FK referencing contract_read_models does not
	// transiently fail while the table is emptied and repopulated. Scoped to
	// this session (the rebuild tx) and restored immediately after.
	if _, err := q.ExecContext(ctx, `SET SESSION foreign_key_checks = 0`); err != nil {
		return fmt.Errorf("disable fk checks: %w", err)
	}
	defer func() { _, _ = q.ExecContext(ctx, `SET SESSION foreign_key_checks = 1`) }()

	if _, err := q.ExecContext(ctx, `DELETE FROM contract_read_models`); err != nil {
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
	q := querierFromContext(ctx, p.db)
	contractID := event.StreamID

	var data map[string]any
	if len(event.Data) > 0 {
		if err := json.Unmarshal(event.Data, &data); err != nil {
			return fmt.Errorf("unmarshal event data: %w", err)
		}
	}
	raw := normalizeJSON(event.Data)

	switch event.Type {
	case contract.EventTypeContractCreated:
		accountID, _ := data["account_id"].(string)
		priceID, _ := data["price_id"].(string)
		createdAt, _ := data["created_at"].(string)
		_, err := q.ExecContext(ctx,
			`INSERT INTO contract_read_models (id, account_id, status, start_date, price_id, data, version, updated_at)
			 VALUES (?, ?, 'draft', ?, ?, ?, ?, NOW(6))
			 ON DUPLICATE KEY UPDATE
			   account_id = VALUES(account_id), status = 'draft', start_date = VALUES(start_date),
			   price_id = VALUES(price_id), data = VALUES(data), version = VALUES(version), updated_at = NOW(6)`,
			contractID, accountID, parseTime(createdAt), priceID, raw, event.Version)
		return err

	case contract.EventTypeContractActivated:
		var endDate *time.Time
		if periodRaw, ok := data["current_period"].(map[string]any); ok {
			if endStr, ok := periodRaw["end"].(string); ok {
				endDate = parseTime(endStr)
			}
		}
		_, err := q.ExecContext(ctx,
			`UPDATE contract_read_models
			 SET status = 'active', end_date = COALESCE(?, end_date), renewal_date = COALESCE(?, renewal_date),
			     data = ?, version = ?, updated_at = NOW(6)
			 WHERE id = ?`, endDate, endDate, raw, event.Version, contractID)
		return err

	case contract.EventTypeContractSuspended:
		_, err := q.ExecContext(ctx,
			`UPDATE contract_read_models SET status = 'suspended', data = ?, version = ?, updated_at = NOW(6) WHERE id = ?`,
			raw, event.Version, contractID)
		return err

	case contract.EventTypeContractResumed:
		_, err := q.ExecContext(ctx,
			`UPDATE contract_read_models SET status = 'active', data = ?, version = ?, updated_at = NOW(6) WHERE id = ?`,
			raw, event.Version, contractID)
		return err

	case contract.EventTypeContractCancelled:
		_, err := q.ExecContext(ctx,
			`UPDATE contract_read_models SET status = 'cancelled', data = ?, version = ?, updated_at = NOW(6) WHERE id = ?`,
			raw, event.Version, contractID)
		return err

	case contract.EventTypeContractExpired:
		_, err := q.ExecContext(ctx,
			`UPDATE contract_read_models SET status = 'expired', data = ?, version = ?, updated_at = NOW(6) WHERE id = ?`,
			raw, event.Version, contractID)
		return err

	case contract.EventTypeTrialStarted:
		var trialEnd *time.Time
		if tc, ok := data["trial_config"].(map[string]any); ok {
			if s, ok := tc["trial_end_date"].(string); ok {
				trialEnd = parseTime(s)
			}
		}
		_, err := q.ExecContext(ctx,
			`UPDATE contract_read_models
			 SET status = 'trialing', trial_end_date = ?, data = ?, version = ?, updated_at = NOW(6)
			 WHERE id = ?`, trialEnd, raw, event.Version, contractID)
		return err

	case contract.EventTypeTrialEnded:
		_, err := q.ExecContext(ctx,
			`UPDATE contract_read_models
			 SET status = 'active', trial_end_date = NULL, data = ?, version = ?, updated_at = NOW(6)
			 WHERE id = ?`, raw, event.Version, contractID)
		return err

	case contract.EventTypeContractRenewed:
		var newEnd *time.Time
		if periodRaw, ok := data["new_period"].(map[string]any); ok {
			if s, ok := periodRaw["end"].(string); ok {
				newEnd = parseTime(s)
			}
		}
		_, err := q.ExecContext(ctx,
			`UPDATE contract_read_models
			 SET end_date = COALESCE(?, end_date), renewal_date = COALESCE(?, renewal_date),
			     data = ?, version = ?, updated_at = NOW(6)
			 WHERE id = ?`, newEnd, newEnd, raw, event.Version, contractID)
		return err

	default:
		_, err := q.ExecContext(ctx,
			`UPDATE contract_read_models SET data = ?, version = ?, updated_at = NOW(6) WHERE id = ?`,
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
