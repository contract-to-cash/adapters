package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/contract-to-cash/core/eventstore"
	"github.com/jackc/pgx/v5/pgxpool"
)

const contractProjectorName = "contract_projector"

// ContractProjector implements projection.Projector for the Contract read model.
// Project handles a single event; Rebuild replays all events up to a timestamp.
type ContractProjector struct {
	pool       *pgxpool.Pool
	es         *PostgresEventStore
	checkpoint *CheckpointStore
}

func NewContractProjector(pool *pgxpool.Pool, es *PostgresEventStore, cp *CheckpointStore) *ContractProjector {
	return &ContractProjector{pool: pool, es: es, checkpoint: cp}
}

// Project processes a single event and updates the contract read model.
func (p *ContractProjector) Project(ctx context.Context, event eventstore.Event) error {
	if !isContractEvent(event) {
		return nil
	}
	return p.handleEvent(ctx, event)
}

// Rebuild drops and recreates the contract read model from all events up to the given time.
func (p *ContractProjector) Rebuild(ctx context.Context, until time.Time) error {
	q := QuerierFromContext(ctx, p.pool)

	_, err := q.Exec(ctx, `TRUNCATE contract_read_models`)
	if err != nil {
		return fmt.Errorf("truncate contract read models: %w", err)
	}

	if err := p.checkpoint.Reset(ctx, contractProjectorName); err != nil {
		return fmt.Errorf("reset checkpoint: %w", err)
	}

	events, err := p.es.loadAllUntil(ctx, until)
	if err != nil {
		return fmt.Errorf("load all events: %w", err)
	}

	for _, evt := range events {
		if !isContractEvent(evt) {
			continue
		}
		if err := p.handleEvent(ctx, evt); err != nil {
			return fmt.Errorf("handle event %s: %w", evt.ID, err)
		}
	}

	if len(events) > 0 {
		lastPos := events[len(events)-1].GlobalPosition
		if err := p.checkpoint.Save(ctx, contractProjectorName, lastPos); err != nil {
			return fmt.Errorf("save checkpoint: %w", err)
		}
	}

	return nil
}

func (p *ContractProjector) handleEvent(ctx context.Context, evt eventstore.Event) error {
	q := QuerierFromContext(ctx, p.pool)

	contractID := extractContractID(evt.StreamID)

	var data map[string]interface{}
	raw, ok := evt.Data.(json.RawMessage)
	if !ok {
		rawBytes, err := json.Marshal(evt.Data)
		if err != nil {
			return fmt.Errorf("marshal event data: %w", err)
		}
		raw = rawBytes
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return fmt.Errorf("unmarshal event data: %w", err)
	}

	eventType := string(evt.Type)

	switch {
	case strings.HasSuffix(eventType, "Created"):
		accountID, _ := data["account_id"].(string)
		startDate, _ := data["start_date"].(string)
		_, err := q.Exec(ctx,
			`INSERT INTO contract_read_models (id, account_id, status, start_date, data, version)
			 VALUES ($1, $2, 'draft', $3, $4, $5)
			 ON CONFLICT (id) DO UPDATE SET
			   account_id = EXCLUDED.account_id, status = 'draft',
			   start_date = EXCLUDED.start_date, data = EXCLUDED.data,
			   version = EXCLUDED.version, updated_at = NOW()`,
			contractID, accountID, startDate, raw, evt.Version,
		)
		return err

	case strings.HasSuffix(eventType, "Activated"):
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models SET status = 'active', version = $1, updated_at = NOW() WHERE id = $2`,
			evt.Version, contractID,
		)
		return err

	case strings.HasSuffix(eventType, "Suspended"):
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models SET status = 'suspended', version = $1, updated_at = NOW() WHERE id = $2`,
			evt.Version, contractID,
		)
		return err

	case strings.HasSuffix(eventType, "Cancelled"):
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models SET status = 'cancelled', version = $1, updated_at = NOW() WHERE id = $2`,
			evt.Version, contractID,
		)
		return err

	case strings.HasSuffix(eventType, "Renewed"):
		endDate, _ := data["end_date"].(string)
		renewalDate, _ := data["renewal_date"].(string)
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models
			 SET end_date = $1, renewal_date = $2, version = $3, updated_at = NOW()
			 WHERE id = $4`,
			endDate, renewalDate, evt.Version, contractID,
		)
		return err

	case strings.HasSuffix(eventType, "TrialStarted"):
		trialEnd, _ := data["trial_end_date"].(string)
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models
			 SET status = 'trial', trial_end_date = $1, version = $2, updated_at = NOW()
			 WHERE id = $3`,
			trialEnd, evt.Version, contractID,
		)
		return err

	default:
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models SET data = $1, version = $2, updated_at = NOW() WHERE id = $3`,
			raw, evt.Version, contractID,
		)
		return err
	}
}

func isContractEvent(evt eventstore.Event) bool {
	return strings.HasPrefix(evt.StreamID, "contract-")
}

func extractContractID(streamID string) string {
	return strings.TrimPrefix(streamID, "contract-")
}
