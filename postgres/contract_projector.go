package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/contract-to-cash/core/eventstore"
	"github.com/jackc/pgx/v5/pgxpool"
)

const contractProjectorName = "contract_projector"

// ContractProjector implements projection.Projector for the Contract read model.
// It listens to contract domain events and maintains the contract_read_models table.
type ContractProjector struct {
	pool       *pgxpool.Pool
	es         *PostgresEventStore
	checkpoint *CheckpointStore
}

func NewContractProjector(pool *pgxpool.Pool, es *PostgresEventStore, cp *CheckpointStore) *ContractProjector {
	return &ContractProjector{pool: pool, es: es, checkpoint: cp}
}

// Project processes events from the last checkpoint and updates the read model.
func (p *ContractProjector) Project(ctx context.Context) error {
	lastPos, err := p.checkpoint.Load(ctx, contractProjectorName)
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}

	events, err := p.es.LoadAll(ctx, lastPos)
	if err != nil {
		return fmt.Errorf("load events: %w", err)
	}

	for _, evt := range events {
		if !isContractEvent(evt) {
			lastPos = evt.GlobalPosition
			continue
		}

		if err := p.handleEvent(ctx, evt); err != nil {
			return fmt.Errorf("handle event %s: %w", evt.ID, err)
		}
		lastPos = evt.GlobalPosition
	}

	if err := p.checkpoint.Save(ctx, contractProjectorName, lastPos); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	return nil
}

// Rebuild drops and recreates the entire contract read model from scratch.
func (p *ContractProjector) Rebuild(ctx context.Context) error {
	q := QuerierFromContext(ctx, p.pool)

	// Truncate read model
	_, err := q.Exec(ctx, `TRUNCATE contract_read_models`)
	if err != nil {
		return fmt.Errorf("truncate contract read models: %w", err)
	}

	// Reset checkpoint
	if err := p.checkpoint.Reset(ctx, contractProjectorName); err != nil {
		return fmt.Errorf("reset checkpoint: %w", err)
	}

	// Replay all events
	return p.Project(ctx)
}

func (p *ContractProjector) handleEvent(ctx context.Context, evt eventstore.Event) error {
	q := QuerierFromContext(ctx, p.pool)

	contractID := extractContractID(evt.StreamID)

	// Extract common fields from event data
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

	switch {
	case strings.HasSuffix(evt.Type, "Created"):
		accountID, _ := data["account_id"].(string)
		startDate, _ := data["start_date"].(string)
		_, err := q.Exec(ctx,
			`INSERT INTO contract_read_models (id, account_id, status, start_date, data, version)
			 VALUES ($1, $2, 'draft', $3, $4, $5)
			 ON CONFLICT (id) DO UPDATE SET
			   account_id = EXCLUDED.account_id,
			   status     = 'draft',
			   start_date = EXCLUDED.start_date,
			   data       = EXCLUDED.data,
			   version    = EXCLUDED.version,
			   updated_at = NOW()`,
			contractID, accountID, startDate, raw, evt.Version,
		)
		return err

	case strings.HasSuffix(evt.Type, "Activated"):
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models SET status = 'active', version = $1, updated_at = NOW() WHERE id = $2`,
			evt.Version, contractID,
		)
		return err

	case strings.HasSuffix(evt.Type, "Suspended"):
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models SET status = 'suspended', version = $1, updated_at = NOW() WHERE id = $2`,
			evt.Version, contractID,
		)
		return err

	case strings.HasSuffix(evt.Type, "Cancelled"):
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models SET status = 'cancelled', version = $1, updated_at = NOW() WHERE id = $2`,
			evt.Version, contractID,
		)
		return err

	case strings.HasSuffix(evt.Type, "Renewed"):
		endDate, _ := data["end_date"].(string)
		renewalDate, _ := data["renewal_date"].(string)
		_, err := q.Exec(ctx,
			`UPDATE contract_read_models
			 SET end_date = $1, renewal_date = $2, version = $3, updated_at = NOW()
			 WHERE id = $4`,
			endDate, renewalDate, evt.Version, contractID,
		)
		return err

	default:
		// Unknown event type — update version and data for forward compatibility
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
