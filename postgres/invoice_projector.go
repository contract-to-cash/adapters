package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/contract-to-cash/core/eventstore"
	"github.com/jackc/pgx/v5/pgxpool"
)

const invoiceProjectorName = "invoice_projector"

// InvoiceProjector implements projection.Projector for the Invoice read model.
// It listens to invoice-related domain events and maintains the invoice_read_models table.
type InvoiceProjector struct {
	pool       *pgxpool.Pool
	es         *PostgresEventStore
	checkpoint *CheckpointStore
}

func NewInvoiceProjector(pool *pgxpool.Pool, es *PostgresEventStore, cp *CheckpointStore) *InvoiceProjector {
	return &InvoiceProjector{pool: pool, es: es, checkpoint: cp}
}

// Project processes events from the last checkpoint and updates the read model.
func (p *InvoiceProjector) Project(ctx context.Context) error {
	lastPos, err := p.checkpoint.Load(ctx, invoiceProjectorName)
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}

	events, err := p.es.LoadAll(ctx, lastPos)
	if err != nil {
		return fmt.Errorf("load events: %w", err)
	}

	for _, evt := range events {
		if !isInvoiceEvent(evt) {
			lastPos = evt.GlobalPosition
			continue
		}

		if err := p.handleEvent(ctx, evt); err != nil {
			return fmt.Errorf("handle event %s: %w", evt.ID, err)
		}
		lastPos = evt.GlobalPosition
	}

	if err := p.checkpoint.Save(ctx, invoiceProjectorName, lastPos); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	return nil
}

// Rebuild drops and recreates the entire invoice read model from scratch.
func (p *InvoiceProjector) Rebuild(ctx context.Context) error {
	q := QuerierFromContext(ctx, p.pool)

	_, err := q.Exec(ctx, `TRUNCATE invoice_read_models`)
	if err != nil {
		return fmt.Errorf("truncate invoice read models: %w", err)
	}

	if err := p.checkpoint.Reset(ctx, invoiceProjectorName); err != nil {
		return fmt.Errorf("reset checkpoint: %w", err)
	}

	return p.Project(ctx)
}

func (p *InvoiceProjector) handleEvent(ctx context.Context, evt eventstore.Event) error {
	q := QuerierFromContext(ctx, p.pool)

	invoiceID := extractInvoiceID(evt.StreamID)

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
	case strings.HasSuffix(evt.Type, "Created"), strings.HasSuffix(evt.Type, "Issued"):
		contractID, _ := data["contract_id"].(string)
		accountID, _ := data["account_id"].(string)
		status, _ := data["status"].(string)
		if status == "" {
			status = "draft"
		}
		amount := int64(0)
		if v, ok := data["amount"].(float64); ok {
			amount = int64(v)
		}
		currency, _ := data["currency"].(string)
		if currency == "" {
			currency = "JPY"
		}

		_, err := q.Exec(ctx,
			`INSERT INTO invoice_read_models (id, contract_id, account_id, status, amount, currency, data, version)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			 ON CONFLICT (id) DO UPDATE SET
			   status     = EXCLUDED.status,
			   amount     = EXCLUDED.amount,
			   data       = EXCLUDED.data,
			   version    = EXCLUDED.version,
			   updated_at = NOW()`,
			invoiceID, contractID, accountID, status, amount, currency, raw, evt.Version,
		)
		return err

	case strings.HasSuffix(evt.Type, "Paid"):
		_, err := q.Exec(ctx,
			`UPDATE invoice_read_models SET status = 'paid', version = $1, updated_at = NOW() WHERE id = $2`,
			evt.Version, invoiceID,
		)
		return err

	case strings.HasSuffix(evt.Type, "Voided"), strings.HasSuffix(evt.Type, "Cancelled"):
		_, err := q.Exec(ctx,
			`UPDATE invoice_read_models SET status = 'voided', version = $1, updated_at = NOW() WHERE id = $2`,
			evt.Version, invoiceID,
		)
		return err

	default:
		_, err := q.Exec(ctx,
			`UPDATE invoice_read_models SET data = $1, version = $2, updated_at = NOW() WHERE id = $3`,
			raw, evt.Version, invoiceID,
		)
		return err
	}
}

func isInvoiceEvent(evt eventstore.Event) bool {
	return strings.HasPrefix(evt.StreamID, "invoice-")
}

func extractInvoiceID(streamID string) string {
	return strings.TrimPrefix(streamID, "invoice-")
}
