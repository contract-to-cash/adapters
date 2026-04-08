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

const invoiceProjectorName = "invoice_projector"

// InvoiceProjector implements projection.Projector for the Invoice read model.
// Project handles a single event; Rebuild replays all events up to a timestamp.
type InvoiceProjector struct {
	pool       *pgxpool.Pool
	es         *PostgresEventStore
	checkpoint *CheckpointStore
}

func NewInvoiceProjector(pool *pgxpool.Pool, es *PostgresEventStore, cp *CheckpointStore) *InvoiceProjector {
	return &InvoiceProjector{pool: pool, es: es, checkpoint: cp}
}

// Project processes a single event and updates the invoice read model.
func (p *InvoiceProjector) Project(ctx context.Context, event eventstore.Event) error {
	if !isInvoiceEvent(event) {
		return nil
	}
	return p.handleEvent(ctx, event)
}

// Rebuild drops and recreates the invoice read model from all events up to the given time.
func (p *InvoiceProjector) Rebuild(ctx context.Context, until time.Time) error {
	q := QuerierFromContext(ctx, p.pool)

	_, err := q.Exec(ctx, `DELETE FROM invoice_read_models`)
	if err != nil {
		return fmt.Errorf("delete invoice read models: %w", err)
	}

	if err := p.checkpoint.Reset(ctx, invoiceProjectorName); err != nil {
		return fmt.Errorf("reset checkpoint: %w", err)
	}

	events, err := p.es.loadAllUntil(ctx, until)
	if err != nil {
		return fmt.Errorf("load all events: %w", err)
	}

	for _, evt := range events {
		if !isInvoiceEvent(evt) {
			continue
		}
		if err := p.handleEvent(ctx, evt); err != nil {
			return fmt.Errorf("handle event %s: %w", evt.ID, err)
		}
	}

	if len(events) > 0 {
		lastPos := events[len(events)-1].GlobalPosition
		if err := p.checkpoint.Save(ctx, invoiceProjectorName, lastPos); err != nil {
			return fmt.Errorf("save checkpoint: %w", err)
		}
	}

	return nil
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

	eventType := string(evt.Type)

	switch {
	case strings.HasSuffix(eventType, "Created"), strings.HasSuffix(eventType, "Issued"):
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
			   status = EXCLUDED.status, amount = EXCLUDED.amount,
			   data = EXCLUDED.data, version = EXCLUDED.version, updated_at = NOW()`,
			invoiceID, contractID, accountID, status, amount, currency, raw, evt.Version,
		)
		return err

	case strings.HasSuffix(eventType, "Paid"):
		_, err := q.Exec(ctx,
			`UPDATE invoice_read_models SET status = 'paid', version = $1, updated_at = NOW() WHERE id = $2`,
			evt.Version, invoiceID,
		)
		return err

	case strings.HasSuffix(eventType, "Voided"), strings.HasSuffix(eventType, "Cancelled"):
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
