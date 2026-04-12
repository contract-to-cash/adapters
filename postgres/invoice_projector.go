package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/contract-to-cash/core/application/projection"
	"github.com/contract-to-cash/core/eventstore"
	"github.com/jackc/pgx/v5/pgxpool"
)

// InvoiceProjectorName is the checkpoint name used by InvoiceProjector.
const InvoiceProjectorName = "invoice"

// InvoiceProjector maintains the invoice_read_models projection.
//
// This projector is intentionally conservative: it only reacts to events
// whose type name begins with "invoice." and writes a single row per
// invoice. The Invoice domain is NOT event-sourced in core today — the
// projector exists so that consumers who want a read-model view of
// invoices fed asynchronously from events have a working reference.
type InvoiceProjector struct {
	pool       *pgxpool.Pool
	eventStore *PostgresEventStore
	checkpoint *CheckpointStore
}

var _ projection.Projector = (*InvoiceProjector)(nil)

// NewInvoiceProjector creates a new InvoiceProjector.
func NewInvoiceProjector(pool *pgxpool.Pool, es *PostgresEventStore, cp *CheckpointStore) *InvoiceProjector {
	return &InvoiceProjector{pool: pool, eventStore: es, checkpoint: cp}
}

// Project applies a single event if it belongs to the invoice domain.
func (p *InvoiceProjector) Project(ctx context.Context, event eventstore.Event) error {
	if !strings.HasPrefix(string(event.Type), "invoice.") {
		return nil
	}
	return p.applyEvent(ctx, event)
}

// Rebuild drops invoice_read_models and replays every invoice event up to
// `until`. No DEFERRABLE tricks are necessary because nothing foreign-keys
// into invoice_read_models.
func (p *InvoiceProjector) Rebuild(ctx context.Context, until time.Time) error {
	q := QuerierFromContext(ctx, p.pool)

	if _, err := q.Exec(ctx, `DELETE FROM invoice_read_models`); err != nil {
		return fmt.Errorf("delete invoice read models: %w", err)
	}
	if err := p.checkpoint.Reset(ctx, InvoiceProjectorName); err != nil {
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
		if err := p.checkpoint.Save(ctx, InvoiceProjectorName, lastPosition); err != nil {
			return fmt.Errorf("save checkpoint: %w", err)
		}
	}
	return nil
}

// applyEvent updates invoice_read_models based on a single event. As with
// contract_projector.applyEvent this is intentionally conservative — we
// pull a few conspicuous fields into dedicated columns and persist the
// rest as raw JSONB in `data`.
func (p *InvoiceProjector) applyEvent(ctx context.Context, event eventstore.Event) error {
	q := QuerierFromContext(ctx, p.pool)
	invoiceID := event.StreamID

	var data map[string]any
	if len(event.Data) > 0 {
		if err := json.Unmarshal(event.Data, &data); err != nil {
			return fmt.Errorf("unmarshal event data: %w", err)
		}
	}
	raw := event.Data
	eventType := string(event.Type)

	switch {
	case strings.HasSuffix(eventType, ".created"), strings.HasSuffix(eventType, ".issued"):
		contractID, _ := data["contract_id"].(string)
		accountID, _ := data["account_id"].(string)
		status := "draft"
		if s, ok := data["status"].(string); ok && s != "" {
			status = s
		}
		currency := "JPY"
		if c, ok := data["currency"].(string); ok && c != "" {
			currency = c
		}
		var total int64
		if v, ok := data["total"].(float64); ok {
			total = int64(v)
		}
		_, err := q.Exec(ctx,
			`INSERT INTO invoice_read_models (id, contract_id, account_id, status, total, currency, data, version, updated_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
			 ON CONFLICT (id) DO UPDATE SET
			   status     = EXCLUDED.status,
			   total      = EXCLUDED.total,
			   currency   = EXCLUDED.currency,
			   data       = EXCLUDED.data,
			   version    = EXCLUDED.version,
			   updated_at = NOW()`,
			invoiceID, contractID, accountID, status, total, currency, raw, event.Version,
		)
		return err

	case strings.HasSuffix(eventType, ".paid"):
		_, err := q.Exec(ctx,
			`UPDATE invoice_read_models SET status = 'paid', data = $1, version = $2, updated_at = NOW() WHERE id = $3`,
			raw, event.Version, invoiceID,
		)
		return err

	case strings.HasSuffix(eventType, ".voided"), strings.HasSuffix(eventType, ".cancelled"):
		_, err := q.Exec(ctx,
			`UPDATE invoice_read_models SET status = 'voided', data = $1, version = $2, updated_at = NOW() WHERE id = $3`,
			raw, event.Version, invoiceID,
		)
		return err

	default:
		_, err := q.Exec(ctx,
			`UPDATE invoice_read_models SET data = $1, version = $2, updated_at = NOW() WHERE id = $3`,
			raw, event.Version, invoiceID,
		)
		return err
	}
}
