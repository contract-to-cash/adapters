package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/contract-to-cash/core/application/projection"
	"github.com/contract-to-cash/core/eventstore"
)

// InvoiceProjectorName is the checkpoint key for the invoice projector.
const InvoiceProjectorName = "invoice"

// InvoiceProjector maintains the invoice_read_models projection table.
type InvoiceProjector struct {
	db         *sql.DB
	eventStore *EventStore
	checkpoint *CheckpointStore
}

var _ projection.Projector = (*InvoiceProjector)(nil)

// NewInvoiceProjector constructs an InvoiceProjector.
func NewInvoiceProjector(db *sql.DB, es *EventStore, cp *CheckpointStore) *InvoiceProjector {
	return &InvoiceProjector{db: db, eventStore: es, checkpoint: cp}
}

func (p *InvoiceProjector) Project(ctx context.Context, event eventstore.Event) error {
	if !strings.HasPrefix(string(event.Type), "invoice.") {
		return nil
	}
	return p.applyEvent(ctx, event)
}

func (p *InvoiceProjector) Rebuild(ctx context.Context, until time.Time) error {
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

func (p *InvoiceProjector) rebuildInTx(ctx context.Context, until time.Time) error {
	q := querierFromContext(ctx, p.db)

	if _, err := q.ExecContext(ctx, `DELETE FROM invoice_read_models`); err != nil {
		return fmt.Errorf("delete invoice read models: %w", err)
	}
	if err := p.checkpoint.Reset(ctx, InvoiceProjectorName); err != nil {
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
			return fmt.Errorf("LoadAll did not advance (stuck at %d)", fromPosition)
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

func (p *InvoiceProjector) applyEvent(ctx context.Context, event eventstore.Event) error {
	q := querierFromContext(ctx, p.db)
	invoiceID := event.StreamID

	var data map[string]any
	if len(event.Data) > 0 {
		if err := json.Unmarshal(event.Data, &data); err != nil {
			return fmt.Errorf("unmarshal event data: %w", err)
		}
	}
	raw := normalizeJSON(event.Data)
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
		// core marshals the total as a shared.Money object
		// ({"amount":"11000/1","currency":"JPY"}), so a float64 assertion always
		// failed and silently projected total=0. Parse the Money payload instead;
		// its embedded currency wins when the top-level currency is absent.
		var total int64
		if v, ok := data["total"]; ok {
			if amt, cur, parsed := parseMoneyPayload(v); parsed {
				total = amt
				if cur != "" {
					currency = cur
				}
			}
		}
		_, err := q.ExecContext(ctx,
			`INSERT INTO invoice_read_models (id, contract_id, account_id, status, total, currency, data, version, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(6))
			 ON DUPLICATE KEY UPDATE
			   status = VALUES(status), total = VALUES(total), currency = VALUES(currency),
			   data = VALUES(data), version = VALUES(version), updated_at = NOW(6)`,
			invoiceID, contractID, accountID, status, total, currency, raw, event.Version)
		return err

	case strings.HasSuffix(eventType, ".paid"):
		_, err := q.ExecContext(ctx,
			`UPDATE invoice_read_models SET status = 'paid', data = ?, version = ?, updated_at = NOW(6) WHERE id = ?`,
			raw, event.Version, invoiceID)
		return err

	case strings.HasSuffix(eventType, ".voided"), strings.HasSuffix(eventType, ".cancelled"):
		_, err := q.ExecContext(ctx,
			`UPDATE invoice_read_models SET status = 'voided', data = ?, version = ?, updated_at = NOW(6) WHERE id = ?`,
			raw, event.Version, invoiceID)
		return err

	default:
		_, err := q.ExecContext(ctx,
			`UPDATE invoice_read_models SET data = ?, version = ?, updated_at = NOW(6) WHERE id = ?`,
			raw, event.Version, invoiceID)
		return err
	}
}
