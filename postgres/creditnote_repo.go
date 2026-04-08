package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/contract-to-cash/core/domain/invoice"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresCreditNoteRepository implements invoice.CreditNoteRepository.
// Operates outside TxManager.Repos — used directly by CreditNoteService.
type PostgresCreditNoteRepository struct {
	pool *pgxpool.Pool
}

var _ invoice.CreditNoteRepository = (*PostgresCreditNoteRepository)(nil)

func NewCreditNoteRepository(pool *pgxpool.Pool) *PostgresCreditNoteRepository {
	return &PostgresCreditNoteRepository{pool: pool}
}

func (r *PostgresCreditNoteRepository) q(ctx context.Context) Querier {
	return QuerierFromContext(ctx, r.pool)
}

func (r *PostgresCreditNoteRepository) Save(ctx context.Context, cn *invoice.CreditNote) error {
	q := r.q(ctx)

	data, err := json.Marshal(cn)
	if err != nil {
		return fmt.Errorf("marshal credit note: %w", err)
	}

	_, err = q.Exec(ctx,
		`INSERT INTO credit_notes (id, invoice_id, amount, currency, reason, status, data)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (id) DO UPDATE SET
		   status     = EXCLUDED.status,
		   data       = EXCLUDED.data,
		   updated_at = NOW()`,
		cn.ID(), cn.InvoiceID(), cn.Amount(), cn.Currency(), cn.Reason(), cn.Status(), data,
	)
	if err != nil {
		return fmt.Errorf("save credit note: %w", err)
	}
	return nil
}

func (r *PostgresCreditNoteRepository) FindByID(ctx context.Context, id string) (*invoice.CreditNote, error) {
	q := r.q(ctx)

	var data json.RawMessage
	err := q.QueryRow(ctx,
		`SELECT data FROM credit_notes WHERE id = $1`, id,
	).Scan(&data)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, invoice.ErrCreditNoteNotFound
		}
		return nil, fmt.Errorf("find credit note: %w", err)
	}

	return invoice.UnmarshalCreditNote(data)
}

func (r *PostgresCreditNoteRepository) FindByInvoiceID(ctx context.Context, invoiceID string) ([]*invoice.CreditNote, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM credit_notes WHERE invoice_id = $1 ORDER BY created_at DESC`,
		invoiceID,
	)
	if err != nil {
		return nil, fmt.Errorf("find credit notes by invoice: %w", err)
	}
	defer rows.Close()

	var notes []*invoice.CreditNote
	for rows.Next() {
		var data json.RawMessage
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("scan credit note: %w", err)
		}
		cn, err := invoice.UnmarshalCreditNote(data)
		if err != nil {
			return nil, fmt.Errorf("unmarshal credit note: %w", err)
		}
		notes = append(notes, cn)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return notes, nil
}
