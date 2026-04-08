package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/contract-to-cash/core/domain/invoice"
	"github.com/contract-to-cash/core/domain/shared"
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
		`INSERT INTO credit_notes (id, invoice_id, contract_id, account_id, amount, currency, reason, status, data)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 ON CONFLICT (id) DO UPDATE SET
		   status     = EXCLUDED.status,
		   data       = EXCLUDED.data,
		   updated_at = NOW()`,
		string(cn.ID()), string(cn.InvoiceID()), string(cn.ContractID()),
		string(cn.AccountID()), cn.Amount(), string(cn.Currency()),
		cn.Reason(), string(cn.Status()), data,
	)
	if err != nil {
		return fmt.Errorf("save credit note: %w", err)
	}
	return nil
}

func (r *PostgresCreditNoteRepository) FindByID(ctx context.Context, id shared.CreditNoteID) (*invoice.CreditNote, error) {
	q := r.q(ctx)

	var data json.RawMessage
	err := q.QueryRow(ctx,
		`SELECT data FROM credit_notes WHERE id = $1`, string(id),
	).Scan(&data)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, invoice.ErrCreditNoteNotFound
		}
		return nil, fmt.Errorf("find credit note: %w", err)
	}

	return invoice.UnmarshalCreditNote(data)
}

func (r *PostgresCreditNoteRepository) FindByInvoiceID(ctx context.Context, invoiceID shared.InvoiceID) ([]*invoice.CreditNote, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM credit_notes WHERE invoice_id = $1 ORDER BY created_at DESC`,
		string(invoiceID),
	)
	if err != nil {
		return nil, fmt.Errorf("find credit notes by invoice: %w", err)
	}
	defer rows.Close()

	return scanCreditNotes(rows)
}

func (r *PostgresCreditNoteRepository) FindByAccountID(ctx context.Context, accountID shared.AccountID) ([]*invoice.CreditNote, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM credit_notes WHERE account_id = $1 ORDER BY created_at DESC`,
		string(accountID),
	)
	if err != nil {
		return nil, fmt.Errorf("find credit notes by account: %w", err)
	}
	defer rows.Close()

	return scanCreditNotes(rows)
}

func (r *PostgresCreditNoteRepository) FindByContractID(ctx context.Context, contractID shared.ContractID) ([]*invoice.CreditNote, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM credit_notes WHERE contract_id = $1 ORDER BY created_at DESC`,
		string(contractID),
	)
	if err != nil {
		return nil, fmt.Errorf("find credit notes by contract: %w", err)
	}
	defer rows.Close()

	return scanCreditNotes(rows)
}

func (r *PostgresCreditNoteRepository) FindByStatus(ctx context.Context, status invoice.CreditNoteStatus) ([]*invoice.CreditNote, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM credit_notes WHERE status = $1 ORDER BY created_at DESC`,
		string(status),
	)
	if err != nil {
		return nil, fmt.Errorf("find credit notes by status: %w", err)
	}
	defer rows.Close()

	return scanCreditNotes(rows)
}

func scanCreditNotes(rows pgx.Rows) ([]*invoice.CreditNote, error) {
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
