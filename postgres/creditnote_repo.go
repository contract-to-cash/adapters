package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/domain/invoice"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

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

type creditNoteJSONState struct {
	Items        []invoice.CreditNoteItemSnapshot `json:"items"`
	Subtotal     shared.Money                     `json:"subtotal"`
	TaxAmount    shared.Money                     `json:"tax_amount"`
	Total        shared.Money                     `json:"total"`
	CreditAmount shared.Money                     `json:"credit_amount"`
	RefundAmount shared.Money                     `json:"refund_amount"`
}

func (r *PostgresCreditNoteRepository) Save(ctx context.Context, cn *invoice.CreditNote) error {
	s := cn.ToSnapshot()
	jsonState, err := json.Marshal(creditNoteJSONState{
		Items: s.Items, Subtotal: s.Subtotal, TaxAmount: s.TaxAmount,
		Total: s.Total, CreditAmount: s.CreditAmount, RefundAmount: s.RefundAmount,
	})
	if err != nil {
		return fmt.Errorf("marshal credit note json state: %w", err)
	}

	_, err = r.q(ctx).Exec(ctx,
		`INSERT INTO credit_notes (id, number, invoice_id, contract_id, account_id, status, reason, memo,
			items, subtotal, tax_amount, total, credit_amount, refund_amount, currency, issued_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, NOW())
		 ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status, reason = EXCLUDED.reason, memo = EXCLUDED.memo,
			items = EXCLUDED.items, subtotal = EXCLUDED.subtotal, tax_amount = EXCLUDED.tax_amount,
			total = EXCLUDED.total, credit_amount = EXCLUDED.credit_amount,
			refund_amount = EXCLUDED.refund_amount, currency = EXCLUDED.currency,
			issued_at = EXCLUDED.issued_at, updated_at = NOW()`,
		string(s.ID), s.Number, string(s.InvoiceID), string(s.ContractID), string(s.AccountID),
		string(s.Status), string(s.Reason), s.Memo,
		jsonState, s.Subtotal.Int64(), s.TaxAmount.Int64(), s.Total.Int64(),
		s.CreditAmount.Int64(), s.RefundAmount.Int64(),
		string(s.Total.Currency()), s.IssuedAt)
	if err != nil {
		return fmt.Errorf("save credit note: %w", err)
	}
	return nil
}

func (r *PostgresCreditNoteRepository) FindByID(ctx context.Context, id shared.CreditNoteID) (*invoice.CreditNote, error) {
	row := r.q(ctx).QueryRow(ctx, selectCreditNoteSQL+` WHERE id = $1`, string(id))
	s, err := scanCreditNoteSnapshot(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, shared.NewDomainError(shared.ErrCodeNotFound,
				fmt.Sprintf("credit note %s not found", id))
		}
		return nil, fmt.Errorf("scan credit note: %w", err)
	}
	return invoice.CreditNoteFromSnapshot(s)
}

func (r *PostgresCreditNoteRepository) FindByInvoiceID(ctx context.Context, invoiceID shared.InvoiceID) ([]*invoice.CreditNote, error) {
	return r.findMany(ctx, `WHERE invoice_id = $1 ORDER BY created_at DESC`, string(invoiceID))
}

func (r *PostgresCreditNoteRepository) FindByAccountID(ctx context.Context, accountID shared.AccountID) ([]*invoice.CreditNote, error) {
	return r.findMany(ctx, `WHERE account_id = $1 ORDER BY created_at DESC`, string(accountID))
}

func (r *PostgresCreditNoteRepository) FindByContractID(ctx context.Context, contractID shared.ContractID) ([]*invoice.CreditNote, error) {
	return r.findMany(ctx, `WHERE contract_id = $1 ORDER BY created_at DESC`, string(contractID))
}

func (r *PostgresCreditNoteRepository) FindByStatus(ctx context.Context, status invoice.CreditNoteStatus) ([]*invoice.CreditNote, error) {
	return r.findMany(ctx, `WHERE status = $1 ORDER BY created_at DESC`, string(status))
}

func (r *PostgresCreditNoteRepository) findMany(ctx context.Context, where string, args ...any) ([]*invoice.CreditNote, error) {
	rows, err := r.q(ctx).Query(ctx, selectCreditNoteSQL+` `+where, args...)
	if err != nil {
		return nil, fmt.Errorf("query credit notes: %w", err)
	}
	defer rows.Close()

	var result []*invoice.CreditNote
	for rows.Next() {
		s, err := scanCreditNoteSnapshot(rows)
		if err != nil {
			return nil, fmt.Errorf("scan credit note row: %w", err)
		}
		cn, err := invoice.CreditNoteFromSnapshot(s)
		if err != nil {
			return nil, fmt.Errorf("reconstruct credit note: %w", err)
		}
		result = append(result, cn)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return result, nil
}

const selectCreditNoteSQL = `
	SELECT id, number, invoice_id, account_id, contract_id, status, reason, memo,
	       items, issued_at, created_at
	FROM credit_notes`

func scanCreditNoteSnapshot(t scanTarget) (invoice.CreditNoteSnapshot, error) {
	var (
		s                                invoice.CreditNoteSnapshot
		id, number                       string
		invoiceID, contractID, accountID string
		status, reason, memo             string
		itemsJSON                        json.RawMessage
		issuedAt                         *time.Time
		createdAt                        time.Time
	)
	if err := t.Scan(
		&id, &number, &invoiceID, &accountID, &contractID,
		&status, &reason, &memo, &itemsJSON, &issuedAt, &createdAt,
	); err != nil {
		return invoice.CreditNoteSnapshot{}, err
	}

	var js creditNoteJSONState
	if len(itemsJSON) > 0 {
		if err := json.Unmarshal(itemsJSON, &js); err != nil {
			return invoice.CreditNoteSnapshot{}, fmt.Errorf("unmarshal credit note json state: %w", err)
		}
	}

	s.ID = shared.CreditNoteID(id)
	s.Number = number
	s.InvoiceID = shared.InvoiceID(invoiceID)
	s.AccountID = shared.AccountID(accountID)
	s.ContractID = shared.ContractID(contractID)
	s.Status = invoice.CreditNoteStatus(status)
	s.Reason = invoice.CreditNoteReason(reason)
	s.Memo = memo
	s.Items = js.Items
	s.Subtotal = js.Subtotal
	s.TaxAmount = js.TaxAmount
	s.Total = js.Total
	s.CreditAmount = js.CreditAmount
	s.RefundAmount = js.RefundAmount
	s.IssuedAt = issuedAt
	s.CreatedAt = createdAt
	return s, nil
}
