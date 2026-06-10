package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/domain/invoice"
	"github.com/contract-to-cash/core/domain/shared"
)

// MySQLCreditNoteRepository is a MySQL-backed invoice.CreditNoteRepository.
type MySQLCreditNoteRepository struct {
	db *sql.DB
}

var _ invoice.CreditNoteRepository = (*MySQLCreditNoteRepository)(nil)

// NewCreditNoteRepository constructs a credit note repository over an existing *sql.DB.
func NewCreditNoteRepository(db *sql.DB) *MySQLCreditNoteRepository {
	return &MySQLCreditNoteRepository{db: db}
}

func (r *MySQLCreditNoteRepository) q(ctx context.Context) Querier {
	return querierFromContext(ctx, r.db)
}

type creditNoteJSONState struct {
	Items        []invoice.CreditNoteItemSnapshot `json:"items"`
	Subtotal     shared.Money                     `json:"subtotal"`
	TaxAmount    shared.Money                     `json:"tax_amount"`
	Total        shared.Money                     `json:"total"`
	CreditAmount shared.Money                     `json:"credit_amount"`
	RefundAmount shared.Money                     `json:"refund_amount"`
}

func (r *MySQLCreditNoteRepository) Save(ctx context.Context, cn *invoice.CreditNote) error {
	s := cn.ToSnapshot()
	jsonState, err := json.Marshal(creditNoteJSONState{
		Items: s.Items, Subtotal: s.Subtotal, TaxAmount: s.TaxAmount,
		Total: s.Total, CreditAmount: s.CreditAmount, RefundAmount: s.RefundAmount,
	})
	if err != nil {
		return fmt.Errorf("marshal credit note json state: %w", err)
	}

	var issuedAt *time.Time
	if s.IssuedAt != nil {
		t := s.IssuedAt.UTC()
		issuedAt = &t
	}

	_, err = r.q(ctx).ExecContext(ctx,
		`INSERT INTO credit_notes (id, number, invoice_id, contract_id, account_id, status, reason, memo,
			items, subtotal, tax_amount, total, credit_amount, refund_amount, currency, issued_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(6))
		 ON DUPLICATE KEY UPDATE
			status = VALUES(status), reason = VALUES(reason), memo = VALUES(memo),
			items = VALUES(items), subtotal = VALUES(subtotal), tax_amount = VALUES(tax_amount),
			total = VALUES(total), credit_amount = VALUES(credit_amount),
			refund_amount = VALUES(refund_amount), currency = VALUES(currency),
			issued_at = VALUES(issued_at), updated_at = NOW(6)`,
		string(s.ID), s.Number, string(s.InvoiceID), string(s.ContractID), string(s.AccountID),
		string(s.Status), string(s.Reason), s.Memo,
		jsonState, s.Subtotal.Int64(), s.TaxAmount.Int64(), s.Total.Int64(),
		s.CreditAmount.Int64(), s.RefundAmount.Int64(),
		string(s.Total.Currency()), issuedAt)
	if err != nil {
		return fmt.Errorf("save credit note: %w", err)
	}
	return nil
}

func (r *MySQLCreditNoteRepository) FindByID(ctx context.Context, id shared.CreditNoteID) (*invoice.CreditNote, error) {
	row := r.q(ctx).QueryRowContext(ctx, selectCreditNoteSQL+` WHERE id = ?`, string(id))
	s, err := scanCreditNoteSnapshot(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, shared.NewDomainError(shared.ErrCodeNotFound,
				fmt.Sprintf("credit note %s not found", id))
		}
		return nil, fmt.Errorf("scan credit note: %w", err)
	}
	return invoice.CreditNoteFromSnapshot(s)
}

func (r *MySQLCreditNoteRepository) FindByInvoiceID(ctx context.Context, invoiceID shared.InvoiceID) ([]*invoice.CreditNote, error) {
	return r.findMany(ctx, `WHERE invoice_id = ? ORDER BY created_at DESC`, string(invoiceID))
}

func (r *MySQLCreditNoteRepository) FindByAccountID(ctx context.Context, accountID shared.AccountID) ([]*invoice.CreditNote, error) {
	return r.findMany(ctx, `WHERE account_id = ? ORDER BY created_at DESC`, string(accountID))
}

func (r *MySQLCreditNoteRepository) FindByContractID(ctx context.Context, contractID shared.ContractID) ([]*invoice.CreditNote, error) {
	return r.findMany(ctx, `WHERE contract_id = ? ORDER BY created_at DESC`, string(contractID))
}

func (r *MySQLCreditNoteRepository) FindByStatus(ctx context.Context, status invoice.CreditNoteStatus) ([]*invoice.CreditNote, error) {
	return r.findMany(ctx, `WHERE status = ? ORDER BY created_at DESC`, string(status))
}

func (r *MySQLCreditNoteRepository) findMany(ctx context.Context, where string, args ...any) ([]*invoice.CreditNote, error) {
	rows, err := r.q(ctx).QueryContext(ctx, selectCreditNoteSQL+` `+where, args...)
	if err != nil {
		return nil, fmt.Errorf("query credit notes: %w", err)
	}
	defer func() { _ = rows.Close() }()

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
		issuedAt                         sql.NullTime
		createdAt                        utcTime
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
	if issuedAt.Valid {
		t := issuedAt.Time.UTC()
		s.IssuedAt = &t
	}
	s.CreatedAt = createdAt.Time
	return s, nil
}
