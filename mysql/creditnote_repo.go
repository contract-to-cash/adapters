package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/application/tx"
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

	q := r.q(ctx)

	// Optimistic-lock guarded write (issue #147). MySQL's INSERT ... ON DUPLICATE
	// KEY UPDATE has no WHERE clause, so a single-statement version-guarded upsert
	// is not possible. We attempt a plain INSERT (a brand-new credit note) and, on
	// a PRIMARY-key duplicate, fall back to a lock_version-guarded UPDATE. This is
	// what stops a concurrent ApplyCreditNote vs RefundCreditNote on the same
	// issued note from both persisting last-writer-wins. lock_version carries the
	// domain version (cn.Version()); a brand-new note INSERTs it directly.
	_, err = q.ExecContext(ctx,
		`INSERT INTO credit_notes (id, number, invoice_id, contract_id, account_id, status, reason, memo,
			items, subtotal, tax_amount, total, credit_amount, refund_amount, currency, issued_at, lock_version, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(6))`,
		string(s.ID), s.Number, string(s.InvoiceID), string(s.ContractID), string(s.AccountID),
		string(s.Status), string(s.Reason), s.Memo,
		jsonState, s.Subtotal.Int64(), s.TaxAmount.Int64(), s.Total.Int64(),
		s.CreditAmount.Int64(), s.RefundAmount.Int64(),
		string(s.Total.Currency()), issuedAt, s.Version)
	if err == nil {
		cn.SetVersion(s.Version)
		return nil
	}

	// PRIMARY-key duplicate: the credit note already exists, so this Save is an
	// update. Guard the UPDATE on id AND lock_version so a concurrent writer that
	// already advanced the version loses (issue #147): the WHERE misses, zero rows
	// change, and we report tx.ErrVersionConflict. Because updated_at = NOW(6)
	// always changes on a matching row, MySQL's changed-rows RowsAffected is 1 on
	// success and 0 only on a genuine version miss — no spurious conflict on a
	// no-op re-save. Mirrors the payment/invoice repos.
	if dupEntryOnKey(err, "PRIMARY") {
		res, upErr := q.ExecContext(ctx,
			`UPDATE credit_notes SET
				number = ?, invoice_id = ?, contract_id = ?, account_id = ?,
				status = ?, reason = ?, memo = ?, items = ?, subtotal = ?, tax_amount = ?,
				total = ?, credit_amount = ?, refund_amount = ?, currency = ?, issued_at = ?,
				lock_version = ?, updated_at = NOW(6)
			 WHERE id = ? AND lock_version = ?`,
			s.Number, string(s.InvoiceID), string(s.ContractID), string(s.AccountID),
			string(s.Status), string(s.Reason), s.Memo, jsonState, s.Subtotal.Int64(), s.TaxAmount.Int64(),
			s.Total.Int64(), s.CreditAmount.Int64(), s.RefundAmount.Int64(), string(s.Total.Currency()), issuedAt,
			s.Version, string(s.ID), cn.LoadedVersion())
		if upErr != nil {
			return fmt.Errorf("save credit note: %w", upErr)
		}
		affected, aErr := res.RowsAffected()
		if aErr != nil {
			return fmt.Errorf("credit note rows affected: %w", aErr)
		}
		if affected == 0 {
			return tx.ErrVersionConflict
		}
		cn.SetVersion(s.Version)
		return nil
	}

	return fmt.Errorf("save credit note: %w", err)
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
	       items, issued_at, created_at, lock_version
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
		lockVersion                      int
	)
	if err := t.Scan(
		&id, &number, &invoiceID, &accountID, &contractID,
		&status, &reason, &memo, &itemsJSON, &issuedAt, &createdAt, &lockVersion,
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
	// The credit_notes.lock_version column is authoritative for the
	// optimistic-locking version (issue #147). CreditNoteFromSnapshot restores
	// both version and loadedVersion from it, so the next Save guards on the right
	// baseline.
	s.Version = lockVersion
	return s, nil
}
