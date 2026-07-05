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

// MySQLInvoiceRepository is a MySQL-backed invoice.Repository.
type MySQLInvoiceRepository struct {
	db *sql.DB
}

var _ invoice.Repository = (*MySQLInvoiceRepository)(nil)

// NewInvoiceRepository constructs an invoice repository over an existing *sql.DB.
func NewInvoiceRepository(db *sql.DB) *MySQLInvoiceRepository {
	return &MySQLInvoiceRepository{db: db}
}

func (r *MySQLInvoiceRepository) q(ctx context.Context) Querier {
	return querierFromContext(ctx, r.db)
}

type invoiceJSONState struct {
	Subtotal       shared.Money               `json:"subtotal"`
	TaxAmount      shared.Money               `json:"tax_amount"`
	DiscountAmount shared.Money               `json:"discount_amount"`
	Total          shared.Money               `json:"total"`
	AppliedBalance shared.Money               `json:"applied_balance"`
	AmountDue      shared.Money               `json:"amount_due"`
	PaidAmount     shared.Money               `json:"paid_amount"`
	Balance        shared.Money               `json:"balance"`
	BillingPeriod  shared.DateRange           `json:"billing_period"`
	LineItems      []invoice.LineItemSnapshot `json:"line_items"`
}

func (r *MySQLInvoiceRepository) Save(ctx context.Context, inv *invoice.Invoice) error {
	s := inv.ToSnapshot()

	jsonState, err := json.Marshal(invoiceJSONState{
		Subtotal: s.Subtotal, TaxAmount: s.TaxAmount, DiscountAmount: s.DiscountAmount,
		Total: s.Total, AppliedBalance: s.AppliedBalance, AmountDue: s.AmountDue,
		PaidAmount: s.PaidAmount, Balance: s.Balance, BillingPeriod: s.BillingPeriod,
		LineItems: s.LineItems,
	})
	if err != nil {
		return fmt.Errorf("marshal invoice json state: %w", err)
	}

	metadata, err := json.Marshal(s.Metadata)
	if err != nil {
		return fmt.Errorf("marshal invoice metadata: %w", err)
	}

	var billingFrom, billingTo, issueDate, dueDate *time.Time
	if !s.BillingPeriod.IsZero() {
		from, to := s.BillingPeriod.Start().UTC(), s.BillingPeriod.End().UTC()
		billingFrom, billingTo = &from, &to
	}
	if !s.IssueDate.IsZero() {
		t := s.IssueDate.UTC()
		issueDate = &t
	}
	if !s.DueDate.IsZero() {
		t := s.DueDate.UTC()
		dueDate = &t
	}

	var paidAt *time.Time
	if s.PaidAt != nil {
		t := s.PaidAt.UTC()
		paidAt = &t
	}

	var revisionOf, originalInvoiceID *string
	if s.RevisionOf != nil {
		v := string(*s.RevisionOf)
		revisionOf = &v
	}
	if s.OriginalInvoiceID != nil {
		v := string(*s.OriginalInvoiceID)
		originalInvoiceID = &v
	}

	q := r.q(ctx)
	_, err = q.ExecContext(ctx,
		`INSERT INTO invoices (
			id, invoice_number, account_id, contract_id, status,
			subtotal, tax_amount, discount_amount, total,
			applied_balance, amount_due, paid_amount, balance,
			currency, billing_period_from, billing_period_to,
			issue_date, due_date, paid_at, void_reason,
			revision_of, original_invoice_id, payment_method_id,
			allow_partial_pay, line_items, metadata, version, updated_at
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, 1, NOW(6)
		)
		ON DUPLICATE KEY UPDATE
			invoice_number = VALUES(invoice_number), status = VALUES(status),
			subtotal = VALUES(subtotal), tax_amount = VALUES(tax_amount),
			discount_amount = VALUES(discount_amount), total = VALUES(total),
			applied_balance = VALUES(applied_balance), amount_due = VALUES(amount_due),
			paid_amount = VALUES(paid_amount), balance = VALUES(balance),
			currency = VALUES(currency), billing_period_from = VALUES(billing_period_from),
			billing_period_to = VALUES(billing_period_to), issue_date = VALUES(issue_date),
			due_date = VALUES(due_date), paid_at = VALUES(paid_at),
			void_reason = VALUES(void_reason), revision_of = VALUES(revision_of),
			original_invoice_id = VALUES(original_invoice_id),
			payment_method_id = VALUES(payment_method_id),
			allow_partial_pay = VALUES(allow_partial_pay),
			line_items = VALUES(line_items), metadata = VALUES(metadata),
			version = version + 1, updated_at = NOW(6)`,
		string(s.ID), s.InvoiceNumber, string(s.AccountID), string(s.ContractID), string(s.Status),
		s.Subtotal.Int64(), s.TaxAmount.Int64(), s.DiscountAmount.Int64(), s.Total.Int64(),
		s.AppliedBalance.Int64(), s.AmountDue.Int64(), s.PaidAmount.Int64(), s.Balance.Int64(),
		string(s.Total.Currency()), billingFrom, billingTo,
		issueDate, dueDate, paidAt, s.VoidReason,
		revisionOf, originalInvoiceID, s.PaymentMethodID,
		s.AllowPartialPay, jsonState, metadata)
	if err != nil {
		return fmt.Errorf("save invoice: %w", err)
	}

	if _, err := q.ExecContext(ctx,
		`UPDATE invoice_history SET valid_to = NOW(6) WHERE id = ? AND valid_to IS NULL`,
		string(s.ID)); err != nil {
		return fmt.Errorf("close invoice history: %w", err)
	}

	historySnapshot, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("marshal invoice history snapshot: %w", err)
	}

	if _, err := q.ExecContext(ctx,
		`INSERT INTO invoice_history (id, version, snapshot, valid_from)
		 VALUES (?, COALESCE((SELECT version FROM invoices WHERE id = ?), 1), ?, NOW(6))
		 ON DUPLICATE KEY UPDATE snapshot = VALUES(snapshot), valid_from = NOW(6), valid_to = NULL`,
		string(s.ID), string(s.ID), historySnapshot); err != nil {
		return fmt.Errorf("insert invoice history: %w", err)
	}
	return nil
}

func (r *MySQLInvoiceRepository) FindByID(ctx context.Context, id shared.InvoiceID) (*invoice.Invoice, error) {
	row := r.q(ctx).QueryRowContext(ctx, selectInvoiceSQL+` WHERE id = ?`, string(id))
	return scanInvoiceRow(row, id)
}

func (r *MySQLInvoiceRepository) FindByContractID(ctx context.Context, contractID shared.ContractID) ([]*invoice.Invoice, error) {
	rows, err := r.q(ctx).QueryContext(ctx, selectInvoiceSQL+` WHERE contract_id = ? ORDER BY created_at DESC`, string(contractID))
	if err != nil {
		return nil, fmt.Errorf("find invoices by contract: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanInvoiceRows(rows)
}

func (r *MySQLInvoiceRepository) FindByAccountID(ctx context.Context, accountID shared.AccountID) ([]*invoice.Invoice, error) {
	rows, err := r.q(ctx).QueryContext(ctx, selectInvoiceSQL+` WHERE account_id = ? ORDER BY created_at DESC`, string(accountID))
	if err != nil {
		return nil, fmt.Errorf("find invoices by account: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanInvoiceRows(rows)
}

func (r *MySQLInvoiceRepository) FindOverdue(ctx context.Context) ([]*invoice.Invoice, error) {
	// Mirrors the core in-memory reference (infrastructure/inmemory/invoice_repository.go):
	// (a) every invoice already marked 'overdue', regardless of due_date, and
	// (b) 'issued' OR 'finalized' invoices whose due_date has passed.
	rows, err := r.q(ctx).QueryContext(ctx,
		selectInvoiceSQL+` WHERE status = 'overdue' OR (status IN ('issued', 'finalized') AND due_date < NOW(6)) ORDER BY due_date ASC`)
	if err != nil {
		return nil, fmt.Errorf("find overdue invoices: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanInvoiceRows(rows)
}

func (r *MySQLInvoiceRepository) FindByStatus(ctx context.Context, status invoice.InvoiceStatus) ([]*invoice.Invoice, error) {
	rows, err := r.q(ctx).QueryContext(ctx, selectInvoiceSQL+` WHERE status = ? ORDER BY created_at DESC`, string(status))
	if err != nil {
		return nil, fmt.Errorf("find invoices by status: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanInvoiceRows(rows)
}

func (r *MySQLInvoiceRepository) FindByIDAsOf(ctx context.Context, id shared.InvoiceID, asOf time.Time) (*invoice.Invoice, error) {
	var data json.RawMessage
	err := r.q(ctx).QueryRowContext(ctx,
		`SELECT snapshot FROM invoice_history
		 WHERE id = ? AND valid_from <= ? AND (valid_to IS NULL OR valid_to > ?)
		 ORDER BY version DESC LIMIT 1`, string(id), asOf.UTC(), asOf.UTC()).Scan(&data)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, shared.NewDomainError(shared.ErrCodeNotFound,
				fmt.Sprintf("invoice %s not found as of %s", id, asOf))
		}
		return nil, fmt.Errorf("find invoice as of: %w", err)
	}
	var snapshot invoice.InvoiceSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("unmarshal invoice history: %w", err)
	}
	return invoice.InvoiceFromSnapshot(snapshot)
}

func (r *MySQLInvoiceRepository) FindByContractAndStatus(ctx context.Context, contractID shared.ContractID, status invoice.InvoiceStatus) ([]*invoice.Invoice, error) {
	rows, err := r.q(ctx).QueryContext(ctx,
		selectInvoiceSQL+` WHERE contract_id = ? AND status = ? ORDER BY created_at DESC`,
		string(contractID), string(status))
	if err != nil {
		return nil, fmt.Errorf("find invoices by contract and status: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanInvoiceRows(rows)
}

func (r *MySQLInvoiceRepository) FindByContractAndPeriod(ctx context.Context, contractID shared.ContractID, period shared.DateRange) ([]*invoice.Invoice, error) {
	rows, err := r.q(ctx).QueryContext(ctx,
		selectInvoiceSQL+` WHERE contract_id = ? AND issue_date >= ? AND issue_date < ? ORDER BY issue_date ASC`,
		string(contractID), period.Start().UTC(), period.End().UTC())
	if err != nil {
		return nil, fmt.Errorf("find invoices by contract and period: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanInvoiceRows(rows)
}

func (r *MySQLInvoiceRepository) FindUnpaidByContract(ctx context.Context, contractID shared.ContractID) ([]*invoice.Invoice, error) {
	// MySQL has no NULLS LAST; emulate postgres' `due_date ASC NULLS LAST` with
	// a leading `due_date IS NULL` sort key (NULLs collate last).
	rows, err := r.q(ctx).QueryContext(ctx,
		selectInvoiceSQL+` WHERE contract_id = ? AND status NOT IN ('paid', 'voided', 'refunded') ORDER BY due_date IS NULL, due_date ASC`,
		string(contractID))
	if err != nil {
		return nil, fmt.Errorf("find unpaid invoices: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanInvoiceRows(rows)
}

const selectInvoiceSQL = `
	SELECT id, invoice_number, account_id, contract_id, status,
	       line_items, subtotal, tax_amount, discount_amount, total,
	       applied_balance, amount_due, paid_amount, balance,
	       billing_period_from, billing_period_to, issue_date, due_date,
	       paid_at, payment_method_id, allow_partial_pay,
	       original_invoice_id, revision_of, void_reason, metadata
	FROM invoices`

func scanInvoiceRow(row *sql.Row, id shared.InvoiceID) (*invoice.Invoice, error) {
	s, err := scanInvoiceSnapshot(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, shared.NewDomainError(shared.ErrCodeNotFound,
				fmt.Sprintf("invoice %s not found", id))
		}
		return nil, fmt.Errorf("scan invoice: %w", err)
	}
	return invoice.InvoiceFromSnapshot(s)
}

func scanInvoiceRows(rows *sql.Rows) ([]*invoice.Invoice, error) {
	var result []*invoice.Invoice
	for rows.Next() {
		s, err := scanInvoiceSnapshot(rows)
		if err != nil {
			return nil, fmt.Errorf("scan invoice row: %w", err)
		}
		inv, err := invoice.InvoiceFromSnapshot(s)
		if err != nil {
			return nil, fmt.Errorf("reconstruct invoice: %w", err)
		}
		result = append(result, inv)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return result, nil
}

func scanInvoiceSnapshot(t scanTarget) (invoice.InvoiceSnapshot, error) {
	var (
		s                                   invoice.InvoiceSnapshot
		id, number, accountID, contractID   string
		status                              string
		lineItems                           json.RawMessage
		subtotal, taxAmount, discountAmount int64
		total, appliedBalance, amountDue    int64
		paidAmount, balance                 int64
		billingFrom, billingTo              sql.NullTime
		issueDate, dueDate                  sql.NullTime
		paidAt                              sql.NullTime
		paymentMethodID                     sql.NullString
		allowPartialPay                     bool
		originalInvoiceID, revisionOf       sql.NullString
		voidReason                          string
		metadata                            json.RawMessage
	)
	if err := t.Scan(
		&id, &number, &accountID, &contractID, &status,
		&lineItems, &subtotal, &taxAmount, &discountAmount, &total,
		&appliedBalance, &amountDue, &paidAmount, &balance,
		&billingFrom, &billingTo, &issueDate, &dueDate,
		&paidAt, &paymentMethodID, &allowPartialPay,
		&originalInvoiceID, &revisionOf, &voidReason, &metadata,
	); err != nil {
		return invoice.InvoiceSnapshot{}, err
	}

	var js invoiceJSONState
	if len(lineItems) > 0 {
		if err := json.Unmarshal(lineItems, &js); err != nil {
			return invoice.InvoiceSnapshot{}, fmt.Errorf("unmarshal invoice json state: %w", err)
		}
	}

	s.ID = shared.InvoiceID(id)
	s.InvoiceNumber = number
	s.AccountID = shared.AccountID(accountID)
	s.ContractID = shared.ContractID(contractID)
	s.Status = invoice.InvoiceStatus(status)
	s.LineItems = js.LineItems
	s.Subtotal = js.Subtotal
	s.TaxAmount = js.TaxAmount
	s.DiscountAmount = js.DiscountAmount
	s.Total = js.Total
	s.AppliedBalance = js.AppliedBalance
	s.AmountDue = js.AmountDue
	s.PaidAmount = js.PaidAmount
	s.Balance = js.Balance
	s.BillingPeriod = js.BillingPeriod
	if issueDate.Valid {
		s.IssueDate = issueDate.Time.UTC()
	}
	if dueDate.Valid {
		s.DueDate = dueDate.Time.UTC()
	}
	if paidAt.Valid {
		t := paidAt.Time.UTC()
		s.PaidAt = &t
	}
	if paymentMethodID.Valid {
		v := paymentMethodID.String
		s.PaymentMethodID = &v
	}
	s.AllowPartialPay = allowPartialPay
	if originalInvoiceID.Valid {
		v := shared.InvoiceID(originalInvoiceID.String)
		s.OriginalInvoiceID = &v
	}
	if revisionOf.Valid {
		v := shared.InvoiceID(revisionOf.String)
		s.RevisionOf = &v
	}
	s.VoidReason = voidReason
	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &s.Metadata); err != nil {
			return invoice.InvoiceSnapshot{}, fmt.Errorf("unmarshal invoice metadata: %w", err)
		}
	}
	return s, nil
}
