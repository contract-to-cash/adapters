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

type PostgresInvoiceRepository struct {
	pool *pgxpool.Pool
}

var _ invoice.Repository = (*PostgresInvoiceRepository)(nil)

func NewInvoiceRepository(pool *pgxpool.Pool) *PostgresInvoiceRepository {
	return &PostgresInvoiceRepository{pool: pool}
}

func (r *PostgresInvoiceRepository) q(ctx context.Context) Querier {
	return QuerierFromContext(ctx, r.pool)
}

// invoiceSnapshotState holds monetary subtotals and billing period in JSONB
// for lossless round-trip of non-JPY currencies. LineItems are persisted
// separately in the line_items JSONB array column so the column name matches
// its content.
type invoiceSnapshotState struct {
	Subtotal       shared.Money     `json:"subtotal"`
	TaxAmount      shared.Money     `json:"tax_amount"`
	DiscountAmount shared.Money     `json:"discount_amount"`
	Total          shared.Money     `json:"total"`
	AppliedBalance shared.Money     `json:"applied_balance"`
	AmountDue      shared.Money     `json:"amount_due"`
	PaidAmount     shared.Money     `json:"paid_amount"`
	Balance        shared.Money     `json:"balance"`
	BillingPeriod  shared.DateRange `json:"billing_period"`
}

func (r *PostgresInvoiceRepository) Save(ctx context.Context, inv *invoice.Invoice) error {
	s := inv.ToSnapshot()

	snapshotState, err := json.Marshal(invoiceSnapshotState{
		Subtotal: s.Subtotal, TaxAmount: s.TaxAmount, DiscountAmount: s.DiscountAmount,
		Total: s.Total, AppliedBalance: s.AppliedBalance, AmountDue: s.AmountDue,
		PaidAmount: s.PaidAmount, Balance: s.Balance, BillingPeriod: s.BillingPeriod,
	})
	if err != nil {
		return fmt.Errorf("marshal invoice snapshot state: %w", err)
	}

	lineItems, err := json.Marshal(s.LineItems)
	if err != nil {
		return fmt.Errorf("marshal invoice line items: %w", err)
	}
	if len(s.LineItems) == 0 {
		lineItems = []byte(`[]`)
	}

	metadata, err := json.Marshal(s.Metadata)
	if err != nil {
		return fmt.Errorf("marshal invoice metadata: %w", err)
	}

	var billingFrom, billingTo, issueDate, dueDate *time.Time
	if !s.BillingPeriod.IsZero() {
		from, to := s.BillingPeriod.Start(), s.BillingPeriod.End()
		billingFrom, billingTo = &from, &to
	}
	if !s.IssueDate.IsZero() {
		t := s.IssueDate
		issueDate = &t
	}
	if !s.DueDate.IsZero() {
		t := s.DueDate
		dueDate = &t
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
	_, err = q.Exec(ctx,
		`INSERT INTO invoices (
			id, invoice_number, account_id, contract_id, status,
			subtotal, tax_amount, discount_amount, total,
			applied_balance, amount_due, paid_amount, balance,
			currency, billing_period_from, billing_period_to,
			issue_date, due_date, paid_at, void_reason,
			revision_of, original_invoice_id, payment_method_id,
			allow_partial_pay, line_items, snapshot_state, metadata, version, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
			$14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24,
			$25, $26, $27, 1, NOW()
		)
		ON CONFLICT (id) DO UPDATE SET
			invoice_number = EXCLUDED.invoice_number, status = EXCLUDED.status,
			subtotal = EXCLUDED.subtotal, tax_amount = EXCLUDED.tax_amount,
			discount_amount = EXCLUDED.discount_amount, total = EXCLUDED.total,
			applied_balance = EXCLUDED.applied_balance, amount_due = EXCLUDED.amount_due,
			paid_amount = EXCLUDED.paid_amount, balance = EXCLUDED.balance,
			currency = EXCLUDED.currency, billing_period_from = EXCLUDED.billing_period_from,
			billing_period_to = EXCLUDED.billing_period_to, issue_date = EXCLUDED.issue_date,
			due_date = EXCLUDED.due_date, paid_at = EXCLUDED.paid_at,
			void_reason = EXCLUDED.void_reason, revision_of = EXCLUDED.revision_of,
			original_invoice_id = EXCLUDED.original_invoice_id,
			payment_method_id = EXCLUDED.payment_method_id,
			allow_partial_pay = EXCLUDED.allow_partial_pay,
			line_items = EXCLUDED.line_items,
			snapshot_state = EXCLUDED.snapshot_state,
			metadata = EXCLUDED.metadata,
			version = invoices.version + 1, updated_at = NOW()`,
		string(s.ID), s.InvoiceNumber, string(s.AccountID), string(s.ContractID), string(s.Status),
		s.Subtotal.Int64(), s.TaxAmount.Int64(), s.DiscountAmount.Int64(), s.Total.Int64(),
		s.AppliedBalance.Int64(), s.AmountDue.Int64(), s.PaidAmount.Int64(), s.Balance.Int64(),
		string(s.Total.Currency()), billingFrom, billingTo,
		issueDate, dueDate, s.PaidAt, s.VoidReason,
		revisionOf, originalInvoiceID, s.PaymentMethodID,
		s.AllowPartialPay, lineItems, snapshotState, metadata)
	if err != nil {
		return fmt.Errorf("save invoice: %w", err)
	}

	if _, err := q.Exec(ctx,
		`UPDATE invoice_history SET valid_to = NOW() WHERE id = $1 AND valid_to IS NULL`,
		string(s.ID)); err != nil {
		return fmt.Errorf("close invoice history: %w", err)
	}

	historySnapshot, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("marshal invoice history snapshot: %w", err)
	}

	if _, err := q.Exec(ctx,
		`INSERT INTO invoice_history (id, version, snapshot, valid_from)
		 VALUES ($1, COALESCE((SELECT version FROM invoices WHERE id = $1), 1), $2, NOW())
		 ON CONFLICT (id, version) DO UPDATE SET snapshot = EXCLUDED.snapshot, valid_from = NOW(), valid_to = NULL`,
		string(s.ID), historySnapshot); err != nil {
		return fmt.Errorf("insert invoice history: %w", err)
	}
	return nil
}

func (r *PostgresInvoiceRepository) FindByID(ctx context.Context, id shared.InvoiceID) (*invoice.Invoice, error) {
	row := r.q(ctx).QueryRow(ctx, selectInvoiceSQL+` WHERE id = $1`, string(id))
	return scanInvoiceRow(row, id)
}

func (r *PostgresInvoiceRepository) FindByContractID(ctx context.Context, contractID shared.ContractID) ([]*invoice.Invoice, error) {
	rows, err := r.q(ctx).Query(ctx, selectInvoiceSQL+` WHERE contract_id = $1 ORDER BY created_at DESC`, string(contractID))
	if err != nil {
		return nil, fmt.Errorf("find invoices by contract: %w", err)
	}
	defer rows.Close()
	return scanInvoiceRows(rows)
}

func (r *PostgresInvoiceRepository) FindByAccountID(ctx context.Context, accountID shared.AccountID) ([]*invoice.Invoice, error) {
	rows, err := r.q(ctx).Query(ctx, selectInvoiceSQL+` WHERE account_id = $1 ORDER BY created_at DESC`, string(accountID))
	if err != nil {
		return nil, fmt.Errorf("find invoices by account: %w", err)
	}
	defer rows.Close()
	return scanInvoiceRows(rows)
}

func (r *PostgresInvoiceRepository) FindOverdue(ctx context.Context) ([]*invoice.Invoice, error) {
	rows, err := r.q(ctx).Query(ctx, selectInvoiceSQL+` WHERE status = 'issued' AND due_date < NOW() ORDER BY due_date ASC`)
	if err != nil {
		return nil, fmt.Errorf("find overdue invoices: %w", err)
	}
	defer rows.Close()
	return scanInvoiceRows(rows)
}

func (r *PostgresInvoiceRepository) FindByStatus(ctx context.Context, status invoice.InvoiceStatus) ([]*invoice.Invoice, error) {
	rows, err := r.q(ctx).Query(ctx, selectInvoiceSQL+` WHERE status = $1 ORDER BY created_at DESC`, string(status))
	if err != nil {
		return nil, fmt.Errorf("find invoices by status: %w", err)
	}
	defer rows.Close()
	return scanInvoiceRows(rows)
}

func (r *PostgresInvoiceRepository) FindByIDAsOf(ctx context.Context, id shared.InvoiceID, asOf time.Time) (*invoice.Invoice, error) {
	var data json.RawMessage
	err := r.q(ctx).QueryRow(ctx,
		`SELECT snapshot FROM invoice_history
		 WHERE id = $1 AND valid_from <= $2 AND (valid_to IS NULL OR valid_to > $2)
		 ORDER BY version DESC LIMIT 1`, string(id), asOf).Scan(&data)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
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

func (r *PostgresInvoiceRepository) FindByContractAndStatus(ctx context.Context, contractID shared.ContractID, status invoice.InvoiceStatus) ([]*invoice.Invoice, error) {
	rows, err := r.q(ctx).Query(ctx,
		selectInvoiceSQL+` WHERE contract_id = $1 AND status = $2 ORDER BY created_at DESC`,
		string(contractID), string(status))
	if err != nil {
		return nil, fmt.Errorf("find invoices by contract and status: %w", err)
	}
	defer rows.Close()
	return scanInvoiceRows(rows)
}

func (r *PostgresInvoiceRepository) FindByContractAndPeriod(ctx context.Context, contractID shared.ContractID, period shared.DateRange) ([]*invoice.Invoice, error) {
	rows, err := r.q(ctx).Query(ctx,
		selectInvoiceSQL+` WHERE contract_id = $1 AND issue_date >= $2 AND issue_date < $3 ORDER BY issue_date ASC`,
		string(contractID), period.Start(), period.End())
	if err != nil {
		return nil, fmt.Errorf("find invoices by contract and period: %w", err)
	}
	defer rows.Close()
	return scanInvoiceRows(rows)
}

func (r *PostgresInvoiceRepository) FindUnpaidByContract(ctx context.Context, contractID shared.ContractID) ([]*invoice.Invoice, error) {
	rows, err := r.q(ctx).Query(ctx,
		selectInvoiceSQL+` WHERE contract_id = $1 AND status NOT IN ('paid', 'voided', 'cancelled') ORDER BY due_date ASC NULLS LAST`,
		string(contractID))
	if err != nil {
		return nil, fmt.Errorf("find unpaid invoices: %w", err)
	}
	defer rows.Close()
	return scanInvoiceRows(rows)
}

const selectInvoiceSQL = `
	SELECT id, invoice_number, account_id, contract_id, status,
	       line_items, snapshot_state, subtotal, tax_amount, discount_amount, total,
	       applied_balance, amount_due, paid_amount, balance, currency,
	       billing_period_from, billing_period_to, issue_date, due_date,
	       paid_at, payment_method_id, allow_partial_pay,
	       original_invoice_id, revision_of, void_reason, metadata
	FROM invoices`

type scanTarget interface {
	Scan(dest ...any) error
}

func scanInvoiceRow(row pgx.Row, id shared.InvoiceID) (*invoice.Invoice, error) {
	s, err := scanInvoiceSnapshot(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, shared.NewDomainError(shared.ErrCodeNotFound,
				fmt.Sprintf("invoice %s not found", id))
		}
		return nil, fmt.Errorf("scan invoice: %w", err)
	}
	return invoice.InvoiceFromSnapshot(s)
}

func scanInvoiceRows(rows pgx.Rows) ([]*invoice.Invoice, error) {
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
		lineItemsRaw                        json.RawMessage
		snapshotStateRaw                    json.RawMessage
		subtotal, taxAmount, discountAmount int64
		total, appliedBalance, amountDue    int64
		paidAmount, balance                 int64
		currency                            string
		billingFrom, billingTo              *time.Time
		issueDate, dueDate                  *time.Time
		paidAt                              *time.Time
		paymentMethodID                     *string
		allowPartialPay                     bool
		originalInvoiceID, revisionOf       *string
		voidReason                          string
		metadata                            json.RawMessage
	)
	if err := t.Scan(
		&id, &number, &accountID, &contractID, &status,
		&lineItemsRaw, &snapshotStateRaw,
		&subtotal, &taxAmount, &discountAmount, &total,
		&appliedBalance, &amountDue, &paidAmount, &balance, &currency,
		&billingFrom, &billingTo, &issueDate, &dueDate,
		&paidAt, &paymentMethodID, &allowPartialPay,
		&originalInvoiceID, &revisionOf, &voidReason, &metadata,
	); err != nil {
		return invoice.InvoiceSnapshot{}, err
	}

	// Parse line items (JSONB array) and snapshot state (JSONB object)
	// from their dedicated columns.
	var lineItems []invoice.LineItemSnapshot
	if len(lineItemsRaw) > 0 {
		if err := json.Unmarshal(lineItemsRaw, &lineItems); err != nil {
			return invoice.InvoiceSnapshot{}, fmt.Errorf("unmarshal invoice line items: %w", err)
		}
	}

	var ss invoiceSnapshotState
	hasSnapshotState := false
	if len(snapshotStateRaw) > 0 && string(snapshotStateRaw) != "{}" {
		if err := json.Unmarshal(snapshotStateRaw, &ss); err != nil {
			return invoice.InvoiceSnapshot{}, fmt.Errorf("unmarshal invoice snapshot state: %w", err)
		}
		hasSnapshotState = true
	}

	s.ID = shared.InvoiceID(id)
	s.InvoiceNumber = number
	s.AccountID = shared.AccountID(accountID)
	s.ContractID = shared.ContractID(contractID)
	s.Status = invoice.InvoiceStatus(status)
	s.LineItems = lineItems

	// Prefer the lossless JSONB snapshot state when present; otherwise
	// reconstruct from the BIGINT columns (lossy for non-JPY but gives
	// a working fallback for rows written by external processes or
	// pre-migration rows).
	cur := shared.Currency(currency)
	if hasSnapshotState {
		s.Subtotal = ss.Subtotal
		s.TaxAmount = ss.TaxAmount
		s.DiscountAmount = ss.DiscountAmount
		s.Total = ss.Total
		s.AppliedBalance = ss.AppliedBalance
		s.AmountDue = ss.AmountDue
		s.PaidAmount = ss.PaidAmount
		s.Balance = ss.Balance
		s.BillingPeriod = ss.BillingPeriod
	} else {
		s.Subtotal = moneyFromInt64(subtotal, cur)
		s.TaxAmount = moneyFromInt64(taxAmount, cur)
		s.DiscountAmount = moneyFromInt64(discountAmount, cur)
		s.Total = moneyFromInt64(total, cur)
		s.AppliedBalance = moneyFromInt64(appliedBalance, cur)
		s.AmountDue = moneyFromInt64(amountDue, cur)
		s.PaidAmount = moneyFromInt64(paidAmount, cur)
		s.Balance = moneyFromInt64(balance, cur)
	}
	// Fall back to billing_period_from/to columns if the snapshot state
	// does not carry a billing period.
	if s.BillingPeriod.IsZero() && billingFrom != nil && billingTo != nil {
		s.BillingPeriod = shared.NewDateRange(*billingFrom, *billingTo)
	}

	if issueDate != nil {
		s.IssueDate = *issueDate
	}
	if dueDate != nil {
		s.DueDate = *dueDate
	}
	s.PaidAt = paidAt
	s.PaymentMethodID = paymentMethodID
	s.AllowPartialPay = allowPartialPay
	if originalInvoiceID != nil {
		v := shared.InvoiceID(*originalInvoiceID)
		s.OriginalInvoiceID = &v
	}
	if revisionOf != nil {
		v := shared.InvoiceID(*revisionOf)
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
