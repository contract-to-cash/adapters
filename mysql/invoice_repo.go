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

// MySQLInvoiceRepository is a MySQL-backed invoice.Repository.
type MySQLInvoiceRepository struct {
	db    *sql.DB
	clock shared.Clock
}

var _ invoice.Repository = (*MySQLInvoiceRepository)(nil)

// NewInvoiceRepository constructs an invoice repository over an existing *sql.DB.
// The clock stamps the bitemporal invoice_history valid_from/valid_to columns;
// inject shared.FixedClock in tests so history windows are deterministic, and
// shared.SystemClock in production. The clock is a required argument, mirroring
// the event store's New(db, clock, ...) so the whole adapter takes its time from
// one injected source rather than a mix of injected and wall-clock reads.
func NewInvoiceRepository(db *sql.DB, clock shared.Clock) *MySQLInvoiceRepository {
	return &MySQLInvoiceRepository{db: db, clock: clock}
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

// Save upserts the invoice and appends its bitemporal history in a single
// atomic unit. The three writes (invoices upsert, close of the prior
// invoice_history row, insert of the new history row) must commit together: run
// as separate auto-committed statements, a crash or connection loss between them
// leaves invoice_history with a permanently-open stale row or a missing version,
// so a later FindByIDAsOf returns the wrong state; two concurrent non-tx Saves
// could likewise interleave and lose a history snapshot.
//
// When an ambient transaction is present (e.g. core's FinalizeInvoice) we join
// it — the caller owns begin/commit, no nesting. Otherwise we wrap the three
// writes in a local transaction, mirroring the event store's Append
// (eventstore.go).
func (r *MySQLInvoiceRepository) Save(ctx context.Context, inv *invoice.Invoice) error {
	if _, ok := TxFromContext(ctx); ok {
		return r.saveOn(ctx, inv)
	}

	sqlTx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin invoice save tx: %w", err)
	}
	if err := r.saveOn(ContextWithTx(ctx, sqlTx), inv); err != nil {
		_ = sqlTx.Rollback()
		return err
	}
	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("commit invoice save tx: %w", err)
	}
	return nil
}

// saveOn performs the three-statement invoice write on the querier resolved from
// ctx (an ambient or Save-local transaction). It must always run inside a
// transaction so the writes are atomic; Save guarantees that.
func (r *MySQLInvoiceRepository) saveOn(ctx context.Context, inv *invoice.Invoice) error {
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

	// Optimistic-lock guarded write (issue #30). MySQL's INSERT ... ON DUPLICATE
	// KEY UPDATE has no WHERE clause, so a single-statement version-guarded upsert
	// is not possible; and the balance-style "LoadedVersion()==0 => INSERT" split
	// is also unsafe here because the billing pipeline persists a draft at
	// version 0 and FinalizeInvoice re-loads it at version 0 (LoadedVersion()==0
	// does NOT imply "no row yet"). We therefore attempt the lock_version-guarded
	// UPDATE first and INSERT only when the row genuinely does not exist. The guard
	// is WHERE lock_version = LoadedVersion(); the separate `version` column stays
	// the per-save counter that keys invoice_history (version = version + 1 here,
	// 1 on insert), so bitemporal history is unaffected. This is option 1 of the
	// Save concurrency contract; the FOR UPDATE read in FindByID (option 2) is kept.
	res, err := q.ExecContext(ctx,
		`UPDATE invoices SET
			invoice_number = ?, status = ?,
			subtotal = ?, tax_amount = ?, discount_amount = ?, total = ?,
			applied_balance = ?, amount_due = ?, paid_amount = ?, balance = ?,
			currency = ?, billing_period_from = ?, billing_period_to = ?,
			issue_date = ?, due_date = ?, paid_at = ?, void_reason = ?,
			revision_of = ?, original_invoice_id = ?, payment_method_id = ?,
			allow_partial_pay = ?, line_items = ?, metadata = ?,
			version = version + 1, lock_version = ?, updated_at = NOW(6)
		 WHERE id = ? AND lock_version = ?`,
		s.InvoiceNumber, string(s.Status),
		s.Subtotal.Int64(), s.TaxAmount.Int64(), s.DiscountAmount.Int64(), s.Total.Int64(),
		s.AppliedBalance.Int64(), s.AmountDue.Int64(), s.PaidAmount.Int64(), s.Balance.Int64(),
		string(s.Total.Currency()), billingFrom, billingTo,
		issueDate, dueDate, paidAt, s.VoidReason,
		revisionOf, originalInvoiceID, s.PaymentMethodID,
		s.AllowPartialPay, jsonState, metadata,
		s.Version, string(s.ID), inv.LoadedVersion())
	if err != nil {
		return translateInvoiceSaveErr(err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("invoice save rows affected: %w", err)
	}

	if affected == 0 {
		// No row matched (id, lock_version = LoadedVersion()). Either the row does
		// not exist yet (brand-new invoice -> INSERT) or it exists at a different
		// lock_version (concurrent writer already advanced it -> optimistic-lock
		// conflict). updated_at = NOW(6) always changes on a matching UPDATE, so a
		// matched row never reports 0 affected — 0 unambiguously means "did not
		// match".
		var probe int
		err := q.QueryRowContext(ctx, `SELECT 1 FROM invoices WHERE id = ?`, string(s.ID)).Scan(&probe)
		switch {
		case err == nil:
			return tx.ErrVersionConflict
		case errors.Is(err, sql.ErrNoRows):
			if _, err := q.ExecContext(ctx,
				`INSERT INTO invoices (
					id, invoice_number, account_id, contract_id, status,
					subtotal, tax_amount, discount_amount, total,
					applied_balance, amount_due, paid_amount, balance,
					currency, billing_period_from, billing_period_to,
					issue_date, due_date, paid_at, void_reason,
					revision_of, original_invoice_id, payment_method_id,
					allow_partial_pay, line_items, metadata, version, lock_version, updated_at
				) VALUES (
					?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
					?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
					?, ?, 1, ?, NOW(6)
				)`,
				string(s.ID), s.InvoiceNumber, string(s.AccountID), string(s.ContractID), string(s.Status),
				s.Subtotal.Int64(), s.TaxAmount.Int64(), s.DiscountAmount.Int64(), s.Total.Int64(),
				s.AppliedBalance.Int64(), s.AmountDue.Int64(), s.PaidAmount.Int64(), s.Balance.Int64(),
				string(s.Total.Currency()), billingFrom, billingTo,
				issueDate, dueDate, paidAt, s.VoidReason,
				revisionOf, originalInvoiceID, s.PaymentMethodID,
				s.AllowPartialPay, jsonState, metadata, s.Version); err != nil {
				return translateInvoiceSaveErr(err)
			}
		default:
			return fmt.Errorf("probe invoice existence: %w", err)
		}
	}

	// Close the current history row and open the new one at a single shared
	// instant. MySQL's NOW(6) is evaluated per statement, so using it in both
	// statements leaves a sub-microsecond gap in which the closed row's valid_to
	// is already past but the new row's valid_from has not yet begun — a
	// FindByIDAsOf at that instant would match neither. Binding one Go-computed
	// timestamp to both closes the gap (postgres avoids it via a tx-stable
	// NOW()). The instant comes from the injected clock (not wall-clock
	// time.Now) so history windows are deterministic under FixedClock and honour
	// the module's clock-injection convention.
	histNow := r.clock.Now().UTC()

	if _, err := q.ExecContext(ctx,
		`UPDATE invoice_history SET valid_to = ? WHERE id = ? AND valid_to IS NULL`,
		histNow, string(s.ID)); err != nil {
		return fmt.Errorf("close invoice history: %w", err)
	}

	historySnapshot, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("marshal invoice history snapshot: %w", err)
	}

	if _, err := q.ExecContext(ctx,
		`INSERT INTO invoice_history (id, version, snapshot, valid_from)
		 VALUES (?, COALESCE((SELECT version FROM invoices WHERE id = ?), 1), ?, ?)
		 ON DUPLICATE KEY UPDATE snapshot = VALUES(snapshot), valid_from = VALUES(valid_from), valid_to = NULL`,
		string(s.ID), string(s.ID), historySnapshot, histNow); err != nil {
		return fmt.Errorf("insert invoice history: %w", err)
	}

	// Record the persisted optimistic-lock version as the new loaded baseline so a
	// subsequent Save of this same in-memory entity compares against the right one.
	inv.SetVersion(s.Version)
	return nil
}

// translateInvoiceSaveErr maps driver-level duplicate-key errors raised by the
// invoices write to structured domain errors:
//   - ux_invoice_period (migration 011) -> shared.ErrCodeConflict with the
//     per-period message from the inmemory reference (issue #45).
//   - PRIMARY (a concurrent INSERT of the same id slipped in between our guarded
//     UPDATE and existence probe) -> tx.ErrVersionConflict, so the caller retries
//     exactly as it would for a version conflict.
//
// Any other error is wrapped verbatim.
func translateInvoiceSaveErr(err error) error {
	if dupEntryOnKey(err, "ux_invoice_period") {
		return shared.NewDomainError(shared.ErrCodeConflict,
			"invoice already exists for this billing period")
	}
	if dupEntryOnKey(err, "PRIMARY") {
		return tx.ErrVersionConflict
	}
	return fmt.Errorf("save invoice: %w", err)
}

func (r *MySQLInvoiceRepository) FindByID(ctx context.Context, id shared.InvoiceID) (*invoice.Invoice, error) {
	// When invoked inside an ambient transaction (e.g. core's
	// BillingService.FinalizeInvoice, which reads -> Finalize -> Save within one
	// tx), take a row lock with SELECT ... FOR UPDATE. This serializes concurrent
	// finalizers on the same invoice: the loser blocks until the winner commits,
	// then reads the already-finalized row and is rejected by Finalize() with
	// invalid_state_transition. Without the lock, both readers would see the draft
	// row and both would finalize, double-firing OnInvoiceIssuedHook. Outside a
	// transaction (pooled read) no lock is taken.
	query := selectInvoiceSQL + ` WHERE id = ?`
	if _, inTx := TxFromContext(ctx); inTx {
		query += ` FOR UPDATE`
	}
	row := r.q(ctx).QueryRowContext(ctx, query, string(id))
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
	// Match on billing-period EQUALITY (start AND end), mirroring the core
	// in-memory reference (infrastructure/inmemory/invoice_repository.go): an
	// invoice belongs to the queried period iff its stored billing_period_from /
	// billing_period_to equal the query period's start / end. Matching on
	// issue_date instead is wrong — an arrears invoice for June issued on July 1
	// has an issue_date OUTSIDE June, so it would be missed by the duplicate
	// pre-check and the RegenerateInvoice voided-original lookup, and an
	// unrelated invoice merely issued within the window would be falsely returned.
	// billing_period_from/to are stored in UTC (see saveOn), so the query bounds
	// are normalised to UTC for exact DATETIME(6) equality. Zero-period invoices
	// store NULL and never match a non-zero query period.
	rows, err := r.q(ctx).QueryContext(ctx,
		selectInvoiceSQL+` WHERE contract_id = ? AND billing_period_from = ? AND billing_period_to = ? ORDER BY issue_date ASC`,
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
	       original_invoice_id, revision_of, void_reason, metadata, lock_version
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
		lockVersion                         int
	)
	if err := t.Scan(
		&id, &number, &accountID, &contractID, &status,
		&lineItems, &subtotal, &taxAmount, &discountAmount, &total,
		&appliedBalance, &amountDue, &paidAmount, &balance,
		&billingFrom, &billingTo, &issueDate, &dueDate,
		&paidAt, &paymentMethodID, &allowPartialPay,
		&originalInvoiceID, &revisionOf, &voidReason, &metadata, &lockVersion,
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
	// The invoices.lock_version column is authoritative for the current-state
	// optimistic-locking version (issue #30). InvoiceFromSnapshot restores both
	// version and loadedVersion from this, so the next Save guards on it.
	s.Version = lockVersion
	return s, nil
}
