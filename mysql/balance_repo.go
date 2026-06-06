package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/contract-to-cash/core/application/tx"
	"github.com/contract-to-cash/core/domain/balance"
	"github.com/contract-to-cash/core/domain/shared"
)

// MySQLBalanceRepository is a MySQL-backed balance.Repository with optimistic locking.
type MySQLBalanceRepository struct {
	db *sql.DB
}

var _ balance.Repository = (*MySQLBalanceRepository)(nil)

// NewBalanceRepository constructs a balance repository over an existing *sql.DB.
func NewBalanceRepository(db *sql.DB) *MySQLBalanceRepository {
	return &MySQLBalanceRepository{db: db}
}

func (r *MySQLBalanceRepository) q(ctx context.Context) Querier {
	return querierFromContext(ctx, r.db)
}

func (r *MySQLBalanceRepository) Save(ctx context.Context, entry *balance.BalanceEntry) error {
	s := entry.ToSnapshot()
	loaded := entry.LoadedVersion()
	q := r.q(ctx)

	var expiresAt *time.Time
	if s.ExpiresAt != nil {
		t := s.ExpiresAt.UTC()
		expiresAt = &t
	}

	if loaded == 0 {
		_, err := q.ExecContext(ctx,
			`INSERT INTO balance_entries (id, account_id, original_amount, remaining_amount, currency,
				reason, source_type, source_id, description, expires_at, version, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			string(s.ID), string(s.AccountID),
			s.OriginalAmount.Int64(), s.RemainingAmount.Int64(),
			string(s.OriginalAmount.Currency()),
			string(s.Reason), string(s.SourceType), s.SourceID, s.Description,
			expiresAt, s.Version, s.CreatedAt.UTC())
		if err != nil {
			return fmt.Errorf("insert balance entry: %w", err)
		}
		entry.SetVersion(s.Version)
		return nil
	}

	res, err := q.ExecContext(ctx,
		`UPDATE balance_entries SET
			original_amount = ?, remaining_amount = ?, currency = ?,
			reason = ?, source_type = ?, source_id = ?,
			description = ?, expires_at = ?, version = ?
		 WHERE id = ? AND version = ?`,
		s.OriginalAmount.Int64(), s.RemainingAmount.Int64(),
		string(s.OriginalAmount.Currency()),
		string(s.Reason), string(s.SourceType), s.SourceID, s.Description,
		expiresAt, s.Version, string(s.ID), loaded)
	if err != nil {
		return fmt.Errorf("update balance entry: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("balance entry rows affected: %w", err)
	}
	if affected == 0 {
		return tx.ErrVersionConflict
	}
	entry.SetVersion(s.Version)
	return nil
}

func (r *MySQLBalanceRepository) FindByID(ctx context.Context, id shared.BalanceEntryID) (*balance.BalanceEntry, error) {
	row := r.q(ctx).QueryRowContext(ctx, selectBalanceEntrySQL+` WHERE id = ?`, string(id))
	s, err := scanBalanceEntrySnapshot(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, shared.NewDomainError(shared.ErrCodeNotFound,
				fmt.Sprintf("balance entry %s not found", id))
		}
		return nil, fmt.Errorf("scan balance entry: %w", err)
	}
	return balance.FromSnapshot(s)
}

func (r *MySQLBalanceRepository) FindAvailable(ctx context.Context, accountID shared.AccountID, currency shared.Currency) ([]*balance.BalanceEntry, error) {
	rows, err := r.q(ctx).QueryContext(ctx,
		selectBalanceEntrySQL+`
		 WHERE account_id = ? AND currency = ? AND remaining_amount > 0
		   AND (expires_at IS NULL OR expires_at > NOW(6))
		 ORDER BY created_at ASC`,
		string(accountID), string(currency))
	if err != nil {
		return nil, fmt.Errorf("find available balance: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanBalanceEntries(rows)
}

func (r *MySQLBalanceRepository) GetBalance(ctx context.Context, accountID shared.AccountID, currency shared.Currency) (shared.Money, error) {
	var total int64
	err := r.q(ctx).QueryRowContext(ctx,
		`SELECT COALESCE(SUM(remaining_amount), 0) FROM balance_entries
		 WHERE account_id = ? AND currency = ?
		   AND (expires_at IS NULL OR expires_at > NOW(6))`,
		string(accountID), string(currency)).Scan(&total)
	if err != nil {
		return shared.Money{}, fmt.Errorf("get balance: %w", err)
	}
	return shared.NewMoney(new(big.Rat).SetInt64(total), currency), nil
}

func (r *MySQLBalanceRepository) SaveApplication(ctx context.Context, app *balance.BalanceApplication) error {
	_, err := r.q(ctx).ExecContext(ctx,
		`INSERT INTO balance_applications (id, balance_entry_id, invoice_id, amount, currency, applied_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		app.ID, string(app.BalanceEntryID), string(app.InvoiceID),
		app.Amount.Int64(), string(app.Amount.Currency()), app.AppliedAt.UTC())
	if err != nil {
		return fmt.Errorf("save balance application: %w", err)
	}
	return nil
}

func (r *MySQLBalanceRepository) FindApplicationsByInvoice(ctx context.Context, invoiceID shared.InvoiceID) ([]*balance.BalanceApplication, error) {
	rows, err := r.q(ctx).QueryContext(ctx,
		`SELECT id, balance_entry_id, invoice_id, amount, currency, applied_at
		 FROM balance_applications WHERE invoice_id = ? ORDER BY applied_at ASC`,
		string(invoiceID))
	if err != nil {
		return nil, fmt.Errorf("find balance applications: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var result []*balance.BalanceApplication
	for rows.Next() {
		var (
			id, entryID, invID string
			amount             int64
			currency           string
			appliedAt          utcTime
		)
		if err := rows.Scan(&id, &entryID, &invID, &amount, &currency, &appliedAt); err != nil {
			return nil, fmt.Errorf("scan balance application: %w", err)
		}
		result = append(result, &balance.BalanceApplication{
			ID: id, BalanceEntryID: shared.BalanceEntryID(entryID),
			InvoiceID: shared.InvoiceID(invID),
			Amount:    moneyFromInt64(amount, shared.Currency(currency)),
			AppliedAt: appliedAt.Time,
		})
	}
	return result, rows.Err()
}

func (r *MySQLBalanceRepository) SaveRefund(ctx context.Context, refund *balance.BalanceRefund) error {
	_, err := r.q(ctx).ExecContext(ctx,
		`INSERT INTO balance_refunds (id, balance_entry_id, account_id, amount, currency, refunded_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		refund.ID, string(refund.BalanceEntryID), string(refund.AccountID),
		refund.Amount.Int64(), string(refund.Amount.Currency()), refund.RefundedAt.UTC())
	if err != nil {
		return fmt.Errorf("save balance refund: %w", err)
	}
	return nil
}

func (r *MySQLBalanceRepository) FindByAccountID(ctx context.Context, accountID shared.AccountID, currency shared.Currency) ([]*balance.BalanceEntry, error) {
	rows, err := r.q(ctx).QueryContext(ctx,
		selectBalanceEntrySQL+` WHERE account_id = ? AND currency = ? ORDER BY created_at ASC`,
		string(accountID), string(currency))
	if err != nil {
		return nil, fmt.Errorf("find balance by account: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanBalanceEntries(rows)
}

const selectBalanceEntrySQL = `
	SELECT id, account_id, original_amount, remaining_amount, currency,
	       reason, source_type, source_id, description, expires_at, version, created_at
	FROM balance_entries`

func scanBalanceEntries(rows *sql.Rows) ([]*balance.BalanceEntry, error) {
	var result []*balance.BalanceEntry
	for rows.Next() {
		s, err := scanBalanceEntrySnapshot(rows)
		if err != nil {
			return nil, fmt.Errorf("scan balance entry: %w", err)
		}
		entry, err := balance.FromSnapshot(s)
		if err != nil {
			return nil, fmt.Errorf("reconstruct balance entry: %w", err)
		}
		result = append(result, entry)
	}
	return result, rows.Err()
}

func scanBalanceEntrySnapshot(t scanTarget) (balance.BalanceEntrySnapshot, error) {
	var (
		s                            balance.BalanceEntrySnapshot
		id, accountID                string
		original, remaining          int64
		currency, reason, sourceType string
		sourceID, description        string
		expiresAt                    sql.NullTime
		version                      int
		createdAt                    utcTime
	)
	if err := t.Scan(
		&id, &accountID, &original, &remaining, &currency,
		&reason, &sourceType, &sourceID, &description, &expiresAt, &version, &createdAt,
	); err != nil {
		return balance.BalanceEntrySnapshot{}, err
	}

	cur := shared.Currency(currency)
	s.ID = shared.BalanceEntryID(id)
	s.AccountID = shared.AccountID(accountID)
	s.OriginalAmount = moneyFromInt64(original, cur)
	s.RemainingAmount = moneyFromInt64(remaining, cur)
	s.Reason = balance.BalanceReason(reason)
	s.SourceType = balance.BalanceSourceType(sourceType)
	s.SourceID = sourceID
	s.Description = description
	if expiresAt.Valid {
		t := expiresAt.Time.UTC()
		s.ExpiresAt = &t
	}
	s.CreatedAt = createdAt.Time
	s.Version = version
	return s, nil
}
