package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/contract-to-cash/core/application/tx"
	"github.com/contract-to-cash/core/domain/balance"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresBalanceRepository implements balance.Repository.
//
// Balance entries use optimistic locking: Save compares the entry's
// LoadedVersion() with the persisted version and returns tx.ErrVersionConflict
// on mismatch so that tx.RetryOnConflict can retry the caller's closure.
type PostgresBalanceRepository struct {
	pool *pgxpool.Pool
}

var _ balance.Repository = (*PostgresBalanceRepository)(nil)

// NewBalanceRepository creates a new PostgresBalanceRepository.
func NewBalanceRepository(pool *pgxpool.Pool) *PostgresBalanceRepository {
	return &PostgresBalanceRepository{pool: pool}
}

func (r *PostgresBalanceRepository) q(ctx context.Context) Querier {
	return QuerierFromContext(ctx, r.pool)
}

// balanceJSONState is the lossless monetary payload for a balance entry.
type balanceJSONState struct {
	OriginalAmount  shared.Money `json:"original_amount"`
	RemainingAmount shared.Money `json:"remaining_amount"`
}

// Save upserts a balance entry using optimistic locking.
//
// On new entries (version == 0 after the first mutation), a plain INSERT is
// used. On existing entries, an UPDATE with a version guard is used so that
// concurrent updates produce a predictable tx.ErrVersionConflict.
func (r *PostgresBalanceRepository) Save(ctx context.Context, entry *balance.BalanceEntry) error {
	s := entry.ToSnapshot()
	loaded := entry.LoadedVersion()

	jsonState, err := json.Marshal(balanceJSONState{
		OriginalAmount:  s.OriginalAmount,
		RemainingAmount: s.RemainingAmount,
	})
	if err != nil {
		return fmt.Errorf("marshal balance entry json state: %w", err)
	}
	_ = jsonState // retained for a future lossless-money column

	q := r.q(ctx)

	if loaded == 0 {
		_, err = q.Exec(ctx,
			`INSERT INTO balance_entries (
				id, account_id, original_amount, remaining_amount, currency,
				reason, source_type, source_id, description, expires_at, version, created_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
			)`,
			string(s.ID), string(s.AccountID),
			s.OriginalAmount.Int64(), s.RemainingAmount.Int64(),
			string(s.OriginalAmount.Currency()),
			string(s.Reason), string(s.SourceType), s.SourceID, s.Description,
			s.ExpiresAt, s.Version, s.CreatedAt,
		)
		if err != nil {
			return fmt.Errorf("insert balance entry: %w", err)
		}
		entry.SetVersion(s.Version)
		return nil
	}

	tag, err := q.Exec(ctx,
		`UPDATE balance_entries SET
			original_amount  = $2,
			remaining_amount = $3,
			currency         = $4,
			reason           = $5,
			source_type      = $6,
			source_id        = $7,
			description      = $8,
			expires_at       = $9,
			version          = $10
		 WHERE id = $1 AND version = $11`,
		string(s.ID),
		s.OriginalAmount.Int64(), s.RemainingAmount.Int64(),
		string(s.OriginalAmount.Currency()),
		string(s.Reason), string(s.SourceType), s.SourceID, s.Description,
		s.ExpiresAt, s.Version, loaded,
	)
	if err != nil {
		return fmt.Errorf("update balance entry: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return tx.ErrVersionConflict
	}
	entry.SetVersion(s.Version)
	return nil
}

// FindByID loads a balance entry by its ID.
func (r *PostgresBalanceRepository) FindByID(ctx context.Context, id shared.BalanceEntryID) (*balance.BalanceEntry, error) {
	row := r.q(ctx).QueryRow(ctx, selectBalanceEntrySQL+` WHERE id = $1`, string(id))
	s, err := scanBalanceEntrySnapshot(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, shared.NewDomainError(shared.ErrCodeNotFound,
				fmt.Sprintf("balance entry %s not found", id))
		}
		return nil, fmt.Errorf("scan balance entry: %w", err)
	}
	return balance.FromSnapshot(s)
}

// FindAvailable returns entries that still have remaining balance.
func (r *PostgresBalanceRepository) FindAvailable(ctx context.Context, accountID shared.AccountID, currency shared.Currency) ([]*balance.BalanceEntry, error) {
	rows, err := r.q(ctx).Query(ctx,
		selectBalanceEntrySQL+`
		 WHERE account_id = $1 AND currency = $2 AND remaining_amount > 0
		   AND (expires_at IS NULL OR expires_at > NOW())
		 ORDER BY created_at ASC`,
		string(accountID), string(currency),
	)
	if err != nil {
		return nil, fmt.Errorf("find available balance: %w", err)
	}
	defer rows.Close()
	return scanBalanceEntries(rows)
}

// GetBalance returns the total remaining balance for an account+currency.
func (r *PostgresBalanceRepository) GetBalance(ctx context.Context, accountID shared.AccountID, currency shared.Currency) (shared.Money, error) {
	var total int64
	err := r.q(ctx).QueryRow(ctx,
		`SELECT COALESCE(SUM(remaining_amount), 0) FROM balance_entries
		 WHERE account_id = $1 AND currency = $2
		   AND (expires_at IS NULL OR expires_at > NOW())`,
		string(accountID), string(currency),
	).Scan(&total)
	if err != nil {
		return shared.Money{}, fmt.Errorf("get balance: %w", err)
	}
	return shared.NewMoney(new(big.Rat).SetInt64(total), currency), nil
}

// SaveApplication persists a balance application.
func (r *PostgresBalanceRepository) SaveApplication(ctx context.Context, app *balance.BalanceApplication) error {
	_, err := r.q(ctx).Exec(ctx,
		`INSERT INTO balance_applications (id, balance_entry_id, invoice_id, amount, currency, applied_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		app.ID, string(app.BalanceEntryID), string(app.InvoiceID),
		app.Amount.Int64(), string(app.Amount.Currency()), app.AppliedAt,
	)
	if err != nil {
		return fmt.Errorf("save balance application: %w", err)
	}
	return nil
}

// FindApplicationsByInvoice returns all balance applications for an invoice.
func (r *PostgresBalanceRepository) FindApplicationsByInvoice(ctx context.Context, invoiceID shared.InvoiceID) ([]*balance.BalanceApplication, error) {
	rows, err := r.q(ctx).Query(ctx,
		`SELECT id, balance_entry_id, invoice_id, amount, currency, applied_at
		 FROM balance_applications WHERE invoice_id = $1 ORDER BY applied_at ASC`,
		string(invoiceID),
	)
	if err != nil {
		return nil, fmt.Errorf("find balance applications: %w", err)
	}
	defer rows.Close()

	var result []*balance.BalanceApplication
	for rows.Next() {
		var (
			id, entryID, invID string
			amount             int64
			currency           string
			appliedAt          time.Time
		)
		if err := rows.Scan(&id, &entryID, &invID, &amount, &currency, &appliedAt); err != nil {
			return nil, fmt.Errorf("scan balance application: %w", err)
		}
		result = append(result, &balance.BalanceApplication{
			ID:             id,
			BalanceEntryID: shared.BalanceEntryID(entryID),
			InvoiceID:      shared.InvoiceID(invID),
			Amount:         moneyFromInt64(amount, shared.Currency(currency)),
			AppliedAt:      appliedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return result, nil
}

// SaveRefund persists a balance refund.
func (r *PostgresBalanceRepository) SaveRefund(ctx context.Context, refund *balance.BalanceRefund) error {
	_, err := r.q(ctx).Exec(ctx,
		`INSERT INTO balance_refunds (id, balance_entry_id, account_id, amount, currency, refunded_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		refund.ID, string(refund.BalanceEntryID), string(refund.AccountID),
		refund.Amount.Int64(), string(refund.Amount.Currency()), refund.RefundedAt,
	)
	if err != nil {
		return fmt.Errorf("save balance refund: %w", err)
	}
	return nil
}

// FindByAccountID returns every entry for an account+currency (including
// expired and fully-consumed ones).
func (r *PostgresBalanceRepository) FindByAccountID(ctx context.Context, accountID shared.AccountID, currency shared.Currency) ([]*balance.BalanceEntry, error) {
	rows, err := r.q(ctx).Query(ctx,
		selectBalanceEntrySQL+` WHERE account_id = $1 AND currency = $2 ORDER BY created_at ASC`,
		string(accountID), string(currency),
	)
	if err != nil {
		return nil, fmt.Errorf("find balance by account: %w", err)
	}
	defer rows.Close()
	return scanBalanceEntries(rows)
}

const selectBalanceEntrySQL = `
	SELECT id, account_id, original_amount, remaining_amount, currency,
	       reason, source_type, source_id, description, expires_at, version, created_at
	FROM balance_entries`

func scanBalanceEntries(rows pgx.Rows) ([]*balance.BalanceEntry, error) {
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
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return result, nil
}

func scanBalanceEntrySnapshot(t scanTarget) (balance.BalanceEntrySnapshot, error) {
	var (
		s                                  balance.BalanceEntrySnapshot
		id, accountID                      string
		original, remaining                int64
		currency, reason, sourceType       string
		sourceID, description              string
		expiresAt                          *time.Time
		version                            int
		createdAt                          time.Time
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
	s.ExpiresAt = expiresAt
	s.CreatedAt = createdAt
	s.Version = version
	return s, nil
}
