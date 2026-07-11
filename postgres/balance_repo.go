package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/application/tx"
	"github.com/contract-to-cash/core/domain/balance"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// balanceEntryJSONState is the precise (big.Rat-backed) representation of a
// BalanceEntry's monetary fields. It is the source of truth on read; the
// original_amount / remaining_amount BIGINT columns are written for
// query/indexing convenience only and are lossy (Money.Int64 truncates any
// fractional part). See issue #11.
type balanceEntryJSONState struct {
	OriginalAmount  shared.Money `json:"original_amount"`
	RemainingAmount shared.Money `json:"remaining_amount"`
}

type PostgresBalanceRepository struct {
	pool *pgxpool.Pool
}

var _ balance.Repository = (*PostgresBalanceRepository)(nil)

func NewBalanceRepository(pool *pgxpool.Pool) *PostgresBalanceRepository {
	return &PostgresBalanceRepository{pool: pool}
}

func (r *PostgresBalanceRepository) q(ctx context.Context) Querier {
	return QuerierFromContext(ctx, r.pool)
}

func (r *PostgresBalanceRepository) Save(ctx context.Context, entry *balance.BalanceEntry) error {
	s := entry.ToSnapshot()
	loaded := entry.LoadedVersion()
	q := r.q(ctx)

	jsonState, err := json.Marshal(balanceEntryJSONState{
		OriginalAmount:  s.OriginalAmount,
		RemainingAmount: s.RemainingAmount,
	})
	if err != nil {
		return fmt.Errorf("marshal balance entry json state: %w", err)
	}

	if loaded == 0 {
		_, err := q.Exec(ctx,
			`INSERT INTO balance_entries (id, account_id, original_amount, remaining_amount, currency,
				reason, source_type, source_id, description, expires_at, version, created_at, state)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
			string(s.ID), string(s.AccountID),
			s.OriginalAmount.Int64(), s.RemainingAmount.Int64(),
			string(s.OriginalAmount.Currency()),
			string(s.Reason), string(s.SourceType), s.SourceID, s.Description,
			s.ExpiresAt, s.Version, s.CreatedAt, jsonState)
		if err != nil {
			return fmt.Errorf("insert balance entry: %w", err)
		}
		entry.SetVersion(s.Version)
		return nil
	}

	tag, err := q.Exec(ctx,
		`UPDATE balance_entries SET
			original_amount = $2, remaining_amount = $3, currency = $4,
			reason = $5, source_type = $6, source_id = $7,
			description = $8, expires_at = $9, version = $10, state = $11
		 WHERE id = $1 AND version = $12`,
		string(s.ID),
		s.OriginalAmount.Int64(), s.RemainingAmount.Int64(),
		string(s.OriginalAmount.Currency()),
		string(s.Reason), string(s.SourceType), s.SourceID, s.Description,
		s.ExpiresAt, s.Version, jsonState, loaded)
	if err != nil {
		return fmt.Errorf("update balance entry: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return tx.ErrVersionConflict
	}
	entry.SetVersion(s.Version)
	return nil
}

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

func (r *PostgresBalanceRepository) FindAvailable(ctx context.Context, accountID shared.AccountID, currency shared.Currency) ([]*balance.BalanceEntry, error) {
	// Availability is decided on the precise remaining amount (from the state
	// JSON), not the lossy BIGINT column: a sub-unit remainder (e.g. 0.75)
	// must still be considered available. The BIGINT remaining_amount is
	// write-only and no longer filtered on here. See issue #11.
	rows, err := r.q(ctx).Query(ctx,
		selectBalanceEntrySQL+`
		 WHERE account_id = $1 AND currency = $2
		   AND (expires_at IS NULL OR expires_at > NOW())
		 ORDER BY created_at ASC`,
		string(accountID), string(currency))
	if err != nil {
		return nil, fmt.Errorf("find available balance: %w", err)
	}
	defer rows.Close()
	entries, err := scanBalanceEntries(rows)
	if err != nil {
		return nil, err
	}
	return filterAvailableBalance(entries), nil
}

func (r *PostgresBalanceRepository) GetBalance(ctx context.Context, accountID shared.AccountID, currency shared.Currency) (shared.Money, error) {
	// Sum the precise remaining amounts (state JSON) rather than the lossy
	// BIGINT column, so fractional credits are not silently truncated. See
	// issue #11.
	rows, err := r.q(ctx).Query(ctx,
		selectBalanceEntrySQL+`
		 WHERE account_id = $1 AND currency = $2
		   AND (expires_at IS NULL OR expires_at > NOW())`,
		string(accountID), string(currency))
	if err != nil {
		return shared.Money{}, fmt.Errorf("get balance: %w", err)
	}
	defer rows.Close()
	entries, err := scanBalanceEntries(rows)
	if err != nil {
		return shared.Money{}, err
	}
	return sumRemaining(entries, currency)
}

func (r *PostgresBalanceRepository) SaveApplication(ctx context.Context, app *balance.BalanceApplication) error {
	_, err := r.q(ctx).Exec(ctx,
		`INSERT INTO balance_applications (id, balance_entry_id, invoice_id, amount, currency, applied_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		app.ID, string(app.BalanceEntryID), string(app.InvoiceID),
		app.Amount.Int64(), string(app.Amount.Currency()), app.AppliedAt)
	if err != nil {
		return fmt.Errorf("save balance application: %w", err)
	}
	return nil
}

func (r *PostgresBalanceRepository) FindApplicationsByInvoice(ctx context.Context, invoiceID shared.InvoiceID) ([]*balance.BalanceApplication, error) {
	rows, err := r.q(ctx).Query(ctx,
		`SELECT id, balance_entry_id, invoice_id, amount, currency, applied_at
		 FROM balance_applications WHERE invoice_id = $1 ORDER BY applied_at ASC`,
		string(invoiceID))
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
			ID: id, BalanceEntryID: shared.BalanceEntryID(entryID),
			InvoiceID: shared.InvoiceID(invID),
			Amount:    moneyFromInt64(amount, shared.Currency(currency)),
			AppliedAt: appliedAt,
		})
	}
	return result, rows.Err()
}

func (r *PostgresBalanceRepository) SaveRefund(ctx context.Context, refund *balance.BalanceRefund) error {
	// invoice_id and application_id (core#184) make void-triggered restoration
	// idempotent: FindRefundsByInvoice reads them back so a double void / retry
	// skips applications whose credit was already restored. Both are empty for
	// refunds not tied to an invoice/application.
	_, err := r.q(ctx).Exec(ctx,
		`INSERT INTO balance_refunds (id, balance_entry_id, account_id, amount, currency, refunded_at, invoice_id, application_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		refund.ID, string(refund.BalanceEntryID), string(refund.AccountID),
		refund.Amount.Int64(), string(refund.Amount.Currency()), refund.RefundedAt,
		string(refund.InvoiceID), refund.ApplicationID)
	if err != nil {
		return fmt.Errorf("save balance refund: %w", err)
	}
	return nil
}

// FindRefundsByInvoice returns all credit refunds recorded against an invoice
// (core#184). The void-restoration flow uses it to skip applications whose
// consumed credit was already restored, making a double void / transaction
// retry idempotent. Ordering is unspecified by the contract; refunded_at ASC is
// used for a deterministic result.
func (r *PostgresBalanceRepository) FindRefundsByInvoice(ctx context.Context, invoiceID shared.InvoiceID) ([]*balance.BalanceRefund, error) {
	rows, err := r.q(ctx).Query(ctx,
		`SELECT id, balance_entry_id, account_id, amount, currency, refunded_at, invoice_id, application_id
		 FROM balance_refunds WHERE invoice_id = $1 ORDER BY refunded_at ASC`,
		string(invoiceID))
	if err != nil {
		return nil, fmt.Errorf("find balance refunds by invoice: %w", err)
	}
	defer rows.Close()

	var result []*balance.BalanceRefund
	for rows.Next() {
		var (
			id, entryID, accountID string
			amount                 int64
			currency               string
			refundedAt             time.Time
			invID, applicationID   string
		)
		if err := rows.Scan(&id, &entryID, &accountID, &amount, &currency, &refundedAt, &invID, &applicationID); err != nil {
			return nil, fmt.Errorf("scan balance refund: %w", err)
		}
		result = append(result, &balance.BalanceRefund{
			ID: id, BalanceEntryID: shared.BalanceEntryID(entryID),
			AccountID:     shared.AccountID(accountID),
			Amount:        moneyFromInt64(amount, shared.Currency(currency)),
			RefundedAt:    refundedAt,
			InvoiceID:     shared.InvoiceID(invID),
			ApplicationID: applicationID,
		})
	}
	return result, rows.Err()
}

// FindExpired returns entries whose expiry has passed as of asOf and whose
// remaining amount is still non-zero (expired credit not yet forfeited by
// MarkExpired), ordered by creation time ascending. It feeds the core
// batch.BalanceExpirationProcessor (issue #159 / adapters#46) and mirrors the
// in-memory reference (infrastructure/inmemory/balance_repository.go) exactly:
//
//   - Expired is BalanceEntry.IsExpired(asOf), i.e. asOf strictly after
//     expires_at. `expires_at < $1` encodes exactly that (asOf > expires_at);
//     rows without an expiry (expires_at IS NULL) are excluded.
//   - Fully-consumed entries are dropped in Go on the PRECISE remaining amount
//     from the state JSON, not the lossy BIGINT column, so a sub-unit remainder
//     still counts as forfeitable credit (same #11 filtering as FindAvailable).
//   - Scanned across all accounts/currencies (the batch is global), ordered by
//     created_at ASC for deterministic processing.
func (r *PostgresBalanceRepository) FindExpired(ctx context.Context, asOf time.Time) ([]*balance.BalanceEntry, error) {
	rows, err := r.q(ctx).Query(ctx,
		selectBalanceEntrySQL+`
		 WHERE expires_at IS NOT NULL AND expires_at < $1
		 ORDER BY created_at ASC`, asOf)
	if err != nil {
		return nil, fmt.Errorf("find expired balance: %w", err)
	}
	defer rows.Close()
	entries, err := scanBalanceEntries(rows)
	if err != nil {
		return nil, err
	}
	return filterExpiredBalance(entries), nil
}

func (r *PostgresBalanceRepository) FindByAccountID(ctx context.Context, accountID shared.AccountID, currency shared.Currency) ([]*balance.BalanceEntry, error) {
	rows, err := r.q(ctx).Query(ctx,
		selectBalanceEntrySQL+` WHERE account_id = $1 AND currency = $2 ORDER BY created_at ASC`,
		string(accountID), string(currency))
	if err != nil {
		return nil, fmt.Errorf("find balance by account: %w", err)
	}
	defer rows.Close()
	return scanBalanceEntries(rows)
}

const selectBalanceEntrySQL = `
	SELECT id, account_id, original_amount, remaining_amount, currency,
	       reason, source_type, source_id, description, expires_at, version, created_at, state
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
	return result, rows.Err()
}

func scanBalanceEntrySnapshot(t scanTarget) (balance.BalanceEntrySnapshot, error) {
	var (
		s                            balance.BalanceEntrySnapshot
		id, accountID                string
		original, remaining          int64
		currency, reason, sourceType string
		sourceID, description        string
		expiresAt                    *time.Time
		version                      int
		createdAt                    time.Time
		stateRaw                     []byte
	)
	if err := t.Scan(
		&id, &accountID, &original, &remaining, &currency,
		&reason, &sourceType, &sourceID, &description, &expiresAt, &version, &createdAt, &stateRaw,
	); err != nil {
		return balance.BalanceEntrySnapshot{}, err
	}

	cur := shared.Currency(currency)
	s.ID = shared.BalanceEntryID(id)
	s.AccountID = shared.AccountID(accountID)
	// Prefer the precise state JSON; fall back to the lossy BIGINT columns for
	// rows written before the state column existed (issue #11).
	if len(stateRaw) > 0 {
		var js balanceEntryJSONState
		if err := json.Unmarshal(stateRaw, &js); err != nil {
			return balance.BalanceEntrySnapshot{}, fmt.Errorf("unmarshal balance entry json state: %w", err)
		}
		s.OriginalAmount = js.OriginalAmount
		s.RemainingAmount = js.RemainingAmount
	} else {
		s.OriginalAmount = moneyFromInt64(original, cur)
		s.RemainingAmount = moneyFromInt64(remaining, cur)
	}
	s.Reason = balance.BalanceReason(reason)
	s.SourceType = balance.BalanceSourceType(sourceType)
	s.SourceID = sourceID
	s.Description = description
	s.ExpiresAt = expiresAt
	s.CreatedAt = createdAt
	s.Version = version
	return s, nil
}
