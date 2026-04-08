package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/contract-to-cash/core/domain/balance"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresBalanceRepository implements balance.Repository.
// Manages BalanceEntry, BalanceApplication, and BalanceRefund entities.
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
	q := r.q(ctx)

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal balance entry: %w", err)
	}

	_, err = q.Exec(ctx,
		`INSERT INTO balance_entries (id, account_id, amount, currency, data)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (id) DO UPDATE SET
		   amount     = EXCLUDED.amount,
		   data       = EXCLUDED.data,
		   updated_at = NOW()`,
		string(entry.ID()), string(entry.AccountID()),
		entry.Amount(), string(entry.Currency()), data,
	)
	if err != nil {
		return fmt.Errorf("save balance entry: %w", err)
	}
	return nil
}

func (r *PostgresBalanceRepository) FindByID(ctx context.Context, id shared.BalanceEntryID) (*balance.BalanceEntry, error) {
	q := r.q(ctx)

	var data json.RawMessage
	err := q.QueryRow(ctx,
		`SELECT data FROM balance_entries WHERE id = $1`, string(id),
	).Scan(&data)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, balance.ErrNotFound
		}
		return nil, fmt.Errorf("find balance entry: %w", err)
	}

	return balance.UnmarshalEntry(data)
}

// FindAvailable returns balance entries with remaining available balance for the account+currency.
func (r *PostgresBalanceRepository) FindAvailable(ctx context.Context, accountID shared.AccountID, currency shared.Currency) ([]*balance.BalanceEntry, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT be.data FROM balance_entries be
		 WHERE be.account_id = $1 AND be.currency = $2
		   AND be.amount > COALESCE(
		     (SELECT SUM(ba.amount) FROM balance_applications ba WHERE ba.balance_entry_id = be.id), 0
		   )
		 ORDER BY be.created_at ASC`,
		string(accountID), string(currency),
	)
	if err != nil {
		return nil, fmt.Errorf("find available balance entries: %w", err)
	}
	defer rows.Close()

	return scanBalanceEntries(rows)
}

// GetBalance returns the net balance (entries - applications + refunds) for the account+currency.
func (r *PostgresBalanceRepository) GetBalance(ctx context.Context, accountID shared.AccountID, currency shared.Currency) (shared.Money, error) {
	q := r.q(ctx)

	var totalEntries, totalApplied int64
	err := q.QueryRow(ctx,
		`SELECT
		   COALESCE(SUM(be.amount), 0),
		   COALESCE((SELECT SUM(ba.amount) FROM balance_applications ba
		             JOIN balance_entries be2 ON ba.balance_entry_id = be2.id
		             WHERE be2.account_id = $1 AND be2.currency = $2), 0)
		 FROM balance_entries be
		 WHERE be.account_id = $1 AND be.currency = $2`,
		string(accountID), string(currency),
	).Scan(&totalEntries, &totalApplied)
	if err != nil {
		return shared.Money{}, fmt.Errorf("get balance: %w", err)
	}

	return shared.NewMoney(totalEntries-totalApplied, currency), nil
}

func (r *PostgresBalanceRepository) SaveApplication(ctx context.Context, app *balance.BalanceApplication) error {
	q := r.q(ctx)

	data, err := json.Marshal(app)
	if err != nil {
		return fmt.Errorf("marshal balance application: %w", err)
	}

	_, err = q.Exec(ctx,
		`INSERT INTO balance_applications (id, balance_entry_id, invoice_id, amount, data)
		 VALUES ($1, $2, $3, $4, $5)`,
		string(app.ID()), string(app.BalanceEntryID()), string(app.InvoiceID()),
		app.Amount(), data,
	)
	if err != nil {
		return fmt.Errorf("save balance application: %w", err)
	}
	return nil
}

func (r *PostgresBalanceRepository) FindApplicationsByInvoice(ctx context.Context, invoiceID shared.InvoiceID) ([]*balance.BalanceApplication, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM balance_applications WHERE invoice_id = $1 ORDER BY created_at ASC`,
		string(invoiceID),
	)
	if err != nil {
		return nil, fmt.Errorf("find applications by invoice: %w", err)
	}
	defer rows.Close()

	var apps []*balance.BalanceApplication
	for rows.Next() {
		var data json.RawMessage
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("scan balance application: %w", err)
		}
		app, err := balance.UnmarshalApplication(data)
		if err != nil {
			return nil, fmt.Errorf("unmarshal balance application: %w", err)
		}
		apps = append(apps, app)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return apps, nil
}

func (r *PostgresBalanceRepository) SaveRefund(ctx context.Context, refund *balance.BalanceRefund) error {
	q := r.q(ctx)

	data, err := json.Marshal(refund)
	if err != nil {
		return fmt.Errorf("marshal balance refund: %w", err)
	}

	_, err = q.Exec(ctx,
		`INSERT INTO balance_refunds (id, balance_entry_id, amount, data)
		 VALUES ($1, $2, $3, $4)`,
		string(refund.ID()), string(refund.BalanceEntryID()), refund.Amount(), data,
	)
	if err != nil {
		return fmt.Errorf("save balance refund: %w", err)
	}
	return nil
}

func (r *PostgresBalanceRepository) FindByAccountID(ctx context.Context, accountID shared.AccountID, currency shared.Currency) ([]*balance.BalanceEntry, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM balance_entries
		 WHERE account_id = $1 AND currency = $2
		 ORDER BY created_at ASC`,
		string(accountID), string(currency),
	)
	if err != nil {
		return nil, fmt.Errorf("find balance entries by account: %w", err)
	}
	defer rows.Close()

	return scanBalanceEntries(rows)
}

func scanBalanceEntries(rows pgx.Rows) ([]*balance.BalanceEntry, error) {
	var entries []*balance.BalanceEntry
	for rows.Next() {
		var data json.RawMessage
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("scan balance entry: %w", err)
		}
		entry, err := balance.UnmarshalEntry(data)
		if err != nil {
			return nil, fmt.Errorf("unmarshal balance entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return entries, nil
}
