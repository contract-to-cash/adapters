package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/domain/invoice"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresInvoiceRepository implements invoice.Repository (10 methods).
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

func (r *PostgresInvoiceRepository) Save(ctx context.Context, inv *invoice.Invoice) error {
	q := r.q(ctx)

	data, err := json.Marshal(inv)
	if err != nil {
		return fmt.Errorf("marshal invoice: %w", err)
	}

	_, err = q.Exec(ctx,
		`INSERT INTO invoices (id, contract_id, account_id, status, amount, currency, due_date, issued_at, data, version)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 1)
		 ON CONFLICT (id) DO UPDATE SET
		   status     = EXCLUDED.status,
		   amount     = EXCLUDED.amount,
		   due_date   = EXCLUDED.due_date,
		   issued_at  = EXCLUDED.issued_at,
		   paid_at    = EXCLUDED.paid_at,
		   data       = EXCLUDED.data,
		   version    = invoices.version + 1,
		   updated_at = NOW()`,
		inv.ID(), inv.ContractID(), inv.AccountID(), inv.Status(),
		inv.Amount(), inv.Currency(), inv.DueDate(), inv.IssuedAt(), data,
	)
	if err != nil {
		return fmt.Errorf("save invoice: %w", err)
	}

	// Record history for temporal queries
	_, err = q.Exec(ctx,
		`UPDATE invoice_history SET valid_to = NOW() WHERE id = $1 AND valid_to IS NULL`,
		inv.ID(),
	)
	if err != nil {
		return fmt.Errorf("close invoice history: %w", err)
	}

	_, err = q.Exec(ctx,
		`INSERT INTO invoice_history (id, data, version, valid_from)
		 SELECT id, data, version, NOW() FROM invoices WHERE id = $1`,
		inv.ID(),
	)
	if err != nil {
		return fmt.Errorf("insert invoice history: %w", err)
	}

	return nil
}

func (r *PostgresInvoiceRepository) FindByID(ctx context.Context, id string) (*invoice.Invoice, error) {
	q := r.q(ctx)

	var data json.RawMessage
	err := q.QueryRow(ctx,
		`SELECT data FROM invoices WHERE id = $1`,
		id,
	).Scan(&data)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, invoice.ErrNotFound
		}
		return nil, fmt.Errorf("find invoice: %w", err)
	}

	return invoice.Unmarshal(data)
}

// FindByIDAsOf performs a temporal query — returns the invoice state as of the given time.
func (r *PostgresInvoiceRepository) FindByIDAsOf(ctx context.Context, id string, asOf time.Time) (*invoice.Invoice, error) {
	q := r.q(ctx)

	var data json.RawMessage
	err := q.QueryRow(ctx,
		`SELECT data FROM invoice_history
		 WHERE id = $1 AND valid_from <= $2 AND (valid_to IS NULL OR valid_to > $2)
		 ORDER BY version DESC
		 LIMIT 1`,
		id, asOf,
	).Scan(&data)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, invoice.ErrNotFound
		}
		return nil, fmt.Errorf("find invoice as of: %w", err)
	}

	return invoice.Unmarshal(data)
}

func (r *PostgresInvoiceRepository) FindByContractID(ctx context.Context, contractID string) ([]*invoice.Invoice, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM invoices WHERE contract_id = $1 ORDER BY created_at DESC`,
		contractID,
	)
	if err != nil {
		return nil, fmt.Errorf("find invoices by contract: %w", err)
	}
	defer rows.Close()

	return scanInvoices(rows)
}

func (r *PostgresInvoiceRepository) FindByAccountID(ctx context.Context, accountID string) ([]*invoice.Invoice, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM invoices WHERE account_id = $1 ORDER BY created_at DESC`,
		accountID,
	)
	if err != nil {
		return nil, fmt.Errorf("find invoices by account: %w", err)
	}
	defer rows.Close()

	return scanInvoices(rows)
}

func (r *PostgresInvoiceRepository) FindPending(ctx context.Context) ([]*invoice.Invoice, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM invoices WHERE status = 'pending' ORDER BY due_date ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("find pending invoices: %w", err)
	}
	defer rows.Close()

	return scanInvoices(rows)
}

func (r *PostgresInvoiceRepository) FindOverdue(ctx context.Context, asOf time.Time) ([]*invoice.Invoice, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM invoices
		 WHERE status = 'issued' AND due_date < $1
		 ORDER BY due_date ASC`,
		asOf,
	)
	if err != nil {
		return nil, fmt.Errorf("find overdue invoices: %w", err)
	}
	defer rows.Close()

	return scanInvoices(rows)
}

func (r *PostgresInvoiceRepository) FindByStatus(ctx context.Context, status string) ([]*invoice.Invoice, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM invoices WHERE status = $1 ORDER BY created_at DESC`,
		status,
	)
	if err != nil {
		return nil, fmt.Errorf("find invoices by status: %w", err)
	}
	defer rows.Close()

	return scanInvoices(rows)
}

func (r *PostgresInvoiceRepository) Update(ctx context.Context, inv *invoice.Invoice) error {
	return r.Save(ctx, inv)
}

func (r *PostgresInvoiceRepository) Delete(ctx context.Context, id string) error {
	q := r.q(ctx)

	tag, err := q.Exec(ctx, `DELETE FROM invoices WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete invoice: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return invoice.ErrNotFound
	}
	return nil
}

func scanInvoices(rows pgx.Rows) ([]*invoice.Invoice, error) {
	var invoices []*invoice.Invoice
	for rows.Next() {
		var data json.RawMessage
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("scan invoice: %w", err)
		}
		inv, err := invoice.Unmarshal(data)
		if err != nil {
			return nil, fmt.Errorf("unmarshal invoice: %w", err)
		}
		invoices = append(invoices, inv)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return invoices, nil
}
