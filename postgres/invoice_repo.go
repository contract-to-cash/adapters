package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/domain/invoice"
	"github.com/contract-to-cash/core/domain/shared"
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
		   data       = EXCLUDED.data,
		   version    = invoices.version + 1,
		   updated_at = NOW()`,
		string(inv.ID()), string(inv.ContractID()), string(inv.AccountID()),
		string(inv.Status()), inv.Amount(), string(inv.Currency()),
		inv.DueDate(), inv.IssuedAt(), data,
	)
	if err != nil {
		return fmt.Errorf("save invoice: %w", err)
	}

	// Maintain history for temporal queries (FindByIDAsOf)
	_, err = q.Exec(ctx,
		`UPDATE invoice_history SET valid_to = NOW() WHERE id = $1 AND valid_to IS NULL`,
		string(inv.ID()),
	)
	if err != nil {
		return fmt.Errorf("close invoice history: %w", err)
	}
	_, err = q.Exec(ctx,
		`INSERT INTO invoice_history (id, data, version, valid_from)
		 SELECT id, data, version, NOW() FROM invoices WHERE id = $1`,
		string(inv.ID()),
	)
	if err != nil {
		return fmt.Errorf("insert invoice history: %w", err)
	}

	return nil
}

func (r *PostgresInvoiceRepository) FindByID(ctx context.Context, id shared.InvoiceID) (*invoice.Invoice, error) {
	q := r.q(ctx)

	var data json.RawMessage
	err := q.QueryRow(ctx,
		`SELECT data FROM invoices WHERE id = $1`, string(id),
	).Scan(&data)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, invoice.ErrNotFound
		}
		return nil, fmt.Errorf("find invoice: %w", err)
	}

	return invoice.Unmarshal(data)
}

func (r *PostgresInvoiceRepository) FindByContractID(ctx context.Context, contractID shared.ContractID) ([]*invoice.Invoice, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM invoices WHERE contract_id = $1 ORDER BY created_at DESC`,
		string(contractID),
	)
	if err != nil {
		return nil, fmt.Errorf("find invoices by contract: %w", err)
	}
	defer rows.Close()

	return scanInvoices(rows)
}

func (r *PostgresInvoiceRepository) FindByAccountID(ctx context.Context, accountID shared.AccountID) ([]*invoice.Invoice, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM invoices WHERE account_id = $1 ORDER BY created_at DESC`,
		string(accountID),
	)
	if err != nil {
		return nil, fmt.Errorf("find invoices by account: %w", err)
	}
	defer rows.Close()

	return scanInvoices(rows)
}

func (r *PostgresInvoiceRepository) FindOverdue(ctx context.Context) ([]*invoice.Invoice, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM invoices
		 WHERE status = 'issued' AND due_date < NOW()
		 ORDER BY due_date ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("find overdue invoices: %w", err)
	}
	defer rows.Close()

	return scanInvoices(rows)
}

func (r *PostgresInvoiceRepository) FindByStatus(ctx context.Context, status invoice.InvoiceStatus) ([]*invoice.Invoice, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM invoices WHERE status = $1 ORDER BY created_at DESC`,
		string(status),
	)
	if err != nil {
		return nil, fmt.Errorf("find invoices by status: %w", err)
	}
	defer rows.Close()

	return scanInvoices(rows)
}

// FindByIDAsOf performs a temporal query — returns the invoice state as of the given time.
func (r *PostgresInvoiceRepository) FindByIDAsOf(ctx context.Context, id shared.InvoiceID, asOf time.Time) (*invoice.Invoice, error) {
	q := r.q(ctx)

	var data json.RawMessage
	err := q.QueryRow(ctx,
		`SELECT data FROM invoice_history
		 WHERE id = $1 AND valid_from <= $2 AND (valid_to IS NULL OR valid_to > $2)
		 ORDER BY version DESC
		 LIMIT 1`,
		string(id), asOf,
	).Scan(&data)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, invoice.ErrNotFound
		}
		return nil, fmt.Errorf("find invoice as of: %w", err)
	}

	return invoice.Unmarshal(data)
}

func (r *PostgresInvoiceRepository) FindByContractAndStatus(ctx context.Context, contractID shared.ContractID, status invoice.InvoiceStatus) ([]*invoice.Invoice, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM invoices
		 WHERE contract_id = $1 AND status = $2
		 ORDER BY created_at DESC`,
		string(contractID), string(status),
	)
	if err != nil {
		return nil, fmt.Errorf("find invoices by contract and status: %w", err)
	}
	defer rows.Close()

	return scanInvoices(rows)
}

func (r *PostgresInvoiceRepository) FindByContractAndPeriod(ctx context.Context, contractID shared.ContractID, period shared.DateRange) ([]*invoice.Invoice, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM invoices
		 WHERE contract_id = $1 AND issued_at >= $2 AND issued_at < $3
		 ORDER BY issued_at ASC`,
		string(contractID), period.From, period.To,
	)
	if err != nil {
		return nil, fmt.Errorf("find invoices by contract and period: %w", err)
	}
	defer rows.Close()

	return scanInvoices(rows)
}

func (r *PostgresInvoiceRepository) FindUnpaidByContract(ctx context.Context, contractID shared.ContractID) ([]*invoice.Invoice, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM invoices
		 WHERE contract_id = $1 AND status NOT IN ('paid', 'voided', 'cancelled')
		 ORDER BY due_date ASC`,
		string(contractID),
	)
	if err != nil {
		return nil, fmt.Errorf("find unpaid invoices by contract: %w", err)
	}
	defer rows.Close()

	return scanInvoices(rows)
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
