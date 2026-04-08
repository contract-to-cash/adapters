package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/contract-to-cash/core/domain/payment"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresPaymentRepository implements payment.Repository.
type PostgresPaymentRepository struct {
	pool *pgxpool.Pool
}

var _ payment.Repository = (*PostgresPaymentRepository)(nil)

func NewPaymentRepository(pool *pgxpool.Pool) *PostgresPaymentRepository {
	return &PostgresPaymentRepository{pool: pool}
}

func (r *PostgresPaymentRepository) q(ctx context.Context) Querier {
	return QuerierFromContext(ctx, r.pool)
}

func (r *PostgresPaymentRepository) Save(ctx context.Context, p *payment.Payment) error {
	q := r.q(ctx)

	data, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal payment: %w", err)
	}

	_, err = q.Exec(ctx,
		`INSERT INTO payments (id, invoice_id, idempotency_key, amount, currency, status, method, data)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (id) DO UPDATE SET
		   status     = EXCLUDED.status,
		   data       = EXCLUDED.data,
		   updated_at = NOW()`,
		string(p.ID()), string(p.InvoiceID()), p.IdempotencyKey(),
		p.Amount(), string(p.Currency()), string(p.Status()), p.Method(), data,
	)
	if err != nil {
		return fmt.Errorf("save payment: %w", err)
	}
	return nil
}

func (r *PostgresPaymentRepository) FindByID(ctx context.Context, id shared.PaymentID) (*payment.Payment, error) {
	q := r.q(ctx)

	var data json.RawMessage
	err := q.QueryRow(ctx, `SELECT data FROM payments WHERE id = $1`, string(id)).Scan(&data)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, payment.ErrNotFound
		}
		return nil, fmt.Errorf("find payment: %w", err)
	}

	return payment.Unmarshal(data)
}

func (r *PostgresPaymentRepository) FindByInvoiceID(ctx context.Context, invoiceID shared.InvoiceID) ([]*payment.Payment, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM payments WHERE invoice_id = $1 ORDER BY created_at DESC`,
		string(invoiceID),
	)
	if err != nil {
		return nil, fmt.Errorf("find payments by invoice: %w", err)
	}
	defer rows.Close()

	return scanPayments(rows)
}

// FindByIdempotencyKey looks up a payment by its idempotency key for duplicate detection.
func (r *PostgresPaymentRepository) FindByIdempotencyKey(ctx context.Context, key string) (*payment.Payment, error) {
	q := r.q(ctx)

	var data json.RawMessage
	err := q.QueryRow(ctx,
		`SELECT data FROM payments WHERE idempotency_key = $1`, key,
	).Scan(&data)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("find payment by idempotency key: %w", err)
	}

	return payment.Unmarshal(data)
}

func scanPayments(rows pgx.Rows) ([]*payment.Payment, error) {
	var payments []*payment.Payment
	for rows.Next() {
		var data json.RawMessage
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("scan payment: %w", err)
		}
		p, err := payment.Unmarshal(data)
		if err != nil {
			return nil, fmt.Errorf("unmarshal payment: %w", err)
		}
		payments = append(payments, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return payments, nil
}
