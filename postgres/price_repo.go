package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/contract-to-cash/core/domain/pricing"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresPriceRepository implements pricing.PriceRepository.
// Master data, read-heavy.
type PostgresPriceRepository struct {
	pool *pgxpool.Pool
}

var _ pricing.PriceRepository = (*PostgresPriceRepository)(nil)

func NewPriceRepository(pool *pgxpool.Pool) *PostgresPriceRepository {
	return &PostgresPriceRepository{pool: pool}
}

func (r *PostgresPriceRepository) q(ctx context.Context) Querier {
	return QuerierFromContext(ctx, r.pool)
}

func (r *PostgresPriceRepository) Save(ctx context.Context, p *pricing.Price) error {
	q := r.q(ctx)

	data, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal price: %w", err)
	}

	_, err = q.Exec(ctx,
		`INSERT INTO prices (id, product_id, amount, currency, billing_period, status, data)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (id) DO UPDATE SET
		   amount         = EXCLUDED.amount,
		   billing_period = EXCLUDED.billing_period,
		   status         = EXCLUDED.status,
		   data           = EXCLUDED.data,
		   updated_at     = NOW()`,
		p.ID(), p.ProductID(), p.Amount(), p.Currency(), p.BillingPeriod(), p.Status(), data,
	)
	if err != nil {
		return fmt.Errorf("save price: %w", err)
	}
	return nil
}

func (r *PostgresPriceRepository) FindByID(ctx context.Context, id string) (*pricing.Price, error) {
	q := r.q(ctx)

	var data json.RawMessage
	err := q.QueryRow(ctx, `SELECT data FROM prices WHERE id = $1`, id).Scan(&data)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, pricing.ErrNotFound
		}
		return nil, fmt.Errorf("find price: %w", err)
	}

	return pricing.UnmarshalPrice(data)
}

func (r *PostgresPriceRepository) FindByProductID(ctx context.Context, productID string) ([]*pricing.Price, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM prices WHERE product_id = $1 ORDER BY created_at DESC`,
		productID,
	)
	if err != nil {
		return nil, fmt.Errorf("find prices by product: %w", err)
	}
	defer rows.Close()

	return scanPrices(rows)
}

func (r *PostgresPriceRepository) FindActive(ctx context.Context) ([]*pricing.Price, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM prices WHERE status = 'active' ORDER BY product_id, created_at DESC`,
	)
	if err != nil {
		return nil, fmt.Errorf("find active prices: %w", err)
	}
	defer rows.Close()

	return scanPrices(rows)
}

func (r *PostgresPriceRepository) FindActiveByProductID(ctx context.Context, productID string) ([]*pricing.Price, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM prices WHERE product_id = $1 AND status = 'active' ORDER BY created_at DESC`,
		productID,
	)
	if err != nil {
		return nil, fmt.Errorf("find active prices by product: %w", err)
	}
	defer rows.Close()

	return scanPrices(rows)
}

func scanPrices(rows pgx.Rows) ([]*pricing.Price, error) {
	var prices []*pricing.Price
	for rows.Next() {
		var data json.RawMessage
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("scan price: %w", err)
		}
		p, err := pricing.UnmarshalPrice(data)
		if err != nil {
			return nil, fmt.Errorf("unmarshal price: %w", err)
		}
		prices = append(prices, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return prices, nil
}
