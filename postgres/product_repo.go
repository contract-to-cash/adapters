package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/contract-to-cash/core/domain/product"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresProductRepository implements product.Repository.
// Master data, read-heavy.
type PostgresProductRepository struct {
	pool *pgxpool.Pool
}

var _ product.Repository = (*PostgresProductRepository)(nil)

func NewProductRepository(pool *pgxpool.Pool) *PostgresProductRepository {
	return &PostgresProductRepository{pool: pool}
}

func (r *PostgresProductRepository) q(ctx context.Context) Querier {
	return QuerierFromContext(ctx, r.pool)
}

func (r *PostgresProductRepository) FindByID(ctx context.Context, id shared.ProductID) (*product.Product, error) {
	q := r.q(ctx)

	var data json.RawMessage
	err := q.QueryRow(ctx, `SELECT data FROM products WHERE id = $1`, string(id)).Scan(&data)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, product.ErrNotFound
		}
		return nil, fmt.Errorf("find product: %w", err)
	}

	return product.Unmarshal(data)
}

func (r *PostgresProductRepository) Save(ctx context.Context, p *product.Product) error {
	q := r.q(ctx)

	data, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal product: %w", err)
	}

	_, err = q.Exec(ctx,
		`INSERT INTO products (id, name, status, data)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (id) DO UPDATE SET
		   name       = EXCLUDED.name,
		   status     = EXCLUDED.status,
		   data       = EXCLUDED.data,
		   updated_at = NOW()`,
		string(p.ID()), p.Name(), string(p.Status()), data,
	)
	if err != nil {
		return fmt.Errorf("save product: %w", err)
	}
	return nil
}
