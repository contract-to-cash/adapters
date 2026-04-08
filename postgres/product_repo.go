package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/contract-to-cash/core/domain/product"
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
		p.ID(), p.Name(), p.Status(), data,
	)
	if err != nil {
		return fmt.Errorf("save product: %w", err)
	}
	return nil
}

func (r *PostgresProductRepository) FindByID(ctx context.Context, id string) (*product.Product, error) {
	q := r.q(ctx)

	var data json.RawMessage
	err := q.QueryRow(ctx, `SELECT data FROM products WHERE id = $1`, id).Scan(&data)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, product.ErrNotFound
		}
		return nil, fmt.Errorf("find product: %w", err)
	}

	return product.Unmarshal(data)
}

func (r *PostgresProductRepository) FindAll(ctx context.Context) ([]*product.Product, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx, `SELECT data FROM products ORDER BY name ASC`)
	if err != nil {
		return nil, fmt.Errorf("find all products: %w", err)
	}
	defer rows.Close()

	return scanProducts(rows)
}

func (r *PostgresProductRepository) FindActive(ctx context.Context) ([]*product.Product, error) {
	q := r.q(ctx)

	rows, err := q.Query(ctx,
		`SELECT data FROM products WHERE status = 'active' ORDER BY name ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("find active products: %w", err)
	}
	defer rows.Close()

	return scanProducts(rows)
}

func scanProducts(rows pgx.Rows) ([]*product.Product, error) {
	var products []*product.Product
	for rows.Next() {
		var data json.RawMessage
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("scan product: %w", err)
		}
		p, err := product.Unmarshal(data)
		if err != nil {
			return nil, fmt.Errorf("unmarshal product: %w", err)
		}
		products = append(products, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return products, nil
}
