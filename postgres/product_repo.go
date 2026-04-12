package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/domain/product"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
	var (
		name, description, status string
		features, usageMetrics    json.RawMessage
		metadata                  json.RawMessage
		createdAt                 time.Time
	)
	err := r.q(ctx).QueryRow(ctx,
		`SELECT name, description, status, features, usage_metrics, metadata, created_at
		 FROM products WHERE id = $1`, string(id),
	).Scan(&name, &description, &status, &features, &usageMetrics, &metadata, &createdAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, shared.NewDomainError(shared.ErrCodeNotFound, fmt.Sprintf("product %s not found", id))
		}
		return nil, fmt.Errorf("find product: %w", err)
	}

	s := product.ProductSnapshot{
		ID: id, Name: name, Description: description,
		Status: product.ProductStatus(status), CreatedAt: createdAt,
	}
	if len(features) > 0 {
		_ = json.Unmarshal(features, &s.Features)
	}
	if len(usageMetrics) > 0 {
		_ = json.Unmarshal(usageMetrics, &s.UsageMetrics)
	}
	if len(metadata) > 0 {
		_ = json.Unmarshal(metadata, &s.Metadata)
	}
	return product.FromSnapshot(s)
}

func (r *PostgresProductRepository) Save(ctx context.Context, p *product.Product) error {
	s := p.ToSnapshot()
	features, _ := json.Marshal(s.Features)
	usageMetrics, _ := json.Marshal(s.UsageMetrics)
	metadata, _ := json.Marshal(s.Metadata)

	_, err := r.q(ctx).Exec(ctx,
		`INSERT INTO products (id, name, description, status, features, usage_metrics, metadata, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
		 ON CONFLICT (id) DO UPDATE SET
		   name = EXCLUDED.name, description = EXCLUDED.description, status = EXCLUDED.status,
		   features = EXCLUDED.features, usage_metrics = EXCLUDED.usage_metrics,
		   metadata = EXCLUDED.metadata, updated_at = NOW()`,
		string(s.ID), s.Name, s.Description, string(s.Status),
		features, usageMetrics, metadata, s.CreatedAt)
	if err != nil {
		return fmt.Errorf("save product: %w", err)
	}
	return nil
}
