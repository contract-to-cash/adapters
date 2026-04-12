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
		if err := json.Unmarshal(features, &s.Features); err != nil {
			return nil, fmt.Errorf("unmarshal product features: %w", err)
		}
	}
	if len(usageMetrics) > 0 {
		if err := json.Unmarshal(usageMetrics, &s.UsageMetrics); err != nil {
			return nil, fmt.Errorf("unmarshal product usage metrics: %w", err)
		}
	}
	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &s.Metadata); err != nil {
			return nil, fmt.Errorf("unmarshal product metadata: %w", err)
		}
	}
	return product.FromSnapshot(s)
}

func (r *PostgresProductRepository) Save(ctx context.Context, p *product.Product) error {
	s := p.ToSnapshot()
	features, err := json.Marshal(s.Features)
	if err != nil {
		return fmt.Errorf("marshal features: %w", err)
	}
	usageMetrics, err := json.Marshal(s.UsageMetrics)
	if err != nil {
		return fmt.Errorf("marshal usage metrics: %w", err)
	}
	metadata, err := json.Marshal(s.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	_, err = r.q(ctx).Exec(ctx,
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
