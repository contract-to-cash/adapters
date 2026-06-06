package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/contract-to-cash/core/domain/product"
	"github.com/contract-to-cash/core/domain/shared"
)

// MySQLProductRepository is a MySQL-backed product.Repository.
type MySQLProductRepository struct {
	db *sql.DB
}

var _ product.Repository = (*MySQLProductRepository)(nil)

// NewProductRepository constructs a product repository over an existing *sql.DB.
func NewProductRepository(db *sql.DB) *MySQLProductRepository {
	return &MySQLProductRepository{db: db}
}

func (r *MySQLProductRepository) q(ctx context.Context) Querier {
	return querierFromContext(ctx, r.db)
}

func (r *MySQLProductRepository) FindByID(ctx context.Context, id shared.ProductID) (*product.Product, error) {
	var (
		name, description, status string
		features, usageMetrics    json.RawMessage
		metadata                  json.RawMessage
		createdAt                 utcTime
	)
	err := r.q(ctx).QueryRowContext(ctx,
		`SELECT name, description, status, features, usage_metrics, metadata, created_at
		 FROM products WHERE id = ?`, string(id),
	).Scan(&name, &description, &status, &features, &usageMetrics, &metadata, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, shared.NewDomainError(shared.ErrCodeNotFound, fmt.Sprintf("product %s not found", id))
		}
		return nil, fmt.Errorf("find product: %w", err)
	}

	s := product.ProductSnapshot{
		ID: id, Name: name, Description: description,
		Status: product.ProductStatus(status), CreatedAt: createdAt.Time,
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

func (r *MySQLProductRepository) Save(ctx context.Context, p *product.Product) error {
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

	_, err = r.q(ctx).ExecContext(ctx,
		`INSERT INTO products (id, name, description, status, features, usage_metrics, metadata, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(6))
		 ON DUPLICATE KEY UPDATE
		   name = VALUES(name), description = VALUES(description), status = VALUES(status),
		   features = VALUES(features), usage_metrics = VALUES(usage_metrics),
		   metadata = VALUES(metadata), updated_at = NOW(6)`,
		string(s.ID), s.Name, s.Description, string(s.Status),
		features, usageMetrics, metadata, s.CreatedAt.UTC())
	if err != nil {
		return fmt.Errorf("save product: %w", err)
	}
	return nil
}
