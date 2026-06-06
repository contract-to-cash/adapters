package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/contract-to-cash/core/domain/pricing"
	"github.com/contract-to-cash/core/domain/shared"
)

// scanTarget abstracts *sql.Row and *sql.Rows so a single snapshot scanner
// serves both single-row and multi-row queries.
type scanTarget interface {
	Scan(dest ...any) error
}

// MySQLPriceRepository is a MySQL-backed pricing.PriceRepository.
type MySQLPriceRepository struct {
	db *sql.DB
}

var _ pricing.PriceRepository = (*MySQLPriceRepository)(nil)

// NewPriceRepository constructs a price repository over an existing *sql.DB.
func NewPriceRepository(db *sql.DB) *MySQLPriceRepository {
	return &MySQLPriceRepository{db: db}
}

func (r *MySQLPriceRepository) q(ctx context.Context) Querier {
	return querierFromContext(ctx, r.db)
}

type pricingModelKind string

const (
	kindFlat   pricingModelKind = "flat"
	kindTiered pricingModelKind = "tiered"
	kindUsage  pricingModelKind = "usage"
)

type pricingModelEnvelope struct {
	Kind   pricingModelKind     `json:"kind"`
	Flat   *pricing.FlatPrice   `json:"flat,omitempty"`
	Tiered *pricing.TieredPrice `json:"tiered,omitempty"`
	Usage  *pricing.UsagePrice  `json:"usage,omitempty"`
}

func marshalPricingModel(m pricing.PricingModel) ([]byte, error) {
	if m == nil {
		return json.Marshal(pricingModelEnvelope{})
	}
	switch v := m.(type) {
	case pricing.FlatPrice:
		return json.Marshal(pricingModelEnvelope{Kind: kindFlat, Flat: &v})
	case pricing.TieredPrice:
		return json.Marshal(pricingModelEnvelope{Kind: kindTiered, Tiered: &v})
	case pricing.UsagePrice:
		return json.Marshal(pricingModelEnvelope{Kind: kindUsage, Usage: &v})
	default:
		return nil, fmt.Errorf("unsupported pricing model type: %T", m)
	}
}

func unmarshalPricingModel(data []byte) (pricing.PricingModel, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var env pricingModelEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("unmarshal pricing model: %w", err)
	}
	switch env.Kind {
	case kindFlat:
		if env.Flat != nil {
			return *env.Flat, nil
		}
	case kindTiered:
		if env.Tiered != nil {
			return *env.Tiered, nil
		}
	case kindUsage:
		if env.Usage != nil {
			return *env.Usage, nil
		}
	}
	return nil, nil
}

func (r *MySQLPriceRepository) FindByID(ctx context.Context, id shared.PriceID) (*pricing.Price, error) {
	row := r.q(ctx).QueryRowContext(ctx, selectPriceSQL+` WHERE id = ?`, string(id))
	return scanPriceRow(row, id)
}

func (r *MySQLPriceRepository) FindByProductID(ctx context.Context, productID shared.ProductID) ([]*pricing.Price, error) {
	rows, err := r.q(ctx).QueryContext(ctx, selectPriceSQL+` WHERE product_id = ? ORDER BY created_at DESC`, string(productID))
	if err != nil {
		return nil, fmt.Errorf("find prices by product: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanPriceRows(rows)
}

func (r *MySQLPriceRepository) FindActiveByProductID(ctx context.Context, productID shared.ProductID) ([]*pricing.Price, error) {
	rows, err := r.q(ctx).QueryContext(ctx,
		selectPriceSQL+` WHERE product_id = ? AND status = 'active' ORDER BY created_at DESC`, string(productID))
	if err != nil {
		return nil, fmt.Errorf("find active prices: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanPriceRows(rows)
}

func (r *MySQLPriceRepository) Save(ctx context.Context, p *pricing.Price) error {
	s := p.ToSnapshot()
	intervalJSON, _ := json.Marshal(s.Interval)
	modelJSON, err := marshalPricingModel(s.PricingModel)
	if err != nil {
		return fmt.Errorf("marshal pricing model: %w", err)
	}

	_, err = r.q(ctx).ExecContext(ctx,
		`INSERT INTO prices (id, product_id, amount, currency, billing_cycle, interval_data, pricing_model, status, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(6))
		 ON DUPLICATE KEY UPDATE
		   amount = VALUES(amount), currency = VALUES(currency),
		   interval_data = VALUES(interval_data), pricing_model = VALUES(pricing_model),
		   status = VALUES(status), updated_at = NOW(6)`,
		string(s.ID), string(s.ProductID), s.Amount.Int64(), string(s.Currency),
		"", intervalJSON, modelJSON, string(s.Status), s.CreatedAt.UTC())
	if err != nil {
		return fmt.Errorf("save price: %w", err)
	}
	return nil
}

const selectPriceSQL = `
	SELECT id, product_id, amount, currency, interval_data, pricing_model, status, created_at
	FROM prices`

func scanPriceRow(row *sql.Row, id shared.PriceID) (*pricing.Price, error) {
	s, err := scanPriceSnapshot(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, shared.NewDomainError(shared.ErrCodeNotFound, fmt.Sprintf("price %s not found", id))
		}
		return nil, fmt.Errorf("scan price: %w", err)
	}
	return pricing.FromSnapshot(s)
}

func scanPriceRows(rows *sql.Rows) ([]*pricing.Price, error) {
	var result []*pricing.Price
	for rows.Next() {
		s, err := scanPriceSnapshot(rows)
		if err != nil {
			return nil, fmt.Errorf("scan price row: %w", err)
		}
		p, err := pricing.FromSnapshot(s)
		if err != nil {
			return nil, fmt.Errorf("reconstruct price: %w", err)
		}
		result = append(result, p)
	}
	return result, rows.Err()
}

func scanPriceSnapshot(t scanTarget) (pricing.PriceSnapshot, error) {
	var (
		s                             pricing.PriceSnapshot
		id, productID                 string
		amount                        int64
		currency, status              string
		intervalJSON, pricingModelRaw json.RawMessage
		createdAt                     utcTime
	)
	if err := t.Scan(&id, &productID, &amount, &currency, &intervalJSON, &pricingModelRaw, &status, &createdAt); err != nil {
		return pricing.PriceSnapshot{}, err
	}

	cur := shared.Currency(currency)
	s.ID = shared.PriceID(id)
	s.ProductID = shared.ProductID(productID)
	s.Amount = moneyFromInt64(amount, cur)
	s.Currency = cur
	s.Status = pricing.PriceStatus(status)
	s.CreatedAt = createdAt.Time
	if len(intervalJSON) > 0 {
		_ = json.Unmarshal(intervalJSON, &s.Interval)
	}
	model, _ := unmarshalPricingModel(pricingModelRaw)
	s.PricingModel = model
	return s, nil
}
