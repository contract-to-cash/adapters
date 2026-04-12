package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/domain/pricing"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresPriceRepository implements pricing.PriceRepository.
//
// Prices are master data. The PricingModel interface is serialized as a
// discriminated union in a JSONB column so that every built-in model
// (FlatPrice / TieredPrice / UsagePrice) round-trips cleanly.
type PostgresPriceRepository struct {
	pool *pgxpool.Pool
}

var _ pricing.PriceRepository = (*PostgresPriceRepository)(nil)

// NewPriceRepository creates a new PostgresPriceRepository.
func NewPriceRepository(pool *pgxpool.Pool) *PostgresPriceRepository {
	return &PostgresPriceRepository{pool: pool}
}

func (r *PostgresPriceRepository) q(ctx context.Context) Querier {
	return QuerierFromContext(ctx, r.pool)
}

// pricingModelKind tags a serialized PricingModel so the reader knows which
// concrete type to unmarshal into.
type pricingModelKind string

const (
	kindFlat   pricingModelKind = "flat"
	kindTiered pricingModelKind = "tiered"
	kindUsage  pricingModelKind = "usage"
)

// pricingModelEnvelope is the JSONB-friendly tagged union written to
// prices.pricing_model.
type pricingModelEnvelope struct {
	Kind pricingModelKind `json:"kind"`
	Flat *pricing.FlatPrice `json:"flat,omitempty"`
	Tiered *pricing.TieredPrice `json:"tiered,omitempty"`
	Usage *pricing.UsagePrice `json:"usage,omitempty"`
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
		return nil, fmt.Errorf("unmarshal pricing model envelope: %w", err)
	}
	switch env.Kind {
	case kindFlat:
		if env.Flat == nil {
			return nil, nil
		}
		return *env.Flat, nil
	case kindTiered:
		if env.Tiered == nil {
			return nil, nil
		}
		return *env.Tiered, nil
	case kindUsage:
		if env.Usage == nil {
			return nil, nil
		}
		return *env.Usage, nil
	case "":
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown pricing model kind: %q", env.Kind)
	}
}

// FindByID loads a price by ID.
func (r *PostgresPriceRepository) FindByID(ctx context.Context, id shared.PriceID) (*pricing.Price, error) {
	row := r.q(ctx).QueryRow(ctx, selectPriceSQL+` WHERE id = $1`, string(id))
	return scanPriceRow(row, id)
}

// FindByProductID returns all prices for a product.
func (r *PostgresPriceRepository) FindByProductID(ctx context.Context, productID shared.ProductID) ([]*pricing.Price, error) {
	rows, err := r.q(ctx).Query(ctx,
		selectPriceSQL+` WHERE product_id = $1 ORDER BY created_at DESC`,
		string(productID),
	)
	if err != nil {
		return nil, fmt.Errorf("find prices by product: %w", err)
	}
	defer rows.Close()
	return scanPriceRows(rows)
}

// FindActiveByProductID returns only active prices for a product.
func (r *PostgresPriceRepository) FindActiveByProductID(ctx context.Context, productID shared.ProductID) ([]*pricing.Price, error) {
	rows, err := r.q(ctx).Query(ctx,
		selectPriceSQL+` WHERE product_id = $1 AND status = 'active' ORDER BY created_at DESC`,
		string(productID),
	)
	if err != nil {
		return nil, fmt.Errorf("find active prices by product: %w", err)
	}
	defer rows.Close()
	return scanPriceRows(rows)
}

// Save upserts a price.
func (r *PostgresPriceRepository) Save(ctx context.Context, p *pricing.Price) error {
	s := p.ToSnapshot()

	intervalJSON, err := json.Marshal(s.Interval)
	if err != nil {
		return fmt.Errorf("marshal billing interval: %w", err)
	}
	modelJSON, err := marshalPricingModel(s.PricingModel)
	if err != nil {
		return fmt.Errorf("marshal pricing model: %w", err)
	}

	_, err = r.q(ctx).Exec(ctx,
		`INSERT INTO prices (
			id, product_id, amount, currency, billing_cycle, interval_data, pricing_model, status, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
		ON CONFLICT (id) DO UPDATE SET
			amount        = EXCLUDED.amount,
			currency      = EXCLUDED.currency,
			interval_data = EXCLUDED.interval_data,
			pricing_model = EXCLUDED.pricing_model,
			status        = EXCLUDED.status,
			updated_at    = NOW()`,
		string(s.ID), string(s.ProductID), s.Amount.Int64(), string(s.Currency),
		"", intervalJSON, modelJSON, string(s.Status), s.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("save price: %w", err)
	}
	return nil
}

const selectPriceSQL = `
	SELECT id, product_id, amount, currency, interval_data, pricing_model, status, created_at
	FROM prices`

func scanPriceRow(row pgx.Row, id shared.PriceID) (*pricing.Price, error) {
	s, err := scanPriceSnapshot(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, shared.NewDomainError(shared.ErrCodeNotFound,
				fmt.Sprintf("price %s not found", id))
		}
		return nil, fmt.Errorf("scan price: %w", err)
	}
	return pricing.FromSnapshot(s)
}

func scanPriceRows(rows pgx.Rows) ([]*pricing.Price, error) {
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
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return result, nil
}

func scanPriceSnapshot(t scanTarget) (pricing.PriceSnapshot, error) {
	var (
		s                             pricing.PriceSnapshot
		id, productID                 string
		amount                        int64
		currency, status              string
		intervalJSON, pricingModelRaw json.RawMessage
		createdAt                     time.Time
	)
	if err := t.Scan(
		&id, &productID, &amount, &currency, &intervalJSON, &pricingModelRaw, &status, &createdAt,
	); err != nil {
		return pricing.PriceSnapshot{}, err
	}

	cur := shared.Currency(currency)
	s.ID = shared.PriceID(id)
	s.ProductID = shared.ProductID(productID)
	s.Amount = moneyFromInt64(amount, cur)
	s.Currency = cur
	s.Status = pricing.PriceStatus(status)
	s.CreatedAt = createdAt

	if len(intervalJSON) > 0 {
		if err := json.Unmarshal(intervalJSON, &s.Interval); err != nil {
			return pricing.PriceSnapshot{}, fmt.Errorf("unmarshal billing interval: %w", err)
		}
	}
	model, err := unmarshalPricingModel(pricingModelRaw)
	if err != nil {
		return pricing.PriceSnapshot{}, err
	}
	s.PricingModel = model
	return s, nil
}
