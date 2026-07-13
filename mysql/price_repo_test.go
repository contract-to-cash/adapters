package mysql

import (
	"context"
	"database/sql"
	"errors"
	"math/big"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/contract-to-cash/core/domain/pricing"
	"github.com/contract-to-cash/core/domain/shared"
)

func newPriceRepo(t *testing.T) (*MySQLPriceRepository, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewPriceRepository(db), mock
}

func jpy(amount int64) shared.Money {
	return shared.NewMoney(new(big.Rat).SetInt64(amount), shared.Currency("JPY"))
}

func samplePrice(t *testing.T) *pricing.Price {
	t.Helper()
	p, err := pricing.FromSnapshot(pricing.PriceSnapshot{
		ID:           "price-1",
		ProductID:    "prod-1",
		Amount:       jpy(1000),
		Currency:     "JPY",
		Status:       pricing.PriceStatusActive,
		PricingModel: pricing.FlatPrice{Price: jpy(1000)},
		Metadata:     map[string]string{"tier": "gold"},
		CreatedAt:    fixedTime,
	})
	if err != nil {
		t.Fatalf("FromSnapshot: %v", err)
	}
	return p
}

func priceRow() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id", "product_id", "amount", "currency", "interval_data", "pricing_model", "status", "created_at", "state", "metadata"}).
		AddRow("price-1", "prod-1", int64(1000), "JPY",
			[]byte(`{}`), []byte(`{"kind":"flat","flat":{"Price":{"amount":"1000","currency":"JPY"}}}`), "active", fixedTime, nil,
			[]byte(`{"tier":"gold"}`))
}

func TestPriceRepo_Save_Upsert(t *testing.T) {
	repo, mock := newPriceRepo(t)
	p := samplePrice(t)

	mock.ExpectExec(`INSERT INTO prices .* ON DUPLICATE KEY UPDATE`).
		WithArgs("price-1", "prod-1", int64(1000), "JPY", "",
			sqlmock.AnyArg(), sqlmock.AnyArg(), "active", sqlmock.AnyArg(),
			[]byte(`{"tier":"gold"}`), fixedTime).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Save(context.Background(), p); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestPriceRepo_FindByID_Found(t *testing.T) {
	repo, mock := newPriceRepo(t)
	mock.ExpectQuery(`SELECT .* FROM prices WHERE id = \?`).
		WithArgs("price-1").
		WillReturnRows(priceRow())

	got, err := repo.FindByID(context.Background(), "price-1")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	s := got.ToSnapshot()
	if s.Amount.Int64() != 1000 {
		t.Errorf("Amount = %d, want 1000", s.Amount.Int64())
	}
	if s.PricingModel == nil {
		t.Fatal("PricingModel is nil")
	}
	if _, ok := s.PricingModel.(pricing.FlatPrice); !ok {
		t.Errorf("PricingModel type = %T, want pricing.FlatPrice", s.PricingModel)
	}
	if md := got.Metadata(); md["tier"] != "gold" || len(md) != 1 {
		t.Errorf("Metadata = %v, want map[tier:gold]", md)
	}
}

func TestPriceRepo_FindByID_NilMetadata(t *testing.T) {
	repo, mock := newPriceRepo(t)
	rows := sqlmock.NewRows([]string{"id", "product_id", "amount", "currency", "interval_data", "pricing_model", "status", "created_at", "state", "metadata"}).
		AddRow("price-1", "prod-1", int64(1000), "JPY",
			[]byte(`{}`), []byte(`{"kind":"flat","flat":{"Price":{"amount":"1000","currency":"JPY"}}}`), "active", fixedTime, nil, nil)
	mock.ExpectQuery(`SELECT .* FROM prices WHERE id = \?`).
		WithArgs("price-1").
		WillReturnRows(rows)

	got, err := repo.FindByID(context.Background(), "price-1")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if md := got.Metadata(); len(md) != 0 {
		t.Errorf("Metadata = %v, want empty", md)
	}
}

func TestPriceRepo_FindByID_NotFound(t *testing.T) {
	repo, mock := newPriceRepo(t)
	mock.ExpectQuery(`SELECT .* FROM prices WHERE id = \?`).
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)

	_, err := repo.FindByID(context.Background(), "missing")
	var de *shared.DomainError
	if !errors.As(err, &de) || de.Code != shared.ErrCodeNotFound {
		t.Fatalf("expected not_found DomainError, got %v", err)
	}
}

func TestPriceRepo_FindByProductID(t *testing.T) {
	repo, mock := newPriceRepo(t)
	mock.ExpectQuery(`SELECT .* FROM prices WHERE product_id = \? ORDER BY created_at DESC`).
		WithArgs("prod-1").
		WillReturnRows(priceRow())

	got, err := repo.FindByProductID(context.Background(), "prod-1")
	if err != nil {
		t.Fatalf("FindByProductID: %v", err)
	}
	if len(got) != 1 || got[0].ToSnapshot().ID != "price-1" {
		t.Fatalf("unexpected result: %+v", got)
	}
}

// Core issue #218: pricing.NewOneTimePrice produces a Price with a zero
// Interval() (json.Marshal renders "null", not omitted), an empty
// BillingCycle(), and a nil PricingModel(). Save must issue interval_data as
// JSON null and pricing_model as the {"kind":""} envelope -- no schema change
// is required for the zero-interval one_time case.
func TestPriceRepo_Save_OneTimePrice_NullIntervalData(t *testing.T) {
	repo, mock := newPriceRepo(t)
	p, err := pricing.NewOneTimePrice("prod-1", jpy(5000), "JPY", fixedTime)
	if err != nil {
		t.Fatalf("NewOneTimePrice: %v", err)
	}
	if !p.Interval().IsZero() {
		t.Fatalf("precondition: interval not zero: %+v", p.Interval())
	}

	mock.ExpectExec(`INSERT INTO prices .* ON DUPLICATE KEY UPDATE`).
		WithArgs(string(p.ID()), "prod-1", int64(5000), "JPY", "",
			[]byte("null"), []byte(`{"kind":""}`), "active", sqlmock.AnyArg(),
			[]byte(`{}`), fixedTime).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Save(context.Background(), p); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// A row persisted with interval_data NULL/empty and the nil-pricing-model
// envelope (as a zero-interval one_time price would be) reconstructs to a
// Price with a zero Interval(), empty BillingCycle(), and nil PricingModel().
func TestPriceRepo_FindByID_OneTimePrice_ReconstructsZeroInterval(t *testing.T) {
	repo, mock := newPriceRepo(t)
	rows := sqlmock.NewRows([]string{"id", "product_id", "amount", "currency", "interval_data", "pricing_model", "status", "created_at", "state", "metadata"}).
		AddRow("price-onetime", "prod-1", int64(5000), "JPY",
			[]byte(`null`), []byte(`{"kind":""}`), "active", fixedTime, nil, nil)
	mock.ExpectQuery(`SELECT .* FROM prices WHERE id = \?`).
		WithArgs("price-onetime").
		WillReturnRows(rows)

	got, err := repo.FindByID(context.Background(), "price-onetime")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if !got.Interval().IsZero() {
		t.Errorf("Interval() = %+v, want zero", got.Interval())
	}
	if got.BillingCycle() != "" {
		t.Errorf("BillingCycle() = %q, want empty", got.BillingCycle())
	}
	if got.PricingModel() != nil {
		t.Errorf("PricingModel() = %#v, want nil", got.PricingModel())
	}
	if got.Amount().Int64() != 5000 {
		t.Errorf("Amount = %d, want 5000", got.Amount().Int64())
	}
}

func TestPriceRepo_FindActiveByProductID(t *testing.T) {
	repo, mock := newPriceRepo(t)
	mock.ExpectQuery(`SELECT .* FROM prices WHERE product_id = \? AND status = 'active' ORDER BY created_at DESC`).
		WithArgs("prod-1").
		WillReturnRows(priceRow())

	got, err := repo.FindActiveByProductID(context.Background(), "prod-1")
	if err != nil {
		t.Fatalf("FindActiveByProductID: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 active price, got %d", len(got))
	}
}
