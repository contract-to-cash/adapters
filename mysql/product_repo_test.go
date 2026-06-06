package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/contract-to-cash/core/domain/product"
	"github.com/contract-to-cash/core/domain/shared"
)

func newProductRepo(t *testing.T) (*MySQLProductRepository, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewProductRepository(db), mock
}

func sampleProduct(t *testing.T) *product.Product {
	t.Helper()
	limit := int64(10)
	p, err := product.FromSnapshot(product.ProductSnapshot{
		ID:          "prod-1",
		Name:        "Test Product",
		Description: "A test product",
		Status:      product.ProductStatusActive,
		Features:    []product.Feature{{Name: "feature-1", Included: true, Limit: &limit}},
		Metadata:    map[string]string{"tier": "gold"},
		CreatedAt:   fixedTime,
	})
	if err != nil {
		t.Fatalf("FromSnapshot: %v", err)
	}
	return p
}

func TestProductRepo_Save_Upsert(t *testing.T) {
	repo, mock := newProductRepo(t)
	p := sampleProduct(t)

	mock.ExpectExec(`INSERT INTO products .* ON DUPLICATE KEY UPDATE`).
		WithArgs("prod-1", "Test Product", "A test product", "active",
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), fixedTime).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Save(context.Background(), p); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestProductRepo_FindByID_Found(t *testing.T) {
	repo, mock := newProductRepo(t)
	rows := sqlmock.NewRows([]string{"name", "description", "status", "features", "usage_metrics", "metadata", "created_at"}).
		AddRow("Test Product", "A test product", "active",
			[]byte(`[{"Name":"feature-1","Included":true,"Limit":10}]`),
			[]byte(`[]`), []byte(`{"tier":"gold"}`), fixedTime)
	mock.ExpectQuery(`SELECT name, description, status, features, usage_metrics, metadata, created_at FROM products WHERE id = \?`).
		WithArgs("prod-1").
		WillReturnRows(rows)

	got, err := repo.FindByID(context.Background(), "prod-1")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	s := got.ToSnapshot()
	if s.Name != "Test Product" {
		t.Errorf("Name = %q, want Test Product", s.Name)
	}
	if s.Status != product.ProductStatusActive {
		t.Errorf("Status = %q, want active", s.Status)
	}
	if !s.CreatedAt.Equal(fixedTime) {
		t.Errorf("CreatedAt = %v, want %v", s.CreatedAt, fixedTime)
	}
}

func TestProductRepo_FindByID_NotFound(t *testing.T) {
	repo, mock := newProductRepo(t)
	mock.ExpectQuery(`SELECT .* FROM products WHERE id = \?`).
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)

	_, err := repo.FindByID(context.Background(), "missing")
	var de *shared.DomainError
	if !errors.As(err, &de) || de.Code != shared.ErrCodeNotFound {
		t.Fatalf("expected not_found DomainError, got %v", err)
	}
}
