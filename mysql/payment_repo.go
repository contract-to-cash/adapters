package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/domain/payment"
	"github.com/contract-to-cash/core/domain/shared"
)

// MySQLPaymentRepository is a MySQL-backed payment.Repository.
type MySQLPaymentRepository struct {
	db *sql.DB
}

var _ payment.Repository = (*MySQLPaymentRepository)(nil)

// NewPaymentRepository constructs a payment repository over an existing *sql.DB.
func NewPaymentRepository(db *sql.DB) *MySQLPaymentRepository {
	return &MySQLPaymentRepository{db: db}
}

func (r *MySQLPaymentRepository) q(ctx context.Context) Querier {
	return querierFromContext(ctx, r.db)
}

func (r *MySQLPaymentRepository) Save(ctx context.Context, p *payment.Payment) error {
	s := p.ToSnapshot()

	metadata, err := json.Marshal(s.Metadata)
	if err != nil {
		return fmt.Errorf("marshal payment metadata: %w", err)
	}

	var idempotencyKey *string
	if s.IdempotencyKey != "" {
		idempotencyKey = &s.IdempotencyKey
	}

	var failureReason string
	if s.FailureReason != nil {
		failureReason = *s.FailureReason
	}

	var processedAt *time.Time
	if !s.ProcessedAt.IsZero() {
		t := s.ProcessedAt.UTC()
		processedAt = &t
	}

	_, err = r.q(ctx).ExecContext(ctx,
		`INSERT INTO payments (id, invoice_id, idempotency_key, amount, refunded_amount,
			currency, status, method, gateway_transaction_id, failure_reason, processed_at, metadata, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(6))
		 ON DUPLICATE KEY UPDATE
			status = VALUES(status), amount = VALUES(amount),
			refunded_amount = VALUES(refunded_amount), currency = VALUES(currency),
			method = VALUES(method), gateway_transaction_id = VALUES(gateway_transaction_id),
			failure_reason = VALUES(failure_reason), processed_at = VALUES(processed_at),
			metadata = VALUES(metadata), updated_at = NOW(6)`,
		string(s.ID), string(s.InvoiceID), idempotencyKey,
		s.Amount.Int64(), s.RefundedAmount.Int64(),
		string(s.Amount.Currency()), string(s.Status), string(s.Method),
		s.GatewayTransactionID, failureReason, processedAt, metadata)
	if err != nil {
		return fmt.Errorf("save payment: %w", err)
	}
	return nil
}

func (r *MySQLPaymentRepository) FindByID(ctx context.Context, id shared.PaymentID) (*payment.Payment, error) {
	row := r.q(ctx).QueryRowContext(ctx, selectPaymentSQL+` WHERE id = ?`, string(id))
	s, err := scanPaymentSnapshot(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, shared.NewDomainError(shared.ErrCodeNotFound,
				fmt.Sprintf("payment %s not found", id))
		}
		return nil, fmt.Errorf("scan payment: %w", err)
	}
	return payment.FromSnapshot(s)
}

func (r *MySQLPaymentRepository) FindByInvoiceID(ctx context.Context, invoiceID shared.InvoiceID) ([]*payment.Payment, error) {
	rows, err := r.q(ctx).QueryContext(ctx, selectPaymentSQL+` WHERE invoice_id = ? ORDER BY created_at DESC`, string(invoiceID))
	if err != nil {
		return nil, fmt.Errorf("find payments by invoice: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanPaymentRows(rows)
}

func (r *MySQLPaymentRepository) FindByIdempotencyKey(ctx context.Context, key string) (*payment.Payment, error) {
	row := r.q(ctx).QueryRowContext(ctx, selectPaymentSQL+` WHERE idempotency_key = ?`, key)
	s, err := scanPaymentSnapshot(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("scan payment by idempotency key: %w", err)
	}
	return payment.FromSnapshot(s)
}

func scanPaymentRows(rows *sql.Rows) ([]*payment.Payment, error) {
	var result []*payment.Payment
	for rows.Next() {
		s, err := scanPaymentSnapshot(rows)
		if err != nil {
			return nil, fmt.Errorf("scan payment row: %w", err)
		}
		p, err := payment.FromSnapshot(s)
		if err != nil {
			return nil, fmt.Errorf("reconstruct payment: %w", err)
		}
		result = append(result, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return result, nil
}

const selectPaymentSQL = `
	SELECT id, invoice_id, idempotency_key, amount, refunded_amount, currency,
	       status, method, gateway_transaction_id, failure_reason, processed_at, metadata
	FROM payments`

func scanPaymentSnapshot(t scanTarget) (payment.PaymentSnapshot, error) {
	var (
		s                                   payment.PaymentSnapshot
		id, invoiceID                       string
		idempotencyKey                      sql.NullString
		amount, refundedAmount              int64
		currency, status, method            string
		gatewayTransactionID, failureReason string
		processedAt                         sql.NullTime
		metadata                            json.RawMessage
	)
	if err := t.Scan(
		&id, &invoiceID, &idempotencyKey, &amount, &refundedAmount, &currency,
		&status, &method, &gatewayTransactionID, &failureReason, &processedAt, &metadata,
	); err != nil {
		return payment.PaymentSnapshot{}, err
	}

	cur := shared.Currency(currency)
	s.ID = shared.PaymentID(id)
	s.InvoiceID = shared.InvoiceID(invoiceID)
	if idempotencyKey.Valid {
		s.IdempotencyKey = idempotencyKey.String
	}
	s.Amount = moneyFromInt64(amount, cur)
	s.RefundedAmount = moneyFromInt64(refundedAmount, cur)
	s.Method = payment.PaymentMethod(method)
	s.Status = payment.PaymentStatus(status)
	s.GatewayTransactionID = gatewayTransactionID
	if failureReason != "" {
		s.FailureReason = &failureReason
	}
	if processedAt.Valid {
		s.ProcessedAt = processedAt.Time.UTC()
	}
	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &s.Metadata); err != nil {
			return payment.PaymentSnapshot{}, fmt.Errorf("unmarshal payment metadata: %w", err)
		}
	}
	return s, nil
}
