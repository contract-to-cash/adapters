package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/domain/payment"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresPaymentRepository struct {
	pool *pgxpool.Pool
}

var _ payment.Repository = (*PostgresPaymentRepository)(nil)

// paymentJSONState is the precise (big.Rat-backed) representation of a Payment's
// monetary fields. It is the source of truth on read; the amount /
// refunded_amount BIGINT columns are written for query/indexing convenience
// only and are lossy (Money.Int64 truncates any fractional part). See issue #11.
type paymentJSONState struct {
	Amount         shared.Money `json:"amount"`
	RefundedAmount shared.Money `json:"refunded_amount"`
}

func NewPaymentRepository(pool *pgxpool.Pool) *PostgresPaymentRepository {
	return &PostgresPaymentRepository{pool: pool}
}

func (r *PostgresPaymentRepository) q(ctx context.Context) Querier {
	return QuerierFromContext(ctx, r.pool)
}

func (r *PostgresPaymentRepository) Save(ctx context.Context, p *payment.Payment) error {
	s := p.ToSnapshot()

	metadata, err := json.Marshal(s.Metadata)
	if err != nil {
		return fmt.Errorf("marshal payment metadata: %w", err)
	}

	jsonState, err := json.Marshal(paymentJSONState{
		Amount:         s.Amount,
		RefundedAmount: s.RefundedAmount,
	})
	if err != nil {
		return fmt.Errorf("marshal payment json state: %w", err)
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
		t := s.ProcessedAt
		processedAt = &t
	}

	_, err = r.q(ctx).Exec(ctx,
		`INSERT INTO payments (id, invoice_id, idempotency_key, amount, refunded_amount,
			currency, status, method, gateway_transaction_id, failure_reason, processed_at, metadata, state, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW())
		 ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status, amount = EXCLUDED.amount,
			refunded_amount = EXCLUDED.refunded_amount, currency = EXCLUDED.currency,
			method = EXCLUDED.method, gateway_transaction_id = EXCLUDED.gateway_transaction_id,
			failure_reason = EXCLUDED.failure_reason, processed_at = EXCLUDED.processed_at,
			metadata = EXCLUDED.metadata, state = EXCLUDED.state, updated_at = NOW()`,
		string(s.ID), string(s.InvoiceID), idempotencyKey,
		s.Amount.Int64(), s.RefundedAmount.Int64(),
		string(s.Amount.Currency()), string(s.Status), string(s.Method),
		s.GatewayTransactionID, failureReason, processedAt, metadata, jsonState)
	if err != nil {
		return fmt.Errorf("save payment: %w", err)
	}
	return nil
}

func (r *PostgresPaymentRepository) FindByID(ctx context.Context, id shared.PaymentID) (*payment.Payment, error) {
	row := r.q(ctx).QueryRow(ctx, selectPaymentSQL+` WHERE id = $1`, string(id))
	s, err := scanPaymentSnapshot(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, shared.NewDomainError(shared.ErrCodeNotFound,
				fmt.Sprintf("payment %s not found", id))
		}
		return nil, fmt.Errorf("scan payment: %w", err)
	}
	return payment.FromSnapshot(s)
}

func (r *PostgresPaymentRepository) FindByInvoiceID(ctx context.Context, invoiceID shared.InvoiceID) ([]*payment.Payment, error) {
	rows, err := r.q(ctx).Query(ctx, selectPaymentSQL+` WHERE invoice_id = $1 ORDER BY created_at DESC`, string(invoiceID))
	if err != nil {
		return nil, fmt.Errorf("find payments by invoice: %w", err)
	}
	defer rows.Close()
	return scanPaymentRows(rows)
}

func (r *PostgresPaymentRepository) FindByIdempotencyKey(ctx context.Context, key string) (*payment.Payment, error) {
	row := r.q(ctx).QueryRow(ctx, selectPaymentSQL+` WHERE idempotency_key = $1`, key)
	s, err := scanPaymentSnapshot(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("scan payment by idempotency key: %w", err)
	}
	return payment.FromSnapshot(s)
}

func scanPaymentRows(rows pgx.Rows) ([]*payment.Payment, error) {
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
	       status, method, gateway_transaction_id, failure_reason, processed_at, metadata, state
	FROM payments`

func scanPaymentSnapshot(t scanTarget) (payment.PaymentSnapshot, error) {
	var (
		s                                   payment.PaymentSnapshot
		id, invoiceID                       string
		idempotencyKey                      *string
		amount, refundedAmount              int64
		currency, status, method            string
		gatewayTransactionID, failureReason string
		processedAt                         *time.Time
		metadata                            json.RawMessage
		stateRaw                            []byte
	)
	if err := t.Scan(
		&id, &invoiceID, &idempotencyKey, &amount, &refundedAmount, &currency,
		&status, &method, &gatewayTransactionID, &failureReason, &processedAt, &metadata, &stateRaw,
	); err != nil {
		return payment.PaymentSnapshot{}, err
	}

	cur := shared.Currency(currency)
	s.ID = shared.PaymentID(id)
	s.InvoiceID = shared.InvoiceID(invoiceID)
	if idempotencyKey != nil {
		s.IdempotencyKey = *idempotencyKey
	}
	// Prefer the precise state JSON; fall back to the lossy BIGINT columns for
	// rows written before the state column existed (issue #11).
	if len(stateRaw) > 0 {
		var js paymentJSONState
		if err := json.Unmarshal(stateRaw, &js); err != nil {
			return payment.PaymentSnapshot{}, fmt.Errorf("unmarshal payment json state: %w", err)
		}
		s.Amount = js.Amount
		s.RefundedAmount = js.RefundedAmount
	} else {
		s.Amount = moneyFromInt64(amount, cur)
		s.RefundedAmount = moneyFromInt64(refundedAmount, cur)
	}
	s.Method = payment.PaymentMethod(method)
	s.Status = payment.PaymentStatus(status)
	s.GatewayTransactionID = gatewayTransactionID
	if failureReason != "" {
		s.FailureReason = &failureReason
	}
	if processedAt != nil {
		s.ProcessedAt = *processedAt
	}
	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &s.Metadata); err != nil {
			return payment.PaymentSnapshot{}, fmt.Errorf("unmarshal payment metadata: %w", err)
		}
	}
	return s, nil
}
