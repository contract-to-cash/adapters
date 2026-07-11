package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/application/tx"
	"github.com/contract-to-cash/core/domain/payment"
	"github.com/contract-to-cash/core/domain/shared"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

	q := r.q(ctx)

	// The upsert conflicts on (id) only, so a same-id re-save (e.g. the 3DS
	// Pending -> Completed upgrade path) is updated in place. A DIFFERENT payment
	// carrying the same idempotency_key does NOT match ON CONFLICT (id); it trips
	// the payments_idempotency_key_key UNIQUE constraint and raises 23505. That
	// raw pgconn error MUST be translated to the core sentinel (issue #35 /
	// core#97) — otherwise PaymentService routes the race loser through saga
	// compensation and refunds the winner's legitimate charge.
	//
	// Optimistic-lock guarded update (core#190). lock_version holds the domain
	// optimistic-locking version (p.Version()); the ON CONFLICT DO UPDATE only
	// fires when the stored lock_version still equals LoadedVersion(). A concurrent
	// writer that already advanced it leaves the update skipped and RowsAffected()
	// == 0, which we report as tx.ErrVersionConflict. This is what stops two
	// concurrent RecordRefund calls on the same completed payment from both
	// persisting last-writer-wins; the loser retries and RecordRefund re-validates
	// against the winner's already-booked total, rejecting the over-refund. A
	// brand-new payment (id absent) INSERTs regardless of lock_version, so a
	// version-0 payment is never mistaken for a conflict — the INSERT-vs-UPDATE
	// decision keys off row existence, never off the lock_version value.
	tag, err := q.Exec(ctx,
		`INSERT INTO payments (id, invoice_id, idempotency_key, amount, refunded_amount,
			currency, status, method, gateway_transaction_id, failure_reason, processed_at, metadata, state, lock_version, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW())
		 ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status, amount = EXCLUDED.amount,
			refunded_amount = EXCLUDED.refunded_amount, currency = EXCLUDED.currency,
			method = EXCLUDED.method, gateway_transaction_id = EXCLUDED.gateway_transaction_id,
			failure_reason = EXCLUDED.failure_reason, processed_at = EXCLUDED.processed_at,
			metadata = EXCLUDED.metadata, state = EXCLUDED.state,
			lock_version = EXCLUDED.lock_version, updated_at = NOW()
		 WHERE payments.lock_version = $15`,
		string(s.ID), string(s.InvoiceID), idempotencyKey,
		s.Amount.Int64(), s.RefundedAmount.Int64(),
		string(s.Amount.Currency()), string(s.Status), string(s.Method),
		s.GatewayTransactionID, failureReason, processedAt, metadata, jsonState,
		s.Version, p.LoadedVersion())
	if err != nil {
		if isIdempotencyKeyConflict(err) {
			return r.duplicateIdempotencyKey(ctx, q, s)
		}
		return fmt.Errorf("save payment: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// The row exists but its stored lock_version no longer equals
		// LoadedVersion() (a concurrent writer advanced it). A brand-new row would
		// have inserted, so 0 unambiguously means an optimistic-lock conflict.
		return tx.ErrVersionConflict
	}
	// Record the persisted optimistic-lock version as the new loaded baseline so a
	// subsequent Save of this same in-memory entity compares against the right one.
	p.SetVersion(s.Version)
	return nil
}

// paymentsIdempotencyKeyConstraint is the auto-generated name of the UNIQUE
// constraint on payments.idempotency_key (Postgres names a column-level UNIQUE
// constraint <table>_<column>_key). Matching on the constraint name — not the
// bare 23505 code — keeps an unrelated UNIQUE violation from being misreported
// as a duplicate idempotency key (mirrors isVersionConflict in eventstore.go).
const paymentsIdempotencyKeyConstraint = "payments_idempotency_key_key"

// isIdempotencyKeyConflict reports whether err is a unique-violation (23505)
// specifically on the payments idempotency_key constraint.
func isIdempotencyKeyConflict(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgUniqueViolation &&
			pgErr.ConstraintName == paymentsIdempotencyKeyConstraint
	}
	return false
}

// duplicateIdempotencyKey builds the core's payment.ErrDuplicateIdempotencyKey
// sentinel for a rejected write. It makes a best-effort lookup of the winning
// record's id for operational debugging; a lookup failure must never mask the
// duplicate signal.
func (r *PostgresPaymentRepository) duplicateIdempotencyKey(ctx context.Context, q Querier, s payment.PaymentSnapshot) error {
	dup := &payment.DuplicateIdempotencyKeyError{
		Key:         s.IdempotencyKey,
		AttemptedID: s.ID,
	}
	var existingID string
	if err := q.QueryRow(ctx,
		`SELECT id FROM payments WHERE idempotency_key = $1`, s.IdempotencyKey).Scan(&existingID); err == nil {
		dup.ExistingID = shared.PaymentID(existingID)
	}
	return dup
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
	       status, method, gateway_transaction_id, failure_reason, processed_at, metadata, state, lock_version
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
		lockVersion                         int
	)
	if err := t.Scan(
		&id, &invoiceID, &idempotencyKey, &amount, &refundedAmount, &currency,
		&status, &method, &gatewayTransactionID, &failureReason, &processedAt, &metadata, &stateRaw, &lockVersion,
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
	// The payments.lock_version column is authoritative for the optimistic-locking
	// version (core#190). FromSnapshot restores both version and loadedVersion from
	// it, so the next Save guards on the right baseline.
	s.Version = lockVersion
	return s, nil
}
