package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/application/tx"
	"github.com/contract-to-cash/core/domain/payment"
	"github.com/contract-to-cash/core/domain/shared"
)

// MySQLPaymentRepository is a MySQL-backed payment.Repository.
type MySQLPaymentRepository struct {
	db *sql.DB
}

var _ payment.Repository = (*MySQLPaymentRepository)(nil)

// paymentJSONState is the precise (big.Rat-backed) representation of a Payment's
// monetary fields. It is the source of truth on read; the amount /
// refunded_amount BIGINT columns are written for query/indexing convenience
// only and are lossy (Money.Int64 truncates any fractional part). See issue #11.
type paymentJSONState struct {
	Amount         shared.Money `json:"amount"`
	RefundedAmount shared.Money `json:"refunded_amount"`
}

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
		t := s.ProcessedAt.UTC()
		processedAt = &t
	}

	q := r.q(ctx)

	// A plain INSERT (NOT `ON DUPLICATE KEY UPDATE`) so that a collision on the
	// idempotency_key UNIQUE index is reported as an error rather than silently
	// overwriting the winner's row with the loser's fields (issue #35 / core#97).
	// Same-id re-saves (e.g. the 3DS Pending -> Completed upgrade path) collide on
	// the PRIMARY key and are routed to a guarded UPDATE below.
	//
	// lock_version carries the domain optimistic-locking version (p.Version(),
	// core#190); a brand-new payment INSERTs it directly.
	_, err = q.ExecContext(ctx,
		`INSERT INTO payments (id, invoice_id, idempotency_key, amount, refunded_amount,
			currency, status, method, gateway_transaction_id, failure_reason, processed_at, metadata, state, lock_version, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(6))`,
		string(s.ID), string(s.InvoiceID), idempotencyKey,
		s.Amount.Int64(), s.RefundedAmount.Int64(),
		string(s.Amount.Currency()), string(s.Status), string(s.Method),
		s.GatewayTransactionID, failureReason, processedAt, metadata, jsonState, s.Version)
	if err == nil {
		p.SetVersion(s.Version)
		return nil
	}

	// InnoDB inserts into the clustered PRIMARY key before any secondary index,
	// so a row that duplicates BOTH id and idempotency_key surfaces as a PRIMARY
	// violation. Therefore an idempotency_key-named duplicate here is always a
	// genuine cross-id collision (a DIFFERENT payment already owns this key) and
	// MUST be translated to the core sentinel; PaymentService converges on the
	// winner instead of firing saga compensation.
	if dupEntryOnKey(err, "idempotency_key") {
		return r.duplicateIdempotencyKey(ctx, q, s)
	}

	// PRIMARY-key duplicate: the payment already exists, so this Save is an
	// update. Guard the UPDATE on id AND lock_version so a concurrent writer that
	// already advanced the version loses (core#190): the WHERE misses, zero rows
	// change, and we report tx.ErrVersionConflict. Because lock_version always
	// changes on a real match (the version is bumped by the mutation that
	// preceded this Save), MySQL's changed-rows RowsAffected is 1 on success and 0
	// only on a genuine version miss — mirroring the balance/invoice repos.
	if dupEntryOnKey(err, "PRIMARY") {
		res, upErr := q.ExecContext(ctx,
			`UPDATE payments SET
				invoice_id = ?, idempotency_key = ?, amount = ?, refunded_amount = ?,
				currency = ?, status = ?, method = ?, gateway_transaction_id = ?,
				failure_reason = ?, processed_at = ?, metadata = ?, state = ?, lock_version = ?, updated_at = NOW(6)
			 WHERE id = ? AND lock_version = ?`,
			string(s.InvoiceID), idempotencyKey, s.Amount.Int64(), s.RefundedAmount.Int64(),
			string(s.Amount.Currency()), string(s.Status), string(s.Method), s.GatewayTransactionID,
			failureReason, processedAt, metadata, jsonState, s.Version, string(s.ID), p.LoadedVersion())
		if upErr != nil {
			// An idempotency_key change that collides with another record still
			// maps to the sentinel; any other error is technical.
			if dupEntryOnKey(upErr, "idempotency_key") {
				return r.duplicateIdempotencyKey(ctx, q, s)
			}
			return fmt.Errorf("save payment: %w", upErr)
		}
		affected, aErr := res.RowsAffected()
		if aErr != nil {
			return fmt.Errorf("payment rows affected: %w", aErr)
		}
		if affected == 0 {
			return tx.ErrVersionConflict
		}
		p.SetVersion(s.Version)
		return nil
	}

	return fmt.Errorf("save payment: %w", err)
}

// duplicateIdempotencyKey builds the core's payment.ErrDuplicateIdempotencyKey
// sentinel for a rejected write. It makes a best-effort lookup of the winning
// record's id for operational debugging; a lookup failure must never mask the
// duplicate signal.
func (r *MySQLPaymentRepository) duplicateIdempotencyKey(ctx context.Context, q Querier, s payment.PaymentSnapshot) error {
	dup := &payment.DuplicateIdempotencyKeyError{
		Key:         s.IdempotencyKey,
		AttemptedID: s.ID,
	}
	var existingID string
	if err := q.QueryRowContext(ctx,
		`SELECT id FROM payments WHERE idempotency_key = ?`, s.IdempotencyKey).Scan(&existingID); err == nil {
		dup.ExistingID = shared.PaymentID(existingID)
	}
	return dup
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
	       status, method, gateway_transaction_id, failure_reason, processed_at, metadata, state, lock_version
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
	if idempotencyKey.Valid {
		s.IdempotencyKey = idempotencyKey.String
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
	if processedAt.Valid {
		s.ProcessedAt = processedAt.Time.UTC()
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
