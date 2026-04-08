package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/contract-to-cash/core/domain/balance"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresBalanceRepository implements balance.Repository with optimistic locking
// via the LoadedVersion pattern.
// Updates use WHERE version = $loaded_version to detect concurrent modifications,
// returning balance.ErrConflict for RetryOnConflict compatibility.
type PostgresBalanceRepository struct {
	pool *pgxpool.Pool
}

var _ balance.Repository = (*PostgresBalanceRepository)(nil)

func NewBalanceRepository(pool *pgxpool.Pool) *PostgresBalanceRepository {
	return &PostgresBalanceRepository{pool: pool}
}

func (r *PostgresBalanceRepository) q(ctx context.Context) Querier {
	return QuerierFromContext(ctx, r.pool)
}

func (r *PostgresBalanceRepository) Save(ctx context.Context, b *balance.Balance) error {
	q := r.q(ctx)

	data, err := json.Marshal(b)
	if err != nil {
		return fmt.Errorf("marshal balance: %w", err)
	}

	_, err = q.Exec(ctx,
		`INSERT INTO balances (id, account_id, amount, currency, version, data)
		 VALUES ($1, $2, $3, $4, 1, $5)`,
		b.ID(), b.AccountID(), b.Amount(), b.Currency(), data,
	)
	if err != nil {
		return fmt.Errorf("save balance: %w", err)
	}
	return nil
}

func (r *PostgresBalanceRepository) FindByID(ctx context.Context, id string) (*balance.Balance, error) {
	q := r.q(ctx)

	var data json.RawMessage
	var version int
	err := q.QueryRow(ctx,
		`SELECT data, version FROM balances WHERE id = $1`, id,
	).Scan(&data, &version)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, balance.ErrNotFound
		}
		return nil, fmt.Errorf("find balance: %w", err)
	}

	b, err := balance.Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("unmarshal balance: %w", err)
	}
	b.SetLoadedVersion(version)
	return b, nil
}

func (r *PostgresBalanceRepository) FindByAccountID(ctx context.Context, accountID string) (*balance.Balance, error) {
	q := r.q(ctx)

	var data json.RawMessage
	var version int
	err := q.QueryRow(ctx,
		`SELECT data, version FROM balances WHERE account_id = $1`, accountID,
	).Scan(&data, &version)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, balance.ErrNotFound
		}
		return nil, fmt.Errorf("find balance by account: %w", err)
	}

	b, err := balance.Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("unmarshal balance: %w", err)
	}
	b.SetLoadedVersion(version)
	return b, nil
}

// Update performs an optimistic-locking update.
// Fails with balance.ErrConflict if the version has changed since load.
func (r *PostgresBalanceRepository) Update(ctx context.Context, b *balance.Balance) error {
	q := r.q(ctx)

	data, err := json.Marshal(b)
	if err != nil {
		return fmt.Errorf("marshal balance: %w", err)
	}

	loadedVersion := b.LoadedVersion()
	tag, err := q.Exec(ctx,
		`UPDATE balances
		 SET amount = $1, data = $2, version = version + 1, updated_at = NOW()
		 WHERE id = $3 AND version = $4`,
		b.Amount(), data, b.ID(), loadedVersion,
	)
	if err != nil {
		return fmt.Errorf("update balance: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return balance.ErrConflict
	}
	return nil
}
