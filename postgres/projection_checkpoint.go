package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CheckpointStore persists the last processed global_position for a named projector.
type CheckpointStore struct {
	pool *pgxpool.Pool
}

func NewCheckpointStore(pool *pgxpool.Pool) *CheckpointStore {
	return &CheckpointStore{pool: pool}
}

func (s *CheckpointStore) q(ctx context.Context) Querier {
	return QuerierFromContext(ctx, s.pool)
}

func (s *CheckpointStore) Load(ctx context.Context, projectorName string) (int64, error) {
	var pos int64
	err := s.q(ctx).QueryRow(ctx,
		`SELECT last_position FROM projection_checkpoints WHERE projector_name = $1`,
		projectorName).Scan(&pos)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("load checkpoint: %w", err)
	}
	return pos, nil
}

func (s *CheckpointStore) Save(ctx context.Context, projectorName string, position int64) error {
	// last_updated is stamped by the database clock (column DEFAULT NOW() on
	// insert, NOW() on update) rather than a Go time.Now(), keeping this
	// adapter free of direct wall-clock reads per the core shared.Clock
	// convention. The checkpoint timestamp is bookkeeping only, so the DB clock
	// is authoritative here.
	_, err := s.q(ctx).Exec(ctx,
		`INSERT INTO projection_checkpoints (projector_name, last_position)
		 VALUES ($1, $2)
		 ON CONFLICT (projector_name) DO UPDATE SET
		   last_position = EXCLUDED.last_position,
		   last_updated  = NOW()`,
		projectorName, position)
	if err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	return nil
}

func (s *CheckpointStore) Reset(ctx context.Context, projectorName string) error {
	_, err := s.q(ctx).Exec(ctx,
		`DELETE FROM projection_checkpoints WHERE projector_name = $1`, projectorName)
	if err != nil {
		return fmt.Errorf("reset checkpoint: %w", err)
	}
	return nil
}
