package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CheckpointStore persists the last processed global_position for a named
// projector, so projection consumers can resume after restart without
// replaying from zero.
//
// This is an adapter-internal concept: core's application/projection package
// does NOT define a CheckpointStore interface. If other adapters (mongo,
// mysql, ...) want the same semantics they can either copy this type or
// propose upstreaming it.
type CheckpointStore struct {
	pool *pgxpool.Pool
}

// NewCheckpointStore creates a new CheckpointStore.
func NewCheckpointStore(pool *pgxpool.Pool) *CheckpointStore {
	return &CheckpointStore{pool: pool}
}

func (s *CheckpointStore) q(ctx context.Context) Querier {
	return QuerierFromContext(ctx, s.pool)
}

// Load returns the last processed global_position for the named projector,
// or 0 when no checkpoint has been recorded yet. A zero-value return is
// compatible with Subscribe(ctx, 0) which means "from the beginning".
func (s *CheckpointStore) Load(ctx context.Context, projectorName string) (int64, error) {
	var pos int64
	err := s.q(ctx).QueryRow(ctx,
		`SELECT last_position FROM projection_checkpoints WHERE projector_name = $1`,
		projectorName,
	).Scan(&pos)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("load checkpoint: %w", err)
	}
	return pos, nil
}

// Save upserts the checkpoint for the named projector.
func (s *CheckpointStore) Save(ctx context.Context, projectorName string, position int64) error {
	_, err := s.q(ctx).Exec(ctx,
		`INSERT INTO projection_checkpoints (projector_name, last_position, last_updated)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (projector_name) DO UPDATE SET
		   last_position = EXCLUDED.last_position,
		   last_updated  = EXCLUDED.last_updated`,
		projectorName, position, time.Now(),
	)
	if err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	return nil
}

// Reset removes the checkpoint for the named projector. Used during
// projection rebuild so the next processing pass starts from the beginning.
func (s *CheckpointStore) Reset(ctx context.Context, projectorName string) error {
	_, err := s.q(ctx).Exec(ctx,
		`DELETE FROM projection_checkpoints WHERE projector_name = $1`,
		projectorName,
	)
	if err != nil {
		return fmt.Errorf("reset checkpoint: %w", err)
	}
	return nil
}
