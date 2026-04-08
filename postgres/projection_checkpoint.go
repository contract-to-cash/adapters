package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CheckpointStore manages projection checkpoint persistence.
// Tracks the last processed global_position per projector, enabling
// restart without full replay.
type CheckpointStore struct {
	pool *pgxpool.Pool
}

func NewCheckpointStore(pool *pgxpool.Pool) *CheckpointStore {
	return &CheckpointStore{pool: pool}
}

// Load returns the last processed position for a projector (0 if not found).
func (s *CheckpointStore) Load(ctx context.Context, projectorName string) (int64, error) {
	q := QuerierFromContext(ctx, s.pool)

	var pos int64
	err := q.QueryRow(ctx,
		`SELECT last_position FROM projection_checkpoints WHERE projector_name = $1`,
		projectorName,
	).Scan(&pos)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("load checkpoint: %w", err)
	}
	return pos, nil
}

// Save upserts the checkpoint for a projector.
func (s *CheckpointStore) Save(ctx context.Context, projectorName string, position int64) error {
	q := QuerierFromContext(ctx, s.pool)

	_, err := q.Exec(ctx,
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

// Reset resets a projector's checkpoint to 0, used during rebuild.
func (s *CheckpointStore) Reset(ctx context.Context, projectorName string) error {
	q := QuerierFromContext(ctx, s.pool)

	_, err := q.Exec(ctx,
		`DELETE FROM projection_checkpoints WHERE projector_name = $1`,
		projectorName,
	)
	if err != nil {
		return fmt.Errorf("reset checkpoint: %w", err)
	}
	return nil
}
