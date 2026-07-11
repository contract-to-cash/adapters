package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/contract-to-cash/core/application/projection"
)

// CheckpointStore persists the last processed global_position for a named projector.
//
// It satisfies the core application/projection.CheckpointStore port (core#192):
// Load returns 0 when no checkpoint exists, and Save upserts the position. The
// additional Reset method is an adapter-local convenience outside the port.
type CheckpointStore struct {
	db *sql.DB
}

var _ projection.CheckpointStore = (*CheckpointStore)(nil)

// NewCheckpointStore constructs a CheckpointStore over an existing *sql.DB.
func NewCheckpointStore(db *sql.DB) *CheckpointStore {
	return &CheckpointStore{db: db}
}

func (s *CheckpointStore) q(ctx context.Context) Querier {
	return querierFromContext(ctx, s.db)
}

func (s *CheckpointStore) Load(ctx context.Context, projectorName string) (int64, error) {
	var pos int64
	err := s.q(ctx).QueryRowContext(ctx,
		`SELECT last_position FROM projection_checkpoints WHERE projector_name = ?`,
		projectorName).Scan(&pos)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("load checkpoint: %w", err)
	}
	return pos, nil
}

func (s *CheckpointStore) Save(ctx context.Context, projectorName string, position int64) error {
	// last_updated is stamped by the database clock (column DEFAULT NOW(6) on
	// insert, NOW(6) on update) rather than a Go time.Now(), keeping this
	// adapter free of direct wall-clock reads per the core shared.Clock
	// convention. The checkpoint timestamp is bookkeeping only, so the DB clock
	// is authoritative here.
	_, err := s.q(ctx).ExecContext(ctx,
		`INSERT INTO projection_checkpoints (projector_name, last_position)
		 VALUES (?, ?)
		 ON DUPLICATE KEY UPDATE
		   last_position = VALUES(last_position),
		   last_updated  = NOW(6)`,
		projectorName, position)
	if err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	return nil
}

func (s *CheckpointStore) Reset(ctx context.Context, projectorName string) error {
	_, err := s.q(ctx).ExecContext(ctx,
		`DELETE FROM projection_checkpoints WHERE projector_name = ?`, projectorName)
	if err != nil {
		return fmt.Errorf("reset checkpoint: %w", err)
	}
	return nil
}
