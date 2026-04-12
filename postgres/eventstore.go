package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/domain/shared"
	"github.com/contract-to-cash/core/eventstore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	eventsChannel     = "events_inserted"
	subscriberBuffer  = 100
	pgUniqueViolation = "23505"
)

// PostgresEventStore implements eventstore.Store.
type PostgresEventStore struct {
	pool *pgxpool.Pool
}

var _ eventstore.Store = (*PostgresEventStore)(nil)

func NewEventStore(pool *pgxpool.Pool) *PostgresEventStore {
	return &PostgresEventStore{pool: pool}
}

func (s *PostgresEventStore) q(ctx context.Context) Querier {
	return QuerierFromContext(ctx, s.pool)
}

func (s *PostgresEventStore) Append(ctx context.Context, streamID string, events []eventstore.Event, expectedVersion int) error {
	if len(events) == 0 {
		return nil
	}

	q := s.q(ctx)
	for i, evt := range events {
		data, err := json.Marshal(evt.Data)
		if err != nil {
			return fmt.Errorf("marshal event data: %w", err)
		}
		metadata, err := json.Marshal(evt.Metadata)
		if err != nil {
			return fmt.Errorf("marshal event metadata: %w", err)
		}

		version := expectedVersion + i + 1
		_, err = q.Exec(ctx,
			`INSERT INTO events (id, stream_id, type, version, schema_version, data, metadata, occurred_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			evt.ID, streamID, string(evt.Type), version, evt.SchemaVersion,
			data, metadata, evt.OccurredAt,
		)
		if err != nil {
			if isVersionConflict(err) {
				return shared.NewDomainError(
					shared.ErrCodeVersionConflict,
					fmt.Sprintf("stream %q version conflict at version %d", streamID, version),
				)
			}
			return fmt.Errorf("insert event: %w", err)
		}
	}
	return nil
}

func isVersionConflict(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgUniqueViolation
	}
	return false
}

func (s *PostgresEventStore) Load(ctx context.Context, streamID string) ([]eventstore.Event, error) {
	rows, err := s.q(ctx).Query(ctx,
		`SELECT id, stream_id, type, version, schema_version, data, metadata, occurred_at, recorded_at, global_position
		 FROM events WHERE stream_id = $1 ORDER BY version ASC`, streamID)
	if err != nil {
		return nil, fmt.Errorf("load events: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

func (s *PostgresEventStore) LoadUntilVersion(ctx context.Context, streamID string, version int) ([]eventstore.Event, error) {
	rows, err := s.q(ctx).Query(ctx,
		`SELECT id, stream_id, type, version, schema_version, data, metadata, occurred_at, recorded_at, global_position
		 FROM events WHERE stream_id = $1 AND version <= $2 ORDER BY version ASC`,
		streamID, version)
	if err != nil {
		return nil, fmt.Errorf("load events until version: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

func (s *PostgresEventStore) LoadUntil(ctx context.Context, streamID string, until time.Time) ([]eventstore.Event, error) {
	rows, err := s.q(ctx).Query(ctx,
		`SELECT id, stream_id, type, version, schema_version, data, metadata, occurred_at, recorded_at, global_position
		 FROM events WHERE stream_id = $1 AND occurred_at <= $2 ORDER BY version ASC`,
		streamID, until)
	if err != nil {
		return nil, fmt.Errorf("load events until time: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

func (s *PostgresEventStore) LoadRange(ctx context.Context, streamID string, from, to time.Time) ([]eventstore.Event, error) {
	rows, err := s.q(ctx).Query(ctx,
		`SELECT id, stream_id, type, version, schema_version, data, metadata, occurred_at, recorded_at, global_position
		 FROM events WHERE stream_id = $1 AND occurred_at >= $2 AND occurred_at <= $3 ORDER BY version ASC`,
		streamID, from, to)
	if err != nil {
		return nil, fmt.Errorf("load events in range: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

func (s *PostgresEventStore) LoadAll(ctx context.Context, fromPosition int64, limit int) ([]eventstore.Event, error) {
	var (
		rows pgx.Rows
		err  error
	)
	if limit > 0 {
		rows, err = s.q(ctx).Query(ctx,
			`SELECT id, stream_id, type, version, schema_version, data, metadata, occurred_at, recorded_at, global_position
			 FROM events WHERE global_position > $1 ORDER BY global_position ASC LIMIT $2`,
			fromPosition, limit)
	} else {
		rows, err = s.q(ctx).Query(ctx,
			`SELECT id, stream_id, type, version, schema_version, data, metadata, occurred_at, recorded_at, global_position
			 FROM events WHERE global_position > $1 ORDER BY global_position ASC`,
			fromPosition)
	}
	if err != nil {
		return nil, fmt.Errorf("load all events: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

func (s *PostgresEventStore) Subscribe(ctx context.Context, fromPosition int64) (<-chan eventstore.Event, error) {
	ch := make(chan eventstore.Event, subscriberBuffer)
	go s.runSubscription(ctx, fromPosition, ch)
	return ch, nil
}

func (s *PostgresEventStore) runSubscription(ctx context.Context, fromPosition int64, ch chan<- eventstore.Event) {
	defer close(ch)

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), fmt.Sprintf("UNLISTEN %s", eventsChannel))
		conn.Release()
	}()

	if _, err := conn.Exec(ctx, fmt.Sprintf("LISTEN %s", eventsChannel)); err != nil {
		return
	}

	position := fromPosition
	if err := s.catchUp(ctx, &position, ch); err != nil {
		return
	}

	for {
		if ctx.Err() != nil {
			return
		}
		if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		if err := s.catchUp(ctx, &position, ch); err != nil {
			return
		}
	}
}

func (s *PostgresEventStore) catchUp(ctx context.Context, position *int64, ch chan<- eventstore.Event) error {
	events, err := s.LoadAll(ctx, *position, 0)
	if err != nil {
		return err
	}
	for _, evt := range events {
		select {
		case ch <- evt:
			*position = evt.GlobalPosition
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *PostgresEventStore) SaveSnapshot(ctx context.Context, snapshot eventstore.Snapshot) error {
	state, err := json.Marshal(snapshot.State)
	if err != nil {
		return fmt.Errorf("marshal snapshot state: %w", err)
	}
	_, err = s.q(ctx).Exec(ctx,
		`INSERT INTO snapshots (stream_id, version, state, as_of)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (stream_id, version) DO UPDATE
		   SET state = EXCLUDED.state, as_of = EXCLUDED.as_of`,
		snapshot.StreamID, snapshot.Version, state, snapshot.AsOf)
	if err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}
	return nil
}

func (s *PostgresEventStore) LoadSnapshot(ctx context.Context, streamID string) (*eventstore.Snapshot, error) {
	return s.loadSnapshotRow(ctx,
		`SELECT stream_id, version, state, as_of, created_at
		 FROM snapshots WHERE stream_id = $1 ORDER BY version DESC LIMIT 1`, streamID)
}

func (s *PostgresEventStore) LoadSnapshotBefore(ctx context.Context, streamID string, before time.Time) (*eventstore.Snapshot, error) {
	return s.loadSnapshotRow(ctx,
		`SELECT stream_id, version, state, as_of, created_at
		 FROM snapshots WHERE stream_id = $1 AND as_of < $2 ORDER BY version DESC LIMIT 1`,
		streamID, before)
}

func (s *PostgresEventStore) loadSnapshotRow(ctx context.Context, sql string, args ...any) (*eventstore.Snapshot, error) {
	var snap eventstore.Snapshot
	var state json.RawMessage
	err := s.q(ctx).QueryRow(ctx, sql, args...).
		Scan(&snap.StreamID, &snap.Version, &state, &snap.AsOf, &snap.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("load snapshot: %w", err)
	}
	snap.State = state
	return &snap, nil
}

func scanEvents(rows pgx.Rows) ([]eventstore.Event, error) {
	var events []eventstore.Event
	for rows.Next() {
		var (
			evt      eventstore.Event
			typeStr  string
			data     json.RawMessage
			metadata json.RawMessage
		)
		if err := rows.Scan(
			&evt.ID, &evt.StreamID, &typeStr, &evt.Version, &evt.SchemaVersion,
			&data, &metadata, &evt.OccurredAt, &evt.RecordedAt, &evt.GlobalPosition,
		); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		evt.Type = eventstore.EventType(typeStr)
		evt.Data = data
		if len(metadata) > 0 {
			if err := json.Unmarshal(metadata, &evt.Metadata); err != nil {
				return nil, fmt.Errorf("unmarshal event metadata: %w", err)
			}
		}
		events = append(events, evt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return events, nil
}
