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
	// eventsChannel is the LISTEN/NOTIFY channel name used by Subscribe.
	eventsChannel = "events_inserted"

	// subscriberBuffer sizes the per-subscriber channel. If a subscriber
	// cannot keep up, catchUp will BLOCK on the send instead of dropping
	// events. Silent event loss is unacceptable for a billing system.
	subscriberBuffer = 100

	// pgUniqueViolation is the SQLSTATE code PostgreSQL returns when a
	// UNIQUE constraint is violated. We translate this specific error on
	// INSERT into the events table into tx.ErrVersionConflict.
	pgUniqueViolation = "23505"
)

// PostgresEventStore implements eventstore.Store on top of a pgxpool.Pool.
//
// It always reads the current transaction from the context (via
// QuerierFromContext) so that Append/Load calls made inside RunInTx
// participate in the same transaction as the surrounding repository writes.
type PostgresEventStore struct {
	pool *pgxpool.Pool
}

var _ eventstore.Store = (*PostgresEventStore)(nil)

// NewEventStore constructs a PostgresEventStore.
func NewEventStore(pool *pgxpool.Pool) *PostgresEventStore {
	return &PostgresEventStore{pool: pool}
}

func (s *PostgresEventStore) q(ctx context.Context) Querier {
	return QuerierFromContext(ctx, s.pool)
}

// Append appends events to streamID with optimistic concurrency control.
//
// The caller passes expectedVersion: the version the caller believes the
// stream to currently be at. If another transaction has meanwhile written
// events for the same stream, the UNIQUE(stream_id, version) constraint
// fires on INSERT and we return tx.ErrVersionConflict, which
// tx.RetryOnConflict knows to retry.
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

// isVersionConflict reports whether err is a PostgreSQL unique-violation
// on the events table, which for our purposes always means a concurrent
// append raced us to the same (stream_id, version) pair.
func isVersionConflict(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgUniqueViolation
	}
	return false
}

// Load returns every event for streamID in version order.
func (s *PostgresEventStore) Load(ctx context.Context, streamID string) ([]eventstore.Event, error) {
	rows, err := s.q(ctx).Query(ctx,
		`SELECT id, stream_id, type, version, schema_version, data, metadata, occurred_at, recorded_at, global_position
		 FROM events
		 WHERE stream_id = $1
		 ORDER BY version ASC`,
		streamID,
	)
	if err != nil {
		return nil, fmt.Errorf("load events: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

// LoadUntilVersion returns events whose version is <= version.
func (s *PostgresEventStore) LoadUntilVersion(ctx context.Context, streamID string, version int) ([]eventstore.Event, error) {
	rows, err := s.q(ctx).Query(ctx,
		`SELECT id, stream_id, type, version, schema_version, data, metadata, occurred_at, recorded_at, global_position
		 FROM events
		 WHERE stream_id = $1 AND version <= $2
		 ORDER BY version ASC`,
		streamID, version,
	)
	if err != nil {
		return nil, fmt.Errorf("load events until version: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

// LoadUntil returns events whose occurred_at is <= until.
func (s *PostgresEventStore) LoadUntil(ctx context.Context, streamID string, until time.Time) ([]eventstore.Event, error) {
	rows, err := s.q(ctx).Query(ctx,
		`SELECT id, stream_id, type, version, schema_version, data, metadata, occurred_at, recorded_at, global_position
		 FROM events
		 WHERE stream_id = $1 AND occurred_at <= $2
		 ORDER BY version ASC`,
		streamID, until,
	)
	if err != nil {
		return nil, fmt.Errorf("load events until time: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

// LoadRange returns events whose occurred_at falls within [from, to].
func (s *PostgresEventStore) LoadRange(ctx context.Context, streamID string, from, to time.Time) ([]eventstore.Event, error) {
	rows, err := s.q(ctx).Query(ctx,
		`SELECT id, stream_id, type, version, schema_version, data, metadata, occurred_at, recorded_at, global_position
		 FROM events
		 WHERE stream_id = $1 AND occurred_at >= $2 AND occurred_at <= $3
		 ORDER BY version ASC`,
		streamID, from, to,
	)
	if err != nil {
		return nil, fmt.Errorf("load events in range: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

// LoadAll returns events across all streams ordered by global_position.
// fromPosition is exclusive; a limit of <= 0 means unlimited.
func (s *PostgresEventStore) LoadAll(ctx context.Context, fromPosition int64, limit int) ([]eventstore.Event, error) {
	var (
		rows pgx.Rows
		err  error
	)
	if limit > 0 {
		rows, err = s.q(ctx).Query(ctx,
			`SELECT id, stream_id, type, version, schema_version, data, metadata, occurred_at, recorded_at, global_position
			 FROM events
			 WHERE global_position > $1
			 ORDER BY global_position ASC
			 LIMIT $2`,
			fromPosition, limit,
		)
	} else {
		rows, err = s.q(ctx).Query(ctx,
			`SELECT id, stream_id, type, version, schema_version, data, metadata, occurred_at, recorded_at, global_position
			 FROM events
			 WHERE global_position > $1
			 ORDER BY global_position ASC`,
			fromPosition,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("load all events: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

// Subscribe returns a channel that first drains existing events past
// fromPosition (inclusive of "catch up from the beginning" when
// fromPosition == 0) and then delivers new events as they are notified.
//
// The channel is closed when ctx is cancelled or when the underlying
// connection dies. Sends on the channel BLOCK when the consumer is slow;
// we never drop events.
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
		// UNLISTEN before returning the connection to the pool so that
		// a later reuse of the connection does not receive stale
		// notifications targeted at a previous subscriber.
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
			// Transient wait error: sleep briefly and try again so we
			// don't spin on a broken connection.
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

// catchUp drains every event with global_position > *position to the channel
// and updates *position in-place. Blocks on the channel send.
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

// SaveSnapshot upserts a snapshot for (stream_id, version).
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
		snapshot.StreamID, snapshot.Version, state, snapshot.AsOf,
	)
	if err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}
	return nil
}

// LoadSnapshot returns the latest snapshot for streamID, or (nil, nil) if
// no snapshot has ever been saved for the stream.
func (s *PostgresEventStore) LoadSnapshot(ctx context.Context, streamID string) (*eventstore.Snapshot, error) {
	return s.loadSnapshotRow(ctx,
		`SELECT stream_id, version, state, as_of, created_at
		 FROM snapshots
		 WHERE stream_id = $1
		 ORDER BY version DESC
		 LIMIT 1`,
		streamID,
	)
}

// LoadSnapshotBefore returns the latest snapshot for streamID taken strictly
// before the given time, or (nil, nil) if none qualifies.
func (s *PostgresEventStore) LoadSnapshotBefore(ctx context.Context, streamID string, before time.Time) (*eventstore.Snapshot, error) {
	return s.loadSnapshotRow(ctx,
		`SELECT stream_id, version, state, as_of, created_at
		 FROM snapshots
		 WHERE stream_id = $1 AND as_of < $2
		 ORDER BY version DESC
		 LIMIT 1`,
		streamID, before,
	)
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

// scanEvents turns rows into []eventstore.Event, unmarshalling the metadata
// JSONB column into the typed EventMetadata struct.
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
