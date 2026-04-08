package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/eventstore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	eventsChannel    = "events_inserted"
	subscriberBuffer = 100
)

// PostgresEventStore implements eventstore.Store backed by PostgreSQL.
type PostgresEventStore struct {
	pool *pgxpool.Pool
	q    Querier
}

var _ eventstore.Store = (*PostgresEventStore)(nil)

// NewEventStore creates a new PostgresEventStore.
func NewEventStore(pool *pgxpool.Pool) *PostgresEventStore {
	return &PostgresEventStore{pool: pool, q: pool}
}

// WithTx returns a transaction-scoped EventStore instance sharing the given Tx.
// This enables ContractRepository.Save to share a transaction with EventStore.Append.
func (s *PostgresEventStore) WithTx(tx pgx.Tx) *PostgresEventStore {
	return &PostgresEventStore{pool: s.pool, q: tx}
}

func (s *PostgresEventStore) querier(ctx context.Context) Querier {
	if tx, ok := TxFromContext(ctx); ok {
		return tx
	}
	return s.q
}

// Append appends events to a stream with optimistic concurrency control.
// Uses the streams table for O(1) version check (PK row lock) instead of
// scanning the events table.
func (s *PostgresEventStore) Append(ctx context.Context, streamID string, events []eventstore.Event, expectedVersion int) error {
	q := s.querier(ctx)

	if expectedVersion == 0 {
		// New stream — insert into streams table.
		// Extract aggregate_type and aggregate_id from streamID ("contract-123" → "contract", "123").
		aggType, aggID := parseStreamID(streamID)
		_, err := q.Exec(ctx,
			`INSERT INTO streams (stream_id, aggregate_type, aggregate_id, current_version)
			 VALUES ($1, $2, $3, 0)
			 ON CONFLICT (stream_id) DO NOTHING`,
			streamID, aggType, aggID,
		)
		if err != nil {
			return fmt.Errorf("ensure stream: %w", err)
		}
	}

	// Lock the stream row and check version — O(1) PK lookup.
	var currentVersion int
	err := q.QueryRow(ctx,
		`SELECT current_version FROM streams WHERE stream_id = $1 FOR UPDATE`,
		streamID,
	).Scan(&currentVersion)
	if err != nil {
		if err == pgx.ErrNoRows {
			return fmt.Errorf("stream not found: %s", streamID)
		}
		return fmt.Errorf("lock stream: %w", err)
	}

	if currentVersion != expectedVersion {
		return eventstore.ErrConcurrencyConflict
	}

	newVersion := expectedVersion
	for i, evt := range events {
		data, err := json.Marshal(evt.Data)
		if err != nil {
			return fmt.Errorf("marshal event data: %w", err)
		}
		metadata, err := json.Marshal(evt.Metadata)
		if err != nil {
			return fmt.Errorf("marshal event metadata: %w", err)
		}
		newVersion = expectedVersion + i + 1
		_, err = q.Exec(ctx,
			`INSERT INTO events (id, stream_id, type, version, schema_version, data, metadata, occurred_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			evt.ID, streamID, evt.Type, newVersion, evt.SchemaVersion, data, metadata, evt.OccurredAt,
		)
		if err != nil {
			return fmt.Errorf("insert event: %w", err)
		}
	}

	// Advance the stream version.
	_, err = q.Exec(ctx,
		`UPDATE streams SET current_version = $1, updated_at = NOW() WHERE stream_id = $2`,
		newVersion, streamID,
	)
	if err != nil {
		return fmt.Errorf("update stream version: %w", err)
	}

	return nil
}

// parseStreamID splits "contract-abc123" into ("contract", "abc123").
func parseStreamID(streamID string) (aggregateType, aggregateID string) {
	for i := 0; i < len(streamID); i++ {
		if streamID[i] == '-' {
			return streamID[:i], streamID[i+1:]
		}
	}
	return streamID, streamID
}

// Load loads all events for a stream ordered by version.
func (s *PostgresEventStore) Load(ctx context.Context, streamID string) ([]eventstore.Event, error) {
	q := s.querier(ctx)

	rows, err := q.Query(ctx,
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

// LoadUntilVersion loads events for a stream up to (inclusive) the given version.
func (s *PostgresEventStore) LoadUntilVersion(ctx context.Context, streamID string, version int) ([]eventstore.Event, error) {
	q := s.querier(ctx)

	rows, err := q.Query(ctx,
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

// LoadUntil loads events for a stream up to the given timestamp.
func (s *PostgresEventStore) LoadUntil(ctx context.Context, streamID string, until time.Time) ([]eventstore.Event, error) {
	q := s.querier(ctx)

	rows, err := q.Query(ctx,
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

// LoadRange loads events for a stream within the given time range.
func (s *PostgresEventStore) LoadRange(ctx context.Context, streamID string, from, to time.Time) ([]eventstore.Event, error) {
	q := s.querier(ctx)

	rows, err := q.Query(ctx,
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

// Subscribe returns a channel that receives events from the given global position.
// Uses a hybrid LISTEN/NOTIFY + polling approach.
// Slow subscribers are dropped (matching InMemory semantics, buffer size 100).
func (s *PostgresEventStore) Subscribe(ctx context.Context, fromPosition int64) (<-chan eventstore.Event, error) {
	ch := make(chan eventstore.Event, subscriberBuffer)
	go s.runSubscription(ctx, fromPosition, ch)
	return ch, nil
}

// SaveSnapshot saves or upserts a snapshot for the given stream.
func (s *PostgresEventStore) SaveSnapshot(ctx context.Context, snapshot eventstore.Snapshot) error {
	q := s.querier(ctx)

	state, err := json.Marshal(snapshot.State)
	if err != nil {
		return fmt.Errorf("marshal snapshot state: %w", err)
	}

	_, err = q.Exec(ctx,
		`INSERT INTO snapshots (stream_id, version, state, as_of)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (stream_id, version)
		 DO UPDATE SET state = EXCLUDED.state, as_of = EXCLUDED.as_of`,
		snapshot.StreamID, snapshot.Version, state, snapshot.AsOf,
	)
	if err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}
	return nil
}

// LoadSnapshot loads the latest snapshot for a stream.
func (s *PostgresEventStore) LoadSnapshot(ctx context.Context, streamID string) (*eventstore.Snapshot, error) {
	q := s.querier(ctx)

	var snap eventstore.Snapshot
	var state json.RawMessage
	err := q.QueryRow(ctx,
		`SELECT stream_id, version, state, as_of, created_at
		 FROM snapshots
		 WHERE stream_id = $1
		 ORDER BY version DESC
		 LIMIT 1`,
		streamID,
	).Scan(&snap.StreamID, &snap.Version, &state, &snap.AsOf, &snap.CreatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("load snapshot: %w", err)
	}
	snap.State = state
	return &snap, nil
}

// LoadSnapshotBefore loads the latest snapshot for a stream that was taken before the given time.
func (s *PostgresEventStore) LoadSnapshotBefore(ctx context.Context, streamID string, before time.Time) (*eventstore.Snapshot, error) {
	q := s.querier(ctx)

	var snap eventstore.Snapshot
	var state json.RawMessage
	err := q.QueryRow(ctx,
		`SELECT stream_id, version, state, as_of, created_at
		 FROM snapshots
		 WHERE stream_id = $1 AND as_of < $2
		 ORDER BY version DESC
		 LIMIT 1`,
		streamID, before,
	).Scan(&snap.StreamID, &snap.Version, &state, &snap.AsOf, &snap.CreatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("load snapshot before: %w", err)
	}
	snap.State = state
	return &snap, nil
}

// --- internal: load all events from global position (used by projectors) ---

func (s *PostgresEventStore) loadAllFrom(ctx context.Context, fromPosition int64) ([]eventstore.Event, error) {
	q := s.querier(ctx)

	rows, err := q.Query(ctx,
		`SELECT id, stream_id, type, version, schema_version, data, metadata, occurred_at, recorded_at, global_position
		 FROM events
		 WHERE global_position > $1
		 ORDER BY global_position ASC`,
		fromPosition,
	)
	if err != nil {
		return nil, fmt.Errorf("load all events: %w", err)
	}
	defer rows.Close()

	return scanEvents(rows)
}

func (s *PostgresEventStore) loadAllUntil(ctx context.Context, until time.Time) ([]eventstore.Event, error) {
	q := s.querier(ctx)

	rows, err := q.Query(ctx,
		`SELECT id, stream_id, type, version, schema_version, data, metadata, occurred_at, recorded_at, global_position
		 FROM events
		 WHERE occurred_at <= $1
		 ORDER BY global_position ASC`,
		until,
	)
	if err != nil {
		return nil, fmt.Errorf("load all events until: %w", err)
	}
	defer rows.Close()

	return scanEvents(rows)
}

// --- helpers ---

func scanEvents(rows pgx.Rows) ([]eventstore.Event, error) {
	var events []eventstore.Event
	for rows.Next() {
		var evt eventstore.Event
		var data, metadata json.RawMessage
		err := rows.Scan(
			&evt.ID, &evt.StreamID, &evt.Type, &evt.Version, &evt.SchemaVersion,
			&data, &metadata, &evt.OccurredAt, &evt.RecordedAt, &evt.GlobalPosition,
		)
		if err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		evt.Data = data
		evt.Metadata = metadata
		events = append(events, evt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return events, nil
}

// --- subscription ---

func (s *PostgresEventStore) runSubscription(ctx context.Context, fromPosition int64, ch chan<- eventstore.Event) {
	defer close(ch)

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", eventsChannel))
	if err != nil {
		return
	}

	position := fromPosition

	// Initial catch-up
	if err := s.catchUp(ctx, &position, ch); err != nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}

		if err := s.catchUp(ctx, &position, ch); err != nil {
			return
		}
	}
}

func (s *PostgresEventStore) catchUp(ctx context.Context, position *int64, ch chan<- eventstore.Event) error {
	events, err := s.loadAllFrom(ctx, *position)
	if err != nil {
		return err
	}

	for _, evt := range events {
		select {
		case ch <- evt:
			*position = evt.GlobalPosition
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Slow subscriber — drop event (matching InMemory semantics)
			*position = evt.GlobalPosition
		}
	}
	return nil
}
