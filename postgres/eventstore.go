package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
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
func (s *PostgresEventStore) Append(ctx context.Context, streamID string, events []eventstore.Event, expectedVersion int) error {
	q := s.querier(ctx)

	var currentVersion int
	err := q.QueryRow(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = $1 FOR UPDATE`,
		streamID,
	).Scan(&currentVersion)
	if err != nil {
		return fmt.Errorf("check stream version: %w", err)
	}

	if currentVersion != expectedVersion {
		return eventstore.ErrConcurrencyConflict
	}

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
			evt.ID, streamID, evt.Type, version, evt.SchemaVersion, data, metadata, evt.OccurredAt,
		)
		if err != nil {
			return fmt.Errorf("insert event: %w", err)
		}
	}

	return nil
}

// Load loads all events for a stream ordered by version.
func (s *PostgresEventStore) Load(ctx context.Context, streamID string) ([]eventstore.Event, error) {
	return s.LoadFrom(ctx, streamID, 0)
}

// LoadFrom loads events for a stream starting from the given version (exclusive).
func (s *PostgresEventStore) LoadFrom(ctx context.Context, streamID string, fromVersion int) ([]eventstore.Event, error) {
	q := s.querier(ctx)

	rows, err := q.Query(ctx,
		`SELECT id, stream_id, type, version, schema_version, data, metadata, occurred_at, recorded_at, global_position
		 FROM events
		 WHERE stream_id = $1 AND version > $2
		 ORDER BY version ASC`,
		streamID, fromVersion,
	)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	return scanEvents(rows)
}

// LoadAll loads all events from the given global position (exclusive), used by projectors.
func (s *PostgresEventStore) LoadAll(ctx context.Context, fromPosition int64) ([]eventstore.Event, error) {
	q := s.querier(ctx)

	rows, err := q.Query(ctx,
		`SELECT id, stream_id, type, version, schema_version, data, metadata, occurred_at, recorded_at, global_position
		 FROM events
		 WHERE global_position > $1
		 ORDER BY global_position ASC`,
		fromPosition,
	)
	if err != nil {
		return nil, fmt.Errorf("query all events: %w", err)
	}
	defer rows.Close()

	return scanEvents(rows)
}

// Subscribe returns a Subscription that receives events from the given global position.
// Uses a hybrid LISTEN/NOTIFY + polling approach.
// Slow subscribers are dropped (buffer size 100) to match InMemory semantics.
func (s *PostgresEventStore) Subscribe(ctx context.Context, fromPosition int64) (eventstore.Subscription, error) {
	sub := &postgresSubscription{
		store:    s,
		ch:       make(chan eventstore.Event, subscriberBuffer),
		done:     make(chan struct{}),
		position: fromPosition,
	}

	go sub.run(ctx)
	return sub, nil
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

// DeleteSnapshot deletes all snapshots for a stream.
func (s *PostgresEventStore) DeleteSnapshot(ctx context.Context, streamID string) error {
	q := s.querier(ctx)

	_, err := q.Exec(ctx,
		`DELETE FROM snapshots WHERE stream_id = $1`,
		streamID,
	)
	if err != nil {
		return fmt.Errorf("delete snapshot: %w", err)
	}
	return nil
}

// GetStreamVersion returns the current version of a stream (0 if stream does not exist).
func (s *PostgresEventStore) GetStreamVersion(ctx context.Context, streamID string) (int, error) {
	q := s.querier(ctx)

	var version int
	err := q.QueryRow(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = $1`,
		streamID,
	).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("get stream version: %w", err)
	}
	return version, nil
}

// GetLastPosition returns the last global position across all events.
func (s *PostgresEventStore) GetLastPosition(ctx context.Context) (int64, error) {
	q := s.querier(ctx)

	var pos int64
	err := q.QueryRow(ctx,
		`SELECT COALESCE(MAX(global_position), 0) FROM events`,
	).Scan(&pos)
	if err != nil {
		return 0, fmt.Errorf("get last position: %w", err)
	}
	return pos, nil
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

type postgresSubscription struct {
	store    *PostgresEventStore
	ch       chan eventstore.Event
	done     chan struct{}
	position int64
	closeOnce sync.Once
	err      error
	mu       sync.Mutex
}

func (sub *postgresSubscription) Events() <-chan eventstore.Event {
	return sub.ch
}

func (sub *postgresSubscription) Close() error {
	sub.closeOnce.Do(func() {
		close(sub.done)
	})
	return nil
}

func (sub *postgresSubscription) Err() error {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	return sub.err
}

func (sub *postgresSubscription) setErr(err error) {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	sub.err = err
}

func (sub *postgresSubscription) run(ctx context.Context) {
	defer close(sub.ch)

	// Acquire a dedicated connection for LISTEN
	conn, err := sub.store.pool.Acquire(ctx)
	if err != nil {
		sub.setErr(fmt.Errorf("acquire conn for listen: %w", err))
		return
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", eventsChannel))
	if err != nil {
		sub.setErr(fmt.Errorf("listen: %w", err))
		return
	}

	// Initial catch-up
	if err := sub.catchUp(ctx); err != nil {
		sub.setErr(err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			sub.setErr(ctx.Err())
			return
		case <-sub.done:
			return
		default:
		}

		// Wait for notification with timeout for periodic polling
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				sub.setErr(ctx.Err())
				return
			}
			// On error, fall through to poll-based catch-up
			time.Sleep(100 * time.Millisecond)
		}
		_ = notification

		if err := sub.catchUp(ctx); err != nil {
			sub.setErr(err)
			return
		}
	}
}

func (sub *postgresSubscription) catchUp(ctx context.Context) error {
	events, err := sub.store.LoadAll(ctx, sub.position)
	if err != nil {
		return fmt.Errorf("catch-up load: %w", err)
	}

	for _, evt := range events {
		select {
		case sub.ch <- evt:
			sub.position = evt.GlobalPosition
		case <-ctx.Done():
			return ctx.Err()
		case <-sub.done:
			return nil
		default:
			// Slow subscriber — drop (matching InMemory semantics)
			sub.position = evt.GlobalPosition
		}
	}
	return nil
}
