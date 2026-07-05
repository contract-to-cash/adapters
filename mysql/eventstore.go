// Package mysql provides MySQL 8.0 implementations of the contract-to-cash
// core persistence interfaces. See schema.sql for the required DDL.
package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/contract-to-cash/core/domain/shared"
	"github.com/contract-to-cash/core/eventstore"
)

// eventColumns is the ordered column list shared by every event SELECT/scan.
const eventColumns = "id, stream_id, type, version, schema_version, data, metadata, occurred_at, recorded_at, global_position"

const insertEventSQL = "INSERT INTO events " +
	"(id, stream_id, type, version, schema_version, data, metadata, occurred_at, recorded_at) " +
	"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"

const upsertSnapshotSQL = "INSERT INTO snapshots " +
	"(stream_id, version, state, as_of, created_at) VALUES (?, ?, ?, ?, ?) " +
	"ON DUPLICATE KEY UPDATE state = VALUES(state), as_of = VALUES(as_of), created_at = VALUES(created_at)"

// EventStore is a MySQL-backed eventstore.Store.
//
// Optimistic concurrency is enforced two ways that reinforce each other:
//   - Append checks the current stream length against expectedVersion, and
//   - the UNIQUE (stream_id, version) constraint rejects a racing append that
//     passed the length check, surfaced as a version conflict.
type EventStore struct {
	db    *sql.DB
	clock shared.Clock

	pollInterval    time.Duration
	pollBatchSize   int
	subscribeBuffer int
}

// Option configures an EventStore.
type Option func(*EventStore)

// WithPollInterval sets the Subscribe polling cadence (default 1s).
func WithPollInterval(d time.Duration) Option {
	return func(s *EventStore) {
		if d > 0 {
			s.pollInterval = d
		}
	}
}

// WithSubscribeBuffer sets the buffered size of the Subscribe channel and the
// per-poll batch size (default 256).
func WithSubscribeBuffer(n int) Option {
	return func(s *EventStore) {
		if n > 0 {
			s.subscribeBuffer = n
			s.pollBatchSize = n
		}
	}
}

// New constructs a MySQL EventStore over an existing *sql.DB.
//
// The DSN should set loc=UTC (e.g. ".../db?loc=UTC&parseTime=true"); all
// timestamps are stored and returned in UTC. The adapter tolerates either
// parseTime setting when reading DATETIME columns.
func New(db *sql.DB, clock shared.Clock, opts ...Option) *EventStore {
	s := &EventStore{
		db:              db,
		clock:           clock,
		pollInterval:    time.Second,
		pollBatchSize:   256,
		subscribeBuffer: 256,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

var _ eventstore.Store = (*EventStore)(nil)

// q returns the ambient *sql.Tx if one is embedded in ctx, otherwise the pool.
func (s *EventStore) q(ctx context.Context) Querier {
	return querierFromContext(ctx, s.db)
}

// Append persists events to a stream under optimistic concurrency control.
//
// If an ambient transaction is present in ctx (see ContextWithTx) the append
// runs directly on it — the caller owns the begin/commit. Otherwise the store
// wraps the COUNT check and inserts in its own transaction so the batch is
// atomic.
func (s *EventStore) Append(ctx context.Context, streamID string, events []eventstore.Event, expectedVersion int) error {
	if len(events) == 0 {
		return nil
	}

	if _, ok := TxFromContext(ctx); ok {
		// Ambient transaction: run on it directly, no begin/commit here.
		return s.appendOn(ctx, s.q(ctx), streamID, events, expectedVersion)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("event store: begin tx: %w", err)
	}
	if err := s.appendOn(ctx, tx, streamID, events, expectedVersion); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("event store: commit: %w", err)
	}
	return nil
}

// appendOn performs the COUNT-based optimistic check and inserts on the given
// Querier (either an ambient *sql.Tx or the store's own tx).
//
// The stored version is derived server-side as expectedVersion+i+1 (and the
// stream id from the streamID argument), matching the postgres adapter. The
// caller-populated Event.Version / Event.StreamID fields are ignored so a
// stale or inconsistent caller value cannot diverge from the optimistic
// concurrency baseline.
func (s *EventStore) appendOn(ctx context.Context, q Querier, streamID string, events []eventstore.Event, expectedVersion int) error {
	var current int
	if err := q.QueryRowContext(ctx, "SELECT COUNT(*) FROM events WHERE stream_id = ?", streamID).Scan(&current); err != nil {
		return fmt.Errorf("event store: count events: %w", err)
	}
	if current != expectedVersion {
		return versionConflict(streamID, fmt.Errorf("expected version %d but stream is at %d", expectedVersion, current))
	}

	recordedAt := s.clock.Now().UTC()
	for i := range events {
		e := events[i]
		meta, err := json.Marshal(e.Metadata)
		if err != nil {
			return fmt.Errorf("event store: marshal metadata for event %s: %w", e.ID, err)
		}
		version := expectedVersion + i + 1
		if _, err := q.ExecContext(ctx, insertEventSQL,
			e.ID, streamID, string(e.Type), version, e.SchemaVersion,
			normalizeJSON(e.Data), meta, e.OccurredAt.UTC(), recordedAt,
		); err != nil {
			// A 1062 duplicate-key can mean two very different things. Only a
			// clash on uq_stream_version is a retryable optimistic-concurrency
			// conflict; a clash on uq_event_id is a duplicate event ID (a
			// caller/infra bug) and must NOT be reported as a version conflict,
			// otherwise callers would retry an append that can never succeed.
			if isDuplicateEventID(err) {
				return duplicateEventID(e.ID, err)
			}
			if isStreamVersionConflict(err) {
				return versionConflict(streamID, err)
			}
			return fmt.Errorf("event store: insert event %s: %w", e.ID, err)
		}
	}
	return nil
}

// Load returns all events for a stream ordered by version ascending.
func (s *EventStore) Load(ctx context.Context, streamID string) ([]eventstore.Event, error) {
	rows, err := s.q(ctx).QueryContext(ctx,
		"SELECT "+eventColumns+" FROM events WHERE stream_id = ? ORDER BY version ASC", streamID)
	if err != nil {
		return nil, fmt.Errorf("event store: load stream %s: %w", streamID, err)
	}
	return scanEvents(rows)
}

// LoadUntilVersion returns events with version <= the given version.
func (s *EventStore) LoadUntilVersion(ctx context.Context, streamID string, version int) ([]eventstore.Event, error) {
	rows, err := s.q(ctx).QueryContext(ctx,
		"SELECT "+eventColumns+" FROM events WHERE stream_id = ? AND version <= ? ORDER BY version ASC",
		streamID, version)
	if err != nil {
		return nil, fmt.Errorf("event store: load until version: %w", err)
	}
	return scanEvents(rows)
}

// LoadUntil returns events with occurred_at <= until.
func (s *EventStore) LoadUntil(ctx context.Context, streamID string, until time.Time) ([]eventstore.Event, error) {
	rows, err := s.q(ctx).QueryContext(ctx,
		"SELECT "+eventColumns+" FROM events WHERE stream_id = ? AND occurred_at <= ? ORDER BY version ASC",
		streamID, until.UTC())
	if err != nil {
		return nil, fmt.Errorf("event store: load until time: %w", err)
	}
	return scanEvents(rows)
}

// LoadRange returns events with from <= occurred_at < to.
func (s *EventStore) LoadRange(ctx context.Context, streamID string, from, to time.Time) ([]eventstore.Event, error) {
	rows, err := s.q(ctx).QueryContext(ctx,
		"SELECT "+eventColumns+" FROM events WHERE stream_id = ? AND occurred_at >= ? AND occurred_at < ? ORDER BY version ASC",
		streamID, from.UTC(), to.UTC())
	if err != nil {
		return nil, fmt.Errorf("event store: load range: %w", err)
	}
	return scanEvents(rows)
}

// LoadAll loads events across all streams ordered by global position.
// fromPosition is exclusive; limit <= 0 means no limit.
func (s *EventStore) LoadAll(ctx context.Context, fromPosition int64, limit int) ([]eventstore.Event, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if limit > 0 {
		rows, err = s.q(ctx).QueryContext(ctx,
			"SELECT "+eventColumns+" FROM events WHERE global_position > ? ORDER BY global_position ASC LIMIT ?",
			fromPosition, limit)
	} else {
		rows, err = s.q(ctx).QueryContext(ctx,
			"SELECT "+eventColumns+" FROM events WHERE global_position > ? ORDER BY global_position ASC",
			fromPosition)
	}
	if err != nil {
		return nil, fmt.Errorf("event store: load all: %w", err)
	}
	return scanEvents(rows)
}

// Subscribe returns a channel that replays events after fromPosition and then
// tails newly appended events. Unlike the in-memory reference store this honours
// fromPosition (replay-then-tail) and is lossless up to DB durability: delivery
// is backed by polling LoadAll, so a slow consumer back-pressures the poller
// rather than dropping events. The channel is closed when ctx is cancelled.
func (s *EventStore) Subscribe(ctx context.Context, fromPosition int64) (<-chan eventstore.Event, error) {
	ch := make(chan eventstore.Event, s.subscribeBuffer)
	go s.pollLoop(ctx, fromPosition, ch)
	return ch, nil
}

func (s *EventStore) pollLoop(ctx context.Context, pos int64, ch chan<- eventstore.Event) {
	defer close(ch)
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		batch, err := s.LoadAll(ctx, pos, s.pollBatchSize)
		if err == nil {
			for _, e := range batch {
				select {
				case ch <- e:
					pos = e.GlobalPosition
				case <-ctx.Done():
					return
				}
			}
			// A full batch likely means more events are waiting; drain
			// immediately instead of sleeping a full interval.
			if len(batch) == s.pollBatchSize {
				continue
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// SaveSnapshot upserts an aggregate snapshot keyed by (stream_id, version).
// A zero CreatedAt (caller did not stamp it) is filled from the store clock,
// matching the postgres adapter's COALESCE(..., NOW()) fallback —
// LoadSnapshotBefore filters on created_at, so a persisted zero value would
// make the snapshot invisible to every temporal query.
func (s *EventStore) SaveSnapshot(ctx context.Context, snapshot eventstore.Snapshot) error {
	createdAt := snapshot.CreatedAt
	if createdAt.IsZero() {
		createdAt = s.clock.Now()
	}
	if _, err := s.q(ctx).ExecContext(ctx, upsertSnapshotSQL,
		snapshot.StreamID, snapshot.Version, normalizeJSON(snapshot.State),
		snapshot.AsOf.UTC(), createdAt.UTC(),
	); err != nil {
		return fmt.Errorf("event store: save snapshot for stream %s: %w", snapshot.StreamID, err)
	}
	return nil
}

// LoadSnapshot loads the latest snapshot for a stream, or (nil, nil) if none.
func (s *EventStore) LoadSnapshot(ctx context.Context, streamID string) (*eventstore.Snapshot, error) {
	row := s.q(ctx).QueryRowContext(ctx,
		"SELECT stream_id, version, state, as_of, created_at FROM snapshots WHERE stream_id = ? ORDER BY version DESC LIMIT 1",
		streamID)
	return scanSnapshot(row)
}

// LoadSnapshotBefore loads the latest snapshot created before the given time,
// or (nil, nil) if none. version DESC breaks ties between snapshots created
// at the same instant deterministically (highest wins).
func (s *EventStore) LoadSnapshotBefore(ctx context.Context, streamID string, before time.Time) (*eventstore.Snapshot, error) {
	row := s.q(ctx).QueryRowContext(ctx,
		"SELECT stream_id, version, state, as_of, created_at FROM snapshots WHERE stream_id = ? AND created_at < ? ORDER BY created_at DESC, version DESC LIMIT 1",
		streamID, before.UTC())
	return scanSnapshot(row)
}

func scanEvents(rows *sql.Rows) ([]eventstore.Event, error) {
	defer func() { _ = rows.Close() }()

	var out []eventstore.Event
	for rows.Next() {
		var (
			e        eventstore.Event
			typ      string
			data     []byte
			meta     []byte
			occurred utcTime
			recorded utcTime
			gp       int64
		)
		if err := rows.Scan(&e.ID, &e.StreamID, &typ, &e.Version, &e.SchemaVersion,
			&data, &meta, &occurred, &recorded, &gp); err != nil {
			return nil, fmt.Errorf("event store: scan event: %w", err)
		}
		e.Type = eventstore.EventType(typ)
		// Copy: database/sql may reuse the scan buffer across rows.
		e.Data = append(json.RawMessage(nil), data...)
		if len(meta) > 0 {
			if err := json.Unmarshal(meta, &e.Metadata); err != nil {
				return nil, fmt.Errorf("event store: unmarshal metadata for event %s: %w", e.ID, err)
			}
		}
		e.OccurredAt = occurred.Time
		e.RecordedAt = recorded.Time
		e.GlobalPosition = gp
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("event store: iterate events: %w", err)
	}
	return out, nil
}

func scanSnapshot(row *sql.Row) (*eventstore.Snapshot, error) {
	var (
		snap      eventstore.Snapshot
		state     []byte
		asOf      utcTime
		createdAt utcTime
	)
	if err := row.Scan(&snap.StreamID, &snap.Version, &state, &asOf, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("event store: scan snapshot: %w", err)
	}
	snap.State = append(json.RawMessage(nil), state...)
	snap.AsOf = asOf.Time
	snap.CreatedAt = createdAt.Time
	return &snap, nil
}

// normalizeJSON guarantees a non-empty value for JSON NOT NULL columns. An empty
// or nil payload (the core's Event.Data/Snapshot.State carry no non-empty
// guarantee) would otherwise be rejected by MySQL as invalid JSON.
func normalizeJSON(b json.RawMessage) []byte {
	if len(b) == 0 {
		return []byte("{}")
	}
	return []byte(b)
}

// utcTime scans a MySQL DATETIME into a UTC time.Time regardless of the driver's
// parseTime setting: with parseTime=true the driver yields a time.Time (expected
// to be UTC when the DSN sets loc=UTC); without it the driver yields the raw
// textual form, which we parse explicitly in UTC. Either way the result is UTC.
type utcTime struct{ Time time.Time }

func (u *utcTime) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		u.Time = time.Time{}
	case time.Time:
		u.Time = v.UTC()
	case []byte:
		return u.parse(string(v))
	case string:
		return u.parse(v)
	default:
		return fmt.Errorf("event store: cannot scan %T into time", src)
	}
	return nil
}

func (u *utcTime) parse(s string) error {
	if s == "" {
		u.Time = time.Time{}
		return nil
	}
	for _, layout := range []string{"2006-01-02 15:04:05.999999", "2006-01-02 15:04:05"} {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			u.Time = t
			return nil
		}
	}
	return fmt.Errorf("event store: cannot parse datetime %q", s)
}
