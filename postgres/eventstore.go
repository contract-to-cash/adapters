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
	eventsChannel       = "events_inserted"
	subscriberBuffer    = 100
	pgUniqueViolation   = "23505"
	defaultCatchUpBatch = 1000

	// eventAppendLockKey is the fixed transaction-level advisory-lock key that
	// serializes every event append against every other (issue #60). It is an
	// arbitrary but stable constant (the ASCII bytes of "events") chosen to be
	// distinctive so it is unlikely to collide with an advisory-lock key an
	// integrator uses elsewhere in the same database; if you do use
	// pg_advisory_xact_lock yourself, avoid this value.
	eventAppendLockKey int64 = 0x6576656e7473 // "events"
)

// PostgresEventStore implements eventstore.Store.
type PostgresEventStore struct {
	pool *pgxpool.Pool

	// onSubErr, when set, is invoked once if a Subscribe goroutine terminates
	// because of an error rather than context cancellation. The core
	// eventstore.Store interface cannot signal this (Subscribe returns a bare
	// channel that is closed on both success and failure), so this optional
	// callback lets integrators distinguish a clean shutdown from a broken
	// subscription. It is called from the subscription goroutine; keep it
	// non-blocking (e.g. log, increment a metric, push to a buffered channel).
	onSubErr func(error)

	// catchUpBatch bounds the LoadAll page size used while replaying/tailing so
	// a large backlog is streamed in chunks instead of loaded unbounded.
	catchUpBatch int
}

var _ eventstore.Store = (*PostgresEventStore)(nil)

// EventStoreOption configures a PostgresEventStore.
type EventStoreOption func(*PostgresEventStore)

// WithSubscriptionErrorHandler registers a callback invoked when a Subscribe
// goroutine ends abnormally (acquire/LISTEN/catch-up failure), before its
// channel is closed. Context cancellation is a normal shutdown and does not
// trigger it.
func WithSubscriptionErrorHandler(fn func(error)) EventStoreOption {
	return func(s *PostgresEventStore) { s.onSubErr = fn }
}

// WithCatchUpBatchSize sets the LoadAll page size used by Subscribe's
// replay/tail loop (default 1000). Values <= 0 are ignored.
func WithCatchUpBatchSize(n int) EventStoreOption {
	return func(s *PostgresEventStore) {
		if n > 0 {
			s.catchUpBatch = n
		}
	}
}

func NewEventStore(pool *pgxpool.Pool, opts ...EventStoreOption) *PostgresEventStore {
	s := &PostgresEventStore{pool: pool, catchUpBatch: defaultCatchUpBatch}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *PostgresEventStore) q(ctx context.Context) Querier {
	return QuerierFromContext(ctx, s.pool)
}

// Append persists events to a stream under optimistic concurrency control.
//
// Timestamp precision (issue #64): core stamps Event.OccurredAt with
// nanosecond-precision time.Now() (indirectly, via shared.Clock). The events
// table's occurred_at column is TIMESTAMPTZ (migration 001), which PostgreSQL
// stores at microsecond precision, so a round trip through Append/Load
// truncates any sub-microsecond component of OccurredAt. A boundary
// comparison (e.g. LoadUntil(ctx, streamID, until)) that relies on
// nanosecond-exact ordering between two events stamped in the same
// microsecond can therefore behave differently than against the in-memory
// reference store (infrastructure/inmemory/event_store.go), which keeps
// OccurredAt at full Go time.Time precision. This is a storage characteristic,
// not a bug: callers needing sub-microsecond ordering guarantees should not
// rely on OccurredAt equality/ordering at that resolution.
//
// RecordedAt provenance (issue #64): unlike the mysql adapter (and the
// in-memory reference store), which stamp RecordedAt from the injected
// shared.Clock, this adapter does NOT set RecordedAt in the INSERT below —
// the events.recorded_at column's DEFAULT NOW() (migration 001) populates it
// from the database server's clock. RecordedAt therefore reflects DB time,
// not the store's caller-supplied clock. This is an intentional adapter
// divergence, not a bug: no behavior change is planned here.
func (s *PostgresEventStore) Append(ctx context.Context, streamID string, events []eventstore.Event, expectedVersion int) error {
	if len(events) == 0 {
		return nil
	}

	// If there is no ambient transaction, wrap the entire append in one so
	// that a multi-event batch is atomic.
	if _, ok := TxFromContext(ctx); !ok {
		pgxTx, err := s.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin append tx: %w", err)
		}
		txCtx := ContextWithTx(ctx, pgxTx)
		if err := s.appendInTx(txCtx, streamID, events, expectedVersion); err != nil {
			_ = pgxTx.Rollback(ctx)
			return err
		}
		if err := pgxTx.Commit(ctx); err != nil {
			return fmt.Errorf("commit append tx: %w", err)
		}
		return nil
	}

	return s.appendInTx(ctx, streamID, events, expectedVersion)
}

func (s *PostgresEventStore) appendInTx(ctx context.Context, streamID string, events []eventstore.Event, expectedVersion int) error {
	q := s.q(ctx)

	// Serialize all appends against each other for the lifetime of this
	// transaction (issue #60). global_position (BIGSERIAL) is assigned at INSERT
	// time but only becomes visible at COMMIT time, and commits are NOT ordered
	// by position: without this lock, tx A could grab positions 100-101 while
	// tx B grabs 102 and commits first. A subscriber's catch-up
	// (`WHERE global_position > pos ORDER BY global_position`) would then read
	// 102, durably advance its checkpoint past it, and never see 100-101 once A
	// commits — permanent, silent event loss in subscriptions/projections.
	//
	// pg_advisory_xact_lock is held until the transaction ends (commit or
	// rollback), so it makes the [assign position, commit] windows of concurrent
	// appends non-overlapping: the holder assigns the lower positions AND commits
	// them before the next append can assign any. Commit order therefore equals
	// position order, which is exactly the gap-free visibility the reader side
	// (Subscribe/LoadAll) relies on — no reader-side change is needed. Aggregate
	// rehydration (per-stream, version-ordered) never depended on this and is
	// unaffected. In the ambient-transaction case the lock is held until the
	// caller commits, which also serializes appends against that wider unit.
	if _, err := q.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, eventAppendLockKey); err != nil {
		return fmt.Errorf("acquire append lock: %w", err)
	}

	// Optimistic-concurrency pre-check (same transaction as the inserts,
	// mirroring the mysql adapter): the UNIQUE (stream_id, version)
	// constraint alone only catches expectedVersion <= current. An
	// expectedVersion AHEAD of the stream would otherwise insert events at
	// versions current+gap+1..., silently leaving a hole in the version
	// sequence. A racing append that passes this check is still caught by
	// the UNIQUE constraint below.
	var current int
	if err := q.QueryRow(ctx,
		`SELECT COUNT(*) FROM events WHERE stream_id = $1`, streamID).Scan(&current); err != nil {
		return fmt.Errorf("count events: %w", err)
	}
	if current != expectedVersion {
		return shared.NewDomainError(
			shared.ErrCodeVersionConflict,
			fmt.Sprintf("stream %q expected version %d but stream is at %d", streamID, expectedVersion, current),
		)
	}

	// Validate Version contiguity: appended events must be numbered
	// sequentially starting at expectedVersion+1, matching the in-memory
	// reference store (infrastructure/inmemory/event_store.go). A gap or
	// out-of-order Version means the events were built against a stale
	// aggregate version and would corrupt the append-only log (breaking
	// optimistic locking and temporal replay), so reject the batch rather
	// than silently renumber and persist a different version than the
	// caller stamped.
	for i := range events {
		wantVersion := expectedVersion + i + 1
		if events[i].Version != wantVersion {
			return shared.NewDomainError(shared.ErrCodeValidation,
				fmt.Sprintf("event %d for stream %q has version %d but expected contiguous version %d",
					i, streamID, events[i].Version, wantVersion))
		}
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
			if isContractIdempotencyKeyConflict(err) {
				// A different contract.created event carried an idempotency key
				// already used by another contract (migration 010's partial
				// unique index ux_contract_idempotency_key). Per
				// contract.Repository.Save's godoc this is a creation conflict,
				// not a version conflict: the retried caller should look up the
				// existing contract instead of retrying. Mirrors the payments
				// #35 idempotency-conflict translation.
				return shared.NewDomainErrorWithCause(
					shared.ErrCodeConflict,
					fmt.Sprintf("contract creation idempotency key conflict for stream %q", streamID),
					err,
				)
			}
			return fmt.Errorf("insert event: %w", err)
		}
	}
	return nil
}

// isVersionConflict reports whether err is a unique-violation specifically
// on the (stream_id, version) constraint. A PK violation (duplicate event
// ID) is NOT a version conflict and must be reported as an infra error.
func isVersionConflict(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgUniqueViolation &&
			pgErr.ConstraintName == "events_stream_id_version_key"
	}
	return false
}

// contractIdempotencyKeyConstraint is the name of the partial unique expression
// index created by migration 010 over the contract.created event's
// data->>'idempotency_key'. A unique-violation carries the index name in
// ConstraintName, so matching on it — not the bare 23505 — keeps an unrelated
// unique violation from being misreported as an idempotency conflict (same
// approach as isVersionConflict / the payments idempotency constraint).
const contractIdempotencyKeyConstraint = "ux_contract_idempotency_key"

// isContractIdempotencyKeyConflict reports whether err is a unique-violation
// (23505) specifically on the contract idempotency-key index.
func isContractIdempotencyKeyConflict(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgUniqueViolation &&
			pgErr.ConstraintName == contractIdempotencyKeyConstraint
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

// LoadRange returns events with from <= occurred_at < to (half-open interval,
// matching the core reference implementation in infrastructure/inmemory).
func (s *PostgresEventStore) LoadRange(ctx context.Context, streamID string, from, to time.Time) ([]eventstore.Event, error) {
	rows, err := s.q(ctx).Query(ctx,
		`SELECT id, stream_id, type, version, schema_version, data, metadata, occurred_at, recorded_at, global_position
		 FROM events WHERE stream_id = $1 AND occurred_at >= $2 AND occurred_at < $3 ORDER BY version ASC`,
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

// Subscribe honours the core eventstore.Store.Subscribe contract (core#192):
// it first backfills every event strictly after fromPosition (via catchUp,
// which pages through LoadAll so replay memory stays bounded), then tails newly
// appended events driven by LISTEN/NOTIFY on eventsChannel. Delivery is lossless
// up to DB durability and at-least-once: a slow consumer back-pressures the
// sender (the channel send blocks rather than dropping), and catch-up is keyed
// off the last delivered global_position so a NOTIFY that races an in-flight
// catch-up cannot skip an event. Losslessness across concurrent appends further
// depends on Append serializing position assignment and commit under
// pg_advisory_xact_lock (issue #60): that makes commit order equal position
// order, so advancing past position N here can never hide an as-yet-uncommitted
// event at a lower position. The channel is closed when ctx is cancelled
// (runSubscription returns on ctx.Err() / ctx.Done() and defers close(ch)); an
// abnormal termination is surfaced via the optional subscription error handler
// (WithSubscriptionErrorHandler), since the bare channel return cannot.
func (s *PostgresEventStore) Subscribe(ctx context.Context, fromPosition int64) (<-chan eventstore.Event, error) {
	ch := make(chan eventstore.Event, subscriberBuffer)
	go s.runSubscription(ctx, fromPosition, ch)
	return ch, nil
}

func (s *PostgresEventStore) runSubscription(ctx context.Context, fromPosition int64, ch chan<- eventstore.Event) {
	defer close(ch)

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		s.reportSubErr(fmt.Errorf("subscribe: acquire connection: %w", err))
		return
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), fmt.Sprintf("UNLISTEN %s", eventsChannel))
		conn.Release()
	}()

	if _, err := conn.Exec(ctx, fmt.Sprintf("LISTEN %s", eventsChannel)); err != nil {
		s.reportSubErr(fmt.Errorf("subscribe: LISTEN: %w", err))
		return
	}

	position := fromPosition
	if err := s.catchUp(ctx, &position, ch); err != nil {
		s.reportSubErr(fmt.Errorf("subscribe: initial catch-up: %w", err))
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
			s.reportSubErr(fmt.Errorf("subscribe: catch-up: %w", err))
			return
		}
	}
}

// reportSubErr forwards an abnormal subscription termination to the registered
// handler. Context cancellation/deadline is a normal shutdown and is dropped so
// integrators are not paged for a graceful close.
func (s *PostgresEventStore) reportSubErr(err error) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	if s.onSubErr != nil {
		s.onSubErr(err)
	}
}

// catchUp streams every event after *position to ch in bounded pages. A large
// backlog is drained catchUpBatch events at a time (rather than one unbounded
// LoadAll) so replay memory stays proportional to the batch, not the log.
func (s *PostgresEventStore) catchUp(ctx context.Context, position *int64, ch chan<- eventstore.Event) error {
	for {
		events, err := s.LoadAll(ctx, *position, s.catchUpBatch)
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
		if len(events) < s.catchUpBatch {
			return nil
		}
	}
}

func (s *PostgresEventStore) SaveSnapshot(ctx context.Context, snapshot eventstore.Snapshot) error {
	state, err := json.Marshal(snapshot.State)
	if err != nil {
		return fmt.Errorf("marshal snapshot state: %w", err)
	}
	// Persist the caller-provided CreatedAt (core's SnapshotService stamps it
	// via shared.Clock); LoadSnapshotBefore filters on it. Fall back to the
	// database clock when the caller left it zero.
	var createdAt any
	if !snapshot.CreatedAt.IsZero() {
		createdAt = snapshot.CreatedAt
	}
	_, err = s.q(ctx).Exec(ctx,
		`INSERT INTO snapshots (stream_id, version, state, as_of, created_at)
		 VALUES ($1, $2, $3, $4, COALESCE($5::timestamptz, NOW()))
		 ON CONFLICT (stream_id, version) DO UPDATE
		   SET state = EXCLUDED.state, as_of = EXCLUDED.as_of, created_at = EXCLUDED.created_at`,
		snapshot.StreamID, snapshot.Version, state, snapshot.AsOf, createdAt)
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

// LoadSnapshotBefore returns the latest snapshot whose CreatedAt is strictly
// before the given time, or (nil, nil) if none. The cutoff is the snapshot
// creation time (created_at), not the as_of time, matching the core reference
// implementation in infrastructure/inmemory. version DESC breaks ties between
// snapshots created at the same instant deterministically (highest wins).
func (s *PostgresEventStore) LoadSnapshotBefore(ctx context.Context, streamID string, before time.Time) (*eventstore.Snapshot, error) {
	return s.loadSnapshotRow(ctx,
		`SELECT stream_id, version, state, as_of, created_at
		 FROM snapshots WHERE stream_id = $1 AND created_at < $2 ORDER BY created_at DESC, version DESC LIMIT 1`,
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
