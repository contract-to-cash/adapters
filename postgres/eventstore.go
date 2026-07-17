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

// Subscription reconnect tuning (issue #61). A LISTEN connection that dies
// (DB failover, admin pg_terminate_backend, network partition) is terminal in
// pgx; the subscription must tear it down and rebuild from the pool instead of
// spinning on the dead connection forever.
const (
	subReconnectInitialBackoff = 100 * time.Millisecond
	subReconnectMaxBackoff     = 5 * time.Second

	// subReconnectHealthyAfter: a connection cycle that stayed up at least
	// this long before failing is considered to have been healthy, so the
	// backoff and the consecutive-failure counter reset. This keeps a
	// long-lived-but-idle connection from escalating the backoff across
	// unrelated outages, while a tight crash loop (connections dying
	// immediately) still backs off exponentially.
	subReconnectHealthyAfter = 30 * time.Second
)

// PostgresEventStore implements eventstore.Store.
type PostgresEventStore struct {
	pool *pgxpool.Pool

	// onSubErr, when set, is invoked whenever a Subscribe goroutine hits an
	// abnormal error: every failed connection cycle (acquire / LISTEN /
	// catch-up / notification-wait failure, each reconnect attempt included)
	// and, if maxReconnects is set and exhausted, a final terminal error
	// before the events channel is closed. The core eventstore.Store
	// interface cannot signal any of this (Subscribe returns a bare channel
	// that is closed on both success and failure), so this optional callback
	// lets integrators distinguish a clean shutdown from a broken/outaged
	// subscription. It is called from the subscription goroutine; keep it
	// non-blocking (e.g. log, increment a metric, push to a buffered channel).
	onSubErr func(error)

	// catchUpBatch bounds the LoadAll page size used while replaying/tailing so
	// a large backlog is streamed in chunks instead of loaded unbounded.
	catchUpBatch int

	// maxReconnects, when > 0, bounds the number of CONSECUTIVE failed
	// reconnect attempts after which the subscription gives up and closes its
	// channel. 0 (default) retries forever with capped backoff.
	maxReconnects int

	// acquireSubConn and loadAll are seams for the subscription reconnect
	// state machine so it is unit-testable without a database. NewEventStore
	// wires them to the pool-backed implementations; only tests override them.
	acquireSubConn func(ctx context.Context) (subListenConn, error)
	loadAll        func(ctx context.Context, fromPosition int64, limit int) ([]eventstore.Event, error)
}

var _ eventstore.Store = (*PostgresEventStore)(nil)

// EventStoreOption configures a PostgresEventStore.
type EventStoreOption func(*PostgresEventStore)

// WithSubscriptionErrorHandler registers a callback invoked when a Subscribe
// goroutine hits an abnormal error. It fires once per failed connection cycle
// (acquire / LISTEN / catch-up / notification-wait failure — including every
// reconnect attempt during an outage, so an ongoing outage is continuously
// observable), and once more with a terminal error if a reconnect limit set
// via WithSubscriptionMaxReconnects is exhausted, just before the events
// channel is closed. Context cancellation is a normal shutdown and does not
// trigger it.
func WithSubscriptionErrorHandler(fn func(error)) EventStoreOption {
	return func(s *PostgresEventStore) { s.onSubErr = fn }
}

// WithSubscriptionMaxReconnects bounds the number of consecutive failed
// reconnect attempts a subscription makes after losing its LISTEN connection
// (issue #61). When the limit is exhausted, a terminal error is reported via
// the subscription error handler and the events channel is closed, so a
// consumer blocked on the channel (e.g. core's ProjectionService.Start, which
// returns when the channel closes) regains control instead of waiting forever.
//
// The counter resets after a healthy cycle (a connection that stayed up for a
// while), so the limit bounds a single continuous outage, not the lifetime
// total. The default (0, or any value <= 0, which is ignored) retries forever
// with capped exponential backoff — see Subscribe for why that is the default.
func WithSubscriptionMaxReconnects(n int) EventStoreOption {
	return func(s *PostgresEventStore) {
		if n > 0 {
			s.maxReconnects = n
		}
	}
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
	s.acquireSubConn = s.acquirePoolSubConn
	s.loadAll = s.LoadAll
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *PostgresEventStore) q(ctx context.Context) Querier {
	return QuerierFromContext(ctx, s.pool)
}

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
// event at a lower position.
//
// Reconnect behavior (issue #61): a broken LISTEN connection (DB failover,
// admin pg_terminate_backend — terminal for that connection in pgx) does NOT
// end the subscription. The dead connection is released back to the pool
// (which health-checks and destroys it), a fresh connection is acquired and
// LISTEN is re-issued with exponential backoff (100ms doubling, capped at 5s,
// honouring ctx cancellation at every wait), and catch-up is re-run keyed off
// the last delivered position so events committed during the outage are
// delivered — LISTEN always precedes catch-up, so nothing can fall between
// the two. Every failed connection cycle (including each reconnect attempt)
// is reported via WithSubscriptionErrorHandler, making an ongoing outage
// observable.
//
// By default reconnection retries forever: closing the channel is NOT a
// usable error signal to the consumer (core's ProjectionService.Start returns
// nil on channel close, indistinguishable from a clean shutdown), so giving
// up would silently freeze projections — the exact failure this reconnect
// logic exists to prevent — while retrying keeps them self-healing across
// arbitrarily long failovers, with the error handler as the observability
// channel and ctx cancellation as the escape hatch. Integrators who prefer
// fail-fast supervision (e.g. an orchestrator restarting the process) can
// bound consecutive attempts with WithSubscriptionMaxReconnects; on
// exhaustion a terminal error is reported and the channel is closed so a
// blocked consumer regains control.
//
// The channel is closed when ctx is cancelled (normal shutdown) or when a
// configured reconnect limit is exhausted; abnormal terminations and outage
// cycles are surfaced via the optional subscription error handler
// (WithSubscriptionErrorHandler), since the bare channel return cannot.
func (s *PostgresEventStore) Subscribe(ctx context.Context, fromPosition int64) (<-chan eventstore.Event, error) {
	ch := make(chan eventstore.Event, subscriberBuffer)
	go s.runSubscription(ctx, fromPosition, ch)
	return ch, nil
}

// subListenConn is the minimal surface of a LISTEN connection used by the
// subscription loop: issue LISTEN, block for a notification, release. The
// production implementation wraps *pgxpool.Conn; unit tests substitute a fake
// via the acquireSubConn seam so the reconnect state machine is testable
// without a database (issue #61).
type subListenConn interface {
	listen(ctx context.Context) error
	waitForNotification(ctx context.Context) error
	release()
}

type poolSubConn struct {
	conn *pgxpool.Conn
}

func (s *PostgresEventStore) acquirePoolSubConn(ctx context.Context) (subListenConn, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return &poolSubConn{conn: conn}, nil
}

func (c *poolSubConn) listen(ctx context.Context) error {
	_, err := c.conn.Exec(ctx, fmt.Sprintf("LISTEN %s", eventsChannel))
	return err
}

func (c *poolSubConn) waitForNotification(ctx context.Context) error {
	_, err := c.conn.Conn().WaitForNotification(ctx)
	return err
}

// release UNLISTENs best-effort and returns the connection to the pool. The
// UNLISTEN is skipped on an already-closed connection and bounded by a short
// deadline otherwise, so a broken-but-not-yet-detected connection (e.g. a
// network partition) cannot wedge the reconnect loop; pgxpool health-checks
// the connection on Release and destroys a dead one, so releasing after a
// terminal error is leak-free.
func (c *poolSubConn) release() {
	if !c.conn.Conn().IsClosed() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = c.conn.Exec(ctx, fmt.Sprintf("UNLISTEN %s", eventsChannel))
		cancel()
	}
	c.conn.Release()
}

// runSubscription drives the reconnect state machine: run a connection cycle
// (acquire → LISTEN → catch-up → tail), and when it fails, report the error,
// back off exponentially (capped), and start a new cycle. Delivery position is
// tracked across cycles, so the catch-up after a reconnect closes the
// notification gap that opened while disconnected.
func (s *PostgresEventStore) runSubscription(ctx context.Context, fromPosition int64, ch chan<- eventstore.Event) {
	defer close(ch)

	position := fromPosition
	backoff := subReconnectInitialBackoff
	reconnects := 0 // consecutive reconnect attempts since the last healthy cycle

	for {
		cycleStart := time.Now()
		err := s.listenAndTail(ctx, &position, ch)
		if ctx.Err() != nil {
			return // normal shutdown; the deferred close signals completion
		}
		s.reportSubErr(fmt.Errorf("subscribe: %w", err))

		if time.Since(cycleStart) >= subReconnectHealthyAfter {
			reconnects = 0
			backoff = subReconnectInitialBackoff
		}
		if s.maxReconnects > 0 && reconnects >= s.maxReconnects {
			s.reportSubErr(fmt.Errorf(
				"subscribe: closing subscription after %d consecutive failed reconnect attempts: %w",
				reconnects, err))
			return
		}
		reconnects++

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > subReconnectMaxBackoff {
			backoff = subReconnectMaxBackoff
		}
	}
}

// listenAndTail runs one connection cycle: acquire a LISTEN connection, issue
// LISTEN, catch up from *position, then tail notifications, re-running catchUp
// after each one. It returns a non-nil error describing the failed step, or
// ctx.Err() on cancellation; the connection is always released before
// returning (leak-free on every path).
//
// Ordering matters: LISTEN is issued BEFORE catchUp, so an event committed
// between the two is seen either by the catch-up query or by an already-queued
// notification — nothing can fall in the gap. This is the same invariant on
// the initial cycle and on every reconnect.
func (s *PostgresEventStore) listenAndTail(ctx context.Context, position *int64, ch chan<- eventstore.Event) error {
	conn, err := s.acquireSubConn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.release()

	if err := conn.listen(ctx); err != nil {
		return fmt.Errorf("LISTEN: %w", err)
	}
	if err := s.catchUp(ctx, position, ch); err != nil {
		return fmt.Errorf("catch-up: %w", err)
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := conn.waitForNotification(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Any non-context error from WaitForNotification is terminal for
			// this connection in pgx (broken/killed connection): tear it down
			// and let the caller reconnect instead of spinning on it forever
			// (issue #61).
			return fmt.Errorf("wait for notification: %w", err)
		}
		if err := s.catchUp(ctx, position, ch); err != nil {
			return fmt.Errorf("catch-up: %w", err)
		}
	}
}

// reportSubErr forwards an abnormal subscription error (a failed connection
// cycle, a reconnect attempt during an outage, or a terminal give-up) to the
// registered handler. Context cancellation/deadline is a normal shutdown and is
// dropped so integrators are not paged for a graceful close.
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
		events, err := s.loadAll(ctx, *position, s.catchUpBatch)
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
//
// Because the cutoff is CreatedAt (wall-clock) while callers bound events by
// OccurredAt, a snapshot returned here is not automatically safe to use for an
// as-of reconstruction: with a skewed/injected clock a snapshot can be
// CreatedAt before asOf yet cover an event that OCCURRED after asOf. The
// caller must apply the core review W7 consistency guard (reject the snapshot
// if its Version exceeds the highest event version within the asOf horizon) —
// see PostgresContractRepository.FindByIDAsOf in contract_repo.go, which
// mirrors core's application/query/temporal_query_service.go GetContractAsOf.
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
