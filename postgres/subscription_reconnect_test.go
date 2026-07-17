package postgres

// Unit tests for the subscription reconnect state machine (issue #61). They
// drive runSubscription through the acquireSubConn/loadAll seams, so the
// reconnect logic (release dead conn → re-acquire → re-LISTEN → catch-up,
// backoff, error reporting, exhaustion policy) is exercised without a
// database.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/contract-to-cash/core/eventstore"
)

// callRecorder records the ordered sequence of seam calls made by the
// subscription goroutine.
type callRecorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *callRecorder) add(call string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
}

func (r *callRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

// scriptedConn is a fake subListenConn whose waitForNotification behavior is
// scripted per test.
type scriptedConn struct {
	rec       *callRecorder
	id        int
	listenErr error
	wait      func(ctx context.Context) error
}

func (c *scriptedConn) listen(_ context.Context) error {
	c.rec.add(fmt.Sprintf("listen#%d", c.id))
	return c.listenErr
}

func (c *scriptedConn) waitForNotification(ctx context.Context) error {
	c.rec.add(fmt.Sprintf("wait#%d", c.id))
	return c.wait(ctx)
}

func (c *scriptedConn) release() {
	c.rec.add(fmt.Sprintf("release#%d", c.id))
}

// errCollector is a concurrency-safe WithSubscriptionErrorHandler sink.
type errCollector struct {
	mu   sync.Mutex
	errs []error
}

func (c *errCollector) handle(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, err)
}

func (c *errCollector) snapshot() []error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]error(nil), c.errs...)
}

func recvEvent(t *testing.T, ch <-chan eventstore.Event, wantID string) {
	t.Helper()
	select {
	case evt, ok := <-ch:
		if !ok {
			t.Fatalf("channel closed while waiting for event %q", wantID)
		}
		if evt.ID != wantID {
			t.Fatalf("received event %q, want %q", evt.ID, wantID)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for event %q", wantID)
	}
}

// A terminal WaitForNotification error (broken/killed connection) must not
// leave the subscription spinning on the dead connection: it must release it,
// re-acquire from the pool, re-LISTEN, and run catch-up so events committed
// during the outage are delivered.
func TestRunSubscription_ReconnectsAfterTerminalWaitError(t *testing.T) {
	rec := &callRecorder{}
	reports := &errCollector{}

	var mu sync.Mutex
	log := []eventstore.Event{{ID: "evt-1", GlobalPosition: 1}}

	s := NewEventStore(nil, WithSubscriptionErrorHandler(reports.handle))
	s.loadAll = func(_ context.Context, fromPosition int64, _ int) ([]eventstore.Event, error) {
		rec.add("loadAll")
		mu.Lock()
		defer mu.Unlock()
		var out []eventstore.Event
		for _, e := range log {
			if e.GlobalPosition > fromPosition {
				out = append(out, e)
			}
		}
		return out, nil
	}

	acquires := 0
	s.acquireSubConn = func(_ context.Context) (subListenConn, error) {
		acquires++
		id := acquires
		rec.add(fmt.Sprintf("acquire#%d", id))
		if id == 1 {
			return &scriptedConn{rec: rec, id: 1, wait: func(_ context.Context) error {
				// Simulate an event committed during the outage window: it is
				// appended just as the connection dies, so only the post-
				// reconnect catch-up can deliver it.
				mu.Lock()
				log = append(log, eventstore.Event{ID: "evt-2", GlobalPosition: 2})
				mu.Unlock()
				return errors.New("server terminated the connection")
			}}, nil
		}
		return &scriptedConn{rec: rec, id: id, wait: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan eventstore.Event, subscriberBuffer)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runSubscription(ctx, 0, ch)
	}()

	recvEvent(t, ch, "evt-1") // initial catch-up on connection 1
	recvEvent(t, ch, "evt-2") // post-reconnect catch-up on connection 2

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("subscription did not stop after context cancellation")
	}
	if _, ok := <-ch; ok {
		t.Fatal("expected the events channel to be closed after shutdown")
	}

	// The dead connection must be released before the new one is acquired, and
	// on the reconnect LISTEN must precede catch-up so no event can fall
	// between re-subscribing and back-filling. (wait#2 is not asserted: the
	// test's cancel() may land before the tail loop reaches it.)
	want := []string{
		"acquire#1", "listen#1", "loadAll", "wait#1", "release#1",
		"acquire#2", "listen#2", "loadAll",
	}
	got := rec.snapshot()
	if len(got) < len(want) {
		t.Fatalf("call sequence too short: got %v, want prefix %v", got, want)
	}
	for i, call := range want {
		if got[i] != call {
			t.Fatalf("call sequence[%d] = %q, want %q (full: %v)", i, got[i], call, got)
		}
	}

	// The outage must be observable via the error handler, and clean shutdown
	// must not be reported.
	errs := reports.snapshot()
	if len(errs) != 1 {
		t.Fatalf("expected exactly 1 reported error, got %d: %v", len(errs), errs)
	}
	if !strings.Contains(errs[0].Error(), "wait for notification") {
		t.Errorf("reported error %q does not name the failed step", errs[0])
	}
}

// Acquire/LISTEN failures during a reconnect cycle must be retried with
// backoff and each failed cycle must be reported, until a cycle succeeds.
func TestRunSubscription_RetriesAcquireFailuresAndReports(t *testing.T) {
	rec := &callRecorder{}
	reports := &errCollector{}

	s := NewEventStore(nil, WithSubscriptionErrorHandler(reports.handle))
	s.loadAll = func(_ context.Context, _ int64, _ int) ([]eventstore.Event, error) {
		return []eventstore.Event{{ID: "evt-after-retry", GlobalPosition: 1}}, nil
	}

	acquires := 0
	s.acquireSubConn = func(_ context.Context) (subListenConn, error) {
		acquires++
		if acquires <= 2 {
			return nil, errors.New("pool exhausted")
		}
		return &scriptedConn{rec: rec, id: acquires, wait: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan eventstore.Event, subscriberBuffer)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runSubscription(ctx, 0, ch)
	}()

	recvEvent(t, ch, "evt-after-retry")

	errs := reports.snapshot()
	if len(errs) != 2 {
		t.Fatalf("expected 2 reported acquire failures, got %d: %v", len(errs), errs)
	}
	for _, err := range errs {
		if !strings.Contains(err.Error(), "acquire connection") {
			t.Errorf("reported error %q does not name the failed step", err)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("subscription did not stop after context cancellation")
	}
}

// With WithSubscriptionMaxReconnects, an unrecoverable outage must not retry
// forever: after the configured number of consecutive failed reconnect
// attempts the subscription reports a terminal error and closes the channel,
// so a consumer blocked on the channel (core's ProjectionService.Start)
// regains control.
func TestRunSubscription_BoundedReconnectsCloseChannel(t *testing.T) {
	reports := &errCollector{}

	s := NewEventStore(nil,
		WithSubscriptionErrorHandler(reports.handle),
		WithSubscriptionMaxReconnects(2),
	)
	s.acquireSubConn = func(_ context.Context) (subListenConn, error) {
		return nil, errors.New("database gone")
	}
	s.loadAll = func(_ context.Context, _ int64, _ int) ([]eventstore.Event, error) {
		t.Error("loadAll must not be reached when acquire always fails")
		return nil, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan eventstore.Event, subscriberBuffer)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runSubscription(ctx, 0, ch)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("subscription did not give up after exhausting reconnect attempts")
	}
	if _, ok := <-ch; ok {
		t.Fatal("expected the events channel to be closed after exhaustion")
	}

	// Initial cycle + 2 reconnect attempts, each reported, plus the terminal
	// give-up report.
	errs := reports.snapshot()
	if len(errs) != 4 {
		t.Fatalf("expected 4 reported errors (3 cycles + terminal), got %d: %v", len(errs), errs)
	}
	last := errs[len(errs)-1].Error()
	if !strings.Contains(last, "closing subscription after 2 consecutive failed reconnect attempts") {
		t.Errorf("terminal error %q does not describe exhaustion", last)
	}
}

// Context cancellation while the reconnect loop is backing off must stop the
// subscription promptly and must not be reported as an error.
func TestRunSubscription_CancelDuringBackoffIsCleanShutdown(t *testing.T) {
	reports := &errCollector{}
	firstFailure := make(chan struct{})
	var once sync.Once

	s := NewEventStore(nil, WithSubscriptionErrorHandler(reports.handle))
	s.acquireSubConn = func(_ context.Context) (subListenConn, error) {
		once.Do(func() { close(firstFailure) })
		return nil, errors.New("database gone")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan eventstore.Event, subscriberBuffer)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runSubscription(ctx, 0, ch)
	}()

	<-firstFailure
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("subscription did not stop after context cancellation during backoff")
	}
	if _, ok := <-ch; ok {
		t.Fatal("expected the events channel to be closed")
	}
	for _, err := range reports.snapshot() {
		if errors.Is(err, context.Canceled) {
			t.Errorf("context cancellation must not be reported, got %v", err)
		}
	}
}

func TestWithSubscriptionMaxReconnects_Option(t *testing.T) {
	if s := NewEventStore(nil); s.maxReconnects != 0 {
		t.Errorf("default maxReconnects = %d, want 0 (retry forever)", s.maxReconnects)
	}
	s := NewEventStore(nil, WithSubscriptionMaxReconnects(3), WithSubscriptionMaxReconnects(-1))
	if s.maxReconnects != 3 {
		t.Errorf("maxReconnects = %d, want 3 (non-positive values ignored)", s.maxReconnects)
	}
}

// fakeClock is a controllable clock for the `now` seam so healthy-window
// semantics can be tested without real 30s waits.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// A failed connection attempt that itself takes longer than the healthy
// window (network partition: pool.Acquire blocks for the OS TCP timeout,
// minutes per attempt) must NOT reset the reconnect budget — the cycle never
// established, so WithSubscriptionMaxReconnects must still exhaust at the
// limit and close the channel. (Regression: the old code keyed the reset on
// bare cycle duration, so slow failures reset the counter every iteration and
// the limit was unreachable in exactly this scenario.)
func TestRunSubscription_SlowFailedAcquiresNeverResetBudget(t *testing.T) {
	reports := &errCollector{}
	clock := newFakeClock()

	s := NewEventStore(nil,
		WithSubscriptionErrorHandler(reports.handle),
		WithSubscriptionMaxReconnects(2),
	)
	s.now = clock.now
	s.acquireSubConn = func(_ context.Context) (subListenConn, error) {
		// Simulate a connect attempt that eats far more wall-clock time than
		// the healthy window before failing.
		clock.advance(subReconnectHealthyAfter + 10*time.Second)
		return nil, errors.New("connect timeout")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan eventstore.Event, subscriberBuffer)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runSubscription(ctx, 0, ch)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("slow failed acquires reset the reconnect budget: limit never exhausted")
	}
	if _, ok := <-ch; ok {
		t.Fatal("expected the events channel to be closed after exhaustion")
	}

	errs := reports.snapshot()
	if len(errs) != 4 {
		t.Fatalf("expected 4 reported errors (3 cycles + terminal), got %d: %v", len(errs), errs)
	}
	if last := errs[len(errs)-1].Error(); !strings.Contains(last, "closing subscription after 2 consecutive failed reconnect attempts") {
		t.Errorf("terminal error %q does not describe exhaustion", last)
	}
}

// A cycle that establishes (LISTEN + catch-up succeeded, tail loop entered)
// and stays up for at least the healthy window resets the reconnect budget:
// under a maxReconnects limit, an arbitrarily long sequence of such cycles
// must never exhaust the subscription.
func TestRunSubscription_EstablishedSustainedCycleResetsBudget(t *testing.T) {
	reports := &errCollector{}
	clock := newFakeClock()
	rec := &callRecorder{}

	s := NewEventStore(nil,
		WithSubscriptionErrorHandler(reports.handle),
		WithSubscriptionMaxReconnects(2),
	)
	s.now = clock.now
	s.loadAll = func(_ context.Context, _ int64, _ int) ([]eventstore.Event, error) {
		return nil, nil
	}

	const cycles = 6 // well past maxReconnects+1
	acquired := make(chan int, cycles+8)
	acquires := 0
	s.acquireSubConn = func(_ context.Context) (subListenConn, error) {
		acquires++
		id := acquires
		acquired <- id
		return &scriptedConn{rec: rec, id: id, wait: func(_ context.Context) error {
			// The connection stays up past the healthy window, then dies.
			clock.advance(subReconnectHealthyAfter + 10*time.Second)
			return errors.New("connection recycled")
		}}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan eventstore.Event, subscriberBuffer)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runSubscription(ctx, 0, ch)
	}()

	for i := 1; i <= cycles; i++ {
		select {
		case <-acquired:
		case <-done:
			t.Fatalf("subscription exhausted after %d healthy cycles despite budget resets", i-1)
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for connection cycle %d", i)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("subscription did not stop after context cancellation")
	}
	for _, err := range reports.snapshot() {
		if strings.Contains(err.Error(), "closing subscription") {
			t.Fatalf("healthy cycles must not lead to a terminal give-up, got %v", err)
		}
	}
}

// Scenario pinned deliberately (issue #61 review): a cycle that establishes
// but dies BEFORE the healthy window elapses still counts against the
// reconnect budget, so a connect-then-die loop cannot bypass a configured
// limit. Integrators whose infrastructure recycles connections faster than
// the healthy window must size the limit accordingly (or keep the default
// unlimited) — see the WithSubscriptionMaxReconnects godoc.
func TestRunSubscription_EstablishedButShortCyclesCountTowardLimit(t *testing.T) {
	reports := &errCollector{}
	clock := newFakeClock()
	rec := &callRecorder{}

	s := NewEventStore(nil,
		WithSubscriptionErrorHandler(reports.handle),
		WithSubscriptionMaxReconnects(2),
	)
	s.now = clock.now
	s.loadAll = func(_ context.Context, _ int64, _ int) ([]eventstore.Event, error) {
		return nil, nil
	}

	acquires := 0
	s.acquireSubConn = func(_ context.Context) (subListenConn, error) {
		acquires++
		return &scriptedConn{rec: rec, id: acquires, wait: func(_ context.Context) error {
			// Established, but killed immediately: no clock advance.
			return errors.New("killed right after establishing")
		}}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan eventstore.Event, subscriberBuffer)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runSubscription(ctx, 0, ch)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("established-but-short cycles must exhaust the limit")
	}
	if _, ok := <-ch; ok {
		t.Fatal("expected the events channel to be closed after exhaustion")
	}
	if acquires != 3 {
		t.Errorf("expected 3 connection cycles (initial + 2 reconnects), got %d", acquires)
	}
	errs := reports.snapshot()
	if len(errs) != 4 {
		t.Fatalf("expected 4 reported errors (3 cycles + terminal), got %d: %v", len(errs), errs)
	}
	if last := errs[len(errs)-1].Error(); !strings.Contains(last, "closing subscription after 2 consecutive failed reconnect attempts") {
		t.Errorf("terminal error %q does not describe exhaustion", last)
	}
}
