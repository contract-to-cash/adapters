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
