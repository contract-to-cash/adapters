package postgres

import (
	"context"
	"errors"
	"testing"
)

func TestReportSubErr_FiltersContextCancellation(t *testing.T) {
	var got []error
	s := &PostgresEventStore{onSubErr: func(err error) { got = append(got, err) }}

	// Normal-shutdown errors must not be reported.
	s.reportSubErr(nil)
	s.reportSubErr(context.Canceled)
	s.reportSubErr(context.DeadlineExceeded)
	s.reportSubErr(errwrap(context.Canceled))
	if len(got) != 0 {
		t.Fatalf("context/nil errors should not be reported, got %v", got)
	}

	// A genuine failure is forwarded.
	real := errors.New("acquire failed")
	s.reportSubErr(real)
	if len(got) != 1 || !errors.Is(got[0], real) {
		t.Fatalf("expected real error to be reported once, got %v", got)
	}
}

func TestReportSubErr_NilHandlerIsSafe(t *testing.T) {
	s := &PostgresEventStore{} // no handler
	s.reportSubErr(errors.New("boom"))
}

func TestNewEventStore_DefaultsAndOptions(t *testing.T) {
	s := NewEventStore(nil)
	if s.catchUpBatch != defaultCatchUpBatch {
		t.Errorf("default catchUpBatch = %d, want %d", s.catchUpBatch, defaultCatchUpBatch)
	}

	called := false
	s = NewEventStore(nil,
		WithCatchUpBatchSize(7),
		WithCatchUpBatchSize(-1), // ignored
		WithSubscriptionErrorHandler(func(error) { called = true }),
	)
	if s.catchUpBatch != 7 {
		t.Errorf("catchUpBatch = %d, want 7", s.catchUpBatch)
	}
	if s.onSubErr == nil {
		t.Fatal("expected error handler to be set")
	}
	s.onSubErr(errors.New("x"))
	if !called {
		t.Error("error handler was not wired through")
	}
}

func errwrap(err error) error { return errors.Join(errors.New("subscribe"), err) }
