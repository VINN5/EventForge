package core

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

// The heartbeat takes the list of running jobs, then calls Extend. A job that
// finishes in between is reported as lost, but there is no handler left to
// cancel, so nothing should be logged for it.

func TestInflightCancel_ReportsWhetherAHandlerWasStillRunning(t *testing.T) {
	f := &inflight{m: map[LeaseRef]context.CancelCauseFunc{}}
	ref := LeaseRef{ID: "job-1", Fence: 1}

	if f.cancel(ref, ErrLeaseLost) {
		t.Fatal("cancel reported a running handler although none is registered")
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	f.add(ref, cancel)
	if !f.cancel(ref, ErrLeaseLost) {
		t.Fatal("cancel did not find the registered handler")
	}
	if !errors.Is(context.Cause(ctx), ErrLeaseLost) {
		t.Fatalf("cause = %v, want ErrLeaseLost", context.Cause(ctx))
	}
}

func newLoggedWorker(t *testing.T) (*Worker, *bytes.Buffer) {
	t.Helper()
	log, out := bufLogger()
	w, err := NewWorker(nil, WorkerConfig{ID: "w1", Rand: zeroRand},
		map[string]Handler{"noop": func(ctx context.Context, job Job) error { return nil }}, log)
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	out.Reset()
	return w, out
}

func TestHandleLostLeases_StaysQuietWhenTheHandlerAlreadyFinished(t *testing.T) {
	w, out := newLoggedWorker(t)

	w.handleLostLeases([]LeaseRef{{ID: "job-1", Fence: 1}})

	if out.Len() != 0 {
		t.Fatalf("expected no log output for a job that already finished, got: %s", out)
	}
}

func TestHandleLostLeases_CancelsAndWarnsWhenTheHandlerIsStillRunning(t *testing.T) {
	w, out := newLoggedWorker(t)
	ref := LeaseRef{ID: "job-1", Fence: 1}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	w.flights.add(ref, cancel)

	w.handleLostLeases([]LeaseRef{ref})

	if !errors.Is(context.Cause(ctx), ErrLeaseLost) {
		t.Fatalf("cause = %v, want ErrLeaseLost", context.Cause(ctx))
	}
	if !strings.Contains(out.String(), "cancelling handler") {
		t.Fatalf("log should say the handler is being cancelled; got: %s", out)
	}
}