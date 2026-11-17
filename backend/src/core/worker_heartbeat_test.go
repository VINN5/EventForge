package core

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func heartbeatConfig(id string) WorkerConfig {
	return WorkerConfig{
		ID:                id,
		Concurrency:       1,
		PollInterval:      20 * time.Millisecond,
		Lease:             300 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		ReapInterval:      20 * time.Millisecond,
	}
}

func TestNewWorker_HeartbeatDefaultAndValidation(t *testing.T) {
	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error { return nil },
	}

	w, err := NewWorker(nil, WorkerConfig{ID: "w1"}, handlers, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	if w.cfg.HeartbeatInterval != 10*time.Second {
		t.Fatalf("default heartbeat = %v, want a third of the 30s lease", w.cfg.HeartbeatInterval)
	}

	for _, hb := range []time.Duration{time.Second, 2 * time.Second} {
		_, err := NewWorker(nil, WorkerConfig{ID: "w1", Lease: time.Second, HeartbeatInterval: hb},
			handlers, quietLogger())
		if err == nil {
			t.Fatalf("heartbeat %v with a 1s lease should be rejected", hb)
		}
	}
}

// slow_not_dead: a handler that runs for five times its lease is still alive,
// so nobody may take its job. A second worker with a fast reaper is watching.
func TestWorker_SlowHandlerIsNotTakenOver(t *testing.T) {
	pool := testPool(t)
	id := submitNoop(t, pool, SubmitParams{})

	var calls atomic.Int32
	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error {
			calls.Add(1)
			select {
			case <-time.After(1500 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}

	cancel1, done1 := startWorker(t, pool, heartbeatConfig("w1"), handlers)
	cancel2, done2 := startWorker(t, pool, heartbeatConfig("w2"), handlers)

	waitFor(t, 15*time.Second, func() bool {
		return readJob(t, pool, id).State == "succeeded"
	}, "the slow job to succeed")
	stopWorker(t, cancel1, done1)
	stopWorker(t, cancel2, done2)

	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want exactly 1", calls.Load())
	}
	if got := readJob(t, pool, id); got.Attempts != 1 || got.Fence != 1 {
		t.Fatalf("attempts=%d fence=%d, want 1 and 1 (never taken over)", got.Attempts, got.Fence)
	}
}

// When the lease is taken away, the handler is told and stops, and the worker
// leaves the new owner's job alone.
func TestWorker_LostLeaseCancelsHandler(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	id := submitNoop(t, pool, SubmitParams{})

	started := make(chan struct{})
	cause := make(chan error, 1)
	var once atomic.Bool
	handlers := map[string]Handler{
		"noop": func(hctx context.Context, job Job) error {
			if !once.CompareAndSwap(false, true) {
				return nil
			}
			close(started)
			<-hctx.Done()
			cause <- context.Cause(hctx)
			return hctx.Err()
		},
	}

	cfg := heartbeatConfig("w1")
	cfg.ReapInterval = time.Hour // only the thief below takes the job
	cancel, done := startWorker(t, pool, cfg, handlers)

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	// Another process takes the job over while the handler is still running.
	// w1's only slot is busy, so it cannot grab the job back itself.
	requeue(t, pool, id)
	thief := claimOne(t, pool, "thief")

	select {
	case c := <-cause:
		if !errors.Is(c, ErrLeaseLost) {
			t.Fatalf("handler was cancelled with cause %v, want ErrLeaseLost", c)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler was not cancelled after its lease was taken")
	}

	// The thief still owns the job and nothing was recorded against it.
	got := readJob(t, pool, id)
	if got.State != "running" || got.Owner != "thief" || got.Attempts != 2 || got.Fence != thief.Fence {
		t.Fatalf("row after takeover: %+v", got)
	}
	if n := closedAttemptRows(t, pool, id); n != 0 {
		t.Fatalf("%d attempt rows closed by the worker that lost the lease, want 0", n)
	}
	if err := Ack(ctx, pool, thief.ID, "thief", thief.Fence); err != nil {
		t.Fatalf("the new owner could not finish the job: %v", err)
	}
	stopWorker(t, cancel, done)
}

// A handler that is still running when shutdown starts keeps its lease for as
// long as it needs, so a reaper elsewhere cannot steal it during the grace period.
func TestWorker_HeartbeatContinuesDuringShutdownGrace(t *testing.T) {
	pool := testPool(t)
	id := submitNoop(t, pool, SubmitParams{})

	started := make(chan struct{})
	var calls atomic.Int32
	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error {
			calls.Add(1)
			close(started)
			time.Sleep(1200 * time.Millisecond) // four times the lease, ignoring ctx
			return nil
		},
	}

	cfg := heartbeatConfig("w1")
	cancel, done := startWorker(t, pool, cfg, handlers)

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	// A fast reaper somewhere else, as another worker would run.
	startReaper(t, pool, 20*time.Millisecond, 10)

	cancel() // shutdown begins; the handler still has about a second to go
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}

	got := readJob(t, pool, id)
	if got.State != "succeeded" || got.Attempts != 1 || calls.Load() != 1 {
		t.Fatalf("row %+v, handler calls %d; want succeeded after 1 attempt, 1 call", got, calls.Load())
	}
}
