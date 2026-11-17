package core

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func startWorker(t *testing.T, db Querier, cfg WorkerConfig, h map[string]Handler) (cancel func(), done <-chan error) {
	t.Helper()
	w, err := NewWorker(db, cfg, h, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancelFn := context.WithCancel(context.Background())
	ch := make(chan error, 1)
	go func() { ch <- w.Run(ctx) }()
	return cancelFn, ch
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func countByState(t *testing.T, q Querier, state string) int {
	t.Helper()
	var n int
	err := q.QueryRow(context.Background(),
		`SELECT count(*) FROM jobs WHERE state = $1`, state).Scan(&n)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func TestWorker_ProcessesAllJobsExactlyOnce(t *testing.T) {
	pool := testPool(t)
	const total = 100
	for i := 0; i < total; i++ {
		submitNoop(t, pool, SubmitParams{})
	}

	var mu sync.Mutex
	calls := map[string]int{}
	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error {
			mu.Lock()
			calls[job.ID]++
			mu.Unlock()
			return nil
		},
	}

	cancel, done := startWorker(t, pool,
		WorkerConfig{ID: "w1", Concurrency: 4, PollInterval: 20 * time.Millisecond}, handlers)
	waitFor(t, 30*time.Second,
		func() bool { return countByState(t, pool, "succeeded") == total },
		"all jobs to succeed")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != total {
		t.Fatalf("handler saw %d distinct jobs, want %d", len(calls), total)
	}
	for id, n := range calls {
		if n != 1 {
			t.Errorf("job %s handled %d times", id, n)
		}
	}
}

func TestWorker_RespectsConcurrencyLimit(t *testing.T) {
	pool := testPool(t)
	const total = 20
	for i := 0; i < total; i++ {
		submitNoop(t, pool, SubmitParams{})
	}

	var current, peak atomic.Int32
	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error {
			n := current.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(30 * time.Millisecond)
			current.Add(-1)
			return nil
		},
	}

	cancel, done := startWorker(t, pool,
		WorkerConfig{ID: "w1", Concurrency: 3, PollInterval: 20 * time.Millisecond}, handlers)
	waitFor(t, 30*time.Second,
		func() bool { return countByState(t, pool, "succeeded") == total },
		"all jobs to succeed")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	if p := peak.Load(); p > 3 || p < 2 {
		t.Fatalf("peak concurrency was %d, want between 2 and 3", p)
	}
}

// Shutdown must let a running job finish and ack it, not abandon it.
func TestWorker_ShutdownWaitsForInFlightJob(t *testing.T) {
	pool := testPool(t)
	id := submitNoop(t, pool, SubmitParams{})

	started := make(chan struct{})
	release := make(chan struct{})
	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error {
			close(started)
			<-release
			return nil
		},
	}

	cancel, done := startWorker(t, pool,
		WorkerConfig{ID: "w1", Concurrency: 1, PollInterval: 20 * time.Millisecond}, handlers)

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	cancel()
	select {
	case err := <-done:
		t.Fatalf("Run returned (%v) while a job was still running", err)
	case <-time.After(300 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the job finished")
	}
	if got := readJob(t, pool, id); got.State != "succeeded" {
		t.Fatalf("state=%q, want succeeded", got.State)
	}
}

// If a handler outlasts the grace period, it is cancelled and the job is
// left running so lease recovery (M2) can take it, never falsely acked.
func TestWorker_GraceExpiryCancelsHandlerAndDoesNotAck(t *testing.T) {
	pool := testPool(t)
	id := submitNoop(t, pool, SubmitParams{})

	started := make(chan struct{})
	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
	}

	cancel, done := startWorker(t, pool, WorkerConfig{
		ID: "w1", Concurrency: 1,
		PollInterval:  20 * time.Millisecond,
		ShutdownGrace: 200 * time.Millisecond,
	}, handlers)

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the grace period")
	}
	if got := readJob(t, pool, id); got.State != "running" {
		t.Fatalf("state=%q, want running (left for lease recovery)", got.State)
	}
}

// With all slots busy, the worker must claim again as soon as a slot frees,
// not wait out the poll interval.
func TestWorker_ReclaimsImmediatelyWhenSlotsFree(t *testing.T) {
	pool := testPool(t)
	const total = 20
	for i := 0; i < total; i++ {
		submitNoop(t, pool, SubmitParams{})
	}

	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error { return nil },
	}

	cancel, done := startWorker(t, pool,
		WorkerConfig{ID: "w1", Concurrency: 2, PollInterval: 2 * time.Second}, handlers)
	waitFor(t, 5*time.Second,
		func() bool { return countByState(t, pool, "succeeded") == total },
		"jobs to finish without waiting out the poll interval")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}
