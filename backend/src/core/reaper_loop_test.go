package core

import (
	"context"
	"testing"
	"time"
)

// startReaper runs RunReaper in the background. The returned cancel stops it;
// done is closed once the loop has really exited.
func startReaper(t *testing.T, db Querier, interval time.Duration, batch int) (cancel func(), done <-chan struct{}) {
	t.Helper()
	ctx, cancelFn := context.WithCancel(context.Background())
	ch := make(chan struct{})
	go func() {
		RunReaper(ctx, db, interval, batch, quietLogger())
		close(ch)
	}()
	t.Cleanup(func() {
		cancelFn()
		<-ch // never leave the loop running into the next test
	})
	return cancelFn, ch
}

func TestRunReaper_RecoversExpiredJob(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	// A worker that claims with a very short lease and then vanishes.
	jobs, err := Claim(ctx, pool, "default", "ghost", 1, 100*time.Millisecond)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("setup claim: jobs=%d err=%v", len(jobs), err)
	}

	startReaper(t, pool, 20*time.Millisecond, 10)

	waitFor(t, 5*time.Second, func() bool {
		return readJob(t, pool, id).State == "queued"
	}, "the reaper to requeue the abandoned job")
}

// A large backlog must be cleared in one pass, not one batch per interval.
func TestRunReaper_DrainsBacklogWithoutWaitingForInterval(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	const total = 25
	for i := 0; i < total; i++ {
		submitNoop(t, pool, SubmitParams{})
	}
	jobs, err := Claim(ctx, pool, "default", "ghost", total, 30*time.Second)
	if err != nil || len(jobs) != total {
		t.Fatalf("setup claim: jobs=%d err=%v", len(jobs), err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE jobs SET lease_expires_at = now() - interval '1 second'
		WHERE state = 'running'`); err != nil {
		t.Fatalf("expire all: %v", err)
	}

	// Batch of 10 and an interval of one hour: only draining can finish this.
	startReaper(t, pool, time.Hour, 10)

	waitFor(t, 5*time.Second, func() bool {
		return countByState(t, pool, "queued") == total
	}, "the whole backlog to be requeued in one pass")
}

func TestRunReaper_StopsWhenContextCancelled(t *testing.T) {
	pool := testPool(t)

	cancel, done := startReaper(t, pool, time.Hour, 10)
	time.Sleep(100 * time.Millisecond) // let it settle into its wait
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunReaper did not return after its context was cancelled")
	}
}

// The end-to-end promise: work abandoned by a dead worker is finished by a live one.
func TestWorker_FinishesJobAbandonedByDeadWorker(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	jobs, err := Claim(ctx, pool, "default", "ghost", 1, 200*time.Millisecond)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("setup claim: jobs=%d err=%v", len(jobs), err)
	}

	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error { return nil },
	}
	cancel, done := startWorker(t, pool, WorkerConfig{
		ID:           "live",
		Concurrency:  1,
		PollInterval: 20 * time.Millisecond,
		ReapInterval: 50 * time.Millisecond,
	}, handlers)

	waitFor(t, 10*time.Second, func() bool {
		return readJob(t, pool, id).State == "succeeded"
	}, "the live worker to finish the abandoned job")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := readJob(t, pool, id)
	if got.Attempts != 2 || got.Fence != 2 {
		t.Fatalf("attempts=%d fence=%d, want 2 and 2 (ghost, then live)", got.Attempts, got.Fence)
	}

	var outcome string
	err = pool.QueryRow(ctx, `
		SELECT COALESCE(outcome, '') FROM job_attempts
		WHERE job_id = $1 AND attempt = 1`, id).Scan(&outcome)
	if err != nil || outcome != "lease_expired" {
		t.Fatalf("attempt 1 outcome = %q (err %v), want lease_expired", outcome, err)
	}
}
