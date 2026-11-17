package core

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// expireLease makes a running job's lease be in the past, on the database clock.
func expireLease(t *testing.T, q Querier, id string) {
	t.Helper()
	_, err := q.Exec(context.Background(), `
		UPDATE jobs SET lease_expires_at = now() - interval '1 second'
		WHERE id = $1 AND state = 'running'`, id)
	if err != nil {
		t.Fatalf("expire lease: %v", err)
	}
}

func TestReap_RequeuesExpiredLease(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	first := claimOne(t, pool, "worker-1")
	expireLease(t, pool, id)

	res, err := Reap(ctx, pool, 100)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if res.Requeued != 1 || res.Dead != 0 {
		t.Fatalf("result = %+v, want 1 requeued, 0 dead", res)
	}

	got := readJob(t, pool, id)
	if got.State != "queued" || got.Owner != "" || got.Attempts != 1 || got.Fence != first.Fence {
		t.Fatalf("unexpected row after reap: %+v", got)
	}

	// The job can be claimed again, with a higher fence.
	second := claimOne(t, pool, "worker-2")
	if second.Attempts != 2 || second.Fence <= first.Fence {
		t.Fatalf("reclaim: attempts=%d fence=%d (first fence %d)",
			second.Attempts, second.Fence, first.Fence)
	}
}

func TestReap_LeavesLiveLeasesAlone(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	claimOne(t, pool, "worker-1") // 30 second lease

	res, err := Reap(ctx, pool, 100)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if res.Requeued != 0 || res.Dead != 0 {
		t.Fatalf("result = %+v, want nothing reaped", res)
	}
	if got := readJob(t, pool, id); got.State != "running" || got.Owner != "worker-1" {
		t.Fatalf("live job was touched: %+v", got)
	}
}

// With max_attempts = 2: the first expiry requeues, the second is final.
func TestReap_DeadAfterMaxAttempts(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{MaxAttempts: 2})

	claimOne(t, pool, "worker-1")
	expireLease(t, pool, id)
	res, err := Reap(ctx, pool, 100)
	if err != nil || res.Requeued != 1 || res.Dead != 0 {
		t.Fatalf("first reap: res=%+v err=%v, want 1 requeued", res, err)
	}

	claimOne(t, pool, "worker-1")
	expireLease(t, pool, id)
	res, err = Reap(ctx, pool, 100)
	if err != nil || res.Requeued != 0 || res.Dead != 1 {
		t.Fatalf("second reap: res=%+v err=%v, want 1 dead", res, err)
	}

	got := readJob(t, pool, id)
	if got.State != "dead" || got.Owner != "" || !got.HasFinished || got.Attempts != 2 {
		t.Fatalf("unexpected row for dead job: %+v", got)
	}

	// A dead job is never claimed again.
	jobs, err := Claim(ctx, pool, "default", "worker-1", 5, 30e9)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("dead job was claimed: jobs=%v err=%v", jobs, err)
	}
}

func TestReap_RecordsLeaseExpiredAttempt(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	claimOne(t, pool, "worker-1")
	expireLease(t, pool, id)
	if _, err := Reap(ctx, pool, 100); err != nil {
		t.Fatalf("Reap: %v", err)
	}

	var worker, outcome string
	var ended bool
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(worker, ''), COALESCE(outcome, ''), ended_at IS NOT NULL
		FROM job_attempts WHERE job_id = $1 AND attempt = 1`, id,
	).Scan(&worker, &outcome, &ended)
	if err != nil {
		t.Fatalf("read attempt row: %v", err)
	}
	if worker != "worker-1" || outcome != "lease_expired" || !ended {
		t.Fatalf("attempt row: worker=%q outcome=%q ended=%v", worker, outcome, ended)
	}
}

// After the reaper takes a job back, the old holder's ack must fail, both
// while the job waits in the queue and after another worker has claimed it.
func TestReap_StaleAckRejected(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	first := claimOne(t, pool, "worker-1")
	expireLease(t, pool, id)
	if _, err := Reap(ctx, pool, 100); err != nil {
		t.Fatalf("Reap: %v", err)
	}

	if err := Ack(ctx, pool, first.ID, "worker-1", first.Fence); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("ack while queued: got %v, want ErrLeaseLost", err)
	}

	second := claimOne(t, pool, "worker-2")
	if err := Ack(ctx, pool, first.ID, "worker-1", first.Fence); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("ack after takeover: got %v, want ErrLeaseLost", err)
	}
	if err := Ack(ctx, pool, second.ID, "worker-2", second.Fence); err != nil {
		t.Fatalf("takeover ack: %v", err)
	}
}

func TestReap_RespectsLimit(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		submitNoop(t, pool, SubmitParams{})
	}
	jobs, err := Claim(ctx, pool, "default", "worker-1", 5, 30e9)
	if err != nil || len(jobs) != 5 {
		t.Fatalf("setup claim: jobs=%d err=%v", len(jobs), err)
	}
	for _, j := range jobs {
		expireLease(t, pool, j.ID)
	}

	res, err := Reap(ctx, pool, 2)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if res.Requeued != 2 {
		t.Fatalf("requeued %d, want 2 (the limit)", res.Requeued)
	}
	if n := countByState(t, pool, "running"); n != 3 {
		t.Fatalf("%d jobs still running, want 3", n)
	}
}

// Several reapers at once: every expired job is handled exactly once.
func TestReap_ConcurrentReapersHandleEachJobOnce(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	const total = 50
	for i := 0; i < total; i++ {
		submitNoop(t, pool, SubmitParams{})
	}
	jobs, err := Claim(ctx, pool, "default", "worker-1", total, 30e9)
	if err != nil || len(jobs) != total {
		t.Fatalf("setup claim: jobs=%d err=%v", len(jobs), err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE jobs SET lease_expires_at = now() - interval '1 second'
		WHERE state = 'running'`); err != nil {
		t.Fatalf("expire all: %v", err)
	}

	var (
		mu       sync.Mutex
		requeued int
		wg       sync.WaitGroup
	)
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				res, err := Reap(ctx, pool, 10)
				if err != nil {
					t.Errorf("Reap: %v", err)
					return
				}
				mu.Lock()
				requeued += res.Requeued
				mu.Unlock()
				if res.Requeued+res.Dead == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	if requeued != total {
		t.Fatalf("reapers requeued %d jobs in total, want %d", requeued, total)
	}
	if n := countByState(t, pool, "queued"); n != total {
		t.Fatalf("%d jobs queued, want %d", n, total)
	}
	var attemptRows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM job_attempts WHERE outcome = 'lease_expired'`).Scan(&attemptRows); err != nil {
		t.Fatal(err)
	}
	if attemptRows != total {
		t.Fatalf("%d lease_expired attempt rows, want %d", attemptRows, total)
	}
}
