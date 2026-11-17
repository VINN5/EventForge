package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

type attemptDetail struct {
	Attempt       int
	Fence         int64
	Worker        string
	Outcome       string
	Ended         bool
	DurationMS    *int
	StartedRecent bool
}

func readAttemptByFence(t *testing.T, q Querier, id string, fence int64) attemptDetail {
	t.Helper()
	var d attemptDetail
	err := q.QueryRow(context.Background(), `
		SELECT attempt, fence, COALESCE(worker, ''), COALESCE(outcome, ''),
		       ended_at IS NOT NULL, duration_ms,
		       started_at > now() - interval '1 minute' AND started_at <= now()
		FROM job_attempts WHERE job_id = $1 AND fence = $2`, id, fence,
	).Scan(&d.Attempt, &d.Fence, &d.Worker, &d.Outcome, &d.Ended, &d.DurationMS, &d.StartedRecent)
	if err != nil {
		t.Fatalf("read attempt row for fence %d: %v", fence, err)
	}
	return d
}

// closedAttemptRows counts rows that have an outcome recorded.
func closedAttemptRows(t *testing.T, q Querier, id string) int {
	t.Helper()
	var n int
	err := q.QueryRow(context.Background(),
		`SELECT count(*) FROM job_attempts WHERE job_id = $1 AND outcome IS NOT NULL`, id).Scan(&n)
	if err != nil {
		t.Fatalf("count closed attempt rows: %v", err)
	}
	return n
}

func requireDurationBetween(t *testing.T, d attemptDetail, minMS, maxMS int) {
	t.Helper()
	if d.DurationMS == nil {
		t.Fatalf("duration_ms is NULL, want between %d and %d", minMS, maxMS)
	}
	if *d.DurationMS < minMS || *d.DurationMS > maxMS {
		t.Fatalf("duration_ms = %d, want between %d and %d", *d.DurationMS, minMS, maxMS)
	}
}

func TestClaim_WritesAnOpenAttemptRow(t *testing.T) {
	pool := testPool(t)

	id := submitNoop(t, pool, SubmitParams{})
	job := claimOne(t, pool, "worker-1")

	a := readAttemptByFence(t, pool, id, job.Fence)
	if a.Attempt != 1 || a.Fence != 1 || a.Worker != "worker-1" {
		t.Fatalf("attempt row: %+v", a)
	}
	if a.Outcome != "" || a.Ended || a.DurationMS != nil {
		t.Fatalf("a fresh attempt row must be open: %+v", a)
	}
	if !a.StartedRecent {
		t.Fatal("started_at is not a recent database timestamp")
	}
	if n := countAttemptRows(t, pool, id); n != 1 {
		t.Fatalf("%d attempt rows, want 1", n)
	}
}

func TestClaim_ManyJobsGetOneRowEach(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		submitNoop(t, pool, SubmitParams{})
	}
	jobs, err := Claim(ctx, pool, "default", "worker-1", 20, 30*time.Second)
	if err != nil || len(jobs) != 20 {
		t.Fatalf("claim: jobs=%d err=%v", len(jobs), err)
	}

	var rows, open int
	err = pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE outcome IS NULL) FROM job_attempts`).Scan(&rows, &open)
	if err != nil || rows != 20 || open != 20 {
		t.Fatalf("rows=%d open=%d err=%v, want 20 and 20", rows, open, err)
	}
}

func TestAck_ClosesTheAttemptRow(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	job := claimOne(t, pool, "worker-1")
	time.Sleep(60 * time.Millisecond)

	if err := Ack(ctx, pool, job.ID, "worker-1", job.Fence); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	a := readAttemptByFence(t, pool, id, job.Fence)
	if a.Outcome != "succeeded" || !a.Ended {
		t.Fatalf("attempt row after ack: %+v", a)
	}
	requireDurationBetween(t, a, 50, 10000)
	if n := countAttemptRows(t, pool, id); n != 1 {
		t.Fatalf("%d attempt rows, want 1 (Ack must close the row, not add one)", n)
	}
}

func TestFail_ClosesTheAttemptRowOpenedByClaim(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	job := claimOne(t, pool, "worker-1")
	time.Sleep(60 * time.Millisecond)

	_, err := Fail(ctx, pool, job.ID, "worker-1", job.Fence,
		FailParams{Class: ClassRetryable, Message: "boom"})
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}

	a := readAttemptByFence(t, pool, id, job.Fence)
	if a.Outcome != "retryable_failure" || !a.Ended || a.Worker != "worker-1" {
		t.Fatalf("attempt row after fail: %+v", a)
	}
	requireDurationBetween(t, a, 50, 10000)
	if n := countAttemptRows(t, pool, id); n != 1 {
		t.Fatalf("%d attempt rows, want 1", n)
	}
}

func TestReap_ClosesTheAttemptRowOpenedByClaim(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	job := claimOne(t, pool, "worker-1")
	time.Sleep(60 * time.Millisecond)
	expireLease(t, pool, id)

	if _, err := Reap(ctx, pool, 10); err != nil {
		t.Fatalf("Reap: %v", err)
	}

	a := readAttemptByFence(t, pool, id, job.Fence)
	if a.Outcome != "lease_expired" || !a.Ended || a.Worker != "worker-1" {
		t.Fatalf("attempt row after reap: %+v", a)
	}
	requireDurationBetween(t, a, 50, 10000)
	if n := countAttemptRows(t, pool, id); n != 1 {
		t.Fatalf("%d attempt rows, want 1", n)
	}
}

// Each claim is its own row, even when the job comes back and is claimed by someone else.
func TestClaim_EveryClaimGetsItsOwnRow(t *testing.T) {
	pool := testPool(t)

	id := submitNoop(t, pool, SubmitParams{})
	first := claimOne(t, pool, "worker-1")
	requeue(t, pool, id) // raw requeue: does not close the first row
	second := claimOne(t, pool, "worker-2")

	if n := countAttemptRows(t, pool, id); n != 2 {
		t.Fatalf("%d attempt rows, want 2", n)
	}
	a1 := readAttemptByFence(t, pool, id, first.Fence)
	a2 := readAttemptByFence(t, pool, id, second.Fence)
	if a1.Attempt != 1 || a1.Worker != "worker-1" || a1.Outcome != "" {
		t.Fatalf("first row: %+v", a1)
	}
	if a2.Attempt != 2 || a2.Worker != "worker-2" || a2.Outcome != "" {
		t.Fatalf("second row: %+v", a2)
	}
}

// The reason attempt rows are keyed by fence: a release lowers attempts, so the
// next claim has the same attempt number, and must still get a row of its own.
func TestClaim_AfterReleaseSameAttemptNumberGetsNewRow(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	first := claimOne(t, pool, "worker-1")
	_, err := pool.Exec(ctx, `
		UPDATE jobs
		SET state = 'queued', lease_owner = NULL, lease_expires_at = NULL,
		    attempts = attempts - 1
		WHERE id = $1`, id)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	second := claimOne(t, pool, "worker-1")

	if first.Attempts != second.Attempts || first.Fence == second.Fence {
		t.Fatalf("test setup: attempts %d/%d, fences %d/%d", first.Attempts, second.Attempts, first.Fence, second.Fence)
	}
	if n := countAttemptRows(t, pool, id); n != 2 {
		t.Fatalf("%d attempt rows, want 2", n)
	}
	for _, f := range []int64{first.Fence, second.Fence} {
		if a := readAttemptByFence(t, pool, id, f); a.Attempt != 1 {
			t.Fatalf("row for fence %d has attempt %d, want 1", f, a.Attempt)
		}
	}
}

// A rejected Ack must not close anyone's attempt row.
func TestAck_RejectedAckLeavesRowsAlone(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	first := claimOne(t, pool, "worker-1")
	requeue(t, pool, id)
	second := claimOne(t, pool, "worker-2")

	if err := Ack(ctx, pool, first.ID, "worker-1", first.Fence); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale ack: got %v, want ErrLeaseLost", err)
	}
	if n := closedAttemptRows(t, pool, id); n != 0 {
		t.Fatalf("a rejected Ack closed %d attempt rows", n)
	}

	if err := Ack(ctx, pool, second.ID, "worker-2", second.Fence); err != nil {
		t.Fatalf("current ack: %v", err)
	}
	if a := readAttemptByFence(t, pool, id, second.Fence); a.Outcome != "succeeded" {
		t.Fatalf("second row: %+v", a)
	}
	// The first row stays open only because this test bypassed the reaper.
	if a := readAttemptByFence(t, pool, id, first.Fence); a.Outcome != "" {
		t.Fatalf("first row was touched by the stale ack: %+v", a)
	}
}
