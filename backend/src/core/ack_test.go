package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

type jobRow struct {
	State       string
	Owner       string
	Attempts    int
	Fence       int64
	HasFinished bool
}

func readJob(t *testing.T, q Querier, id string) jobRow {
	t.Helper()
	var r jobRow
	err := q.QueryRow(context.Background(), `
		SELECT state, COALESCE(lease_owner, ''), attempts, fence, finished_at IS NOT NULL
		FROM jobs WHERE id = $1`, id,
	).Scan(&r.State, &r.Owner, &r.Attempts, &r.Fence, &r.HasFinished)
	if err != nil {
		t.Fatalf("read job: %v", err)
	}
	return r
}

func claimOne(t *testing.T, q Querier, worker string) Job {
	t.Helper()
	jobs, err := Claim(context.Background(), q, "default", worker, 1, 30*time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("claim by %s returned %d jobs, want 1", worker, len(jobs))
	}
	return jobs[0]
}

// requeue simulates what the reaper does when a lease expires.
func requeue(t *testing.T, q Querier, id string) {
	t.Helper()
	_, err := q.Exec(context.Background(), `
		UPDATE jobs SET state = 'queued', lease_owner = NULL, lease_expires_at = NULL
		WHERE id = $1`, id)
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
}

func TestAck_CompletesRunningJob(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	job := claimOne(t, pool, "worker-1")

	if err := Ack(ctx, pool, job.ID, "worker-1", job.Fence); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	got := readJob(t, pool, id)
	if got.State != "succeeded" || got.Owner != "" || !got.HasFinished {
		t.Fatalf("unexpected row after ack: %+v", got)
	}
}

// A slow worker whose lease expired must not overwrite the worker that took over.
func TestAck_StaleWorkerCannotOverwriteTakeover(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	first := claimOne(t, pool, "worker-1")

	requeue(t, pool, id)
	second := claimOne(t, pool, "worker-2")
	if second.Attempts != 2 {
		t.Fatalf("second claim has attempts=%d, want 2", second.Attempts)
	}

	// worker-1 wakes up late and tries to report success.
	err := Ack(ctx, pool, first.ID, "worker-1", first.Fence)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale ack: got %v, want ErrLeaseLost", err)
	}
	got := readJob(t, pool, id)
	if got.State != "running" || got.Owner != "worker-2" || got.Attempts != 2 {
		t.Fatalf("stale ack changed the row: %+v", got)
	}

	// The current holder can still finish the job.
	if err := Ack(ctx, pool, second.ID, "worker-2", second.Fence); err != nil {
		t.Fatalf("current ack: %v", err)
	}
	if got := readJob(t, pool, id); got.State != "succeeded" {
		t.Fatalf("state=%q, want succeeded", got.State)
	}
}

// The owner name alone is not enough: the same worker re-claiming the job
// gets a new fence, and the old one must stop working.
func TestAck_SameWorkerOldFenceRejected(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	first := claimOne(t, pool, "worker-1")
	requeue(t, pool, id)
	second := claimOne(t, pool, "worker-1")

	err := Ack(ctx, pool, first.ID, "worker-1", first.Fence)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("old fence ack: got %v, want ErrLeaseLost", err)
	}
	if err := Ack(ctx, pool, second.ID, "worker-1", second.Fence); err != nil {
		t.Fatalf("new fence ack: %v", err)
	}
}

func TestAck_WrongWorkerRejected(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	job := claimOne(t, pool, "worker-1")

	err := Ack(ctx, pool, job.ID, "worker-2", job.Fence)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("got %v, want ErrLeaseLost", err)
	}
	if got := readJob(t, pool, id); got.State != "running" {
		t.Fatalf("state=%q, want running", got.State)
	}
}

func TestAck_SecondAckRejected(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	submitNoop(t, pool, SubmitParams{})
	job := claimOne(t, pool, "worker-1")

	if err := Ack(ctx, pool, job.ID, "worker-1", job.Fence); err != nil {
		t.Fatal(err)
	}
	err := Ack(ctx, pool, job.ID, "worker-1", job.Fence)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("second ack: got %v, want ErrLeaseLost", err)
	}
}
