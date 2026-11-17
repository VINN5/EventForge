package core

import (
	"context"
	"testing"
	"time"
)

// secondsLeft is how long the job's lease has left, on the database clock.
func secondsLeft(t *testing.T, q Querier, id string) float64 {
	t.Helper()
	var s float64
	err := q.QueryRow(context.Background(), `
		SELECT EXTRACT(EPOCH FROM (lease_expires_at - now()))::float8
		FROM jobs WHERE id = $1`, id).Scan(&s)
	if err != nil {
		t.Fatalf("read lease: %v", err)
	}
	return s
}

func ref(j Job) LeaseRef { return LeaseRef{ID: j.ID, Fence: j.Fence} }

func TestExtend_PushesTheLeaseOut(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	jobs, err := Claim(ctx, pool, "default", "worker-1", 1, 2*time.Second)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("setup claim: jobs=%d err=%v", len(jobs), err)
	}
	if left := secondsLeft(t, pool, id); left > 3 {
		t.Fatalf("test setup: lease has %.1fs left, want about 2", left)
	}

	lost, err := Extend(ctx, pool, "worker-1", []LeaseRef{ref(jobs[0])}, time.Hour)
	if err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if len(lost) != 0 {
		t.Fatalf("lost = %+v, want none", lost)
	}
	if left := secondsLeft(t, pool, id); left < 3500 || left > 3700 {
		t.Fatalf("lease has %.0fs left after Extend, want about 3600", left)
	}

	// Extending changes nothing else about the job.
	got := readJob(t, pool, id)
	if got.State != "running" || got.Owner != "worker-1" || got.Attempts != 1 || got.Fence != jobs[0].Fence {
		t.Fatalf("unexpected row: %+v", got)
	}
}

func TestExtend_HandlesManyJobsInOneCall(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		submitNoop(t, pool, SubmitParams{})
	}
	jobs, err := Claim(ctx, pool, "default", "worker-1", 5, 2*time.Second)
	if err != nil || len(jobs) != 5 {
		t.Fatalf("setup claim: jobs=%d err=%v", len(jobs), err)
	}
	refs := make([]LeaseRef, len(jobs))
	for i, j := range jobs {
		refs[i] = ref(j)
	}

	lost, err := Extend(ctx, pool, "worker-1", refs, time.Hour)
	if err != nil || len(lost) != 0 {
		t.Fatalf("Extend: lost=%+v err=%v", lost, err)
	}
	for _, j := range jobs {
		if left := secondsLeft(t, pool, j.ID); left < 3500 {
			t.Fatalf("job %s has only %.0fs left", j.ID, left)
		}
	}
}

func TestExtend_ReportsJobsItCouldNotExtend(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	idA := submitNoop(t, pool, SubmitParams{})
	idB := submitNoop(t, pool, SubmitParams{})
	idC := submitNoop(t, pool, SubmitParams{})
	jobs, err := Claim(ctx, pool, "default", "worker-1", 3, 2*time.Second)
	if err != nil || len(jobs) != 3 {
		t.Fatalf("setup claim: jobs=%d err=%v", len(jobs), err)
	}
	byID := map[string]Job{}
	for _, j := range jobs {
		byID[j.ID] = j
	}

	// A stays ours. B is taken over by another worker. C finishes.
	expireLease(t, pool, idB)
	if _, err := Reap(ctx, pool, 10); err != nil {
		t.Fatal(err)
	}
	takeover, err := ClaimTypes(ctx, pool, "default", "worker-2", nil, 10, 30*time.Second)
	if err != nil || len(takeover) != 1 || takeover[0].ID != idB {
		t.Fatalf("setup takeover: jobs=%+v err=%v", takeover, err)
	}
	if err := Ack(ctx, pool, idC, "worker-1", byID[idC].Fence); err != nil {
		t.Fatalf("setup ack: %v", err)
	}

	lost, err := Extend(ctx, pool, "worker-1",
		[]LeaseRef{ref(byID[idA]), ref(byID[idB]), ref(byID[idC])}, time.Hour)
	if err != nil {
		t.Fatalf("Extend: %v", err)
	}

	got := map[string]int64{}
	for _, l := range lost {
		got[l.ID] = l.Fence
	}
	if len(got) != 2 || got[idB] != byID[idB].Fence || got[idC] != byID[idC].Fence {
		t.Fatalf("lost = %+v, want exactly B and C with their fences", lost)
	}
	if left := secondsLeft(t, pool, idA); left < 3500 {
		t.Fatalf("A should have been extended, has %.0fs left", left)
	}

	// The takeover owner's lease must be untouched by worker-1's attempt.
	if left := secondsLeft(t, pool, idB); left > 60 {
		t.Fatalf("worker-1 extended worker-2's lease: %.0fs left", left)
	}
	if got := readJob(t, pool, idB); got.Owner != "worker-2" {
		t.Fatalf("row B: %+v", got)
	}
}

func TestExtend_WrongWorkerCannotExtend(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	job := claimOne(t, pool, "worker-1")

	lost, err := Extend(ctx, pool, "worker-2", []LeaseRef{ref(job)}, time.Hour)
	if err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if len(lost) != 1 || lost[0].ID != id {
		t.Fatalf("lost = %+v, want the job", lost)
	}
	if left := secondsLeft(t, pool, id); left > 60 {
		t.Fatalf("lease was extended by the wrong worker: %.0fs left", left)
	}
}

// Same worker name, old fence: a restarted process must not extend a lease it
// no longer holds.
func TestExtend_SameWorkerOldFenceIsLost(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	first := claimOne(t, pool, "worker-1")
	requeue(t, pool, id)
	second := claimOne(t, pool, "worker-1")

	lost, err := Extend(ctx, pool, "worker-1", []LeaseRef{ref(first)}, time.Hour)
	if err != nil || len(lost) != 1 {
		t.Fatalf("old fence: lost=%+v err=%v, want 1 lost", lost, err)
	}
	lost, err = Extend(ctx, pool, "worker-1", []LeaseRef{ref(second)}, time.Hour)
	if err != nil || len(lost) != 0 {
		t.Fatalf("current fence: lost=%+v err=%v, want none", lost, err)
	}
}

// A lease that has passed but not been reaped yet can still be extended, the
// same way Ack still works then. The fence decides ownership, not the clock.
func TestExtend_ExpiredButNotYetReapedStillExtends(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	job := claimOne(t, pool, "worker-1")
	expireLease(t, pool, id)

	lost, err := Extend(ctx, pool, "worker-1", []LeaseRef{ref(job)}, time.Hour)
	if err != nil || len(lost) != 0 {
		t.Fatalf("Extend: lost=%+v err=%v, want none", lost, err)
	}

	// And now the reaper must leave it alone.
	res, err := Reap(ctx, pool, 10)
	if err != nil || res.Requeued != 0 || res.Dead != 0 {
		t.Fatalf("Reap: res=%+v err=%v, want nothing", res, err)
	}
	if got := readJob(t, pool, id); got.State != "running" {
		t.Fatalf("state=%q, want running", got.State)
	}
}

// Once the reaper has taken the job back, extending is too late.
func TestExtend_AfterReapIsLost(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	job := claimOne(t, pool, "worker-1")
	expireLease(t, pool, id)
	if _, err := Reap(ctx, pool, 10); err != nil {
		t.Fatal(err)
	}

	lost, err := Extend(ctx, pool, "worker-1", []LeaseRef{ref(job)}, time.Hour)
	if err != nil || len(lost) != 1 {
		t.Fatalf("Extend: lost=%+v err=%v, want 1 lost", lost, err)
	}
	if got := readJob(t, pool, id); got.State != "queued" {
		t.Fatalf("Extend changed a reaped job: %+v", got)
	}
}

func TestExtend_EmptyListAndBadLease(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	lost, err := Extend(ctx, pool, "worker-1", nil, time.Hour)
	if err != nil || len(lost) != 0 {
		t.Fatalf("empty list: lost=%+v err=%v, want nothing", lost, err)
	}

	submitNoop(t, pool, SubmitParams{})
	job := claimOne(t, pool, "worker-1")
	if _, err := Extend(ctx, pool, "worker-1", []LeaseRef{ref(job)}, 0); err == nil {
		t.Fatal("a zero lease should be an error")
	}
	if _, err := Extend(ctx, pool, "", []LeaseRef{ref(job)}, time.Hour); err == nil {
		t.Fatal("an empty worker name should be an error")
	}
}
