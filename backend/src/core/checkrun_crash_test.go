package core

import (
	"context"
	"testing"
	"time"
)

// killUntilDead drives a job to death the way a poison payload does: it is
// claimed, the worker dies (the lease is forced to expire), the reaper takes
// it back, and so on until max_attempts is used up.
func killUntilDead(t *testing.T, q Querier, typ string, maxAttempts int) string {
	t.Helper()
	ctx := context.Background()

	id := submitNoop(t, q, SubmitParams{Type: typ, MaxAttempts: maxAttempts})
	for i := 0; i < maxAttempts; i++ {
		claimType(t, q, typ)
		expireLease(t, q, id)
		if _, err := Reap(ctx, q, 10); err != nil {
			t.Fatalf("Reap: %v", err)
		}
	}
	return id
}

func TestCheckRun_JobsThatDiedByCrashingAreAccepted(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	healthyRun(t, pool, 3)
	killUntilDead(t, pool, "poison", 3)
	killUntilDead(t, pool, "poison", 3)

	r, err := CheckRun(ctx, pool, CheckOptions{ExpectedDead: 2, DeadByCrashOnly: true})
	if err != nil {
		t.Fatalf("CheckRun: %v", err)
	}
	requireNoProblems(t, r)
	if r.Total != 5 || r.Succeeded != 3 || r.Dead != 2 || r.Unfinished != 0 {
		t.Fatalf("job counts: %+v", r)
	}
	if r.LeaseExpiredAttempts != 6 || r.DeadNotFromCrashes != 0 {
		t.Fatalf("attempt counts: %+v", r)
	}
}

// A job that died for another reason must not pass as a crash victim.
func TestCheckRun_DeadFromAFailureIsNotACrashVictim(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	healthyRun(t, pool, 3)
	makeDead(t, pool, "perm", "t1", "invalid address", time.Minute)

	r, err := CheckRun(ctx, pool, CheckOptions{AllowDead: true, DeadByCrashOnly: true})
	if err != nil {
		t.Fatalf("CheckRun: %v", err)
	}
	requireProblem(t, r, "did not die from expired leases")
	if r.DeadNotFromCrashes != 1 {
		t.Fatalf("DeadNotFromCrashes = %d, want 1", r.DeadNotFromCrashes)
	}

	// Without the crash-only rule the same dead job is acceptable.
	lenient, err := CheckRun(ctx, pool, CheckOptions{AllowDead: true})
	if err != nil {
		t.Fatalf("CheckRun: %v", err)
	}
	requireNoProblems(t, lenient)
}

// One crash short of max_attempts is not "dead from crashing": the job must have
// used every attempt.
func TestCheckRun_DeadWithAttemptsToSpareIsNotACrashVictim(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	healthyRun(t, pool, 2)

	id := killUntilDead(t, pool, "poison", 3)
	// Remove one attempt row, as if the history were incomplete.
	exec(t, pool, `DELETE FROM job_attempts WHERE job_id = $1 AND attempt = 3`, id)

	r, err := CheckRun(ctx, pool, CheckOptions{AllowDead: true, DeadByCrashOnly: true})
	if err != nil {
		t.Fatalf("CheckRun: %v", err)
	}
	requireProblem(t, r, "did not die from expired leases")
}

func TestCheckRun_ExpectedDeadCountMustMatch(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	healthyRun(t, pool, 3)
	killUntilDead(t, pool, "poison", 3)

	r, err := CheckRun(ctx, pool, CheckOptions{ExpectedDead: 2, DeadByCrashOnly: true})
	if err != nil {
		t.Fatalf("CheckRun: %v", err)
	}
	requireProblem(t, r, "expected 2 dead jobs, found 1")
}
