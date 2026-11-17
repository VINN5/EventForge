package core

import (
	"context"
	"errors"
	"testing"
)

func TestClaim_ReturnsIncreasingFence(t *testing.T) {
	pool := testPool(t)

	id := submitNoop(t, pool, SubmitParams{})
	first := claimOne(t, pool, "worker-1")
	if first.Fence != 1 {
		t.Fatalf("first fence = %d, want 1", first.Fence)
	}

	requeue(t, pool, id)
	second := claimOne(t, pool, "worker-1")
	if second.Fence <= first.Fence {
		t.Fatalf("fence did not increase: %d then %d", first.Fence, second.Fence)
	}
}

// The reason the fence exists: releasing a job gives back an attempt, so
// attempts can repeat. The fencing token must still be different.
func TestFence_NotReusedWhenAttemptsGoDown(t *testing.T) {
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
	if second.Attempts != first.Attempts {
		t.Fatalf("test setup: attempts %d and %d should match", first.Attempts, second.Attempts)
	}
	if second.Fence == first.Fence {
		t.Fatalf("fence %d was reused after a release", first.Fence)
	}

	// Same worker, same attempts value, old fence: must be rejected.
	if err := Ack(ctx, pool, first.ID, "worker-1", first.Fence); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale ack: got %v, want ErrLeaseLost", err)
	}
	if err := Ack(ctx, pool, second.ID, "worker-1", second.Fence); err != nil {
		t.Fatalf("current ack: %v", err)
	}
}

// The database itself refuses a fence below attempts.
func TestFence_DatabaseRejectsFenceBelowAttempts(t *testing.T) {
	pool := testPool(t)

	id := submitNoop(t, pool, SubmitParams{})
	claimOne(t, pool, "worker-1")

	_, err := pool.Exec(context.Background(),
		`UPDATE jobs SET fence = 0 WHERE id = $1`, id)
	if err == nil {
		t.Fatal("expected the CHECK constraint to reject fence < attempts")
	}
}
