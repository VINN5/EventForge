package core

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func submitNoop(t *testing.T, q Querier, p SubmitParams) string {
	t.Helper()
	if p.TenantID == "" {
		p.TenantID = "t1"
	}
	if p.Type == "" {
		p.Type = "noop"
	}
	if p.Payload == nil {
		p.Payload = []byte(`{}`)
	}
	id, err := Submit(context.Background(), q, p)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	return id
}

func TestClaim_PrefersHigherPriority(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	submitNoop(t, pool, SubmitParams{Priority: 0})
	highID := submitNoop(t, pool, SubmitParams{Priority: 10})

	jobs, err := Claim(ctx, pool, "default", "worker-1", 1, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ID != highID {
		t.Fatalf("expected the high-priority job %s, got %+v", highID, jobs)
	}
}

func TestClaim_SkipsJobsScheduledForTheFuture(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	submitNoop(t, pool, SubmitParams{RunAfter: time.Hour})

	jobs, err := Claim(ctx, pool, "default", "worker-1", 5, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Fatalf("claimed %d jobs that are not due yet", len(jobs))
	}
}

func TestClaim_SetsLeaseAndIncrementsAttempts(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})

	jobs, err := Claim(ctx, pool, "default", "worker-1", 1, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].Attempts != 1 || jobs[0].LeaseOwner != "worker-1" {
		t.Fatalf("unexpected claim result: %+v", jobs)
	}

	var state, owner string
	var attempts int
	var leaseInFuture bool
	err = pool.QueryRow(ctx, `
		SELECT state, lease_owner, attempts, lease_expires_at > now()
		FROM jobs WHERE id = $1`, id,
	).Scan(&state, &owner, &attempts, &leaseInFuture)
	if err != nil {
		t.Fatal(err)
	}
	if state != "running" || owner != "worker-1" || attempts != 1 || !leaseInFuture {
		t.Fatalf("state=%q owner=%q attempts=%d leaseInFuture=%v", state, owner, attempts, leaseInFuture)
	}
}

// The core promise: many workers, each job claimed exactly once.
func TestClaim_ConcurrentWorkersNeverShareJobs(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	const total = 200
	for i := 0; i < total; i++ {
		submitNoop(t, pool, SubmitParams{})
	}

	var (
		mu      sync.Mutex
		claimed = map[string]int{}
		wg      sync.WaitGroup
	)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(worker string) {
			defer wg.Done()
			for {
				jobs, err := Claim(ctx, pool, "default", worker, 5, 30*time.Second)
				if err != nil {
					t.Errorf("%s: claim: %v", worker, err)
					return
				}
				if len(jobs) == 0 {
					return
				}
				mu.Lock()
				for _, j := range jobs {
					claimed[j.ID]++
				}
				mu.Unlock()
			}
		}(fmt.Sprintf("worker-%d", w))
	}
	wg.Wait()

	if len(claimed) != total {
		t.Fatalf("claimed %d distinct jobs, want %d", len(claimed), total)
	}
	for id, n := range claimed {
		if n != 1 {
			t.Errorf("job %s was claimed %d times", id, n)
		}
	}
}
