package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type idemResult struct {
	id      string
	created bool
	err     error
}

// duplicate_submit: the same request, 100 callers at once. Exactly one job,
// exactly one caller sees created=true, and everyone gets the same ID.
func TestSubmitIdempotent_Duplicate100Parallel(t *testing.T) {
	pool := testPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const n = 100
	results := make([]idemResult, n)
	start := make(chan struct{}) // closed to release every goroutine at once
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			id, created, err := SubmitIdempotent(ctx, pool, "client-a", "dup-key", idemParams())
			results[i] = idemResult{id, created, err} // each goroutine owns one slot
		}()
	}
	close(start)
	wg.Wait()

	creators := 0
	var want string
	for i, r := range results {
		if r.err != nil {
			t.Errorf("caller %d: %v", i, r.err)
			continue
		}
		if r.created {
			creators++
		}
		if want == "" {
			want = r.id
		}
		if r.id != want {
			t.Errorf("caller %d got job %q, others got %q", i, r.id, want)
		}
	}
	if creators != 1 {
		t.Errorf("%d callers saw created=true, want exactly 1", creators)
	}
	if got := idemCount(t, pool, "jobs"); got != 1 {
		t.Errorf("jobs = %d, want 1", got)
	}
	if got := idemCount(t, pool, "idempotency_keys"); got != 1 {
		t.Errorf("keys = %d, want 1", got)
	}
	assertNoOrphanKeys(t, pool)
}

// key_reuse under contention: two different requests share one key, 10
// callers each. One request wins; its callers get the job; the other
// request's callers get a conflict.
func TestSubmitIdempotent_ParallelConflictingRequests(t *testing.T) {
	pool := testPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bodies := []SubmitParams{idemParams(), idemParams()}
	bodies[0].Payload = []byte(`{"v":"A"}`)
	bodies[1].Payload = []byte(`{"v":"B"}`)

	const n = 20
	results := make([]idemResult, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			id, created, err := SubmitIdempotent(ctx, pool, "client-a", "shared-key", bodies[i%2])
			results[i] = idemResult{id, created, err}
		}()
	}
	close(start)
	wg.Wait()

	winner, winnerID := -1, ""
	for i, r := range results {
		if r.created {
			if winner != -1 {
				t.Fatalf("two callers saw created=true (%d and %d)", winner, i)
			}
			winner, winnerID = i, r.id
		}
	}
	if winner == -1 {
		t.Fatal("no caller created the job")
	}
	for i, r := range results {
		if i%2 == winner%2 { // same request as the winner
			if r.err != nil || r.id != winnerID {
				t.Errorf("caller %d (same request): id=%q err=%v, want id=%q", i, r.id, r.err, winnerID)
			}
		} else if !errors.Is(r.err, ErrIdempotencyConflict) {
			t.Errorf("caller %d (other request): err=%v, want ErrIdempotencyConflict", i, r.err)
		}
	}
	if got := idemCount(t, pool, "jobs"); got != 1 {
		t.Errorf("jobs = %d, want 1", got)
	}
}

// waitForLockWaiter polls until some session is blocked on a lock inside our
// key insert. That is positive proof the duplicate is waiting, which is
// stronger than sleeping and hoping.
func waitForLockWaiter(t *testing.T, q Querier) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		err := q.QueryRow(context.Background(), `
			SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database()
			  AND pid <> pg_backend_pid()
			  AND wait_event_type = 'Lock'
			  AND query LIKE '%INSERT INTO idempotency_keys%'`).Scan(&n)
		if err != nil {
			t.Fatalf("lock waiter query: %v", err)
		}
		if n > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no submit is waiting on a lock: the duplicate did not block")
}

// startDuplicate runs a SubmitIdempotent on its own goroutine and delivers
// its result on the returned channel.
func startDuplicate(ctx context.Context, q Querier, key string) <-chan idemResult {
	ch := make(chan idemResult, 1)
	go func() {
		id, created, err := SubmitIdempotent(ctx, q, "client-a", key, idemParams())
		ch <- idemResult{id, created, err}
	}()
	return ch
}

func receiveWithin(t *testing.T, ch <-chan idemResult, d time.Duration) idemResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(d):
		t.Fatal("duplicate submit did not return in time")
		return idemResult{}
	}
}

// A duplicate blocks while the first submission's transaction is open, then
// returns the original job once that transaction commits.
func TestSubmitIdempotent_DuplicateBlocksUntilFirstCommits(t *testing.T) {
	pool := testPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)

	first, created, err := SubmitIdempotent(ctx, tx, "client-a", "slow-key", idemParams())
	if err != nil || !created {
		t.Fatalf("first: created=%v err=%v", created, err)
	}

	ch := startDuplicate(ctx, pool, "slow-key")
	waitForLockWaiter(t, pool)
	select {
	case r := <-ch:
		t.Fatalf("duplicate returned while the first transaction was open: %+v", r)
	default:
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	r := receiveWithin(t, ch, 5*time.Second)
	if r.err != nil || r.created || r.id != first {
		t.Fatalf("duplicate: id=%q created=%v err=%v, want id=%q created=false", r.id, r.created, r.err, first)
	}
	if got := idemCount(t, pool, "jobs"); got != 1 {
		t.Fatalf("jobs = %d, want 1", got)
	}
}

// If the first transaction rolls back, nothing was stored, so the blocked
// duplicate proceeds and becomes the creator.
func TestSubmitIdempotent_BlockedDuplicateCreatesIfFirstRollsBack(t *testing.T) {
	pool := testPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)

	first, created, err := SubmitIdempotent(ctx, tx, "client-a", "abort-key", idemParams())
	if err != nil || !created {
		t.Fatalf("first: created=%v err=%v", created, err)
	}

	ch := startDuplicate(ctx, pool, "abort-key")
	waitForLockWaiter(t, pool)

	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	r := receiveWithin(t, ch, 5*time.Second)
	if r.err != nil || !r.created {
		t.Fatalf("duplicate: created=%v err=%v, want created=true", r.created, r.err)
	}
	if r.id == first {
		t.Fatal("duplicate reused the rolled-back job's ID")
	}
	if got := idemCount(t, pool, "jobs"); got != 1 {
		t.Fatalf("jobs = %d, want 1", got)
	}
	assertNoOrphanKeys(t, pool)
}

// Anything but READ COMMITTED is refused, and the caller's rollback leaves
// nothing behind.
func TestSubmitIdempotent_RefusesHigherIsolationLevels(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	for name, level := range map[string]pgx.TxIsoLevel{
		"repeatable read": pgx.RepeatableRead,
		"serializable":    pgx.Serializable,
	} {
		t.Run(name, func(t *testing.T) {
			tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: level})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = SubmitIdempotent(ctx, tx, "client-a", "iso-key", idemParams())
			if !errors.Is(err, ErrUnsupportedIsolation) {
				t.Errorf("err = %v, want ErrUnsupportedIsolation", err)
			}
			if err := tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			if got := idemCount(t, pool, "jobs"); got != 0 {
				t.Errorf("jobs = %d after rollback, want 0", got)
			}
		})
	}
}