package core

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// RunOnce(ctx, db, jobID, name, effect) runs effect at most once per
// (jobID, name): the effect and its marker row commit together or not at all.
// It returns ran=true only if this call executed the effect and committed it.

var errProbeBoom = errors.New("boom")

// The probe table stands in for "the application's own data": effects write a
// row here, so the tests can count how often an effect really committed.
func ensureProbeTable(t *testing.T, q Querier) {
	t.Helper()
	ctx := context.Background()
	if _, err := q.Exec(ctx, `CREATE TABLE IF NOT EXISTS effect_probe (
		id serial PRIMARY KEY, note text NOT NULL)`); err != nil {
		t.Fatalf("create probe table: %v", err)
	}
	if _, err := q.Exec(ctx, `TRUNCATE effect_probe`); err != nil {
		t.Fatalf("truncate probe table: %v", err)
	}
}

func probeEffect(note string) func(ctx context.Context, tx pgx.Tx) error {
	return func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO effect_probe (note) VALUES ($1)`, note)
		return err
	}
}

const runOnceJob = "6f1c2b0e-0000-4000-8000-000000000001"

func TestRunOnce_FirstCallRunsTheEffectAndMarks(t *testing.T) {
	pool := testPool(t)
	ensureProbeTable(t, pool)

	ran, err := RunOnce(context.Background(), pool, runOnceJob, "main", probeEffect("x"))
	if err != nil || !ran {
		t.Fatalf("ran=%v err=%v, want ran=true", ran, err)
	}
	if n := idemCount(t, pool, "effect_probe"); n != 1 {
		t.Fatalf("probe rows = %d, want 1", n)
	}
	if n := idemCount(t, pool, "job_effects"); n != 1 {
		t.Fatalf("markers = %d, want 1", n)
	}
}

func TestRunOnce_SecondCallSkipsTheEffect(t *testing.T) {
	pool := testPool(t)
	ensureProbeTable(t, pool)
	ctx := context.Background()

	if _, err := RunOnce(ctx, pool, runOnceJob, "main", probeEffect("first")); err != nil {
		t.Fatal(err)
	}
	called := false
	ran, err := RunOnce(ctx, pool, runOnceJob, "main", func(ctx context.Context, tx pgx.Tx) error {
		called = true
		return nil
	})
	if err != nil || ran || called {
		t.Fatalf("ran=%v called=%v err=%v, want ran=false and the effect not called", ran, called, err)
	}
	if n := idemCount(t, pool, "effect_probe"); n != 1 {
		t.Fatalf("probe rows = %d, want 1", n)
	}
}

// A failed effect must leave no trace: neither its own writes nor the marker.
// A retry then runs it again.
func TestRunOnce_FailedEffectRollsBackEverything(t *testing.T) {
	pool := testPool(t)
	ensureProbeTable(t, pool)
	ctx := context.Background()

	ran, err := RunOnce(ctx, pool, runOnceJob, "main", func(ctx context.Context, tx pgx.Tx) error {
		if err := probeEffect("half-done")(ctx, tx); err != nil {
			return err
		}
		return errProbeBoom
	})
	if !errors.Is(err, errProbeBoom) || ran {
		t.Fatalf("ran=%v err=%v, want ran=false and errProbeBoom", ran, err)
	}
	if n := idemCount(t, pool, "effect_probe"); n != 0 {
		t.Fatalf("probe rows = %d after a failed effect, want 0", n)
	}
	if n := idemCount(t, pool, "job_effects"); n != 0 {
		t.Fatalf("markers = %d after a failed effect, want 0", n)
	}

	ran, err = RunOnce(ctx, pool, runOnceJob, "main", probeEffect("retry"))
	if err != nil || !ran {
		t.Fatalf("retry: ran=%v err=%v, want ran=true", ran, err)
	}
	if n := idemCount(t, pool, "effect_probe"); n != 1 {
		t.Fatalf("probe rows = %d, want 1", n)
	}
}

// A panic inside the effect must also roll back, and must not leak the
// transaction. RunOnce lets the panic continue upward (the worker already
// recovers handler panics).
func TestRunOnce_PanicRollsBackAndPropagates(t *testing.T) {
	pool := testPool(t)
	ensureProbeTable(t, pool)
	ctx := context.Background()

	func() {
		defer func() {
			if recover() == nil {
				t.Error("expected the panic to propagate out of RunOnce")
			}
		}()
		_, _ = RunOnce(ctx, pool, runOnceJob, "main", func(ctx context.Context, tx pgx.Tx) error {
			_ = probeEffect("half-done")(ctx, tx)
			panic("effect exploded")
		})
	}()

	if n := idemCount(t, pool, "effect_probe"); n != 0 {
		t.Fatalf("probe rows = %d after a panic, want 0", n)
	}
	if n := idemCount(t, pool, "job_effects"); n != 0 {
		t.Fatalf("markers = %d after a panic, want 0", n)
	}
	ran, err := RunOnce(ctx, pool, runOnceJob, "main", probeEffect("after"))
	if err != nil || !ran {
		t.Fatalf("after panic: ran=%v err=%v, want ran=true", ran, err)
	}
}

func TestRunOnce_NamesAndJobsAreIndependent(t *testing.T) {
	pool := testPool(t)
	ensureProbeTable(t, pool)
	ctx := context.Background()

	for _, c := range []struct{ job, name string }{
		{runOnceJob, "a"},
		{runOnceJob, "b"},
		{"6f1c2b0e-0000-4000-8000-000000000002", "a"},
	} {
		ran, err := RunOnce(ctx, pool, c.job, c.name, probeEffect(c.name))
		if err != nil || !ran {
			t.Fatalf("%v: ran=%v err=%v, want ran=true", c, ran, err)
		}
	}
	if n := idemCount(t, pool, "effect_probe"); n != 3 {
		t.Fatalf("probe rows = %d, want 3", n)
	}
}

func TestRunOnce_RejectsBadArguments(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	noop := func(ctx context.Context, tx pgx.Tx) error { return nil }

	cases := map[string]struct {
		job, name string
		fn        func(ctx context.Context, tx pgx.Tx) error
	}{
		"empty job id": {"", "main", noop},
		"not a uuid":   {"nope", "main", noop},
		"empty name":   {runOnceJob, "", noop},
		"nil effect":   {runOnceJob, "main", nil},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if ran, err := RunOnce(ctx, pool, c.job, c.name, c.fn); err == nil || ran {
				t.Fatalf("ran=%v err=%v, want an error", ran, err)
			}
		})
	}
	if n := idemCount(t, pool, "job_effects"); n != 0 {
		t.Fatalf("bad calls left %d markers", n)
	}
}

// Many workers racing on one job: the effect commits exactly once.
func TestRunOnce_ConcurrentCallersRunTheEffectOnce(t *testing.T) {
	pool := testPool(t)
	ensureProbeTable(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const n = 20
	var ranCount, calls atomic.Int32
	errs := make([]error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ran, err := RunOnce(ctx, pool, runOnceJob, "main", func(ctx context.Context, tx pgx.Tx) error {
				calls.Add(1)
				time.Sleep(20 * time.Millisecond) // widen the race window
				return probeEffect("racer")(ctx, tx)
			})
			errs[i] = err
			if ran {
				ranCount.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: %v", i, err)
		}
	}
	if ranCount.Load() != 1 || calls.Load() != 1 {
		t.Errorf("ran=%d effect calls=%d, want 1 and 1", ranCount.Load(), calls.Load())
	}
	if got := idemCount(t, pool, "effect_probe"); got != 1 {
		t.Errorf("probe rows = %d, want 1", got)
	}
}

// waitForLockOn polls until a session is blocked on a lock inside a statement
// containing fragment: positive proof that a caller is waiting.
func waitForLockOn(t *testing.T, q Querier, fragment string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		err := q.QueryRow(context.Background(), `
			SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database()
			  AND pid <> pg_backend_pid()
			  AND wait_event_type = 'Lock'
			  AND query LIKE $1`, "%"+fragment+"%").Scan(&n)
		if err != nil {
			t.Fatalf("lock waiter query: %v", err)
		}
		if n > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("nobody is waiting on a lock in %q", fragment)
}

type onceResult struct {
	ran bool
	err error
}

// holdFirstCaller starts a RunOnce whose effect waits for release, then
// finishes with finishWith. It returns once the effect is running (so the
// marker is inserted but uncommitted).
func holdFirstCaller(t *testing.T, ctx context.Context, db TxBeginner, release <-chan struct{}, finishWith error) <-chan onceResult {
	t.Helper()
	inEffect := make(chan struct{})
	out := make(chan onceResult, 1)
	go func() {
		ran, err := RunOnce(ctx, db, runOnceJob, "main", func(ctx context.Context, tx pgx.Tx) error {
			if err := probeEffect("first")(ctx, tx); err != nil {
				return err
			}
			close(inEffect)
			<-release
			return finishWith
		})
		out <- onceResult{ran, err}
	}()
	select {
	case <-inEffect:
	case <-time.After(5 * time.Second):
		t.Fatal("first caller never reached its effect")
	}
	return out
}

func startSecondCaller(ctx context.Context, db TxBeginner, called *atomic.Bool) <-chan onceResult {
	out := make(chan onceResult, 1)
	go func() {
		ran, err := RunOnce(ctx, db, runOnceJob, "main", func(ctx context.Context, tx pgx.Tx) error {
			called.Store(true)
			return probeEffect("second")(ctx, tx)
		})
		out <- onceResult{ran, err}
	}()
	return out
}

func receiveOnce(t *testing.T, ch <-chan onceResult) onceResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("RunOnce did not return in time")
		return onceResult{}
	}
}

// First caller commits: the waiting second caller must skip its effect.
func TestRunOnce_WaiterSkipsWhenFirstCommits(t *testing.T) {
	pool := testPool(t)
	ensureProbeTable(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	release := make(chan struct{})
	first := holdFirstCaller(t, ctx, pool, release, nil)
	var secondCalled atomic.Bool
	second := startSecondCaller(ctx, pool, &secondCalled)
	waitForLockOn(t, pool, "INSERT INTO job_effects")

	close(release)
	if r := receiveOnce(t, first); r.err != nil || !r.ran {
		t.Fatalf("first: %+v, want ran=true", r)
	}
	if r := receiveOnce(t, second); r.err != nil || r.ran || secondCalled.Load() {
		t.Fatalf("second: %+v called=%v, want ran=false and effect not called", r, secondCalled.Load())
	}
	if n := idemCount(t, pool, "effect_probe"); n != 1 {
		t.Fatalf("probe rows = %d, want 1", n)
	}
}

// First caller fails: nothing was stored, so the waiter takes over.
func TestRunOnce_WaiterRunsWhenFirstRollsBack(t *testing.T) {
	pool := testPool(t)
	ensureProbeTable(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	release := make(chan struct{})
	first := holdFirstCaller(t, ctx, pool, release, errProbeBoom)
	var secondCalled atomic.Bool
	second := startSecondCaller(ctx, pool, &secondCalled)
	waitForLockOn(t, pool, "INSERT INTO job_effects")

	close(release)
	if r := receiveOnce(t, first); !errors.Is(r.err, errProbeBoom) || r.ran {
		t.Fatalf("first: %+v, want errProbeBoom", r)
	}
	if r := receiveOnce(t, second); r.err != nil || !r.ran || !secondCalled.Load() {
		t.Fatalf("second: %+v called=%v, want ran=true", r, secondCalled.Load())
	}
	if n := idemCount(t, pool, "effect_probe"); n != 1 {
		t.Fatalf("probe rows = %d, want 1 (only the second caller's)", n)
	}
	if n := idemCount(t, pool, "job_effects"); n != 1 {
		t.Fatalf("markers = %d, want 1", n)
	}
}

// Inside the application's own transaction, RunOnce joins it (a savepoint):
// rolling the outer transaction back undoes the effect and its marker.
func TestRunOnce_JoinsTheCallersTransaction(t *testing.T) {
	pool := testPool(t)
	ensureProbeTable(t, pool)
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)

	ran, err := RunOnce(ctx, tx, runOnceJob, "main", probeEffect("inside"))
	if err != nil || !ran {
		t.Fatalf("ran=%v err=%v, want ran=true", ran, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if n := idemCount(t, pool, "effect_probe"); n != 0 {
		t.Fatalf("probe rows = %d after the outer rollback, want 0", n)
	}
	if n := idemCount(t, pool, "job_effects"); n != 0 {
		t.Fatalf("markers = %d after the outer rollback, want 0", n)
	}
}