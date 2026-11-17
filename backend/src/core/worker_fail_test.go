package core

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fastRetryConfig makes retries happen in milliseconds instead of seconds.
func fastRetryConfig(id string) WorkerConfig {
	return WorkerConfig{
		ID:           id,
		Concurrency:  4,
		PollInterval: 20 * time.Millisecond,
		BackoffBase:  time.Millisecond,
		BackoffCap:   5 * time.Millisecond,
	}
}

func stopWorker(t *testing.T, cancel func(), done <-chan error) {
	t.Helper()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestWorker_RetriesFailedJobUntilItSucceeds(t *testing.T) {
	pool := testPool(t)
	id := submitNoop(t, pool, SubmitParams{})

	var calls atomic.Int32
	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error {
			if calls.Add(1) < 3 {
				return errors.New("flaky downstream")
			}
			return nil
		},
	}
	cancel, done := startWorker(t, pool, fastRetryConfig("w1"), handlers)
	waitFor(t, 10*time.Second, func() bool {
		return readJob(t, pool, id).State == "succeeded"
	}, "the job to succeed on its third attempt")
	stopWorker(t, cancel, done)

	got := readJob(t, pool, id)
	if got.Attempts != 3 || calls.Load() != 3 {
		t.Fatalf("attempts=%d handler calls=%d, want 3 and 3", got.Attempts, calls.Load())
	}
	for attempt := 1; attempt <= 2; attempt++ {
		a := readAttempt(t, pool, id, attempt)
		if a.Outcome != "retryable_failure" || !strings.Contains(a.Error, "flaky downstream") {
			t.Fatalf("attempt %d row: %+v", attempt, a)
		}
	}
}

func TestWorker_PanicIsARetryableFailure(t *testing.T) {
	pool := testPool(t)
	id := submitNoop(t, pool, SubmitParams{})

	var calls atomic.Int32
	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error {
			if calls.Add(1) == 1 {
				panic("kaboom")
			}
			return nil
		},
	}
	cancel, done := startWorker(t, pool, fastRetryConfig("w1"), handlers)
	waitFor(t, 10*time.Second, func() bool {
		return readJob(t, pool, id).State == "succeeded"
	}, "the job to succeed after the panic")
	stopWorker(t, cancel, done)

	if got := readJob(t, pool, id); got.Attempts != 2 {
		t.Fatalf("attempts=%d, want 2", got.Attempts)
	}
	a := readAttempt(t, pool, id, 1)
	if a.Outcome != "retryable_failure" ||
		!strings.Contains(a.Error, "handler panic") || !strings.Contains(a.Error, "kaboom") {
		t.Fatalf("attempt 1 row: %+v", a)
	}
}

func TestWorker_PermanentErrorDeadLettersAtOnce(t *testing.T) {
	pool := testPool(t)
	id := submitNoop(t, pool, SubmitParams{}) // default max_attempts is 8

	var calls atomic.Int32
	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error {
			calls.Add(1)
			return Permanent(errors.New("bad address"))
		},
	}
	cancel, done := startWorker(t, pool, fastRetryConfig("w1"), handlers)
	waitFor(t, 10*time.Second, func() bool {
		return readJob(t, pool, id).State == "dead"
	}, "the job to be dead-lettered")
	time.Sleep(200 * time.Millisecond) // a wrongly retried job would run again here
	stopWorker(t, cancel, done)

	if got := readJob(t, pool, id); got.State != "dead" || got.Attempts != 1 {
		t.Fatalf("row: %+v, want dead after 1 attempt", got)
	}
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", calls.Load())
	}
	if a := readAttempt(t, pool, id, 1); a.Outcome != "permanent_failure" || !strings.Contains(a.Error, "bad address") {
		t.Fatalf("attempt row: %+v", a)
	}
	if msg := readLastError(t, pool, id); !strings.Contains(msg, "bad address") {
		t.Fatalf("last_error = %q", msg)
	}
}

// A downstream that says "try again in an hour" must be obeyed, even though the
// computed backoff is only a few milliseconds.
func TestWorker_RetryAfterIsHonoured(t *testing.T) {
	pool := testPool(t)
	id := submitNoop(t, pool, SubmitParams{})

	var calls atomic.Int32
	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error {
			calls.Add(1)
			return RetryableAfter(errors.New("rate limited"), time.Hour)
		},
	}
	cancel, done := startWorker(t, pool, fastRetryConfig("w1"), handlers)
	waitFor(t, 10*time.Second, func() bool {
		got := readJob(t, pool, id)
		return got.State == "queued" && got.Attempts == 1
	}, "the job to fail once and wait")
	time.Sleep(200 * time.Millisecond)
	stopWorker(t, cancel, done)

	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", calls.Load())
	}
	var about bool
	err := pool.QueryRow(context.Background(), `
		SELECT run_at > now() + interval '50 minutes'
		   AND run_at < now() + interval '70 minutes'
		FROM jobs WHERE id = $1`, id).Scan(&about)
	if err != nil || !about {
		t.Fatalf("run_at is not about an hour away (err %v)", err)
	}
}

// poison_job: jobs that always fail, by error or by panic, die after max_attempts
// and do not slow down or block the healthy jobs around them.
func TestWorker_PoisonJobsDieWithoutHurtingOthers(t *testing.T) {
	pool := testPool(t)

	const healthy = 20
	for i := 0; i < healthy; i++ {
		submitNoop(t, pool, SubmitParams{})
	}
	errID := submitNoop(t, pool, SubmitParams{Type: "poison_err", MaxAttempts: 3})
	panicID := submitNoop(t, pool, SubmitParams{Type: "poison_panic", MaxAttempts: 3})

	var errCalls, panicCalls atomic.Int32
	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error { return nil },
		"poison_err": func(ctx context.Context, job Job) error {
			errCalls.Add(1)
			return errors.New("always fails")
		},
		"poison_panic": func(ctx context.Context, job Job) error {
			panicCalls.Add(1)
			panic("always panics")
		},
	}
	cancel, done := startWorker(t, pool, fastRetryConfig("w1"), handlers)
	waitFor(t, 20*time.Second, func() bool {
		return countByState(t, pool, "succeeded") == healthy && countByState(t, pool, "dead") == 2
	}, "healthy jobs to succeed and both poison jobs to die")
	time.Sleep(200 * time.Millisecond) // nothing may run again after dying
	stopWorker(t, cancel, done)

	for name, p := range map[string]struct {
		id    string
		calls int32
	}{
		"poison_err":   {errID, errCalls.Load()},
		"poison_panic": {panicID, panicCalls.Load()},
	} {
		got := readJob(t, pool, p.id)
		if got.State != "dead" || got.Attempts != 3 || !got.HasFinished {
			t.Errorf("%s: row %+v, want dead after 3 attempts", name, got)
		}
		if p.calls != 3 {
			t.Errorf("%s: handler ran %d times, want 3", name, p.calls)
		}
		if n := countAttemptRows(t, pool, p.id); n != 3 {
			t.Errorf("%s: %d attempt rows, want 3", name, n)
		}
	}
	if a := readAttempt(t, pool, errID, 3); a.Outcome != "retryable_failure" || !strings.Contains(a.Error, "always fails") {
		t.Errorf("last poison_err attempt row: %+v", a)
	}
}

func TestClaimTypes_OnlyClaimsListedTypes(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	idA := submitNoop(t, pool, SubmitParams{Type: "a"})
	idB := submitNoop(t, pool, SubmitParams{Type: "b"})

	jobs, err := ClaimTypes(ctx, pool, "default", "w1", []string{"b"}, 10, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ID != idB {
		t.Fatalf("claimed %+v, want only job %s", jobs, idB)
	}
	if got := readJob(t, pool, idA); got.State != "queued" || got.Attempts != 0 {
		t.Fatalf("job of an unlisted type was touched: %+v", got)
	}

	// nil means every type.
	rest, err := ClaimTypes(ctx, pool, "default", "w1", nil, 10, 30*time.Second)
	if err != nil || len(rest) != 1 || rest[0].ID != idA {
		t.Fatalf("nil types: jobs=%+v err=%v, want job %s", rest, err, idA)
	}
}

// The rolling-deploy hazard: a worker must leave jobs it has no handler for
// alone, so a worker that can run them still can, with all attempts intact.
func TestWorker_DoesNotClaimTypesItCannotHandle(t *testing.T) {
	pool := testPool(t)

	otherID := submitNoop(t, pool, SubmitParams{Type: "other"})
	doneID := submitNoop(t, pool, SubmitParams{})

	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error { return nil },
	}
	cancel, done := startWorker(t, pool, fastRetryConfig("w1"), handlers)
	waitFor(t, 10*time.Second, func() bool {
		return readJob(t, pool, doneID).State == "succeeded"
	}, "the handled job to succeed")
	time.Sleep(300 * time.Millisecond)
	stopWorker(t, cancel, done)

	if got := readJob(t, pool, otherID); got.State != "queued" || got.Attempts != 0 {
		t.Fatalf("job with no handler was claimed: %+v", got)
	}
}
