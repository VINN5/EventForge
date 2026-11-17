package core

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type attemptRow struct {
	Worker  string
	Outcome string
	Error   string
	Ended   bool
}

func readAttempt(t *testing.T, q Querier, id string, attempt int) attemptRow {
	t.Helper()
	var r attemptRow
	err := q.QueryRow(context.Background(), `
		SELECT COALESCE(worker, ''), COALESCE(outcome, ''), COALESCE(error, ''), ended_at IS NOT NULL
		FROM job_attempts WHERE job_id = $1 AND attempt = $2`, id, attempt,
	).Scan(&r.Worker, &r.Outcome, &r.Error, &r.Ended)
	if err != nil {
		t.Fatalf("read attempt row %d: %v", attempt, err)
	}
	return r
}

func countAttemptRows(t *testing.T, q Querier, id string) int {
	t.Helper()
	var n int
	err := q.QueryRow(context.Background(),
		`SELECT count(*) FROM job_attempts WHERE job_id = $1`, id).Scan(&n)
	if err != nil {
		t.Fatalf("count attempt rows: %v", err)
	}
	return n
}

func readLastError(t *testing.T, q Querier, id string) string {
	t.Helper()
	var s string
	err := q.QueryRow(context.Background(),
		`SELECT COALESCE(last_error, '') FROM jobs WHERE id = $1`, id).Scan(&s)
	if err != nil {
		t.Fatalf("read last_error: %v", err)
	}
	return s
}

func assertLeaseLost(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("got %v, want ErrLeaseLost", err)
	}
}

func TestFail_RetryableSchedulesRetryWithDelay(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	job := claimOne(t, pool, "worker-1")

	out, err := Fail(ctx, pool, job.ID, "worker-1", job.Fence,
		FailParams{Class: ClassRetryable, Message: "smtp timeout", Delay: time.Minute})
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if out != FailRetried {
		t.Fatalf("outcome = %v, want FailRetried", out)
	}

	got := readJob(t, pool, id)
	if got.State != "queued" || got.Owner != "" || got.HasFinished {
		t.Fatalf("unexpected row: %+v", got)
	}
	if got.Attempts != 1 || got.Fence != job.Fence {
		t.Fatalf("attempts=%d fence=%d, want 1 and %d (Fail must not change them)",
			got.Attempts, got.Fence, job.Fence)
	}

	// run_at is about a minute from now, on the database clock.
	var inWindow bool
	err = pool.QueryRow(ctx, `
		SELECT run_at > now() + interval '40 seconds'
		   AND run_at < now() + interval '80 seconds'
		FROM jobs WHERE id = $1`, id).Scan(&inWindow)
	if err != nil || !inWindow {
		t.Fatalf("run_at is not about a minute away (err %v)", err)
	}

	if msg := readLastError(t, pool, id); msg != "smtp timeout" {
		t.Fatalf("last_error = %q", msg)
	}
	a := readAttempt(t, pool, id, 1)
	if a.Worker != "worker-1" || a.Outcome != "retryable_failure" || a.Error != "smtp timeout" || !a.Ended {
		t.Fatalf("attempt row: %+v", a)
	}

	// Not due yet, so nobody can claim it.
	jobs, err := Claim(ctx, pool, "default", "worker-2", 5, 30*time.Second)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("claimed a job that is waiting out its delay: jobs=%d err=%v", len(jobs), err)
	}
}

func TestFail_ZeroOrNegativeDelayIsClaimableImmediately(t *testing.T) {
	for _, delay := range []time.Duration{0, -time.Second} {
		t.Run(delay.String(), func(t *testing.T) {
			pool := testPool(t)
			ctx := context.Background()

			submitNoop(t, pool, SubmitParams{})
			first := claimOne(t, pool, "worker-1")
			_, err := Fail(ctx, pool, first.ID, "worker-1", first.Fence,
				FailParams{Class: ClassRetryable, Message: "x", Delay: delay})
			if err != nil {
				t.Fatalf("Fail: %v", err)
			}

			second := claimOne(t, pool, "worker-2")
			if second.Attempts != 2 || second.Fence <= first.Fence {
				t.Fatalf("reclaim: attempts=%d fence=%d (first fence %d)",
					second.Attempts, second.Fence, first.Fence)
			}
		})
	}
}

func TestFail_PermanentGoesDeadEvenWithAttemptsLeft(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{}) // default max_attempts is 8
	job := claimOne(t, pool, "worker-1")

	out, err := Fail(ctx, pool, job.ID, "worker-1", job.Fence,
		FailParams{Class: ClassPermanent, Message: "invalid address", Delay: time.Minute})
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if out != FailDead {
		t.Fatalf("outcome = %v, want FailDead", out)
	}

	got := readJob(t, pool, id)
	if got.State != "dead" || got.Owner != "" || !got.HasFinished || got.Attempts != 1 {
		t.Fatalf("unexpected row: %+v", got)
	}
	if msg := readLastError(t, pool, id); msg != "invalid address" {
		t.Fatalf("last_error = %q", msg)
	}
	a := readAttempt(t, pool, id, 1)
	if a.Outcome != "permanent_failure" || a.Error != "invalid address" || !a.Ended {
		t.Fatalf("attempt row: %+v", a)
	}

	jobs, err := Claim(ctx, pool, "default", "worker-2", 5, 30*time.Second)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("dead job was claimed: jobs=%d err=%v", len(jobs), err)
	}
}

func TestFail_RetryableGoesDeadWhenAttemptsExhausted(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	retry := FailParams{Class: ClassRetryable, Message: "still failing"}

	id := submitNoop(t, pool, SubmitParams{MaxAttempts: 2})

	first := claimOne(t, pool, "worker-1")
	out, err := Fail(ctx, pool, first.ID, "worker-1", first.Fence, retry)
	if err != nil || out != FailRetried {
		t.Fatalf("first failure: out=%v err=%v, want FailRetried", out, err)
	}

	second := claimOne(t, pool, "worker-1")
	out, err = Fail(ctx, pool, second.ID, "worker-1", second.Fence, retry)
	if err != nil || out != FailDead {
		t.Fatalf("second failure: out=%v err=%v, want FailDead", out, err)
	}

	got := readJob(t, pool, id)
	if got.State != "dead" || !got.HasFinished || got.Attempts != 2 {
		t.Fatalf("unexpected row: %+v", got)
	}
	if n := countAttemptRows(t, pool, id); n != 2 {
		t.Fatalf("%d attempt rows, want 2", n)
	}
}

func TestFail_RejectedWhenNotCurrentHolder(t *testing.T) {
	ctx := context.Background()
	retry := FailParams{Class: ClassRetryable, Message: "x"}

	t.Run("wrong worker", func(t *testing.T) {
		pool := testPool(t)
		id := submitNoop(t, pool, SubmitParams{})
		job := claimOne(t, pool, "worker-1")

		_, err := Fail(ctx, pool, job.ID, "worker-2", job.Fence, retry)
		assertLeaseLost(t, err)
		if got := readJob(t, pool, id); got.State != "running" || got.Owner != "worker-1" {
			t.Fatalf("row changed: %+v", got)
		}
		if n := closedAttemptRows(t, pool, id); n != 0 {
			t.Fatalf("a rejected Fail closed %d attempt rows", n)
		}
	})

	t.Run("stale fence after takeover", func(t *testing.T) {
		pool := testPool(t)
		id := submitNoop(t, pool, SubmitParams{})
		first := claimOne(t, pool, "worker-1")
		requeue(t, pool, id)
		claimOne(t, pool, "worker-2")

		_, err := Fail(ctx, pool, first.ID, "worker-1", first.Fence, retry)
		assertLeaseLost(t, err)
		if got := readJob(t, pool, id); got.State != "running" || got.Owner != "worker-2" || got.Attempts != 2 {
			t.Fatalf("stale Fail changed the row: %+v", got)
		}
		if n := closedAttemptRows(t, pool, id); n != 0 {
			t.Fatalf("a rejected Fail closed %d attempt rows", n)
		}
	})

	t.Run("already succeeded", func(t *testing.T) {
		pool := testPool(t)
		id := submitNoop(t, pool, SubmitParams{})
		job := claimOne(t, pool, "worker-1")
		if err := Ack(ctx, pool, job.ID, "worker-1", job.Fence); err != nil {
			t.Fatal(err)
		}

		_, err := Fail(ctx, pool, job.ID, "worker-1", job.Fence, retry)
		assertLeaseLost(t, err)
		if got := readJob(t, pool, id); got.State != "succeeded" {
			t.Fatalf("state=%q, want succeeded", got.State)
		}
	})

	t.Run("already failed", func(t *testing.T) {
		pool := testPool(t)
		id := submitNoop(t, pool, SubmitParams{})
		job := claimOne(t, pool, "worker-1")
		if _, err := Fail(ctx, pool, job.ID, "worker-1", job.Fence, retry); err != nil {
			t.Fatal(err)
		}

		_, err := Fail(ctx, pool, job.ID, "worker-1", job.Fence, retry)
		assertLeaseLost(t, err)
		if n := countAttemptRows(t, pool, id); n != 1 {
			t.Fatalf("%d attempt rows, want 1", n)
		}
	})
}

// Ack and Fail present the same fence at the same moment: exactly one wins.
func TestFail_AckAndFailRaceHasExactlyOneWinner(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	for i := 0; i < 30; i++ {
		submitNoop(t, pool, SubmitParams{})
		job := claimOne(t, pool, "worker-1")

		var (
			wg      sync.WaitGroup
			ackErr  error
			failErr error
			start   = make(chan struct{})
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			ackErr = Ack(ctx, pool, job.ID, "worker-1", job.Fence)
		}()
		go func() {
			defer wg.Done()
			<-start
			// A long delay keeps a retried job from being claimed in the next round.
			_, failErr = Fail(ctx, pool, job.ID, "worker-1", job.Fence,
				FailParams{Class: ClassRetryable, Message: "x", Delay: time.Hour})
		}()
		close(start)
		wg.Wait()

		wins := 0
		for _, err := range []error{ackErr, failErr} {
			switch {
			case err == nil:
				wins++
			case !errors.Is(err, ErrLeaseLost):
				t.Fatalf("round %d: unexpected error: %v", i, err)
			}
		}
		if wins != 1 {
			t.Fatalf("round %d: %d winners (ack err %v, fail err %v), want exactly 1",
				i, wins, ackErr, failErr)
		}
	}
}

// One strange error message must not make the failure statement itself fail,
// or the job would be stuck running with nobody to report why.
func TestFail_OddErrorTextIsStored(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{})
	job := claimOne(t, pool, "worker-1")

	msg := "bad\x00value \xff\xfe " + strings.Repeat("é", 5000)
	if _, err := Fail(ctx, pool, job.ID, "worker-1", job.Fence,
		FailParams{Class: ClassRetryable, Message: msg}); err != nil {
		t.Fatalf("Fail with odd text: %v", err)
	}

	for name, got := range map[string]string{
		"jobs.last_error":    readLastError(t, pool, id),
		"job_attempts.error": readAttempt(t, pool, id, 1).Error,
	} {
		if len(got) == 0 || len(got) > 2000 {
			t.Errorf("%s has %d bytes, want between 1 and 2000", name, len(got))
		}
		if strings.Contains(got, "\x00") {
			t.Errorf("%s still contains a NUL byte", name)
		}
	}
}
