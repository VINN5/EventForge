package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Bounded retry of the calls that report a result to the database: Ack, Fail,
// MarkPublished and FailOutbox.
//
// Why a retry is safe: all four are fenced. If the first send committed and
// only the reply was lost, the second send matches no row and comes back as
// ErrLeaseLost, so nothing is applied twice.
//
// Why the log wording matters: when a call that was retried after a connection
// error finally gets ErrLeaseLost, that may be our own first send. The code must
// then say "outcome unknown", not claim that another worker or publisher owns
// the job or event.
//
// The contract these tests pin down (all in package core, none of it written yet):
//
//	type ConnRetry struct {
//	    Attempts  int           // total tries including the first; default 5; 1 means no retry
//	    BaseDelay time.Duration // default 100ms
//	    MaxDelay  time.Duration // default 1s
//	    // plus one unexported field, wait, so tests can see the delays
//	}
//	func (c ConnRetry) withDefaults() ConnRetry
//	func isTransient(err error) bool
//	func retryTransient(ctx, policy ConnRetry, rnd func() float64,
//	    fn func(ctx context.Context) error) (attempts int, err error)
//	WorkerConfig.ConnRetry and PublisherConfig.ConnRetry (zero value = defaults)

// errConnLost looks like what Postgres sends a session that was terminated.
var errConnLost = &pgconn.PgError{
	Code:    "57P01",
	Message: "terminating connection due to administrator command",
}

func zeroRand() float64 { return 0 }

// flakyMode says what a failed call looks like.
type flakyMode int

const (
	// failBefore: the statement never reaches the database.
	failBefore flakyMode = iota
	// failAfter: the statement runs and commits, then the reply is lost. This is
	// the dangerous case, because the caller cannot tell it from failBefore.
	failAfter
)

// flakyQuerier wraps a real Querier and fails the first `failures` calls with
// err. It counts every call, so tests can check how often the code tried.
type flakyQuerier struct {
	inner    Querier
	mode     flakyMode
	failures int32
	err      error
	calls    atomic.Int32
}

var _ Querier = (*flakyQuerier)(nil)

func newFlaky(inner Querier, mode flakyMode, failures int) *flakyQuerier {
	return &flakyQuerier{inner: inner, mode: mode, failures: int32(failures), err: errConnLost}
}

func (f *flakyQuerier) shouldFail() bool { return f.calls.Add(1) <= f.failures }

func (f *flakyQuerier) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if !f.shouldFail() {
		return f.inner.Exec(ctx, sql, args...)
	}
	if f.mode == failAfter {
		if _, err := f.inner.Exec(ctx, sql, args...); err != nil {
			return pgconn.CommandTag{}, err
		}
	}
	return pgconn.CommandTag{}, f.err
}

func (f *flakyQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if !f.shouldFail() {
		return f.inner.Query(ctx, sql, args...)
	}
	if f.mode == failAfter {
		rows, err := f.inner.Query(ctx, sql, args...)
		if err != nil {
			return nil, err
		}
		rows.Close() // runs the statement to the end, then we pretend the reply was lost
	}
	return nil, f.err
}

func (f *flakyQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if !f.shouldFail() {
		return f.inner.QueryRow(ctx, sql, args...)
	}
	if f.mode == failAfter {
		rows, err := f.inner.Query(ctx, sql, args...)
		if err != nil {
			return errRow{err: err}
		}
		rows.Close()
	}
	return errRow{err: f.err}
}

// errRow is a pgx.Row whose Scan returns a fixed error.
type errRow struct{ err error }

func (r errRow) Scan(...any) error { return r.err }

var retryModes = []struct {
	name string
	mode flakyMode
}{
	{"statement never ran", failBefore},
	{"statement committed, reply lost", failAfter},
}

func bufLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

// ---------------------------------------------------------------------------
// isTransient: which errors are worth retrying
// ---------------------------------------------------------------------------

func TestIsTransient(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"EOF", io.EOF, true},
		{"unexpected EOF", io.ErrUnexpectedEOF, true},
		{"wrapped EOF", fmt.Errorf("read: %w", io.EOF), true},
		{"network error", &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}, true},
		{"admin_shutdown 57P01", errConnLost, true},
		{"wrapped admin_shutdown", fmt.Errorf("mark published: %w", errConnLost), true},
		{"crash_shutdown 57P02", &pgconn.PgError{Code: "57P02"}, true},
		{"cannot_connect_now 57P03", &pgconn.PgError{Code: "57P03"}, true},
		{"connection_failure 08006", &pgconn.PgError{Code: "08006"}, true},
		{"connection_does_not_exist 08003", &pgconn.PgError{Code: "08003"}, true},
		{"serialization failure 40001", &pgconn.PgError{Code: "40001"}, false},
		{"unique violation 23505", &pgconn.PgError{Code: "23505"}, false},
		{"context canceled", context.Canceled, false},
		{"deadline exceeded", context.DeadlineExceeded, false},
		{"lease lost", ErrLeaseLost, false},
		{"unclassified error", errors.New("boom"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isTransient(tc.err); got != tc.want {
				t.Fatalf("isTransient(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// A real terminated session must be classified as transient. This is the one
// test that does not rely on my guess of what pgx returns.
func TestIsTransient_RealKilledSession(t *testing.T) {
	admin := testPool(t)
	ctx := context.Background()

	cfg, err := pgxpool.ParseConfig(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	cfg.MaxConns = 1
	victim, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(victim.Close)

	var pid int32
	if err := victim.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatalf("backend pid: %v", err)
	}
	if _, err := admin.Exec(ctx, `SELECT pg_terminate_backend($1)`, pid); err != nil {
		t.Fatalf("terminate: %v", err)
	}

	var got error
	for i := 0; i < 40 && got == nil; i++ {
		_, got = victim.Exec(ctx, `SELECT 1`)
		if got == nil {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if got == nil {
		t.Fatal("the pool never reported the terminated session")
	}
	if !isTransient(got) {
		t.Fatalf("a terminated session was classified as permanent: %T: %v", got, got)
	}
}

// Nothing listens on port 1, so this is a real "connection refused".
func TestIsTransient_RealConnectionRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, "postgres://nobody:nothing@127.0.0.1:1/none?connect_timeout=3")
	if err == nil {
		defer pool.Close()
		_, err = pool.Exec(ctx, `SELECT 1`)
	}
	if err == nil {
		t.Fatal("expected a connection error")
	}
	if !isTransient(err) {
		t.Fatalf("a refused connection was classified as permanent: %T: %v", err, err)
	}
}

// ---------------------------------------------------------------------------
// retryTransient: the loop itself (no database)
// ---------------------------------------------------------------------------

func TestConnRetry_Defaults(t *testing.T) {
	d := ConnRetry{}.withDefaults()
	if d.Attempts != 5 || d.BaseDelay != 100*time.Millisecond || d.MaxDelay != time.Second {
		t.Fatalf("defaults = %+v, want 5 attempts, 100ms base, 1s max", d)
	}
	set := ConnRetry{Attempts: 2, BaseDelay: 7 * time.Millisecond, MaxDelay: 9 * time.Millisecond}
	got := set.withDefaults()
	if got.Attempts != 2 || got.BaseDelay != 7*time.Millisecond || got.MaxDelay != 9*time.Millisecond {
		t.Fatalf("explicit values were changed: %+v", got)
	}
}

func TestRetryTransient_SucceedsFirstTry(t *testing.T) {
	calls := 0
	attempts, err := retryTransient(context.Background(), ConnRetry{Attempts: 5}, zeroRand,
		func(ctx context.Context) error { calls++; return nil })
	if err != nil || attempts != 1 || calls != 1 {
		t.Fatalf("attempts=%d calls=%d err=%v, want 1, 1, nil", attempts, calls, err)
	}
}

func TestRetryTransient_RetriesThenSucceeds(t *testing.T) {
	calls := 0
	attempts, err := retryTransient(context.Background(), ConnRetry{Attempts: 5}, zeroRand,
		func(ctx context.Context) error {
			calls++
			if calls <= 2 {
				return errConnLost
			}
			return nil
		})
	if err != nil || attempts != 3 || calls != 3 {
		t.Fatalf("attempts=%d calls=%d err=%v, want 3, 3, nil", attempts, calls, err)
	}
}

func TestRetryTransient_StopsAtANonTransientError(t *testing.T) {
	uniqueViolation := &pgconn.PgError{Code: "23505"}
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"lease lost", ErrLeaseLost},
		{"unique violation", uniqueViolation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			attempts, err := retryTransient(context.Background(), ConnRetry{Attempts: 5}, zeroRand,
				func(ctx context.Context) error { calls++; return tc.err })
			if attempts != 1 || calls != 1 {
				t.Fatalf("attempts=%d calls=%d, want 1, 1 (must not retry)", attempts, calls)
			}
			if !errors.Is(err, tc.err) {
				t.Fatalf("err = %v, want the original error", err)
			}
		})
	}
}

func TestRetryTransient_GivesUpAfterTheBudget(t *testing.T) {
	calls := 0
	attempts, err := retryTransient(context.Background(), ConnRetry{Attempts: 4}, zeroRand,
		func(ctx context.Context) error { calls++; return errConnLost })
	if attempts != 4 || calls != 4 {
		t.Fatalf("attempts=%d calls=%d, want 4, 4", attempts, calls)
	}
	if !errors.Is(err, errConnLost) {
		t.Fatalf("err = %v, want the last error", err)
	}
}

func TestRetryTransient_StopsWhenTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Waits of about 5 s, but the context ends after 100 ms.
	policy := ConnRetry{Attempts: 5, BaseDelay: 5 * time.Second, MaxDelay: 5 * time.Second}
	start := time.Now()
	attempts, err := retryTransient(ctx, policy, func() float64 { return 0.99 },
		func(ctx context.Context) error { return errConnLost })

	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("took %v: the retry must stop when its context ends", elapsed)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (no new try after the context ended)", attempts)
	}
	if !errors.Is(err, errConnLost) {
		t.Fatalf("err = %v, want the last operation error", err)
	}
}

func TestRetryTransient_DelaysAreBoundedAndCounted(t *testing.T) {
	run := func(rnd func() float64) []time.Duration {
		var delays []time.Duration
		policy := ConnRetry{
			Attempts:  4,
			BaseDelay: 10 * time.Millisecond,
			MaxDelay:  30 * time.Millisecond,
			wait:      func(ctx context.Context, d time.Duration) { delays = append(delays, d) },
		}
		attempts, _ := retryTransient(context.Background(), policy, rnd,
			func(ctx context.Context) error { return errConnLost })
		if attempts != 4 {
			t.Fatalf("attempts = %d, want 4", attempts)
		}
		return delays
	}

	delays := run(func() float64 { return 0.999 })
	if len(delays) != 3 {
		t.Fatalf("waited %d times, want 3 (one between each pair of tries)", len(delays))
	}
	for i, d := range delays {
		if d <= 0 || d > 30*time.Millisecond {
			t.Fatalf("delay %d = %v, want within (0, 30ms]", i, d)
		}
	}

	for i, d := range run(zeroRand) {
		if d != 0 {
			t.Fatalf("delay %d = %v with zero randomness, want 0", i, d)
		}
	}
}

// ---------------------------------------------------------------------------
// Worker call sites
// ---------------------------------------------------------------------------

func newRetryWorker(t *testing.T, db Querier, attempts int) *Worker {
	t.Helper()
	w, err := NewWorker(db, WorkerConfig{
		ID:        "w1",
		ConnRetry: ConnRetry{Attempts: attempts},
		Rand:      zeroRand,
	}, map[string]Handler{
		"noop": func(ctx context.Context, job Job) error { return nil },
	}, quietLogger())
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	return w
}

func TestWorkerAck_RetriesAfterATransientError(t *testing.T) {
	for _, m := range retryModes {
		t.Run(m.name, func(t *testing.T) {
			pool := testPool(t)
			submitNoop(t, pool, SubmitParams{})
			job := claimOne(t, pool, "w1")
			flaky := newFlaky(pool, m.mode, 1)
			w := newRetryWorker(t, flaky, 3)
			log, out := bufLogger()

			w.ack(log, job)

			if n := countByState(t, pool, "succeeded"); n != 1 {
				t.Fatalf("succeeded jobs = %d, want 1; log: %s", n, out)
			}
			if got := flaky.calls.Load(); got != 2 {
				t.Fatalf("ack statements sent = %d, want 2", got)
			}
			logged := out.String()
			if m.mode == failAfter {
				if !strings.Contains(logged, "outcome unknown") {
					t.Fatalf("log should say the outcome is unknown; got: %s", logged)
				}
				if strings.Contains(logged, "another worker owns") {
					t.Fatalf("log wrongly blames another worker; got: %s", logged)
				}
			} else if strings.Contains(logged, "outcome unknown") || strings.Contains(logged, "lease lost") {
				t.Fatalf("a clean retry should not log a lost lease; got: %s", logged)
			}
		})
	}
}

func TestWorkerAck_GivesUpAfterTheBudget(t *testing.T) {
	pool := testPool(t)
	submitNoop(t, pool, SubmitParams{})
	job := claimOne(t, pool, "w1")
	flaky := newFlaky(pool, failBefore, 100)
	w := newRetryWorker(t, flaky, 3)
	log, out := bufLogger()

	w.ack(log, job)

	if got := flaky.calls.Load(); got != 3 {
		t.Fatalf("ack statements sent = %d, want 3", got)
	}
	if n := countByState(t, pool, "running"); n != 1 {
		t.Fatalf("running jobs = %d, want 1 (it waits out its lease)", n)
	}
	if !strings.Contains(out.String(), "ack failed") {
		t.Fatalf("log should report the failed ack; got: %s", out)
	}
}

func TestWorkerAck_DoesNotRetryALostLease(t *testing.T) {
	pool := testPool(t)
	id := submitNoop(t, pool, SubmitParams{})
	job := claimOne(t, pool, "w1")
	requeue(t, pool, id)
	claimOne(t, pool, "w2") // takes over; the fence moves on

	flaky := newFlaky(pool, failBefore, 0) // counts calls, never fails
	w := newRetryWorker(t, flaky, 3)
	log, out := bufLogger()

	w.ack(log, job)

	if got := flaky.calls.Load(); got != 1 {
		t.Fatalf("ack statements sent = %d, want 1 (a lost lease is not retried)", got)
	}
	if n := countByState(t, pool, "running"); n != 1 {
		t.Fatalf("running jobs = %d, want 1 (held by w2)", n)
	}
	if n := countByState(t, pool, "succeeded"); n != 0 {
		t.Fatalf("succeeded jobs = %d, want 0", n)
	}
	logged := out.String()
	if !strings.Contains(logged, "another worker owns this job") {
		t.Fatalf("log should say another worker owns the job; got: %s", logged)
	}
	if strings.Contains(logged, "outcome unknown") {
		t.Fatalf("nothing was retried, so the outcome is not unknown; got: %s", logged)
	}
}

func TestWorkerReportFailure_RetriesAfterATransientError(t *testing.T) {
	for _, m := range retryModes {
		t.Run(m.name, func(t *testing.T) {
			pool := testPool(t)
			submitNoop(t, pool, SubmitParams{})
			job := claimOne(t, pool, "w1")
			flaky := newFlaky(pool, m.mode, 1)
			w := newRetryWorker(t, flaky, 3)
			log, out := bufLogger()

			w.reportFailure(log, job, FailParams{Class: ClassRetryable, Message: "boom"})

			if n := countByState(t, pool, "queued"); n != 1 {
				t.Fatalf("queued jobs = %d, want 1 (failure recorded once); log: %s", n, out)
			}
			if got := flaky.calls.Load(); got != 2 {
				t.Fatalf("fail statements sent = %d, want 2", got)
			}
			logged := out.String()
			if m.mode == failAfter {
				if !strings.Contains(logged, "outcome unknown") {
					t.Fatalf("log should say the outcome is unknown; got: %s", logged)
				}
				if strings.Contains(logged, "another worker owns") {
					t.Fatalf("log wrongly blames another worker; got: %s", logged)
				}
			}
		})
	}
}

func TestWorkerReportFailure_GivesUpAfterTheBudget(t *testing.T) {
	pool := testPool(t)
	submitNoop(t, pool, SubmitParams{})
	job := claimOne(t, pool, "w1")
	flaky := newFlaky(pool, failBefore, 100)
	w := newRetryWorker(t, flaky, 3)
	log, out := bufLogger()

	w.reportFailure(log, job, FailParams{Class: ClassRetryable, Message: "boom"})

	if got := flaky.calls.Load(); got != 3 {
		t.Fatalf("fail statements sent = %d, want 3", got)
	}
	if n := countByState(t, pool, "running"); n != 1 {
		t.Fatalf("running jobs = %d, want 1 (it waits out its lease)", n)
	}
	if !strings.Contains(out.String(), "recording failure failed") {
		t.Fatalf("log should report the failed recording; got: %s", out)
	}
}

// ---------------------------------------------------------------------------
// Publisher call sites
// ---------------------------------------------------------------------------

func newRetryPublisher(t *testing.T, db Querier, id string, attempts int) *Publisher {
	t.Helper()
	p, err := NewPublisher(db,
		func(ctx context.Context, item OutboxItem) error { return nil },
		PublisherConfig{ID: id, ConnRetry: ConnRetry{Attempts: attempts}, Rand: zeroRand},
		quietLogger())
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	return p
}

func TestPublisherMark_RetriesAfterATransientError(t *testing.T) {
	for _, m := range retryModes {
		t.Run(m.name, func(t *testing.T) {
			pool := testPool(t)
			obAdd(t, pool, 1)
			item := obClaim(t, pool, "p1", 1)[0]
			flaky := newFlaky(pool, m.mode, 1)
			p := newRetryPublisher(t, flaky, "p1", 3)
			log, out := bufLogger()

			ok := p.mark(log, item)

			if n := obCountWhere(t, pool, "published_at IS NOT NULL"); n != 1 {
				t.Fatalf("published events = %d, want 1; log: %s", n, out)
			}
			if got := flaky.calls.Load(); got != 2 {
				t.Fatalf("mark statements sent = %d, want 2", got)
			}
			logged := out.String()
			if m.mode == failAfter {
				// The result of mark is deliberately not checked here: after a
				// committed first send the retry cannot tell, and either answer is
				// defensible. The log must not claim a takeover, though.
				if !strings.Contains(logged, "outcome unknown") {
					t.Fatalf("log should say the outcome is unknown; got: %s", logged)
				}
				if strings.Contains(logged, "another publisher owns") {
					t.Fatalf("log wrongly blames another publisher; got: %s", logged)
				}
			} else if !ok {
				t.Fatalf("mark returned false after a clean retry; log: %s", logged)
			}
		})
	}
}

func TestPublisherMark_GivesUpAfterTheBudget(t *testing.T) {
	pool := testPool(t)
	obAdd(t, pool, 1)
	item := obClaim(t, pool, "p1", 1)[0]
	flaky := newFlaky(pool, failBefore, 100)
	p := newRetryPublisher(t, flaky, "p1", 3)
	log, out := bufLogger()

	ok := p.mark(log, item)

	if ok {
		t.Fatal("mark returned true although it never reached the database")
	}
	if got := flaky.calls.Load(); got != 3 {
		t.Fatalf("mark statements sent = %d, want 3", got)
	}
	if n := obCountWhere(t, pool, "published_at IS NOT NULL"); n != 0 {
		t.Fatalf("published events = %d, want 0", n)
	}
	if !strings.Contains(out.String(), "could not be marked published") {
		t.Fatalf("log should report the failed mark; got: %s", out)
	}
}

func TestPublisherMark_DoesNotRetryALostLease(t *testing.T) {
	pool := testPool(t)
	obAdd(t, pool, 1)
	first := obClaim(t, pool, "p1", 1)[0]
	obExpire(t, pool, first.EventID)
	obClaim(t, pool, "p2", 1) // p2 takes the event over

	flaky := newFlaky(pool, failBefore, 0) // counts calls, never fails
	p := newRetryPublisher(t, flaky, "p1", 3)
	log, out := bufLogger()

	ok := p.mark(log, first)

	if ok {
		t.Fatal("mark returned true for an event another publisher owns")
	}
	if got := flaky.calls.Load(); got != 1 {
		t.Fatalf("mark statements sent = %d, want 1 (a lost lease is not retried)", got)
	}
	if n := obCountWhere(t, pool, "published_at IS NOT NULL"); n != 0 {
		t.Fatalf("published events = %d, want 0", n)
	}
	logged := out.String()
	if !strings.Contains(logged, "another publisher owns") {
		t.Fatalf("log should say another publisher owns the event; got: %s", logged)
	}
	if strings.Contains(logged, "outcome unknown") {
		t.Fatalf("nothing was retried, so the outcome is not unknown; got: %s", logged)
	}
}

func TestPublisherReportFailure_RetriesAfterATransientError(t *testing.T) {
	for _, m := range retryModes {
		t.Run(m.name, func(t *testing.T) {
			pool := testPool(t)
			obAdd(t, pool, 1)
			item := obClaim(t, pool, "p1", 1)[0]
			flaky := newFlaky(pool, m.mode, 1)
			p := newRetryPublisher(t, flaky, "p1", 3)
			log, out := bufLogger()

			p.reportFailure(log, item, FailParams{Class: ClassRetryable, Message: "x"})

			n := obCountWhere(t, pool,
				"published_at IS NULL AND dead_at IS NULL AND lease_owner IS NULL AND last_error = 'x'")
			if n != 1 {
				t.Fatalf("events with the failure recorded and no lease = %d, want 1; log: %s", n, out)
			}
			if got := flaky.calls.Load(); got != 2 {
				t.Fatalf("fail statements sent = %d, want 2", got)
			}
			logged := out.String()
			if m.mode == failAfter {
				if !strings.Contains(logged, "outcome unknown") {
					t.Fatalf("log should say the outcome is unknown; got: %s", logged)
				}
				if strings.Contains(logged, "another publisher owns") {
					t.Fatalf("log wrongly blames another publisher; got: %s", logged)
				}
			}
		})
	}
}