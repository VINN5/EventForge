package core

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Publisher.Step does one cycle: claim a batch, deliver each event through the
// Sink in parallel, then mark each published or record its failure. It
// returns how many events it settled (published, or failure recorded).

func obNewPublisher(t *testing.T, q Querier, sink Sink, mutate func(*PublisherConfig)) *Publisher {
	t.Helper()
	cfg := PublisherConfig{
		ID:             "pub-1",
		BatchSize:      10,
		Lease:          30 * time.Second,
		DeliverTimeout: 5 * time.Second,
		BackoffBase:    time.Hour, // with Rand 0.5 the first retry is 30 minutes away
		BackoffCap:     2 * time.Hour,
		Rand:           func() float64 { return 0.5 },
	}
	if mutate != nil {
		mutate(&cfg)
	}
	p, err := NewPublisher(q, sink, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	return p
}

func obStep(t *testing.T, p *Publisher) int {
	t.Helper()
	n, err := p.Step(context.Background())
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	return n
}

func obCountWhere(t *testing.T, q Querier, cond string) int {
	t.Helper()
	var n int
	if err := q.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE `+cond).Scan(&n); err != nil {
		t.Fatalf("count where %s: %v", cond, err)
	}
	return n
}

func obSecondsUntilDue(t *testing.T, q Querier, eventID string) float64 {
	t.Helper()
	var s float64
	err := q.QueryRow(context.Background(), `
		SELECT EXTRACT(EPOCH FROM (next_attempt_at - now()))::float8
		FROM outbox WHERE event_id = $1::uuid`, eventID).Scan(&s)
	if err != nil {
		t.Fatalf("seconds until due: %v", err)
	}
	return s
}

func obWaitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

type obRecorder struct {
	mu    sync.Mutex
	items []OutboxItem
}

func (r *obRecorder) Publish(ctx context.Context, it OutboxItem) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items = append(r.items, it)
	return nil
}

func (r *obRecorder) snapshot() []OutboxItem {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]OutboxItem(nil), r.items...)
}

func obFailWith(err error) Sink {
	return func(ctx context.Context, it OutboxItem) error { return err }
}

func TestNewPublisher_Validation(t *testing.T) {
	pool := testPool(t)
	ok := func(context.Context, OutboxItem) error { return nil }

	cases := map[string]struct {
		sink    Sink
		cfg     PublisherConfig
		wantErr bool
	}{
		"minimal config uses the defaults":                {ok, PublisherConfig{ID: "p"}, false},
		"missing id":                                      {ok, PublisherConfig{}, true},
		"nil sink":                                        {nil, PublisherConfig{ID: "p"}, true},
		"lease not longer than the delivery timeout":      {ok, PublisherConfig{ID: "p", Lease: 5 * time.Second, DeliverTimeout: 5 * time.Second}, true},
		"lease shorter than the default delivery timeout": {ok, PublisherConfig{ID: "p", Lease: time.Second}, true},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := NewPublisher(pool, c.sink, c.cfg, nil)
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, c.wantErr)
			}
		})
	}
}

func TestPublisherStep_DeliversAndMarksPublished(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 3)
	rec := &obRecorder{}
	p := obNewPublisher(t, pool, rec.Publish, nil)

	if n := obStep(t, p); n != 3 {
		t.Fatalf("Step settled %d events, want 3", n)
	}
	got := map[string]bool{}
	for _, it := range rec.snapshot() {
		got[it.EventID] = true
		if it.Topic != "t" || string(it.Payload) != "{}" || it.Attempts != 1 {
			t.Fatalf("sink received %+v", it)
		}
	}
	for _, id := range ids {
		if !got[id] {
			t.Errorf("event %s never reached the sink", id)
		}
		if r := obRead(t, pool, id); !r.Published || r.LeaseOwner != "" || r.Dead {
			t.Errorf("event %s row = %+v, want published and unleased", id, r)
		}
	}
	if n := obStep(t, p); n != 0 {
		t.Fatalf("second Step settled %d, want 0", n)
	}
}

func TestPublisherStep_RespectsTheBatchSize(t *testing.T) {
	pool := testPool(t)
	obAdd(t, pool, 5)
	rec := &obRecorder{}
	p := obNewPublisher(t, pool, rec.Publish, func(c *PublisherConfig) { c.BatchSize = 2 })

	for _, want := range []int{2, 2, 1, 0} {
		if n := obStep(t, p); n != want {
			t.Fatalf("Step settled %d, want %d", n, want)
		}
	}
	if got := len(rec.snapshot()); got != 5 {
		t.Fatalf("sink saw %d events, want 5", got)
	}
}

// The events of one batch are delivered in parallel: every delivery waits
// here until all five have arrived, which only works if they overlap.
func TestPublisherStep_DeliversABatchInParallel(t *testing.T) {
	pool := testPool(t)
	obAdd(t, pool, 5)
	var arrived atomic.Int32
	sink := func(ctx context.Context, it OutboxItem) error {
		arrived.Add(1)
		deadline := time.Now().Add(3 * time.Second)
		for arrived.Load() < 5 {
			if time.Now().After(deadline) {
				return Permanent(errors.New("deliveries are not running in parallel"))
			}
			time.Sleep(5 * time.Millisecond)
		}
		return nil
	}
	p := obNewPublisher(t, pool, sink, func(c *PublisherConfig) { c.BatchSize = 5 })

	if n := obStep(t, p); n != 5 {
		t.Fatalf("Step settled %d, want 5", n)
	}
	if got := obCountWhere(t, pool, `published_at IS NOT NULL`); got != 5 {
		t.Fatalf("%d events published, want 5", got)
	}
}

func TestPublisherStep_RetryableFailureSchedulesARetry(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 1)
	p := obNewPublisher(t, pool, obFailWith(Retryable(errors.New("broker unavailable"))), nil)

	if n := obStep(t, p); n != 1 {
		t.Fatalf("Step settled %d, want 1", n)
	}
	r := obRead(t, pool, ids[0])
	if r.Dead || r.Published || r.LeaseOwner != "" || r.DueNow || r.Attempts != 1 {
		t.Fatalf("row = %+v, want pending, unleased, not yet due, attempts 1", r)
	}
	if got := obLastError(t, pool, ids[0]); got != "broker unavailable" {
		t.Fatalf("last_error = %q", got)
	}
	// Backoff 1h * 0.5 with the fixed Rand.
	if s := obSecondsUntilDue(t, pool, ids[0]); s < 1700 || s > 1900 {
		t.Fatalf("due in %.0fs, want about 1800s", s)
	}
	if n := obStep(t, p); n != 0 {
		t.Fatalf("an event waiting out its backoff was delivered again (settled %d)", n)
	}
}

func TestPublisherStep_AnUnmarkedErrorIsRetryable(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 1)
	p := obNewPublisher(t, pool, obFailWith(errors.New("something odd")), nil)

	obStep(t, p)
	if r := obRead(t, pool, ids[0]); r.Dead || r.Published || r.DueNow {
		t.Fatalf("row = %+v, want pending and waiting for its retry", r)
	}
}

func TestPublisherStep_PermanentFailureDeadLetters(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 1)
	p := obNewPublisher(t, pool, obFailWith(Permanent(errors.New("topic does not exist"))), nil)

	if n := obStep(t, p); n != 1 {
		t.Fatalf("Step settled %d, want 1", n)
	}
	if r := obRead(t, pool, ids[0]); !r.Dead || r.LeaseOwner != "" {
		t.Fatalf("row = %+v, want dead and unleased", r)
	}
	if got := obLastError(t, pool, ids[0]); got != "topic does not exist" {
		t.Fatalf("last_error = %q", got)
	}
}

func TestPublisherStep_ARetryableFailureOnTheLastAttemptIsDead(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 1)
	exec(t, pool, `UPDATE outbox SET max_attempts = 1`)
	p := obNewPublisher(t, pool, obFailWith(Retryable(errors.New("down"))), nil)

	obStep(t, p)
	if r := obRead(t, pool, ids[0]); !r.Dead {
		t.Fatalf("row = %+v, want dead", r)
	}
}

func TestPublisherStep_RetryAfterIsHonouredUpToTheCeiling(t *testing.T) {
	cases := map[string]struct {
		ceiling      time.Duration
		wantMin, max float64
	}{
		"default ceiling is one hour": {0, 3500, 3700},
		"custom ceiling":              {10 * time.Minute, 500, 700},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			pool := testPool(t)
			ids := obAdd(t, pool, 1)
			sink := obFailWith(RetryableAfter(errors.New("429 slow down"), 2*time.Hour))
			p := obNewPublisher(t, pool, sink, func(cfg *PublisherConfig) {
				cfg.BackoffBase = time.Millisecond // backoff is negligible, Retry-After decides
				cfg.BackoffCap = time.Second
				cfg.MaxRetryAfter = c.ceiling
			})

			obStep(t, p)
			if s := obSecondsUntilDue(t, pool, ids[0]); s < c.wantMin || s > c.max {
				t.Fatalf("due in %.0fs, want between %.0f and %.0f", s, c.wantMin, c.max)
			}
		})
	}
}

func TestPublisherStep_APanicIsARetryableFailure(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 1)
	p := obNewPublisher(t, pool, func(ctx context.Context, it OutboxItem) error { panic("kaboom") }, nil)

	if n := obStep(t, p); n != 1 {
		t.Fatalf("Step settled %d, want 1", n)
	}
	if r := obRead(t, pool, ids[0]); r.Dead || r.Published || r.LeaseOwner != "" {
		t.Fatalf("row = %+v, want pending and unleased", r)
	}
	if got := obLastError(t, pool, ids[0]); !strings.Contains(got, "kaboom") {
		t.Fatalf("last_error = %q, want it to mention the panic", got)
	}
}

// A sink that outlasts DeliverTimeout is cancelled, and that counts as a
// failure of the attempt (shutdown, below, does not).
func TestPublisherStep_ASlowSinkTimesOut(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 1)
	sink := func(ctx context.Context, it OutboxItem) error {
		<-ctx.Done()
		return ctx.Err()
	}
	p := obNewPublisher(t, pool, sink, func(c *PublisherConfig) {
		c.DeliverTimeout = 50 * time.Millisecond
		c.Lease = 5 * time.Second
	})

	if n := obStep(t, p); n != 1 {
		t.Fatalf("Step settled %d, want 1", n)
	}
	r := obRead(t, pool, ids[0])
	if r.Dead || r.Published || r.LeaseOwner != "" {
		t.Fatalf("row = %+v, want pending and unleased", r)
	}
	if got := obLastError(t, pool, ids[0]); !strings.Contains(got, "deadline exceeded") {
		t.Fatalf("last_error = %q, want a deadline error", got)
	}
}

// A publisher that died after claiming: its lease runs out and the event is
// delivered by the next publisher.
func TestPublisherStep_DeliversAnEventWhoseLeaseExpired(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 1)
	obClaim(t, pool, "dead-publisher", 1)
	obExpire(t, pool, ids[0])

	rec := &obRecorder{}
	p := obNewPublisher(t, pool, rec.Publish, nil)
	if n := obStep(t, p); n != 1 {
		t.Fatalf("Step settled %d, want 1", n)
	}
	got := rec.snapshot()
	if len(got) != 1 || got[0].Attempts != 2 {
		t.Fatalf("sink saw %+v, want the event once with attempts 2", got)
	}
	if r := obRead(t, pool, ids[0]); !r.Published {
		t.Fatalf("row = %+v, want published", r)
	}
}

// The fence in action: the event is taken over while this publisher is still
// delivering, so its mark is rejected and the event stays with its new owner.
func TestPublisherStep_AStalePublisherCannotMarkAfterTakeover(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 1)
	sink := func(ctx context.Context, it OutboxItem) error {
		bg := context.Background()
		if _, err := pool.Exec(bg, `UPDATE outbox SET lease_expires_at = now() - interval '1 second'
			WHERE id = $1`, it.ID); err != nil {
			t.Errorf("expire lease: %v", err)
		}
		got, err := ClaimOutbox(bg, pool, "other", 1, time.Minute)
		if err != nil || len(got) != 1 {
			t.Errorf("takeover claim: %v err=%v", got, err)
		}
		return nil // the (late) delivery itself succeeded
	}
	p := obNewPublisher(t, pool, sink, nil)

	if n := obStep(t, p); n != 0 {
		t.Fatalf("Step settled %d, want 0 (the mark must be rejected)", n)
	}
	r := obRead(t, pool, ids[0])
	if r.Published || r.LeaseOwner != "other" || r.Attempts != 2 {
		t.Fatalf("row = %+v, want unpublished and leased to the new owner with attempts 2", r)
	}
}

func TestPublisherStep_ShutdownIsNotChargedAsAFailure(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 1)
	started := make(chan struct{})
	sink := func(ctx context.Context, it OutboxItem) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	p := obNewPublisher(t, pool, sink, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := p.Step(ctx)
		done <- result{n, err}
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the sink never started")
	}
	cancel()

	select {
	case r := <-done:
		if r.err != nil || r.n != 0 {
			t.Fatalf("Step = (%d, %v), want (0, nil): shutdown is not an error and settles nothing", r.n, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Step did not return after shutdown")
	}
	r := obRead(t, pool, ids[0])
	if r.Dead || r.Published || r.LeaseOwner != "pub-1" || r.Attempts != 1 {
		t.Fatalf("row = %+v, want still leased to pub-1, attempts 1", r)
	}
	if got := obLastError(t, pool, ids[0]); got != "" {
		t.Fatalf("shutdown left error text %q", got)
	}
}

// If the delivery succeeded, it is recorded even though shutdown began.
func TestPublisherStep_ASuccessDuringShutdownIsStillMarked(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 1)
	started := make(chan struct{})
	release := make(chan struct{})
	sink := func(ctx context.Context, it OutboxItem) error {
		close(started)
		<-release
		return nil
	}
	p := obNewPublisher(t, pool, sink, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)
	go func() {
		n, _ := p.Step(ctx)
		done <- n
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the sink never started")
	}
	cancel()
	close(release)

	select {
	case n := <-done:
		if n != 1 {
			t.Fatalf("Step settled %d, want 1", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Step did not return")
	}
	if r := obRead(t, pool, ids[0]); !r.Published {
		t.Fatalf("row = %+v, want published", r)
	}
}

// ---- Run ----

func TestPublisherRun_PublishesEverythingAndStopsOnCancel(t *testing.T) {
	pool := testPool(t)
	obAdd(t, pool, 20)
	rec := &obRecorder{}
	p := obNewPublisher(t, pool, rec.Publish, func(c *PublisherConfig) {
		c.BatchSize = 5
		c.PollInterval = 10 * time.Millisecond
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	obWaitFor(t, "all 20 events to be published", 10*time.Second, func() bool {
		return obCountWhere(t, pool, `published_at IS NOT NULL`) == 20
	})
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil after cancel", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}

	seen := map[string]int{}
	for _, it := range rec.snapshot() {
		seen[it.EventID]++
	}
	if len(seen) != 20 {
		t.Fatalf("sink saw %d distinct events, want 20", len(seen))
	}
}

// Events whose publisher crashed on every attempt never get a failure
// reported; Run's reaper dead-letters them.
func TestPublisherRun_DeadLettersEventsThatCrashedEveryAttempt(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 1)
	exec(t, pool, `UPDATE outbox SET max_attempts = 1`)
	obClaim(t, pool, "dead-publisher", 1)
	obExpire(t, pool, ids[0])

	var calls atomic.Int32
	p := obNewPublisher(t, pool, func(ctx context.Context, it OutboxItem) error {
		calls.Add(1)
		return nil
	}, func(c *PublisherConfig) {
		c.PollInterval = 10 * time.Millisecond
		c.ReapInterval = 20 * time.Millisecond
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	obWaitFor(t, "the exhausted event to be dead-lettered", 5*time.Second, func() bool {
		return obCountWhere(t, pool, `dead_at IS NOT NULL`) == 1
	})
	cancel()
	<-done
	if got := calls.Load(); got != 0 {
		t.Fatalf("the sink was called %d times for an event with no attempts left", got)
	}
}