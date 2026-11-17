package core

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Outbox publishing primitives. The fencing token is the row's attempts
// counter: a claim increments it, and mark/fail must present the value they
// were given.

func obAdd(t *testing.T, q Querier, n int) []string {
	t.Helper()
	ids := make([]string, n)
	for i := range ids {
		id, err := Enqueue(context.Background(), q, OutboxEvent{Topic: "t", Payload: []byte(`{}`)})
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		ids[i] = id
	}
	return ids
}

func obClaim(t *testing.T, q Querier, publisher string, limit int) []OutboxItem {
	t.Helper()
	items, err := ClaimOutbox(context.Background(), q, publisher, limit, 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimOutbox(%s): %v", publisher, err)
	}
	return items
}

// obExpire makes a held lease run out, as if the publisher had died.
func obExpire(t *testing.T, q Querier, eventID string) {
	t.Helper()
	exec(t, q, `UPDATE outbox SET lease_expires_at = now() - interval '1 second'
		WHERE event_id = $1::uuid AND lease_owner IS NOT NULL`, eventID)
}

func obLastError(t *testing.T, q Querier, eventID string) string {
	t.Helper()
	var s string
	if err := q.QueryRow(context.Background(),
		`SELECT COALESCE(last_error, '') FROM outbox WHERE event_id = $1::uuid`, eventID).Scan(&s); err != nil {
		t.Fatalf("read last_error: %v", err)
	}
	return s
}

// ---- ClaimOutbox ----

func TestClaimOutbox_LeasesDueEventsOldestFirst(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 3)

	first := obClaim(t, pool, "p1", 2)
	if len(first) != 2 || first[0].EventID != ids[0] || first[1].EventID != ids[1] {
		t.Fatalf("claimed %+v, want the two oldest events in order", first)
	}
	for _, it := range first {
		if it.Attempts != 1 || it.MaxAttempts != 10 || it.Topic != "t" || string(it.Payload) != "{}" {
			t.Fatalf("item = %+v", it)
		}
	}
	if r := obRead(t, pool, ids[0]); r.LeaseOwner != "p1" || r.Attempts != 1 {
		t.Fatalf("row = %+v, want leased to p1 with attempts 1", r)
	}

	second := obClaim(t, pool, "p2", 5)
	if len(second) != 1 || second[0].EventID != ids[2] {
		t.Fatalf("second claim = %+v, want only the third event", second)
	}
	if third := obClaim(t, pool, "p3", 5); len(third) != 0 {
		t.Fatalf("third claim = %+v, want nothing", third)
	}
}

func TestClaimOutbox_SkipsLiveLeases(t *testing.T) {
	pool := testPool(t)
	obAdd(t, pool, 1)

	obClaim(t, pool, "p1", 1)
	if got := obClaim(t, pool, "p2", 1); len(got) != 0 {
		t.Fatalf("claimed an event under a live lease: %+v", got)
	}
}

// No reaper is needed for crashes: an expired lease is simply claimable.
func TestClaimOutbox_ReclaimsAnExpiredLease(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 1)

	obClaim(t, pool, "p1", 1)
	obExpire(t, pool, ids[0])

	got := obClaim(t, pool, "p2", 1)
	if len(got) != 1 || got[0].EventID != ids[0] || got[0].Attempts != 2 {
		t.Fatalf("reclaim = %+v, want the same event with attempts 2", got)
	}
	if r := obRead(t, pool, ids[0]); r.LeaseOwner != "p2" {
		t.Fatalf("owner = %q, want p2", r.LeaseOwner)
	}
}

func TestClaimOutbox_SkipsEventsNotYetDue(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 1)
	exec(t, pool, `UPDATE outbox SET next_attempt_at = now() + interval '1 hour'
		WHERE event_id = $1::uuid`, ids[0])

	if got := obClaim(t, pool, "p1", 1); len(got) != 0 {
		t.Fatalf("claimed a future event: %+v", got)
	}
}

func TestClaimOutbox_SkipsPublishedDeadAndExhaustedEvents(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 4)
	exec(t, pool, `UPDATE outbox SET published_at = now() WHERE event_id = $1::uuid`, ids[0])
	exec(t, pool, `UPDATE outbox SET dead_at = now() WHERE event_id = $1::uuid`, ids[1])
	exec(t, pool, `UPDATE outbox SET attempts = max_attempts WHERE event_id = $1::uuid`, ids[2])

	got := obClaim(t, pool, "p1", 10)
	if len(got) != 1 || got[0].EventID != ids[3] {
		t.Fatalf("claimed %+v, want only the fourth (pending) event", got)
	}
}

func TestClaimOutbox_ConcurrentPublishersNeverShareEvents(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 50)
	ctx := context.Background()

	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for p := 0; p < 5; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := fmt.Sprintf("p%d", p)
			for {
				items, err := ClaimOutbox(ctx, pool, name, 3, time.Minute)
				if err != nil {
					t.Errorf("%s: %v", name, err)
					return
				}
				if len(items) == 0 {
					return
				}
				mu.Lock()
				for _, it := range items {
					seen[it.EventID]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != len(ids) {
		t.Fatalf("claimed %d distinct events, want %d", len(seen), len(ids))
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("event %s was claimed %d times", id, n)
		}
	}
}

func TestClaimOutbox_RejectsBadArguments(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	obAdd(t, pool, 1)

	cases := map[string]func() ([]OutboxItem, error){
		"empty publisher": func() ([]OutboxItem, error) { return ClaimOutbox(ctx, pool, "", 1, time.Minute) },
		"zero limit":      func() ([]OutboxItem, error) { return ClaimOutbox(ctx, pool, "p1", 0, time.Minute) },
		"zero lease":      func() ([]OutboxItem, error) { return ClaimOutbox(ctx, pool, "p1", 1, 0) },
		"negative lease":  func() ([]OutboxItem, error) { return ClaimOutbox(ctx, pool, "p1", 1, -time.Second) },
	}
	for name, call := range cases {
		t.Run(name, func(t *testing.T) {
			if items, err := call(); err == nil || len(items) != 0 {
				t.Fatalf("items=%v err=%v, want an error and no items", items, err)
			}
		})
	}
	if got := obClaim(t, pool, "p1", 1); len(got) != 1 || got[0].Attempts != 1 {
		t.Fatalf("a rejected call changed the row: %+v", got)
	}
}

// ---- MarkPublished ----

func TestMarkPublished_RecordsTheDelivery(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 1)
	item := obClaim(t, pool, "p1", 1)[0]

	if err := MarkPublished(context.Background(), pool, item.ID, "p1", item.Attempts); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}
	r := obRead(t, pool, ids[0])
	if !r.Published || r.LeaseOwner != "" || r.Dead {
		t.Fatalf("row = %+v, want published, unleased, not dead", r)
	}
	if got := obClaim(t, pool, "p2", 1); len(got) != 0 {
		t.Fatalf("a published event was claimed again: %+v", got)
	}
	// A second mark finds nothing to do.
	assertLeaseLost(t, MarkPublished(context.Background(), pool, item.ID, "p1", item.Attempts))
}

// The delivery really happened, so recording it is right even if the lease
// ran out in the meantime, as long as nobody took the event over.
func TestMarkPublished_AllowedAfterLeaseExpiredIfNobodyReclaimed(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 1)
	item := obClaim(t, pool, "p1", 1)[0]
	obExpire(t, pool, ids[0])

	if err := MarkPublished(context.Background(), pool, item.ID, "p1", item.Attempts); err != nil {
		t.Fatalf("MarkPublished after expiry: %v", err)
	}
	if r := obRead(t, pool, ids[0]); !r.Published {
		t.Fatalf("row = %+v, want published", r)
	}
}

func TestMarkPublished_StalePublisherIsRejected(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	ids := obAdd(t, pool, 1)

	a := obClaim(t, pool, "p1", 1)[0]
	obExpire(t, pool, ids[0])
	b := obClaim(t, pool, "p2", 1)[0]
	if b.ID != a.ID || b.Attempts != 2 {
		t.Fatalf("takeover = %+v after %+v", b, a)
	}

	assertLeaseLost(t, MarkPublished(ctx, pool, a.ID, "p1", a.Attempts)) // old owner, old token
	assertLeaseLost(t, MarkPublished(ctx, pool, b.ID, "p1", b.Attempts)) // wrong owner, right token
	assertLeaseLost(t, MarkPublished(ctx, pool, b.ID, "p2", a.Attempts)) // right owner, old token
	if r := obRead(t, pool, ids[0]); r.Published || r.LeaseOwner != "p2" {
		t.Fatalf("rejected marks changed the row: %+v", r)
	}

	if err := MarkPublished(ctx, pool, b.ID, "p2", b.Attempts); err != nil {
		t.Fatalf("the current holder's mark: %v", err)
	}
}

// ---- FailOutbox ----

func TestFailOutbox_RetryableSchedulesARetry(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 1)
	item := obClaim(t, pool, "p1", 1)[0]

	out, err := FailOutbox(context.Background(), pool, item.ID, "p1", item.Attempts,
		FailParams{Class: ClassRetryable, Message: "broker timeout", Delay: time.Minute})
	if err != nil || out != FailRetried {
		t.Fatalf("outcome=%v err=%v, want FailRetried", out, err)
	}
	r := obRead(t, pool, ids[0])
	if r.Dead || r.Published || r.LeaseOwner != "" || r.DueNow || r.Attempts != 1 {
		t.Fatalf("row = %+v, want pending, unleased, due in the future, attempts 1", r)
	}
	if got := obLastError(t, pool, ids[0]); got != "broker timeout" {
		t.Fatalf("last_error = %q", got)
	}
	if got := obClaim(t, pool, "p2", 1); len(got) != 0 {
		t.Fatalf("claimed an event that is waiting out its delay: %+v", got)
	}
}

func TestFailOutbox_ZeroOrNegativeDelayIsClaimableAtOnce(t *testing.T) {
	for _, delay := range []time.Duration{0, -time.Second} {
		t.Run(delay.String(), func(t *testing.T) {
			pool := testPool(t)
			obAdd(t, pool, 1)
			item := obClaim(t, pool, "p1", 1)[0]

			if _, err := FailOutbox(context.Background(), pool, item.ID, "p1", item.Attempts,
				FailParams{Class: ClassRetryable, Message: "x", Delay: delay}); err != nil {
				t.Fatal(err)
			}
			got := obClaim(t, pool, "p2", 1)
			if len(got) != 1 || got[0].Attempts != 2 {
				t.Fatalf("claim = %+v, want the event again with attempts 2", got)
			}
		})
	}
}

func TestFailOutbox_PermanentFailureIsDeadAtOnce(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 1)
	item := obClaim(t, pool, "p1", 1)[0]

	out, err := FailOutbox(context.Background(), pool, item.ID, "p1", item.Attempts,
		FailParams{Class: ClassPermanent, Message: "topic does not exist", Delay: time.Minute})
	if err != nil || out != FailDead {
		t.Fatalf("outcome=%v err=%v, want FailDead", out, err)
	}
	if r := obRead(t, pool, ids[0]); !r.Dead || r.Published || r.LeaseOwner != "" {
		t.Fatalf("row = %+v, want dead and unleased", r)
	}
	if got := obClaim(t, pool, "p2", 1); len(got) != 0 {
		t.Fatalf("claimed a dead event: %+v", got)
	}
}

func TestFailOutbox_RetryableIsDeadWhenAttemptsAreExhausted(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	ids := obAdd(t, pool, 1)
	exec(t, pool, `UPDATE outbox SET max_attempts = 2 WHERE event_id = $1::uuid`, ids[0])

	one := obClaim(t, pool, "p1", 1)[0]
	if out, err := FailOutbox(ctx, pool, one.ID, "p1", one.Attempts,
		FailParams{Class: ClassRetryable, Message: "x"}); err != nil || out != FailRetried {
		t.Fatalf("first failure: outcome=%v err=%v, want FailRetried", out, err)
	}
	two := obClaim(t, pool, "p1", 1)[0]
	if out, err := FailOutbox(ctx, pool, two.ID, "p1", two.Attempts,
		FailParams{Class: ClassRetryable, Message: "x"}); err != nil || out != FailDead {
		t.Fatalf("second failure: outcome=%v err=%v, want FailDead", out, err)
	}
	if r := obRead(t, pool, ids[0]); !r.Dead {
		t.Fatalf("row = %+v, want dead", r)
	}
}

func TestFailOutbox_RejectedWhenNotTheCurrentHolder(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	ids := obAdd(t, pool, 1)
	p := FailParams{Class: ClassPermanent, Message: "x"}

	a := obClaim(t, pool, "p1", 1)[0]
	obExpire(t, pool, ids[0])
	b := obClaim(t, pool, "p2", 1)[0]

	for name, call := range map[string]func() error{
		"old owner, old token": func() error { _, err := FailOutbox(ctx, pool, a.ID, "p1", a.Attempts, p); return err },
		"wrong owner":          func() error { _, err := FailOutbox(ctx, pool, b.ID, "p1", b.Attempts, p); return err },
		"old token":            func() error { _, err := FailOutbox(ctx, pool, b.ID, "p2", a.Attempts, p); return err },
	} {
		t.Run(name, func(t *testing.T) { assertLeaseLost(t, call()) })
	}
	if r := obRead(t, pool, ids[0]); r.Dead || r.LeaseOwner != "p2" || r.Attempts != 2 {
		t.Fatalf("rejected failures changed the row: %+v", r)
	}
	if got := obLastError(t, pool, ids[0]); got != "" {
		t.Fatalf("a rejected failure left error text %q", got)
	}

	// A published event cannot be failed either.
	if err := MarkPublished(ctx, pool, b.ID, "p2", b.Attempts); err != nil {
		t.Fatal(err)
	}
	_, err := FailOutbox(ctx, pool, b.ID, "p2", b.Attempts, p)
	assertLeaseLost(t, err)
}

// ---- ReapOutbox ----

func TestReapOutbox_DeadLettersEventsThatCrashedEveryAttempt(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	ids := obAdd(t, pool, 1)
	exec(t, pool, `UPDATE outbox SET max_attempts = 1 WHERE event_id = $1::uuid`, ids[0])

	obClaim(t, pool, "p1", 1) // attempts 1 of 1, publisher then "dies"

	n, err := ReapOutbox(ctx, pool, 10)
	if err != nil || n != 0 {
		t.Fatalf("live lease: reaped %d err=%v, want 0", n, err)
	}

	obExpire(t, pool, ids[0])
	n, err = ReapOutbox(ctx, pool, 10)
	if err != nil || n != 1 {
		t.Fatalf("expired lease: reaped %d err=%v, want 1", n, err)
	}
	if r := obRead(t, pool, ids[0]); !r.Dead || r.LeaseOwner != "" {
		t.Fatalf("row = %+v, want dead and unleased", r)
	}
	if got := obLastError(t, pool, ids[0]); got != "lease expired" {
		t.Fatalf("last_error = %q, want %q", got, "lease expired")
	}
	if n, err := ReapOutbox(ctx, pool, 10); err != nil || n != 0 {
		t.Fatalf("second reap: %d err=%v, want 0", n, err)
	}
}

func TestReapOutbox_LeavesEventsWithAttemptsToSpareAlone(t *testing.T) {
	pool := testPool(t)
	ids := obAdd(t, pool, 1)
	obClaim(t, pool, "p1", 1)
	obExpire(t, pool, ids[0])

	if n, err := ReapOutbox(context.Background(), pool, 10); err != nil || n != 0 {
		t.Fatalf("reaped %d err=%v, want 0", n, err)
	}
	if got := obClaim(t, pool, "p2", 1); len(got) != 1 {
		t.Fatalf("the event should be claimable again, got %+v", got)
	}
}

func TestReapOutbox_RespectsTheLimit(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	obAdd(t, pool, 3)
	exec(t, pool, `UPDATE outbox SET max_attempts = 1, attempts = 1`)

	for _, want := range []int{2, 1, 0} {
		n, err := ReapOutbox(ctx, pool, 2)
		if err != nil || n != want {
			t.Fatalf("reaped %d err=%v, want %d", n, err, want)
		}
	}
}