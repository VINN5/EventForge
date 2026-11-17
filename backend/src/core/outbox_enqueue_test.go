package core

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// Enqueue(ctx, q, OutboxEvent) adds an event to the outbox and returns its
// event_id. With a transaction as q, the event commits or rolls back with the
// application's own writes: that is the whole point of the outbox.

type obRow struct {
	Topic       string
	Attempts    int
	MaxAttempts int
	Published   bool
	Dead        bool
	LeaseOwner  string
	DueNow      bool
	PayloadA    string
}

func obRead(t *testing.T, q Querier, eventID string) obRow {
	t.Helper()
	var r obRow
	err := q.QueryRow(context.Background(), `
		SELECT topic, attempts, max_attempts,
		       published_at IS NOT NULL, dead_at IS NOT NULL,
		       COALESCE(lease_owner, ''), next_attempt_at <= now(),
		       COALESCE(payload->>'a', '')
		FROM outbox WHERE event_id = $1::uuid`, eventID).
		Scan(&r.Topic, &r.Attempts, &r.MaxAttempts, &r.Published, &r.Dead,
			&r.LeaseOwner, &r.DueNow, &r.PayloadA)
	if err != nil {
		t.Fatalf("read outbox row %s: %v", eventID, err)
	}
	return r
}

func TestEnqueue_StoresAPendingEvent(t *testing.T) {
	pool := testPool(t)

	id, err := Enqueue(context.Background(), pool, OutboxEvent{
		Topic:   "booking.created",
		Payload: []byte(`{"a":"1"}`),
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if !isUUID(id) {
		t.Fatalf("returned event id %q is not a uuid", id)
	}

	r := obRead(t, pool, id)
	want := obRow{Topic: "booking.created", MaxAttempts: 10, DueNow: true, PayloadA: "1"}
	if r != want {
		t.Fatalf("row = %+v, want %+v", r, want)
	}
}

func TestEnqueue_UsesTheCallersEventID(t *testing.T) {
	pool := testPool(t)
	const mine = "7a1d3c52-0000-4000-8000-00000000abcd"

	id, err := Enqueue(context.Background(), pool, OutboxEvent{
		Topic: "t", Payload: []byte(`{}`), EventID: mine,
	})
	if err != nil {
		t.Fatal(err)
	}
	if id != mine {
		t.Fatalf("id = %q, want %q", id, mine)
	}
	obRead(t, pool, mine) // fails the test if the row is missing
}

func TestEnqueue_DuplicateEventIDIsAnError(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	const mine = "7a1d3c52-0000-4000-8000-00000000abcd"

	if _, err := Enqueue(ctx, pool, OutboxEvent{Topic: "t", Payload: []byte(`{"a":"first"}`), EventID: mine}); err != nil {
		t.Fatal(err)
	}
	_, err := Enqueue(ctx, pool, OutboxEvent{Topic: "t", Payload: []byte(`{"a":"second"}`), EventID: mine})
	if !errors.Is(err, ErrDuplicateEvent) {
		t.Fatalf("err = %v, want ErrDuplicateEvent", err)
	}
	if n := idemCount(t, pool, "outbox"); n != 1 {
		t.Fatalf("outbox rows = %d, want 1", n)
	}
	if r := obRead(t, pool, mine); r.PayloadA != "first" {
		t.Fatalf("the original event was overwritten: payload a = %q", r.PayloadA)
	}
}

func TestEnqueue_RejectsBadInput(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	cases := map[string]OutboxEvent{
		"empty topic":       {Topic: "", Payload: []byte(`{}`)},
		"topic too long":    {Topic: strings.Repeat("t", 256), Payload: []byte(`{}`)},
		"invalid json":      {Topic: "t", Payload: []byte(`{oops`)},
		"empty payload":     {Topic: "t", Payload: nil},
		"event id not uuid": {Topic: "t", Payload: []byte(`{}`), EventID: "nope"},
	}
	for name, e := range cases {
		t.Run(name, func(t *testing.T) {
			if id, err := Enqueue(ctx, pool, e); err == nil || id != "" {
				t.Fatalf("id=%q err=%v, want an error and no id", id, err)
			}
		})
	}
	if n := idemCount(t, pool, "outbox"); n != 0 {
		t.Fatalf("bad input left %d rows", n)
	}
}

// The outbox guarantee: the business change and its event are saved together
// or not at all.
func TestEnqueue_CommitsAndRollsBackWithTheBusinessChange(t *testing.T) {
	pool := testPool(t)
	ensureProbeTable(t, pool)
	ctx := context.Background()

	for _, commit := range []bool{true, false} {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		exec(t, tx, `INSERT INTO effect_probe (note) VALUES ('booking')`)
		if _, err := Enqueue(ctx, tx, OutboxEvent{Topic: "booking.created", Payload: []byte(`{}`)}); err != nil {
			t.Fatalf("Enqueue in tx: %v", err)
		}
		if commit {
			err = tx.Commit(ctx)
		} else {
			err = tx.Rollback(ctx)
		}
		if err != nil {
			t.Fatal(err)
		}
	}

	// Only the committed transaction left anything behind, and both halves.
	if n := idemCount(t, pool, "effect_probe"); n != 1 {
		t.Fatalf("business rows = %d, want 1", n)
	}
	if n := idemCount(t, pool, "outbox"); n != 1 {
		t.Fatalf("outbox rows = %d, want 1", n)
	}
}

// The migration's own safety net: the database refuses contradictory rows, so
// a bug in later code cannot leave a half-leased or published-and-dead event.
func TestOutbox_SchemaRejectsInconsistentRows(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO outbox (topic, payload, event_id)
		VALUES ('t', '{}', gen_random_uuid()) RETURNING id`).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}

	bad := map[string]string{
		"owner without expiry":      `UPDATE outbox SET lease_owner = 'p1' WHERE id = $1`,
		"expiry without owner":      `UPDATE outbox SET lease_expires_at = now() WHERE id = $1`,
		"published and dead":        `UPDATE outbox SET published_at = now(), dead_at = now() WHERE id = $1`,
		"zero max attempts":         `UPDATE outbox SET max_attempts = 0 WHERE id = $1`,
		"negative max attempts":     `UPDATE outbox SET max_attempts = -3 WHERE id = $1`,
	}
	for name, sql := range bad {
		t.Run(name, func(t *testing.T) {
			if _, err := pool.Exec(ctx, sql, id); err == nil {
				t.Fatal("the database accepted an inconsistent row")
			}
		})
	}

	// And the consistent versions are fine.
	if _, err := pool.Exec(ctx, `
		UPDATE outbox SET lease_owner = 'p1', lease_expires_at = now() + interval '1 minute'
		WHERE id = $1`, id); err != nil {
		t.Fatalf("leased row rejected: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE outbox SET published_at = now(), lease_owner = NULL, lease_expires_at = NULL
		WHERE id = $1`, id); err != nil {
		t.Fatalf("published row rejected: %v", err)
	}
}