package core

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// SubmitIdempotent(ctx, q, clientID, key, params) returns the job ID and
// whether this call created the job. The first call with a (client, key)
// creates it; later calls with the same request return the same ID and
// created=false; the same key with a different request is a conflict.

func idemParams() SubmitParams {
	return SubmitParams{
		TenantID: "t1",
		Type:     "send_email",
		Payload:  []byte(`{"to":"a@example.com"}`),
	}
}

func idemCount(t *testing.T, q Querier, table string) int {
	t.Helper()
	var n int
	// table is always a literal from these tests, never user input.
	if err := q.QueryRow(context.Background(), `SELECT count(*) FROM `+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// A key must never exist without its job (the foreign key is gone, so only
// the way SubmitIdempotent writes them keeps this true).
func assertNoOrphanKeys(t *testing.T, q Querier) {
	t.Helper()
	var n int
	err := q.QueryRow(context.Background(), `
		SELECT count(*) FROM idempotency_keys k
		LEFT JOIN jobs j ON j.id = k.job_id
		WHERE j.id IS NULL`).Scan(&n)
	if err != nil {
		t.Fatalf("orphan check: %v", err)
	}
	if n != 0 {
		t.Fatalf("%d idempotency keys point at no job", n)
	}
}

func TestSubmitIdempotent_FirstCallCreates(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	p := idemParams()

	id, created, err := SubmitIdempotent(ctx, pool, "client-a", "key-1", p)
	if err != nil {
		t.Fatalf("SubmitIdempotent: %v", err)
	}
	if !created {
		t.Fatal("first call: created = false, want true")
	}

	var state string
	if err := pool.QueryRow(ctx, `SELECT state FROM jobs WHERE id = $1`, id).Scan(&state); err != nil {
		t.Fatalf("read job: %v", err)
	}
	if state != "queued" {
		t.Fatalf("state = %q, want queued", state)
	}

	// The stored hash is exactly what requestHash produces.
	wantHash := mustHash(t, p)
	var gotHash, gotJob string
	err = pool.QueryRow(ctx, `
		SELECT request_hash, job_id::text FROM idempotency_keys
		WHERE client_id = 'client-a' AND key = 'key-1'`).Scan(&gotHash, &gotJob)
	if err != nil {
		t.Fatalf("read key: %v", err)
	}
	if gotHash != wantHash || gotJob != id {
		t.Fatalf("key row: hash=%q job=%q, want hash=%q job=%q", gotHash, gotJob, wantHash, id)
	}
	assertNoOrphanKeys(t, pool)
}

func TestSubmitIdempotent_SameRequestReturnsOriginal(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	first, created, err := SubmitIdempotent(ctx, pool, "client-a", "key-1", idemParams())
	if err != nil || !created {
		t.Fatalf("first: id=%q created=%v err=%v", first, created, err)
	}

	for i := 0; i < 3; i++ {
		id, created, err := SubmitIdempotent(ctx, pool, "client-a", "key-1", idemParams())
		if err != nil {
			t.Fatalf("repeat %d: %v", i, err)
		}
		if created {
			t.Fatalf("repeat %d: created = true, want false", i)
		}
		if id != first {
			t.Fatalf("repeat %d: id = %q, want the original %q", i, id, first)
		}
	}
	if n := idemCount(t, pool, "jobs"); n != 1 {
		t.Fatalf("jobs = %d, want 1", n)
	}
	if n := idemCount(t, pool, "idempotency_keys"); n != 1 {
		t.Fatalf("keys = %d, want 1", n)
	}
}

// Same JSON document, spelled differently: still the same request.
func TestSubmitIdempotent_PayloadSpellingIsTheSameRequest(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	a := idemParams()
	a.Payload = []byte(`{"x":1,"y":2}`)
	b := idemParams()
	b.Payload = []byte("{ \"y\": 2,\n \"x\": 1 }")

	first, _, err := SubmitIdempotent(ctx, pool, "client-a", "key-1", a)
	if err != nil {
		t.Fatal(err)
	}
	second, created, err := SubmitIdempotent(ctx, pool, "client-a", "key-1", b)
	if err != nil {
		t.Fatalf("second spelling: %v", err)
	}
	if created || second != first {
		t.Fatalf("created=%v id=%q, want created=false id=%q", created, second, first)
	}
}

func TestSubmitIdempotent_DifferentRequestIsAConflict(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	first, _, err := SubmitIdempotent(ctx, pool, "client-a", "key-1", idemParams())
	if err != nil {
		t.Fatal(err)
	}

	other := idemParams()
	other.Payload = []byte(`{"to":"someone-else@example.com"}`)
	id, created, err := SubmitIdempotent(ctx, pool, "client-a", "key-1", other)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
	}
	if created || id != "" {
		t.Fatalf("on conflict got id=%q created=%v, want empty and false", id, created)
	}

	// Nothing changed: still one job, and the key still points at the first.
	if n := idemCount(t, pool, "jobs"); n != 1 {
		t.Fatalf("jobs = %d, want 1", n)
	}
	var gotJob string
	if err := pool.QueryRow(ctx, `SELECT job_id::text FROM idempotency_keys
		WHERE client_id = 'client-a' AND key = 'key-1'`).Scan(&gotJob); err != nil {
		t.Fatal(err)
	}
	if gotJob != first {
		t.Fatalf("key now points at %q, want %q", gotJob, first)
	}
}

// Keys are scoped per client: the same key from another client is unrelated.
func TestSubmitIdempotent_ClientsAreIndependent(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	a, createdA, err := SubmitIdempotent(ctx, pool, "client-a", "key-1", idemParams())
	if err != nil || !createdA {
		t.Fatalf("a: created=%v err=%v", createdA, err)
	}
	b, createdB, err := SubmitIdempotent(ctx, pool, "client-b", "key-1", idemParams())
	if err != nil || !createdB {
		t.Fatalf("b: created=%v err=%v", createdB, err)
	}
	if a == b {
		t.Fatal("two clients got the same job")
	}
	if n := idemCount(t, pool, "jobs"); n != 2 {
		t.Fatalf("jobs = %d, want 2", n)
	}
}

func TestSubmitIdempotent_RejectsInvalidInput(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	badTenant := idemParams()
	badTenant.TenantID = ""
	badJSON := idemParams()
	badJSON.Payload = []byte(`{oops`)

	cases := []struct {
		name   string
		client string
		key    string
		p      SubmitParams
	}{
		{"empty client id", "", "k", idemParams()},
		{"empty key", "c", "", idemParams()},
		{"key longer than 255 bytes", "c", strings.Repeat("k", 256), idemParams()},
		{"missing tenant", "c", "k", badTenant},
		{"invalid json", "c", "k", badJSON},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, _, err := SubmitIdempotent(ctx, pool, c.client, c.key, c.p); err == nil {
				t.Fatal("expected an error, got nil")
			}
		})
	}
	if n := idemCount(t, pool, "jobs"); n != 0 {
		t.Fatalf("invalid input left %d jobs behind", n)
	}
	if n := idemCount(t, pool, "idempotency_keys"); n != 0 {
		t.Fatalf("invalid input left %d keys behind", n)
	}
}

// A 255-byte key is allowed; the limit is on the 256th byte.
func TestSubmitIdempotent_AcceptsKeyOfExactlyMaxLength(t *testing.T) {
	pool := testPool(t)
	_, created, err := SubmitIdempotent(context.Background(), pool, "c",
		strings.Repeat("k", 255), idemParams())
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
}

// Transactional enqueue: key and job live and die with the caller's
// transaction. After a rollback the same key can be used again.
func TestSubmitIdempotent_RolledBackWithTransaction(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := SubmitIdempotent(ctx, tx, "client-a", "key-1", idemParams()); err != nil || !created {
		t.Fatalf("in tx: created=%v err=%v", created, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	if n := idemCount(t, pool, "jobs"); n != 0 {
		t.Fatalf("rollback left %d jobs", n)
	}
	if n := idemCount(t, pool, "idempotency_keys"); n != 0 {
		t.Fatalf("rollback left %d keys", n)
	}

	_, created, err := SubmitIdempotent(ctx, pool, "client-a", "key-1", idemParams())
	if err != nil || !created {
		t.Fatalf("after rollback: created=%v err=%v", created, err)
	}
}

// The same transaction submitting the same key twice: the second call must
// see the first one's uncommitted row and return its ID.
func TestSubmitIdempotent_SameKeyTwiceInOneTransaction(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) // harmless after Commit

	first, created, err := SubmitIdempotent(ctx, tx, "client-a", "key-1", idemParams())
	if err != nil || !created {
		t.Fatalf("first: created=%v err=%v", created, err)
	}
	second, created, err := SubmitIdempotent(ctx, tx, "client-a", "key-1", idemParams())
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if created || second != first {
		t.Fatalf("second: id=%q created=%v, want id=%q created=false", second, created, first)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if n := idemCount(t, pool, "jobs"); n != 1 {
		t.Fatalf("jobs = %d, want 1", n)
	}
	assertNoOrphanKeys(t, pool)
}

// Plain Submit validation I flagged earlier: negative values used to either
// surface as a raw database error or silently schedule in the past.
func TestSubmit_RejectsNegativeValues(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	negAttempts := idemParams()
	negAttempts.MaxAttempts = -1
	negDelay := idemParams()
	negDelay.RunAfter = -1

	for name, p := range map[string]SubmitParams{
		"negative max attempts": negAttempts,
		"negative delay":        negDelay,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Submit(ctx, pool, p); err == nil {
				t.Fatal("expected an error, got nil")
			}
			if _, _, err := SubmitIdempotent(ctx, pool, "c", "k", p); err == nil {
				t.Fatal("SubmitIdempotent: expected an error, got nil")
			}
		})
	}
	if n := idemCount(t, pool, "jobs"); n != 0 {
		t.Fatalf("left %d jobs behind", n)
	}
}