package core

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func claimType(t *testing.T, q Querier, typ string) Job {
	t.Helper()
	jobs, err := ClaimTypes(context.Background(), q, "default", "dl-worker", []string{typ}, 1, 30*time.Second)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim of type %q: jobs=%d err=%v", typ, len(jobs), err)
	}
	return jobs[0]
}

// makeDead creates a job of the given type that ends up dead after a permanent
// failure, then sets its tenant and finish time so tests control them exactly.
// Use a type no other queued job in the test shares.
func makeDead(t *testing.T, q Querier, typ, tenant, message string, finishedAgo time.Duration) string {
	t.Helper()
	ctx := context.Background()

	id := submitNoop(t, q, SubmitParams{Type: typ})
	job := claimType(t, q, typ)
	if job.ID != id {
		t.Fatalf("makeDead claimed %s, want %s", job.ID, id)
	}
	if _, err := Fail(ctx, q, id, "dl-worker", job.Fence,
		FailParams{Class: ClassPermanent, Message: message}); err != nil {
		t.Fatalf("makeDead fail: %v", err)
	}
	_, err := q.Exec(ctx, `
		UPDATE jobs
		SET tenant_id = $2, finished_at = now() - make_interval(secs => $3::float8)
		WHERE id = $1`, id, tenant, finishedAgo.Seconds())
	if err != nil {
		t.Fatalf("makeDead set metadata: %v", err)
	}
	return id
}

func dbTimeAgo(t *testing.T, q Querier, ago time.Duration) time.Time {
	t.Helper()
	var ts time.Time
	err := q.QueryRow(context.Background(),
		`SELECT now() - make_interval(secs => $1::float8)`, ago.Seconds()).Scan(&ts)
	if err != nil {
		t.Fatalf("read database time: %v", err)
	}
	return ts
}

func deadIDs(jobs []DeadJob) []string {
	ids := make([]string, len(jobs))
	for i, j := range jobs {
		ids[i] = j.ID
	}
	return ids
}

func requireIDs(t *testing.T, got []DeadJob, want ...string) {
	t.Helper()
	g := deadIDs(got)
	if len(g) != len(want) {
		t.Fatalf("got ids %v, want %v", g, want)
	}
	for i := range want {
		if g[i] != want[i] {
			t.Fatalf("got ids %v, want %v", g, want)
		}
	}
}

func TestListDead_ReturnsOnlyDeadJobsNewestFirst(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	oldest := makeDead(t, pool, "a", "t1", "oldest error", 300*time.Second)
	middle := makeDead(t, pool, "a", "t1", "middle error", 200*time.Second)
	newest := makeDead(t, pool, "a", "t1", "newest error", 100*time.Second)

	// Jobs in other states must not show up.
	submitNoop(t, pool, SubmitParams{Type: "live-queued"})
	submitNoop(t, pool, SubmitParams{Type: "live-running"})
	claimType(t, pool, "live-running")
	okID := submitNoop(t, pool, SubmitParams{Type: "live-ok"})
	ok := claimType(t, pool, "live-ok")
	if err := Ack(ctx, pool, okID, "dl-worker", ok.Fence); err != nil {
		t.Fatalf("setup ack: %v", err)
	}

	jobs, next, err := ListDead(ctx, pool, DeadFilter{})
	if err != nil {
		t.Fatalf("ListDead: %v", err)
	}
	requireIDs(t, jobs, newest, middle, oldest)
	if next != nil {
		t.Fatalf("next cursor = %+v, want nil", next)
	}

	j := jobs[0]
	if j.Type != "a" || j.TenantID != "t1" || j.Queue != "default" ||
		j.LastError != "newest error" || j.Attempts != 1 {
		t.Fatalf("job fields: %+v", j)
	}
	if j.CreatedAt.IsZero() || j.FinishedAt.IsZero() {
		t.Fatalf("timestamps missing: %+v", j)
	}
}

func TestListDead_Empty(t *testing.T) {
	pool := testPool(t)

	jobs, next, err := ListDead(context.Background(), pool, DeadFilter{})
	if err != nil || len(jobs) != 0 || next != nil {
		t.Fatalf("jobs=%v next=%v err=%v, want nothing", jobs, next, err)
	}
}

func TestListDead_Filters(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	emailA := makeDead(t, pool, "email", "tenantA", "e1", 300*time.Second)
	emailB := makeDead(t, pool, "email", "tenantB", "e2", 200*time.Second)
	smsA := makeDead(t, pool, "sms", "tenantA", "e3", 100*time.Second)

	tests := []struct {
		name   string
		filter DeadFilter
		want   []string
	}{
		{"by type", DeadFilter{Type: "email"}, []string{emailB, emailA}},
		{"by tenant", DeadFilter{TenantID: "tenantA"}, []string{smsA, emailA}},
		{"type and tenant", DeadFilter{Type: "email", TenantID: "tenantA"}, []string{emailA}},
		{"no match", DeadFilter{Type: "email", TenantID: "nobody"}, nil},
		{"finished after", DeadFilter{FinishedAfter: dbTimeAgo(t, pool, 250*time.Second)}, []string{smsA, emailB}},
		{"finished before", DeadFilter{FinishedBefore: dbTimeAgo(t, pool, 150*time.Second)}, []string{emailB, emailA}},
		{"time range", DeadFilter{
			FinishedAfter:  dbTimeAgo(t, pool, 250*time.Second),
			FinishedBefore: dbTimeAgo(t, pool, 150*time.Second),
		}, []string{emailB}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			jobs, _, err := ListDead(ctx, pool, tc.filter)
			if err != nil {
				t.Fatalf("ListDead: %v", err)
			}
			requireIDs(t, jobs, tc.want...)
		})
	}
}

func TestListDead_PagesWithoutGapsOrRepeats(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	// Created oldest first, so newest-first order is the reverse.
	var created []string
	for i := 7; i >= 1; i-- {
		created = append(created, makeDead(t, pool, "p", "t1", "x", time.Duration(i*100)*time.Second))
	}
	var want []string
	for i := len(created) - 1; i >= 0; i-- {
		want = append(want, created[i])
	}

	var got []string
	var sizes []int
	var cursor *DeadCursor
	for page := 0; page < 10; page++ {
		jobs, next, err := ListDead(ctx, pool, DeadFilter{Limit: 3, After: cursor})
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		got = append(got, deadIDs(jobs)...)
		sizes = append(sizes, len(jobs))
		if next == nil {
			break
		}
		cursor = next
	}

	if len(sizes) != 3 || sizes[0] != 3 || sizes[1] != 3 || sizes[2] != 1 {
		t.Fatalf("page sizes = %v, want [3 3 1]", sizes)
	}
	requireIDs(t, toDead(got), want...)
}

func toDead(ids []string) []DeadJob {
	out := make([]DeadJob, len(ids))
	for i, id := range ids {
		out[i] = DeadJob{ID: id}
	}
	return out
}

// With exactly two full pages there must be no third, empty page.
func TestListDead_ExactMultipleHasNoTrailingPage(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	for i := 6; i >= 1; i-- {
		makeDead(t, pool, "p", "t1", "x", time.Duration(i*100)*time.Second)
	}

	first, next, err := ListDead(ctx, pool, DeadFilter{Limit: 3})
	if err != nil || len(first) != 3 || next == nil {
		t.Fatalf("page 1: len=%d next=%v err=%v, want 3 and a cursor", len(first), next, err)
	}
	second, next, err := ListDead(ctx, pool, DeadFilter{Limit: 3, After: next})
	if err != nil || len(second) != 3 || next != nil {
		t.Fatalf("page 2: len=%d next=%v err=%v, want 3 and no cursor", len(second), next, err)
	}
}

// Jobs that died in the same instant are still returned once each, in id order.
func TestListDead_PagingSurvivesIdenticalTimestamps(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		makeDead(t, pool, "tie", "t1", "x", time.Minute)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE jobs SET finished_at = '2026-01-01T00:00:00Z' WHERE state = 'dead'`); err != nil {
		t.Fatalf("set identical timestamps: %v", err)
	}

	var got []string
	var cursor *DeadCursor
	for page := 0; page < 10; page++ {
		jobs, next, err := ListDead(ctx, pool, DeadFilter{Limit: 1, After: cursor})
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		got = append(got, deadIDs(jobs)...)
		if next == nil {
			break
		}
		cursor = next
	}

	if len(got) != 4 {
		t.Fatalf("got %d jobs over all pages, want 4: %v", len(got), got)
	}
	seen := map[string]bool{}
	for i, id := range got {
		if seen[id] {
			t.Fatalf("job %s returned twice: %v", id, got)
		}
		seen[id] = true
		if i > 0 && got[i-1] <= id {
			t.Fatalf("ties are not in descending id order: %v", got)
		}
	}
}

func TestListDead_LimitHandling(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	for i := 3; i >= 1; i-- {
		makeDead(t, pool, "l", "t1", "x", time.Duration(i*100)*time.Second)
	}

	jobs, _, err := ListDead(ctx, pool, DeadFilter{Limit: 0})
	if err != nil || len(jobs) != 3 {
		t.Fatalf("limit 0: len=%d err=%v, want the default to return all 3", len(jobs), err)
	}
	if _, _, err := ListDead(ctx, pool, DeadFilter{Limit: -1}); err == nil {
		t.Fatal("a negative limit should be an error")
	}
	if _, _, err := ListDead(ctx, pool, DeadFilter{Limit: 100000}); err != nil {
		t.Fatalf("a huge limit should be clamped, not rejected: %v", err)
	}
}

func TestGetDead_ReturnsPayloadAndFullAttemptHistory(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{Type: "hist", MaxAttempts: 3})
	if _, err := pool.Exec(ctx,
		`UPDATE jobs SET payload = '{"to":"a@b.c"}'::jsonb, tenant_id = 't9' WHERE id = $1`, id); err != nil {
		t.Fatalf("set payload: %v", err)
	}

	// Attempt 1: retryable failure. Attempt 2: the worker dies. Attempt 3: permanent failure.
	first := claimType(t, pool, "hist")
	if _, err := Fail(ctx, pool, id, "dl-worker", first.Fence,
		FailParams{Class: ClassRetryable, Message: "first failure"}); err != nil {
		t.Fatalf("fail 1: %v", err)
	}
	claimType(t, pool, "hist")
	expireLease(t, pool, id)
	if _, err := Reap(ctx, pool, 10); err != nil {
		t.Fatalf("reap: %v", err)
	}
	third := claimType(t, pool, "hist")
	if _, err := Fail(ctx, pool, id, "dl-worker", third.Fence,
		FailParams{Class: ClassPermanent, Message: "final failure"}); err != nil {
		t.Fatalf("fail 3: %v", err)
	}

	d, err := GetDead(ctx, pool, id)
	if err != nil {
		t.Fatalf("GetDead: %v", err)
	}
	if d.ID != id || d.TenantID != "t9" || d.Type != "hist" ||
		d.Attempts != 3 || d.MaxAttempts != 3 || d.LastError != "final failure" {
		t.Fatalf("job fields: %+v", d)
	}
	if d.FinishedAt.IsZero() {
		t.Fatal("FinishedAt is zero")
	}

	var payload map[string]string
	if err := json.Unmarshal(d.Payload, &payload); err != nil || payload["to"] != "a@b.c" {
		t.Fatalf("payload = %s (err %v)", d.Payload, err)
	}

	wantOutcomes := []string{"retryable_failure", "lease_expired", "permanent_failure"}
	wantErrors := []string{"first failure", "lease expired", "final failure"}
	if len(d.History) != 3 {
		t.Fatalf("history has %d rows, want 3: %+v", len(d.History), d.History)
	}
	for i, h := range d.History {
		if h.Fence != int64(i+1) || h.Attempt != i+1 {
			t.Errorf("row %d: attempt=%d fence=%d, want %d and %d", i, h.Attempt, h.Fence, i+1, i+1)
		}
		if h.Outcome != wantOutcomes[i] || h.Error != wantErrors[i] {
			t.Errorf("row %d: outcome=%q error=%q, want %q and %q",
				i, h.Outcome, h.Error, wantOutcomes[i], wantErrors[i])
		}
		if h.Worker != "dl-worker" || h.StartedAt.IsZero() || h.EndedAt == nil || h.DurationMS == nil {
			t.Errorf("row %d incomplete: %+v", i, h)
		}
	}
}

func TestGetDead_NotFound(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	queuedID := submitNoop(t, pool, SubmitParams{Type: "live"})

	for name, id := range map[string]string{
		"unknown id":     "00000000-0000-0000-0000-000000000000",
		"not a uuid":     "not-a-uuid",
		"empty":          "",
		"job not dead":   queuedID,
		"sql-ish string": "x'; DROP TABLE jobs; --",
	} {
		if _, err := GetDead(ctx, pool, id); !errors.Is(err, ErrJobNotFound) {
			t.Errorf("%s: got %v, want ErrJobNotFound", name, err)
		}
	}
}

func TestGetDead_AcceptsUppercaseID(t *testing.T) {
	pool := testPool(t)

	id := makeDead(t, pool, "u", "t1", "x", time.Minute)
	d, err := GetDead(context.Background(), pool, strings.ToUpper(id))
	if err != nil || d.ID != id {
		t.Fatalf("GetDead(upper): id=%q err=%v, want %s", d.ID, err, id)
	}
}
