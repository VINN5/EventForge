package core

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// ReplayDead(ctx, q, id, actor, reason) puts a dead job back in the queue and
// writes an audit row, in one statement. Attempts restart from 0; the fence is
// never touched, so no token is ever reused.

type auditRow struct {
	Actor, Action, Subject, Reason string
	AttemptsBefore                 int
	FenceBefore                    int64
	LastError                      string
}

func auditRows(t *testing.T, q Querier) []auditRow {
	t.Helper()
	rows, err := q.Query(context.Background(), `
		SELECT actor, action, COALESCE(subject, ''), COALESCE(reason, ''),
		       COALESCE((details->>'attempts_before')::int, -1),
		       COALESCE((details->>'fence_before')::bigint, -1),
		       COALESCE(details->>'last_error', '')
		FROM audit_log ORDER BY id`)
	if err != nil {
		t.Fatalf("read audit_log: %v", err)
	}
	defer rows.Close()
	var out []auditRow
	for rows.Next() {
		var a auditRow
		if err := rows.Scan(&a.Actor, &a.Action, &a.Subject, &a.Reason,
			&a.AttemptsBefore, &a.FenceBefore, &a.LastError); err != nil {
			t.Fatalf("scan audit row: %v", err)
		}
		out = append(out, a)
	}
	return out
}

type replayedRow struct {
	State       string
	Attempts    int
	Fence       int64
	FinishedSet bool
	LastError   string
	RunAtDue    bool
}

func readReplayed(t *testing.T, q Querier, id string) replayedRow {
	t.Helper()
	var r replayedRow
	err := q.QueryRow(context.Background(), `
		SELECT state, attempts, fence, finished_at IS NOT NULL,
		       COALESCE(last_error, ''), run_at <= now()
		FROM jobs WHERE id = $1`, id).
		Scan(&r.State, &r.Attempts, &r.Fence, &r.FinishedSet, &r.LastError, &r.RunAtDue)
	if err != nil {
		t.Fatalf("read job %s: %v", id, err)
	}
	return r
}

func TestReplayDead_RequeuesTheJobAndWritesTheAuditRow(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	id := makeDead(t, pool, "send_email", "t1", "provider said no", time.Minute)
	before := readReplayed(t, pool, id)
	if before.State != "dead" {
		t.Fatalf("setup: state = %q, want dead", before.State)
	}

	if err := ReplayDead(ctx, pool, id, "alice", "provider fixed their API"); err != nil {
		t.Fatalf("ReplayDead: %v", err)
	}

	after := readReplayed(t, pool, id)
	want := replayedRow{State: "queued", Attempts: 0, Fence: before.Fence, FinishedSet: false,
		LastError: "provider said no", RunAtDue: true}
	if after != want {
		t.Fatalf("job = %+v, want %+v", after, want)
	}

	audit := auditRows(t, pool)
	if len(audit) != 1 {
		t.Fatalf("audit rows = %d, want 1", len(audit))
	}
	wantAudit := auditRow{
		Actor: "alice", Action: "replay_dead_job", Subject: id,
		Reason: "provider fixed their API", AttemptsBefore: before.Attempts,
		FenceBefore: before.Fence, LastError: "provider said no",
	}
	if audit[0] != wantAudit {
		t.Fatalf("audit row = %+v, want %+v", audit[0], wantAudit)
	}
}

func TestReplayDead_TheJobCanBeClaimedAgainAndTheFenceKeepsGrowing(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	id := makeDead(t, pool, "send_email", "t1", "boom", time.Minute)
	before := readReplayed(t, pool, id)

	if err := ReplayDead(ctx, pool, id, "alice", "retry"); err != nil {
		t.Fatal(err)
	}
	job := claimType(t, pool, "send_email")
	if job.ID != id {
		t.Fatalf("claimed %s, want the replayed job %s", job.ID, id)
	}
	if job.Fence <= before.Fence {
		t.Fatalf("fence %d is not above the old fence %d: a token was reused", job.Fence, before.Fence)
	}
	if got := readReplayed(t, pool, id); got.Attempts != 1 {
		t.Fatalf("attempts after the first new claim = %d, want 1", got.Attempts)
	}
	// The old holder's token is dead, the new one works.
	assertLeaseLost(t, Ack(ctx, pool, id, "dl-worker", before.Fence))
	if err := Ack(ctx, pool, id, "dl-worker", job.Fence); err != nil {
		t.Fatalf("Ack with the new token: %v", err)
	}
}

// The attempt history survives a replay, and the new attempts sit beside the
// old ones instead of overwriting them.
func TestReplayDead_KeepsTheAttemptHistory(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	id := makeDead(t, pool, "send_email", "t1", "boom", time.Minute)
	rowsBefore := countAttemptRows(t, pool, id)

	if err := ReplayDead(ctx, pool, id, "alice", "retry"); err != nil {
		t.Fatal(err)
	}
	job := claimType(t, pool, "send_email")
	if err := Ack(ctx, pool, id, "dl-worker", job.Fence); err != nil {
		t.Fatal(err)
	}
	if got := countAttemptRows(t, pool, id); got != rowsBefore+1 {
		t.Fatalf("attempt rows = %d, want %d (old history plus the new attempt)", got, rowsBefore+1)
	}
}

func TestReplayDead_OnlyDeadJobsCanBeReplayed(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	queued := submitNoop(t, pool, SubmitParams{Type: "a"})
	running := submitNoop(t, pool, SubmitParams{Type: "b"})
	claimType(t, pool, "b")
	succeeded := submitNoop(t, pool, SubmitParams{Type: "c"})
	job := claimType(t, pool, "c")
	if err := Ack(ctx, pool, succeeded, "dl-worker", job.Fence); err != nil {
		t.Fatal(err)
	}

	cases := map[string]string{
		"queued":    queued,
		"running":   running,
		"succeeded": succeeded,
		"unknown":   "6f1c2b0e-0000-4000-8000-0000000000ff",
		"malformed": "not-a-uuid",
		"uppercase": strings.ToUpper(makeDead(t, pool, "d", "t1", "x", time.Minute)),
	}
	for name, id := range cases {
		t.Run(name, func(t *testing.T) {
			err := ReplayDead(ctx, pool, id, "alice", "try")
			if name == "uppercase" {
				// GetDead accepts uppercase ids; replay must behave the same.
				if err != nil {
					t.Fatalf("uppercase id: %v", err)
				}
				return
			}
			if err == nil || !errors.Is(err, ErrJobNotFound) {
				t.Fatalf("err = %v, want ErrJobNotFound", err)
			}
		})
	}

	// Only the uppercase case replayed anything, so only it audited anything.
	if audit := auditRows(t, pool); len(audit) != 1 {
		t.Fatalf("audit rows = %d, want 1: rejected replays must leave no trace", len(audit))
	}
	if got := readReplayed(t, pool, running); got.State != "running" {
		t.Fatalf("a rejected replay changed the running job: %+v", got)
	}
}

func TestReplayDead_RejectsMissingActorOrReason(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	id := makeDead(t, pool, "send_email", "t1", "boom", time.Minute)

	for name, c := range map[string]struct{ actor, reason string }{
		"no actor":          {"", "because"},
		"blank actor":       {"   ", "because"},
		"no reason":         {"alice", ""},
		"blank reason":      {"alice", "  \t "},
		"neither":           {"", ""},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ReplayDead(ctx, pool, id, c.actor, c.reason); err == nil {
				t.Fatal("expected an error, got nil")
			}
		})
	}
	if got := readReplayed(t, pool, id); got.State != "dead" {
		t.Fatalf("a rejected replay changed the job: %+v", got)
	}
	if audit := auditRows(t, pool); len(audit) != 0 {
		t.Fatalf("audit rows = %d, want 0", len(audit))
	}
}

// Replaying the same job twice: the second call finds it queued, not dead.
func TestReplayDead_TheSecondReplayOfTheSameJobIsRejected(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	id := makeDead(t, pool, "send_email", "t1", "boom", time.Minute)

	if err := ReplayDead(ctx, pool, id, "alice", "first"); err != nil {
		t.Fatal(err)
	}
	if err := ReplayDead(ctx, pool, id, "bob", "second"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("err = %v, want ErrJobNotFound", err)
	}
	if audit := auditRows(t, pool); len(audit) != 1 || audit[0].Actor != "alice" {
		t.Fatalf("audit rows = %+v, want only alice's", audit)
	}
}

// Two operators replaying one job at once: exactly one wins and exactly one
// audit row exists.
func TestReplayDead_ConcurrentReplaysHaveExactlyOneWinner(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	id := makeDead(t, pool, "send_email", "t1", "boom", time.Minute)

	const n = 10
	errs := make([]error, n)
	start := make(chan struct{})
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go func() {
			<-start
			errs[i] = ReplayDead(ctx, pool, id, "operator", "race")
			done <- struct{}{}
		}()
	}
	close(start)
	for i := 0; i < n; i++ {
		<-done
	}

	winners := 0
	for i, err := range errs {
		switch {
		case err == nil:
			winners++
		case !errors.Is(err, ErrJobNotFound):
			t.Errorf("caller %d: %v", i, err)
		}
	}
	if winners != 1 {
		t.Fatalf("%d callers replayed the job, want exactly 1", winners)
	}
	if audit := auditRows(t, pool); len(audit) != 1 {
		t.Fatalf("audit rows = %d, want 1", len(audit))
	}
}

// The audit row and the state change commit together: if the caller's
// transaction rolls back, neither survives.
func TestReplayDead_RollsBackWithTheCallersTransaction(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	id := makeDead(t, pool, "send_email", "t1", "boom", time.Minute)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := ReplayDead(ctx, tx, id, "alice", "oops"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	if got := readReplayed(t, pool, id); got.State != "dead" {
		t.Fatalf("job = %+v after rollback, want still dead", got)
	}
	if audit := auditRows(t, pool); len(audit) != 0 {
		t.Fatalf("audit rows = %d after rollback, want 0", len(audit))
	}
}
