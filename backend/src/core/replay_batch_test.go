package core

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"
)

// ReplayDeadBatch(ctx, q, filter, limit, actor, reason) replays up to limit
// dead jobs that match the filter, oldest finished first, with one audit row
// per job, in one statement. It returns the replayed ids in that order.

// batchFixture makes four dead jobs:
//
//	a1: type a, tenant t1, died 180 min ago
//	a2: type a, tenant t2, died 120 min ago
//	b1: type b, tenant t1, died  60 min ago
//	b2: type b, tenant t2, died  30 min ago
func batchFixture(t *testing.T, q Querier) (a1, a2, b1, b2 string) {
	t.Helper()
	a1 = makeDead(t, q, "a", "t1", "boom a1", 180*time.Minute)
	a2 = makeDead(t, q, "a", "t2", "boom a2", 120*time.Minute)
	b1 = makeDead(t, q, "b", "t1", "boom b1", 60*time.Minute)
	b2 = makeDead(t, q, "b", "t2", "boom b2", 30*time.Minute)
	return
}

func sameOrder(t *testing.T, what string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %v, want %v", what, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: got %v, want %v", what, got, want)
		}
	}
}

func sameSet(t *testing.T, what string, got, want []string) {
	t.Helper()
	g := append([]string(nil), got...)
	w := append([]string(nil), want...)
	sort.Strings(g)
	sort.Strings(w)
	sameOrder(t, what, g, w)
}

func auditSubjects(t *testing.T, q Querier) []string {
	t.Helper()
	var out []string
	for _, a := range auditRows(t, q) {
		out = append(out, a.Subject)
	}
	return out
}

func replayBatch(t *testing.T, q Querier, f ReplayFilter, limit int) []string {
	t.Helper()
	ids, err := ReplayDeadBatch(context.Background(), q, f, limit, "alice", "provider recovered")
	if err != nil {
		t.Fatalf("ReplayDeadBatch: %v", err)
	}
	return ids
}

func TestReplayDeadBatch_EmptyFilterReplaysEverythingOldestFirst(t *testing.T) {
	pool := testPool(t)
	a1, a2, b1, b2 := batchFixture(t, pool)
	before := readReplayed(t, pool, a1)

	ids := replayBatch(t, pool, ReplayFilter{}, 10)
	sameOrder(t, "replayed ids", ids, []string{a1, a2, b1, b2})

	for _, id := range ids {
		got := readReplayed(t, pool, id)
		if got.State != "queued" || got.Attempts != 0 || got.FinishedSet || !got.RunAtDue {
			t.Fatalf("job %s = %+v, want queued, attempts 0, due, not finished", id, got)
		}
	}
	// The fence is untouched and last_error is kept, as for a single replay.
	if got := readReplayed(t, pool, a1); got.Fence != before.Fence || got.LastError != "boom a1" {
		t.Fatalf("a1 = %+v, want the fence %d and the old error kept", got, before.Fence)
	}

	audit := auditRows(t, pool)
	if len(audit) != 4 {
		t.Fatalf("audit rows = %d, want 4 (one per job)", len(audit))
	}
	for _, a := range audit {
		if a.Actor != "alice" || a.Action != "replay_dead_job" || a.Reason != "provider recovered" {
			t.Fatalf("audit row = %+v", a)
		}
	}
	sameSet(t, "audit subjects", auditSubjects(t, pool), ids)
	if n := obCountAudit(t, pool, `details->>'batch' = 'true'`); n != 4 {
		t.Fatalf("%d audit rows are marked as batch, want 4", n)
	}
}

func obCountAudit(t *testing.T, q Querier, cond string) int {
	t.Helper()
	var n int
	if err := q.QueryRow(context.Background(), `SELECT count(*) FROM audit_log WHERE `+cond).Scan(&n); err != nil {
		t.Fatalf("count audit where %s: %v", cond, err)
	}
	return n
}

func TestReplayDeadBatch_Filters(t *testing.T) {
	cases := map[string]struct {
		filter func(t *testing.T, q Querier) ReplayFilter
		want   func(a1, a2, b1, b2 string) []string
	}{
		"by type": {
			func(t *testing.T, q Querier) ReplayFilter { return ReplayFilter{Type: "a"} },
			func(a1, a2, b1, b2 string) []string { return []string{a1, a2} },
		},
		"by tenant": {
			func(t *testing.T, q Querier) ReplayFilter { return ReplayFilter{TenantID: "t1"} },
			func(a1, a2, b1, b2 string) []string { return []string{a1, b1} },
		},
		"by type and tenant": {
			func(t *testing.T, q Querier) ReplayFilter { return ReplayFilter{Type: "a", TenantID: "t1"} },
			func(a1, a2, b1, b2 string) []string { return []string{a1} },
		},
		"by finished time: after is inclusive, before is exclusive": {
			func(t *testing.T, q Querier) ReplayFilter {
				return ReplayFilter{
					FinishedAfter:  dbTimeAgo(t, q, 150*time.Minute),
					FinishedBefore: dbTimeAgo(t, q, 45*time.Minute),
				}
			},
			func(a1, a2, b1, b2 string) []string { return []string{a2, b1} },
		},
		"nothing matches": {
			func(t *testing.T, q Querier) ReplayFilter { return ReplayFilter{Type: "no-such-type"} },
			func(a1, a2, b1, b2 string) []string { return nil },
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			pool := testPool(t)
			a1, a2, b1, b2 := batchFixture(t, pool)
			want := c.want(a1, a2, b1, b2)

			ids := replayBatch(t, pool, c.filter(t, pool), 10)
			sameOrder(t, "replayed ids", ids, want)
			sameSet(t, "audit subjects", auditSubjects(t, pool), want)

			// Everything the filter did not match is still dead.
			if dead := countByState(t, pool, "dead"); dead != 4-len(want) {
				t.Fatalf("%d jobs are still dead, want %d", dead, 4-len(want))
			}
		})
	}
}

func TestReplayDeadBatch_TheAuditRowRecordsTheFilter(t *testing.T) {
	pool := testPool(t)
	batchFixture(t, pool)
	replayBatch(t, pool, ReplayFilter{Type: "a", TenantID: ""}, 10)

	if n := obCountAudit(t, pool, `details->'filter'->>'type' = 'a'`); n != 2 {
		t.Fatalf("%d audit rows record the type filter, want 2", n)
	}
	if n := obCountAudit(t, pool, `details->'filter'->>'tenant' = ''`); n != 2 {
		t.Fatalf("%d audit rows record the empty tenant filter, want 2", n)
	}
}

// The limit caps one call; calling again continues where the last stopped,
// because replayed jobs are no longer dead.
func TestReplayDeadBatch_TheLimitCapsEachCallAndRepeatedCallsDrainTheBacklog(t *testing.T) {
	pool := testPool(t)
	a1, a2, b1, b2 := batchFixture(t, pool)

	sameOrder(t, "first call", replayBatch(t, pool, ReplayFilter{}, 3), []string{a1, a2, b1})
	sameOrder(t, "second call", replayBatch(t, pool, ReplayFilter{}, 3), []string{b2})
	if third := replayBatch(t, pool, ReplayFilter{}, 3); len(third) != 0 {
		t.Fatalf("third call = %v, want nothing left", third)
	}
	if n := len(auditRows(t, pool)); n != 4 {
		t.Fatalf("audit rows = %d, want 4", n)
	}
}

func TestReplayDeadBatch_OnlyDeadJobsAreTouched(t *testing.T) {
	pool := testPool(t)
	a1, _, _, _ := batchFixture(t, pool)
	queued := submitNoop(t, pool, SubmitParams{Type: "q"})
	running := submitNoop(t, pool, SubmitParams{Type: "r"})
	claimType(t, pool, "r")

	ids := replayBatch(t, pool, ReplayFilter{Type: "a"}, 10)
	if len(ids) != 2 || ids[0] != a1 {
		t.Fatalf("replayed %v", ids)
	}
	if got := readReplayed(t, pool, queued); got.State != "queued" || got.Attempts != 0 {
		t.Fatalf("queued job = %+v", got)
	}
	if got := readReplayed(t, pool, running); got.State != "running" || got.Attempts != 1 {
		t.Fatalf("running job = %+v", got)
	}
	if n := len(auditRows(t, pool)); n != 2 {
		t.Fatalf("audit rows = %d, want 2 (only the replayed jobs)", n)
	}
}

func TestReplayDeadBatch_RejectsBadArgumentsAndChangesNothing(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	batchFixture(t, pool)

	cases := map[string]func() ([]string, error){
		"zero limit":   func() ([]string, error) { return ReplayDeadBatch(ctx, pool, ReplayFilter{}, 0, "alice", "why") },
		"huge limit":   func() ([]string, error) { return ReplayDeadBatch(ctx, pool, ReplayFilter{}, 1001, "alice", "why") },
		"no actor":     func() ([]string, error) { return ReplayDeadBatch(ctx, pool, ReplayFilter{}, 10, "", "why") },
		"blank actor":  func() ([]string, error) { return ReplayDeadBatch(ctx, pool, ReplayFilter{}, 10, "  ", "why") },
		"no reason":    func() ([]string, error) { return ReplayDeadBatch(ctx, pool, ReplayFilter{}, 10, "alice", "") },
		"blank reason": func() ([]string, error) { return ReplayDeadBatch(ctx, pool, ReplayFilter{}, 10, "alice", " \t") },
	}
	for name, call := range cases {
		t.Run(name, func(t *testing.T) {
			if ids, err := call(); err == nil || len(ids) != 0 {
				t.Fatalf("ids=%v err=%v, want an error and no ids", ids, err)
			}
		})
	}
	if dead := countByState(t, pool, "dead"); dead != 4 {
		t.Fatalf("%d jobs are dead, want all 4 still dead", dead)
	}
	if n := len(auditRows(t, pool)); n != 0 {
		t.Fatalf("audit rows = %d, want 0", n)
	}
}

func TestReplayDeadBatch_RollsBackWithTheCallersTransaction(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	batchFixture(t, pool)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if ids, err := ReplayDeadBatch(ctx, tx, ReplayFilter{}, 10, "alice", "oops"); err != nil || len(ids) != 4 {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if dead := countByState(t, pool, "dead"); dead != 4 {
		t.Fatalf("%d jobs dead after the rollback, want 4", dead)
	}
	if n := len(auditRows(t, pool)); n != 0 {
		t.Fatalf("audit rows = %d after the rollback, want 0", n)
	}
}

// Several operators draining the same backlog: no job is replayed twice and
// none is left behind.
func TestReplayDeadBatch_ConcurrentCallersNeverReplayTheSameJobTwice(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	var all []string
	for i := 0; i < 20; i++ {
		all = append(all, makeDead(t, pool, "a", "t1", "boom", time.Minute))
	}

	var mu sync.Mutex
	replayed := map[string]int{}
	record := func(ids []string) {
		mu.Lock()
		defer mu.Unlock()
		for _, id := range ids {
			replayed[id]++
		}
	}

	var wg sync.WaitGroup
	for c := 0; c < 4; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				ids, err := ReplayDeadBatch(ctx, pool, ReplayFilter{}, 5, "operator", "race")
				if err != nil {
					t.Errorf("ReplayDeadBatch: %v", err)
					return
				}
				if len(ids) == 0 {
					return
				}
				record(ids)
			}
		}()
	}
	wg.Wait()

	// A caller may have stopped early because every row it needed was locked
	// by another caller at that moment. Finish whatever is left.
	for {
		ids := replayBatch(t, pool, ReplayFilter{}, 5)
		if len(ids) == 0 {
			break
		}
		record(ids)
	}

	if len(replayed) != len(all) {
		t.Fatalf("%d distinct jobs replayed, want %d", len(replayed), len(all))
	}
	for id, n := range replayed {
		if n != 1 {
			t.Errorf("job %s was replayed %d times", id, n)
		}
	}
	if dead := countByState(t, pool, "dead"); dead != 0 {
		t.Fatalf("%d jobs are still dead", dead)
	}
	sameSet(t, "audit subjects", auditSubjects(t, pool), all)
}