package core

import (
	"context"
	"strings"
	"testing"
)

func ensureEffectsTable(t *testing.T, q Querier) {
	t.Helper()
	ctx := context.Background()
	if _, err := q.Exec(ctx, `CREATE TABLE IF NOT EXISTS effects (
		job_id uuid PRIMARY KEY,
		worker text NOT NULL,
		runs   int  NOT NULL DEFAULT 1,
		at     timestamptz NOT NULL DEFAULT now())`); err != nil {
		t.Fatalf("create effects table: %v", err)
	}
	if _, err := q.Exec(ctx, `TRUNCATE effects`); err != nil {
		t.Fatalf("truncate effects: %v", err)
	}
}

// recordEffect does what the load test's handler does: insert the effect, or
// count a repeat if it already exists.
func recordEffect(t *testing.T, q Querier, jobID, worker string) {
	t.Helper()
	_, err := q.Exec(context.Background(), `
		INSERT INTO effects (job_id, worker) VALUES ($1, $2)
		ON CONFLICT (job_id) DO UPDATE SET runs = effects.runs + 1`, jobID, worker)
	if err != nil {
		t.Fatalf("record effect: %v", err)
	}
}

// healthyRun finishes n jobs the way a correct system does: claim, effect, ack.
func healthyRun(t *testing.T, q Querier, n int) []string {
	t.Helper()
	ensureEffectsTable(t, q)
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id := submitNoop(t, q, SubmitParams{})
		job := claimOne(t, q, "w1")
		recordEffect(t, q, job.ID, "w1")
		if err := Ack(context.Background(), q, job.ID, "w1", job.Fence); err != nil {
			t.Fatalf("healthyRun ack: %v", err)
		}
		ids = append(ids, id)
	}
	return ids
}

func hasProblem(r RunReport, substr string) bool {
	for _, p := range r.Problems {
		if strings.Contains(p, substr) {
			return true
		}
	}
	return false
}

func requireNoProblems(t *testing.T, r RunReport) {
	t.Helper()
	if len(r.Problems) != 0 {
		t.Fatalf("expected a clean run, got problems: %v", r.Problems)
	}
}

func requireProblem(t *testing.T, r RunReport, substr string) {
	t.Helper()
	if !hasProblem(r, substr) {
		t.Fatalf("expected a problem mentioning %q, got: %v", substr, r.Problems)
	}
}

func TestCheckRun_HealthyRunIsClean(t *testing.T) {
	pool := testPool(t)
	healthyRun(t, pool, 3)

	r, err := CheckRun(context.Background(), pool, CheckOptions{ExpectedJobs: 3})
	if err != nil {
		t.Fatalf("CheckRun: %v", err)
	}
	requireNoProblems(t, r)
	if r.Total != 3 || r.Succeeded != 3 || r.Dead != 0 || r.Unfinished != 0 || r.Running != 0 {
		t.Fatalf("job counts: %+v", r)
	}
	if r.Effects != 3 || r.RerunJobs != 0 || r.ExtraRuns != 0 {
		t.Fatalf("effect counts: %+v", r)
	}
	if r.OpenAttempts != 0 || r.LeaseExpiredAttempts != 0 {
		t.Fatalf("attempt counts: %+v", r)
	}
}

func TestCheckRun_EmptyRunIsAProblem(t *testing.T) {
	pool := testPool(t)
	ensureEffectsTable(t, pool)

	r, err := CheckRun(context.Background(), pool, CheckOptions{})
	if err != nil {
		t.Fatalf("CheckRun: %v", err)
	}
	requireProblem(t, r, "no jobs found")
}

// Each case damages a healthy run in one way. The checker must notice.
func TestCheckRun_DetectsDamage(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name   string
		opts   CheckOptions
		damage func(t *testing.T, q Querier, ids []string)
		want   string
	}{
		{
			name: "a succeeded job lost its effect",
			damage: func(t *testing.T, q Querier, ids []string) {
				exec(t, q, `DELETE FROM effects WHERE job_id = $1`, ids[0])
			},
			want: "without an effect",
		},
		{
			name: "an effect with no succeeded job",
			damage: func(t *testing.T, q Querier, ids []string) {
				exec(t, q, `INSERT INTO effects (job_id, worker) VALUES (gen_random_uuid(), 'ghost')`)
			},
			want: "without a succeeded job",
		},
		{
			name: "an effect ran twice in a run that allows no repeats",
			damage: func(t *testing.T, q Querier, ids []string) {
				exec(t, q, `UPDATE effects SET runs = 2 WHERE job_id = $1`, ids[0])
			},
			want: "more than once",
		},
		{
			name: "repeats that no abnormal attempt explains",
			opts: CheckOptions{AllowReruns: true},
			damage: func(t *testing.T, q Querier, ids []string) {
				exec(t, q, `UPDATE effects SET runs = 2 WHERE job_id = $1`, ids[0])
			},
			want: "repeated effects",
		},
		{
			name: "a job stuck running",
			damage: func(t *testing.T, q Querier, ids []string) {
				submitNoop(t, q, SubmitParams{})
				claimOne(t, q, "w1")
			},
			want: "stuck running",
		},
		{
			name: "a job still queued",
			damage: func(t *testing.T, q Querier, ids []string) {
				submitNoop(t, q, SubmitParams{})
			},
			want: "unfinished",
		},
		{
			name: "an attempt row that was never closed",
			damage: func(t *testing.T, q Querier, ids []string) {
				exec(t, q, `UPDATE job_attempts
					SET outcome = NULL, ended_at = NULL, duration_ms = NULL WHERE job_id = $1`, ids[0])
			},
			want: "never closed",
		},
		{
			name: "two succeeded attempts on one job",
			damage: func(t *testing.T, q Querier, ids []string) {
				exec(t, q, `INSERT INTO job_attempts
					(job_id, attempt, fence, worker, started_at, ended_at, outcome)
					VALUES ($1, 9, 99, 'ghost', now(), now(), 'succeeded')`, ids[0])
			},
			want: "more than one succeeded attempt",
		},
		{
			name: "a succeeded job whose attempt was never recorded as succeeded",
			damage: func(t *testing.T, q Querier, ids []string) {
				exec(t, q, `UPDATE job_attempts SET outcome = 'lease_expired' WHERE job_id = $1`, ids[0])
			},
			want: "no succeeded attempt",
		},
		{
			name: "the wrong number of jobs",
			opts: CheckOptions{ExpectedJobs: 5},
			damage: func(t *testing.T, q Querier, ids []string) {
			},
			want: "expected 5 jobs",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool := testPool(t)
			ids := healthyRun(t, pool, 3)
			tc.damage(t, pool, ids)

			r, err := CheckRun(ctx, pool, tc.opts)
			if err != nil {
				t.Fatalf("CheckRun: %v", err)
			}
			requireProblem(t, r, tc.want)
		})
	}
}

func exec(t *testing.T, q Querier, sql string, args ...any) {
	t.Helper()
	if _, err := q.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

// After a crash the effect can legitimately run twice, because it happens
// before the ack. With AllowReruns that is fine, as long as an attempt that
// ended abnormally explains each repeat.
func TestCheckRun_ExplainedRepeatIsAllowedWhenReRunsAreAllowed(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	healthyRun(t, pool, 2)

	id := submitNoop(t, pool, SubmitParams{})
	first := claimOne(t, pool, "w1")
	recordEffect(t, pool, first.ID, "w1") // the effect happens, then the worker dies
	expireLease(t, pool, id)
	if _, err := Reap(ctx, pool, 10); err != nil {
		t.Fatalf("Reap: %v", err)
	}
	second := claimOne(t, pool, "w2")
	recordEffect(t, pool, second.ID, "w2") // the retry repeats it
	if err := Ack(ctx, pool, second.ID, "w2", second.Fence); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	r, err := CheckRun(ctx, pool, CheckOptions{AllowReruns: true})
	if err != nil {
		t.Fatalf("CheckRun: %v", err)
	}
	requireNoProblems(t, r)
	if r.RerunJobs != 1 || r.ExtraRuns != 1 || r.LeaseExpiredAttempts != 1 || r.Succeeded != 3 {
		t.Fatalf("counts: %+v", r)
	}

	// The same run is a failure when repeats are not expected.
	strict, err := CheckRun(ctx, pool, CheckOptions{})
	if err != nil {
		t.Fatalf("CheckRun: %v", err)
	}
	requireProblem(t, strict, "more than once")
}

func TestCheckRun_DeadJobsAreAProblemUnlessAllowed(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	healthyRun(t, pool, 2)

	submitNoop(t, pool, SubmitParams{})
	job := claimOne(t, pool, "w1")
	if _, err := Fail(ctx, pool, job.ID, "w1", job.Fence,
		FailParams{Class: ClassPermanent, Message: "boom"}); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	strict, err := CheckRun(ctx, pool, CheckOptions{})
	if err != nil {
		t.Fatalf("CheckRun: %v", err)
	}
	requireProblem(t, strict, "dead")
	if strict.Dead != 1 {
		t.Fatalf("Dead = %d, want 1", strict.Dead)
	}

	lenient, err := CheckRun(ctx, pool, CheckOptions{AllowDead: true})
	if err != nil {
		t.Fatalf("CheckRun: %v", err)
	}
	requireNoProblems(t, lenient)
}

func TestCheckRun_TenantFilterIgnoresOtherTenants(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	healthyRun(t, pool, 2)
	exec(t, pool, `UPDATE jobs SET tenant_id = 'load'`)

	// A stuck job belonging to someone else must not fail the load tenant's check.
	id := submitNoop(t, pool, SubmitParams{})
	exec(t, pool, `UPDATE jobs SET tenant_id = 'other' WHERE id = $1`, id)

	r, err := CheckRun(ctx, pool, CheckOptions{TenantID: "load", ExpectedJobs: 2})
	if err != nil {
		t.Fatalf("CheckRun: %v", err)
	}
	requireNoProblems(t, r)
	if r.Total != 2 {
		t.Fatalf("Total = %d, want 2", r.Total)
	}

	all, err := CheckRun(ctx, pool, CheckOptions{})
	if err != nil {
		t.Fatalf("CheckRun: %v", err)
	}
	requireProblem(t, all, "unfinished")
}
