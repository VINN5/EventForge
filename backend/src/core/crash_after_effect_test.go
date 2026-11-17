package core

import (
	"context"
	"testing"
)

// crash_after_effect: a worker performs the effect and then dies before it
// can ack. The lease expires, the reaper requeues the job, another attempt
// runs, and the effect must still have happened once.
//
// The crash is simulated by expiring the lease (as killUntilDead does); the
// real-process version runs through the loadtest harness.

func cfxOutcomes(t *testing.T, q Querier, jobID string) map[string]int {
	t.Helper()
	rows, err := q.Query(context.Background(), `
		SELECT COALESCE(outcome, 'open'), count(*) FROM job_attempts
		WHERE job_id = $1 GROUP BY 1`, jobID)
	if err != nil {
		t.Fatalf("attempt outcomes: %v", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var outcome string
		var n int
		if err := rows.Scan(&outcome, &n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[outcome] = n
	}
	return out
}

func cfxClaimAs(t *testing.T, q Querier, worker, typ string) Job {
	t.Helper()
	jobs, err := ClaimTypes(context.Background(), q, "default", worker, []string{typ}, 1, 30*1e9)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim by %s: jobs=%d err=%v", worker, len(jobs), err)
	}
	return jobs[0]
}

func cfxJobState(t *testing.T, q Querier, id string) (string, int) {
	t.Helper()
	var state string
	var attempts int
	err := q.QueryRow(context.Background(),
		`SELECT state, attempts FROM jobs WHERE id = $1`, id).Scan(&state, &attempts)
	if err != nil {
		t.Fatalf("read job: %v", err)
	}
	return state, attempts
}

func TestCrashAfterEffect_IdempotentEffectHappensOnce(t *testing.T) {
	pool := testPool(t)
	ensureProbeTable(t, pool)
	ctx := context.Background()

	const crashes = 3
	id := submitNoop(t, pool, SubmitParams{Type: "crash_fx", MaxAttempts: 5})

	effectsRun := 0
	for attempt := 1; attempt <= crashes+1; attempt++ {
		job := cfxClaimAs(t, pool, "w1", "crash_fx")
		ran, err := RunOnce(ctx, pool, job.ID, "main", probeEffect("fx"))
		if err != nil {
			t.Fatalf("attempt %d: RunOnce: %v", attempt, err)
		}
		if ran {
			effectsRun++
		}
		if attempt <= crashes {
			// The worker dies here: effect committed, ack never sent.
			expireLease(t, pool, id)
			if _, err := Reap(ctx, pool, 10); err != nil {
				t.Fatalf("Reap: %v", err)
			}
			continue
		}
		if err := Ack(ctx, pool, job.ID, "w1", job.Fence); err != nil {
			t.Fatalf("final Ack: %v", err)
		}
	}

	if effectsRun != 1 {
		t.Fatalf("effect executed %d times across %d attempts, want 1", effectsRun, crashes+1)
	}
	if n := idemCount(t, pool, "effect_probe"); n != 1 {
		t.Fatalf("probe rows = %d, want 1", n)
	}
	if state, attempts := cfxJobState(t, pool, id); state != "succeeded" || attempts != crashes+1 {
		t.Fatalf("job: state=%q attempts=%d, want succeeded and %d", state, attempts, crashes+1)
	}
	out := cfxOutcomes(t, pool, id)
	if out["lease_expired"] != crashes || out["succeeded"] != 1 || len(out) != 2 {
		t.Fatalf("attempt outcomes = %v, want %d lease_expired and 1 succeeded", out, crashes)
	}
}

// The negative control: the same crashes with a plain, non-idempotent effect.
// Every retry repeats it. This is the problem RunOnce exists to solve, and it
// shows that this kind of test can see a duplicate.
func TestCrashAfterEffect_PlainEffectRepeatsOnEveryRetry(t *testing.T) {
	pool := testPool(t)
	ensureProbeTable(t, pool)
	ctx := context.Background()

	const crashes = 3
	id := submitNoop(t, pool, SubmitParams{Type: "crash_fx", MaxAttempts: 5})

	for attempt := 1; attempt <= crashes+1; attempt++ {
		job := cfxClaimAs(t, pool, "w1", "crash_fx")
		exec(t, pool, `INSERT INTO effect_probe (note) VALUES ('plain')`)
		if attempt <= crashes {
			expireLease(t, pool, id)
			if _, err := Reap(ctx, pool, 10); err != nil {
				t.Fatalf("Reap: %v", err)
			}
			continue
		}
		if err := Ack(ctx, pool, job.ID, "w1", job.Fence); err != nil {
			t.Fatalf("final Ack: %v", err)
		}
	}
	if n := idemCount(t, pool, "effect_probe"); n != crashes+1 {
		t.Fatalf("probe rows = %d, want %d (one per attempt)", n, crashes+1)
	}
}

// A slow worker that lost its lease: A did the effect and stalled, B took
// over and finished, A wakes up. A's ack must be rejected and the effect must
// still be counted once.
func TestCrashAfterEffect_StaleWorkerAfterTakeover(t *testing.T) {
	pool := testPool(t)
	ensureProbeTable(t, pool)
	ctx := context.Background()

	id := submitNoop(t, pool, SubmitParams{Type: "crash_fx", MaxAttempts: 5})

	a := cfxClaimAs(t, pool, "wA", "crash_fx")
	if ran, err := RunOnce(ctx, pool, a.ID, "main", probeEffect("A")); err != nil || !ran {
		t.Fatalf("A: ran=%v err=%v, want ran=true", ran, err)
	}

	// A stalls past its lease; the reaper hands the job to B.
	expireLease(t, pool, id)
	if _, err := Reap(ctx, pool, 10); err != nil {
		t.Fatalf("Reap: %v", err)
	}
	b := cfxClaimAs(t, pool, "wB", "crash_fx")
	if b.Fence <= a.Fence {
		t.Fatalf("B's fence %d is not above A's %d", b.Fence, a.Fence)
	}
	if ran, err := RunOnce(ctx, pool, b.ID, "main", probeEffect("B")); err != nil || ran {
		t.Fatalf("B: ran=%v err=%v, want ran=false (A's effect already committed)", ran, err)
	}
	if err := Ack(ctx, pool, b.ID, "wB", b.Fence); err != nil {
		t.Fatalf("B Ack: %v", err)
	}

	// A wakes up: its ack is rejected, and rerunning its effect does nothing.
	assertLeaseLost(t, Ack(ctx, pool, a.ID, "wA", a.Fence))
	if ran, err := RunOnce(ctx, pool, a.ID, "main", probeEffect("A again")); err != nil || ran {
		t.Fatalf("A again: ran=%v err=%v, want ran=false", ran, err)
	}

	if n := idemCount(t, pool, "effect_probe"); n != 1 {
		t.Fatalf("probe rows = %d, want 1", n)
	}
	if state, _ := cfxJobState(t, pool, id); state != "succeeded" {
		t.Fatalf("state = %q, want succeeded", state)
	}
	out := cfxOutcomes(t, pool, id)
	if out["lease_expired"] != 1 || out["succeeded"] != 1 || len(out) != 2 {
		t.Fatalf("attempt outcomes = %v, want 1 lease_expired and 1 succeeded", out)
	}
}