package core

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// Both *testing.T and *rapid.T satisfy this.
type fataler interface {
	Fatalf(format string, args ...any)
}

type snap struct {
	State       string
	Owner       string
	Attempts    int
	MaxAttempts int
	Fence       int64
}

func truncateAll(t fataler, q Querier) {
	_, err := q.Exec(context.Background(), `TRUNCATE jobs, job_attempts,
		idempotency_keys, outbox, tenant_limits, audit_log, job_effects RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func snapshotJobs(t fataler, q Querier) map[string]snap {
	rows, err := q.Query(context.Background(), `
		SELECT id, state, COALESCE(lease_owner, ''), attempts, max_attempts, fence
		FROM jobs`)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	defer rows.Close()

	out := map[string]snap{}
	for rows.Next() {
		var id string
		var s snap
		if err := rows.Scan(&id, &s.State, &s.Owner, &s.Attempts, &s.MaxAttempts, &s.Fence); err != nil {
			t.Fatalf("snapshot scan: %v", err)
		}
		out[id] = s
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("snapshot rows: %v", err)
	}
	return out
}

func TestProperty_QueueInvariants(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	rapid.Check(t, func(rt *rapid.T) {
		truncateAll(rt, pool)

		type claimRec struct {
			id, worker string
			fence      int64
		}
		var claims []claimRec
		prev := map[string]snap{}
		submitted := 0

		// Runs after every action: the rules that must always hold.
		check := func(rt *rapid.T) {
			cur := snapshotJobs(rt, pool)
			if len(cur) != submitted {
				rt.Fatalf("have %d jobs, submitted %d", len(cur), submitted)
			}
			for id, s := range cur {
				if (s.State == "running") != (s.Owner != "") {
					rt.Fatalf("job %s: state=%q owner=%q", id, s.State, s.Owner)
				}
				if s.Fence < int64(s.Attempts) {
					rt.Fatalf("job %s: fence %d below attempts %d", id, s.Fence, s.Attempts)
				}
				if s.Attempts > s.MaxAttempts {
					rt.Fatalf("job %s: attempts %d above max_attempts %d", id, s.Attempts, s.MaxAttempts)
				}
				if s.State == "dead" && s.Attempts < s.MaxAttempts {
					rt.Fatalf("job %s: dead with attempts left (%d of %d)", id, s.Attempts, s.MaxAttempts)
				}
				if p, ok := prev[id]; ok {
					// attempts may go down (release); the fence never does.
					if s.Fence < p.Fence {
						rt.Fatalf("job %s: fence went down %d -> %d", id, p.Fence, s.Fence)
					}
					// Succeeded and dead are final.
					if p.State == "succeeded" || p.State == "dead" {
						if s.State != p.State || s.Fence != p.Fence || s.Attempts != p.Attempts {
							rt.Fatalf("job %s left %s: %+v -> %+v", id, p.State, p, s)
						}
					}
				}
			}
			prev = cur
		}

		rt.Repeat(map[string]func(*rapid.T){
			"": check,

			"submit": func(rt *rapid.T) {
				_, err := Submit(ctx, pool, SubmitParams{
					TenantID:    "t1",
					Type:        "noop",
					Payload:     []byte(`{}`),
					Priority:    rapid.IntRange(0, 3).Draw(rt, "priority"),
					MaxAttempts: rapid.IntRange(1, 3).Draw(rt, "max_attempts"),
				})
				if err != nil {
					rt.Fatalf("submit: %v", err)
				}
				submitted++
			},

			"claim": func(rt *rapid.T) {
				worker := fmt.Sprintf("w%d", rapid.IntRange(0, 2).Draw(rt, "worker"))
				limit := rapid.IntRange(1, 3).Draw(rt, "limit")
				jobs, err := Claim(ctx, pool, "default", worker, limit, time.Minute)
				if err != nil {
					rt.Fatalf("claim: %v", err)
				}
				for _, j := range jobs {
					claims = append(claims, claimRec{j.ID, worker, j.Fence})
				}
			},

			// Ack a random past claim, which may be stale. It must succeed
			// exactly when that claim is still the current holder.
			"ack": func(rt *rapid.T) {
				if len(claims) == 0 {
					rt.Skip()
				}
				c := claims[rapid.IntRange(0, len(claims)-1).Draw(rt, "which")]
				before := snapshotJobs(rt, pool)[c.id]
				expected := before.State == "running" &&
					before.Owner == c.worker && before.Fence == c.fence

				err := Ack(ctx, pool, c.id, c.worker, c.fence)
				if err != nil && !errors.Is(err, ErrLeaseLost) {
					rt.Fatalf("ack: %v", err)
				}
				if (err == nil) != expected {
					rt.Fatalf("ack of %+v: succeeded=%v, expected=%v (row was %+v)",
						c, err == nil, expected, before)
				}
			},

			// Make one job's lease expire, then run the real reaper. The
			// expected outcome is worked out from the row before the call.
			"expire_and_reap": func(rt *rapid.T) {
				if len(claims) == 0 {
					rt.Skip()
				}
				c := claims[rapid.IntRange(0, len(claims)-1).Draw(rt, "which")]
				before := snapshotJobs(rt, pool)[c.id]

				_, err := pool.Exec(ctx, `
					UPDATE jobs SET lease_expires_at = now() - interval '1 second'
					WHERE id = $1 AND state = 'running'`, c.id)
				if err != nil {
					rt.Fatalf("expire: %v", err)
				}

				res, err := Reap(ctx, pool, rapid.IntRange(1, 3).Draw(rt, "reap_limit"))
				if err != nil {
					rt.Fatalf("reap: %v", err)
				}

				wantRequeued, wantDead := 0, 0
				if before.State == "running" {
					if before.Attempts >= before.MaxAttempts {
						wantDead = 1
					} else {
						wantRequeued = 1
					}
				}
				if res.Requeued != wantRequeued || res.Dead != wantDead {
					rt.Fatalf("reap result %+v, want requeued=%d dead=%d (row was %+v)",
						res, wantRequeued, wantDead, before)
				}
			},

			// Simulates a release: the job goes back without costing an attempt.
			// attempts goes down; the fence must still never be reused.
			"release": func(rt *rapid.T) {
				if len(claims) == 0 {
					rt.Skip()
				}
				c := claims[rapid.IntRange(0, len(claims)-1).Draw(rt, "which")]
				_, err := pool.Exec(ctx, `
					UPDATE jobs
					SET state = 'queued', lease_owner = NULL, lease_expires_at = NULL,
					    attempts = GREATEST(attempts - 1, 0)
					WHERE id = $1 AND state = 'running'`, c.id)
				if err != nil {
					rt.Fatalf("release: %v", err)
				}
			},
		})
	})
}
