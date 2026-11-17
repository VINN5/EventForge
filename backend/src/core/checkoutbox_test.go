package core

import (
	"context"
	"testing"
)

// CheckOutbox(ctx, q, opts) judges an outbox run from the database alone.
// It reads the outbox plus two tables the load tester owns: broker_log (one
// row per delivery, so a duplicate delivery is a second row) and
// consumer_effects (one row per event the consumer applied, with a runs
// counter that goes up on a repeat).

func cobSetup(t *testing.T, q Querier) {
	t.Helper()
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS broker_log (
			id bigserial PRIMARY KEY, event_id uuid NOT NULL, publisher text NOT NULL,
			at timestamptz NOT NULL DEFAULT now())`,
		`CREATE TABLE IF NOT EXISTS consumer_effects (
			event_id uuid PRIMARY KEY, runs int NOT NULL DEFAULT 1,
			at timestamptz NOT NULL DEFAULT now())`,
		`TRUNCATE broker_log, consumer_effects`,
	} {
		if _, err := q.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}
}

// cobEvent inserts an outbox event in state "published", "dead" or "pending"
// and returns its event id.
func cobEvent(t *testing.T, q Querier, state string) string {
	t.Helper()
	var id string
	err := q.QueryRow(context.Background(), `
		INSERT INTO outbox (topic, payload, event_id, attempts, published_at, dead_at)
		VALUES ('t', '{}', gen_random_uuid(), 1,
		        CASE WHEN $1::text = 'published' THEN now() END,
		        CASE WHEN $1::text = 'dead' THEN now() END)
		RETURNING event_id::text`, state).Scan(&id)
	if err != nil {
		t.Fatalf("insert %s event: %v", state, err)
	}
	return id
}

func cobDeliver(t *testing.T, q Querier, eventID string, times int) {
	t.Helper()
	exec(t, q, `INSERT INTO broker_log (event_id, publisher)
		SELECT $1::uuid, 'p1' FROM generate_series(1, $2::int)`, eventID, times)
}

func cobApply(t *testing.T, q Querier, eventID string, runs int) {
	t.Helper()
	exec(t, q, `INSERT INTO consumer_effects (event_id, runs) VALUES ($1::uuid, $2)`, eventID, runs)
}

// cobHealthy makes n published events, each delivered once and applied once.
func cobHealthy(t *testing.T, q Querier, n int) []string {
	t.Helper()
	ids := make([]string, n)
	for i := range ids {
		ids[i] = cobEvent(t, q, "published")
		cobDeliver(t, q, ids[i], 1)
		cobApply(t, q, ids[i], 1)
	}
	return ids
}

func cobCheck(t *testing.T, q Querier, opts OutboxCheckOptions) OutboxCheckResult {
	t.Helper()
	r, err := CheckOutbox(context.Background(), q, opts)
	if err != nil {
		t.Fatalf("CheckOutbox: %v", err)
	}
	return r
}

func TestCheckOutbox_HealthyRunIsClean(t *testing.T) {
	pool := testPool(t)
	cobSetup(t, pool)
	ids := cobHealthy(t, pool, 3)
	cobDeliver(t, pool, ids[0], 2) // two more deliveries of the first event

	r := cobCheck(t, pool, OutboxCheckOptions{ExpectedEvents: 3, MinExtraDeliveries: 2})
	if len(r.Problems) != 0 {
		t.Fatalf("problems on a healthy run: %v", r.Problems)
	}
	if r.Total != 3 || r.Published != 3 || r.Deliveries != 5 || r.ExtraDeliveries != 2 || r.ConsumerRows != 3 {
		t.Fatalf("result = %+v", r)
	}
}

func TestCheckOutbox_EmptyRunIsAProblem(t *testing.T) {
	pool := testPool(t)
	cobSetup(t, pool)
	if r := cobCheck(t, pool, OutboxCheckOptions{}); len(r.Problems) == 0 {
		t.Fatal("an empty outbox passed; a run with no events proves nothing")
	}
}

func TestCheckOutbox_DetectsDamage(t *testing.T) {
	cases := map[string]struct {
		damage func(t *testing.T, q Querier, ids []string)
		count  func(r OutboxCheckResult) int // the field that must read 1
	}{
		"a published event that was never delivered": {
			func(t *testing.T, q Querier, ids []string) {
				exec(t, q, `DELETE FROM broker_log WHERE event_id = $1::uuid`, ids[0])
			},
			func(r OutboxCheckResult) int { return r.PublishedWithoutDelivery },
		},
		"an event that never finished": {
			func(t *testing.T, q Querier, ids []string) { cobEvent(t, q, "pending") },
			func(r OutboxCheckResult) int { return r.Unfinished },
		},
		"a dead event": {
			func(t *testing.T, q Querier, ids []string) { cobEvent(t, q, "dead") },
			func(r OutboxCheckResult) int { return r.Dead },
		},
		"a delivery of an event the outbox does not know": {
			func(t *testing.T, q Querier, ids []string) {
				exec(t, q, `INSERT INTO broker_log (event_id, publisher) VALUES (gen_random_uuid(), 'p1')`)
			},
			func(r OutboxCheckResult) int { return r.UnknownDeliveries },
		},
		"a published event the consumer never applied": {
			func(t *testing.T, q Querier, ids []string) {
				exec(t, q, `DELETE FROM consumer_effects WHERE event_id = $1::uuid`, ids[0])
			},
			func(r OutboxCheckResult) int { return r.MissingConsumerEffects },
		},
		"a consumer effect for an event that is not published": {
			func(t *testing.T, q Querier, ids []string) { cobApply(t, q, cobEvent(t, q, "pending"), 1) },
			func(r OutboxCheckResult) int { return r.OrphanConsumerEffects },
		},
		"a published event that still holds a lease": {
			func(t *testing.T, q Querier, ids []string) {
				exec(t, q, `UPDATE outbox SET lease_owner = 'p1', lease_expires_at = now() + interval '1 minute'
					WHERE event_id = $1::uuid`, ids[0])
			},
			func(r OutboxCheckResult) int { return r.PublishedStillLeased },
		},
		"an event the consumer applied twice": {
			func(t *testing.T, q Querier, ids []string) {
				exec(t, q, `UPDATE consumer_effects SET runs = 2 WHERE event_id = $1::uuid`, ids[0])
			},
			func(r OutboxCheckResult) int { return r.RepeatedConsumerEffects },
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			pool := testPool(t)
			cobSetup(t, pool)
			ids := cobHealthy(t, pool, 3)
			c.damage(t, pool, ids)

			r := cobCheck(t, pool, OutboxCheckOptions{})
			if got := c.count(r); got != 1 {
				t.Fatalf("the matching count = %d, want 1 (result %+v)", got, r)
			}
			if len(r.Problems) == 0 {
				t.Fatalf("no problem reported (result %+v)", r)
			}
		})
	}
}

func TestCheckOutbox_ExpectedEventCountMustMatch(t *testing.T) {
	pool := testPool(t)
	cobSetup(t, pool)
	cobHealthy(t, pool, 3)
	if r := cobCheck(t, pool, OutboxCheckOptions{ExpectedEvents: 5}); len(r.Problems) == 0 {
		t.Fatal("3 events passed a check that expected 5")
	}
}

// The negative control: a consumer that does not deduplicate is expected to
// show repeated applications, so that run opts in to them.
func TestCheckOutbox_ConsumerRepeatsAreAllowedOnlyWhenAskedFor(t *testing.T) {
	pool := testPool(t)
	cobSetup(t, pool)
	ids := cobHealthy(t, pool, 2)
	exec(t, pool, `UPDATE consumer_effects SET runs = 3 WHERE event_id = $1::uuid`, ids[0])

	strict := cobCheck(t, pool, OutboxCheckOptions{})
	if strict.RepeatedConsumerEffects != 1 || len(strict.Problems) == 0 {
		t.Fatalf("strict result = %+v, want the repeat reported as a problem", strict)
	}
	lenient := cobCheck(t, pool, OutboxCheckOptions{AllowConsumerRepeats: true})
	if len(lenient.Problems) != 0 || lenient.RepeatedConsumerEffects != 1 {
		t.Fatalf("lenient result = %+v, want the repeat counted but not a problem", lenient)
	}
}

// A pass must not be vacuous: if the run was supposed to produce duplicate
// deliveries and produced none, the crash scenario never happened.
func TestCheckOutbox_ExpectedDuplicateDeliveriesMustHappen(t *testing.T) {
	pool := testPool(t)
	cobSetup(t, pool)
	ids := cobHealthy(t, pool, 2)

	if r := cobCheck(t, pool, OutboxCheckOptions{MinExtraDeliveries: 1}); len(r.Problems) == 0 {
		t.Fatal("no duplicate deliveries, but the check required at least 1")
	}
	cobDeliver(t, pool, ids[0], 1)
	if r := cobCheck(t, pool, OutboxCheckOptions{MinExtraDeliveries: 1}); len(r.Problems) != 0 {
		t.Fatalf("problems with one duplicate delivery: %v", r.Problems)
	}
}

func TestCheckOutbox_DeadEventsAreAcceptedOnlyWhenAllowed(t *testing.T) {
	pool := testPool(t)
	cobSetup(t, pool)
	cobHealthy(t, pool, 2)
	cobEvent(t, pool, "dead")

	if r := cobCheck(t, pool, OutboxCheckOptions{}); len(r.Problems) == 0 {
		t.Fatal("a dead event passed without AllowDead")
	}
	r := cobCheck(t, pool, OutboxCheckOptions{AllowDead: true})
	if len(r.Problems) != 0 || r.Dead != 1 {
		t.Fatalf("result = %+v, want the dead event counted but not a problem", r)
	}
}