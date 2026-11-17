package core

import (
	"context"
	"fmt"
)

// OutboxCheckOptions says what a run is expected to look like.
type OutboxCheckOptions struct {
	ExpectedEvents       int  // 0 = don't check the count
	AllowDead            bool // dead events are not a problem
	AllowConsumerRepeats bool // the negative control: a consumer that does not deduplicate
	// MinExtraDeliveries makes a pass non-vacuous: a run that was supposed to
	// cause duplicate deliveries (publishers crashing after sending) must
	// actually show at least this many, or the scenario never happened.
	MinExtraDeliveries int
}

// OutboxCheckResult holds the counts CheckOutbox found and, in Problems, every
// invariant that does not hold. An empty Problems means the run is clean.
type OutboxCheckResult struct {
	Total, Published, Dead, Unfinished int

	// PublishedStillLeased: published events that still carry a lease owner.
	PublishedStillLeased int

	// Deliveries counts every row in broker_log. ExtraDeliveries counts the
	// deliveries beyond the first for each known event.
	Deliveries, ExtraDeliveries int
	// PublishedWithoutDelivery: marked published but never sent: a lost event.
	PublishedWithoutDelivery int
	// UnknownDeliveries: broker rows for an event the outbox has no record of.
	UnknownDeliveries int

	ConsumerRows            int
	MissingConsumerEffects  int // published events the consumer never applied
	OrphanConsumerEffects   int // applied events that are not published
	RepeatedConsumerEffects int // events the consumer applied more than once

	Problems []string
}

// CheckOutbox judges an outbox run from the database alone. It reads the
// outbox plus two tables the load tester owns: broker_log, one row per
// delivery (a duplicate delivery is a second row), and consumer_effects, one
// row per event the consumer applied (runs goes up on a repeat).
//
// It proves the guarantee the guide states: every event is delivered at least
// once, and with a consumer that deduplicates on event_id, applied exactly
// once. It cannot prove anything about events that were never enqueued.
func CheckOutbox(ctx context.Context, q Querier, opts OutboxCheckOptions) (OutboxCheckResult, error) {
	var r OutboxCheckResult

	err := q.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE published_at IS NOT NULL),
		       count(*) FILTER (WHERE dead_at IS NOT NULL),
		       count(*) FILTER (WHERE published_at IS NULL AND dead_at IS NULL),
		       count(*) FILTER (WHERE published_at IS NOT NULL AND lease_owner IS NOT NULL)
		FROM outbox`).
		Scan(&r.Total, &r.Published, &r.Dead, &r.Unfinished, &r.PublishedStillLeased)
	if err != nil {
		return r, fmt.Errorf("check outbox: events: %w", err)
	}

	err = q.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM outbox o WHERE o.event_id = b.event_id))
		FROM broker_log b`).Scan(&r.Deliveries, &r.UnknownDeliveries)
	if err != nil {
		return r, fmt.Errorf("check outbox: deliveries: %w", err)
	}

	err = q.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE o.published_at IS NOT NULL AND d.n IS NULL),
		       COALESCE(sum(d.n - 1), 0)::int
		FROM outbox o
		LEFT JOIN (SELECT event_id, count(*) AS n FROM broker_log GROUP BY event_id) d
		       ON d.event_id = o.event_id`).
		Scan(&r.PublishedWithoutDelivery, &r.ExtraDeliveries)
	if err != nil {
		return r, fmt.Errorf("check outbox: delivery coverage: %w", err)
	}

	err = q.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE c.runs > 1),
		       count(*) FILTER (WHERE o.event_id IS NULL OR o.published_at IS NULL)
		FROM consumer_effects c
		LEFT JOIN outbox o ON o.event_id = c.event_id`).
		Scan(&r.ConsumerRows, &r.RepeatedConsumerEffects, &r.OrphanConsumerEffects)
	if err != nil {
		return r, fmt.Errorf("check outbox: consumer: %w", err)
	}

	err = q.QueryRow(ctx, `
		SELECT count(*) FROM outbox o
		WHERE o.published_at IS NOT NULL
		  AND NOT EXISTS (SELECT 1 FROM consumer_effects c WHERE c.event_id = o.event_id)`).
		Scan(&r.MissingConsumerEffects)
	if err != nil {
		return r, fmt.Errorf("check outbox: consumer coverage: %w", err)
	}

	problem := func(format string, args ...any) {
		r.Problems = append(r.Problems, fmt.Sprintf(format, args...))
	}
	if r.Total == 0 {
		problem("the outbox is empty, so this run proves nothing")
	}
	if opts.ExpectedEvents > 0 && r.Total != opts.ExpectedEvents {
		problem("found %d events, expected %d", r.Total, opts.ExpectedEvents)
	}
	if r.Unfinished > 0 {
		problem("%d events are neither published nor dead", r.Unfinished)
	}
	if r.Dead > 0 && !opts.AllowDead {
		problem("%d events are dead", r.Dead)
	}
	if r.PublishedWithoutDelivery > 0 {
		problem("%d events are marked published but were never delivered", r.PublishedWithoutDelivery)
	}
	if r.UnknownDeliveries > 0 {
		problem("%d deliveries are for events the outbox does not know", r.UnknownDeliveries)
	}
	if r.PublishedStillLeased > 0 {
		problem("%d published events still hold a lease", r.PublishedStillLeased)
	}
	if r.MissingConsumerEffects > 0 {
		problem("%d published events were never applied by the consumer", r.MissingConsumerEffects)
	}
	if r.OrphanConsumerEffects > 0 {
		problem("%d consumer effects belong to events that are not published", r.OrphanConsumerEffects)
	}
	if r.RepeatedConsumerEffects > 0 && !opts.AllowConsumerRepeats {
		problem("%d events were applied by the consumer more than once", r.RepeatedConsumerEffects)
	}
	if r.ExtraDeliveries < opts.MinExtraDeliveries {
		problem("only %d duplicate deliveries, expected at least %d: the crash scenario did not happen",
			r.ExtraDeliveries, opts.MinExtraDeliveries)
	}
	return r, nil
}