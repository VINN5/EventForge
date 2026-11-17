package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"eventforge/src/core"
)

// consumerApplySQL applies one delivered event. On a repeat it counts the
// repeat in runs, so a consumer that does not deduplicate shows up in the
// verifier.
const consumerApplySQL = `
	INSERT INTO consumer_effects (event_id) VALUES ($1::uuid)
	ON CONFLICT (event_id) DO UPDATE SET runs = consumer_effects.runs + 1`

// ensureOutboxTables creates the two tables the outbox run owns: broker_log
// (one row per delivery) and consumer_effects (one row per applied event).
func ensureOutboxTables(ctx context.Context, pool *pgxpool.Pool) error {
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS broker_log (
			id bigserial PRIMARY KEY, event_id uuid NOT NULL, publisher text NOT NULL,
			at timestamptz NOT NULL DEFAULT now())`,
		`CREATE TABLE IF NOT EXISTS consumer_effects (
			event_id uuid PRIMARY KEY, runs int NOT NULL DEFAULT 1,
			at timestamptz NOT NULL DEFAULT now())`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func outboxSeed(ctx context.Context, pool *pgxpool.Pool, args []string) error {
	fs := flag.NewFlagSet("outbox-seed", flag.ContinueOnError)
	n := fs.Int("n", 1000, "events to enqueue")
	reset := fs.Bool("reset", false, "delete all existing events, deliveries and consumer effects first")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := ensureOutboxTables(ctx, pool); err != nil {
		return err
	}

	if *reset {
		if _, err := pool.Exec(ctx,
			`TRUNCATE outbox, broker_log, consumer_effects, job_effects RESTART IDENTITY CASCADE`); err != nil {
			return err
		}
	} else {
		var existing int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&existing); err != nil {
			return err
		}
		if existing > 0 {
			return fmt.Errorf("outbox has %d rows; use -reset to clear it first", existing)
		}
	}

	// Through the real writer, in one transaction.
	start := time.Now()
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for i := 1; i <= *n; i++ {
		_, err := core.Enqueue(ctx, tx, core.OutboxEvent{
			Topic:   "load.event",
			Payload: []byte(fmt.Sprintf(`{"n":%d}`, i)),
		})
		if err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	fmt.Printf("enqueued %d events in %s\n", *n, time.Since(start).Round(time.Millisecond))
	return nil
}

// outboxWork runs one publisher. Its sink "delivers" by appending to
// broker_log, then kills the process on an event's first -crash-attempts
// attempts, before the publisher can mark the event published.
func outboxWork(ctx context.Context, pool *pgxpool.Pool, args []string) error {
	fs := flag.NewFlagSet("outbox-work", flag.ContinueOnError)
	id := fs.String("id", "", "publisher id")
	batch := fs.Int("batch", 1, "events delivered in parallel (1 keeps a crash from stranding other events)")
	lease := fs.Duration("lease", 3*time.Second, "lease length")
	deliverTimeout := fs.Duration("deliver-timeout", time.Second, "per-delivery timeout (must be shorter than the lease)")
	reapInterval := fs.Duration("reap-interval", time.Second, "how often to dead-letter exhausted events")
	crashAttempts := fs.Int("crash-attempts", 1, "kill the process after delivering, on an event's first this-many attempts")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		*id = fmt.Sprintf("publisher-%d", os.Getpid())
	}
	pubID := *id

	sink := func(ctx context.Context, item core.OutboxItem) error {
		_, err := pool.Exec(ctx,
			`INSERT INTO broker_log (event_id, publisher) VALUES ($1::uuid, $2)`, item.EventID, pubID)
		if err != nil {
			return err
		}
		// The delivery is on record, so the broker has the event. Now die
		// before the publisher can mark it published. os.Exit skips every
		// deferred function, as a crash would.
		if item.Attempts <= *crashAttempts {
			fmt.Fprintf(os.Stderr, "%s: event %s attempt %d: delivered, crashing before the mark\n",
				pubID, item.EventID, item.Attempts)
			os.Exit(4)
		}
		return nil
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	p, err := core.NewPublisher(pool, sink, core.PublisherConfig{
		ID:             pubID,
		BatchSize:      *batch,
		Lease:          *lease,
		DeliverTimeout: *deliverTimeout,
		PollInterval:   50 * time.Millisecond,
		ReapInterval:   *reapInterval,
	}, logger)
	if err != nil {
		return err
	}

	fmt.Printf("%s started (batch %d, lease %s)\n", pubID, *batch, *lease)
	err = p.Run(ctx)
	fmt.Printf("%s stopped\n", pubID)
	return err
}

// outboxConsume plays the consumer: it applies every row of broker_log, in
// order, once the publishers have stopped. "idempotent" deduplicates on the
// event id through RunOnce; "plain" applies every delivery.
func outboxConsume(ctx context.Context, pool *pgxpool.Pool, args []string) error {
	fs := flag.NewFlagSet("outbox-consume", flag.ContinueOnError)
	mode := fs.String("mode", "idempotent", "idempotent (each event applied once, through RunOnce) or plain (every delivery applied)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *mode != "idempotent" && *mode != "plain" {
		return fmt.Errorf("unknown -mode %q", *mode)
	}

	rows, err := pool.Query(ctx, `SELECT event_id::text FROM broker_log ORDER BY id`)
	if err != nil {
		return err
	}
	var deliveries []string
	for rows.Next() {
		var eventID string
		if err := rows.Scan(&eventID); err != nil {
			rows.Close()
			return err
		}
		deliveries = append(deliveries, eventID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, eventID := range deliveries {
		if *mode == "plain" {
			if _, err := pool.Exec(ctx, consumerApplySQL, eventID); err != nil {
				return err
			}
			continue
		}
		_, err := core.RunOnce(ctx, pool, eventID, "consume",
			func(ctx context.Context, tx pgx.Tx) error {
				_, err := tx.Exec(ctx, consumerApplySQL, eventID)
				return err
			})
		if err != nil {
			return err
		}
	}
	fmt.Printf("consumed %d deliveries (%s)\n", len(deliveries), *mode)
	return nil
}

// outboxStatus prints one line the script can read.
func outboxStatus(ctx context.Context, pool *pgxpool.Pool) error {
	var unfinished, published, dead int
	err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE published_at IS NULL AND dead_at IS NULL),
		       count(*) FILTER (WHERE published_at IS NOT NULL),
		       count(*) FILTER (WHERE dead_at IS NOT NULL)
		FROM outbox`).Scan(&unfinished, &published, &dead)
	if err != nil {
		return err
	}
	fmt.Printf("unfinished=%d published=%d dead=%d\n", unfinished, published, dead)
	return nil
}

func outboxVerify(ctx context.Context, pool *pgxpool.Pool, args []string) error {
	fs := flag.NewFlagSet("outbox-verify", flag.ContinueOnError)
	want := fs.Int("n", 0, "expected number of events (0 = don't check)")
	minExtra := fs.Int("min-extra", 0, "require at least this many deliveries beyond the first")
	allowRepeats := fs.Bool("allow-consumer-repeats", false, "accept events the consumer applied more than once (the negative control)")
	allowDead := fs.Bool("allow-dead", false, "do not treat dead events as a problem")
	if err := fs.Parse(args); err != nil {
		return err
	}

	r, err := core.CheckOutbox(ctx, pool, core.OutboxCheckOptions{
		ExpectedEvents:       *want,
		AllowDead:            *allowDead,
		AllowConsumerRepeats: *allowRepeats,
		MinExtraDeliveries:   *minExtra,
	})
	if err != nil {
		return err
	}

	fmt.Printf("events:      %d total, %d published, %d dead, %d unfinished (%d published events still hold a lease)\n",
		r.Total, r.Published, r.Dead, r.Unfinished, r.PublishedStillLeased)
	fmt.Printf("deliveries:  %d total, %d beyond the first; %d published events never delivered; %d for unknown events\n",
		r.Deliveries, r.ExtraDeliveries, r.PublishedWithoutDelivery, r.UnknownDeliveries)
	fmt.Printf("consumer:    %d rows; %d events applied more than once; %d published events never applied; %d applied but not published\n",
		r.ConsumerRows, r.RepeatedConsumerEffects, r.MissingConsumerEffects, r.OrphanConsumerEffects)

	if len(r.Problems) > 0 {
		for _, p := range r.Problems {
			fmt.Println("FAIL:", p)
		}
		return errors.New("verification failed")
	}
	fmt.Printf("PASS: every event was delivered at least once and published once; %d duplicate deliveries; %d events applied more than once by the consumer\n",
		r.ExtraDeliveries, r.RepeatedConsumerEffects)
	return nil
}
