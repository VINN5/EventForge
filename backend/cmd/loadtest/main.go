package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"eventforge/src/core"
)

// crashEffectSQL is the same effect the "record" handler performs: insert, or
// count a repeat. Inside RunOnce a repeat can never happen; in the plain
// handler it happens on every retry.
const crashEffectSQL = `
	INSERT INTO effects (job_id, worker) VALUES ($1, $2)
	ON CONFLICT (job_id) DO UPDATE SET runs = effects.runs + 1`

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return errors.New("usage: loadtest seed|work|status|verify [flags]")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := core.NewPool(ctx)
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	defer pool.Close()

	switch os.Args[1] {
	case "seed":
		return seed(ctx, pool, os.Args[2:])
	case "work":
		return work(ctx, pool, os.Args[2:])
	case "status":
		return status(ctx, pool)
	case "verify":
		return verify(ctx, pool, os.Args[2:])
	case "outbox-seed":
		return outboxSeed(ctx, pool, os.Args[2:])
	case "outbox-work":
		return outboxWork(ctx, pool, os.Args[2:])
	case "outbox-consume":
		return outboxConsume(ctx, pool, os.Args[2:])
	case "outbox-status":
		return outboxStatus(ctx, pool)
	case "outbox-verify":
		return outboxVerify(ctx, pool, os.Args[2:])
	default:
		return fmt.Errorf("unknown command %q", os.Args[1])
	}
}

func seed(ctx context.Context, pool *pgxpool.Pool, args []string) error {
	fs := flag.NewFlagSet("seed", flag.ContinueOnError)
	n := fs.Int("n", 100000, "number of healthy jobs")
	poison := fs.Int("poison", 0, "extra jobs of type poison, whose handler crashes the worker process (max_attempts 3)")
	reset := fs.Bool("reset", false, "delete all existing jobs and effects first")
	crash := fs.Int("crash", 0, "extra jobs whose handler records its effect and then kills the process on its first -crash-attempts attempts (max_attempts 5)")
	crashType := fs.String("crash-type", "crash_after_effect", "handler for the crash jobs: crash_after_effect (idempotent, through RunOnce) or crash_plain (plain insert, the negative control)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *crashType != "crash_after_effect" && *crashType != "crash_plain" {
		return fmt.Errorf("unknown -crash-type %q", *crashType)
	}
	_, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS effects (
		job_id uuid PRIMARY KEY,
		worker text NOT NULL,
		runs   int  NOT NULL DEFAULT 1,
		at     timestamptz NOT NULL DEFAULT now())`)
	if err != nil {
		return err
	}

	if *reset {
		if _, err := pool.Exec(ctx,
			`TRUNCATE jobs, job_attempts, idempotency_keys, effects, job_effects RESTART IDENTITY CASCADE`); err != nil {
			return err
		}
	} else {
		var existing int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs`).Scan(&existing); err != nil {
			return err
		}
		if existing > 0 {
			return fmt.Errorf("jobs table has %d rows; use -reset to clear it first", existing)
		}
	}

	// One transaction, so every job gets the same run_at (now() is the
	// transaction's start time). Their random ids then mix the poison jobs
	// into the claim order instead of leaving them all at the end.
	start := time.Now()
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		INSERT INTO jobs (tenant_id, type, payload)
		SELECT 'load', 'record', jsonb_build_object('n', g)
		FROM generate_series(1, $1::int) AS g`, *n); err != nil {
		return err
	}
	if *poison > 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jobs (tenant_id, type, payload, max_attempts)
			SELECT 'load', 'poison', jsonb_build_object('poison', g), 3
			FROM generate_series(1, $1::int) AS g`, *poison); err != nil {
			return err
		}
	}
	if *crash > 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jobs (tenant_id, type, payload, max_attempts)
			SELECT 'load', $1::text, jsonb_build_object('crash', g), 5
			FROM generate_series(1, $2::int) AS g`, *crashType, *crash); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	fmt.Printf("seeded %d jobs (+%d poison, +%d %s) in %s\n", *n, *poison, *crash, *crashType, time.Since(start).Round(time.Millisecond))
	return nil
}

func work(ctx context.Context, pool *pgxpool.Pool, args []string) error {
	fs := flag.NewFlagSet("work", flag.ContinueOnError)
	id := fs.String("id", "", "worker id")
	concurrency := fs.Int("concurrency", 8, "jobs handled in parallel")
	untilDrained := fs.Bool("until-drained", false, "exit once no queued or running jobs remain")
	lease := fs.Duration("lease", 30*time.Second, "lease length")
	heartbeat := fs.Duration("heartbeat", 0, "heartbeat interval (0 = a third of the lease)")
	reapInterval := fs.Duration("reap-interval", 5*time.Second, "how often to look for expired leases")
	delay := fs.Duration("delay", 0, "pause after recording the effect and before the ack, to widen the crash window")
	crashAttempts := fs.Int("crash-attempts", 2, "crash jobs kill the process on their first this-many attempts")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		*id = fmt.Sprintf("worker-%d", os.Getpid())
	}
	workerID := *id

	// crashIfDue kills the process, as a segfault or an out-of-memory kill
	// would: os.Exit skips every deferred function, so nothing is reported.
	// job.Attempts is 1 on the first claim.
	crashIfDue := func(job core.Job) {
		if job.Attempts <= *crashAttempts {
			fmt.Fprintf(os.Stderr, "%s: job %s attempt %d: effect recorded, crashing before the ack\n",
				workerID, job.ID, job.Attempts)
			os.Exit(4)
		}
	}
	handlers := map[string]core.Handler{
		"record": func(ctx context.Context, job core.Job) error {
			_, err := pool.Exec(ctx, `
				INSERT INTO effects (job_id, worker) VALUES ($1, $2)
				ON CONFLICT (job_id) DO UPDATE SET runs = effects.runs + 1`,
				job.ID, workerID)
			if err != nil {
				return err
			}
			if *delay > 0 {
				select {
				case <-time.After(*delay):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		},
		// A poison payload: the process dies, as it would on a segfault or an
		// out-of-memory kill. os.Exit skips every deferred function, so nothing
		// gets a chance to report the failure.
		"poison": func(ctx context.Context, job core.Job) error {
			fmt.Fprintf(os.Stderr, "%s: poison job %s: crashing on purpose\n", workerID, job.ID)
			os.Exit(3)
			return nil
		},
		// Effect through RunOnce, then die before the ack. A retry finds the
		// marker and skips the effect.
		"crash_after_effect": func(ctx context.Context, job core.Job) error {
			_, err := core.RunOnce(ctx, pool, job.ID, "effect",
				func(ctx context.Context, tx pgx.Tx) error {
					_, err := tx.Exec(ctx, crashEffectSQL, job.ID, workerID)
					return err
				})
			if err != nil {
				return err
			}
			crashIfDue(job)
			return nil
		},
		// The negative control: the same crash with a plain insert. Every
		// retry repeats the effect.
		"crash_plain": func(ctx context.Context, job core.Job) error {
			if _, err := pool.Exec(ctx, crashEffectSQL, job.ID, workerID); err != nil {
				return err
			}
			crashIfDue(job)
			return nil
		},
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	w, err := core.NewWorker(pool, core.WorkerConfig{
		ID:                workerID,
		Concurrency:       *concurrency,
		PollInterval:      100 * time.Millisecond,
		Lease:             *lease,
		HeartbeatInterval: *heartbeat,
		ReapInterval:      *reapInterval,
	}, handlers, logger)
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if *untilDrained {
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			idle := 0
			for {
				select {
				case <-runCtx.Done():
					return
				case <-ticker.C:
				}
				var busy bool
				err := pool.QueryRow(runCtx, `
					SELECT EXISTS (SELECT 1 FROM jobs WHERE state IN ('queued','running'))`).Scan(&busy)
				if err != nil {
					continue
				}
				if busy {
					idle = 0
					continue
				}
				idle++
				if idle >= 2 {
					cancel()
					return
				}
			}
		}()
	}

	fmt.Printf("%s started (concurrency %d, lease %s)\n", workerID, *concurrency, *lease)
	err = w.Run(runCtx)
	fmt.Printf("%s stopped\n", workerID)
	return err
}

// status prints one line the kill script can read.
// status prints one line the kill and restart scripts can read. overdue counts
// running jobs whose lease has already expired: the reaper should take them
// back within one reap interval.
func status(ctx context.Context, pool *pgxpool.Pool) error {
	var queued, running, succeeded, dead, overdue int
	err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE state = 'queued'),
		       count(*) FILTER (WHERE state = 'running'),
		       count(*) FILTER (WHERE state = 'succeeded'),
		       count(*) FILTER (WHERE state = 'dead'),
		       count(*) FILTER (WHERE state = 'running' AND lease_expires_at < now())
		FROM jobs WHERE tenant_id = 'load'`).Scan(&queued, &running, &succeeded, &dead, &overdue)
	if err != nil {
		return err
	}
	fmt.Printf("queued=%d running=%d succeeded=%d dead=%d overdue=%d\n", queued, running, succeeded, dead, overdue)
	return nil
}

func verify(ctx context.Context, pool *pgxpool.Pool, args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	want := fs.Int("n", 0, "expected number of jobs (0 = don't check)")
	allowReruns := fs.Bool("allow-reruns", false, "allow effects that ran twice when an expired lease explains them (use after kills)")
	allowDead := fs.Bool("allow-dead", false, "do not treat dead jobs as a problem")
	expectedDead := fs.Int("expected-dead", 0, "require exactly this many dead jobs (0 = don't check)")
	deadByCrash := fs.Bool("dead-by-crash-only", false, "every dead job must have died from expired leases on all its attempts")
	if err := fs.Parse(args); err != nil {
		return err
	}

	r, err := core.CheckRun(ctx, pool, core.CheckOptions{
		TenantID:        "load",
		ExpectedJobs:    *want,
		AllowReruns:     *allowReruns,
		AllowDead:       *allowDead,
		ExpectedDead:    *expectedDead,
		DeadByCrashOnly: *deadByCrash,
	})
	if err != nil {
		return err
	}

	var windowSecs float64
	err = pool.QueryRow(ctx, `
		SELECT COALESCE(EXTRACT(EPOCH FROM (max(finished_at) - min(finished_at))), 0)::float8
		FROM jobs WHERE tenant_id = 'load' AND state = 'succeeded'`).Scan(&windowSecs)
	if err != nil {
		return err
	}

	fmt.Printf("jobs:        %d total, %d succeeded, %d dead, %d unfinished (%d stuck running)\n",
		r.Total, r.Succeeded, r.Dead, r.Unfinished, r.Running)
	fmt.Printf("effects:     %d rows; %d jobs ran their effect more than once (%d repeats in total)\n",
		r.Effects, r.RerunJobs, r.ExtraRuns)
	fmt.Printf("consistency: %d succeeded jobs without an effect, %d effects without a succeeded job\n",
		r.MissingEffects, r.OrphanEffects)
	fmt.Printf("attempts:    %d ended by an expired lease, %d never closed, %d jobs with two succeeded attempts, %d succeeded jobs without a succeeded attempt\n",
		r.LeaseExpiredAttempts, r.OpenAttempts, r.MultiSucceeded, r.NoSucceededAttempt)
	if r.Dead > 0 {
		fmt.Printf("dead jobs:   %d dead, %d of them did not die purely from expired leases\n",
			r.Dead, r.DeadNotFromCrashes)
	}
	if windowSecs > 0 {
		fmt.Printf("throughput:  %.0f jobs/s over %.1fs (first to last completion; not a benchmark)\n",
			float64(r.Succeeded)/windowSecs, windowSecs)
	}

	if len(r.Problems) > 0 {
		for _, p := range r.Problems {
			fmt.Println("FAIL:", p)
		}
		return errors.New("verification failed")
	}
	switch {
	case *expectedDead > 0:
		fmt.Printf("PASS: %d jobs succeeded with one effect and one succeeded attempt each; %d poison jobs died after using every attempt, each ended by an expired lease; %d repeated effects, none unexplained\n",
			r.Succeeded, r.Dead, r.ExtraRuns)
	case *allowReruns:
		fmt.Printf("PASS: no job lost or stuck; every succeeded job has one effect row and one succeeded attempt; %d repeated effects, none unexplained\n", r.ExtraRuns)
	default:
		fmt.Println("PASS: every job succeeded once, with exactly one effect")
	}
	return nil
}
