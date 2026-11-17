package core

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connection loss, in process. pg_terminate_backend kills every other session
// on the test database, as a database restart does to a worker's pool. The
// real container restart is scripts\db-restart.ps1.
//
// The killing stops after a while; then everything must finish by itself.

// clAdminPool is a separate one-connection pool, so the killer and the
// assertions never share a session with the worker being attacked.
func clAdminPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}
	cfg.MaxConns = 1
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

// clStartKiller terminates every other session on the database every `every`.
// The returned function stops it and reports how many sessions it killed.
func clStartKiller(t *testing.T, admin *pgxpool.Pool, every time.Duration) (stop func() int) {
	t.Helper()
	done := make(chan struct{})
	finished := make(chan int, 1)
	go func() {
		killed := 0
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				finished <- killed
				return
			case <-ticker.C:
			}
			var n int
			err := admin.QueryRow(context.Background(), `
				SELECT count(*) FROM (
					SELECT pg_terminate_backend(pid) FROM pg_stat_activity
					WHERE datname = current_database() AND pid <> pg_backend_pid()
				) s`).Scan(&n)
			if err != nil {
				t.Errorf("kill sessions: %v", err)
				finished <- killed
				return
			}
			killed += n
		}
	}()
	return func() int {
		close(done)
		return <-finished
	}
}

func clQuietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestWorker_SurvivesItsConnectionsBeingKilled(t *testing.T) {
	pool := testPool(t)
	admin := clAdminPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const jobs = 60
	for i := 0; i < jobs; i++ {
		// Plenty of attempts: a killed connection can cost an attempt, and
		// this test is about recovery, not about running out.
		submitNoop(t, admin, SubmitParams{Type: "cl_work", MaxAttempts: 50})
	}

	var mu sync.Mutex
	runs := map[string]int{}
	handler := func(ctx context.Context, job Job) error {
		mu.Lock()
		runs[job.ID]++
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		return nil
	}
	w, err := NewWorker(pool, WorkerConfig{
		ID:           "cl-worker",
		Concurrency:  4,
		PollInterval: 20 * time.Millisecond,
		Lease:        2 * time.Second,
		ReapInterval: 200 * time.Millisecond,
	}, map[string]Handler{"cl_work": handler}, clQuietLogger())
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	stopKiller := clStartKiller(t, admin, 100*time.Millisecond)
	time.Sleep(1500 * time.Millisecond)
	killed := stopKiller()

	// The killing has stopped: everything must now finish on its own.
	obWaitFor(t, "all jobs to succeed", 30*time.Second, func() bool {
		return countByState(t, admin, "succeeded") == jobs
	})

	select {
	case err := <-runDone:
		t.Fatalf("Run returned while jobs were still being processed: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run returned %v after cancel, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}

	if killed == 0 {
		t.Fatal("no sessions were killed: the test proved nothing")
	}
	for state, want := range map[string]int{"succeeded": jobs, "dead": 0, "running": 0, "queued": 0} {
		if got := countByState(t, admin, state); got != want {
			t.Errorf("%d jobs are %s, want %d", got, state, want)
		}
	}
	var succeededAttempts, openAttempts int
	err = admin.QueryRow(context.Background(), `
		SELECT count(*) FILTER (WHERE outcome = 'succeeded'),
		       count(*) FILTER (WHERE ended_at IS NULL)
		FROM job_attempts`).Scan(&succeededAttempts, &openAttempts)
	if err != nil {
		t.Fatal(err)
	}
	if succeededAttempts != jobs || openAttempts != 0 {
		t.Errorf("succeeded attempts = %d (want %d), open attempts = %d (want 0)",
			succeededAttempts, jobs, openAttempts)
	}

	mu.Lock()
	defer mu.Unlock()
	total := 0
	for _, n := range runs {
		total += n
	}
	if len(runs) != jobs {
		t.Errorf("%d distinct jobs ran, want %d", len(runs), jobs)
	}
	t.Logf("%d sessions killed; %d handler runs for %d jobs (%d repeats)", killed, total, jobs, total-jobs)
}

func TestPublisher_SurvivesItsConnectionsBeingKilled(t *testing.T) {
	pool := testPool(t)
	admin := clAdminPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const events = 40
	for i := 0; i < events; i++ {
		if _, err := Enqueue(ctx, admin, OutboxEvent{Topic: "t", Payload: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	exec(t, admin, `UPDATE outbox SET max_attempts = 50`)

	var mu sync.Mutex
	deliveries := map[string]int{}
	sink := func(ctx context.Context, item OutboxItem) error {
		mu.Lock()
		deliveries[item.EventID]++
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		return nil
	}
	p, err := NewPublisher(pool, sink, PublisherConfig{
		ID:             "cl-pub",
		BatchSize:      4,
		Lease:          2 * time.Second,
		DeliverTimeout: 500 * time.Millisecond,
		PollInterval:   20 * time.Millisecond,
		ReapInterval:   200 * time.Millisecond,
	}, clQuietLogger())
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- p.Run(ctx) }()

	stopKiller := clStartKiller(t, admin, 100*time.Millisecond)
	time.Sleep(1500 * time.Millisecond)
	killed := stopKiller()

	obWaitFor(t, "all events to be published", 30*time.Second, func() bool {
		return obCountWhere(t, admin, `published_at IS NOT NULL`) == events
	})

	select {
	case err := <-runDone:
		t.Fatalf("Run returned while events were still being published: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run returned %v after cancel, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}

	if killed == 0 {
		t.Fatal("no sessions were killed: the test proved nothing")
	}
	if dead := obCountWhere(t, admin, `dead_at IS NOT NULL`); dead != 0 {
		t.Errorf("%d events are dead, want 0", dead)
	}
	if leased := obCountWhere(t, admin, `published_at IS NOT NULL AND lease_owner IS NOT NULL`); leased != 0 {
		t.Errorf("%d published events still hold a lease", leased)
	}

	mu.Lock()
	defer mu.Unlock()
	total := 0
	for _, n := range deliveries {
		total += n
	}
	if len(deliveries) != events {
		t.Errorf("%d distinct events were delivered, want %d", len(deliveries), events)
	}
	t.Logf("%d sessions killed; %d deliveries for %d events (%d duplicates)", killed, total, events, total-events)
}