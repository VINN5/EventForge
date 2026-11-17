package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"sort"
	"sync"
	"time"
)

// Handler does the work for one job. It must be idempotent, because a job
// can run more than once. Use job.ID as the idempotency key.
//
// Return nil for success. Wrap an error with Permanent to dead-letter the job
// at once, or with RetryableAfter to honour a downstream's Retry-After. Any
// other error is retried with backoff, but gives up sooner than a marked one
// (see WorkerConfig.UnclassifiedMaxAttempts).
//
// The context is cancelled if the worker loses the job's lease (then
// context.Cause is ErrLeaseLost) or if shutdown outlasts the grace period.
// Handlers should watch it and stop.
type Handler func(ctx context.Context, job Job) error

type WorkerConfig struct {
	ID                      string
	Queue                   string         // default "default"
	Concurrency             int            // jobs handled in parallel, default 4
	Lease                   time.Duration  // default 30s
	HeartbeatInterval       time.Duration  // how often leases are extended, default Lease/3; must be below Lease
	PollInterval            time.Duration  // wait when the queue is empty, default 500ms
	ReapInterval            time.Duration  // how often to look for expired leases, default 5s
	ShutdownGrace           time.Duration  // time allowed for in-flight jobs, default 30s
	BackoffBase             time.Duration  // first retry ceiling, default 2s
	BackoffCap              time.Duration  // longest retry ceiling, default 15m
	MaxRetryAfter           time.Duration  // longest wait a downstream's Retry-After can impose, default 1h
	UnclassifiedMaxAttempts int            // attempts allowed for errors nobody marked retryable, default 3
	Rand                    func() float64 // random source in [0,1) for jitter; default math/rand
	ConnRetry               ConnRetry      // bounded retry of Ack and Fail after a connection error; zero value = defaults
}

// inflight tracks the claims whose handlers are running right now.
type inflight struct {
	mu sync.Mutex
	m  map[LeaseRef]context.CancelCauseFunc
}

func (f *inflight) add(r LeaseRef, cancel context.CancelCauseFunc) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[r] = cancel
}

func (f *inflight) remove(r LeaseRef) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, r)
}

func (f *inflight) refs() []LeaseRef {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]LeaseRef, 0, len(f.m))
	for r := range f.m {
		out = append(out, r)
	}
	return out
}
// cancel cancels the handler registered for r. It reports whether a handler was
// still registered: false means that one had already finished.
func (f *inflight) cancel(r LeaseRef, cause error) bool {
	f.mu.Lock()
	cancel := f.m[r]
	f.mu.Unlock()
	if cancel == nil {
		return false
	}
	cancel(cause)
	return true
}

type Worker struct {
	cfg      WorkerConfig
	db       Querier
	handlers map[string]Handler
	types    []string // job types this worker has handlers for
	flights  *inflight
	log      *slog.Logger
}

func NewWorker(db Querier, cfg WorkerConfig, handlers map[string]Handler, log *slog.Logger) (*Worker, error) {
	if cfg.ID == "" {
		return nil, errors.New("worker: ID is required")
	}
	if len(handlers) == 0 {
		return nil, errors.New("worker: at least one handler is required")
	}
	if cfg.Queue == "" {
		cfg.Queue = "default"
	}
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 4
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = cfg.Lease / 3
	}
	if cfg.HeartbeatInterval >= cfg.Lease {
		return nil, fmt.Errorf("worker: heartbeat interval %v must be shorter than the lease %v",
			cfg.HeartbeatInterval, cfg.Lease)
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.ReapInterval <= 0 {
		cfg.ReapInterval = 5 * time.Second
	}
	if cfg.ShutdownGrace <= 0 {
		cfg.ShutdownGrace = 30 * time.Second
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = 2 * time.Second
	}
	if cfg.BackoffCap <= 0 {
		cfg.BackoffCap = 15 * time.Minute
	}
	if cfg.MaxRetryAfter <= 0 {
		cfg.MaxRetryAfter = time.Hour
	}
	if cfg.UnclassifiedMaxAttempts <= 0 {
		cfg.UnclassifiedMaxAttempts = 3
	}
	if cfg.Rand == nil {
		cfg.Rand = rand.Float64
	}
	cfg.ConnRetry = cfg.ConnRetry.withDefaults()
	if log == nil {
		log = slog.Default()
	}

	types := make([]string, 0, len(handlers))
	for t := range handlers {
		types = append(types, t)
	}
	sort.Strings(types)

	return &Worker{
		cfg:      cfg,
		db:       db,
		handlers: handlers,
		types:    types,
		flights:  &inflight{m: map[LeaseRef]context.CancelCauseFunc{}},
		log:      log.With("worker", cfg.ID),
	}, nil
}

// Run claims and processes jobs until ctx is cancelled, then waits for
// in-flight jobs (up to ShutdownGrace) before returning. It also runs the
// reaper, which takes back jobs whose workers died, and the heartbeat, which
// keeps the leases of running jobs alive.
func (w *Worker) Run(ctx context.Context) error {
	// Handlers get their own context so that shutdown can let them finish.
	jobCtx, cancelJobs := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelJobs()

	// The reaper stops when ctx is cancelled. The main loop below only exits
	// once ctx is cancelled, so by the time Run returns the reaper is told to
	// stop, and this deferred wait makes sure it really has.
	reaperDone := make(chan struct{})
	go func() {
		defer close(reaperDone)
		RunReaper(ctx, w.db, w.cfg.ReapInterval, 100, w.log)
	}()
	defer func() { <-reaperDone }()

	// The heartbeat must NOT stop when shutdown starts: a handler using its
	// grace period still needs its lease extended. It stops only when Run
	// returns, which is after the handlers have finished or been cancelled.
	hbCtx, stopHeartbeat := context.WithCancel(context.WithoutCancel(ctx))
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		w.heartbeatLoop(hbCtx)
	}()
	defer func() {
		stopHeartbeat()
		<-hbDone
	}()

	slots := make(chan struct{}, w.cfg.Concurrency)
	var wg sync.WaitGroup
	wake := make(chan struct{}, 1)

	for ctx.Err() == nil {
		free := w.cfg.Concurrency - len(slots)
		if free == 0 {
			sleepOrWake(ctx, w.cfg.PollInterval, wake)
			continue
		}

		// A claim is not cancelled by shutdown, so we never abandon jobs the
		// database has already marked as ours. The timeout keeps it bounded.
		claimCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		jobs, err := ClaimTypes(claimCtx, w.db, w.cfg.Queue, w.cfg.ID, w.types, free, w.cfg.Lease)
		cancel()
		if err != nil {
			w.log.Error("claim failed", "error", err)
			sleep(ctx, w.cfg.PollInterval)
			continue
		}
		if len(jobs) == 0 {
			sleep(ctx, w.cfg.PollInterval)
			continue
		}

		for _, job := range jobs {
			slots <- struct{}{}
			wg.Add(1)
			go func(job Job) {
				defer wg.Done()
				defer func() {
					<-slots
					select {
					case wake <- struct{}{}:
					default:
					}
				}()
				w.runOne(jobCtx, job)
			}(job)
		}
	}

	w.log.Info("shutting down; waiting for in-flight jobs")
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(w.cfg.ShutdownGrace):
		w.log.Warn("grace period expired; cancelling running jobs")
		cancelJobs()
	}

	select {
	case <-done:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("worker: handlers did not stop after cancellation")
	}
}

// heartbeatLoop extends the leases of every running job, once per interval,
// in a single statement. A job whose lease turns out to be lost has its
// handler cancelled. A failed call is logged and tried again on the next tick:
// the lease still has most of its time left.
func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()

	callTimeout := max(w.cfg.Lease/2, time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		refs := w.flights.refs()
		if len(refs) == 0 {
			continue
		}

		callCtx, cancel := context.WithTimeout(ctx, callTimeout)
		lost, err := Extend(callCtx, w.db, w.cfg.ID, refs, w.cfg.Lease)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				w.log.Error("heartbeat failed; will retry", "error", err)
			}
			continue
		}
		w.handleLostLeases(lost)
	}
}

// handleLostLeases cancels the handlers of jobs whose lease Extend reported as
// lost. A job can finish between the heartbeat taking its list and Extend
// answering. It is then reported as lost although no handler is left to cancel,
// and that is not worth a warning.
func (w *Worker) handleLostLeases(lost []LeaseRef) {
	for _, r := range lost {
		if !w.flights.cancel(r, ErrLeaseLost) {
			continue
		}
		w.log.Warn("lease lost; cancelling handler", "job_id", r.ID, "fence", r.Fence)
	}
}

func (w *Worker) runOne(ctx context.Context, job Job) {
	log := w.log.With("job_id", job.ID, "type", job.Type, "attempt", job.Attempts)

	h, ok := w.handlers[job.Type]
	if !ok {
		// ClaimTypes only returns types we handle, so this should not happen.
		// Retrying cannot help, so dead-letter it with a clear reason.
		w.reportFailure(log, job, FailParams{
			Class:   ClassPermanent,
			Message: "no handler registered for job type " + job.Type,
		})
		return
	}

	// The handler gets its own cancellable context so the heartbeat can stop
	// this one job without touching the others.
	hctx, cancel := context.WithCancelCause(ctx)
	ref := LeaseRef{ID: job.ID, Fence: job.Fence}
	w.flights.add(ref, cancel)
	defer func() {
		w.flights.remove(ref)
		cancel(nil)
	}()

	err := safeRun(hctx, h, job)

	// The lease is gone. Ack and Fail would both be rejected by the fence, so
	// we do not even try: the new owner decides this job's fate.
	if errors.Is(context.Cause(hctx), ErrLeaseLost) {
		log.Warn("lease lost while running; abandoning this attempt")
		return
	}

	if err == nil {
		w.ack(log, job)
		return
	}

	// The grace period ran out and shutdown cancelled the handler. That is not
	// the job's fault, so it is not charged as a failure: it stays running and
	// the reaper gives it to another worker when the lease expires.
	if ctx.Err() != nil {
		log.Warn("handler stopped by shutdown; leaving job for lease recovery", "error", err)
		return
	}

	w.reportFailure(log, job, w.failureFor(job, err))
}

// failureFor decides how a failed attempt is recorded: its class, its message
// and how long to wait before the retry.
func (w *Worker) failureFor(job Job, err error) FailParams {
	class, retryAfter := Classify(err)
	msg := err.Error()

	// An error nobody marked is only a guess that retrying helps, so it gets
	// fewer attempts than a marked one. When the job's own max_attempts is
	// reached first, the job dies as an ordinary exhausted retry instead.
	//
	// A lost connection is not a guess about the job: the database or network
	// was unavailable. It does not use up the early cap; only max_attempts
	// stops it.
	if class == ClassRetryable && !Classified(err) && !isTransient(err) &&
		job.Attempts >= w.cfg.UnclassifiedMaxAttempts && job.Attempts < job.MaxAttempts {
		class = ClassPermanent
		msg = fmt.Sprintf("unclassified error on attempt %d (limit %d for unclassified errors): %s",
			job.Attempts, w.cfg.UnclassifiedMaxAttempts, msg)
	}

	// A downstream's Retry-After is a minimum wait, but never longer than the
	// ceiling: "retry in two days" must not park a job for two days.
	retryAfter = min(retryAfter, w.cfg.MaxRetryAfter)
	delay := max(Backoff(job.Attempts, w.cfg.BackoffBase, w.cfg.BackoffCap, w.cfg.Rand), retryAfter)

	return FailParams{Class: class, Message: msg, Delay: delay}
}

func (w *Worker) ack(log *slog.Logger, job Job) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Ack is fenced, so sending it again after a lost reply cannot apply it twice.
	// If the first send did commit, the retry is rejected as ErrLeaseLost, which is
	// why that case is reported as "outcome unknown" below.
	attempts, err := retryTransient(ctx, w.cfg.ConnRetry, w.cfg.Rand, func(ctx context.Context) error {
		return Ack(ctx, w.db, job.ID, w.cfg.ID, job.Fence)
	})
	switch {
	case errors.Is(err, ErrLeaseLost) && attempts > 1:
		log.Warn("lease lost after a connection error; outcome unknown (our own ack may have committed)",
			"attempts", attempts)
	case errors.Is(err, ErrLeaseLost):
		log.Warn("lease lost before ack; another worker owns this job now")
	case err != nil:
		log.Error("ack failed; job stays running until its lease expires", "error", err, "attempts", attempts)
	}
}

func (w *Worker) reportFailure(log *slog.Logger, job Job, p FailParams) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var outcome FailOutcome
	attempts, err := retryTransient(ctx, w.cfg.ConnRetry, w.cfg.Rand, func(ctx context.Context) error {
		var callErr error
		outcome, callErr = Fail(ctx, w.db, job.ID, w.cfg.ID, job.Fence, p)
		return callErr
	})
	switch {
	case errors.Is(err, ErrLeaseLost) && attempts > 1:
		log.Warn("lease lost after a connection error; outcome unknown (our own failure report may have committed)",
			"attempts", attempts)
	case errors.Is(err, ErrLeaseLost):
		log.Warn("lease lost before failure could be recorded; another worker owns this job now")
	case err != nil:
		log.Error("recording failure failed; job stays running until its lease expires",
			"error", err, "attempts", attempts)
	default:
		log.Warn("job failed", "class", p.Class.String(), "outcome", outcome.String(),
			"retry_in", p.Delay, "error", p.Message)
	}
}

// safeRun turns a handler panic into an error so one bad job cannot crash the
// worker. The stack trace is kept in the message (it is cut to size when stored).
func safeRun(ctx context.Context, h Handler, job Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v\n%s", r, debug.Stack())
		}
	}()
	return h(ctx, job)
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func sleepOrWake(ctx context.Context, d time.Duration, wake <-chan struct{}) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-wake:
	case <-t.C:
	}
}