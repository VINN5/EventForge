package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// Sink delivers one outbox event to the outside world (a broker, a webhook).
// Return nil only when the event was really delivered. Mark failures with
// Retryable, RetryableAfter or Permanent, as handlers do. The context is
// cancelled when DeliverTimeout passes or the publisher shuts down; a Sink
// that ignores it can overrun its lease and cause a duplicate delivery.
type Sink func(ctx context.Context, item OutboxItem) error

// PublisherConfig tunes a Publisher. Zero values take the defaults noted.
type PublisherConfig struct {
	ID             string         // required; unique per running publisher
	BatchSize      int            // events claimed and delivered in parallel per Step; default 10
	Lease          time.Duration  // default 30s
	DeliverTimeout time.Duration  // per delivery; default 10s; must be shorter than Lease
	PollInterval   time.Duration  // idle wait in Run; default 500ms
	ReapInterval   time.Duration  // how often Run dead-letters exhausted events; default 5s
	BackoffBase    time.Duration  // default 1s
	BackoffCap     time.Duration  // default 5m
	MaxRetryAfter  time.Duration  // longest wait a Retry-After can impose; default 1h
	Rand           func() float64 // jitter source in [0, 1); default math/rand/v2
	ConnRetry      ConnRetry      // bounded retry of mark and failure reports after a connection error; zero value = defaults
}

// Publisher claims outbox events and delivers them through a Sink.
type Publisher struct {
	db   Querier
	sink Sink
	cfg  PublisherConfig
	log  *slog.Logger
}

func NewPublisher(db Querier, sink Sink, cfg PublisherConfig, log *slog.Logger) (*Publisher, error) {
	if cfg.ID == "" {
		return nil, errors.New("publisher: id is required")
	}
	if sink == nil {
		return nil, errors.New("publisher: sink is required")
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 10
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.DeliverTimeout <= 0 {
		cfg.DeliverTimeout = 10 * time.Second
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.ReapInterval <= 0 {
		cfg.ReapInterval = 5 * time.Second
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = time.Second
	}
	if cfg.BackoffCap <= 0 {
		cfg.BackoffCap = 5 * time.Minute
	}
	if cfg.MaxRetryAfter <= 0 {
		cfg.MaxRetryAfter = time.Hour
	}
	if cfg.Rand == nil {
		cfg.Rand = rand.Float64
	}
	cfg.ConnRetry = cfg.ConnRetry.withDefaults()
	// A live delivery must never outlast its own lease: that would hand the
	// event to a second publisher while the first is still sending it.
	if cfg.DeliverTimeout >= cfg.Lease {
		return nil, fmt.Errorf("publisher: delivery timeout (%s) must be shorter than the lease (%s)",
			cfg.DeliverTimeout, cfg.Lease)
	}
	if log == nil {
		log = slog.Default()
	}
	return &Publisher{db: db, sink: sink, cfg: cfg, log: log.With("publisher", cfg.ID)}, nil
}

// Step claims one batch, delivers its events in parallel and records each
// result. It returns how many events it settled: published, or a failure
// recorded. An event whose mark or failure was rejected by the fence (someone
// else took it over), or that shutdown interrupted, is not counted.
//
// The error is for infrastructure problems only (the claim failed). Shutdown
// is not an error.
func (p *Publisher) Step(ctx context.Context) (int, error) {
	items, err := ClaimOutbox(ctx, p.db, p.cfg.ID, p.cfg.BatchSize, p.cfg.Lease)
	if err != nil {
		if ctx.Err() != nil {
			return 0, nil
		}
		return 0, err
	}

	var settled atomic.Int32
	var wg sync.WaitGroup
	for _, item := range items {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if p.deliver(ctx, item) {
				settled.Add(1)
			}
		}()
	}
	wg.Wait()
	return int(settled.Load()), nil
}

// Run publishes until ctx is cancelled, and dead-letters exhausted events in
// the background. It returns nil on shutdown.
func (p *Publisher) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.runReaper(ctx)
	}()
	defer wg.Wait()

	for ctx.Err() == nil {
		n, err := p.Step(ctx)
		if err != nil {
			p.log.Error("publisher step failed", "error", err)
			sleep(ctx, p.cfg.PollInterval)
			continue
		}
		if n == 0 {
			sleep(ctx, p.cfg.PollInterval)
		}
		// n > 0: there may be more waiting, so go again at once.
	}
	return nil
}

// deliver sends one event and records the result. It reports whether a result
// was recorded.
func (p *Publisher) deliver(ctx context.Context, item OutboxItem) bool {
	log := p.log.With("event_id", item.EventID, "topic", item.Topic, "attempt", item.Attempts)

	dctx, cancel := context.WithTimeout(ctx, p.cfg.DeliverTimeout)
	err := safeDeliver(dctx, p.sink, item)
	cancel()

	if err == nil {
		// Recorded even if shutdown has started: the delivery happened.
		return p.mark(log, item)
	}
	// Shutdown cut the delivery short. That is not the event's fault, so no
	// failure is charged: it stays leased and is retried after the lease runs
	// out (this costs one attempt; release on shutdown is not implemented).
	if ctx.Err() != nil {
		log.Warn("delivery stopped by shutdown; leaving event for lease recovery", "error", err)
		return false
	}
	return p.reportFailure(log, item, p.failureFor(item, err))
}

func (p *Publisher) failureFor(item OutboxItem, err error) FailParams {
	class, retryAfter := Classify(err)
	retryAfter = min(retryAfter, p.cfg.MaxRetryAfter)
	delay := max(Backoff(item.Attempts, p.cfg.BackoffBase, p.cfg.BackoffCap, p.cfg.Rand), retryAfter)
	return FailParams{Class: class, Message: err.Error(), Delay: delay}
}

func (p *Publisher) mark(log *slog.Logger, item OutboxItem) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// MarkPublished is fenced, so sending it again cannot apply it twice. If the
	// first send did commit, the retry is rejected as ErrLeaseLost: the event is
	// then published, but we cannot tell that from a takeover, so we say so.
	attempts, err := retryTransient(ctx, p.cfg.ConnRetry, p.cfg.Rand, func(ctx context.Context) error {
		return MarkPublished(ctx, p.db, item.ID, p.cfg.ID, item.Attempts)
	})
	switch {
	case errors.Is(err, ErrLeaseLost) && attempts > 1:
				log.Warn("event was delivered; after a connection error, outcome unknown: marking it may have committed on the first try",
			"attempts", attempts)
		return false
	case errors.Is(err, ErrLeaseLost):
		log.Warn("event was delivered but another publisher owns it now; it may be delivered twice")
		return false
	case err != nil:
		log.Error("event was delivered but could not be marked published; it will be delivered again after its lease expires",
			"error", err, "attempts", attempts)
		return false
	}
	return true
}

func (p *Publisher) reportFailure(log *slog.Logger, item OutboxItem, fp FailParams) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var outcome FailOutcome
	attempts, err := retryTransient(ctx, p.cfg.ConnRetry, p.cfg.Rand, func(ctx context.Context) error {
		var callErr error
		outcome, callErr = FailOutbox(ctx, p.db, item.ID, p.cfg.ID, item.Attempts, fp)
		return callErr
	})
	switch {
	case errors.Is(err, ErrLeaseLost) && attempts > 1:
				log.Warn("after a connection error, outcome unknown: recording the failure may have committed on the first try",
			"attempts", attempts)
		return false
	case errors.Is(err, ErrLeaseLost):
		log.Warn("lease lost before the failure could be recorded; another publisher owns this event now")
		return false
	case err != nil:
		log.Error("recording the failure failed; the event stays leased until its lease expires",
			"error", err, "attempts", attempts)
		return false
	}
	log.Warn("delivery failed", "class", fp.Class.String(), "outcome", outcome.String(),
		"retry_in", fp.Delay, "error", fp.Message)
	return true
}

// runReaper dead-letters events that crashed every attempt, until ctx ends.
func (p *Publisher) runReaper(ctx context.Context) {
	const batch = 100
	for ctx.Err() == nil {
		for ctx.Err() == nil {
			rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			n, err := ReapOutbox(rctx, p.db, batch)
			cancel()
			if err != nil {
				if ctx.Err() == nil {
					p.log.Error("outbox reap failed", "error", err)
				}
				break
			}
			if n > 0 {
				p.log.Warn("dead-lettered events that crashed every attempt", "count", n)
				if n >= batch {
					continue // a full batch: there may be more
				}
			}
			break
		}
		sleep(ctx, p.cfg.ReapInterval)
	}
}

// safeDeliver turns a sink panic into a retryable error, so one bad event
// cannot crash the publisher.
func safeDeliver(ctx context.Context, sink Sink, item OutboxItem) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = Retryable(fmt.Errorf("sink panicked: %v\n%s", r, debug.Stack()))
		}
	}()
	return sink(ctx, item)
}