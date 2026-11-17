package core

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// ConnRetry bounds how often a worker or publisher re-sends a result report
// (Ack, Fail, MarkPublished, FailOutbox) after a connection error.
//
// The retry is safe because those calls are fenced: if the first send did
// commit and only the reply was lost, the second send matches no row and comes
// back as ErrLeaseLost. Nothing is ever applied twice.
//
// It is deliberately short. The call still holds a worker slot while it
// retries, and the whole retry, waits included, must fit inside the 10 second
// limit its caller sets. An outage longer than the budget falls back to lease
// expiry, as before.
//
// The zero value takes the defaults below.
type ConnRetry struct {
	Attempts  int           // total tries including the first; default 5; 1 means no retry
	BaseDelay time.Duration // ceiling of the first wait; default 100ms
	MaxDelay  time.Duration // ceiling of any wait; default 1s

	// wait lets tests see the delays without sleeping. Nil means sleep.
	wait func(ctx context.Context, d time.Duration)
}

func (c ConnRetry) withDefaults() ConnRetry {
	if c.Attempts <= 0 {
		c.Attempts = 5
	}
	if c.BaseDelay <= 0 {
		c.BaseDelay = 100 * time.Millisecond
	}
	if c.MaxDelay <= 0 {
		c.MaxDelay = time.Second
	}
	if c.MaxDelay < c.BaseDelay {
		c.MaxDelay = c.BaseDelay
	}
	return c
}

// isTransient reports whether err looks like a lost or unavailable connection,
// the kind of failure a second try can get past. It is false for everything
// else: a cancelled or expired context (the caller's budget is spent), a lost
// lease, constraint errors, serialization failures, and errors nobody has
// classified.
func isTransient(err error) bool {
	if err == nil {
		return false
	}
	// A context error also satisfies net.Error, so it must be ruled out first.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "57P01", // admin_shutdown: the session was terminated
			"57P02", // crash_shutdown
			"57P03": // cannot_connect_now: the server is starting up
			return true
		}
		return strings.HasPrefix(pgErr.Code, "08") // connection exception class
	}

	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

// retryTransient calls fn until it succeeds, returns an error that is not
// transient, or the policy runs out. It returns how many calls it made and the
// last error.
//
// ctx bounds the whole retry, waits included: once it ends, no further call is
// made and the last operation error is returned (not the context error, which
// says less about what went wrong).
func retryTransient(ctx context.Context, policy ConnRetry, rnd func() float64,
	fn func(ctx context.Context) error) (attempts int, err error) {

	policy = policy.withDefaults()
	wait := policy.wait
	if wait == nil {
		wait = sleep
	}
	if rnd == nil {
		rnd = rand.Float64
	}

	for attempt := 1; ; attempt++ {
		err = fn(ctx)
		if err == nil || !isTransient(err) {
			return attempt, err
		}
		if attempt >= policy.Attempts || ctx.Err() != nil {
			return attempt, err
		}
		wait(ctx, Backoff(attempt, policy.BaseDelay, policy.MaxDelay, rnd))
		if ctx.Err() != nil {
			return attempt, err
		}
	}
}