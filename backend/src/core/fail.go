package core

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// maxErrorText is the most error text stored per failure, in bytes.
const maxErrorText = 2000

// FailParams describes how an attempt failed.
type FailParams struct {
	Class   ErrorClass    // ClassPermanent dead-letters the job at once
	Message string        // stored on the job and on its attempt row
	Delay   time.Duration // wait before the retry; used only for retryable failures
}

// FailOutcome says what happened to the job after a failure was recorded.
type FailOutcome int

const (
	FailRetried FailOutcome = iota + 1 // back in the queue, due after Delay
	FailDead                           // dead-lettered
)

func (o FailOutcome) String() string {
	switch o {
	case FailRetried:
		return "retried"
	case FailDead:
		return "dead"
	}
	return "unknown"
}

// Fail records that a worker's attempt failed. Like Ack it is fenced: the job
// must still be running, owned by this worker, with this fence, otherwise
// nothing changes and ErrLeaseLost comes back.
//
// A permanent failure, or a retryable one with no attempts left, makes the job
// dead. Otherwise the job goes back to queued with run_at set Delay from now
// on the database clock. attempts and fence are left alone: the next Claim
// raises them.
//
// The job update and the closing of its attempt row are one SQL statement, so
// a rejected call leaves no trace.
func Fail(ctx context.Context, q Querier, jobID, worker string, fence int64, p FailParams) (FailOutcome, error) {
	permanent := p.Class == ClassPermanent
	outcome := "retryable_failure"
	if permanent {
		outcome = "permanent_failure"
	}
	delay := p.Delay
	if delay < 0 {
		delay = 0
	}
	msg := cleanErrorText(p.Message, maxErrorText)

	var state string
	err := q.QueryRow(ctx, `
		WITH cur AS (
			SELECT id, attempts, max_attempts
			FROM jobs
			WHERE id = $1 AND state = 'running' AND lease_owner = $2 AND fence = $3
			FOR UPDATE
		), upd AS (
			UPDATE jobs j
			SET state = CASE WHEN $4::boolean OR c.attempts >= c.max_attempts
			                 THEN 'dead' ELSE 'queued' END,
			    run_at = CASE WHEN $4::boolean OR c.attempts >= c.max_attempts
			                  THEN j.run_at
			                  ELSE now() + make_interval(secs => $5::float8) END,
			    finished_at = CASE WHEN $4::boolean OR c.attempts >= c.max_attempts
			                       THEN now() END,
			    lease_owner = NULL,
			    lease_expires_at = NULL,
			    last_error = $6::text
			FROM cur c
			WHERE j.id = c.id
			RETURNING j.id, j.state, j.fence
		), att AS (
			UPDATE job_attempts a
			SET ended_at = now(),
			    outcome = $7::text,
			    error = $6::text,
			    duration_ms = (EXTRACT(EPOCH FROM (now() - a.started_at)) * 1000)::int
			FROM upd
			WHERE a.job_id = upd.id AND a.fence = upd.fence AND a.outcome IS NULL
		)
		SELECT state FROM upd`,
		jobID, worker, fence, permanent, delay.Seconds(), msg, outcome,
	).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrLeaseLost
	}
	if err != nil {
		return 0, fmt.Errorf("fail: %w", err)
	}
	if state == "dead" {
		return FailDead, nil
	}
	return FailRetried, nil
}
