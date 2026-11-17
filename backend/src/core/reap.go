package core

import (
	"context"
	"errors"
	"fmt"
)

// ReapResult says what one call to Reap did.
type ReapResult struct {
	Requeued int // expired jobs put back in the queue
	Dead     int // expired jobs that had used all their attempts
}

// Reap finds running jobs whose lease has expired (on the database clock) and
// takes them back, at most limit per call. A job with attempts left goes back
// to queued; a job that has used all of them becomes dead. The crashed attempt
// was already counted when the job was claimed, so a job that keeps killing
// its worker eventually dies instead of looping forever.
//
// Everything, including closing the attempt row as lease_expired, happens in
// one SQL statement, so it is atomic, and concurrent reapers skip each other's
// rows.
func Reap(ctx context.Context, q Querier, limit int) (ReapResult, error) {
	if limit < 1 {
		return ReapResult{}, errors.New("reap: limit must be at least 1")
	}

	var res ReapResult
	err := q.QueryRow(ctx, `
		WITH expired AS (
			SELECT id, attempts, max_attempts
			FROM jobs
			WHERE state = 'running' AND lease_expires_at < now()
			ORDER BY lease_expires_at
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		), upd AS (
			UPDATE jobs j
			SET state = CASE WHEN e.attempts >= e.max_attempts THEN 'dead' ELSE 'queued' END,
			    lease_owner = NULL,
			    lease_expires_at = NULL,
			    finished_at = CASE WHEN e.attempts >= e.max_attempts THEN now() END,
			    last_error = 'lease expired'
			FROM expired e
			WHERE j.id = e.id
			RETURNING j.id, j.state, j.fence
		), att AS (
			UPDATE job_attempts a
			SET ended_at = now(),
			    outcome = 'lease_expired',
			    error = 'lease expired',
			    duration_ms = (EXTRACT(EPOCH FROM (now() - a.started_at)) * 1000)::int
			FROM upd
			WHERE a.job_id = upd.id AND a.fence = upd.fence AND a.outcome IS NULL
		)
		SELECT count(*) FILTER (WHERE state = 'queued'),
		       count(*) FILTER (WHERE state = 'dead')
		FROM upd`,
		limit).Scan(&res.Requeued, &res.Dead)
	if err != nil {
		return ReapResult{}, fmt.Errorf("reap: %w", err)
	}
	return res, nil
}
