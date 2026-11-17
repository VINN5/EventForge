package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Job is a claimed job as seen by a worker.
type Job struct {
	ID          string
	TenantID    string
	Queue       string
	Type        string
	Downstream  string
	Payload     json.RawMessage
	Attempts    int   // how many times this job has been claimed and not released
	Fence       int64 // fencing token for ack, fail and heartbeat; only ever increases
	MaxAttempts int
	LeaseOwner  string
}

// Claim takes up to limit due jobs of any type from a queue and leases them
// to worker. The lease length is measured on the database clock.
func Claim(ctx context.Context, q Querier, queue, worker string, limit int, lease time.Duration) ([]Job, error) {
	return ClaimTypes(ctx, q, queue, worker, nil, limit, lease)
}

// ClaimTypes is Claim restricted to the given job types, so a worker only takes
// work it has a handler for. A nil or empty list means every type.
//
// Each claim also opens a row in job_attempts, in the same statement. Ack,
// Fail and Reap close that row, so every attempt has a history.
func ClaimTypes(ctx context.Context, q Querier, queue, worker string, types []string, limit int, lease time.Duration) ([]Job, error) {
	if queue == "" || worker == "" {
		return nil, errors.New("claim: queue and worker are required")
	}
	if limit < 1 {
		return nil, errors.New("claim: limit must be at least 1")
	}
	if lease <= 0 {
		return nil, errors.New("claim: lease must be positive")
	}
	if len(types) == 0 {
		types = nil // SQL NULL: no type filter
	}

	rows, err := q.Query(ctx, `
		WITH next AS (
			SELECT id FROM jobs
			WHERE state = 'queued' AND queue = $1 AND run_at <= now()
			  AND ($5::text[] IS NULL OR type = ANY($5::text[]))
			ORDER BY priority DESC, run_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		), upd AS (
			UPDATE jobs j
			SET state = 'running',
			    lease_owner = $3,
			    lease_expires_at = now() + make_interval(secs => $4),
			    attempts = j.attempts + 1,
			    fence = j.fence + 1
			FROM next
			WHERE j.id = next.id
			RETURNING j.id, j.tenant_id, j.queue, j.type, j.downstream,
			          j.payload, j.attempts, j.fence, j.max_attempts, j.lease_owner
		), ins AS (
			INSERT INTO job_attempts (job_id, attempt, fence, worker, started_at)
			SELECT id, attempts, fence, lease_owner, now() FROM upd
		)
		SELECT id, tenant_id, queue, type, COALESCE(downstream, ''),
		       payload, attempts, fence, max_attempts, lease_owner
		FROM upd`,
		queue, limit, worker, lease.Seconds(), types)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var j Job
		var payload []byte
		if err := rows.Scan(&j.ID, &j.TenantID, &j.Queue, &j.Type, &j.Downstream,
			&payload, &j.Attempts, &j.Fence, &j.MaxAttempts, &j.LeaseOwner); err != nil {
			return nil, fmt.Errorf("claim: scan: %w", err)
		}
		j.Payload = json.RawMessage(payload)
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	return jobs, nil
}
