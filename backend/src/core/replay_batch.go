package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// maxReplayBatch caps one ReplayDeadBatch call. A larger backlog is drained by
// calling again: replayed jobs are no longer dead, so each call continues
// where the last stopped.
const maxReplayBatch = 1000

// ReplayFilter selects dead jobs to replay. Zero values mean "no filter", so
// the zero ReplayFilter matches every dead job.
type ReplayFilter struct {
	Type           string
	TenantID       string
	FinishedAfter  time.Time // inclusive
	FinishedBefore time.Time // exclusive
}

// ReplayDeadBatch replays up to limit dead jobs that match the filter, oldest
// finished first, and returns their ids in that order. Each job gets its own
// audit row (the same action as ReplayDead, plus batch=true and the filter in
// its details), and everything happens in one statement: the state changes and
// the audit rows exist together or not at all.
//
// The limit is mandatory (1 to maxReplayBatch), so a mistaken empty filter can
// never replay an entire backlog in one call.
//
// Jobs are reset exactly as ReplayDead does: attempts back to 0, fence
// untouched, history and last_error kept. A replayed poison job gets a full
// fresh set of attempts.
//
// Concurrent callers get disjoint sets: rows another caller holds are skipped,
// so a call can return fewer jobs than the limit even though more match. Call
// again until it returns nothing.
func ReplayDeadBatch(ctx context.Context, q Querier, f ReplayFilter, limit int, actor, reason string) ([]string, error) {
	actor = strings.TrimSpace(actor)
	reason = strings.TrimSpace(reason)
	if actor == "" {
		return nil, errors.New("replay batch: actor is required")
	}
	if reason == "" {
		return nil, errors.New("replay batch: reason is required")
	}
	if limit < 1 || limit > maxReplayBatch {
		return nil, fmt.Errorf("replay batch: limit must be between 1 and %d", maxReplayBatch)
	}

	var after, before *time.Time
	if !f.FinishedAfter.IsZero() {
		after = &f.FinishedAfter
	}
	if !f.FinishedBefore.IsZero() {
		before = &f.FinishedBefore
	}

	rows, err := q.Query(ctx, `
		WITH target AS (
			SELECT id, attempts, fence, COALESCE(last_error, '') AS last_error, finished_at
			FROM jobs
			WHERE state = 'dead'
			  AND ($1::text = '' OR type = $1)
			  AND ($2::text = '' OR tenant_id = $2)
			  AND ($3::timestamptz IS NULL OR finished_at >= $3)
			  AND ($4::timestamptz IS NULL OR finished_at < $4)
			ORDER BY finished_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT $5::int
		), requeued AS (
			UPDATE jobs j
			SET state = 'queued', attempts = 0, run_at = now(), finished_at = NULL
			FROM target t
			WHERE j.id = t.id
			RETURNING j.id
		), audited AS (
			INSERT INTO audit_log (actor, action, subject, reason, details)
			SELECT $6::text, 'replay_dead_job', t.id::text, $7::text,
			       jsonb_build_object(
			           'attempts_before', t.attempts,
			           'fence_before', t.fence,
			           'last_error', t.last_error,
			           'batch', true,
			           'filter', jsonb_build_object(
			               'type', $1::text,
			               'tenant', $2::text,
			               'finished_after', $3::timestamptz,
			               'finished_before', $4::timestamptz))
			FROM target t
			JOIN requeued r ON r.id = t.id
			RETURNING subject
		)
		SELECT a.subject
		FROM audited a
		JOIN target t ON t.id::text = a.subject
		ORDER BY t.finished_at, t.id`,
		f.Type, f.TenantID, after, before, limit, actor, reason)
	if err != nil {
		return nil, fmt.Errorf("replay batch: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("replay batch: scan: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("replay batch: %w", err)
	}
	return ids, nil
}