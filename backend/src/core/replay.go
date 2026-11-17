package core

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ReplayDead puts one dead job back in the queue and records who did it and
// why, in a single statement: the state change and the audit row exist
// together or not at all.
//
// The job restarts with a fresh set of attempts (attempts goes back to 0) and
// is due immediately. A job that crashes workers will do so again, up to
// max_attempts times: replay deliberately gives it the full allowance. The
// fence is never touched, so every future claim still gets a token that no
// earlier attempt held, and the attempt history stays intact. last_error is
// kept, so an operator still sees why the job died; the next failure
// overwrites it.
//
// The audit row records the attempts, fence and last error as they were at the
// moment of replay.
//
// Only a dead job can be replayed. For any other job, an unknown id or a
// malformed id the result is ErrJobNotFound and nothing changes, not even the
// audit log. Two callers replaying the same job at once: exactly one wins.
// (That relies on READ COMMITTED; under a stricter level the loser may get a
// serialization error instead, which the caller can retry.)
func ReplayDead(ctx context.Context, q Querier, id, actor, reason string) error {
	actor = strings.TrimSpace(actor)
	reason = strings.TrimSpace(reason)
	if actor == "" {
		return errors.New("replay: actor is required")
	}
	if reason == "" {
		return errors.New("replay: reason is required")
	}
	id = strings.ToLower(id)
	if !isUUID(id) {
		return ErrJobNotFound
	}

	var subject string
	err := q.QueryRow(ctx, `
		WITH target AS (
			SELECT id, attempts, fence, COALESCE(last_error, '') AS last_error
			FROM jobs
			WHERE id = $1::uuid AND state = 'dead'
			FOR UPDATE
		), requeued AS (
			UPDATE jobs j
			SET state = 'queued', attempts = 0, run_at = now(), finished_at = NULL
			FROM target t
			WHERE j.id = t.id
			RETURNING j.id
		), audited AS (
			INSERT INTO audit_log (actor, action, subject, reason, details)
			SELECT $2::text, 'replay_dead_job', t.id::text, $3::text,
			       jsonb_build_object('attempts_before', t.attempts,
			                          'fence_before', t.fence,
			                          'last_error', t.last_error)
			FROM target t
			JOIN requeued r ON r.id = t.id
			RETURNING subject
		)
		SELECT subject FROM audited`,
		id, actor, reason).Scan(&subject)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrJobNotFound
	}
	if err != nil {
		return fmt.Errorf("replay: %w", err)
	}
	return nil
}