package core

import (
	"context"
	"errors"
	"fmt"
)

// ErrLeaseLost means the caller no longer owns this claim of the job: the
// lease expired and was taken over, the job already finished, or it never
// belonged to this worker. The worker must stop and must not report success.
var ErrLeaseLost = errors.New("lease lost")

// Ack marks a running job as succeeded and closes its attempt row, in one
// statement. The fence is the fencing token: it must match the fence this
// worker received when it claimed the job.
func Ack(ctx context.Context, q Querier, jobID, worker string, fence int64) error {
	var n int
	err := q.QueryRow(ctx, `
		WITH upd AS (
			UPDATE jobs
			SET state = 'succeeded',
			    finished_at = now(),
			    lease_owner = NULL,
			    lease_expires_at = NULL
			WHERE id = $1
			  AND state = 'running'
			  AND lease_owner = $2
			  AND fence = $3
			RETURNING id, fence
		), att AS (
			UPDATE job_attempts a
			SET ended_at = now(),
			    outcome = 'succeeded',
			    duration_ms = (EXTRACT(EPOCH FROM (now() - a.started_at)) * 1000)::int
			FROM upd
			WHERE a.job_id = upd.id AND a.fence = upd.fence AND a.outcome IS NULL
		)
		SELECT count(*) FROM upd`,
		jobID, worker, fence).Scan(&n)
	if err != nil {
		return fmt.Errorf("ack: %w", err)
	}
	if n == 0 {
		return ErrLeaseLost
	}
	return nil
}
