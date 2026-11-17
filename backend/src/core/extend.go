package core

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// LeaseRef names one claim a worker holds: the job and the fence it was given.
type LeaseRef struct {
	ID    string
	Fence int64
}

// Extend pushes the lease of every listed job out to now + lease, in one SQL
// statement. A job is extended only if it is still running, owned by worker,
// with the same fence, which is the rule Ack and Fail use. An expired lease
// that the reaper has not taken yet can still be extended, because the fence
// decides ownership and the clock only decides when the reaper may act.
//
// It returns the refs that could not be extended: those jobs are no longer
// this worker's, and their handlers should be stopped.
func Extend(ctx context.Context, q Querier, worker string, refs []LeaseRef, lease time.Duration) ([]LeaseRef, error) {
	if worker == "" {
		return nil, errors.New("extend: worker is required")
	}
	if lease <= 0 {
		return nil, errors.New("extend: lease must be positive")
	}
	if len(refs) == 0 {
		return nil, nil
	}

	ids := make([]string, len(refs))
	fences := make([]int64, len(refs))
	for i, r := range refs {
		ids[i] = r.ID
		fences[i] = r.Fence
	}

	rows, err := q.Query(ctx, `
		UPDATE jobs j
		SET lease_expires_at = now() + make_interval(secs => $3)
		FROM unnest($1::text[], $2::bigint[]) AS h(id, fence)
		WHERE j.id = h.id::uuid
		  AND j.state = 'running'
		  AND j.lease_owner = $4
		  AND j.fence = h.fence
		RETURNING j.id::text`,
		ids, fences, lease.Seconds(), worker)
	if err != nil {
		return nil, fmt.Errorf("extend: %w", err)
	}
	defer rows.Close()

	extended := make(map[string]bool, len(refs))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("extend: scan: %w", err)
		}
		extended[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("extend: %w", err)
	}

	var lost []LeaseRef
	for _, r := range refs {
		if !extended[r.ID] {
			lost = append(lost, r)
		}
	}
	return lost, nil
}
