package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrJobNotFound means no dead job has that id. It is also returned for an id
// that is not a valid UUID and for a job that is not dead, so callers (an HTTP
// handler, say) can treat all three as a plain 404.
var ErrJobNotFound = errors.New("job not found")

const (
	defaultDeadLimit = 50
	maxDeadLimit     = 500
)

// DeadJob is the summary of a dead-lettered job.
type DeadJob struct {
	ID         string
	TenantID   string
	Queue      string
	Type       string
	Attempts   int
	LastError  string
	CreatedAt  time.Time
	FinishedAt time.Time
}

// DeadCursor marks a position in the dead-letter list. Pass the cursor that
// ListDead returned as DeadFilter.After to get the next page.
type DeadCursor struct {
	FinishedAt time.Time
	ID         string
}

// DeadFilter narrows ListDead. Zero values mean "no filter".
type DeadFilter struct {
	Type           string
	TenantID       string
	FinishedAfter  time.Time // inclusive
	FinishedBefore time.Time // exclusive
	Limit          int       // 0 means 50; more than 500 is clamped to 500
	After          *DeadCursor
}

// AttemptRecord is one row of a job's attempt history.
type AttemptRecord struct {
	Attempt    int
	Fence      int64
	Worker     string
	Outcome    string // empty while the attempt is still open
	Error      string
	StartedAt  time.Time // zero for old rows written before attempts were opened at claim time
	EndedAt    *time.Time
	DurationMS *int
}

// DeadDetail is a dead job with its payload and full attempt history.
type DeadDetail struct {
	DeadJob
	MaxAttempts int
	Payload     json.RawMessage
	History     []AttemptRecord
}

// ListDead returns dead jobs, newest first (ties broken by id, descending).
// It pages with a cursor, not an offset, so pages stay correct while jobs
// keep dying. The returned cursor is nil when there are no more pages.
//
// It assumes every dead job has finished_at set, which Fail and Reap
// guarantee.
func ListDead(ctx context.Context, q Querier, f DeadFilter) ([]DeadJob, *DeadCursor, error) {
	limit := f.Limit
	switch {
	case limit < 0:
		return nil, nil, errors.New("list dead: limit must not be negative")
	case limit == 0:
		limit = defaultDeadLimit
	case limit > maxDeadLimit:
		limit = maxDeadLimit
	}

	var after, before, curTime *time.Time
	var curID *string
	if !f.FinishedAfter.IsZero() {
		after = &f.FinishedAfter
	}
	if !f.FinishedBefore.IsZero() {
		before = &f.FinishedBefore
	}
	if f.After != nil {
		curTime = &f.After.FinishedAt
		curID = &f.After.ID
	}

	rows, err := q.Query(ctx, `
		SELECT id::text, tenant_id, queue, type, attempts,
		       COALESCE(last_error, ''), created_at, finished_at
		FROM jobs
		WHERE state = 'dead'
		  AND ($1::text = '' OR type = $1)
		  AND ($2::text = '' OR tenant_id = $2)
		  AND ($3::timestamptz IS NULL OR finished_at >= $3)
		  AND ($4::timestamptz IS NULL OR finished_at < $4)
		  AND ($5::timestamptz IS NULL OR (finished_at, id) < ($5::timestamptz, $6::uuid))
		ORDER BY finished_at DESC, id DESC
		LIMIT $7`,
		f.Type, f.TenantID, after, before, curTime, curID, limit+1)
	if err != nil {
		return nil, nil, fmt.Errorf("list dead: %w", err)
	}
	defer rows.Close()

	var jobs []DeadJob
	for rows.Next() {
		var j DeadJob
		if err := rows.Scan(&j.ID, &j.TenantID, &j.Queue, &j.Type, &j.Attempts,
			&j.LastError, &j.CreatedAt, &j.FinishedAt); err != nil {
			return nil, nil, fmt.Errorf("list dead: scan: %w", err)
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("list dead: %w", err)
	}

	// One row more than asked for tells us another page exists.
	var next *DeadCursor
	if len(jobs) > limit {
		jobs = jobs[:limit]
		last := jobs[len(jobs)-1]
		next = &DeadCursor{FinishedAt: last.FinishedAt, ID: last.ID}
	}
	return jobs, next, nil
}

// GetDead returns one dead job with its payload and attempt history, oldest
// attempt first. It returns ErrJobNotFound for an unknown or malformed id and
// for a job that is not dead.
func GetDead(ctx context.Context, q Querier, id string) (DeadDetail, error) {
	id = strings.ToLower(id)
	if !isUUID(id) {
		return DeadDetail{}, ErrJobNotFound
	}

	var d DeadDetail
	var payload []byte
	err := q.QueryRow(ctx, `
		SELECT id::text, tenant_id, queue, type, attempts,
		       COALESCE(last_error, ''), created_at, finished_at,
		       max_attempts, payload
		FROM jobs
		WHERE id = $1::uuid AND state = 'dead'`, id,
	).Scan(&d.ID, &d.TenantID, &d.Queue, &d.Type, &d.Attempts,
		&d.LastError, &d.CreatedAt, &d.FinishedAt, &d.MaxAttempts, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return DeadDetail{}, ErrJobNotFound
	}
	if err != nil {
		return DeadDetail{}, fmt.Errorf("get dead: %w", err)
	}
	d.Payload = json.RawMessage(payload)

	rows, err := q.Query(ctx, `
		SELECT attempt, fence, COALESCE(worker, ''), COALESCE(outcome, ''),
		       COALESCE(error, ''), started_at, ended_at, duration_ms
		FROM job_attempts
		WHERE job_id = $1::uuid
		ORDER BY fence`, id)
	if err != nil {
		return DeadDetail{}, fmt.Errorf("get dead: history: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var h AttemptRecord
		var started *time.Time
		if err := rows.Scan(&h.Attempt, &h.Fence, &h.Worker, &h.Outcome,
			&h.Error, &started, &h.EndedAt, &h.DurationMS); err != nil {
			return DeadDetail{}, fmt.Errorf("get dead: history scan: %w", err)
		}
		if started != nil {
			h.StartedAt = *started
		}
		d.History = append(d.History, h)
	}
	if err := rows.Err(); err != nil {
		return DeadDetail{}, fmt.Errorf("get dead: history: %w", err)
	}
	return d, nil
}

// isUUID reports whether s is a lowercase UUID in 8-4-4-4-12 form.
func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case i == 8 || i == 13 || i == 18 || i == 23:
			if c != '-' {
				return false
			}
		case (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'):
			// valid hex digit
		default:
			return false
		}
	}
	return true
}
