package core

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ErrIdempotencyConflict means the key was already used for a different
// request (different hash). The caller should treat it as a client error.
var ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")

// ErrUnsupportedIsolation means SubmitIdempotent was called inside a
// transaction that is not READ COMMITTED. The duplicate check below relies on
// each statement getting a fresh snapshot. The caller must roll back.
var ErrUnsupportedIsolation = errors.New("idempotent submit requires READ COMMITTED isolation")

const (
	maxIdempotencyFieldBytes = 255
	maxIdempotencyTries      = 3
)

// SubmitIdempotent stores a job at most once per (clientID, key).
//
// The first call with a key creates the job and returns created=true. A later
// call with the same key and the same request returns the original job ID
// with created=false. The same key with a different request returns
// ErrIdempotencyConflict.
//
// It works on a pool or inside the application's transaction. In a
// transaction the key and job are committed or rolled back together with the
// caller's other writes.
//
// Two things callers should know:
//   - A concurrent duplicate BLOCKS until the first submission's transaction
//     commits or rolls back. If it rolls back, the duplicate creates the job.
//     In a long application transaction, give ctx a deadline.
//   - Two transactions that use keys in opposite orders can deadlock;
//     Postgres aborts one of them and the caller should retry its transaction.
func SubmitIdempotent(ctx context.Context, q Querier, clientID, key string, p SubmitParams) (string, bool, error) {
	if clientID == "" || key == "" {
		return "", false, errors.New("submit idempotent: client id and key are required")
	}
	if len(clientID) > maxIdempotencyFieldBytes || len(key) > maxIdempotencyFieldBytes {
		return "", false, fmt.Errorf("submit idempotent: client id and key must be at most %d bytes",
			maxIdempotencyFieldBytes)
	}
	if err := validateSubmit(p); err != nil {
		return "", false, err
	}

	// Hash the request as the client sent it, before defaults are applied.
	hash, err := requestHash(p)
	if err != nil {
		return "", false, fmt.Errorf("submit idempotent: %w", err)
	}

	if p.Queue == "" {
		p.Queue = "default"
	}
	if p.MaxAttempts == 0 {
		p.MaxAttempts = 8
	}

	for try := 0; try < maxIdempotencyTries; try++ {
		id, created, err := insertKeyAndJob(ctx, q, clientID, key, hash, p)
		if err != nil {
			return "", false, err
		}
		if created {
			return id, true, nil
		}

		// The key already existed. Read it in a NEW statement: under READ
		// COMMITTED this takes a fresh snapshot, so it sees a row that a
		// concurrent transaction committed while our insert was waiting.
		var existingID, existingHash string
		err = q.QueryRow(ctx, `
			SELECT job_id::text, request_hash
			FROM idempotency_keys
			WHERE client_id = $1 AND key = $2`,
			clientID, key).Scan(&existingID, &existingHash)
		if errors.Is(err, pgx.ErrNoRows) {
			continue // purged between the two statements: start over
		}
		if err != nil {
			return "", false, fmt.Errorf("submit idempotent: read key: %w", err)
		}
		// Both hashes are v1 today. When a v2 exists, compare under the
		// version stored here instead of requiring equal strings.
		if existingHash != hash {
			return "", false, ErrIdempotencyConflict
		}
		return existingID, false, nil
	}
	return "", false, errors.New("submit idempotent: key kept disappearing; giving up")
}

// insertKeyAndJob is statement 1: insert the key, and insert the job only if
// the key insert actually inserted a row. It also reports the transaction's
// isolation level so the caller can refuse anything but READ COMMITTED.
func insertKeyAndJob(ctx context.Context, q Querier, clientID, key, hash string, p SubmitParams) (string, bool, error) {
	var isolation string
	var id *string // NULL when the key already existed
	err := q.QueryRow(ctx, `
		WITH new_job AS (
			SELECT gen_random_uuid() AS id
		), key_ins AS (
			INSERT INTO idempotency_keys (client_id, key, request_hash, job_id)
			SELECT $1::text, $2::text, $3::text, id FROM new_job
			ON CONFLICT (client_id, key) DO NOTHING
			RETURNING job_id
		), job_ins AS (
			INSERT INTO jobs (id, tenant_id, queue, type, downstream, payload,
			                  priority, run_at, ordering_key, max_attempts)
			SELECT k.job_id, $4::text, $5::text, $6::text, NULLIF($7::text, ''), $8::jsonb,
			       $9::int, now() + make_interval(secs => $10::float8),
			       NULLIF($11::text, ''), $12::int
			FROM key_ins k
			RETURNING id
		)
		SELECT current_setting('transaction_isolation'),
		       (SELECT id::text FROM job_ins)`,
		clientID, key, hash,
		p.TenantID, p.Queue, p.Type, p.Downstream, p.Payload,
		p.Priority, p.RunAfter.Seconds(), p.OrderingKey, p.MaxAttempts,
	).Scan(&isolation, &id)
	if err != nil {
		return "", false, fmt.Errorf("submit idempotent: %w", err)
	}
	if !strings.EqualFold(isolation, "read committed") {
		return "", false, ErrUnsupportedIsolation
	}
	if id == nil {
		return "", false, nil
	}
	return *id, true, nil
}