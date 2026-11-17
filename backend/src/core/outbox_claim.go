package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

// OutboxItem is an event leased to a publisher.
type OutboxItem struct {
	ID          int64
	EventID     string // consumers deduplicate on this
	Topic       string
	Payload     json.RawMessage
	Attempts    int // the fencing token: pass it back to MarkPublished / FailOutbox
	MaxAttempts int
}

// ClaimOutbox leases up to limit due events to publisher, oldest first. The
// lease length is measured on the database clock.
//
// An event is due when it is neither published nor dead, still has attempts
// left, its retry time has come, and it has no live lease. An event whose
// lease ran out is therefore simply claimable again: a publisher that died
// needs no separate reaper. Every claim costs the event one attempt.
func ClaimOutbox(ctx context.Context, q Querier, publisher string, limit int, lease time.Duration) ([]OutboxItem, error) {
	if publisher == "" {
		return nil, errors.New("claim outbox: publisher is required")
	}
	if limit < 1 {
		return nil, errors.New("claim outbox: limit must be at least 1")
	}
	if lease <= 0 {
		return nil, errors.New("claim outbox: lease must be positive")
	}

	rows, err := q.Query(ctx, `
		WITH next AS (
			SELECT id FROM outbox
			WHERE published_at IS NULL AND dead_at IS NULL
			  AND attempts < max_attempts
			  AND next_attempt_at <= now()
			  AND (lease_expires_at IS NULL OR lease_expires_at <= now())
			ORDER BY next_attempt_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT $3::int
		)
		UPDATE outbox o
		SET lease_owner = $1::text,
		    lease_expires_at = now() + make_interval(secs => $2::float8),
		    attempts = o.attempts + 1
		FROM next
		WHERE o.id = next.id
		RETURNING o.id, o.event_id::text, o.topic, o.payload, o.attempts, o.max_attempts`,
		publisher, lease.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("claim outbox: %w", err)
	}
	defer rows.Close()

	var items []OutboxItem
	for rows.Next() {
		var it OutboxItem
		if err := rows.Scan(&it.ID, &it.EventID, &it.Topic, &it.Payload, &it.Attempts, &it.MaxAttempts); err != nil {
			return nil, fmt.Errorf("claim outbox: scan: %w", err)
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim outbox: %w", err)
	}
	// UPDATE ... RETURNING has no defined order; ids follow insertion order.
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return items, nil
}

// MarkPublished records that an event was delivered. It is fenced: the event
// must still be held by this publisher with this attempts value, otherwise
// nothing changes and ErrLeaseLost comes back.
//
// An expired lease does not matter as long as nobody has reclaimed the event:
// the delivery really happened, so recording it is right. If another
// publisher did reclaim it, this call is rejected and the event may be
// delivered a second time. That is the at-least-once guarantee.
func MarkPublished(ctx context.Context, q Querier, id int64, publisher string, attempts int) error {
	tag, err := q.Exec(ctx, `
		UPDATE outbox
		SET published_at = now(), lease_owner = NULL, lease_expires_at = NULL
		WHERE id = $1 AND published_at IS NULL AND dead_at IS NULL
		  AND lease_owner = $2 AND attempts = $3`,
		id, publisher, attempts)
	if err != nil {
		return fmt.Errorf("mark published: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// FailOutbox records that a delivery attempt failed. Fenced like MarkPublished.
//
// A permanent failure, or a retryable one with no attempts left, makes the
// event dead. Otherwise it becomes due again Delay from now on the database
// clock. A zero or negative delay means due at once.
//
// It is one statement, so a rejected call leaves no trace.
func FailOutbox(ctx context.Context, q Querier, id int64, publisher string, attempts int, p FailParams) (FailOutcome, error) {
	permanent := p.Class == ClassPermanent
	delay := p.Delay
	if delay < 0 {
		delay = 0
	}
	msg := cleanErrorText(p.Message, maxErrorText)

	var dead bool
	err := q.QueryRow(ctx, `
		UPDATE outbox
		SET dead_at = CASE WHEN $4::boolean OR attempts >= max_attempts THEN now() END,
		    next_attempt_at = CASE WHEN $4::boolean OR attempts >= max_attempts
		                           THEN next_attempt_at
		                           ELSE now() + make_interval(secs => $5::float8) END,
		    lease_owner = NULL,
		    lease_expires_at = NULL,
		    last_error = $6::text
		WHERE id = $1 AND published_at IS NULL AND dead_at IS NULL
		  AND lease_owner = $2 AND attempts = $3
		RETURNING dead_at IS NOT NULL`,
		id, publisher, attempts, permanent, delay.Seconds(), msg).Scan(&dead)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrLeaseLost
	}
	if err != nil {
		return 0, fmt.Errorf("fail outbox: %w", err)
	}
	if dead {
		return FailDead, nil
	}
	return FailRetried, nil
}

// ReapOutbox dead-letters events that used every attempt without ever
// reporting a failure (the publisher crashed or timed out each time) and whose
// lease has run out. ClaimOutbox never hands such events out again, so without
// this they would sit in limbo, neither delivered nor dead. It handles at most
// limit events and returns how many it dead-lettered.
func ReapOutbox(ctx context.Context, q Querier, limit int) (int, error) {
	if limit < 1 {
		return 0, errors.New("reap outbox: limit must be at least 1")
	}
	tag, err := q.Exec(ctx, `
		WITH exhausted AS (
			SELECT id FROM outbox
			WHERE published_at IS NULL AND dead_at IS NULL
			  AND attempts >= max_attempts
			  AND (lease_expires_at IS NULL OR lease_expires_at <= now())
			ORDER BY id
			FOR UPDATE SKIP LOCKED
			LIMIT $1::int
		)
		UPDATE outbox o
		SET dead_at = now(), lease_owner = NULL, lease_expires_at = NULL,
		    last_error = 'lease expired'
		FROM exhausted e
		WHERE o.id = e.id`, limit)
	if err != nil {
		return 0, fmt.Errorf("reap outbox: %w", err)
	}
	return int(tag.RowsAffected()), nil
}