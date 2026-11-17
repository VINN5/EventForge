package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ErrDuplicateEvent means an event with the caller's EventID is already in the
// outbox. The existing event is left untouched.
var ErrDuplicateEvent = errors.New("outbox event id already exists")

const maxOutboxTopicBytes = 255

// OutboxEvent is one event to publish.
type OutboxEvent struct {
	Topic   string
	Payload json.RawMessage
	// EventID is optional. Leave it empty to have one generated. Consumers
	// deduplicate on it, so a caller that wants a retried business operation
	// to produce the same event can supply a deterministic id.
	EventID string
}

// Enqueue adds an event to the outbox and returns its event id.
//
// Pass the application's transaction as q and the event is committed or rolled
// back together with the application's own writes. Pass a pool and it is
// stored on its own. Either way nothing is published here: the publisher
// delivers it later, at least once.
//
// A duplicate EventID returns ErrDuplicateEvent without aborting the caller's
// transaction (the insert uses ON CONFLICT DO NOTHING instead of failing).
func Enqueue(ctx context.Context, q Querier, e OutboxEvent) (string, error) {
	if e.Topic == "" {
		return "", errors.New("enqueue: topic is required")
	}
	if len(e.Topic) > maxOutboxTopicBytes {
		return "", fmt.Errorf("enqueue: topic must be at most %d bytes", maxOutboxTopicBytes)
	}
	if !json.Valid(e.Payload) { // also false for an empty payload
		return "", errors.New("enqueue: payload must be valid JSON")
	}
	if e.EventID != "" && !isUUID(e.EventID) {
		return "", errors.New("enqueue: event id must be a uuid")
	}

	var id string
	err := q.QueryRow(ctx, `
		INSERT INTO outbox (topic, payload, event_id)
		VALUES ($1::text, $2::jsonb, COALESCE(NULLIF($3::text, '')::uuid, gen_random_uuid()))
		ON CONFLICT (event_id) DO NOTHING
		RETURNING event_id::text`,
		e.Topic, e.Payload, e.EventID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrDuplicateEvent
	}
	if err != nil {
		return "", fmt.Errorf("enqueue: %w", err)
	}
	return id, nil
}