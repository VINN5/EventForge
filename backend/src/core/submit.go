package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Querier is satisfied by both *pgxpool.Pool and pgx.Tx, so Submit can
// run on its own or inside the caller's transaction.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

type SubmitParams struct {
	TenantID    string
	Queue       string // default "default"
	Type        string
	Downstream  string
	Payload     json.RawMessage
	Priority    int
	RunAfter    time.Duration // delay, measured on the database clock
	OrderingKey string
	MaxAttempts int // default 8
}

// validateSubmit checks the parts of a request that do not need the database.
// A zero MaxAttempts or RunAfter means "use the default / run now"; negative
// values are mistakes and are rejected.
func validateSubmit(p SubmitParams) error {
	if p.TenantID == "" {
		return errors.New("submit: tenant is required")
	}
	if p.Type == "" {
		return errors.New("submit: type is required")
	}
	if !json.Valid(p.Payload) {
		return errors.New("submit: payload must be valid JSON")
	}
	if p.MaxAttempts < 0 {
		return errors.New("submit: max attempts must not be negative")
	}
	if p.RunAfter < 0 {
		return errors.New("submit: delay must not be negative")
	}
	return nil
}

// Submit stores a job and returns its ID.
func Submit(ctx context.Context, q Querier, p SubmitParams) (string, error) {
	if err := validateSubmit(p); err != nil {
		return "", err
	}
	if p.Queue == "" {
		p.Queue = "default"
	}
	if p.MaxAttempts == 0 {
		p.MaxAttempts = 8
	}

	var id string
	err := q.QueryRow(ctx, `
		INSERT INTO jobs (tenant_id, queue, type, downstream, payload,
		                  priority, run_at, ordering_key, max_attempts)
		VALUES ($1, $2, $3, NULLIF($4, ''), $5,
		        $6, now() + make_interval(secs => $7), NULLIF($8, ''), $9)
		RETURNING id`,
		p.TenantID, p.Queue, p.Type, p.Downstream, p.Payload,
		p.Priority, p.RunAfter.Seconds(), p.OrderingKey, p.MaxAttempts,
	).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("submit: %w", err)
	}
	return id, nil
}