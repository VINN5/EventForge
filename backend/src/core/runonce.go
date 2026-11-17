package core

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// TxBeginner is satisfied by *pgxpool.Pool (starts a real transaction) and by
// pgx.Tx (starts a savepoint inside the caller's transaction).
type TxBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// RunOnce runs effect at most once for the pair (jobID, name), so a handler
// that is retried or run twice does not repeat its database effect.
//
// It inserts a marker row into job_effects and runs effect in the same
// transaction. Both commit together or neither does:
//   - marker already present (committed by an earlier run): effect is skipped,
//     ran=false, err=nil.
//   - effect returns an error or panics: everything rolls back, including the
//     marker, so a retry runs the effect again. The error is returned
//     unchanged, so the worker's retryable/permanent classification still
//     works. A panic continues upward after the rollback.
//   - another caller is running the same (jobID, name) right now: the marker
//     insert waits for that transaction. If it commits, this call skips; if it
//     rolls back, this call runs the effect.
//
// What this covers: effects written through tx, in the same database. It
// cannot make a call to an outside service atomic. For those, pass the job ID
// as the provider's idempotency key, and list the handler in limitations.md if
// the provider has none.
//
// Keep effect short and database-only: it holds a transaction and a pool
// connection while it runs.
//
// If Commit itself fails, the outcome is unknown (the commit may have reached
// the server). RunOnce reports the error; the job is retried, and the marker
// then tells the truth about whether the effect happened.
func RunOnce(ctx context.Context, db TxBeginner, jobID, name string,
	effect func(ctx context.Context, tx pgx.Tx) error) (bool, error) {

	if jobID == "" || name == "" {
		return false, errors.New("run once: job id and name are required")
	}
	if effect == nil {
		return false, errors.New("run once: effect is required")
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("run once: begin: %w", err)
	}

	committed := false
	defer func() {
		if committed {
			return
		}
		// Runs on every early return and on a panic. Use a context that
		// survives cancellation of ctx, or the rollback itself could be
		// skipped and leave the transaction open.
		rbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rbCtx)
	}()

	// DO NOTHING means "someone already committed this marker". Skipping is
	// right whether or not we could see that row ourselves, so this does not
	// depend on the isolation level.
	tag, err := tx.Exec(ctx, `
		INSERT INTO job_effects (job_id, name) VALUES ($1::uuid, $2)
		ON CONFLICT (job_id, name) DO NOTHING`, jobID, name)
	if err != nil {
		return false, fmt.Errorf("run once: marker: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil // already done; the deferred rollback ends our empty transaction
	}

	if err := effect(ctx, tx); err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("run once: commit: %w", err)
	}
	committed = true
	return true, nil
}