# 0012: Handler idempotency through a marker row in the same transaction

Status: accepted

## Context
A worker can finish an effect and die before acking, so the job runs again. The
guide asks for an effects row unique on job id, written with the effect.

## Decision
`RunOnce(db, jobID, name, effect)` opens a transaction, inserts a marker into
`job_effects (job_id, name)` with `ON CONFLICT DO NOTHING`, and runs the effect
in the same transaction. If the marker already exists the effect is skipped. If
the effect fails or panics, everything rolls back, including the marker.
A concurrent duplicate waits on the marker's transaction.

The key is `(job_id, name)`, not `job_id` alone, so a handler with several
separate effects can mark each. The table has no foreign key to `jobs`, so
archiving jobs never blocks on it and markers expire on their own retention.

## Consequences
- Works for effects in the same database only. For an outside service, the job
  id is the provider's idempotency key.
- The effect must be short and database-only: it holds a transaction and a pool
  connection.
- A replayed dead job whose effect already committed skips it, which is correct.
- Evidence: `TestRunOnce_*` and the real-process `crash_after_effect` run with its
  plain-insert control.

## Rejected
- Marker keyed by job id only: breaks handlers with more than one effect.
- A marker table with a foreign key: ties marker lifetime to job archival.