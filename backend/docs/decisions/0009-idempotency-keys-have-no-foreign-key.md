# 0009: Idempotency keys have no foreign key to jobs

Status: accepted

## Context
`idempotency_keys.job_id` referenced `jobs(id) ON DELETE CASCADE`. Archiving
(M4) deletes finished jobs. With the cascade, a job's key vanishes with it, so a
late client retry creates a duplicate job. Without the cascade, the foreign key
blocks the delete. Either way the key's lifetime was tied to the job's.

## Decision
Drop the foreign key. `job_id` is a plain uuid. A key lives until a separate
purge deletes it, based on `idempotency_keys.created_at` (indexed since 002).

## Consequences
- The idempotency guarantee holds only while the key row exists. The retention
  window must be longer than the longest period in which a client may retry the
  same request. The value is decided with the archival policy (M4) and recorded
  in docs/guarantees.md and limitations.md.
- A duplicate submission after the job was archived (but before its key is
  purged) returns the original job ID, and looking that ID up finds no job.
  This is deliberate: it still prevents a second job.
- Nothing enforces that a key points at a real job. The only writer is
  SubmitIdempotent, which inserts key and job in one statement. A test checks
  that no key is ever created without its job.

## Rejected
- Purge keys before deleting jobs: every archive path would need a two-table
  delete in the right order, and one missed path would block archiving.
- Keep ON DELETE CASCADE: duplicates after archival.