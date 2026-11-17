# 0010: The outbox publisher claims rows with a lease

Status: accepted

## Context
The publisher must deliver outbox rows to a broker or webhook. A delivery is a
network call of unknown duration.

## Decision
Claim rows in a short transaction (FOR UPDATE SKIP LOCKED), stamping an owner
and a lease expiry, then deliver OUTSIDE any transaction, then mark the row
published in a second short statement. Expired leases are reclaimed by the next
claim, so no separate reaper is needed for crashes. Rows that exhaust their
attempts without reporting a failure are dead-lettered by one small statement.
Retries use backoff with jitter, as jobs do.

The fencing token is `attempts`: a claim increments it, and mark and fail
require owner and attempts to match. This is valid only because outbox rows are
never released (attempts never decreases). If a release is ever added, add a
separate fence column as the jobs table has.

## Consequences
- Delivery is at least once. A publisher that crashes after sending, or whose
  lease expires mid-send, causes a second delivery. Consumers deduplicate on
  event_id. This is in the guarantees table.
- Recovery after a publisher crash takes one lease length.
- Every crash or timeout costs an attempt; an event that keeps failing ends up
  dead and is visible to operators.
- No ordering guarantee across events, and none within a topic once more than
  one publisher runs. Per-key ordering would need its own design.
- Published and dead rows need a retention purge (M4, with archival).

## Rejected
- Hold the claiming transaction open during delivery: a slow broker holds a
  connection and a transaction open (blocking vacuum), and a permanently
  failing event cannot back off without the extra columns this option was
  meant to avoid.