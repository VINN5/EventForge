# 0013: Replay restarts a dead job's attempts and audits it atomically

Status: accepted

## Context
A dead job exhausted its attempts. Putting it back must give it a chance to
succeed, leave an answer to "who replayed this, and why", and never reuse a
fencing token.

## Decision
Replay sets `attempts` to 0, `run_at` to now, clears `finished_at`, and leaves
`fence`, the attempt history and `last_error` alone. The audit row (actor,
reason, and the attempts, fence and last error at that moment) is written by the
same statement. Actor and reason are required. A batch replays oldest first with
a mandatory limit and one audit row per job, recording the filter used.

## Consequences
- The fence is separate from attempts, so tokens stay unique across replays.
- A replayed poison job gets the full allowance of attempts again.
- Only dead jobs can be replayed; anything else returns `ErrJobNotFound` and
  leaves no trace.

## Rejected
- Continuing the old attempts count: a job that died on its last attempt would
  die again on its first.
- Resetting the fence: would let a stale holder's token become valid again.