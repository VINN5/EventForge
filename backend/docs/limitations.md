# Limitations

What does not work, and what is not yet tested. Updated with each milestone.

## Not yet tested

- `partition_worker`: a worker cut off from the database. Heartbeat and lease
  rules are designed for it; the experiment has not been run.
- `clock_skew`, `soak_24h` and a run at the guide's scale (1M jobs, 500
  kills). `kill_workers` passed on four hard-kill runs (15, 15, 15 and 40
  kills) and two poison runs (seeds 11 and 12). `db_restart` passed in process
  (layer 1) and against the real container (layer 2: one seed per mode, 5
  restarts each, 4 workers, 3 s lease, handlers of 5 ms). A handler that runs
  longer than an outage was not part of those runs.
- Benchmarks (throughput, latency percentiles): none yet.
- Two idempotent submits taking keys in opposite orders can deadlock; Postgres
  aborts one with error 40P01 and the caller must retry its transaction. This
  is expected behaviour, but it is not tested.

## Known behaviour that costs something

- Shutdown does not release a job or outbox event that is mid-flight. It stays
  leased until the lease expires, and that costs one attempt.
- A poison job that kills its worker also strands the healthy jobs that worker
  held, until their leases expire. That slowdown is not measured.
- Replaying a dead job restores its full set of attempts. A poison job will
  crash workers again, up to `max_attempts` times.
- Outbox recovery after a publisher crash takes one lease length. Every crash
  or timeout costs the event an attempt.
- The outbox publisher waits for the slowest delivery in a batch before it
  claims again.
- When a database connection is lost, Ack, Fail, MarkPublished and FailOutbox
  are retried a few times (5 tries, short waits of at most 1 s each). Layer 1
  (a session killed every 100 ms) measured 0 repeated handler runs per 60 jobs
  in 6 runs, and 0 or 1 duplicate deliveries per 40 events (1 in 2 of the 6
  runs); before the retry it was 16-18 and 12-18. An outage longer than the
  retry budget still falls back to lease expiry: the job or event runs again
  after its lease, and each repeat costs an attempt, so a long outage could
  dead-letter healthy jobs that have few attempts.    A real restart (outages of 6 to 17 s with a 3 s lease) is far longer than that
  budget: the reports in flight were lost and those jobs ran again after their
  lease, about 8 to 11 repeats per restart with 32 jobs in flight
  (`results/db_restart_stop_seed1`, `results/db_restart_kill_seed1`).
- A Claim or ClaimOutbox whose reply is lost after the database committed it
  leaves those jobs or events leased to a caller that never saw them, until the
  lease expires.
- A handler error that is a lost or refused connection (to the database or to
  an outside service) is not subject to the early cap for unmarked errors
  (UnclassifiedMaxAttempts); only the job's max_attempts stops it. A job that
  fails this way on every attempt therefore uses all of its attempts before it
  is dead-lettered.


## Guarantee limits

- Idempotent submission only works while the key row exists. Keys have no
  retention purge yet (decision record 0009); the window must be set to exceed
  the longest period in which a client may retry.
- `RunOnce` markers have no retention purge yet.
- `RunOnce` covers effects written through its transaction, in the same
  database. For an outside service, the job id must be the provider's
  idempotency key; if the provider has none, that handler is not idempotent and
  must be listed as such.
- A failed `Commit` inside `RunOnce` leaves the outcome unknown; the job is
  retried, and the marker then tells the truth.
- The request hash treats `1`, `1.0` and `1e2` as different payloads (a false
  conflict, never a false match). A lone surrogate escape (`\ud800`) is
  replaced by Go's decoder and is not covered by the golden tests; Postgres
  rejects it at insert.
- The outbox has no ordering guarantee, within a topic or across them.
- Deduplication is the consumer's job. The `outbox_crash` run applies the
  deliveries after the publishers stop; it does not race two consumers on one
  duplicate. `RunOnce`'s own concurrency tests cover that race.
- `outbox_crash` exercises a crash after the send and before the mark. A crash
  before the send leaves nothing delivered and is covered by the claim and reap
  tests.

## Operations

- Worker and publisher ids need a random suffix in production; the load tester
  uses unique incarnation ids.
- If worker versions are ever mixed with a large backlog, `ClaimTypes` needs the
  job type in an index.
- Finished jobs, published outbox events, idempotency keys and effect markers
  accumulate: archival and retention are not built (M4).
- Tests must run with `go test -p 1` once another package uses the test
  database. The slowest test is `TestProperty_QueueInvariants` (about 26 s).

## Not built

- The HTTP API, authentication, tenant isolation (row-level security), payload
  size limits and rate limits (guide chapters 6 and 12).
- Metrics and traces (M4), concurrency limits and the circuit breaker (M4),
  per-key ordering, per-tenant fairness and workflows (M6), the AI layer (M5).
