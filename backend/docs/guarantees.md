# Guarantees

What EventForge promises, how, and the evidence. Every "proven" row names a
test or a recorded run. Anything not proven is marked, and what it does not
cover is in limitations.md. EventForge offers **at-least-once delivery plus
idempotent handlers**, never exactly-once.

Tests live in `src/core`. Real-process run results are in `results/`
(re-running a script overwrites its folder; copy it first to keep a run).

## Jobs

| Guarantee | Mechanism | Evidence | Status |
|---|---|---|---|
| A submitted job is stored before the caller gets its id | The id is returned by the INSERT | `TestSubmit_InsertsQueuedJob` | Proven |
| A job can be enqueued inside the application's transaction and disappears if it rolls back | `Submit` takes a `Querier` (pool or transaction) | `TestSubmit_RolledBackWithTransaction` | Proven |
| No two workers hold one job at the same time | `FOR UPDATE SKIP LOCKED` claim | `TestClaim_ConcurrentWorkersNeverShareJobs`, `TestProperty_QueueInvariants` | Proven |
| A slow or stale worker cannot overwrite the result of the worker that took over | Fence token bumped on every claim; ack, fail and extend must match owner and fence | `TestAck_StaleWorkerCannotOverwriteTakeover`, `TestAck_SameWorkerOldFenceRejected`, `TestFail_RejectedWhenNotCurrentHolder`, `TestExtend_SameWorkerOldFenceIsLost`, `TestFence_NotReusedWhenAttemptsGoDown` | Proven |
| A job whose worker is killed is recovered | Lease expiry plus the reaper | `TestReap_RequeuesExpiredLease`, `TestWorker_FinishesJobAbandonedByDeadWorker`; `kill_workers` with hard process kills, 4 runs, all PASS: seeds 1, 2 and 3 with 15 kills each, seed 4 with 40 kills (`results/kill_workers_seed1` to `results/kill_workers_seed4`) | Proven at that scale (at most 40 kills in one run). Not run at the guide's 1M jobs and 500 kills |
| A long handler is not taken over while its worker is alive; a lost lease cancels the handler | Heartbeat extends the lease; a lost lease cancels the handler's context | `TestWorker_SlowHandlerIsNotTakenOver`, `TestWorker_LostLeaseCancelsHandler` | Proven. A worker partitioned from the database is not tested (`partition_worker`) |
| Transient failures retry with jittered backoff; permanent ones go to the dead-letter store | Typed errors, full-jitter backoff, Retry-After capped, a lower attempt cap for unclassified errors | `TestFail_RetryableSchedulesRetryWithDelay`, `TestFail_PermanentGoesDeadEvenWithAttemptsLeft`, `TestWorker_RetryAfterIsCappedAtTheCeiling`, `TestWorker_UnclassifiedErrorsGiveUpEarly` | Proven |
| A poison job ends up dead and does not stop healthy jobs | Attempt limit; a crash counts as an attempt | `TestWorker_PoisonJobsDieWithoutHurtingOthers`; the real-process poison runs, both PASS: `results/kill_workers_seed11` (2 poison jobs, no harness kills) and `results/kill_workers_seed12` (5 poison jobs, 10 kills) | Proven. Healthy jobs on a crashed worker wait out their lease (unmeasured slowdown) |
| Dead jobs can be listed and inspected with their attempt history | `ListDead` (cursor paging), `GetDead` | `TestListDead_*`, `TestGetDead_*` | Proven |
| Lease and delay timing uses the database clock | Every lease and run time is computed with `now()` in SQL | By construction in the queries | Mechanism only: the `clock_skew` experiment has not been run |
| Jobs and outbox events still finish when database sessions are killed under a running worker or publisher | Leases and the reaper recover whatever a lost connection left unreported | `TestWorker_SurvivesItsConnectionsBeingKilled`, `TestPublisher_SurvivesItsConnectionsBeingKilled` | Proven in process (one kill per 100 ms for 1.5 s): all finish, none dead or stuck, no open attempts. Retried reports keep repeats rare (0 repeats in 6 runs; 0 or 1 duplicate deliveries per 40 events).The real container restart is the next row |
| A restart of the database under load loses no job and strands none | Leases, the reaper, fenced reports, a short bounded retry | Real container, 60000 jobs, 4 workers, 3 s lease, 5 restarts each: `results/db_restart_stop_seed1` (graceful, outages 7 to 10 s: all succeeded, 0 dead, none stuck, 39 repeated effects all explained by expired leases) and `results/db_restart_kill_seed1` (crash-style kill, outages 9 to 17 s: all succeeded, 0 dead, none stuck, 57 repeats all explained). Negative control `results/db_restart_stop_seed1_noreaper` (reaper off: 118 jobs stuck, caught) | Proven for one seed per mode. Repeats are expected: about 8 to 11 per restart with 32 jobs in flight. Not tested: a handler that runs longer than the outage, and a worker cut off while the database stays up (`partition_worker`) ||

## Idempotency

| Guarantee | Mechanism | Evidence | Status |
|---|---|---|---|
| The same request submitted many times creates one job | Key and job inserted in one statement; the unique key decides | `TestSubmitIdempotent_Duplicate100Parallel` (100 parallel callers: one creator, one job, one id for everyone) | Proven in process. Not through HTTP (no API yet) |
| The same key with a different request is rejected | Request hash compared on conflict | `TestSubmitIdempotent_DifferentRequestIsAConflict`, `TestSubmitIdempotent_ParallelConflictingRequests` | Proven |
| The request hash never changes under an upgrade | Documented byte-for-byte format `v1`, own canonical JSON encoder | `TestRequestHash_Golden`, `TestRequestHash_GoldenPayloads` and the other `TestRequestHash_*` tests | Proven, within the limits in decision record 0011 |
| A duplicate waits for the first submission and a rolled-back first submission lets the duplicate create the job | `ON CONFLICT DO NOTHING` waits on the first transaction | `TestSubmitIdempotent_DuplicateBlocksUntilFirstCommits`, `TestSubmitIdempotent_BlockedDuplicateCreatesIfFirstRollsBack` | Proven |
| An effect in the application's own database happens once per job, even if the job runs several times | `RunOnce`: marker row and effect commit together | `TestRunOnce_*` (14 passes); real processes: `results/crash_after_effect_idempotent` (40 jobs, 80 crashes after the effect, 0 repeats) against the control `results/crash_after_effect_plain` (all 40 repeated) | Proven for same-database effects |
| An effect in an outside service happens once | The job id is the provider's idempotency key | Nothing in EventForge can enforce this | **Not guaranteed.** Depends on the provider |

## Outbox

| Guarantee | Mechanism | Evidence | Status |
|---|---|---|---|
| An event is saved if and only if the business change is saved | `Enqueue` takes the application's transaction | `TestEnqueue_CommitsAndRollsBackWithTheBusinessChange` | Proven |
| Every event is delivered at least once | Lease-based claim; an expired lease is reclaimed; exhausted events are dead-lettered, not lost | `TestClaimOutbox_*`, `TestPublisherStep_*`; real processes: `results/outbox_crash_idempotent` (40 events, 40 crashes between send and mark, all delivered and published) | Proven |
| A stale publisher cannot mark or fail an event another publisher owns | Fence = the row's attempts counter | `TestMarkPublished_StalePublisherIsRejected`, `TestFailOutbox_RejectedWhenNotTheCurrentHolder`, `TestPublisherStep_AStalePublisherCannotMarkAfterTakeover` | Proven |
| An event may be delivered more than once; a consumer that deduplicates on `event_id` applies each event once | Consumer-side dedupe (`RunOnce` keyed by event id) | `results/outbox_crash_idempotent`: 40 duplicate deliveries, 0 events applied twice. Control `results/outbox_crash_plain`: all 40 applied twice and caught | Proven for that consumer. Duplicates are expected, not a bug |
| A slow broker does not hold a database transaction open | Delivery happens outside any transaction; delivery timeout is shorter than the lease | `TestNewPublisher_Validation`, `TestPublisherStep_ASlowSinkTimesOut` | Proven |

## Dead-letter replay

| Guarantee | Mechanism | Evidence | Status |
|---|---|---|---|
| Replay is audited: who, when, why, and the job's state at that moment | One statement changes the job and writes the audit row | `TestReplayDead_RequeuesTheJobAndWritesTheAuditRow`, `TestReplayDeadBatch_EmptyFilterReplaysEverythingOldestFirst` | Proven |
| A rejected or rolled-back replay leaves no trace | Single statement; `ErrJobNotFound` writes nothing | `TestReplayDead_OnlyDeadJobsCanBeReplayed`, `TestReplayDead_RollsBackWithTheCallersTransaction`, `TestReplayDeadBatch_RollsBackWithTheCallersTransaction` | Proven |
| Concurrent replays never replay a job twice | `FOR UPDATE` (single) and `SKIP LOCKED` (batch) | `TestReplayDead_ConcurrentReplaysHaveExactlyOneWinner`, `TestReplayDeadBatch_ConcurrentCallersNeverReplayTheSameJobTwice` | Proven |
| A replayed job never reuses a fence token | The fence is never reset | `TestReplayDead_TheJobCanBeClaimedAgainAndTheFenceKeepsGrowing` | Proven |

## Explicitly not promised

- Exactly-once delivery across a network.
- Ordering: not across jobs, and not across outbox events.
- That an external side effect happens once (see above).
- Anything the "Not yet tested" list in limitations.md names.
