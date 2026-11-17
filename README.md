# EventForge

**A PostgreSQL-backed job and event engine with durable execution, strong delivery guarantees, and a published failure suite.**

EventForge is designed for small teams that need background work to survive crashes, retry sensibly, and stay fully inspectable, without operating Kafka, Temporal, or a managed broker.

> EventForge is a PostgreSQL-backed job and event engine for small teams that need background work to survive crashes, retry sensibly and be inspectable, with a published failure suite that proves its delivery guarantees.

---

## Why EventForge?

Modern applications often need to do work *after* a business transaction succeeds: send an email, create an invoice, notify another service. Doing that work inline makes requests slow and fragile. Doing it in a fire-and-forget background thread risks silent data loss on a crash.

EventForge addresses this by:

- Writing jobs into PostgreSQL **in the same transaction** as your business data (transactional outbox pattern).
- Using **leases and fencing tokens**, so workers can be killed at any moment without losing jobs or double-acknowledging them. Handlers must be idempotent for effects to happen once.
- Providing an **audited dead-letter queue** and **replay**.
- Shipping a real **failure suite** (`kill_workers`, `crash_after_effect`, `db_restart`, and more) that demonstrates the guarantees instead of just claiming them.

---

## Core Guarantees

| Guarantee                    | How it is achieved                                     |
|------------------------------|--------------------------------------------------------|
| At-least-once delivery       | Leases, reaper, retries                                |
| Effectively-once (with care) | At-least-once delivery plus idempotent handlers keyed by job ID |
| No lost jobs on crash        | Lease expiry and the reaper                            |
| No silent double effects     | Fencing tokens on ack, plus idempotent handlers        |
| Inspectable failures         | Dead-letter queue and full attempt history             |
| Audited replay               | Replay records the actor and reason in `audit_log`     |

See [`backend/docs/guarantees.md`](backend/docs/guarantees.md) for the precise claims and the tests behind each one, and [`backend/docs/limitations.md`](backend/docs/limitations.md) for what is not guaranteed.

---

## Features

### Implemented
- Durable job submission, with optional idempotent submission by key
- Worker claiming with `FOR UPDATE SKIP LOCKED`
- Leases, heartbeats and fencing tokens
- Reaper for expired leases
- Exponential backoff with full jitter
- Retryable vs permanent error classification
- Per-attempt history (`job_attempts`)
- Dead-letter queue with filtered listing and audited replay (single and batch)
- Transactional outbox with a leased publisher
- Handler idempotency helper (`RunOnce`)
- Failure suite harnesses (see below)

### Planned
- HTTP API, authentication and tenant isolation
- Per-key ordering
- OpenTelemetry metrics, logs and traces
- Per-tenant fairness / weighted round-robin
- Circuit breakers and per-downstream concurrency limits
- Multi-step workflows with compensating actions (sagas)
- Minimal admin UI (React)

---

## Failure Suite

Each experiment runs real processes against a real PostgreSQL instance, uses a seed so it can be reproduced, and includes a negative control where it makes sense.

| Experiment           | What it tests                                         | Status  |
|----------------------|-------------------------------------------------------|---------|
| `kill_workers`       | Random hard kills of worker processes under load      | Passing |
| `crash_after_effect` | Effect succeeds, process dies before ack              | Passing |
| `db_restart`         | PostgreSQL stopped or killed and restarted mid-run    | Passing |
| `outbox_crash`       | Publisher dies after send, before marking as published | Passing |
| `poison_job`         | Jobs that always crash the worker                     | Passing |
| `partition_worker`   | Worker alive but cut off from the database            | Planned |
| `clock_skew`         | Worker clocks that disagree with the database         | Planned |
| `noisy_neighbor`     | One tenant floods the queue                           | Planned |

Run summaries (seed, parameters and the final `RESULT` line) are kept under `backend/results/`.

---

## Project Structure

```
EventForge/
├── backend/                  # Go service
│   ├── cmd/
│   │   ├── eventforge/       # API + worker binary
│   │   └── loadtest/         # Load and failure testing tool
│   ├── src/
│   │   └── core/             # Queue, leases, reaper, outbox, replay (tests beside code)
│   ├── migrations/           # SQL migrations (goose)
│   ├── scripts/              # Failure experiment scripts (PowerShell)
│   ├── results/              # Outputs from failure runs
│   └── docs/                 # Guarantees, limitations, decision records
├── frontend/                 # Admin UI (planned): Vite + React + TypeScript
└── README.md
```

---

## Quick Start

### Prerequisites
- Go (see `backend/go.mod` for the minimum version)
- PostgreSQL 15 or newer (developed against 17)
- Docker, to run PostgreSQL locally
- [goose](https://github.com/pressly/goose) for migrations
- Node.js 20+ (only for the planned frontend)

### Backend

```bash
cd backend
cp .env.example .env            # set DATABASE_URL and TEST_DATABASE_URL
docker compose up -d            # starts PostgreSQL
goose -dir migrations postgres "$DATABASE_URL" up
go test ./src/core/...
```

On Windows PowerShell, load the environment first with `. .\scripts\env.ps1`.

### Frontend (planned)

```bash
cd frontend
npm install
npm run dev
```

---

## Running the Failure Suite

The harness scripts live in `backend/scripts/`, for example:

```powershell
.\scripts\crash-after-effect.ps1
.\scripts\outbox-crash.ps1
.\scripts\db-restart.ps1 -Mode stop -Seed 1
```

Each run writes its logs and result summary to `backend/results/<experiment>_seed<N>/`.

---

## Status

EventForge is under active development, following the EventForge Build Guide. The core queue, lease, reaper, retry, dead-letter, outbox and replay paths are implemented and covered by tests and failure experiments.

Current focus:
- Hardening the lease, fencing and reaper path (`partition_worker`)
- Expanding the failure suite
- Making results reproducible and publishable

---

## License

MIT

## Acknowledgments

Inspired by the need for a small, readable, PostgreSQL-native alternative to heavy orchestration systems, with guarantees that can be demonstrated rather than just claimed.
