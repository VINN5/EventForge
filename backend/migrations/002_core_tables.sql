-- +goose Up
CREATE TABLE job_attempts (
    job_id      UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt     INT  NOT NULL CHECK (attempt >= 1),
    worker      TEXT,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at    TIMESTAMPTZ,
    outcome     TEXT CHECK (outcome IN
        ('succeeded','retryable_failure','permanent_failure','lease_expired','released')),
    error       TEXT,
    duration_ms INT,
    PRIMARY KEY (job_id, attempt)
);

CREATE TABLE idempotency_keys (
    client_id     TEXT NOT NULL,
    key           TEXT NOT NULL,
    request_hash  TEXT NOT NULL,
    job_id        UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (client_id, key)
);

CREATE INDEX idempotency_keys_created ON idempotency_keys (created_at);

CREATE TABLE outbox (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic         TEXT NOT NULL,
    payload       JSONB NOT NULL,
    event_id      UUID NOT NULL UNIQUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ,
    attempts      INT NOT NULL DEFAULT 0
);

CREATE INDEX outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;

CREATE TABLE tenant_limits (
    tenant_id    TEXT PRIMARY KEY,
    max_running  INT NOT NULL CHECK (max_running > 0),
    weight       INT NOT NULL DEFAULT 1 CHECK (weight > 0)
);

CREATE TABLE audit_log (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor      TEXT NOT NULL,
    action     TEXT NOT NULL,
    subject    TEXT,
    reason     TEXT,
    details    JSONB
);

-- +goose Down
DROP TABLE IF EXISTS audit_log;
DROP TABLE IF EXISTS tenant_limits;
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS idempotency_keys;
DROP TABLE IF EXISTS job_attempts;