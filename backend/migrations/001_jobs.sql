-- +goose Up
CREATE TABLE jobs (
    id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id         text        NOT NULL,
    queue             text        NOT NULL DEFAULT 'default',
    type              text        NOT NULL,
    downstream        text,
    payload           jsonb       NOT NULL,
    payload_version   int         NOT NULL DEFAULT 1,
    state             text        NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued','running','succeeded','dead','cancelled')),
    priority          int         NOT NULL DEFAULT 0,
    run_at            timestamptz NOT NULL DEFAULT now(),
    ordering_key      text,
    seq               bigint      GENERATED ALWAYS AS IDENTITY,
    attempts          int         NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts      int         NOT NULL DEFAULT 8 CHECK (max_attempts >= 1),
    lease_owner       text,
    lease_expires_at  timestamptz,
    last_error        text,
    error_fingerprint text,
    workflow_id       uuid,
    step_name         text,
    created_at        timestamptz NOT NULL DEFAULT now(),
    finished_at       timestamptz,
    CHECK ((state = 'running') = (lease_owner IS NOT NULL)),
    CHECK ((state = 'running') = (lease_expires_at IS NOT NULL))
) WITH (fillfactor = 80);

CREATE INDEX jobs_ready ON jobs (queue, priority DESC, run_at, id)
    WHERE state = 'queued';

CREATE INDEX jobs_lease ON jobs (lease_expires_at)
    WHERE state = 'running';

CREATE INDEX jobs_key ON jobs (ordering_key, seq)
    WHERE ordering_key IS NOT NULL AND state IN ('queued','running');

-- +goose Down
DROP TABLE jobs;