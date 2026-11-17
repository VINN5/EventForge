-- +goose Up
ALTER TABLE outbox
    ADD COLUMN max_attempts     INT         NOT NULL DEFAULT 10,
    ADD COLUMN lease_owner      TEXT,
    ADD COLUMN lease_expires_at TIMESTAMPTZ,
    ADD COLUMN next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN last_error       TEXT,
    ADD COLUMN dead_at          TIMESTAMPTZ;

ALTER TABLE outbox
    ADD CONSTRAINT outbox_max_attempts_positive CHECK (max_attempts > 0),
    ADD CONSTRAINT outbox_lease_consistent CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL)),
    ADD CONSTRAINT outbox_final_exclusive CHECK (published_at IS NULL OR dead_at IS NULL);

-- The claim query: due, not finished, ordered by when it becomes due.
CREATE INDEX outbox_due ON outbox (next_attempt_at, id)
    WHERE published_at IS NULL AND dead_at IS NULL;

-- +goose Down
DROP INDEX IF EXISTS outbox_due;
ALTER TABLE outbox
    DROP CONSTRAINT IF EXISTS outbox_final_exclusive,
    DROP CONSTRAINT IF EXISTS outbox_lease_consistent,
    DROP CONSTRAINT IF EXISTS outbox_max_attempts_positive;
ALTER TABLE outbox
    DROP COLUMN IF EXISTS dead_at,
    DROP COLUMN IF EXISTS last_error,
    DROP COLUMN IF EXISTS next_attempt_at,
    DROP COLUMN IF EXISTS lease_expires_at,
    DROP COLUMN IF EXISTS lease_owner,
    DROP COLUMN IF EXISTS max_attempts;