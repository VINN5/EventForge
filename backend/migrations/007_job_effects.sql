-- +goose Up
CREATE TABLE job_effects (
    job_id     uuid        NOT NULL,
    name       text        NOT NULL CHECK (name <> ''),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, name)
);
-- For the retention purge: delete markers older than the retention window.
CREATE INDEX job_effects_created ON job_effects (created_at);

-- +goose Down
DROP TABLE job_effects;