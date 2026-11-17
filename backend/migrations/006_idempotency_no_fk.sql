-- +goose Up
-- Keys expire on their own retention window, not when their job is archived.
-- Without this, deleting an archived job is blocked (or silently takes its key with it).
ALTER TABLE idempotency_keys DROP CONSTRAINT idempotency_keys_job_id_fkey;

-- +goose Down
-- Fails if any key points at a job that has been deleted.
ALTER TABLE idempotency_keys
    ADD CONSTRAINT idempotency_keys_job_id_fkey
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE;