-- +goose Up
ALTER TABLE job_attempts ADD COLUMN fence bigint;
UPDATE job_attempts SET fence = attempt;
ALTER TABLE job_attempts ALTER COLUMN fence SET NOT NULL;
ALTER TABLE job_attempts DROP CONSTRAINT job_attempts_pkey;
ALTER TABLE job_attempts ADD PRIMARY KEY (job_id, fence);

-- +goose Down
-- Fails if releases have produced two rows with the same attempt number.
ALTER TABLE job_attempts DROP CONSTRAINT job_attempts_pkey;
ALTER TABLE job_attempts DROP COLUMN fence;
ALTER TABLE job_attempts ADD PRIMARY KEY (job_id, attempt);