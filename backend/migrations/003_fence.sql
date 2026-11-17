-- +goose Up
ALTER TABLE jobs ADD COLUMN fence bigint NOT NULL DEFAULT 0;

-- Rows that already exist keep their current token, so in-flight work stays valid.
UPDATE jobs SET fence = attempts;

-- attempts can go down (release); fence never does. So fence is never below attempts.
ALTER TABLE jobs ADD CONSTRAINT jobs_fence_ge_attempts CHECK (fence >= attempts);

-- +goose Down
ALTER TABLE jobs DROP CONSTRAINT jobs_fence_ge_attempts;
ALTER TABLE jobs DROP COLUMN fence;