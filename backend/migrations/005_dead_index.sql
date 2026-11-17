-- +goose Up
-- Serves ListDead: dead jobs, newest first. Partial, so it stays tiny.
CREATE INDEX jobs_dead ON jobs (finished_at DESC, id DESC) WHERE state = 'dead';

-- +goose Down
DROP INDEX jobs_dead;