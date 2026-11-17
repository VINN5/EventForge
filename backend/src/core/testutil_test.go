package core

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testPool connects to the test database and empties every table,
// so each test starts from a clean state.
//
// A missing TEST_DATABASE_URL is a failure, not a skip: a suite that silently
// skips its database tests still reports "ok". To skip them on purpose, set
// EVENTFORGE_SKIP_DB_TESTS=1.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		if os.Getenv("EVENTFORGE_SKIP_DB_TESTS") == "1" {
			t.Skip("EVENTFORGE_SKIP_DB_TESTS=1; skipping database test")
		}
		t.Fatal("TEST_DATABASE_URL is not set: run `. .\\scripts\\env.ps1` first " +
			"(or set EVENTFORGE_SKIP_DB_TESTS=1 to skip database tests on purpose)")
	}
	if !strings.Contains(url, "eventforge_test") {
		t.Fatal("refusing to run: TEST_DATABASE_URL must point at the eventforge_test database")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

		_, err = pool.Exec(ctx, `TRUNCATE jobs, job_attempts, idempotency_keys,
		outbox, tenant_limits, audit_log, job_effects RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("reset tables: %v", err)
	}
	return pool
}
