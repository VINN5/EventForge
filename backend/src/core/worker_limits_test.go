package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestClassified(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain error", errBoom, false},
		{"wrapped plain error", fmt.Errorf("sending: %w", errBoom), false},
		{"retryable", Retryable(errBoom), true},
		{"retryable after", RetryableAfter(errBoom, time.Second), true},
		{"permanent", Permanent(errBoom), true},
		{"wrapped permanent", fmt.Errorf("sending: %w", Permanent(errBoom)), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classified(tc.err); got != tc.want {
				t.Fatalf("Classified = %v, want %v", got, tc.want)
			}
		})
	}
}

func minutesUntilRun(t *testing.T, q Querier, id string) float64 {
	t.Helper()
	var m float64
	err := q.QueryRow(context.Background(), `
		SELECT EXTRACT(EPOCH FROM (run_at - now()))::float8 / 60
		FROM jobs WHERE id = $1`, id).Scan(&m)
	if err != nil {
		t.Fatalf("read run_at: %v", err)
	}
	return m
}

func waitUntilWaitingForRetry(t *testing.T, q Querier, id string) {
	t.Helper()
	waitFor(t, 10*time.Second, func() bool {
		got := readJob(t, q, id)
		return got.State == "queued" && got.Attempts == 1
	}, "the job to fail once and wait for its retry")
}

// A downstream that asks for two days is not obeyed literally.
func TestWorker_RetryAfterIsCappedAtTheCeiling(t *testing.T) {
	tests := []struct {
		name    string
		ceiling time.Duration // 0 means the default
		minMin  float64
		maxMin  float64
	}{
		{"default ceiling is one hour", 0, 50, 70},
		{"custom ceiling", 2 * time.Hour, 110, 130},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool := testPool(t)
			id := submitNoop(t, pool, SubmitParams{})

			handlers := map[string]Handler{
				"noop": func(ctx context.Context, job Job) error {
					return RetryableAfter(errors.New("rate limited"), 48*time.Hour)
				},
			}
			cfg := fastRetryConfig("w1")
			cfg.MaxRetryAfter = tc.ceiling
			cancel, done := startWorker(t, pool, cfg, handlers)
			waitUntilWaitingForRetry(t, pool, id)
			stopWorker(t, cancel, done)

			if m := minutesUntilRun(t, pool, id); m < tc.minMin || m > tc.maxMin {
				t.Fatalf("run_at is %.1f minutes away, want between %.0f and %.0f", m, tc.minMin, tc.maxMin)
			}
		})
	}
}

// Retry-After is a minimum. If the computed backoff is longer, the backoff wins.
func TestWorker_BackoffWinsWhenLongerThanRetryAfter(t *testing.T) {
	pool := testPool(t)
	id := submitNoop(t, pool, SubmitParams{})

	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error {
			return RetryableAfter(errors.New("rate limited"), time.Second)
		},
	}
	cfg := fastRetryConfig("w1")
	cfg.BackoffBase = 10 * time.Minute
	cfg.BackoffCap = 10 * time.Minute
	cfg.Rand = func() float64 { return 0.5 } // full jitter: half of ten minutes
	cancel, done := startWorker(t, pool, cfg, handlers)
	waitUntilWaitingForRetry(t, pool, id)
	stopWorker(t, cancel, done)

	if m := minutesUntilRun(t, pool, id); m < 4 || m > 6 {
		t.Fatalf("run_at is %.1f minutes away, want about 5", m)
	}
}

func TestWorker_UnclassifiedErrorsGiveUpEarly(t *testing.T) {
	pool := testPool(t)
	id := submitNoop(t, pool, SubmitParams{MaxAttempts: 8})

	var calls atomic.Int32
	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error {
			calls.Add(1)
			return errors.New("mystery failure")
		},
	}
	cfg := fastRetryConfig("w1")
	cfg.UnclassifiedMaxAttempts = 2
	cancel, done := startWorker(t, pool, cfg, handlers)
	waitFor(t, 10*time.Second, func() bool {
		return readJob(t, pool, id).State == "dead"
	}, "the job to be dead-lettered after two unclassified failures")
	time.Sleep(200 * time.Millisecond) // a wrongly retried job would run again here
	stopWorker(t, cancel, done)

	if got := readJob(t, pool, id); got.State != "dead" || got.Attempts != 2 || calls.Load() != 2 {
		t.Fatalf("row %+v, handler calls %d; want dead after 2 attempts and 2 calls", got, calls.Load())
	}
	if a := readAttempt(t, pool, id, 1); a.Outcome != "retryable_failure" {
		t.Fatalf("attempt 1: %+v, want a retryable failure", a)
	}
	a := readAttempt(t, pool, id, 2)
	if a.Outcome != "permanent_failure" ||
		!strings.Contains(a.Error, "unclassified") || !strings.Contains(a.Error, "mystery failure") {
		t.Fatalf("attempt 2: %+v, want a permanent failure that names both the cap and the error", a)
	}
}

// An error that someone marked retryable is trusted: only max_attempts stops it.
func TestWorker_ExplicitlyRetryableErrorsAreNotCappedEarly(t *testing.T) {
	pool := testPool(t)
	id := submitNoop(t, pool, SubmitParams{MaxAttempts: 4})

	var calls atomic.Int32
	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error {
			calls.Add(1)
			return Retryable(errors.New("flaky downstream"))
		},
	}
	cfg := fastRetryConfig("w1")
	cfg.UnclassifiedMaxAttempts = 2
	cancel, done := startWorker(t, pool, cfg, handlers)
	waitFor(t, 10*time.Second, func() bool {
		return readJob(t, pool, id).State == "dead"
	}, "the job to die after max_attempts")
	time.Sleep(200 * time.Millisecond)
	stopWorker(t, cancel, done)

	if got := readJob(t, pool, id); got.Attempts != 4 || calls.Load() != 4 {
		t.Fatalf("row %+v, handler calls %d; want 4 attempts and 4 calls", got, calls.Load())
	}
	if a := readAttempt(t, pool, id, 4); a.Outcome != "retryable_failure" {
		t.Fatalf("attempt 4: %+v, want retryable_failure (ended by max_attempts)", a)
	}
}
