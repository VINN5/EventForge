package core

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// A handler that returns a lost-connection error did not fail because of its
// job. A database outage must not use up the small attempt budget that exists
// for unexplained errors (WorkerConfig.UnclassifiedMaxAttempts).

func TestFailureFor_ConnectionErrorsAreNotCappedAsUnclassified(t *testing.T) {
	w, err := NewWorker(nil,
		WorkerConfig{ID: "w1", UnclassifiedMaxAttempts: 3, Rand: zeroRand},
		map[string]Handler{"noop": func(ctx context.Context, job Job) error { return nil }},
		quietLogger())
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	cases := []struct {
		name     string
		err      error
		attempts int
		want     ErrorClass
	}{
		{"a plain unmarked error is still capped on attempt 3", errors.New("boom"), 3, ClassPermanent},
		{"a lost connection stays retryable on attempt 3", errConnLost, 3, ClassRetryable},
		{"a wrapped lost connection stays retryable on attempt 5", fmt.Errorf("insert effect: %w", errConnLost), 5, ClassRetryable},
		{"a lost connection on the last allowed attempt is still a retryable class", errConnLost, 8, ClassRetryable},
		{"an explicit Permanent still wins", Permanent(errConnLost), 3, ClassPermanent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			job := Job{Attempts: tc.attempts, MaxAttempts: 8}
			if got := w.failureFor(job, tc.err).Class; got != tc.want {
				t.Fatalf("class = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestWorker_LostConnectionErrorsFromAHandlerDoNotKillTheJob(t *testing.T) {
	pool := testPool(t)
	id := submitNoop(t, pool, SubmitParams{MaxAttempts: 8})

	var calls atomic.Int32
	handlers := map[string]Handler{
		"noop": func(ctx context.Context, job Job) error {
			if calls.Add(1) <= 4 {
				return fmt.Errorf("insert effect: %w", errConnLost)
			}
			return nil
		},
	}
	cfg := fastRetryConfig("w1")
	cfg.UnclassifiedMaxAttempts = 2 // a plain error would die on attempt 2
	cancel, done := startWorker(t, pool, cfg, handlers)
	waitFor(t, 10*time.Second, func() bool {
		return readJob(t, pool, id).State == "succeeded"
	}, "the job to succeed on its fifth attempt")
	stopWorker(t, cancel, done)

	if got := readJob(t, pool, id); got.Attempts != 5 || calls.Load() != 5 {
		t.Fatalf("row %+v, handler calls %d; want 5 attempts and 5 calls", got, calls.Load())
	}
}