package core

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

var errBoom = errors.New("boom")

func TestClassify(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantClass ErrorClass
		wantAfter time.Duration
	}{
		{"plain error is retryable", errBoom, ClassRetryable, 0},
		{"explicit retryable", Retryable(errBoom), ClassRetryable, 0},
		{"retryable with retry-after", RetryableAfter(errBoom, 7*time.Second), ClassRetryable, 7 * time.Second},
		{"explicit permanent", Permanent(errBoom), ClassPermanent, 0},
		{"wrapped permanent", fmt.Errorf("sending email: %w", Permanent(errBoom)), ClassPermanent, 0},
		{"wrapped retry-after", fmt.Errorf("calling api: %w", RetryableAfter(errBoom, 3*time.Second)), ClassRetryable, 3 * time.Second},
		{"outermost wins: retryable around permanent", Retryable(Permanent(errBoom)), ClassRetryable, 0},
		{"outermost wins: permanent around retryable", Permanent(Retryable(errBoom)), ClassPermanent, 0},
		{"negative retry-after is ignored", RetryableAfter(errBoom, -time.Second), ClassRetryable, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			class, after := Classify(tc.err)
			if class != tc.wantClass || after != tc.wantAfter {
				t.Fatalf("Classify = (%v, %v), want (%v, %v)", class, after, tc.wantClass, tc.wantAfter)
			}
		})
	}
}

func TestTypedErrors_KeepTheOriginalError(t *testing.T) {
	for name, err := range map[string]error{
		"retryable":       Retryable(errBoom),
		"retryable-after": RetryableAfter(errBoom, time.Second),
		"permanent":       Permanent(errBoom),
	} {
		if !errors.Is(err, errBoom) {
			t.Errorf("%s: errors.Is cannot see the wrapped error", name)
		}
		if !strings.Contains(err.Error(), "boom") {
			t.Errorf("%s: message %q lost the original text", name, err.Error())
		}
	}
}

func TestBackoff_GrowsThenStopsAtCap(t *testing.T) {
	half := func() float64 { return 0.5 }
	base, limit := 2*time.Second, 15*time.Second

	// With a random value of 0.5 the delay is half the ceiling.
	want := map[int]time.Duration{
		1:  1 * time.Second,         // ceiling 2s
		2:  2 * time.Second,         // ceiling 4s
		3:  4 * time.Second,         // ceiling 8s
		4:  7500 * time.Millisecond, // ceiling 16s, capped to 15s
		10: 7500 * time.Millisecond,
	}
	for attempt, w := range want {
		if got := Backoff(attempt, base, limit, half); got != w {
			t.Errorf("attempt %d: got %v, want %v", attempt, got, w)
		}
	}
}

func TestBackoff_ZeroRandomGivesZeroDelay(t *testing.T) {
	zero := func() float64 { return 0 }
	if got := Backoff(5, 2*time.Second, time.Minute, zero); got != 0 {
		t.Fatalf("got %v, want 0", got)
	}
}

func TestBackoff_HugeAttemptDoesNotOverflow(t *testing.T) {
	half := func() float64 { return 0.5 }
	for _, attempt := range []int{63, 64, 100, 1000, 1 << 30} {
		got := Backoff(attempt, 2*time.Second, 15*time.Minute, half)
		if got != 450*time.Second { // half of the 15 minute cap
			t.Errorf("attempt %d: got %v, want 7m30s", attempt, got)
		}
	}
}

func TestBackoff_AttemptBelowOneIsTreatedAsOne(t *testing.T) {
	half := func() float64 { return 0.5 }
	for _, attempt := range []int{0, -3} {
		if got := Backoff(attempt, 2*time.Second, time.Minute, half); got != time.Second {
			t.Errorf("attempt %d: got %v, want 1s", attempt, got)
		}
	}
}

func TestBackoff_RealRandomnessStaysInBounds(t *testing.T) {
	base, limit := 100*time.Millisecond, 5*time.Second
	for attempt := 1; attempt <= 12; attempt++ {
		ceiling := base << (attempt - 1)
		if ceiling > limit {
			ceiling = limit
		}
		for i := 0; i < 500; i++ {
			got := Backoff(attempt, base, limit, rand.Float64)
			if got < 0 || got > ceiling {
				t.Fatalf("attempt %d: delay %v outside [0, %v]", attempt, got, ceiling)
			}
		}
	}
}

func TestCleanErrorText(t *testing.T) {
	t.Run("short text is unchanged", func(t *testing.T) {
		if got := cleanErrorText("connection refused", 100); got != "connection refused" {
			t.Fatalf("got %q", got)
		}
	})
	t.Run("NUL bytes are removed", func(t *testing.T) {
		if got := cleanErrorText("bad\x00value", 100); got != "badvalue" {
			t.Fatalf("got %q", got)
		}
	})
	t.Run("long text is cut to the limit", func(t *testing.T) {
		got := cleanErrorText(strings.Repeat("a", 5000), 100)
		if len(got) > 100 {
			t.Fatalf("length %d, want at most 100", len(got))
		}
	})
	t.Run("cutting never splits a multi-byte character", func(t *testing.T) {
		// "é" is 2 bytes. A limit of 101 would land in the middle of one.
		got := cleanErrorText(strings.Repeat("é", 200), 101)
		if !utf8.ValidString(got) {
			t.Fatalf("result is not valid UTF-8: %q", got)
		}
		if len(got) > 101 {
			t.Fatalf("length %d, want at most 101", len(got))
		}
	})
	t.Run("invalid UTF-8 in the input is repaired", func(t *testing.T) {
		got := cleanErrorText("ok\xff\xfeend", 100)
		if !utf8.ValidString(got) {
			t.Fatalf("result is not valid UTF-8: %q", got)
		}
	})
	t.Run("empty stays empty", func(t *testing.T) {
		if got := cleanErrorText("", 100); got != "" {
			t.Fatalf("got %q", got)
		}
	})
}
