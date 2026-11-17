package core

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

// ErrorClass says whether a failed job is worth trying again.
type ErrorClass int

const (
	ClassRetryable ErrorClass = iota // a later attempt may succeed
	ClassPermanent                   // it never will: stop and dead-letter
)

func (c ErrorClass) String() string {
	if c == ClassPermanent {
		return "permanent"
	}
	return "retryable"
}

// classifiedError carries a class (and an optional retry delay) alongside the
// original error. Unwrap lets errors.Is and errors.As see the original.
type classifiedError struct {
	err        error
	class      ErrorClass
	retryAfter time.Duration
}

func (e *classifiedError) Error() string { return e.err.Error() }
func (e *classifiedError) Unwrap() error { return e.err }

// Retryable marks err as worth retrying. A nil error stays nil.
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &classifiedError{err: err, class: ClassRetryable}
}

// RetryableAfter is Retryable with a minimum wait, for a downstream that said
// "try again in N seconds" (HTTP Retry-After). A negative wait counts as none.
func RetryableAfter(err error, after time.Duration) error {
	if err == nil {
		return nil
	}
	if after < 0 {
		after = 0
	}
	return &classifiedError{err: err, class: ClassRetryable, retryAfter: after}
}

// Permanent marks err as one that retrying cannot fix (bad input, rejected
// credentials). A nil error stays nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &classifiedError{err: err, class: ClassPermanent}
}

// Classify reads the class and retry delay off an error. If several markers
// are nested, the outermost wins. An error with no marker is treated as
// retryable.
func Classify(err error) (ErrorClass, time.Duration) {
	var ce *classifiedError
	if errors.As(err, &ce) {
		return ce.class, ce.retryAfter
	}
	return ClassRetryable, 0
}

// Classified reports whether anyone marked err with Retryable, RetryableAfter
// or Permanent. An unmarked error is a guess: the handler's author did not
// say whether retrying can help.
func Classified(err error) bool {
	var ce *classifiedError
	return errors.As(err, &ce)
}

// Backoff returns the wait before retry number attempt, using full jitter:
// a random value between 0 and min(limit, base * 2^(attempt-1)).
// rnd returns a number in [0, 1); passing it in makes the function exactly
// testable.
func Backoff(attempt int, base, limit time.Duration, rnd func() float64) time.Duration {
	if base <= 0 || limit <= 0 {
		return 0
	}
	if attempt < 1 {
		attempt = 1
	}

	// Double up to the limit, stopping early so it can never overflow.
	ceiling := base
	for i := 1; i < attempt; i++ {
		if ceiling >= limit/2 {
			ceiling = limit
			break
		}
		ceiling *= 2
	}
	if ceiling > limit {
		ceiling = limit
	}

	r := rnd()
	if r < 0 {
		r = 0
	}
	if r > 1 {
		r = 1
	}
	delay := time.Duration(r * float64(ceiling))
	if delay > ceiling {
		delay = ceiling
	}
	return delay
}

// cleanErrorText makes error text safe to store in PostgreSQL text columns:
// no NUL bytes, valid UTF-8, and at most max bytes without cutting a
// character in half. Without this, one odd error message could make the
// statement that records the failure fail itself.
func cleanErrorText(s string, max int) string {
	if max <= 0 {
		return ""
	}
	s = strings.ToValidUTF8(s, "\uFFFD")
	s = strings.ReplaceAll(s, "\x00", "")
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
