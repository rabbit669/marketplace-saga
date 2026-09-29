// Package resilience provides the retry and circuit-breaking primitives used
// when a service calls a dependency that can be slow or down.
//
// Two rules shape the design:
//
//  1. Retry only what can succeed on a second attempt. Retrying an
//     InvalidArgument forever turns a bug into an outage.
//  2. Always space retries out, and add jitter. Without jitter, every client
//     that failed at the same instant retries at the same instant, and the
//     recovery of a struggling dependency is immediately followed by a second
//     synchronised spike.
package resilience

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/marketplace/pkg/apperr"
)

// DelayFunc waits for d, returning early with an error when ctx is done. It
// exists so that tests can run a retry schedule without sleeping.
type DelayFunc func(ctx context.Context, d time.Duration) error

// RetryPolicy describes how a failing operation is retried.
//
// The zero value is not usable; use DefaultRetryPolicy or construct one
// explicitly.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts, not the number of retries.
	// MaxAttempts=1 means "never retry".
	MaxAttempts int
	// BaseDelay is the delay before the second attempt.
	BaseDelay time.Duration
	// MaxDelay caps the computed delay. It is the only thing preventing
	// exponential backoff from turning into a multi-minute hang.
	MaxDelay time.Duration
	// Multiplier grows the delay between attempts. Values below 1 are clamped
	// to 1 so that the schedule is at least constant.
	Multiplier float64
	// Jitter is the fraction of the delay to randomise, in [0, 1]. Zero means
	// no jitter, which is almost always the wrong choice outside tests.
	Jitter float64
	// Retryable decides whether an error justifies another attempt. Nil means
	// RetryableByCode.
	Retryable func(error) bool
	// OnRetry is invoked before sleeping, for metrics and logging.
	OnRetry func(attempt int, err error, delay time.Duration)
	// Sleep overrides the waiting implementation. Nil means a real timer that
	// respects context cancellation.
	Sleep DelayFunc
	// Rand overrides the jitter source, in [0, 1). Nil means the global source.
	Rand func() float64
}

// DefaultRetryPolicy is a sensible starting point for inter-service calls:
// four attempts over roughly 1.5 seconds, with jitter.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 4,
		BaseDelay:   50 * time.Millisecond,
		MaxDelay:    2 * time.Second,
		Multiplier:  2,
		Jitter:      0.2,
	}
}

// withDefaults returns p with unset fields filled in, so a partially specified
// policy behaves predictably instead of producing zero delays.
func (p RetryPolicy) withDefaults() RetryPolicy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 1
	}
	if p.BaseDelay <= 0 {
		p.BaseDelay = 50 * time.Millisecond
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = 2 * time.Second
	}
	if p.Multiplier < 1 {
		p.Multiplier = 2
	}
	p.Jitter = clamp01(p.Jitter)
	if p.Retryable == nil {
		p.Retryable = RetryableByCode
	}
	if p.Sleep == nil {
		p.Sleep = sleepCtx
	}
	if p.Rand == nil {
		p.Rand = rand.Float64
	}
	return p
}

// Do runs fn, retrying according to the policy.
//
// It returns the last error, wrapped with the attempt count so logs show how
// hard the system tried. The final error is returned unwrapped at the top of
// the chain, which keeps errors.As working for callers up the stack.
func (p RetryPolicy) Do(ctx context.Context, op string, fn func(ctx context.Context) error) error {
	pol := p.withDefaults()

	var lastErr error
	// made counts the attempts actually performed, which is not the same as the
	// policy's limit: a permanent error stops after one, and a cancelled context
	// stops before any. Reporting the limit made a single declined card look
	// like four retries in the logs, which is exactly the wrong thing for an
	// operator to read while diagnosing a failure.
	made := 0

	for attempt := 1; attempt <= pol.MaxAttempts; attempt++ {
		// Stop before starting new work if the caller has already gone away.
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return fmt.Errorf("%s: abandoned after %d attempt(s): %w", op, attempt-1, lastErr)
			}
			return fmt.Errorf("%s: %w", op, err)
		}

		made++
		err := fn(ctx)
		if err == nil {
			return nil
		}
		lastErr = err

		if attempt == pol.MaxAttempts || !pol.Retryable(err) {
			break
		}

		delay := pol.delayFor(attempt)

		if pol.OnRetry != nil {
			pol.OnRetry(attempt, err, delay)
		}

		if sleepErr := pol.Sleep(ctx, delay); sleepErr != nil {
			// The caller gave up while we were waiting. Report the original
			// failure, not the cancellation, since that is the useful fact.
			return fmt.Errorf("%s: interrupted after %d attempt(s): %w", op, attempt, lastErr)
		}
	}

	return fmt.Errorf("%s: failed after %d attempt(s): %w", op, made, lastErr)
}

// delayFor computes the wait before the attempt following the given one.
func (p RetryPolicy) delayFor(attempt int) time.Duration {
	d := float64(p.BaseDelay) * math.Pow(p.Multiplier, float64(attempt-1))
	if d > float64(p.MaxDelay) {
		d = float64(p.MaxDelay)
	}

	if p.Jitter > 0 {
		// Spread the delay uniformly across [d*(1-j), d*(1+j)].
		d *= 1 - p.Jitter + 2*p.Jitter*p.Rand()
	}

	if d < 0 {
		d = 0
	}
	return time.Duration(d)
}

// sleepCtx waits for d or until ctx is done, whichever comes first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func clamp01(f float64) float64 {
	switch {
	case f < 0:
		return 0
	case f > 1:
		return 1
	default:
		return f
	}
}

// RetryableByCode reports whether an error is worth another attempt, based on
// its apperr classification.
//
// Only transient classes qualify. In particular a raw error that was never
// classified is treated as permanent: without a reason to believe otherwise,
// retrying an unknown failure mostly amplifies load. Adapters that call the
// network are expected to classify connection failures as CodeUnavailable,
// which is what makes those calls retryable.
func RetryableByCode(err error) bool {
	switch apperr.CodeOf(err) {
	case apperr.CodeUnavailable, apperr.CodeDeadlineExceeded, apperr.CodeResourceExhausted:
		return true
	case apperr.CodeInternal:
		// An internal error may be a transient database blip or a bug; the
		// classification cannot tell, so err on the side of not retrying.
		return false
	default:
		return false
	}
}

// NeverRetry is a Retryable implementation for operations that must run at most
// once per caller action, such as a non-idempotent side effect.
func NeverRetry(error) bool { return false }

// AlwaysRetry is a Retryable implementation for genuinely idempotent local
// operations. Do not use it for calls across a network boundary.
func AlwaysRetry(error) bool { return true }

// ErrCircuitOpen is returned when a circuit breaker is rejecting calls to
// protect a failing dependency.
var ErrCircuitOpen = errors.New("resilience: circuit breaker is open")
