package resilience

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marketplace/pkg/apperr"
)

// newTestPolicy returns a policy whose sleeps are recorded instead of performed.
func newTestPolicy(maxAttempts int) (RetryPolicy, *[]time.Duration) {
	delays := &[]time.Duration{}
	p := RetryPolicy{
		MaxAttempts: maxAttempts,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    time.Second,
		Multiplier:  2,
		Jitter:      0,
		Sleep: func(_ context.Context, d time.Duration) error {
			*delays = append(*delays, d)
			return nil
		},
	}
	return p, delays
}

func TestRetry_StopsAtFirstSuccess(t *testing.T) {
	policy, delays := newTestPolicy(5)

	calls := 0
	err := policy.Do(context.Background(), "test", func(context.Context) error {
		calls++
		return nil
	})

	if err != nil {
		t.Fatalf("Do = %v, want nil", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
	if len(*delays) != 0 {
		t.Errorf("delays = %v, want none", *delays)
	}
}

func TestRetry_RetriesUntilSuccess(t *testing.T) {
	policy, delays := newTestPolicy(5)

	calls := 0
	err := policy.Do(context.Background(), "test", func(context.Context) error {
		calls++
		if calls < 3 {
			return apperr.Unavailable("test", "dependency is down")
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Do = %v, want nil", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
	if len(*delays) != 2 {
		t.Errorf("delays = %v, want 2 entries", *delays)
	}
}

func TestRetry_BackoffIsExponentialAndCapped(t *testing.T) {
	policy, delays := newTestPolicy(6)

	_ = policy.Do(context.Background(), "test", func(context.Context) error {
		return apperr.Unavailable("test", "still down")
	})

	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		time.Second, // capped by MaxDelay, not 1600ms
	}

	if len(*delays) != len(want) {
		t.Fatalf("delays = %v, want %v", *delays, want)
	}
	for i := range want {
		if (*delays)[i] != want[i] {
			t.Errorf("delay[%d] = %v, want %v", i, (*delays)[i], want[i])
		}
	}
}

func TestRetry_DoesNotRetryPermanentErrors(t *testing.T) {
	// The point of the taxonomy: a rejected argument will be rejected again.
	// Retrying it wastes the caller's latency budget and multiplies load.
	policy, delays := newTestPolicy(5)

	calls := 0
	err := policy.Do(context.Background(), "test", func(context.Context) error {
		calls++
		return apperr.InvalidArgument("test", "quantity must be positive")
	})

	if err == nil {
		t.Fatal("Do = nil, want an error")
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1: InvalidArgument must not be retried", calls)
	}
	if len(*delays) != 0 {
		t.Errorf("delays = %v, want none", *delays)
	}

	// The original classification must survive for callers up the stack.
	if !apperr.Is(err, apperr.CodeInvalidArgument) {
		t.Errorf("CodeOf = %q, want %q", apperr.CodeOf(err), apperr.CodeInvalidArgument)
	}
}

func TestRetry_RetryableClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"unavailable", apperr.Unavailable("op", "down"), true},
		{"deadline exceeded", context.DeadlineExceeded, true},
		{"not found", apperr.NotFound("op", "missing"), false},
		{"invalid argument", apperr.InvalidArgument("op", "bad"), false},
		{"conflict", apperr.Conflict("op", "stale"), false},
		{"unclassified", errors.New("something odd"), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := RetryableByCode(tc.err); got != tc.want {
				t.Errorf("RetryableByCode(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestRetry_ExhaustsAttemptsAndReportsCount(t *testing.T) {
	policy, _ := newTestPolicy(3)

	calls := 0
	err := policy.Do(context.Background(), "payment.Authorize", func(context.Context) error {
		calls++
		return apperr.Unavailable("payment.Authorize", "provider unreachable")
	})

	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
	if err == nil {
		t.Fatal("Do = nil, want an error")
	}
	if !strings.Contains(err.Error(), "3 attempt") {
		t.Errorf("error = %q, want it to state the attempt count", err)
	}
	if !errors.Is(err, context.Canceled) && !apperr.Is(err, apperr.CodeUnavailable) {
		t.Error("the wrapped cause must remain inspectable")
	}
}

func TestRetry_JitterStaysWithinBounds(t *testing.T) {
	base := 100 * time.Millisecond

	tests := []struct {
		name     string
		jitter   float64
		randVal  float64
		wantMult float64
	}{
		{"no jitter", 0, 0.5, 1},
		{"low end", 0.5, 0, 0.5},
		{"midpoint", 0.5, 0.5, 1},
		{"high end", 0.5, 1, 1.5},
		{"jitter clamped above 1", 2, 1, 2},
		{"jitter clamped below 0", -1, 0.5, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got time.Duration
			p := RetryPolicy{
				MaxAttempts: 2,
				BaseDelay:   base,
				MaxDelay:    time.Minute,
				Multiplier:  1,
				Jitter:      tc.jitter,
				Rand:        func() float64 { return tc.randVal },
				Sleep: func(_ context.Context, d time.Duration) error {
					got = d
					return nil
				},
			}

			_ = p.Do(context.Background(), "test", func(context.Context) error {
				return apperr.Unavailable("test", "down")
			})

			want := time.Duration(float64(base) * tc.wantMult)
			if got != want {
				t.Errorf("delay = %v, want %v", got, want)
			}
		})
	}
}

func TestRetry_OnRetryObservabilityHook(t *testing.T) {
	type retryEvent struct {
		attempt int
		delay   time.Duration
	}

	var events []retryEvent

	p := RetryPolicy{
		MaxAttempts: 4,
		BaseDelay:   time.Millisecond,
		MaxDelay:    time.Second,
		Multiplier:  2,
		Sleep:       func(context.Context, time.Duration) error { return nil },
		OnRetry: func(attempt int, _ error, delay time.Duration) {
			events = append(events, retryEvent{attempt, delay})
		},
	}

	_ = p.Do(context.Background(), "test", func(context.Context) error {
		return apperr.Unavailable("test", "down")
	})

	if len(events) != 3 {
		t.Fatalf("OnRetry called %d times, want 3 (one before each retry)", len(events))
	}
	for i, e := range events {
		if e.attempt != i+1 {
			t.Errorf("event[%d].attempt = %d, want %d", i, e.attempt, i+1)
		}
	}
}

func TestRetry_StopsWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	calls := 0
	p := RetryPolicy{
		MaxAttempts: 10,
		BaseDelay:   time.Millisecond,
		MaxDelay:    time.Millisecond,
		Multiplier:  1,
		Sleep: func(ctx context.Context, _ time.Duration) error {
			// Simulate the caller giving up while we were waiting.
			cancel()
			return ctx.Err()
		},
	}

	err := p.Do(ctx, "test", func(context.Context) error {
		calls++
		return apperr.Unavailable("test", "down")
	})

	if calls != 1 {
		t.Errorf("calls = %d, want 1: retrying must stop as soon as the caller is gone", calls)
	}
	if err == nil {
		t.Fatal("Do = nil, want an error")
	}
	if !strings.Contains(err.Error(), "interrupted") {
		t.Errorf("error = %q, want it to report interruption", err)
	}
}

func TestRetry_RejectsImmediatelyWhenContextAlreadyDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p := RetryPolicy{MaxAttempts: 5, Sleep: func(context.Context, time.Duration) error { return nil }}

	calls := 0
	err := p.Do(ctx, "test", func(context.Context) error {
		calls++
		return nil
	})

	if calls != 0 {
		t.Errorf("calls = %d, want 0: no work should start for a cancelled context", calls)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Do = %v, want context.Canceled", err)
	}
}

func TestRetry_ZeroValuePolicyStillWorks(t *testing.T) {
	var p RetryPolicy

	calls := 0
	err := p.Do(context.Background(), "test", func(context.Context) error {
		calls++
		return apperr.Unavailable("test", "down")
	})

	if calls != 1 {
		t.Errorf("calls = %d, want 1: a zero policy means a single attempt", calls)
	}
	if err == nil {
		t.Error("Do = nil, want the failure to be reported")
	}
}

// ------------------------------------------------------------- breaker

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func TestBreaker_OpensAfterThresholdFailures(t *testing.T) {
	clock := newFakeClock()
	b := NewCircuitBreaker(BreakerConfig{
		Name:             "payment",
		FailureThreshold: 3,
		OpenTimeout:      30 * time.Second,
		Now:              clock.Now,
	})

	fail := func() error {
		return b.Do(context.Background(), func(context.Context) error {
			return apperr.Unavailable("payment", "down")
		})
	}

	for i := range 2 {
		if err := fail(); err == nil {
			t.Fatalf("call %d = nil, want the underlying error", i+1)
		}
		if got := b.State(); got != StateClosed {
			t.Fatalf("state after %d failures = %v, want closed", i+1, got)
		}
	}

	// The third failure crosses the threshold.
	if err := fail(); err == nil {
		t.Fatal("third call = nil, want an error")
	}
	if got := b.State(); got != StateOpen {
		t.Fatalf("state after 3 failures = %v, want open", got)
	}

	// While open, calls are rejected without touching the dependency, and the
	// rejection is distinguishable from a dependency failure.
	reached := false
	err := b.Do(context.Background(), func(context.Context) error {
		reached = true
		return nil
	})
	if reached {
		t.Error("the dependency was called while the breaker was open")
	}
	if !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("Do = %v, want ErrCircuitOpen", err)
	}
}

func TestBreaker_ClosesAfterSuccessfulProbes(t *testing.T) {
	clock := newFakeClock()
	b := NewCircuitBreaker(BreakerConfig{
		Name:             "payment",
		FailureThreshold: 2,
		OpenTimeout:      10 * time.Second,
		HalfOpenMaxCalls: 1,
		SuccessThreshold: 2,
		Now:              clock.Now,
	})

	fail := func() {
		_ = b.Do(context.Background(), func(context.Context) error {
			return apperr.Unavailable("payment", "down")
		})
	}
	succeed := func() error {
		return b.Do(context.Background(), func(context.Context) error { return nil })
	}

	fail()
	fail()
	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %v, want open", got)
	}

	// Too early: still open.
	clock.Advance(5 * time.Second)
	if err := succeed(); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("call before OpenTimeout elapsed = %v, want ErrCircuitOpen", err)
	}

	// Past the timeout the breaker admits a probe.
	clock.Advance(10 * time.Second)
	if err := succeed(); err != nil {
		t.Fatalf("probe = %v, want nil", err)
	}
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("state after the first probe = %v, want half_open (SuccessThreshold is 2)", got)
	}

	if err := succeed(); err != nil {
		t.Fatalf("second probe = %v, want nil", err)
	}
	if got := b.State(); got != StateClosed {
		t.Fatalf("state after 2 probes = %v, want closed", got)
	}
}

func TestBreaker_FailedProbeReopensImmediately(t *testing.T) {
	clock := newFakeClock()
	b := NewCircuitBreaker(BreakerConfig{
		Name:             "payment",
		FailureThreshold: 1,
		OpenTimeout:      10 * time.Second,
		SuccessThreshold: 3,
		Now:              clock.Now,
	})

	_ = b.Do(context.Background(), func(context.Context) error {
		return apperr.Unavailable("payment", "down")
	})
	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %v, want open", got)
	}

	clock.Advance(10 * time.Second)

	// One failed probe is enough: the dependency just proved it is unhealthy,
	// and continuing to probe would only add load.
	_ = b.Do(context.Background(), func(context.Context) error {
		return apperr.Unavailable("payment", "still down")
	})
	if got := b.State(); got != StateOpen {
		t.Fatalf("state after a failed probe = %v, want open", got)
	}
}

func TestBreaker_HalfOpenLimitsConcurrentProbes(t *testing.T) {
	clock := newFakeClock()
	b := NewCircuitBreaker(BreakerConfig{
		Name:             "payment",
		FailureThreshold: 1,
		OpenTimeout:      time.Second,
		HalfOpenMaxCalls: 1,
		Now:              clock.Now,
	})

	_ = b.Do(context.Background(), func(context.Context) error {
		return apperr.Unavailable("payment", "down")
	})
	clock.Advance(time.Second)

	// First probe is admitted but not yet finished.
	done, err := b.Allow()
	if err != nil {
		t.Fatalf("first probe = %v, want admission", err)
	}

	// A second concurrent call must be turned away: recovery traffic should not
	// be able to stampede a dependency that is only just coming back.
	if _, err := b.Allow(); !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("second concurrent probe = %v, want ErrCircuitOpen", err)
	}

	done(true)

	// Closing needs SuccessThreshold successes; the default here is 1.
	if got := b.State(); got != StateClosed {
		t.Errorf("state = %v, want closed after a successful probe", got)
	}
}

func TestBreaker_ReportingTheOutcomeTwiceIsIgnored(t *testing.T) {
	clock := newFakeClock()
	b := NewCircuitBreaker(BreakerConfig{
		Name:             "payment",
		FailureThreshold: 2,
		OpenTimeout:      time.Second,
		Now:              clock.Now,
	})

	done, err := b.Allow()
	if err != nil {
		t.Fatalf("Allow = %v", err)
	}

	// A deferred call plus an explicit one is an easy mistake to make; it must
	// not count twice towards the threshold.
	done(false)
	done(false)
	done(false)

	if got := b.State(); got != StateClosed {
		t.Errorf("state = %v, want closed: one call must count as one failure", got)
	}
	if got := b.Snapshot().ConsecutiveFailures; got != 1 {
		t.Errorf("ConsecutiveFailures = %d, want 1", got)
	}
}

func TestBreaker_IsFailurePredicateExcludesCallerErrors(t *testing.T) {
	clock := newFakeClock()
	b := NewCircuitBreaker(BreakerConfig{
		Name:             "catalog",
		FailureThreshold: 2,
		OpenTimeout:      time.Second,
		Now:              clock.Now,
		// Only blame the dependency for its own failures.
		IsFailure: func(err error) bool {
			return apperr.CodeOf(err) == apperr.CodeUnavailable
		},
	})

	for range 5 {
		_ = b.Do(context.Background(), func(context.Context) error {
			return apperr.NotFound("catalog.Get", "no such product")
		})
	}

	if got := b.State(); got != StateClosed {
		t.Errorf("state = %v, want closed: caller errors must not trip the breaker", got)
	}

	// A genuine dependency failure still counts.
	_ = b.Do(context.Background(), func(context.Context) error {
		return apperr.Unavailable("catalog.Get", "database down")
	})
	_ = b.Do(context.Background(), func(context.Context) error {
		return apperr.Unavailable("catalog.Get", "database down")
	})

	if got := b.State(); got != StateOpen {
		t.Errorf("state = %v, want open after dependency failures", got)
	}
}

func TestBreaker_SuccessResetsTheFailureCount(t *testing.T) {
	clock := newFakeClock()
	b := NewCircuitBreaker(BreakerConfig{
		Name:             "payment",
		FailureThreshold: 3,
		OpenTimeout:      time.Second,
		Now:              clock.Now,
	})

	fail := func() {
		_ = b.Do(context.Background(), func(context.Context) error {
			return apperr.Unavailable("payment", "down")
		})
	}

	fail()
	fail()

	// A success means the dependency is not in a sustained failure state, so
	// the count restarts. This prevents unrelated blips spread over an hour
	// from accumulating into an open breaker.
	if err := b.Do(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("successful call = %v", err)
	}
	if got := b.Snapshot().ConsecutiveFailures; got != 0 {
		t.Fatalf("ConsecutiveFailures = %d, want 0 after a success", got)
	}

	fail()
	fail()
	if got := b.State(); got != StateClosed {
		t.Errorf("state = %v, want closed: the counter should have restarted", got)
	}
}

func TestBreaker_DefaultConfigIsUsable(t *testing.T) {
	b := NewCircuitBreaker(DefaultBreakerConfig(""))

	if got := b.State(); got != StateClosed {
		t.Errorf("a new breaker must start closed, got %v", got)
	}
	if err := b.Do(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Errorf("Do = %v, want nil", err)
	}
	if got := b.Snapshot().Transitions; got != 0 {
		t.Errorf("Transitions = %d, want 0", got)
	}
}

func TestBreakerState_String(t *testing.T) {
	tests := map[BreakerState]string{
		StateClosed:      "closed",
		StateOpen:        "open",
		StateHalfOpen:    "half_open",
		BreakerState(99): "unknown",
	}
	for state, want := range tests {
		if got := state.String(); got != want {
			t.Errorf("State(%d).String() = %q, want %q", state, got, want)
		}
	}
}

func TestRetry_ReportsAttemptsActuallyMade(t *testing.T) {
	// The message has to describe what happened, not what was allowed. Reading
	// "failed after 4 attempts" when the call was rejected once sends an
	// operator looking for three retries that never occurred.
	t.Run("permanent error stops after one", func(t *testing.T) {
		policy, _ := newTestPolicy(5)

		err := policy.Do(context.Background(), "order.Create", func(context.Context) error {
			return apperr.InvalidArgument("order.Create", "quantity must be positive")
		})

		if !strings.Contains(err.Error(), "after 1 attempt") {
			t.Errorf("error = %q, want it to report a single attempt", err)
		}
	})

	t.Run("exhausted policy reports the limit", func(t *testing.T) {
		policy, _ := newTestPolicy(3)

		err := policy.Do(context.Background(), "order.Create", func(context.Context) error {
			return apperr.Unavailable("order.Create", "down")
		})

		if !strings.Contains(err.Error(), "after 3 attempt") {
			t.Errorf("error = %q, want it to report three attempts", err)
		}
	})
}
