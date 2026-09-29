package resilience

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// BreakerState is the state of a CircuitBreaker.
type BreakerState int

const (
	// StateClosed passes calls through and counts failures.
	StateClosed BreakerState = iota
	// StateOpen rejects calls immediately, giving a failing dependency room to
	// recover instead of being hammered by its own clients.
	StateOpen
	// StateHalfOpen admits a limited number of probe calls. Success closes the
	// breaker; failure opens it again.
	StateHalfOpen
)

func (s BreakerState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

// BreakerConfig configures a CircuitBreaker.
type BreakerConfig struct {
	// Name identifies the breaker in logs and metrics.
	Name string
	// FailureThreshold is the number of consecutive failures that opens the
	// breaker. Values below 1 become 1.
	FailureThreshold int
	// OpenTimeout is how long the breaker stays open before allowing probes.
	OpenTimeout time.Duration
	// HalfOpenMaxCalls bounds concurrent probes, so that recovery traffic
	// cannot itself overwhelm the dependency. Values below 1 become 1.
	HalfOpenMaxCalls int
	// SuccessThreshold is the number of consecutive successful probes needed
	// to close the breaker. Values below 1 become 1.
	SuccessThreshold int
	// IsFailure decides which errors count against the dependency. Nil counts
	// every error. Use it to exclude caller-caused failures (a validation
	// error is not the dependency's fault) from tripping the breaker.
	IsFailure func(error) bool
	// Now overrides the clock, for tests.
	Now func() time.Time
}

// DefaultBreakerConfig returns a reasonable breaker for a synchronous
// inter-service call.
func DefaultBreakerConfig(name string) BreakerConfig {
	return BreakerConfig{
		Name:             name,
		FailureThreshold: 5,
		OpenTimeout:      30 * time.Second,
		HalfOpenMaxCalls: 1,
		SuccessThreshold: 2,
	}
}

func (c BreakerConfig) withDefaults() BreakerConfig {
	if c.FailureThreshold < 1 {
		c.FailureThreshold = 5
	}
	if c.OpenTimeout <= 0 {
		c.OpenTimeout = 30 * time.Second
	}
	if c.HalfOpenMaxCalls < 1 {
		c.HalfOpenMaxCalls = 1
	}
	if c.SuccessThreshold < 1 {
		c.SuccessThreshold = 1
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

// BreakerSnapshot is a point-in-time view of a breaker, for metrics and tests.
type BreakerSnapshot struct {
	Name                 string
	State                BreakerState
	ConsecutiveFailures  int
	ConsecutiveSuccesses int
	OpenedAt             time.Time
	// Transitions counts how many times the breaker changed state.
	Transitions uint64
}

// CircuitBreaker implements the standard three-state breaker.
//
// A breaker is not a retry mechanism and does not replace one: retries handle
// transient blips, the breaker handles a dependency that is down for a while.
// Used together, retries are attempted while the breaker is closed, and the
// breaker stops new attempts once failure is clearly not transient.
type CircuitBreaker struct {
	cfg BreakerConfig

	mu                   sync.Mutex
	state                BreakerState
	consecutiveFailures  int
	consecutiveSuccesses int
	openedAt             time.Time
	halfOpenInFlight     int
	transitions          uint64
}

// NewCircuitBreaker returns a breaker in the closed state.
func NewCircuitBreaker(cfg BreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{cfg: cfg.withDefaults(), state: StateClosed}
}

// Name returns the configured breaker name.
func (b *CircuitBreaker) Name() string { return b.cfg.Name }

// Snapshot returns the current state.
func (b *CircuitBreaker) Snapshot() BreakerSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()

	return BreakerSnapshot{
		Name:                 b.cfg.Name,
		State:                b.state,
		ConsecutiveFailures:  b.consecutiveFailures,
		ConsecutiveSuccesses: b.consecutiveSuccesses,
		OpenedAt:             b.openedAt,
		Transitions:          b.transitions,
	}
}

// State returns just the current state.
func (b *CircuitBreaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.currentStateLocked()
}

// Allow reports whether a call may proceed, and returns a function that must be
// called exactly once with the outcome.
//
//	done, err := breaker.Allow()
//	if err != nil { return err }
//	err = doWork()
//	done(err == nil)
//	return err
//
// It returns ErrCircuitOpen when the breaker is rejecting calls.
func (b *CircuitBreaker) Allow() (func(success bool), error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	state := b.currentStateLocked()

	switch state {
	case StateOpen:
		return nil, fmt.Errorf("%s: %w", b.cfg.Name, ErrCircuitOpen)

	case StateHalfOpen:
		if b.halfOpenInFlight >= b.cfg.HalfOpenMaxCalls {
			// The probe slot is taken; stay closed to further traffic.
			return nil, fmt.Errorf("%s: %w", b.cfg.Name, ErrCircuitOpen)
		}
		b.halfOpenInFlight++
	}

	// Remember whether this call consumed a half-open probe slot. The state can
	// move on while the call is in flight (another probe may close the breaker),
	// so the outcome cannot be attributed by re-reading the state afterwards.
	admittedAsProbe := state == StateHalfOpen

	var once sync.Once
	return func(success bool) {
		// Guard against a caller reporting the outcome twice, which would
		// double-count failures and could open the breaker prematurely.
		once.Do(func() { b.record(success, admittedAsProbe) })
	}, nil
}

// Do runs fn under the breaker, calling Allow and reporting the outcome.
func (b *CircuitBreaker) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	done, err := b.Allow()
	if err != nil {
		return err
	}

	err = fn(ctx)
	done(b.isSuccess(err))
	return err
}

// isSuccess applies the configured failure predicate.
func (b *CircuitBreaker) isSuccess(err error) bool {
	if err == nil {
		return true
	}
	if b.cfg.IsFailure == nil {
		return false
	}
	return !b.cfg.IsFailure(err)
}

// currentStateLocked resolves the Open -> HalfOpen transition on demand, which
// is what makes the breaker self-healing without a background timer.
// b.mu must be held.
func (b *CircuitBreaker) currentStateLocked() BreakerState {
	if b.state == StateOpen && !b.cfg.Now().Before(b.openedAt.Add(b.cfg.OpenTimeout)) {
		b.setStateLocked(StateHalfOpen)
	}
	return b.state
}

// record applies the outcome of a finished call. admittedAsProbe reports
// whether the call consumed a half-open probe slot when it was admitted.
func (b *CircuitBreaker) record(success, admittedAsProbe bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Resolve an elapsed Open period before interpreting the outcome, so a
	// probe that started in the open state is recorded against half-open.
	state := b.currentStateLocked()

	if admittedAsProbe && b.halfOpenInFlight > 0 {
		b.halfOpenInFlight--
	}

	switch {
	case admittedAsProbe:
		// The outcome belongs to the probe, whatever the breaker has done in
		// the meantime.
		if success {
			b.consecutiveSuccesses++
			if b.consecutiveSuccesses >= b.cfg.SuccessThreshold {
				b.closeLocked()
			}
			return
		}
		// A single failed probe is enough to reopen: the dependency has just
		// demonstrated it is still unhealthy.
		b.openLocked()

	case state == StateOpen:
		// Admitted while closed, before the breaker opened. This outcome is not
		// evidence about the current probe period, so it is not counted.

	default:
		if success {
			b.consecutiveFailures = 0
			return
		}
		b.consecutiveFailures++
		if b.consecutiveFailures >= b.cfg.FailureThreshold {
			b.openLocked()
		}
	}
}

func (b *CircuitBreaker) openLocked() {
	b.setStateLocked(StateOpen)
	b.openedAt = b.cfg.Now()
	b.consecutiveSuccesses = 0
	// Failures are intentionally not reset here: the snapshot should show how
	// bad things got before the breaker tripped.
}

func (b *CircuitBreaker) closeLocked() {
	b.setStateLocked(StateClosed)
	b.consecutiveFailures = 0
	b.consecutiveSuccesses = 0
	b.halfOpenInFlight = 0
	b.openedAt = time.Time{}
}

func (b *CircuitBreaker) setStateLocked(s BreakerState) {
	if b.state == s {
		return
	}
	b.state = s
	b.transitions++
}
