// Package ratelimit provides a token-bucket rate limiter.
//
// A token bucket rather than a fixed window, for one practical reason: with a
// fixed window, a client that sends its whole quota at the end of one window and
// again at the start of the next produces a burst of twice the limit, which is
// exactly what the limit was meant to prevent. A bucket refills continuously, so
// the burst is bounded by the bucket size no matter when it arrives.
//
// The limiter here is in-process. That makes it correct for a single instance
// and approximate across a fleet — each replica allows its own share. Redis
// backed limiting is the answer once the gateway runs more than one replica, and
// this type is deliberately small so that swapping the store is not a rewrite.
package ratelimit

import (
	"sync"
	"time"
)

// Memory is a per-process token bucket.
//
// It is the concrete implementation the interface's MemoryLimiter wraps; it is
// unexported because callers should depend on the Limiter interface, which is
// also satisfied by the Redis-backed implementation.
type Memory struct {
	mu      sync.Mutex
	buckets map[string]*bucket

	capacity   float64
	refillRate float64 // tokens per second
	now        func() time.Time
}

type bucket struct {
	tokens   float64
	lastSeen time.Time
}

// Config configures a Limiter.
type Config struct {
	// PerSecond is the sustained rate allowed per key.
	PerSecond float64
	// Burst is the bucket size: how much a key may spend at once after being
	// idle. A burst of zero means exactly PerSecond.
	Burst float64
	// Now overrides the clock, for tests.
	Now func() time.Time
}

// New returns an in-process limiter.
func New(cfg Config) *Memory {
	if cfg.PerSecond <= 0 {
		cfg.PerSecond = 1
	}
	if cfg.Burst <= 0 {
		cfg.Burst = cfg.PerSecond
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	return &Memory{
		buckets:    make(map[string]*bucket),
		capacity:   cfg.Burst,
		refillRate: cfg.PerSecond,
		now:        cfg.Now,
	}
}

// Allow reports whether a request for key may proceed, and how long to wait if
// it may not.
//
// The returned duration lets the caller emit a Retry-After header, which is the
// difference between a well-behaved client backing off and one that hammers the
// endpoint until it gets through.
func (l *Memory) Allow(key string) (allowed bool, retryAfter time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()

	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.capacity, lastSeen: now}
		l.buckets[key] = b
	} else {
		// Refill for the time that passed, capped at the bucket size so that an
		// idle key cannot accumulate an unlimited allowance.
		elapsed := now.Sub(b.lastSeen).Seconds()
		if elapsed > 0 {
			b.tokens = min(l.capacity, b.tokens+elapsed*l.refillRate)
			b.lastSeen = now
		}
	}

	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}

	// Time until the bucket holds one whole token.
	needed := (1 - b.tokens) / l.refillRate
	return false, time.Duration(needed * float64(time.Second))
}

// Reap drops buckets that have been idle long enough to have fully refilled,
// bounding memory growth under a flood of distinct keys (which is what an
// attacker sends precisely to exhaust it).
func (l *Memory) Reap(idleFor time.Duration) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	removed := 0

	for key, b := range l.buckets {
		if now.Sub(b.lastSeen) >= idleFor {
			delete(l.buckets, key)
			removed++
		}
	}
	return removed
}

// Len returns how many buckets are tracked, for tests and metrics.
func (l *Memory) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}
