package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limiter decides whether a key may proceed.
//
// An interface rather than a struct because the correct implementation depends
// on how many replicas are running. One process is well served by a map; two or
// more need a shared counter, or each replica enforces the limit independently
// and the effective allowance is the limit multiplied by the replica count —
// a bug that only appears when the service scales out, which is precisely when
// it is least welcome.
type Limiter interface {
	Allow(ctx context.Context, key string) (allowed bool, retryAfter time.Duration)
}

// MemoryLimiter is the in-process token bucket from Limiter above.
//
// It implements the interface so that a service can run without Redis, and so
// that tests do not need one. Its limit is per process; see the interface
// comment.
type MemoryLimiter struct{ *Memory }

// NewMemory returns an in-process limiter.
func NewMemory(cfg Config) *MemoryLimiter {
	return &MemoryLimiter{Memory: New(cfg)}
}

// Allow implements Limiter.
func (m *MemoryLimiter) Allow(_ context.Context, key string) (bool, time.Duration) {
	return m.Memory.Allow(key)
}

// tokenBucketScript implements the bucket in Redis.
//
// It has to be a script rather than a sequence of commands: "read the bucket,
// decide, write it back" is a read-modify-write, and two replicas doing it
// concurrently would both read the same token count and both allow the request.
// Redis executes a script atomically, so the decision and the decrement cannot
// be interleaved.
//
// KEYS[1] the bucket
// ARGV[1] capacity, ARGV[2] refill per second, ARGV[3] now in milliseconds
//
// Returns the number of milliseconds to wait, or 0 when the request is allowed.
const tokenBucketScript = `
local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])

local bucket = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts     = tonumber(bucket[2])

if tokens == nil then
  -- A key that has never been seen starts full, which is what makes a burst
  -- after an idle period allowed rather than punished.
  tokens = capacity
  ts = now
end

local elapsed = math.max(0, now - ts) / 1000
tokens = math.min(capacity, tokens + elapsed * rate)

local retryAfter = 0
if tokens >= 1 then
  tokens = tokens - 1
else
  -- Time until one whole token exists. Rounding up means a client that honours
  -- Retry-After exactly does not come back a millisecond too early.
  retryAfter = math.ceil((1 - tokens) / rate * 1000)
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
-- Expire once the bucket would have refilled completely: after that the stored
-- value is indistinguishable from a fresh one, so keeping it is only memory.
redis.call('PEXPIRE', key, math.ceil(capacity / rate * 1000) + 1000)

return retryAfter
`

// RedisLimiter is a shared token bucket.
type RedisLimiter struct {
	client   redis.UniversalClient
	script   *redis.Script
	capacity float64
	rate     float64

	// fallback is used when Redis is unreachable.
	//
	// Failing open — allowing the request — is the deliberate choice. A rate
	// limiter protects availability, and a limiter that turns a Redis outage
	// into a total outage of the thing it protects has made the problem worse.
	// The alternative, failing closed, is defensible for authentication and is
	// the wrong trade here.
	fallback *MemoryLimiter
}

// RedisConfig configures the shared limiter.
type RedisConfig struct {
	Client redis.UniversalClient
	// PerSecond is the sustained rate per key.
	PerSecond float64
	// Burst is the bucket size.
	Burst float64
	// Fallback, when set, is consulted if Redis cannot answer.
	Fallback *MemoryLimiter
}

// NewRedis returns a limiter backed by Redis.
func NewRedis(cfg RedisConfig) (*RedisLimiter, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("ratelimit: a redis client is required")
	}
	if cfg.PerSecond <= 0 {
		cfg.PerSecond = 1
	}
	if cfg.Burst <= 0 {
		cfg.Burst = cfg.PerSecond
	}

	return &RedisLimiter{
		client:   cfg.Client,
		script:   redis.NewScript(tokenBucketScript),
		capacity: cfg.Burst,
		rate:     cfg.PerSecond,
		fallback: cfg.Fallback,
	}, nil
}

// Allow implements Limiter.
func (r *RedisLimiter) Allow(ctx context.Context, key string) (bool, time.Duration) {
	// One second, not the caller's deadline: this runs inside every request, and
	// a limiter that can block for as long as the slowest endpoint is worse than
	// no limiter at all.
	callCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	raw, err := r.script.Run(callCtx, r.client, []string{"ratelimit:" + key},
		r.capacity, r.rate, time.Now().UnixMilli()).Result()
	if err != nil {
		return r.degrade(ctx, key, err)
	}

	retryAfterMS, ok := raw.(int64)
	if !ok {
		return r.degrade(ctx, key, fmt.Errorf("unexpected script result %T", raw))
	}
	if retryAfterMS <= 0 {
		return true, 0
	}
	return false, time.Duration(retryAfterMS) * time.Millisecond
}

// degrade handles a Redis failure.
func (r *RedisLimiter) degrade(ctx context.Context, key string, err error) (bool, time.Duration) {
	if r.fallback != nil {
		// A local bucket is an approximation of the shared one and is far better
		// than no protection at all.
		return r.fallback.Allow(ctx, key)
	}
	// No fallback configured: fail open, so that losing Redis does not take down
	// the API.
	return true, 0
}

// Reap is a no-op: Redis expires buckets itself.
//
// It exists so that the gateway's janitor goroutine does not have to know which
// implementation it holds.
func (r *RedisLimiter) Reap(time.Duration) int { return 0 }
