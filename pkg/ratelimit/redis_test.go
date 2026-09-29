package ratelimit_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/marketplace/pkg/ratelimit"
)

// newRedisClient connects to the Redis from the development stack, or skips.
//
// The test is not tagged and not mocked: a rate limiter's whole job is to be
// atomic under concurrency, and a fake Redis cannot demonstrate that. Skipping
// keeps the suite green on a machine without one.
func newRedisClient(t *testing.T) *redis.Client {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Skipf("no Redis at %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func TestRedisLimiter_AllowsBurstThenRefuses(t *testing.T) {
	client := newRedisClient(t)
	ctx := context.Background()

	key := "test-burst-" + time.Now().Format("150405.000000000")
	limiter, err := ratelimit.NewRedis(ratelimit.RedisConfig{
		Client:    client,
		PerSecond: 1,
		Burst:     3,
	})
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}

	// The bucket starts full, so a burst up to its capacity is allowed.
	for i := range 3 {
		allowed, _ := limiter.Allow(ctx, key)
		if !allowed {
			t.Fatalf("request %d was refused; the bucket starts full with %d tokens", i+1, 3)
		}
	}

	allowed, retryAfter := limiter.Allow(ctx, key)
	if allowed {
		t.Fatal("the fourth request was allowed; the bucket held only three")
	}
	if retryAfter <= 0 {
		t.Error("a refusal must report how long to wait, or a client cannot back off")
	}
}

func TestRedisLimiter_IsAtomicAcrossConcurrentCallers(t *testing.T) {
	// This is the property the in-memory limiter cannot provide and the reason
	// the shared one exists. Twenty goroutines spend a bucket of five: if the
	// read-decide-write were not atomic, several would read the same token count
	// and all of them would be allowed.
	client := newRedisClient(t)
	ctx := context.Background()

	key := "test-atomic-" + time.Now().Format("150405.000000000")
	limiter, err := ratelimit.NewRedis(ratelimit.RedisConfig{
		Client:    client,
		PerSecond: 0.001, // effectively no refill during the test
		Burst:     5,
	})
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}

	const callers = 20

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		allowed int
	)

	start := make(chan struct{})
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // release everyone at once, to maximise overlap

			ok, _ := limiter.Allow(ctx, key)
			if ok {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}

	close(start)
	wg.Wait()

	if allowed != 5 {
		t.Errorf("allowed = %d, want exactly 5: the bucket is not being spent atomically", allowed)
	}
}

func TestRedisLimiter_FallsBackWhenRedisIsGone(t *testing.T) {
	// A limiter that turns a Redis outage into an outage of the thing it
	// protects has made the problem worse. With a fallback configured the
	// request is governed locally; without one it is allowed.
	t.Run("with a fallback", func(t *testing.T) {
		dead := redis.NewClient(&redis.Options{
			Addr:        "127.0.0.1:1", // nothing listens here
			DialTimeout: 200 * time.Millisecond,
			MaxRetries:  0,
		})
		defer func() { _ = dead.Close() }()

		// Refill is slowed to a crawl on purpose. A failed call to the dead
		// Redis costs about a second, and at the production-like rate of one
		// token per second the bucket would refill between the calls — which
		// would make this test measure the clock rather than the fallback.
		fallback := ratelimit.NewMemory(ratelimit.Config{PerSecond: 0.001, Burst: 2})
		limiter, err := ratelimit.NewRedis(ratelimit.RedisConfig{
			Client:    dead,
			PerSecond: 1,
			Burst:     2,
			Fallback:  fallback,
		})
		if err != nil {
			t.Fatalf("NewRedis: %v", err)
		}

		ctx := context.Background()
		key := "test-degraded-" + time.Now().Format("150405.000000000")

		first, _ := limiter.Allow(ctx, key)
		if !first {
			t.Error("the first request was refused while Redis was down; the fallback is not being used")
		}

		// The local bucket still enforces its own limit.
		_, _ = limiter.Allow(ctx, key)
		third, _ := limiter.Allow(ctx, key)
		if third {
			t.Error("the fallback bucket allowed more than its capacity")
		}
	})

	t.Run("without a fallback it fails open", func(t *testing.T) {
		dead := redis.NewClient(&redis.Options{
			Addr:        "127.0.0.1:1",
			DialTimeout: 200 * time.Millisecond,
			MaxRetries:  0,
		})
		defer func() { _ = dead.Close() }()

		limiter, err := ratelimit.NewRedis(ratelimit.RedisConfig{
			Client: dead, PerSecond: 1, Burst: 1,
		})
		if err != nil {
			t.Fatalf("NewRedis: %v", err)
		}

		// Failing open is deliberate: availability beats strictness for a
		// protection mechanism.
		for i := range 5 {
			if allowed, _ := limiter.Allow(context.Background(), "anything"); !allowed {
				t.Fatalf("request %d was refused while Redis was unreachable; the limiter must fail open", i+1)
			}
		}
	})
}

func TestRedisLimiter_SharedAcrossInstances(t *testing.T) {
	// Two limiters over one Redis represent two replicas of the gateway. A
	// client must not get the configured allowance from each of them.
	client := newRedisClient(t)
	ctx := context.Background()

	key := "test-shared-" + time.Now().Format("150405.000000000")

	newInstance := func() *ratelimit.RedisLimiter {
		l, err := ratelimit.NewRedis(ratelimit.RedisConfig{
			Client: client, PerSecond: 0.001, Burst: 3,
		})
		if err != nil {
			t.Fatalf("NewRedis: %v", err)
		}
		return l
	}

	replicaA, replicaB := newInstance(), newInstance()

	allowed := 0
	for _, l := range []*ratelimit.RedisLimiter{replicaA, replicaB, replicaA, replicaB, replicaA, replicaB} {
		if ok, _ := l.Allow(ctx, key); ok {
			allowed++
		}
	}

	if allowed != 3 {
		t.Errorf("allowed = %d across two replicas, want 3: the limit is being enforced per replica", allowed)
	}
}
