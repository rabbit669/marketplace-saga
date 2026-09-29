// Command gateway is the public HTTP edge.
//
// It verifies access tokens, limits each caller's rate, and translates REST into
// gRPC. It holds no business rules: every decision about what a valid order is,
// or what to do when a payment is declined, belongs to the order service. What
// the gateway owns is the things that must happen exactly once for every request
// — authentication, rate limiting, correlation ids, error shaping — which is
// precisely why there is one of them.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/marketplace/pkg/bootstrap"
	"github.com/marketplace/pkg/grpcx"
	"github.com/marketplace/pkg/jwtx"
	authv1 "github.com/marketplace/pkg/proto/auth/v1"
	orderv1 "github.com/marketplace/pkg/proto/order/v1"
	"github.com/marketplace/pkg/ratelimit"
	"github.com/redis/go-redis/v9"

	"github.com/marketplace/services/gateway/internal/transport/httpapi"
)

func main() {
	err := bootstrap.Run(bootstrap.Options{
		Service:           "gateway",
		EnvPrefix:         "GATEWAY",
		DefaultHealthAddr: ":9180",
	}, run)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, rt *bootstrap.Runtime) error {
	// The JWT settings are shared with the auth service, so they are read
	// unprefixed: the gateway verifies tokens that another service signs, and a
	// prefix mismatch would let the two disagree about the issuer.
	jwtCfg := jwtx.Config{
		Secret: rt.Shared.String("JWT_SECRET", ""),
		Issuer: rt.Shared.String("JWT_ISSUER", "marketplace"),
		TTL:    rt.Shared.Duration("JWT_ACCESS_TTL", 15*time.Minute),
	}
	if err := jwtCfg.Validate(); err != nil {
		return fmt.Errorf("gateway: %w (set JWT_SECRET in .env, at least %d bytes)",
			err, jwtx.MinSecretLength)
	}

	orderTarget := rt.Config.String("ORDER_GRPC_TARGET",
		rt.Shared.String("ORDER_GRPC_TARGET", "order:9094"))

	// Redis backs the shared rate limiter. A missing address is not an error:
	// the in-memory limiter is correct for a single replica and is what the
	// shared one degrades to anyway.
	var redisClient redis.UniversalClient
	if addr := rt.Shared.String("REDIS_ADDR", ""); addr != "" {
		redisClient = redis.NewClient(&redis.Options{Addr: addr})
		defer func() { _ = redisClient.Close() }()

		rt.Health.Register("redis", func(ctx context.Context) error {
			if err := redisClient.Ping(ctx).Err(); err != nil {
				// Readiness, not liveness. Losing Redis makes the rate limit
				// per-replica, which is degraded but still serving; taking the
				// gateway out of rotation for it would be worse.
				return fmt.Errorf("redis is not reachable: %w", err)
			}
			return nil
		})
	}

	authTarget := rt.Config.String("AUTH_GRPC_TARGET",
		rt.Shared.String("AUTH_GRPC_TARGET", "auth:9091"))

	authConn, err := grpcx.Dial(ctx, grpcx.ClientConfig{
		Name:    "auth",
		Target:  authTarget,
		Logger:  rt.Log,
		Timeout: rt.Shared.Duration("GRPC_CALL_TIMEOUT", 10*time.Second),
	})
	if err != nil {
		return err
	}
	defer func() { _ = authConn.Close() }()

	conn, err := grpcx.Dial(ctx, grpcx.ClientConfig{
		Name:    "order",
		Target:  orderTarget,
		Logger:  rt.Log,
		Timeout: rt.Shared.Duration("GRPC_CALL_TIMEOUT", 10*time.Second),
	})
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	rt.Health.Register("order_dependency", func(context.Context) error {
		// Connectivity is not probed here on purpose. A readiness check that
		// fails whenever a dependency is briefly unreachable would take the
		// gateway out of rotation for a problem it can still serve around, and
		// the circuit breaker already handles failing fast.
		return nil
	})

	server, err := httpapi.NewServer(httpapi.Config{
		Addr: rt.Config.String("HTTP_ADDR", ":8080"),
		JWT:  jwtCfg,
		RateLimit: ratelimit.Config{
			PerSecond: rt.Shared.Float64("RATE_LIMIT_RPS", 50),
			Burst:     rt.Shared.Float64("RATE_LIMIT_BURST", 100),
		},
		Redis:           redisClient,
		RequestTimeout:  rt.Config.Duration("REQUEST_TIMEOUT", 15*time.Second),
		ShutdownTimeout: rt.Config.Duration("SHUTDOWN_TIMEOUT", 15*time.Second),
		Logger:          rt.Log,
		Metrics:         rt.Metrics,
		OrderClient:     orderv1.NewOrderServiceClient(conn),
		AuthClient:      authv1.NewAuthServiceClient(authConn),
	})
	if err != nil {
		return err
	}

	if err := rt.Config.Err(); err != nil {
		return err
	}

	rt.Log.Info("gateway ready",
		"http_addr", rt.Config.String("HTTP_ADDR", ":8080"),
		"order_target", orderTarget,
		"rate_limit_rps", rt.Shared.Float64("RATE_LIMIT_RPS", 50),
	)

	return server.Serve(ctx)
}
