package grpcx

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/marketplace/pkg/apperr"
	"github.com/marketplace/pkg/resilience"
)

// ClientConfig describes a connection to one dependency.
type ClientConfig struct {
	// Name is the dependency's name, used in logs and in error messages.
	Name string
	// Target is host:port inside the compose or Kubernetes network.
	Target string
	// Logger receives one record per completed call.
	Logger *slog.Logger
	// Timeout bounds a single logical call. Zero means no client-side deadline,
	// which is almost always wrong: without one, a hung dependency holds the
	// caller's connection until something further up gives up, and the caller
	// has no chance to degrade gracefully.
	Timeout time.Duration
	// Retry applies to the logical call. Its classification defaults to
	// retrying only transient gRPC statuses.
	Retry resilience.RetryPolicy
	// Breaker, when set, wraps the whole call. A call rejected by an open
	// breaker never reaches the network and is not retried.
	Breaker *resilience.CircuitBreaker
	// MaxRecvMsgSize overrides the 4 MiB default.
	MaxRecvMsgSize int
}

// Dial opens a client connection to a dependency.
//
// It does not block: grpc.NewClient connects lazily, so a service can start
// before its dependencies are up. That matters during a rolling deploy, where
// requiring connectivity at startup turns a restart of one service into a
// restart of all of them.
func Dial(_ context.Context, cfg ClientConfig) (*grpc.ClientConn, error) {
	if cfg.Target == "" {
		return nil, fmt.Errorf("grpc client %s: target is required", cfg.Name)
	}

	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	cfg.Logger = log

	opts := []grpc.DialOption{
		// Internal traffic stays inside the cluster network. TLS is terminated
		// by the mesh; adding it here without that would be theatre.
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(unaryClientInterceptor(cfg)),
		// Injects the trace context into the outgoing metadata, which is what
		// lets the callee's span become a child of the caller's rather than a
		// disconnected root.
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	}
	if cfg.MaxRecvMsgSize > 0 {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(cfg.MaxRecvMsgSize)))
	}

	conn, err := grpc.NewClient(cfg.Target, opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc client %s: dial %s: %w", cfg.Name, cfg.Target, err)
	}

	log.Info("grpc client configured",
		slog.String("dependency", cfg.Name),
		slog.String("target", cfg.Target),
		slog.Duration("timeout", cfg.Timeout),
	)
	return conn, nil
}

// unaryClientInterceptor applies the deadline, the retry policy and the circuit
// breaker to every call to a dependency.
//
// The composition is deliberate: the breaker is *outside* the retry loop, so an
// open breaker fails fast without spending the retry budget, and one logical
// call counts as one breaker outcome rather than one per attempt. Counting per
// attempt would open the breaker after a single failed call that had four
// attempts, which is too trigger-happy.
func unaryClientInterceptor(cfg ClientConfig) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		callCtx := ctx
		if cfg.Timeout > 0 {
			var cancel context.CancelFunc
			callCtx, cancel = context.WithTimeout(ctx, cfg.Timeout)
			defer cancel()
		}

		invoke := func(ctx context.Context) error {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		run := func(ctx context.Context) error {
			return withRetry(ctx, cfg, method, invoke)
		}

		start := time.Now()

		var err error
		if cfg.Breaker != nil {
			done, allowErr := cfg.Breaker.Allow()
			if allowErr != nil {
				// Reported as an unavailable dependency, because from the
				// caller's point of view that is exactly what it is.
				return apperr.GRPC(apperr.Wrap(cfg.Name, apperr.CodeUnavailable,
					"dependency is unavailable, request not attempted", allowErr))
			}
			err = run(callCtx)
			done(err == nil)
		} else {
			err = run(callCtx)
		}

		attrs := []any{
			slog.String("dependency", cfg.Name),
			slog.String("method", method),
			slog.Duration("duration", time.Since(start)),
		}
		if err != nil {
			attrs = append(attrs, slog.String("error", err.Error()))
			cfg.Logger.WarnContext(ctx, "dependency call failed", attrs...)
		} else {
			cfg.Logger.DebugContext(ctx, "dependency call", attrs...)
		}

		return err
	}
}

// withRetry runs invoke under the configured retry policy, reading a BaseDelay
// of zero as "no retry".
func withRetry(
	ctx context.Context,
	cfg ClientConfig,
	method string,
	invoke func(context.Context) error,
) error {
	policy := cfg.Retry
	if policy.MaxAttempts <= 0 {
		return invoke(ctx)
	}

	if policy.Retryable == nil {
		// Default classification: a transient gRPC status is retried, a
		// rejected request is not. Retrying InvalidArgument cannot succeed.
		policy.Retryable = func(err error) bool {
			return resilience.RetryableByCode(apperr.FromGRPC(cfg.Name, err))
		}
	}

	if policy.OnRetry == nil {
		policy.OnRetry = func(attempt int, err error, delay time.Duration) {
			cfg.Logger.WarnContext(ctx, "retrying dependency call",
				slog.String("dependency", cfg.Name),
				slog.String("method", method),
				slog.Int("attempt", attempt),
				slog.Duration("retry_in", delay),
				slog.String("error", err.Error()),
			)
		}
	}

	return policy.Do(ctx, fmt.Sprintf("%s.%s", cfg.Name, method), invoke)
}
