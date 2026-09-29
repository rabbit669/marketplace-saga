// Package grpcx provides the gRPC server and client plumbing shared by every
// service.
//
// Services speak gRPC internally and REST only at the gateway (ADR-0004). That
// makes this package the transport boundary for six of the seven services, and
// it is where three cross-cutting concerns are handled exactly once:
//
//  1. **Panics become errors.** A panic in a handler would otherwise kill the
//     whole process, taking down every in-flight request rather than the one
//     that misbehaved.
//  2. **Errors become statuses.** Domain code returns *apperr.Error values with
//     a transport-neutral Code; this package translates that Code into a gRPC
//     status and back again on the client side, so no handler ever imports
//     `codes`.
//  3. **Every call is logged with its outcome.** Method, duration and status
//     code, correlated with the trace id.
package grpcx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"runtime/debug"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/marketplace/pkg/apperr"
	"github.com/marketplace/pkg/logger"
)

// ServerConfig describes a gRPC server.
type ServerConfig struct {
	// Name identifies the service in logs, e.g. "order".
	Name string
	// Logger receives one record per call. Defaults to slog.Default.
	Logger *slog.Logger
	// UnaryInterceptors are appended after this package's own, so a service can
	// add tracing or authentication without losing panic recovery.
	UnaryInterceptors []grpc.UnaryServerInterceptor
	// MaxRecvMsgSize overrides the 4 MiB default. Batch calls such as
	// GetProductsBatch can legitimately exceed it.
	MaxRecvMsgSize int
	// Metrics receives one observation per call. It is an interface rather than
	// a Prometheus type so that this package has no opinion about the metrics
	// backend, and a service can pass nil when it has none.
	Metrics MetricsRecorder
}

// MetricsRecorder receives the outcome of every RPC.
//
// The method is the full gRPC method name and the code is the final status, so
// an implementation can compute rate, error ratio and latency from one
// observation without correlating anything.
type MetricsRecorder interface {
	ObserveRPC(ctx context.Context, method string, code codes.Code, duration time.Duration)
	RPCStarted()
	RPCFinished()
}

// NewServer builds a gRPC server with the standard interceptor chain installed.
//
// Interceptor order is outermost first, and it is load bearing:
//
//	logging  — wraps everything, so it records the final status of *every* call,
//	           including one that panicked. Put inside recovery, it would be
//	           skipped entirely for exactly the calls an operator most wants to
//	           see.
//	mapping  — outside recovery so that the error recovery produces is
//	           translated too. Put inside it, a recovered panic reaches the
//	           client as codes.Unknown rather than Internal, because nothing
//	           converted it.
//	recovery — innermost of the three, so it is the last thing between the
//	           handler and the wire.
func NewServer(cfg ServerConfig) *grpc.Server {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	interceptors := []grpc.UnaryServerInterceptor{
		metricsInterceptor(cfg.Metrics),
		loggingInterceptor(log),
		errorInterceptor(),
		recoveryInterceptor(log),
	}
	interceptors = append(interceptors, cfg.UnaryInterceptors...)

	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(interceptors...),
		grpc.ChainStreamInterceptor(streamLoggingInterceptor(log), streamErrorInterceptor(), streamRecoveryInterceptor(log)),
		// A stats handler rather than an interceptor: it sees the transport
		// events, so it also covers the metadata exchange that carries the trace
		// context. An interceptor sees only the call, which is enough to start a
		// span but not to link it to the caller's.
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	}
	if cfg.MaxRecvMsgSize > 0 {
		opts = append(opts, grpc.MaxRecvMsgSize(cfg.MaxRecvMsgSize))
	}

	return grpc.NewServer(opts...)
}

// metricsInterceptor records the outcome of every call.
//
// It is outermost so that it observes the final status — including one produced
// by recovery, and including the latency of a call that spent its time in
// retries. Placed inside the chain it would miss exactly the slow and failing
// calls an operator is looking for.
func metricsInterceptor(m MetricsRecorder) grpc.UnaryServerInterceptor {
	if m == nil {
		return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			return handler(ctx, req)
		}
	}

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		m.RPCStarted()

		resp, err := handler(ctx, req)

		m.RPCFinished()
		m.ObserveRPC(ctx, info.FullMethod, status.Code(err), time.Since(start))
		return resp, err
	}
}

// recoveryInterceptor turns a panic in a handler into an Internal error.
//
// Without it, a single nil dereference in one request handler terminates the
// process and drops every other in-flight request with it. The stack trace is
// logged but never returned: it names internal types and file paths.
func recoveryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.ErrorContext(ctx, "panic recovered in gRPC handler",
					slog.String("method", info.FullMethod),
					slog.Any("panic", r),
					slog.String("stack", string(debug.Stack())),
				)
				resp = nil
				err = apperr.Internal(info.FullMethod, "internal error")
			}
		}()

		return handler(ctx, req)
	}
}

// streamLoggingInterceptor records one line per stream call.
func streamLoggingInterceptor(log *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)

		attrs := []any{
			slog.String("method", info.FullMethod),
			slog.Duration("duration", time.Since(start)),
			slog.String("code", status.Code(err).String()),
			slog.Bool("stream", true),
		}
		if err != nil {
			log.DebugContext(ss.Context(), "grpc stream call rejected", append(attrs, slog.String("error", err.Error()))...)
		} else {
			log.DebugContext(ss.Context(), "grpc stream call", attrs...)
		}
		return err
	}
}

// streamErrorInterceptor converts an apperr returned by a stream handler.
func streamErrorInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, ss)
		if err == nil {
			return nil
		}
		if _, ok := status.FromError(err); ok && !isAppError(err) {
			return err
		}
		return apperr.GRPC(err)
	}
}

func streamRecoveryInterceptor(log *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				log.ErrorContext(ss.Context(), "panic recovered in gRPC stream handler",
					slog.String("method", info.FullMethod),
					slog.Any("panic", r),
					slog.String("stack", string(debug.Stack())),
				)
				err = apperr.Internal(info.FullMethod, "internal error")
			}
		}()

		return handler(srv, ss)
	}
}

// loggingInterceptor records one line per call with its outcome.
func loggingInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()

		resp, err := handler(ctx, req)

		attrs := []any{
			slog.String("method", info.FullMethod),
			slog.Duration("duration", time.Since(start)),
			slog.String("code", status.Code(err).String()),
		}

		switch {
		case err == nil:
			log.DebugContext(ctx, "grpc call", attrs...)
		case status.Code(err) == codes.Internal:
			// An Internal error is a bug or an outage: it is logged at error
			// level with the cause, which the caller never sees.
			log.ErrorContext(ctx, "grpc call failed", append(attrs, slog.String("error", err.Error()))...)
		default:
			log.DebugContext(ctx, "grpc call rejected", append(attrs, slog.String("error", err.Error()))...)
		}

		return resp, err
	}
}

// errorInterceptor converts an apperr into a gRPC status.
//
// Handlers return ordinary errors and never construct statuses themselves; this
// is the single place where the domain's vocabulary becomes the transport's.
func errorInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		resp, err := handler(ctx, req)
		if err == nil {
			return resp, nil
		}

		// Already a status (from an interceptor above, or a gRPC-internal
		// failure): leave it alone rather than wrapping it twice.
		if _, ok := status.FromError(err); ok && !isAppError(err) {
			return nil, err
		}

		// An op-less conversion would lose the method name in logs, so fill it
		// in when the handler did not.
		var appErr *apperr.Error
		if errors.As(err, &appErr) && appErr.Op == "" {
			appErr.Op = info.FullMethod
		}

		return nil, apperr.GRPC(err)
	}
}

func isAppError(err error) bool {
	var appErr *apperr.Error
	return errors.As(err, &appErr)
}

// Serve runs srv on addr until ctx is cancelled, then drains in-flight calls.
//
// GracefulStop waits for active RPCs to finish, which matters for a saga step
// that is mid-write. A client that has already sent a request gets a real answer
// instead of a connection reset, so it does not have to guess whether its write
// landed — the exact ambiguity that makes compensations necessary in the first
// place.
func Serve(ctx context.Context, srv *grpc.Server, addr string, log *slog.Logger, drain time.Duration) error {
	if log == nil {
		log = slog.Default()
	}
	if drain <= 0 {
		drain = 10 * time.Second
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("grpc: listen on %s: %w", addr, err)
	}

	log.Info("grpc server listening", slog.String("addr", addr))

	serveErr := make(chan error, 1)
	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serveErr <- fmt.Errorf("grpc: serve: %w", err)
		}
	}()

	select {
	case err := <-serveErr:
		return err

	case <-ctx.Done():
		log.Info("grpc server draining", slog.Duration("grace", drain))
	}

	stopped := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		log.Info("grpc server stopped")
	case <-time.After(drain):
		// A client that will not go away must not hold the process open past
		// the orchestrator's grace period; it would be SIGKILLed anyway, with
		// even less chance to log.
		log.Warn("grpc graceful stop timed out, forcing")
		srv.Stop()
	}

	return nil
}

// LogFrom extracts the request-scoped logger, so interceptors and handlers do
// not each have to reach into the context themselves.
func LogFrom(ctx context.Context) *slog.Logger { return logger.FromContext(ctx) }
