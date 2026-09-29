package grpcx_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/marketplace/pkg/apperr"
	"github.com/marketplace/pkg/grpcx"
)

// The tests here drive a real gRPC server over an in-process listener rather
// than calling the interceptors directly. That is deliberate: the bug these
// tests exist to prevent was an *ordering* mistake between interceptors, and an
// ordering mistake is invisible unless the whole chain runs.

const testServiceName = "grpcx.test.Service"

// echoService is a hand-written ServiceDesc, so the tests depend on nothing in
// pkg/proto and stay readable.
func echoService(handler func(context.Context) (any, error)) *grpc.ServiceDesc {
	return &grpc.ServiceDesc{
		ServiceName: testServiceName,
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Call",
				Handler: func(
					srv any,
					ctx context.Context,
					_ func(any) error,
					interceptor grpc.UnaryServerInterceptor,
				) (any, error) {
					if interceptor == nil {
						return handler(ctx)
					}
					info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/" + testServiceName + "/Call"}
					return interceptor(ctx, nil, info, func(context.Context, any) (any, error) {
						return handler(ctx)
					})
				},
			},
		},
		Metadata: "grpcx_test",
	}
}

// startServer runs a server with the given handler and returns a client for it.
func startServer(t *testing.T, log *slog.Logger, handler func(context.Context) (any, error)) *grpc.ClientConn {
	t.Helper()

	lis := bufconn.Listen(1 << 20)

	srv := grpcx.NewServer(grpcx.ServerConfig{Name: "test", Logger: log})
	srv.RegisterService(echoService(handler), struct{}{})

	go func() {
		// Serve returns when the listener is closed by the cleanup below.
		_ = srv.Serve(lis)
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	})

	return conn
}

// call invokes the test method.
//
// Both request and reply are real proto messages: with a nil reply, the client
// fails while unmarshalling a successful response, which would make the
// success-path assertions test the wrong thing.
func call(ctx context.Context, conn *grpc.ClientConn) error {
	return conn.Invoke(ctx, "/"+testServiceName+"/Call", &emptypb.Empty{}, &emptypb.Empty{})
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
}

func TestServer_PanicBecomesInternalNotUnknown(t *testing.T) {
	// The regression this guards: with recovery installed *outside* the error
	// mapper, a recovered panic was never translated, so it reached the client
	// as codes.Unknown. A caller branching on Internal — "our fault, alert" —
	// would have classified it as something else entirely.
	conn := startServer(t, discardLogger(), func(context.Context) (any, error) {
		panic("boom: a nil map write, presumably")
	})

	err := call(context.Background(), conn)
	if err == nil {
		t.Fatal("call returned nil, want an error")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error is not a gRPC status: %v", err)
	}
	if st.Code() != codes.Internal {
		t.Errorf("code = %v, want Internal", st.Code())
	}
	if st.Code() == codes.Unknown {
		t.Error("a recovered panic reached the client as Unknown, so the error mapper is not in its path")
	}
}

func TestServer_PanicDetailIsNotLeaked(t *testing.T) {
	conn := startServer(t, discardLogger(), func(context.Context) (any, error) {
		panic("secret internal detail: postgres://user:hunter2@db")
	})

	err := call(context.Background(), conn)
	st, _ := status.FromError(err)

	if strings.Contains(st.Message(), "hunter2") || strings.Contains(st.Message(), "secret") {
		t.Errorf("panic detail leaked to the caller: %q", st.Message())
	}
	if st.Message() != "internal error" {
		t.Errorf("message = %q, want the opaque fallback", st.Message())
	}
}

func TestServer_PanicIsLoggedWithAStackTrace(t *testing.T) {
	// The panic value is useless without knowing where it came from, and the
	// caller must not be told. So the stack has to be in the log.
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	conn := startServer(t, log, func(context.Context) (any, error) {
		panic("deliberate")
	})

	_ = call(context.Background(), conn)

	out := buf.String()
	if !strings.Contains(out, "panic recovered") {
		t.Errorf("no panic record was logged:\n%s", out)
	}
	if !strings.Contains(out, "grpcx_test") {
		t.Errorf("the log has no stack trace naming the panic site:\n%s", out)
	}
}

func TestServer_AppErrorsKeepTheirCode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"not found", apperr.NotFound("order.Get", "no such order"), codes.NotFound},
		{"invalid argument", apperr.InvalidArgument("order.Create", "quantity must be positive"), codes.InvalidArgument},
		{"already exists", apperr.AlreadyExists("order.Create", "duplicate"), codes.AlreadyExists},
		{"conflict", apperr.Conflict("order.Create", "stale"), codes.Aborted},
		{"failed precondition", apperr.FailedPrecondition("order.Create", "declined"), codes.FailedPrecondition},
		{"unavailable", apperr.Unavailable("payment", "down"), codes.Unavailable},
		{"unauthenticated", apperr.Unauthenticated("gateway", "no token"), codes.Unauthenticated},
		{"permission denied", apperr.PermissionDenied("gateway", "nope"), codes.PermissionDenied},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn := startServer(t, discardLogger(), func(context.Context) (any, error) {
				return nil, tc.err
			})

			st, _ := status.FromError(call(context.Background(), conn))
			if st.Code() != tc.want {
				t.Errorf("code = %v, want %v", st.Code(), tc.want)
			}
		})
	}
}

func TestServer_ForeignErrorsAreNotLeaked(t *testing.T) {
	// A database driver error carries table names, hostnames and sometimes
	// credentials. It must become an opaque Internal.
	conn := startServer(t, discardLogger(), func(context.Context) (any, error) {
		return nil, errors.New(`pq: password authentication failed for user "marketplace"`)
	})

	err := call(context.Background(), conn)
	st, _ := status.FromError(err)

	if st.Code() != codes.Internal {
		t.Errorf("code = %v, want Internal", st.Code())
	}
	if strings.Contains(st.Message(), "authentication") || strings.Contains(st.Message(), "marketplace") {
		t.Errorf("internal error text leaked: %q", st.Message())
	}
}

func TestServer_ExistingStatusErrorsPassThrough(t *testing.T) {
	// An interceptor deeper in the chain, or a handler that legitimately returns
	// a status, must not have it wrapped twice.
	original := status.Error(codes.ResourceExhausted, "slow down")

	conn := startServer(t, discardLogger(), func(context.Context) (any, error) {
		return nil, original
	})

	err := call(context.Background(), conn)
	st, _ := status.FromError(err)

	if st.Code() != codes.ResourceExhausted {
		t.Errorf("code = %v, want ResourceExhausted", st.Code())
	}
	if st.Message() != "slow down" {
		t.Errorf("message = %q, want the original", st.Message())
	}
}

func TestServer_EveryCallIsLoggedEvenWhenItPanics(t *testing.T) {
	// Logging is outermost precisely so that a panicking call still produces a
	// record. An operator debugging a 500 needs to see that the request
	// happened.
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	conn := startServer(t, log, func(context.Context) (any, error) {
		panic("kaboom")
	})

	_ = call(context.Background(), conn)

	out := buf.String()
	if !strings.Contains(out, testServiceName) {
		t.Errorf("no access log line for the panicking call:\n%s", out)
	}
	if !strings.Contains(out, "Internal") {
		t.Errorf("the access log does not record the final status:\n%s", out)
	}
}

func TestServer_SuccessIsLogged(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	conn := startServer(t, log, func(context.Context) (any, error) { return &emptypb.Empty{}, nil })

	if err := call(context.Background(), conn); err != nil {
		t.Fatalf("call: %v", err)
	}
	if !strings.Contains(buf.String(), testServiceName) {
		t.Errorf("no access log line for a successful call:\n%s", buf.String())
	}
}

func TestServe_StopsOnContextCancellation(t *testing.T) {
	srv := grpcx.NewServer(grpcx.ServerConfig{Name: "test", Logger: discardLogger()})
	srv.RegisterService(echoService(func(context.Context) (any, error) { return &emptypb.Empty{}, nil }), struct{}{})

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		// Port 0 lets the OS choose, so the test cannot collide with a developer's
		// running stack.
		done <- grpcx.Serve(ctx, srv, "127.0.0.1:0", discardLogger(), time.Second)
	}()

	// Give the listener a moment to bind before cancelling.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Serve = %v, want nil after a clean shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("Serve did not return within 5s of cancellation")
	}
}
