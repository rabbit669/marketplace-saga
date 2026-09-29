package apperr

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCodeOf(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Code
	}{
		{"nil", nil, ""},
		{"direct", NotFound("order.Get", "order %s missing", "o-1"), CodeNotFound},
		{"wrapped twice", fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", Conflict("order.Create", "stale"))), CodeConflict},
		{"plain error", errors.New("boom"), CodeInternal},
		{"context canceled", context.Canceled, CodeUnavailable},
		{"deadline exceeded", context.DeadlineExceeded, CodeDeadlineExceeded},
		{"grpc not found", status.Error(codes.NotFound, "nope"), CodeNotFound},
		{"grpc aborted maps to conflict", status.Error(codes.Aborted, "nope"), CodeConflict},
		{"grpc internal", status.Error(codes.Internal, "nope"), CodeInternal},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := CodeOf(tc.err); got != tc.want {
				t.Errorf("CodeOf() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestIsMatchesOnCode(t *testing.T) {
	err := fmt.Errorf("checkout failed: %w", NotFound("order.Get", "order o-42 does not exist"))

	if !Is(err, CodeNotFound) {
		t.Error("Is(err, CodeNotFound) = false, want true")
	}
	if Is(err, CodeConflict) {
		t.Error("Is(err, CodeConflict) = true, want false")
	}
}

func TestError_IsIgnoresEmptyOpInTarget(t *testing.T) {
	err := NotFound("order.Get", "missing")

	if !errors.Is(err, &Error{Code: CodeNotFound}) {
		t.Error("a target with no Op should match on Code alone")
	}
	if errors.Is(err, &Error{Code: CodeNotFound, Op: "catalog.Get"}) {
		t.Error("a target with a different Op must not match")
	}
}

func TestWrapReturnsNilForNilCause(t *testing.T) {
	// Callers rely on `return Wrap(...)` being a no-op on the success path.
	if err := Wrap("order.Get", CodeInternal, "failed", nil); err != nil {
		t.Errorf("Wrap(nil) = %v, want nil", err)
	}
	if err := Wrapf("order.Get", CodeInternal, nil, "failed %d", 1); err != nil {
		t.Errorf("Wrapf(nil) = %v, want nil", err)
	}
}

func TestError_WithMeta(t *testing.T) {
	err := InvalidArgument("order.Create", "quantity must be positive").
		WithMeta("field", "quantity").
		WithMeta("order_id", "o-1")

	if err.Meta["field"] != "quantity" || err.Meta["order_id"] != "o-1" {
		t.Errorf("Meta = %v, want both keys present", err.Meta)
	}
	// WithMeta mutates and returns the same value, so it chains inline.
	if !Is(err, CodeInvalidArgument) {
		t.Error("WithMeta must not change the code")
	}
}

func TestGRPCRoundTrip(t *testing.T) {
	original := NotFound("order.Get", "order o-7 does not exist")

	converted := GRPC(original)
	if converted == nil {
		t.Fatal("GRPC() = nil, want a status error")
	}

	st, ok := status.FromError(converted)
	if !ok {
		t.Fatal("GRPC() did not produce a gRPC status error")
	}
	if st.Code() != codes.NotFound {
		t.Errorf("status code = %v, want NotFound", st.Code())
	}
	if st.Message() != "order o-7 does not exist" {
		t.Errorf("status message = %q, want the public message", st.Message())
	}

	back := FromGRPC("order.Get", converted)
	if back.Code != CodeNotFound {
		t.Errorf("round-tripped code = %q, want %q", back.Code, CodeNotFound)
	}
}

func TestGRPCAttachesStructuredMetadata(t *testing.T) {
	err := InvalidArgument("order.Create", "bad quantity").WithMeta("field", "quantity")

	st, ok := status.FromError(GRPC(err))
	if !ok {
		t.Fatal("expected a gRPC status error")
	}
	if len(st.Details()) == 0 {
		t.Fatal("expected an ErrorInfo detail carrying the metadata")
	}
}

func TestGRPC_NilAndForeignErrors(t *testing.T) {
	if got := GRPC(nil); got != nil {
		t.Errorf("GRPC(nil) = %v, want nil", got)
	}

	// A foreign error must not leak its text through the API boundary.
	st, ok := status.FromError(GRPC(errors.New("pq: relation \"orders\" does not exist")))
	if !ok {
		t.Fatal("expected a status error")
	}
	if st.Code() != codes.Internal {
		t.Errorf("code = %v, want Internal", st.Code())
	}
	if st.Message() != "internal error" {
		t.Errorf("message = %q, want the opaque fallback", st.Message())
	}
}

func TestGRPC_PassesThroughExistingStatusErrors(t *testing.T) {
	original := status.Error(codes.Unavailable, "upstream down")

	if got := GRPC(original); got.Error() != original.Error() {
		t.Errorf("GRPC() = %v, want the original status error unchanged", got)
	}
}

func TestHTTPStatus(t *testing.T) {
	tests := []struct {
		code Code
		want int
	}{
		{CodeInvalidArgument, http.StatusBadRequest},
		{CodeNotFound, http.StatusNotFound},
		{CodeAlreadyExists, http.StatusConflict},
		{CodeConflict, http.StatusConflict},
		{CodeFailedPrecondition, http.StatusUnprocessableEntity},
		{CodeUnauthenticated, http.StatusUnauthorized},
		{CodePermissionDenied, http.StatusForbidden},
		{CodeResourceExhausted, http.StatusTooManyRequests},
		{CodeUnavailable, http.StatusServiceUnavailable},
		{CodeDeadlineExceeded, http.StatusGatewayTimeout},
		{CodeInternal, http.StatusInternalServerError},
	}

	for _, tc := range tests {
		t.Run(string(tc.code), func(t *testing.T) {
			if got := HTTPStatus(New("op", tc.code, "msg")); got != tc.want {
				t.Errorf("HTTPStatus(%s) = %d, want %d", tc.code, got, tc.want)
			}
		})
	}

	if got := HTTPStatus(errors.New("raw")); got != http.StatusInternalServerError {
		t.Errorf("HTTPStatus(unknown) = %d, want 500", got)
	}
}

func TestPublicMessage(t *testing.T) {
	if got := PublicMessage(InvalidArgument("op", "quantity must be positive")); got != "quantity must be positive" {
		t.Errorf("PublicMessage = %q, want the original message", got)
	}

	// Internal details must never reach the caller, even when wrapped.
	internal := Wrap("order.Create", CodeInternal, "could not persist order", errors.New("pq: deadlock detected on orders_pkey"))
	if got := PublicMessage(internal); got != "internal error" {
		t.Errorf("PublicMessage(internal) = %q, want %q", got, "internal error")
	}

	if got := PublicMessage(errors.New("some raw failure")); got != "internal error" {
		t.Errorf("PublicMessage(raw) = %q, want %q", got, "internal error")
	}
}

func TestError_ErrorStringIncludesCause(t *testing.T) {
	cause := errors.New("connection refused")
	err := Wrap("payment.Authorize", CodeUnavailable, "provider unreachable", cause)

	msg := err.Error()
	for _, want := range []string{"payment.Authorize", string(CodeUnavailable), "provider unreachable", "connection refused"} {
		if !strings.Contains(msg, want) {
			t.Errorf("Error() = %q, want it to contain %q", msg, want)
		}
	}
	if !errors.Is(err, cause) {
		t.Error("Unwrap must expose the cause to errors.Is")
	}
}

func TestPublicMessage_UnderstandsGRPCStatusErrors(t *testing.T) {
	// Regression: a message that arrived from another service over gRPC used to
	// be replaced with "internal error" at the gateway, because PublicMessage
	// only recognised *Error values produced in the same process. The effect was
	// that a declined card reached the customer as an opaque server fault, with
	// the explanation — the one useful part — stripped off.
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "failed precondition keeps its message",
			err:  status.Error(codes.FailedPrecondition, "payment was declined: insufficient_funds"),
			want: "payment was declined: insufficient_funds",
		},
		{
			name: "not found keeps its message",
			err:  status.Error(codes.NotFound, "order o-1 does not exist"),
			want: "order o-1 does not exist",
		},
		{
			name: "invalid argument keeps its message",
			err:  status.Error(codes.InvalidArgument, "quantity must be positive"),
			want: "quantity must be positive",
		},
		{
			name: "internal is still opaque",
			err:  status.Error(codes.Internal, `pq: relation "orders" does not exist`),
			want: "internal error",
		},
		{
			name: "unknown is opaque",
			err:  status.Error(codes.Unknown, "something went wrong internally"),
			want: "internal error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := PublicMessage(tc.err); got != tc.want {
				t.Errorf("PublicMessage() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPublicMessage_GRPCAndInProcessAgree(t *testing.T) {
	// Whichever side of a service boundary an error is observed from, the code
	// and the message a caller sees must be the same. When they disagree, the
	// bug looks like "the API says 422 but the message says internal error" —
	// which is precisely how this was found.
	inProcess := FailedPrecondition("order.CreateOrder", "payment was declined: insufficient_funds")
	overTheWire := status.Error(codes.FailedPrecondition, "payment was declined: insufficient_funds")

	if CodeOf(inProcess) != CodeOf(overTheWire) {
		t.Errorf("codes disagree: %q vs %q", CodeOf(inProcess), CodeOf(overTheWire))
	}
	if PublicMessage(inProcess) != PublicMessage(overTheWire) {
		t.Errorf("messages disagree: %q vs %q", PublicMessage(inProcess), PublicMessage(overTheWire))
	}
}
