// Package apperr defines the error taxonomy shared by every service.
//
// Services speak gRPC internally and HTTP at the edge, and both need to agree
// on what went wrong. Rather than sprinkling status codes through business
// logic, code returns *apperr.Error values carrying a transport-neutral Code;
// the gRPC and HTTP layers translate that Code at the boundary.
//
// The important consequence: domain code never imports grpc or net/http, and a
// new transport can be added without touching business rules.
package apperr

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Code is a transport-neutral classification of a failure.
type Code string

const (
	// CodeInvalidArgument means the caller sent something malformed. Retrying
	// the same request will not help.
	CodeInvalidArgument Code = "invalid_argument"
	// CodeNotFound means the addressed resource does not exist.
	CodeNotFound Code = "not_found"
	// CodeAlreadyExists means a uniqueness constraint was violated.
	CodeAlreadyExists Code = "already_exists"
	// CodeConflict means the request conflicts with current state, typically a
	// concurrent modification or a state machine that refuses the transition.
	CodeConflict Code = "conflict"
	// CodeFailedPrecondition means the request is well formed but the system is
	// not in a state where it can be applied.
	CodeFailedPrecondition Code = "failed_precondition"
	// CodeUnauthenticated means credentials are missing or invalid.
	CodeUnauthenticated Code = "unauthenticated"
	// CodePermissionDenied means the caller is authenticated but not allowed.
	CodePermissionDenied Code = "permission_denied"
	// CodeResourceExhausted means a quota or rate limit was hit.
	CodeResourceExhausted Code = "resource_exhausted"
	// CodeUnavailable means a dependency is down. Retrying may help.
	CodeUnavailable Code = "unavailable"
	// CodeDeadlineExceeded means the operation ran out of time. Retrying may help.
	CodeDeadlineExceeded Code = "deadline_exceeded"
	// CodeInternal means an unexpected failure. Never expose details to callers.
	CodeInternal Code = "internal"
)

// Error is the canonical error type produced by services.
type Error struct {
	// Code classifies the failure.
	Code Code
	// Op names the operation that failed, e.g. "order.CreateOrder".
	Op string
	// Message is safe to return to the caller.
	Message string
	// Err is the wrapped cause, if any. It is logged but never returned to the
	// caller, because it frequently contains internal detail.
	Err error
	// Meta carries structured context such as the offending field name.
	Meta map[string]string
}

// Error implements the error interface.
func (e *Error) Error() string {
	switch {
	case e.Op != "" && e.Err != nil:
		return fmt.Sprintf("%s: %s: %s: %v", e.Op, e.Code, e.Message, e.Err)
	case e.Op != "":
		return fmt.Sprintf("%s: %s: %s", e.Op, e.Code, e.Message)
	case e.Err != nil:
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Err)
	default:
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
}

// Unwrap exposes the cause to errors.Is and errors.As.
func (e *Error) Unwrap() error { return e.Err }

// Is reports two errors as matching when they share a Code and Op. This lets
// callers write errors.Is(err, apperr.NotFound("order.Get", "")) without
// depending on message text.
func (e *Error) Is(target error) bool {
	var t *Error
	if !errors.As(target, &t) {
		return false
	}
	if e.Code != t.Code {
		return false
	}
	return t.Op == "" || e.Op == t.Op
}

// WithMeta attaches a structured key/value pair and returns the error, so it
// can be used inline: return apperr.NotFoundf(...).WithMeta("order_id", id).
func (e *Error) WithMeta(k, v string) *Error {
	if e.Meta == nil {
		e.Meta = make(map[string]string, 2)
	}
	e.Meta[k] = v
	return e
}

// New builds an Error with a literal message.
func New(op string, code Code, msg string) *Error {
	return &Error{Op: op, Code: code, Message: msg}
}

// Newf builds an Error with a formatted message.
func Newf(op string, code Code, format string, args ...any) *Error {
	return &Error{Op: op, Code: code, Message: fmt.Sprintf(format, args...)}
}

// Wrap builds an Error around a cause. When err is nil, Wrap returns nil, which
// makes `return apperr.Wrap(...)` safe on a success path.
func Wrap(op string, code Code, msg string, err error) *Error {
	if err == nil {
		return nil
	}
	return &Error{Op: op, Code: code, Message: msg, Err: err}
}

// Wrapf builds an Error around a cause with a formatted message.
func Wrapf(op string, code Code, err error, format string, args ...any) *Error {
	if err == nil {
		return nil
	}
	return &Error{Op: op, Code: code, Message: fmt.Sprintf(format, args...), Err: err}
}

// ---------------------------------------------------------------- helpers

// InvalidArgument reports malformed input.
func InvalidArgument(op, format string, args ...any) *Error {
	return Newf(op, CodeInvalidArgument, format, args...)
}

// NotFound reports a missing resource.
func NotFound(op, format string, args ...any) *Error {
	return Newf(op, CodeNotFound, format, args...)
}

// AlreadyExists reports a violated uniqueness constraint.
func AlreadyExists(op, format string, args ...any) *Error {
	return Newf(op, CodeAlreadyExists, format, args...)
}

// Conflict reports a state conflict.
func Conflict(op, format string, args ...any) *Error {
	return Newf(op, CodeConflict, format, args...)
}

// FailedPrecondition reports an inapplicable request.
func FailedPrecondition(op, format string, args ...any) *Error {
	return Newf(op, CodeFailedPrecondition, format, args...)
}

// Unauthenticated reports missing or invalid credentials.
func Unauthenticated(op, format string, args ...any) *Error {
	return Newf(op, CodeUnauthenticated, format, args...)
}

// PermissionDenied reports an authorization failure.
func PermissionDenied(op, format string, args ...any) *Error {
	return Newf(op, CodePermissionDenied, format, args...)
}

// Unavailable reports a dependency failure that may succeed on retry.
func Unavailable(op, format string, args ...any) *Error {
	return Newf(op, CodeUnavailable, format, args...)
}

// Internal reports an unexpected failure. Callers see a generic message; the
// real cause stays in the wrapped error for the logs.
func Internal(op, format string, args ...any) *Error {
	return Newf(op, CodeInternal, format, args...)
}

// ------------------------------------------------------------- inspection

// CodeOf extracts the Code from err. It understands *Error, wrapped *Error
// values, gRPC status errors, context cancellation and deadline expiry.
// Anything unrecognised is CodeInternal.
func CodeOf(err error) Code {
	if err == nil {
		return ""
	}

	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}

	switch {
	case errors.Is(err, context.Canceled):
		return CodeUnavailable
	case errors.Is(err, context.DeadlineExceeded):
		return CodeDeadlineExceeded
	}

	if s, ok := status.FromError(err); ok {
		if c, known := codeFromGRPC(s.Code()); known {
			return c
		}
	}

	return CodeInternal
}

// Is reports whether err carries the given Code anywhere in its chain.
func Is(err error, code Code) bool {
	return CodeOf(err) == code
}

// ------------------------------------------------------------------ gRPC

// GRPC converts err into a gRPC status error, preserving the Code and any
// structured metadata. It is applied by the server interceptor, so individual
// handlers just return ordinary errors.
func GRPC(err error) error {
	if err == nil {
		return nil
	}

	var e *Error
	if !errors.As(err, &e) {
		// Not ours: do not leak internals to the caller.
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Error(codes.Internal, "internal error")
	}

	st := status.New(codeToGRPC(e.Code), e.Message)

	// Structured metadata travels as an ErrorInfo detail, so a gRPC client can
	// read fields such as the offending field name without parsing the message.
	if len(e.Meta) > 0 {
		info := &errdetails.ErrorInfo{Reason: string(e.Code), Domain: "marketplace", Metadata: e.Meta}
		if withDetails, derr := st.WithDetails(info); derr == nil {
			st = withDetails
		}
		// If the detail is rejected the caller still receives a correct status,
		// so there is nothing useful to do but carry on.
	}
	return st.Err()
}

// FromGRPC converts a gRPC status error back into an *Error, so client-side
// code can branch on Code without importing grpc codes.
func FromGRPC(op string, err error) *Error {
	if err == nil {
		return nil
	}

	var e *Error
	if errors.As(err, &e) {
		return e
	}

	s, ok := status.FromError(err)
	if !ok {
		return &Error{Op: op, Code: CodeInternal, Message: "internal error", Err: err}
	}

	code, _ := codeFromGRPC(s.Code())
	return &Error{Op: op, Code: code, Message: s.Message(), Err: err}
}

func codeToGRPC(c Code) codes.Code {
	switch c {
	case CodeInvalidArgument:
		return codes.InvalidArgument
	case CodeNotFound:
		return codes.NotFound
	case CodeAlreadyExists:
		return codes.AlreadyExists
	case CodeConflict:
		return codes.Aborted
	case CodeFailedPrecondition:
		return codes.FailedPrecondition
	case CodeUnauthenticated:
		return codes.Unauthenticated
	case CodePermissionDenied:
		return codes.PermissionDenied
	case CodeResourceExhausted:
		return codes.ResourceExhausted
	case CodeUnavailable:
		return codes.Unavailable
	case CodeDeadlineExceeded:
		return codes.DeadlineExceeded
	default:
		return codes.Internal
	}
}

func codeFromGRPC(c codes.Code) (Code, bool) {
	switch c {
	case codes.InvalidArgument, codes.OutOfRange:
		return CodeInvalidArgument, true
	case codes.NotFound:
		return CodeNotFound, true
	case codes.AlreadyExists:
		return CodeAlreadyExists, true
	case codes.Aborted:
		return CodeConflict, true
	case codes.FailedPrecondition:
		return CodeFailedPrecondition, true
	case codes.Unauthenticated:
		return CodeUnauthenticated, true
	case codes.PermissionDenied:
		return CodePermissionDenied, true
	case codes.ResourceExhausted:
		return CodeResourceExhausted, true
	case codes.Unavailable:
		return CodeUnavailable, true
	case codes.DeadlineExceeded:
		return CodeDeadlineExceeded, true
	case codes.Canceled:
		return CodeUnavailable, true
	default:
		return CodeInternal, false
	}
}

// ------------------------------------------------------------------ HTTP

// HTTPStatus maps err to the HTTP status the gateway should return.
func HTTPStatus(err error) int {
	return HTTPStatusForCode(CodeOf(err))
}

// HTTPStatusForCode maps a classification to an HTTP status.
//
// It exists separately from HTTPStatus because a failure that travelled as a
// successful response — see the checkout contract, which has to do this because
// gRPC has no partial success — arrives as a code string rather than as an
// error. Both paths must produce the same status, or the same failure would be
// a 422 when raised locally and a 500 when raised remotely.
func HTTPStatusForCode(code Code) int {
	switch code {
	case CodeInvalidArgument:
		return http.StatusBadRequest
	case CodeNotFound:
		return http.StatusNotFound
	case CodeAlreadyExists, CodeConflict:
		return http.StatusConflict
	case CodeFailedPrecondition:
		return http.StatusUnprocessableEntity
	case CodeUnauthenticated:
		return http.StatusUnauthorized
	case CodePermissionDenied:
		return http.StatusForbidden
	case CodeResourceExhausted:
		return http.StatusTooManyRequests
	case CodeUnavailable:
		return http.StatusServiceUnavailable
	case CodeDeadlineExceeded:
		return http.StatusGatewayTimeout
	default:
		return http.StatusInternalServerError
	}
}

// PublicMessage returns a message safe to send to an external caller. Internal
// failures are deliberately opaque so that implementation details, SQL text and
// hostnames cannot leak through the API.
//
// It understands both halves of the taxonomy: an *Error that was produced
// in-process, and a gRPC status error that arrived over the wire. Handling only
// the first was a real bug — a service boundary turned every remote failure into
// "internal error", so a declined card reached the customer as an opaque server
// fault with the explanation stripped off.
func PublicMessage(err error) string {
	if err == nil {
		return ""
	}

	var e *Error
	if errors.As(err, &e) {
		if e.Code == CodeInternal {
			return "internal error"
		}
		return e.Message
	}

	// A transport error that nobody converted. Its status code carries the same
	// classification the in-process Code would, so the message is exactly as
	// safe — and exactly as unsafe for Internal.
	if s, ok := status.FromError(err); ok {
		if code, known := codeFromGRPC(s.Code()); known && code != CodeInternal {
			return s.Message()
		}
		return "internal error"
	}

	return "internal error"
}
