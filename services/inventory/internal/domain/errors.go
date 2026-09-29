package domain

import (
	"context"
	"errors"

	"github.com/marketplace/pkg/apperr"
)

// Domain errors.
//
// They are plain sentinel values with no transport meaning: the repository
// returns ErrReservationNotFound, and the gRPC layer decides that means
// `codes.NotFound`. That separation is what lets the same rules serve gRPC, an
// admin HTTP handler and the expiry worker without knowing about any of them.

// ErrInsufficientStock is returned when a request asks for more units than are
// available. It is the ordinary "sold out" answer, not a fault.
var ErrInsufficientStock = errors.New("insufficient stock")

// ErrReservationNotFound is returned when a reservation id is unknown.
var ErrReservationNotFound = errors.New("reservation not found")

// ErrStockNotFound is returned when a product has no stock row at all.
//
// Kept distinct from ErrInsufficientStock on purpose. "We have none left" and
// "we have never heard of this product" are different incidents with different
// owners — one is a warehouse problem, the other is a catalogue problem — and
// collapsing them would send every operator to the wrong team.
var ErrStockNotFound = errors.New("stock not found")

// ErrInvalidRequest is returned for input that can never succeed, however many
// times it is retried: a malformed line, a quantity beyond the bounds, a hold
// that has already been released.
var ErrInvalidRequest = errors.New("invalid request")

// op namespaces the operation in error messages, so every error from this
// service is attributable without inspecting its text.
const op = "inventory"

// AsAppError maps a domain error onto the shared transport-neutral taxonomy.
//
// This is the single translation point between "what went wrong in the
// business" and "what the transport should say about it". It lives in the
// domain because the mapping is a statement about the rules — that a sold-out
// product is a precondition the caller cannot fix by retrying, and a missing
// reservation is not — rather than about gRPC or HTTP.
//
// The messages are the domain error's own text, minus nothing: they name a
// product, a reservation or a requested quantity, all of which the caller
// already knows, and none of which reveals anything about this service's
// internals. Anything unclassified becomes Internal, whose message is never
// returned, because an unrecognised error is a bug or an outage and its text
// may name a type or a host.
//
// Mapping is idempotent: an error that already carries a Code is returned
// unchanged. The translation legitimately happens at more than one boundary in
// a real request path, and a second pass would otherwise fall into the default
// branch and downgrade an actionable NotFound to an opaque Internal.
func AsAppError(err error) error {
	if err == nil {
		return nil
	}

	var mapped *apperr.Error
	if errors.As(err, &mapped) {
		return mapped
	}

	switch {
	case errors.Is(err, ErrInsufficientStock):
		// FailedPrecondition, not InvalidArgument: the request is perfectly
		// well formed, the world simply does not contain that much stock. It is
		// also not retryable, which the saga must be able to tell.
		return apperr.Wrap(op, apperr.CodeFailedPrecondition, err.Error(), err)

	case errors.Is(err, ErrReservationNotFound), errors.Is(err, ErrStockNotFound):
		return apperr.Wrap(op, apperr.CodeNotFound, err.Error(), err)

	case errors.Is(err, ErrInvalidRequest):
		// This also covers the state-machine refusals — committing a released
		// hold, releasing a committed one. They are classified here rather than
		// under a code of their own because what the caller needs to know is
		// the same in every case: this request cannot succeed, so do not retry
		// it. Which state it found is in the message, and in the logs.
		return apperr.Wrap(op, apperr.CodeInvalidArgument, err.Error(), err)

	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// A call that was abandoned before it began is not a fault of this
		// service. Reporting it as Internal would inflate the error rate with
		// client disconnects and hide the failures that need attention.
		return apperr.Wrap(op, apperr.CodeOf(err), "request was cancelled before it could be applied", err)

	default:
		return apperr.Wrap(op, apperr.CodeInternal, "internal error", err)
	}
}
