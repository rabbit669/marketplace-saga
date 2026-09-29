package domain

import (
	"errors"

	"github.com/marketplace/pkg/apperr"
)

// Sentinel errors, carrying no transport meaning. The gRPC layer decides what
// each one becomes on the wire; see AsAppError.
var (
	// ErrPaymentNotFound is returned when a payment does not exist.
	ErrPaymentNotFound = errors.New("payment not found")

	// ErrDuplicateIdempotencyKey is returned when a key has already been used.
	// It is an expected outcome, not a failure: the caller replays the original
	// payment rather than creating a second one.
	ErrDuplicateIdempotencyKey = errors.New("idempotency key has already been used")

	// ErrInvalidPayment is returned for a payment that violates its own
	// invariants, including one read back from storage in a corrupt state.
	ErrInvalidPayment = errors.New("invalid payment")

	// ErrInvalidTransition is returned when a state change is not allowed from
	// the payment's current status.
	ErrInvalidTransition = errors.New("invalid payment state transition")

	// ErrPaymentInProgress is returned when a second call arrives with an
	// idempotency key whose first call is still waiting on the provider.
	//
	// This is the genuinely awkward case in idempotent APIs and it is handled
	// explicitly rather than hidden: the first call's outcome is not yet known,
	// so replaying it is impossible and proceeding would double-charge.
	ErrPaymentInProgress = errors.New("a payment with this idempotency key is already in progress")

	// ErrProviderUnavailable wraps a transport failure to the provider.
	ErrProviderUnavailable = errors.New("payment provider unavailable")
)

// op namespaces every error this service produces.
const op = "payment"

// AsAppError maps a domain error onto the shared transport-neutral taxonomy.
//
// The mapping is a statement about the business, not about HTTP: a declined
// card is the caller's problem to resolve (FailedPrecondition), while an
// unreachable provider is ours (Unavailable), and the difference decides
// whether the checkout saga retries or compensates.
func AsAppError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, ErrPaymentNotFound):
		return apperr.Wrap(op, apperr.CodeNotFound, "payment not found", err)

	case errors.Is(err, ErrPaymentInProgress):
		return apperr.Wrap(op, apperr.CodeConflict, "a payment with this key is already in progress", err)

	case errors.Is(err, ErrInvalidPayment):
		return apperr.Wrap(op, apperr.CodeInvalidArgument, "invalid payment", err)

	case errors.Is(err, ErrInvalidTransition):
		return apperr.Wrap(op, apperr.CodeFailedPrecondition, "payment cannot make that transition", err)

	case errors.Is(err, ErrProviderUnavailable):
		// Transient by definition: the caller should retry, and the checkout
		// saga's retry policy classifies on exactly this code.
		return apperr.Wrap(op, apperr.CodeUnavailable, "payment provider is unavailable", err)

	default:
		return apperr.Wrap(op, apperr.CodeInternal, "internal error", err)
	}
}
