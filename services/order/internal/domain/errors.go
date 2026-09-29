package domain

import (
	"errors"
	"fmt"

	"github.com/marketplace/pkg/apperr"
)

// Domain errors.
//
// They are plain sentinel values with no transport meaning: a repository returns
// ErrOrderNotFound, and the gRPC layer decides that means `codes.NotFound`. That
// separation is what lets the same domain serve gRPC, an HTTP handler and a
// background worker without knowing about any of them.

// ErrOrderNotFound is returned when an order does not exist.
var ErrOrderNotFound = errors.New("order not found")

// ErrDuplicateOrder is returned when an order id is already taken.
var ErrDuplicateOrder = errors.New("order already exists")

// ErrProductUnavailable is returned when a cart references a product that does
// not exist or is no longer on sale.
var ErrProductUnavailable = errors.New("product is not available")

// op namespaces the operation in error messages. Keeping it in one place means
// every error from this service is attributable without inspecting the message.
const op = "order"

// ValidationError reports input that the caller can fix. It is separated from
// the other errors because it is the only class that is safe to return to a
// client verbatim, and the gateway relies on that distinction.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	if e.Field == "" {
		return fmt.Sprintf("%s: invalid request: %s", op, e.Message)
	}
	return fmt.Sprintf("%s: invalid %s: %s", op, e.Field, e.Message)
}

// Invalid builds a ValidationError.
func Invalid(field, format string, args ...any) *ValidationError {
	return &ValidationError{Field: field, Message: fmt.Sprintf(format, args...)}
}

// AsAppError maps a domain error onto the shared transport-neutral taxonomy.
//
// This is the single translation point between "what went wrong in the
// business" and "what the transport should say about it". It lives in the
// domain because the mapping is a statement about the business rules — that a
// missing product is a caller error, not a server fault — rather than about
// HTTP or gRPC.
func AsAppError(err error) error {
	if err == nil {
		return nil
	}

	var validation *ValidationError
	if errors.As(err, &validation) {
		e := apperr.InvalidArgument(op, "%s", validation.Message)
		if validation.Field != "" {
			return e.WithMeta("field", validation.Field)
		}
		return e
	}

	switch {
	case errors.Is(err, ErrOrderNotFound):
		return apperr.Wrap(op, apperr.CodeNotFound, "order not found", err)
	case errors.Is(err, ErrDuplicateOrder):
		return apperr.Wrap(op, apperr.CodeAlreadyExists, "order already exists", err)
	case errors.Is(err, ErrProductUnavailable):
		return apperr.Wrap(op, apperr.CodeFailedPrecondition, "product is not available", err)
	default:
		// Unclassified means a bug or an infrastructure failure. It becomes
		// CodeInternal, whose message is never returned to the caller.
		return apperr.Wrap(op, apperr.CodeInternal, "internal error", err)
	}
}
