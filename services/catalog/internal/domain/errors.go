package domain

import (
	"errors"
	"fmt"

	"github.com/marketplace/pkg/apperr"
)

// Domain errors.
//
// They are plain sentinel values with no transport meaning: a repository returns
// ErrProductNotFound, and the gRPC layer decides that means `codes.NotFound`.
// That separation is what lets the same domain serve gRPC today and an HTTP
// handler or a Kafka consumer later without knowing about any of them.

// ErrProductNotFound is returned when a product does not exist.
var ErrProductNotFound = errors.New("product not found")

// ErrDuplicateSKU is returned when a SKU is already taken.
//
// The uniqueness is case- and whitespace-insensitive, because the SKU is
// normalised before it is compared: see NormalizeSKU.
var ErrDuplicateSKU = errors.New("sku already exists")

// ErrDuplicateProduct is returned when a product id is already taken.
//
// It is separate from ErrDuplicateSKU because the two mean different things to
// the caller. A duplicate SKU is a business conflict the caller can resolve by
// choosing another SKU; a duplicate id means the caller reused an identifier it
// was given, which is a bug in the caller. Neither should be reported as the
// other — support reading "sku already exists" for a colliding id would go
// looking in the wrong place.
var ErrDuplicateProduct = errors.New("product already exists")

// ErrInvalidProduct is returned when a stored product fails validation on the
// way out of storage.
//
// It is distinct from ValidationError, which describes a caller's bad request:
// this one says the data itself is corrupt, so the caller cannot fix it by
// changing the request.
var ErrInvalidProduct = errors.New("invalid product")

// op namespaces the operation in error messages. Keeping it in one place means
// every error from this service is attributable without inspecting the message.
const op = "catalog"

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
// gRPC or HTTP.
func AsAppError(err error) error {
	if err == nil {
		return nil
	}

	// Checked before ValidationError: Rehydrate wraps a rule violation in
	// ErrInvalidProduct, and the wrapped value is still a *ValidationError. The
	// corrupt-data reading is the more useful one to surface, because the caller
	// cannot fix it by resending the request.
	if errors.Is(err, ErrInvalidProduct) {
		return apperr.Wrap(op, apperr.CodeInvalidArgument, "product data is invalid", err)
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
	case errors.Is(err, ErrProductNotFound):
		return apperr.Wrap(op, apperr.CodeNotFound, "product not found", err)
	case errors.Is(err, ErrDuplicateSKU):
		return apperr.Wrap(op, apperr.CodeAlreadyExists, "a product with this SKU already exists", err)
	case errors.Is(err, ErrDuplicateProduct):
		return apperr.Wrap(op, apperr.CodeAlreadyExists, "a product with this id already exists", err)
	default:
		// Unclassified means a bug or an infrastructure failure. It becomes
		// CodeInternal, whose message is never returned to the caller: the
		// cause stays in the wrapped error for the logs.
		return apperr.Wrap(op, apperr.CodeInternal, "internal error", err)
	}
}
