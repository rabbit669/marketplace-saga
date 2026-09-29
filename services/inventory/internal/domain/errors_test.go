package domain_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/marketplace/pkg/apperr"

	"github.com/marketplace/services/inventory/internal/domain"
)

func TestAsAppErrorIgnoresNil(t *testing.T) {
	if err := domain.AsAppError(nil); err != nil {
		t.Fatalf("AsAppError(nil) = %v, want nil", err)
	}
}

func TestAsAppErrorMapsTheTaxonomy(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want apperr.Code
	}{
		{
			name: "insufficient stock is a failed precondition",
			// Well formed request, world says no: the caller must not treat
			// this as a bug and must not retry it hopefully.
			err:  fmt.Errorf("%w: product p-1: requested 3, available 1", domain.ErrInsufficientStock),
			want: apperr.CodeFailedPrecondition,
		},
		{
			name: "unknown reservation",
			err:  fmt.Errorf("%w: r-1", domain.ErrReservationNotFound),
			want: apperr.CodeNotFound,
		},
		{
			name: "unknown product",
			err:  fmt.Errorf("%w: product p-9", domain.ErrStockNotFound),
			want: apperr.CodeNotFound,
		},
		{
			name: "malformed request",
			err:  fmt.Errorf("%w: reservation_id is required", domain.ErrInvalidRequest),
			want: apperr.CodeInvalidArgument,
		},
		{
			name: "state machine refusal keeps the wrapped reason",
			err:  fmt.Errorf("%w: reservation r-1 is released", domain.ErrInvalidRequest),
			want: apperr.CodeInvalidArgument,
		},
		{
			name: "unclassified becomes internal",
			err:  errors.New("connection reset by peer"),
			want: apperr.CodeInternal,
		},
		{
			name: "cancelled call is not a server fault",
			err:  context.Canceled,
			want: apperr.CodeUnavailable,
		},
		{
			name: "expired deadline",
			err:  context.DeadlineExceeded,
			want: apperr.CodeDeadlineExceeded,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mapped := domain.AsAppError(tc.err)
			if got := apperr.CodeOf(mapped); got != tc.want {
				t.Fatalf("code = %q, want %q (error: %v)", got, tc.want, mapped)
			}

			// The mapping wraps rather than replaces, so a caller that only has
			// the transport error can still ask which domain rule was broken.
			if !errors.Is(mapped, tc.err) {
				t.Errorf("mapped error does not wrap the cause: %v", mapped)
			}
		})
	}
}

func TestAsAppErrorKeepsDetailVisibleButInternalOpaque(t *testing.T) {
	stockErr := domain.AsAppError(fmt.Errorf("%w: product p-1: requested 3, available 1", domain.ErrInsufficientStock))

	// The caller already knows the product and the quantity it asked for, so
	// repeating them is what turns "failed precondition" into an actionable
	// answer for the checkout UI.
	if !strings.Contains(apperr.PublicMessage(stockErr), "p-1") {
		t.Errorf("public message %q should name the product", apperr.PublicMessage(stockErr))
	}

	internal := domain.AsAppError(errors.New("sql: connection refused to db-primary.internal:5432"))
	if got := apperr.PublicMessage(internal); got != "internal error" {
		t.Errorf("public message = %q, want a generic one", got)
	}
	// The cause is still attached for the log line, just never returned.
	if !strings.Contains(internal.Error(), "db-primary.internal") {
		t.Errorf("internal error lost its cause: %v", internal)
	}
}

func TestAsAppErrorIsIdempotent(t *testing.T) {
	// Mapping happens at more than one boundary in a real request path, and a
	// second pass over an already-mapped error must not reclassify it: falling
	// into the default branch would turn a NotFound into an Internal and lose
	// the only information the caller can act on.
	once := domain.AsAppError(fmt.Errorf("%w: r-1", domain.ErrReservationNotFound))
	if apperr.CodeOf(once) != apperr.CodeNotFound {
		t.Fatalf("code = %q, want not_found", apperr.CodeOf(once))
	}

	twice := domain.AsAppError(once)
	if apperr.CodeOf(twice) != apperr.CodeNotFound {
		t.Fatalf("code after mapping twice = %q, want not_found", apperr.CodeOf(twice))
	}
	if twice.Error() != once.Error() {
		t.Errorf("mapping twice changed the message: %q vs %q", twice, once)
	}
}
