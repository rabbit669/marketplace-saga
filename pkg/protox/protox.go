// Package prox holds conversions between generated protobuf types and the
// repository's own value types.
//
// Every service needs the same handful of conversions — money, timestamps — and
// doing them by hand in each one is how two services end up disagreeing about
// whether an absent currency is an error. Keeping them here means the answer is
// decided once.
//
// The conversions return errors rather than zero values because a malformed
// amount from the wire is a caller's mistake, and silently treating it as zero
// would turn a bug into a free order.
package prox

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/marketplace/pkg/money"
	commonv1 "github.com/marketplace/pkg/proto/common/v1"
)

// ErrInvalidMoney is returned when a wire amount cannot be trusted.
type ErrInvalidMoney struct {
	Reason string
}

func (e *ErrInvalidMoney) Error() string { return "invalid money: " + e.Reason }

// MoneyToProto converts a domain amount to its wire representation.
func MoneyToProto(m money.Money) (*commonv1.Money, error) {
	if !m.IsValid() {
		return nil, &ErrInvalidMoney{Reason: "amount has no currency"}
	}
	return &commonv1.Money{
		AmountMinor: m.Amount(),
		Currency:    string(m.Currency()),
	}, nil
}

// MoneyFromProto converts a wire amount to a domain amount.
//
// Minor units are copied verbatim. The wire format deliberately carries an
// integer rather than a decimal string precisely so that no rounding can occur
// in transit, and re-deriving it here from anything else would reintroduce the
// problem the format exists to avoid.
func MoneyFromProto(p *commonv1.Money) (money.Money, error) {
	if p == nil {
		return money.Money{}, &ErrInvalidMoney{Reason: "amount is missing"}
	}

	currency := money.Currency(strings.ToUpper(strings.TrimSpace(p.GetCurrency())))
	m, err := money.New(p.GetAmountMinor(), currency)
	if err != nil {
		return money.Money{}, &ErrInvalidMoney{Reason: err.Error()}
	}
	return m, nil
}

// TimeFromProto converts a wire timestamp, returning the zero time for nil.
//
// The zero time is the right answer for an unset optional field: callers test it
// with IsZero, whereas substituting time.Now would invent a fact.
func TimeFromProto(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

// TimeToProto converts a time to its wire form, mapping the zero time to nil so
// that an unset field round-trips as unset.
func TimeToProto(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// MustMoney is a convenience for fixtures and tests, panicking on a bad amount.
func MustMoney(minor int64, currency string) *commonv1.Money {
	return &commonv1.Money{AmountMinor: minor, Currency: currency}
}

// String renders a wire amount for logs without requiring a domain conversion.
func String(p *commonv1.Money) string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%d %s", p.GetAmountMinor(), p.GetCurrency())
}
