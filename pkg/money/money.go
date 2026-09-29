// Package money provides exact monetary arithmetic.
//
// Money is never a float. 0.1 + 0.2 != 0.3 in binary floating point, and a
// marketplace that computes a cart total that way will eventually disagree with
// its payment provider by a cent — which, at scale, is a reconciliation
// incident. Amounts are therefore stored as a signed integer count of minor
// units (cents, kopecks) plus an ISO-4217 currency.
//
// The type is immutable: every operation returns a new Money. Sharing a value
// that can be mutated behind a caller's back is how totals silently drift.
package money

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Currency is an ISO-4217 alphabetic code, for example "USD" or "EUR".
type Currency string

// Currencies the platform currently settles in. The list is deliberately short:
// adding one is a business decision that also implies a rounding policy.
const (
	USD Currency = "USD"
	EUR Currency = "EUR"
	RUB Currency = "RUB"
)

// MinorUnits returns how many decimal places the currency's minor unit has.
// Most currencies use 2; the exceptions matter when formatting, which is the
// only place this is needed.
func (c Currency) MinorUnits() int {
	switch c {
	case "JPY", "KRW", "VND", "CLP", "ISK":
		return 0
	case "BHD", "KWD", "OMR", "TND":
		return 3
	default:
		return 2
	}
}

// valid reports whether c looks like an ISO-4217 code.
func (c Currency) valid() bool {
	if len(c) != 3 {
		return false
	}
	for i := range len(c) {
		if c[i] < 'A' || c[i] > 'Z' {
			return false
		}
	}
	return true
}

// Money is an exact amount in a specific currency.
//
// The zero value is not a usable amount: it has no currency. Construct values
// with New, MustNew or Zero.
type Money struct {
	minor int64
	cur   Currency
}

// New returns a Money of minor units in the given currency.
func New(minor int64, cur Currency) (Money, error) {
	if !cur.valid() {
		return Money{}, fmt.Errorf("money: %q is not a valid ISO-4217 currency code", cur)
	}
	return Money{minor: minor, cur: cur}, nil
}

// MustNew is New for compile-time constants and tests. It panics on invalid
// input, which is acceptable only where the arguments are literals.
func MustNew(minor int64, cur Currency) Money {
	m, err := New(minor, cur)
	if err != nil {
		panic(err)
	}
	return m
}

// Zero returns a zero amount in the given currency. It is never equal to a zero
// amount in a different currency.
func Zero(cur Currency) Money { return Money{cur: cur} }

// Amount returns the amount in minor units.
func (m Money) Amount() int64 { return m.minor }

// Currency returns the currency of the amount.
func (m Money) Currency() Currency { return m.cur }

// IsZero reports whether the amount is exactly zero.
func (m Money) IsZero() bool { return m.minor == 0 }

// IsNegative reports whether the amount is below zero. Negative amounts are
// legitimate for refunds and adjustments; they are only invalid where a
// specific domain rule says so, which is the domain's job to check.
func (m Money) IsNegative() bool { return m.minor < 0 }

// IsPositive reports whether the amount is strictly above zero.
func (m Money) IsPositive() bool { return m.minor > 0 }

// IsValid reports whether the value was constructed properly. The zero value of
// Money is invalid, so this catches a Money that was never initialised.
func (m Money) IsValid() bool { return m.cur.valid() }

// Add returns m + o. Mixing currencies is an error rather than a silent
// conversion: there is no exchange rate in this system.
func (m Money) Add(o Money) (Money, error) {
	if err := m.checkSameCurrency(o); err != nil {
		return Money{}, err
	}
	return Money{minor: m.minor + o.minor, cur: m.cur}, nil
}

// Sub returns m - o.
func (m Money) Sub(o Money) (Money, error) {
	if err := m.checkSameCurrency(o); err != nil {
		return Money{}, err
	}
	return Money{minor: m.minor - o.minor, cur: m.cur}, nil
}

// Mul multiplies the amount by an integer quantity, which is the only
// multiplication that is exact. Percentage discounts go through Allocate or an
// explicit rounding decision, never through a float.
func (m Money) Mul(n int64) Money {
	return Money{minor: m.minor * n, cur: m.cur}
}

// Neg returns the amount with the opposite sign.
func (m Money) Neg() Money { return Money{minor: -m.minor, cur: m.cur} }

// Abs returns the amount without its sign.
func (m Money) Abs() Money {
	if m.minor < 0 {
		return m.Neg()
	}
	return m
}

// Compare returns -1, 0 or 1 as m is less than, equal to, or greater than o.
// Comparing across currencies is an error.
func (m Money) Compare(o Money) (int, error) {
	if err := m.checkSameCurrency(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	default:
		return 0, nil
	}
}

// Equal reports whether two amounts are the same value in the same currency.
// Amounts in different currencies are never equal, and an invalid Money is
// never equal to anything, including itself.
func (m Money) Equal(o Money) bool {
	if !m.IsValid() || !o.IsValid() {
		return false
	}
	return m.cur == o.cur && m.minor == o.minor
}

// LessThan reports whether m is strictly smaller than o, ignoring currency
// mismatches by returning false. Use Compare when the mismatch matters.
func (m Money) LessThan(o Money) bool {
	c, err := m.Compare(o)
	return err == nil && c < 0
}

func (m Money) checkSameCurrency(o Money) error {
	if !m.IsValid() {
		return fmt.Errorf("money: left operand has no currency")
	}
	if !o.IsValid() {
		return fmt.Errorf("money: right operand has no currency")
	}
	if m.cur != o.cur {
		return fmt.Errorf("money: cannot combine %s and %s without an exchange rate", m.cur, o.cur)
	}
	return nil
}

// Sum adds a slice of amounts. An empty slice returns zero in cur. All elements
// must share a currency.
func Sum(amounts []Money, cur Currency) (Money, error) {
	total := Zero(cur)
	for i, a := range amounts {
		var err error
		if total, err = total.Add(a); err != nil {
			return Money{}, fmt.Errorf("money: sum at index %d: %w", i, err)
		}
	}
	return total, nil
}

// String renders the amount with a decimal point, e.g. "12.34 USD". It is
// intended for logs and debugging, not for user-facing formatting: locale,
// symbol placement and thousands separators are presentation concerns.
func (m Money) String() string {
	if !m.IsValid() {
		return "<invalid money>"
	}

	units := m.cur.MinorUnits()
	if units == 0 {
		return fmt.Sprintf("%d %s", m.minor, m.cur)
	}

	sign := ""
	minor := m.minor
	if minor < 0 {
		sign = "-"
		minor = -minor
	}

	divisor := int64(1)
	for range units {
		divisor *= 10
	}

	return fmt.Sprintf("%s%d.%0*d %s", sign, minor/divisor, units, minor%divisor, m.cur)
}

// MarshalJSON encodes the amount as a string ("12.34 USD") rather than a
// number, so that a JavaScript client cannot silently lose precision by parsing
// it into a double.
func (m Money) MarshalJSON() ([]byte, error) {
	if !m.IsValid() {
		return nil, fmt.Errorf("money: cannot marshal a value with no currency")
	}
	return json.Marshal(struct {
		AmountMinor int64    `json:"amount_minor"`
		Currency    Currency `json:"currency"`
	}{AmountMinor: m.minor, Currency: m.cur})
}

// UnmarshalJSON is the inverse of MarshalJSON.
func (m *Money) UnmarshalJSON(data []byte) error {
	var wire struct {
		AmountMinor int64    `json:"amount_minor"`
		Currency    Currency `json:"currency"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return fmt.Errorf("money: %w", err)
	}

	parsed, err := New(wire.AmountMinor, Currency(strings.ToUpper(string(wire.Currency))))
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}
