package money

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNew_ValidatesCurrency(t *testing.T) {
	if _, err := New(100, USD); err != nil {
		t.Errorf("New(100, USD) = %v, want nil", err)
	}

	for _, cur := range []Currency{"", "usd", "US", "USDD", "U1D", "ЮЙД"} {
		if _, err := New(100, cur); err == nil {
			t.Errorf("New(100, %q) = nil, want a validation error", cur)
		}
	}
}

func TestZeroValueIsInvalid(t *testing.T) {
	var m Money

	if m.IsValid() {
		t.Error("the zero value must not be a valid Money")
	}
	if _, err := m.Add(MustNew(1, USD)); err == nil {
		t.Error("adding to an uninitialised Money must fail")
	}
	// Crucially, an invalid value must not compare equal to anything, not even
	// another invalid value. Otherwise an uninitialised field could pass an
	// "amount matches" check.
	if m.Equal(Money{}) {
		t.Error("an invalid Money must not equal another invalid Money")
	}
}

func TestAddAndSub(t *testing.T) {
	a := MustNew(1250, USD)
	b := MustNew(750, USD)

	sum, err := a.Add(b)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if sum.Amount() != 2000 {
		t.Errorf("sum = %d, want 2000", sum.Amount())
	}

	diff, err := a.Sub(b)
	if err != nil {
		t.Fatalf("Sub: %v", err)
	}
	if diff.Amount() != 500 {
		t.Errorf("difference = %d, want 500", diff.Amount())
	}

	// Operations return new values; the operands must be untouched.
	if a.Amount() != 1250 || b.Amount() != 750 {
		t.Error("Add/Sub must not mutate their operands")
	}
}

func TestAddRejectsMixedCurrencies(t *testing.T) {
	_, err := MustNew(100, USD).Add(MustNew(100, EUR))
	if err == nil {
		t.Fatal("adding USD to EUR must fail: there is no exchange rate in this system")
	}
	if !strings.Contains(err.Error(), "exchange rate") {
		t.Errorf("error = %v, want it to explain the currency mismatch", err)
	}
}

func TestMul(t *testing.T) {
	price := MustNew(1999, USD)

	if got := price.Mul(3).Amount(); got != 5997 {
		t.Errorf("Mul(3) = %d, want 5997", got)
	}
	if got := price.Mul(0); !got.IsZero() {
		t.Errorf("Mul(0) = %v, want zero", got)
	}
	if got := price.Mul(-1); got.Amount() != -1999 {
		t.Errorf("Mul(-1) = %d, want -1999", got.Amount())
	}

	// The classic float trap this package exists to avoid.
	thirds := MustNew(10, USD)
	total := thirds.Mul(3)
	if total.Amount() != 30 {
		t.Errorf("3 x 0.10 = %d, want exactly 30", total.Amount())
	}
}

func TestCompareAndEqual(t *testing.T) {
	small := MustNew(100, USD)
	big := MustNew(200, USD)

	cmp, err := small.Compare(big)
	if err != nil || cmp != -1 {
		t.Errorf("Compare = (%d, %v), want (-1, nil)", cmp, err)
	}
	if cmp, _ := big.Compare(small); cmp != 1 {
		t.Errorf("Compare = %d, want 1", cmp)
	}
	if cmp, _ := small.Compare(MustNew(100, USD)); cmp != 0 {
		t.Errorf("Compare = %d, want 0", cmp)
	}
	if _, err := small.Compare(MustNew(100, EUR)); err == nil {
		t.Error("comparing across currencies must fail")
	}

	if !small.Equal(MustNew(100, USD)) {
		t.Error("Equal must be true for identical amounts")
	}
	if small.Equal(MustNew(100, EUR)) {
		t.Error("Equal must be false across currencies, even at the same magnitude")
	}
	if small.Equal(big) {
		t.Error("Equal must be false for different amounts")
	}

	if !small.LessThan(big) {
		t.Error("LessThan = false, want true")
	}
	if big.LessThan(small) {
		t.Error("LessThan = true, want false")
	}
	// A currency mismatch is not an ordering; LessThan degrades to false
	// instead of inventing one.
	if small.LessThan(MustNew(100, EUR)) {
		t.Error("LessThan across currencies must be false")
	}
}

func TestNegAndAbs(t *testing.T) {
	m := MustNew(500, USD)

	if got := m.Neg().Amount(); got != -500 {
		t.Errorf("Neg = %d, want -500", got)
	}
	if got := m.Neg().Abs().Amount(); got != 500 {
		t.Errorf("Abs of a negative = %d, want 500", got)
	}
	if got := m.Abs().Amount(); got != 500 {
		t.Errorf("Abs of a positive = %d, want 500", got)
	}
}

func TestSignPredicates(t *testing.T) {
	if !MustNew(0, USD).IsZero() {
		t.Error("IsZero = false for zero")
	}
	if !MustNew(-1, USD).IsNegative() {
		t.Error("IsNegative = false for a negative amount")
	}
	if !MustNew(1, USD).IsPositive() {
		t.Error("IsPositive = false for a positive amount")
	}
	if MustNew(0, USD).IsPositive() {
		t.Error("IsPositive = true for zero")
	}
}

func TestSum(t *testing.T) {
	lines := []Money{
		MustNew(1999, USD).Mul(2), // 39.98
		MustNew(500, USD),         //  5.00
		MustNew(1, USD),           //  0.01
	}

	total, err := Sum(lines, USD)
	if err != nil {
		t.Fatalf("Sum: %v", err)
	}
	if total.Amount() != 4499 {
		t.Errorf("Sum = %d, want 4499", total.Amount())
	}

	empty, err := Sum(nil, USD)
	if err != nil {
		t.Fatalf("Sum(nil): %v", err)
	}
	if !empty.IsZero() || empty.Currency() != USD {
		t.Errorf("Sum(nil) = %v, want zero USD", empty)
	}

	// A mixed-currency slice must fail, and the error must say where.
	if _, err := Sum([]Money{MustNew(1, USD), MustNew(1, EUR)}, USD); err == nil {
		t.Error("Sum across currencies must fail")
	}
}

func TestString(t *testing.T) {
	tests := []struct {
		m    Money
		want string
	}{
		{MustNew(1234, USD), "12.34 USD"},
		{MustNew(0, USD), "0.00 USD"},
		{MustNew(5, USD), "0.05 USD"},
		{MustNew(-1234, USD), "-12.34 USD"},
		{MustNew(100, USD), "1.00 USD"},
		{MustNew(1000, "JPY"), "1000 JPY"},  // zero-decimal currency
		{MustNew(1234, "KWD"), "1.234 KWD"}, // three-decimal currency
		{Money{}, "<invalid money>"},
	}

	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			if got := tc.m.String(); got != tc.want {
				t.Errorf("String() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCurrencyMinorUnits(t *testing.T) {
	tests := map[Currency]int{
		USD:   2,
		EUR:   2,
		RUB:   2,
		"JPY": 0,
		"KWD": 3,
	}

	for cur, want := range tests {
		if got := cur.MinorUnits(); got != want {
			t.Errorf("%s.MinorUnits() = %d, want %d", cur, got, want)
		}
	}
}

func TestJSONRoundTrip(t *testing.T) {
	original := MustNew(4499, USD)

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// Encoded as minor units, never as a float, so a JavaScript client cannot
	// lose precision parsing it.
	want := `{"amount_minor":4499,"currency":"USD"}`
	if string(data) != want {
		t.Errorf("Marshal = %s, want %s", data, want)
	}

	var decoded Money
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !decoded.Equal(original) {
		t.Errorf("round trip = %v, want %v", decoded, original)
	}
}

func TestMarshalInvalidMoneyFails(t *testing.T) {
	if _, err := json.Marshal(Money{}); err == nil {
		t.Error("marshalling a Money with no currency must fail rather than emit garbage")
	}
}
