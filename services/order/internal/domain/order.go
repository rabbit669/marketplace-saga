// Package domain holds the order service's business rules.
//
// It deliberately imports nothing from the outside world: no database driver,
// no gRPC, no Kafka, not even this repository's own infrastructure packages.
// The only exceptions are the standard library and the shared `money` value
// type, which is itself dependency-free.
//
// The payoff is not purity for its own sake. Checkout is the part of this
// system most likely to be got wrong, and keeping the rules free of
// infrastructure is what allows the entire rollback path to be tested without a
// database, a broker or a network.
package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/marketplace/pkg/money"
)

// Status is the lifecycle state of an order.
type Status string

const (
	// StatusPending means the checkout saga is still running.
	StatusPending Status = "pending"
	// StatusConfirmed means payment was captured and stock committed. Terminal.
	StatusConfirmed Status = "confirmed"
	// StatusCancelled means the saga compensated everything it had applied.
	// Terminal.
	StatusCancelled Status = "cancelled"
	// StatusFailed means compensation itself failed and the order is in a state
	// only a human can resolve. Terminal for the machine, actionable for us.
	StatusFailed Status = "failed"
)

// Terminal reports whether the order has stopped moving.
func (s Status) Terminal() bool {
	switch s {
	case StatusConfirmed, StatusCancelled, StatusFailed:
		return true
	default:
		return false
	}
}

// Valid reports whether s is a known status. Guards against a value read from
// storage being silently treated as pending.
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusConfirmed, StatusCancelled, StatusFailed:
		return true
	default:
		return false
	}
}

// MaxLineQuantity bounds a single line. The limit exists to stop a typo from
// reserving a million units and holding the stock of every other customer.
const MaxLineQuantity = 1000

// MaxLines bounds the size of a cart, which bounds the work the saga must do
// and the number of reservations it must create.
const MaxLines = 50

// Line is one product in an order.
type Line struct {
	// ProductID identifies the product in the catalogue. It is stored as an
	// opaque string: the order service never assumes another service's key
	// format.
	ProductID string
	// SKU is copied at checkout time for display and support, not for lookup.
	SKU string
	// Name is copied at checkout time.
	//
	// This looks like denormalisation and it is deliberate: an order must show
	// what the customer bought, even after the catalogue entry is renamed or
	// deleted. Joining to the catalogue to render a six-month-old invoice would
	// mean the invoice changes whenever marketing edits a title.
	Name string
	// Quantity is the number of units.
	Quantity int64
	// UnitPrice is the price at the moment of checkout.
	//
	// Copied for the same reason, and with more at stake: the amount a customer
	// agreed to pay must never change because the catalogue was re-priced while
	// they were typing their card number.
	UnitPrice money.Money
}

// Total returns the line's extended price.
func (l Line) Total() (money.Money, error) {
	if l.Quantity < 0 {
		return money.Money{}, fmt.Errorf("order line: quantity must not be negative, got %d", l.Quantity)
	}
	if !l.UnitPrice.IsValid() {
		return money.Money{}, errors.New("order line: unit price has no currency")
	}
	return l.UnitPrice.Mul(l.Quantity), nil
}

// Order is the aggregate root for checkout.
//
// It is a value that can be constructed only through NewOrder or Rehydrate,
// both of which validate. An Order built by hand and written to the database
// would bypass every rule in this file, so the repository accepts only the
// constructor output.
type Order struct {
	ID         string
	CustomerID string
	Status     Status
	Lines      []Line
	Total      money.Money
	Shipping   Address

	// PaymentID and ReservationID record what the saga's steps produced. They
	// are the handles a compensation needs: releasing stock requires the
	// reservation id, and voiding a payment requires the payment id.
	PaymentID     string
	ReservationID string

	// FailureReason explains a cancelled or failed order in terms a support
	// agent can act on.
	FailureReason string

	CreatedAt time.Time
	UpdatedAt time.Time
}

// Address is a shipping destination.
type Address struct {
	Country    string
	City       string
	Street     string
	PostalCode string
	Recipient  string
}

// Validate checks the address is complete enough to ship to.
func (a Address) Validate() error {
	var missing []string
	for name, value := range map[string]string{
		"country":     a.Country,
		"city":        a.City,
		"street":      a.Street,
		"postal_code": a.PostalCode,
		"recipient":   a.Recipient,
	} {
		if strings.TrimSpace(value) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		// Sorted for a stable message, which keeps the API response and the
		// test assertions from depending on map iteration order.
		sortStrings(missing)
		return fmt.Errorf("shipping address is incomplete: missing %s", strings.Join(missing, ", "))
	}
	return nil
}

// LineRequest is a caller's request for one line, before pricing.
type LineRequest struct {
	ProductID string
	Quantity  int64
}

// Validate checks a raw line request. Pricing happens later, against the
// catalogue, so this is only about shape.
func (r LineRequest) Validate() error {
	if strings.TrimSpace(r.ProductID) == "" {
		return errors.New("product_id is required")
	}
	if r.Quantity <= 0 {
		return fmt.Errorf("quantity must be positive, got %d", r.Quantity)
	}
	if r.Quantity > MaxLineQuantity {
		return fmt.Errorf("quantity %d exceeds the per-line maximum of %d", r.Quantity, MaxLineQuantity)
	}
	return nil
}

// NewOrderInput is everything needed to create an order.
type NewOrderInput struct {
	ID         string
	CustomerID string
	Lines      []Line
	Shipping   Address
	CreatedAt  time.Time
}

// NewOrder builds a validated order in the pending state.
//
// The total is computed here rather than accepted from the caller. A client
// that supplies its own total is a client that can supply a wrong one, and the
// failure mode of trusting it is selling goods below cost.
func NewOrder(in NewOrderInput) (*Order, error) {
	if strings.TrimSpace(in.ID) == "" {
		return nil, errors.New("order id is required")
	}
	if strings.TrimSpace(in.CustomerID) == "" {
		return nil, errors.New("customer_id is required")
	}
	if len(in.Lines) == 0 {
		return nil, errors.New("an order must have at least one line")
	}
	if len(in.Lines) > MaxLines {
		return nil, fmt.Errorf("an order may have at most %d lines, got %d", MaxLines, len(in.Lines))
	}
	if err := in.Shipping.Validate(); err != nil {
		return nil, err
	}

	lines := make([]Line, len(in.Lines))
	copy(lines, in.Lines)

	total, err := totalOf(lines)
	if err != nil {
		return nil, err
	}
	if !total.IsPositive() {
		return nil, fmt.Errorf("order total must be positive, got %s", total)
	}

	now := in.CreatedAt
	if now.IsZero() {
		now = time.Now()
	}

	return &Order{
		ID:         in.ID,
		CustomerID: in.CustomerID,
		Status:     StatusPending,
		Lines:      lines,
		Total:      total,
		Shipping:   in.Shipping,
		CreatedAt:  now,
		UpdatedAt:  now,
	}, nil
}

// Rehydrate rebuilds an order from storage.
//
// It re-runs the same validation as NewOrder, with one deliberate difference:
// it accepts a status that is not pending, because a stored order is expected
// to have moved on. Re-validating on load means a row that was written by an
// older version of the rules, or edited by hand, is caught at read time rather
// than propagating into business logic.
func Rehydrate(o Order) (*Order, error) {
	if strings.TrimSpace(o.ID) == "" {
		return nil, errors.New("order id is required")
	}
	if !o.Status.Valid() {
		return nil, fmt.Errorf("order %s has unknown status %q", o.ID, o.Status)
	}
	if len(o.Lines) == 0 {
		return nil, fmt.Errorf("order %s has no lines", o.ID)
	}

	total, err := totalOf(o.Lines)
	if err != nil {
		return nil, fmt.Errorf("order %s: %w", o.ID, err)
	}
	if !o.Total.Equal(total) {
		// A mismatch means the stored total disagrees with its own lines. That
		// should be impossible, so it is an error rather than a correction:
		// silently recomputing would hide a bug that is already corrupting data.
		return nil, fmt.Errorf("order %s: stored total %s does not match the sum of its lines %s", o.ID, o.Total, total)
	}

	rebuilt := o
	rebuilt.Lines = make([]Line, len(o.Lines))
	copy(rebuilt.Lines, o.Lines)
	rebuilt.Total = total

	return &rebuilt, nil
}

// totalOf sums the extended prices of the lines.
func totalOf(lines []Line) (money.Money, error) {
	if len(lines) == 0 {
		return money.Money{}, errors.New("cannot total an order with no lines")
	}

	currency := lines[0].UnitPrice.Currency()
	if currency == "" {
		return money.Money{}, errors.New("order line 0 has no currency")
	}

	total := money.Zero(currency)
	for i, l := range lines {
		lineTotal, err := l.Total()
		if err != nil {
			return money.Money{}, fmt.Errorf("order line %d: %w", i, err)
		}
		if l.UnitPrice.Currency() != currency {
			return money.Money{}, fmt.Errorf(
				"order line %d is priced in %s but line 0 is in %s: an order must be settled in one currency",
				i, l.UnitPrice.Currency(), currency,
			)
		}
		if total, err = total.Add(lineTotal); err != nil {
			return money.Money{}, fmt.Errorf("order line %d: %w", i, err)
		}
	}
	return total, nil
}

// MarkConfirmed records a successful checkout.
func (o *Order) MarkConfirmed(now time.Time) error {
	if o.Status.Terminal() {
		return fmt.Errorf("order %s is already %s and cannot be confirmed", o.ID, o.Status)
	}
	o.Status = StatusConfirmed
	o.FailureReason = ""
	o.UpdatedAt = now
	return nil
}

// MarkCancelled records that the saga rolled everything back.
//
// Cancelling is a successful outcome: the customer was charged nothing and no
// stock is held. It is recorded distinctly from StatusFailed so that a support
// query for "orders needing attention" does not return every declined card.
func (o *Order) MarkCancelled(reason string, now time.Time) error {
	if o.Status.Terminal() {
		return fmt.Errorf("order %s is already %s and cannot be cancelled", o.ID, o.Status)
	}
	o.Status = StatusCancelled
	o.FailureReason = reason
	o.UpdatedAt = now
	return nil
}

// MarkFailed records that compensation could not be completed.
func (o *Order) MarkFailed(reason string, now time.Time) error {
	if o.Status == StatusConfirmed {
		return fmt.Errorf("order %s is confirmed and cannot be marked failed", o.ID)
	}
	o.Status = StatusFailed
	o.FailureReason = reason
	o.UpdatedAt = now
	return nil
}

// RecordReservation stores the reservation the inventory service created.
func (o *Order) RecordReservation(reservationID string) {
	o.ReservationID = reservationID
}

// RecordPayment stores the payment the payment service created.
func (o *Order) RecordPayment(paymentID string) {
	o.PaymentID = paymentID
}

// ReservedLines is the reservation request derived from the order.
func (o *Order) ReservedLines() []LineRequest {
	lines := make([]LineRequest, 0)
	for _, l := range o.Lines {
		lines = append(lines, LineRequest{ProductID: l.ProductID, Quantity: l.Quantity})
	}
	return lines
}

// sortStrings sorts in place. A tiny helper so that `domain` stays free of the
// `slices` import purely for one call site.
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
