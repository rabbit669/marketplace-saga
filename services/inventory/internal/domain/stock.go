// Package domain holds the inventory service's business rules.
//
// Like every other domain package in this repository it imports nothing from
// the outside world: no protobuf, no gRPC, no storage, not even the service's
// own adapters. That is not purity for its own sake. Stock is the one resource
// in this system that cannot be conjured back once it is gone — a unit sold
// twice has to be refunded by a human — so the rules that prevent overselling
// must be readable, reviewable and testable on their own, without a network or
// a database in the way.
//
// The vocabulary is deliberately small:
//
//	StockItem    how many units exist and how many are currently promised
//	Reservation  a promise: a set of units held for one order, for a while
package domain

import (
	"fmt"
	"strings"
	"time"
)

// Reservation parameters.
//
// These are bounds, not preferences. Each one exists to stop a single caller
// from holding an amount of stock that would freeze the catalogue for everyone
// else, and each is enforced when a reservation is created rather than when it
// is committed, because a hold that should never have been granted must be
// refused before the customer is told their order is being processed.
const (
	// DefaultReservationTTL is how long a hold survives when the caller does
	// not ask for something else.
	//
	// Fifteen minutes is chosen against the checkout saga: it is far longer
	// than a checkout takes (seconds), and far shorter than a customer's
	// patience. The TTL is the safety net beneath the recovery worker — if the
	// saga's process dies mid-flight, this is what eventually gives the stock
	// back without a human.
	DefaultReservationTTL = 15 * time.Minute

	// MaxLinesPerReservation bounds the size of one hold. It matches the order
	// service's cart limit, so a legitimate order can always be reserved.
	MaxLinesPerReservation = 50

	// MaxQuantityPerLine stops a typo — an extra zero — from holding a
	// thousand units of a product nobody wants a thousand of.
	MaxQuantityPerLine = 1000

	// MaxReservationTTL caps what a caller may ask for via ttl_seconds.
	//
	// A hold that outlives the checkout it belongs to is indistinguishable from
	// a leak: the customer has gone, and the stock is frozen for everyone else
	// until somebody notices. Callers that need longer must renew explicitly,
	// which leaves a trace in the logs.
	MaxReservationTTL = 24 * time.Hour
)

// StockItem is the on-hand quantity of one product and how much of it is
// currently promised to active reservations.
//
// Quantity and Reserved are separate fields rather than a single "available"
// number because the two answer different questions. Quantity is what is in the
// warehouse; Reserved is what has already been sold, pending payment. Collapsing
// them would make it impossible to tell "we have 5 units, 5 of them are spoken
// for" from "we have 0 units", and the first case is the one an operator needs
// to see when a customer complains.
type StockItem struct {
	// ProductID is the catalogue's key, stored as an opaque string. This
	// service never assumes another service's key format.
	ProductID string
	// Quantity is the on-hand count, including units held by active
	// reservations.
	Quantity int64
	// Reserved is the number of units held by active reservations. It is
	// always meant to be <= Quantity; Available is what remains.
	Reserved int64
	// UpdatedAt is when the row last changed, for the inventory screen and for
	// reasoning about a stale replica later.
	UpdatedAt time.Time
}

// NewStockItem builds a stock row for a product.
//
// A negative quantity is rejected rather than clamped: it can only come from a
// bug or a hand-written admin call, and silently turning it into zero would
// destroy the evidence while making the catalogue look healthy.
func NewStockItem(productID string, quantity int64, now time.Time) (*StockItem, error) {
	if strings.TrimSpace(productID) == "" {
		return nil, fmt.Errorf("%w: product_id is required", ErrInvalidRequest)
	}
	if quantity < 0 {
		return nil, fmt.Errorf("%w: quantity must not be negative, got %d", ErrInvalidRequest, quantity)
	}

	return &StockItem{ProductID: productID, Quantity: quantity, UpdatedAt: now}, nil
}

// Available returns the units that a new reservation may still take.
//
// It never returns a negative number. The clamp is not cosmetic: an operator
// can set an absolute quantity below what is already held (the stock was
// miscounted, or written off), and the holds are promises that SetStock does
// not cancel. Reporting -3 available would make every downstream comparison
// wrong in a different way, so the floor is enforced here once.
func (s StockItem) Available() int64 {
	if s.Quantity <= s.Reserved {
		return 0
	}
	return s.Quantity - s.Reserved
}

// CheckAvailable reports whether quantity units can be held, without holding
// them.
//
// It is separate from Hold on purpose. A reservation that spans several
// products has to be all-or-nothing, so the adapter must be able to test every
// line before it commits to any of them; a check-and-hold in one call would
// leave the earlier lines held when a later one failed.
func (s StockItem) CheckAvailable(quantity int64) error {
	if quantity <= 0 {
		return fmt.Errorf("%w: quantity to hold must be positive, got %d", ErrInvalidRequest, quantity)
	}
	if quantity > s.Available() {
		return fmt.Errorf("%w: product %s: requested %d, available %d",
			ErrInsufficientStock, s.ProductID, quantity, s.Available())
	}
	return nil
}

// Hold records a promise of quantity units.
//
// Hold deliberately cannot fail. It is the second half of the all-or-nothing
// sequence in the adapter: every line was checked with CheckAvailable under the
// same lock a moment earlier, and a failure here would leave the lines held so
// far still held — exactly the partial state that the check-then-hold split
// exists to prevent. Callers must therefore have checked first; the adapter is
// the only caller and does.
func (s *StockItem) Hold(quantity int64) {
	s.Reserved += quantity
}

// SetQuantity applies an absolute on-hand quantity.
//
// It does not touch Reserved. The holds already granted are promises to
// customers who are mid-checkout, and an operator correcting a stock count is
// not cancelling those orders; if the new quantity is below the held amount,
// Available floors at zero and the outstanding holds still commit. Freeing
// that stock is what ReleaseReservation and the expiry worker are for.
func (s *StockItem) SetQuantity(quantity int64, now time.Time) error {
	if quantity < 0 {
		return fmt.Errorf("%w: quantity must not be negative, got %d", ErrInvalidRequest, quantity)
	}
	s.Quantity = quantity
	s.UpdatedAt = now
	return nil
}

// CommitHold converts held units into a permanent deduction: the goods have
// left the warehouse.
//
// Both counters move together, which is what keeps the invariant meaningful —
// after a commit the units are neither on hand nor promised, they are sold. The
// floor at zero covers the operator-error case described on SetQuantity, where
// the on-hand count was already corrected below what had been promised; the
// alternative, a negative on-hand count, is arithmetic nobody can act on.
func (s *StockItem) CommitHold(quantity int64, now time.Time) {
	s.Quantity -= quantity
	if s.Quantity < 0 {
		s.Quantity = 0
	}
	s.ReleaseHold(quantity, now)
}

// ReleaseHold returns held units to the pool, without changing the on-hand
// count: nothing physically moved, the promise was simply withdrawn.
//
// The floor at zero mirrors CommitHold: a release that overshoots must not
// invent negative promises, which would make Available larger than Quantity.
func (s *StockItem) ReleaseHold(quantity int64, now time.Time) {
	s.Reserved -= quantity
	if s.Reserved < 0 {
		s.Reserved = 0
	}
	s.UpdatedAt = now
}
