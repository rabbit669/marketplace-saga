package domain

import (
	"context"
	"time"
)

// The interfaces in this file are the inventory service's ports. The domain
// defines what it needs; adapters (the in-memory store, a future PostgreSQL
// repository) provide it.
//
// The dependency points inward: adapters import domain, never the reverse. The
// reservation protocol is the part of this service most likely to be got wrong,
// and keeping it free of infrastructure is what allows the whole of it —
// including the awkward parts, like a compensation arriving for a reservation
// that was never created — to be tested without a database.

// ReserveRequest asks the repository to hold stock.
type ReserveRequest struct {
	// ReservationID is chosen by the caller and must be deterministic for the
	// order it belongs to: the order service passes its order id.
	//
	// It is the key of the whole protocol. A repository that keyed on an
	// internally generated id would hold a second set of units on every saga
	// retry, and the customer's failed first attempt would quietly halve the
	// stock available.
	ReservationID string
	// OrderID is the order the hold belongs to. Recorded, not keyed on.
	OrderID string
	Lines   []ReservationLine
	// TTL is how long the hold survives. Zero means the repository's default.
	TTL time.Duration
}

// ReserveResult reports the outcome of a reservation.
type ReserveResult struct {
	Reservation Reservation
	// AlreadyExisted distinguishes a replay from a first execution. The saga
	// logs it and a compensation treats both identically, but it is the only
	// evidence available that a retry happened at all.
	AlreadyExisted bool
}

// ReleaseResult reports the outcome of a compensation.
type ReleaseResult struct {
	// Reservation is nil when there was nothing to release: the id is unknown.
	// A caller that needs a reservation back must therefore treat nil as
	// "nothing was held", which is exactly what a compensation wants to know.
	Reservation *Reservation
	// Released is true only when this call actually returned stock to the pool.
	// It is false for a hold that was already released, had expired, or never
	// existed — the three cases in which a caller logs "nothing to compensate"
	// and moves on.
	Released bool
}

// Repository stores stock levels and reservations.
//
// Every method takes a context so that a caller's deadline and cancellation
// reach the storage layer, which is what stops a slow write outliving the
// request that needed it.
//
// The semantics below are not suggestions. They are the guarantees the checkout
// saga is written against, and weakening one of them turns a retry into
// corrupted stock.
type Repository interface {
	// GetStock returns the requested products. Products that have never been
	// stocked are absent rather than returned as zero, because "unknown" and
	// "none left" lead an operator to different actions. An empty productIDs
	// means every stocked product.
	GetStock(ctx context.Context, productIDs []string) ([]StockItem, error)

	// SetStock sets an absolute on-hand quantity and returns the resulting row.
	// It rejects a negative quantity with ErrInvalidRequest and leaves Reserved
	// untouched: existing holds are promises, not stock.
	SetStock(ctx context.Context, productID string, quantity int64) (StockItem, error)

	// Reserve holds stock for a reservation.
	//
	// It must be idempotent on ReserveRequest.ReservationID: a request for an
	// id that already exists returns the stored reservation with
	// AlreadyExisted=true and holds nothing further. This is what makes a saga
	// retry safe.
	//
	// It must be all-or-nothing. If any line cannot be satisfied the whole
	// request fails with ErrInsufficientStock and no line remains held —
	// holding the satisfiable lines would leak stock on every failed checkout.
	//
	// A product with no stock row fails with ErrStockNotFound.
	Reserve(ctx context.Context, req ReserveRequest) (ReserveResult, error)

	// Commit converts a hold into a permanent deduction.
	//
	// It is idempotent: committing a reservation that is already committed is a
	// no-op success, so the saga's retried commit step cannot deduct twice.
	//
	// It fails with ErrReservationNotFound for an unknown id, and with
	// ErrInvalidRequest for a hold that was released, expired, or whose
	// deadline has passed: in all of those cases the units are back in the pool
	// and committing them would sell stock twice.
	Commit(ctx context.Context, reservationID string) (Reservation, error)

	// Release returns a hold to the pool.
	//
	// It must be idempotent and must not fail when the reservation does not
	// exist or was already released: a compensation runs precisely when the
	// caller does not know whether the forward step applied, so it cannot be
	// allowed to fail because there was nothing to undo. The result's Released
	// flag reports whether stock actually moved.
	//
	// Releasing a committed reservation fails with ErrInvalidRequest: those
	// units are sold.
	Release(ctx context.Context, reservationID, reason string) (ReleaseResult, error)

	// GetReservation returns a reservation, or ErrReservationNotFound.
	GetReservation(ctx context.Context, reservationID string) (Reservation, error)

	// ExpireReservations releases every active reservation whose deadline has
	// passed as of now, and returns the ones it expired so the caller can log
	// them. Expiring is not an error condition for the reservation involved:
	// the hold lapsed and the stock is back.
	ExpireReservations(ctx context.Context, now time.Time) ([]Reservation, error)
}
