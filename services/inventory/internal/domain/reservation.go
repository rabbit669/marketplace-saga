package domain

import (
	"fmt"
	"strings"
	"time"
)

// ReservationStatus is the lifecycle state of a hold.
type ReservationStatus string

const (
	// ReservationActive is stock held, awaiting a commit or a release. The only
	// state that can still move.
	ReservationActive ReservationStatus = "active"
	// ReservationCommitted is stock permanently deducted: the units are sold.
	// Terminal.
	ReservationCommitted ReservationStatus = "committed"
	// ReservationReleased is a hold the saga gave back while compensating.
	// Terminal.
	ReservationReleased ReservationStatus = "released"
	// ReservationExpired is a hold that ran out of time and was returned to the
	// pool by the expiry worker. Terminal, and deliberately distinct from
	// released: "the saga rolled back" and "nobody ever came back" are
	// different incidents, and support needs to tell them apart.
	ReservationExpired ReservationStatus = "expired"
)

// Terminal reports whether the reservation has stopped moving.
//
// Every transition is guarded by this, which is what makes a repeated commit or
// a duplicate release safe: once a hold is terminal there is nothing left to
// take, and re-deriving an earlier state from it would resurrect stock that has
// already been given away.
func (s ReservationStatus) Terminal() bool {
	switch s {
	case ReservationCommitted, ReservationReleased, ReservationExpired:
		return true
	default:
		return false
	}
}

// Valid reports whether s is a known status. Guards against a value read from
// storage being silently treated as active.
func (s ReservationStatus) Valid() bool {
	switch s {
	case ReservationActive, ReservationCommitted, ReservationReleased, ReservationExpired:
		return true
	default:
		return false
	}
}

// ReservationLine is one product and the number of units held for it.
type ReservationLine struct {
	ProductID string
	Quantity  int64
}

// Reservation is a promise: these units, for this order, until this deadline.
//
// It is the unit of compensatability in the checkout saga. Everything before
// Commit can still be undone by releasing the hold; after it the goods are sold
// and undoing the sale is a refund — a different business operation. The
// boundary is this type's state machine, so the state machine is small and
// explicit.
type Reservation struct {
	// ID is chosen by the caller and must be deterministic for the order it
	// belongs to. The order service passes its order id.
	//
	// That is what makes reserving safe to retry: the adapter keys on this id,
	// so a second attempt after a timeout returns this reservation instead of
	// holding a second set of units. Without a caller-supplied key the service
	// would have to invent one, and a saga retry would silently halve the
	// catalogue.
	ID string
	// OrderID is the order this hold belongs to. Informational: the identity of
	// a reservation is its ID, and OrderID is carried for support and audit.
	OrderID string
	// Lines are the held units, one entry per product.
	//
	// NewReservation merges repeated products, so a caller may treat this slice
	// as the complete per-product holds and sum it without de-duplicating.
	Lines []ReservationLine
	// Status is the current state. See ReservationStatus.
	Status ReservationStatus
	// CreatedAt is when the hold was taken.
	CreatedAt time.Time
	// ExpiresAt is when the expiry worker may return the units to the pool.
	ExpiresAt time.Time
	// UpdatedAt is when the reservation last changed state.
	UpdatedAt time.Time
	// ReleaseReason records why a hold was given back, e.g. "checkout_failed".
	// It is the audit trail for a compensation, which otherwise looks identical
	// to a timeout in the logs.
	ReleaseReason string
}

// NewReservationInput is a caller's request to hold stock.
type NewReservationInput struct {
	ID      string
	OrderID string
	Lines   []ReservationLine
	// TTL is how long the hold survives. Zero means DefaultReservationTTL.
	TTL time.Duration
	// Now is the clock reading used for CreatedAt and ExpiresAt. Passing it in
	// keeps the type testable and keeps time.Now out of the rules.
	Now time.Time
}

// NewReservation validates a request and builds an active reservation.
//
// It is the only way to obtain a reservation that holds anything, because every
// rule about how much stock one caller may take lives here: the number of
// lines, the size of each line, and the lifetime of the hold. An adapter that
// accepted a Reservation built by hand would bypass all of them.
func NewReservation(in NewReservationInput) (*Reservation, error) {
	if strings.TrimSpace(in.ID) == "" {
		return nil, fmt.Errorf("%w: reservation_id is required", ErrInvalidRequest)
	}
	if len(in.Lines) == 0 {
		return nil, fmt.Errorf("%w: a reservation must hold at least one line", ErrInvalidRequest)
	}
	if len(in.Lines) > MaxLinesPerReservation {
		return nil, fmt.Errorf("%w: a reservation may hold at most %d lines, got %d",
			ErrInvalidRequest, MaxLinesPerReservation, len(in.Lines))
	}

	for i, l := range in.Lines {
		if strings.TrimSpace(l.ProductID) == "" {
			return nil, fmt.Errorf("%w: line %d has no product_id", ErrInvalidRequest, i)
		}
		if l.Quantity <= 0 {
			return nil, fmt.Errorf("%w: line %d for product %s must have a positive quantity, got %d",
				ErrInvalidRequest, i, l.ProductID, l.Quantity)
		}
		if l.Quantity > MaxQuantityPerLine {
			return nil, fmt.Errorf("%w: line %d for product %s asks for %d units, above the per-line maximum of %d",
				ErrInvalidRequest, i, l.ProductID, l.Quantity, MaxQuantityPerLine)
		}
	}

	lines, err := mergeLines(in.Lines)
	if err != nil {
		return nil, err
	}

	ttl := in.TTL
	if ttl <= 0 {
		ttl = DefaultReservationTTL
	}

	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}

	return &Reservation{
		ID:        in.ID,
		OrderID:   in.OrderID,
		Lines:     lines,
		Status:    ReservationActive,
		CreatedAt: now,
		ExpiresAt: now.Add(ttl),
		UpdatedAt: now,
	}, nil
}

// mergeLines collapses repeated products into one line per product, keeping the
// first-seen order.
//
// A cart may legitimately contain the same product on two lines, and the order
// service does not forbid it. Holding the sum is what the customer actually
// asked for; holding only one of the lines would ship a partial order, and
// rejecting the request would fail a checkout that the shop considers valid.
// The first-seen order is preserved so that the stored reservation does not
// depend on map iteration order.
func mergeLines(lines []ReservationLine) ([]ReservationLine, error) {
	merged := make([]ReservationLine, 0, len(lines))
	index := make(map[string]int, len(lines))

	for _, l := range lines {
		at, seen := index[l.ProductID]
		if !seen {
			index[l.ProductID] = len(merged)
			merged = append(merged, ReservationLine{ProductID: l.ProductID, Quantity: l.Quantity})
			continue
		}
		merged[at].Quantity += l.Quantity
	}

	// The per-line bound is re-checked on the merged total, because the merged
	// total is what is actually held: two lines of 600 units are one hold of
	// 1200, and each line passed the check on its own.
	for _, l := range merged {
		if l.Quantity > MaxQuantityPerLine {
			return nil, fmt.Errorf("%w: product %s appears on several lines totalling %d units, above the per-line maximum of %d",
				ErrInvalidRequest, l.ProductID, l.Quantity, MaxQuantityPerLine)
		}
	}
	return merged, nil
}

// Clone returns a deep copy.
//
// The adapter stores reservations behind a lock and hands copies out; without
// the copy, a caller that appended to Lines would be writing into the store's
// live state from outside the lock.
func (r Reservation) Clone() Reservation {
	cp := r
	cp.Lines = make([]ReservationLine, len(r.Lines))
	copy(cp.Lines, r.Lines)
	return cp
}

// Rehydrate rebuilds a reservation from storage, re-running the same validation
// as NewReservation but accepting a non-active status and any TTL, because a
// stored reservation is expected to have moved on and to have been created
// under whatever bounds applied at the time.
func Rehydrate(r Reservation) (*Reservation, error) {
	if strings.TrimSpace(r.ID) == "" {
		return nil, fmt.Errorf("%w: reservation_id is required", ErrInvalidRequest)
	}
	if !r.Status.Valid() {
		return nil, fmt.Errorf("%w: reservation %s has unknown status %q", ErrInvalidRequest, r.ID, r.Status)
	}
	if len(r.Lines) == 0 {
		return nil, fmt.Errorf("%w: reservation %s holds no lines", ErrInvalidRequest, r.ID)
	}
	for _, l := range r.Lines {
		if strings.TrimSpace(l.ProductID) == "" || l.Quantity <= 0 {
			return nil, fmt.Errorf("%w: reservation %s has an invalid line for product %q", ErrInvalidRequest, r.ID, l.ProductID)
		}
	}
	return &r, nil
}

// IsExpired reports whether the hold's deadline has passed.
//
// A terminal reservation is never "expired": it has already been dealt with,
// and calling it expired would invite a second release of stock that was
// committed. The deadline itself belongs to the hold — now >= ExpiresAt is
// late, not early — and that is the conservative reading, because refusing a
// commit that arrives exactly on the deadline can only delay an order, while
// allowing one can sell units the pool has already promised elsewhere.
func (r Reservation) IsExpired(now time.Time) bool {
	if r.Status.Terminal() {
		return false
	}
	return !now.Before(r.ExpiresAt)
}

// Commit turns an active hold into a permanent deduction.
//
// The bool reports whether this call performed the transition. A repeated
// commit — the saga's commit step is retried by the recovery worker — returns
// (false, nil), which is what lets the adapter deduct stock exactly once
// without keeping its own record of what it has already done.
//
// The two failure cases are the interesting ones:
//
//   - A released or expired hold cannot be committed. The units are back in the
//     pool and may already be promised to somebody else; committing would sell
//     stock twice, which is precisely the outcome this service exists to
//     prevent. It is reported instead, so the order ends up in front of a human.
//   - An active hold whose deadline has passed cannot be committed either, for
//     the same reason: the expiry worker may have returned the units the moment
//     the saga stopped looking. Failing loudly beats a silent oversell.
func (r *Reservation) Commit(now time.Time) (bool, error) {
	switch r.Status {
	case ReservationCommitted:
		// Idempotent by design: this is a retry, not a second sale.
		return false, nil

	case ReservationActive:
		if r.IsExpired(now) {
			return false, fmt.Errorf("%w: reservation %s expired at %s and its stock was returned to the pool",
				ErrInvalidRequest, r.ID, r.ExpiresAt.UTC().Format(time.RFC3339))
		}
		r.Status = ReservationCommitted
		r.UpdatedAt = now
		return true, nil

	default:
		return false, fmt.Errorf("%w: reservation %s is %s and cannot be committed",
			ErrInvalidRequest, r.ID, r.Status)
	}
}

// Release returns an active hold to the pool. It is the saga's compensation for
// a failed checkout.
//
// The bool reports whether stock was actually given back. A release of an
// already-released reservation returns (false, nil): a compensation runs
// precisely when the caller does not know whether the forward step applied, so
// "there was nothing to undo" is a success, not an error. The same applies to
// an expired hold — the units went back when the deadline passed, and the
// reason recorded here would be a lie about who released them.
//
// A committed reservation is the one case that must fail. Those units are sold;
// releasing them would return stock that the customer has paid for and make it
// possible to oversell retroactively.
func (r *Reservation) Release(reason string, now time.Time) (bool, error) {
	switch r.Status {
	case ReservationReleased, ReservationExpired:
		return false, nil

	case ReservationActive:
		r.Status = ReservationReleased
		r.ReleaseReason = reason
		r.UpdatedAt = now
		return true, nil

	default:
		return false, fmt.Errorf("%w: reservation %s is %s and cannot be released",
			ErrInvalidRequest, r.ID, r.Status)
	}
}

// Expire transitions an overdue active hold to expired.
//
// The bool reports whether the transition happened; everything else — a hold
// that is not yet due, or one that has already been committed or released —
// returns false and is left alone. There is no error case on purpose: the
// caller is a sweep over every reservation in the store, and "this one was not
// due yet" is an ordinary answer rather than a failure. Reporting it as an
// error would make the worker log noise for every healthy reservation.
func (r *Reservation) Expire(now time.Time) bool {
	if r.Status != ReservationActive || !r.IsExpired(now) {
		return false
	}
	r.Status = ReservationExpired
	r.UpdatedAt = now
	return true
}
