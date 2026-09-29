// Package memory is an in-memory implementation of the inventory domain's
// Repository port.
//
// It exists so the reservation protocol can be exercised at full speed, and so
// the service runs out of the box in development. It is not a toy: the two
// properties the checkout saga depends on — a reservation is never held twice
// for one id, and a multi-line hold is all-or-nothing — are the reason this
// package has tests of its own rather than being mocked away.
//
// # Why a single mutex is enough to make overselling impossible
//
// There is one lock, it is held for the whole of every read-modify-write, and
// there is exactly one place that increases the reserved count. Concretely, let
// `free(p) = quantity(p) - reserved(p)`. Reserve does:
//
//	lock
//	  for every line: assert free(p) >= quantity(line)      (phase 1, read only)
//	  for every line: reserved(p) += quantity(line)         (phase 2, write)
//	unlock
//
// Because both phases run under the same lock with no other writer able to
// interleave, the value of free(p) that phase 1 observed is still the value
// phase 2 acts on: a request is only applied if the stock was sufficient at the
// moment of application. Every other mutation — commit, release, expiry —
// either decreases reserved(p) or decreases quantity(p) and reserved(p)
// together, so none of them can raise free(p) above what phase 1 saw. By
// induction, the sum of the holds taken for a product never exceeds its
// quantity, which is the definition of not overselling.
//
// The lock is an RWMutex rather than a Mutex because reads dominate in
// production: a catalogue page asks for stock levels far more often than a
// checkout changes them, and readers that never touch reservation state can run
// concurrently.
package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/marketplace/services/inventory/internal/domain"
)

// Options configures a Store.
type Options struct {
	// Clock supplies the current time. Injecting it keeps expiry testable
	// without sleeping. Defaults to time.Now.
	Clock func() time.Time
	// DefaultTTL is applied when a reserve request does not ask for a TTL of
	// its own. Zero falls back to domain.DefaultReservationTTL.
	DefaultTTL time.Duration
}

// Store is an in-memory Repository.
//
// Both maps are guarded by mu; nothing outside this package ever sees the
// stored values, and every method that returns one returns a copy. That is what
// makes the RWMutex sufficient: there is no path by which a caller can mutate
// state without the lock.
type Store struct {
	mu           sync.RWMutex
	stock        map[string]*domain.StockItem
	reservations map[string]*domain.Reservation

	clock      func() time.Time
	defaultTTL time.Duration
}

// New builds an empty store.
func New(opts Options) *Store {
	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}
	return &Store{
		stock:        make(map[string]*domain.StockItem),
		reservations: make(map[string]*domain.Reservation),
		clock:        clock,
		defaultTTL:   opts.DefaultTTL,
	}
}

// --------------------------------------------------------------- stock

// GetStock returns the requested rows, in the order asked for.
//
// Unknown products are dropped rather than zero-filled: the proto promises
// "absent means never stocked", and an operator cannot tell an empty shelf from
// a product this service has never heard of if both come back as 0. Repeats in
// the request are collapsed, and an empty request means every stocked product,
// which is what the admin view needs.
func (s *Store) GetStock(ctx context.Context, productIDs []string) ([]domain.StockItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(productIDs) == 0 {
		return s.allStockLocked(), nil
	}

	items := make([]domain.StockItem, 0, len(productIDs))
	seen := make(map[string]struct{}, len(productIDs))
	for _, id := range productIDs {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		if item, ok := s.stock[id]; ok {
			items = append(items, *item)
		}
	}
	return items, nil
}

// allStockLocked returns every row, ordered by product id.
//
// The order is stable so that an unfiltered response — and a test asserting on
// one — does not depend on Go's map iteration order.
func (s *Store) allStockLocked() []domain.StockItem {
	items := make([]domain.StockItem, 0, len(s.stock))
	for _, item := range s.stock {
		items = append(items, *item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ProductID < items[j].ProductID })
	return items
}

// SetStock sets an absolute on-hand quantity, creating the row if needed.
//
// Reserved is left alone. An admin adjusting a stock count is describing the
// warehouse, not cancelling orders; the holds already granted were promises and
// are still honoured, even if the corrected count is lower than what is held.
// See domain.StockItem.SetQuantity.
func (s *Store) SetStock(ctx context.Context, productID string, quantity int64) (domain.StockItem, error) {
	if err := ctx.Err(); err != nil {
		return domain.StockItem{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock()

	if item, ok := s.stock[productID]; ok {
		if err := item.SetQuantity(quantity, now); err != nil {
			return domain.StockItem{}, err
		}
		return *item, nil
	}

	item, err := domain.NewStockItem(productID, quantity, now)
	if err != nil {
		return domain.StockItem{}, err
	}
	s.stock[productID] = item
	return *item, nil
}

// --------------------------------------------------------- reservations

// Reserve holds stock, idempotently and all-or-nothing.
//
// The request is validated before the lock is taken, so a malformed request
// never blocks a concurrent checkout and never leaves a trace in the store. The
// consequence is that a replay has to be well formed to be answered with the
// original reservation, which a genuine retry always is: it is the same request
// that was accepted the first time.
func (s *Store) Reserve(ctx context.Context, req domain.ReserveRequest) (domain.ReserveResult, error) {
	if err := ctx.Err(); err != nil {
		return domain.ReserveResult{}, err
	}

	ttl := req.TTL
	if ttl <= 0 {
		// A zero here is a deliberate "use the service default"; the domain
		// applies DefaultReservationTTL if this store has none configured.
		ttl = s.defaultTTL
	}

	pending, err := domain.NewReservation(domain.NewReservationInput{
		ID:      req.ReservationID,
		OrderID: req.OrderID,
		Lines:   req.Lines,
		TTL:     ttl,
		Now:     s.clock(),
	})
	if err != nil {
		return domain.ReserveResult{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Idempotency comes first, before any stock is examined. A retry must
	// succeed with the original answer even if the catalogue has moved on since
	// — the units it holds are already spoken for, and re-checking them against
	// today's availability would fail a reservation that is perfectly valid.
	if existing, ok := s.reservations[pending.ID]; ok {
		return domain.ReserveResult{Reservation: existing.Clone(), AlreadyExisted: true}, nil
	}

	// NewReservation has merged repeated products already, so these lines are
	// already aggregate per product. The aggregation is repeated anyway,
	// because the check-then-hold sequence below is this service's entire
	// safety property and it must not rest on a guarantee made in another
	// layer: two lines of the same product checked separately would each pass
	// against the same free units.
	holds := aggregate(pending.Lines)

	// Phase 1: check everything, mutate nothing. A failure here leaves the
	// store exactly as it was, which is what makes a partially satisfiable
	// request hold none of its lines instead of some of them. Holding what fits
	// would be worse than failing: the saga would compensate a reservation that
	// it believes failed, and the leftover holds would sit on the shelf until
	// their TTL expired.
	for _, line := range holds {
		item, ok := s.stock[line.ProductID]
		if !ok {
			return domain.ReserveResult{}, fmt.Errorf("%w: product %s", domain.ErrStockNotFound, line.ProductID)
		}
		if err := item.CheckAvailable(line.Quantity); err != nil {
			return domain.ReserveResult{}, err
		}
	}

	// Phase 2: apply. Every check above passed under this lock, so nothing can
	// fail here and the request becomes atomic by construction.
	for _, line := range holds {
		s.stock[line.ProductID].Hold(line.Quantity)
	}

	s.reservations[pending.ID] = pending

	return domain.ReserveResult{Reservation: pending.Clone()}, nil
}

// Commit deducts held stock permanently.
func (s *Store) Commit(ctx context.Context, reservationID string) (domain.Reservation, error) {
	if err := ctx.Err(); err != nil {
		return domain.Reservation{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	res, ok := s.reservations[reservationID]
	if !ok {
		return domain.Reservation{}, fmt.Errorf("%w: %s", domain.ErrReservationNotFound, reservationID)
	}

	now := s.clock()

	// An overdue hold is expired here, before the decision, rather than being
	// left for the worker. The worker is a sweep on a timer; if it is down, or
	// the process restarted between ticks, a hold can be past its deadline
	// while still marked active, and its units must not be sellable twice.
	s.expireIfDue(res, now)

	applied, err := res.Commit(now)
	if err != nil {
		return domain.Reservation{}, err
	}

	// applied is false only for a replay of an already-committed reservation.
	// Deducting again there would sell the same units twice.
	if applied {
		s.eachHeldRow(res, func(item *domain.StockItem, line domain.ReservationLine) {
			item.CommitHold(line.Quantity, now)
		})
	}

	return res.Clone(), nil
}

// Release gives a hold back to the pool.
//
// The three "nothing to do" cases — unknown id, already released, already
// expired — all succeed. A compensation runs precisely when the caller cannot
// know whether the forward step applied, so refusing to compensate an unknown
// reservation would strand the saga on the one path it most needs to work. The
// result's Released flag is the honest answer to "did I return anything".
func (s *Store) Release(ctx context.Context, reservationID, reason string) (domain.ReleaseResult, error) {
	if err := ctx.Err(); err != nil {
		return domain.ReleaseResult{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	res, ok := s.reservations[reservationID]
	if !ok {
		return domain.ReleaseResult{Reservation: nil, Released: false}, nil
	}

	now := s.clock()
	s.expireIfDue(res, now)

	applied, err := res.Release(reason, now)
	if err != nil {
		return domain.ReleaseResult{}, err
	}

	if applied {
		s.eachHeldRow(res, func(item *domain.StockItem, line domain.ReservationLine) {
			item.ReleaseHold(line.Quantity, now)
		})
	}

	released := res.Clone()
	return domain.ReleaseResult{Reservation: &released, Released: applied}, nil
}

// GetReservation returns a reservation by id.
//
// It is a read, so it does not expire anything, and the status it reports is
// the recorded one: a hold past its deadline but not yet swept reads as active
// with an ExpiresAt in the past. Callers that need the effective state can see
// it from that timestamp, and inventing a transition inside a read would make
// GET return different answers depending on which writer ran first.
func (s *Store) GetReservation(ctx context.Context, reservationID string) (domain.Reservation, error) {
	if err := ctx.Err(); err != nil {
		return domain.Reservation{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	res, ok := s.reservations[reservationID]
	if !ok {
		return domain.Reservation{}, fmt.Errorf("%w: %s", domain.ErrReservationNotFound, reservationID)
	}
	return res.Clone(), nil
}

// ExpireReservations returns the units of every overdue hold to the pool.
//
// It returns the reservations it expired so the caller can log them: an
// expiring hold is invisible otherwise — no request caused it, and the customer
// who abandoned the checkout will never ask — which makes the log line the only
// evidence that stock was frozen for fifteen minutes.
//
// Expired reservations are kept rather than deleted. The saga that owns one may
// still ask about it (and must be told "expired" rather than "unknown"), and
// the id must stay claimed so that a late retry cannot re-reserve the same id
// and hold the stock a second time. A persistent adapter would need a retention
// window; in memory the process lifetime is the window.
func (s *Store) ExpireReservations(ctx context.Context, now time.Time) ([]domain.Reservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	expired := make([]domain.Reservation, 0, 4)
	for _, id := range s.sortedReservationIDsLocked() {
		res := s.reservations[id]
		if s.expireIfDue(res, now) {
			expired = append(expired, res.Clone())
		}
	}
	return expired, nil
}

// ------------------------------------------------------------- internal

// expireIfDue transitions an overdue hold to expired and returns its units.
//
// It is called from the sweep and from both of the mutating paths, so that the
// service's answer never depends on the worker being alive: a hold's deadline
// is a property of the hold, not of the timer that notices it.
func (s *Store) expireIfDue(res *domain.Reservation, now time.Time) bool {
	if !res.Expire(now) {
		return false
	}
	s.eachHeldRow(res, func(item *domain.StockItem, line domain.ReservationLine) {
		item.ReleaseHold(line.Quantity, now)
	})
	return true
}

// eachHeldRow calls fn for each stock row a reservation holds.
//
// A missing row is skipped. That is unreachable — Reserve refuses to hold a
// product that has no row, and SetStock never removes one — and skipping is the
// right response to an impossible state: failing halfway through a commit would
// leave a committed reservation whose stock was never deducted, which is a
// second bug on top of the first.
func (s *Store) eachHeldRow(res *domain.Reservation, fn func(item *domain.StockItem, line domain.ReservationLine)) {
	for _, line := range res.Lines {
		if item, ok := s.stock[line.ProductID]; ok {
			fn(item, line)
		}
	}
}

// sortedReservationIDsLocked orders ids so that a sweep always visits
// reservations in the same order, which keeps its log output and its tests
// deterministic.
func (s *Store) sortedReservationIDsLocked() []string {
	ids := make([]string, 0, len(s.reservations))
	for id := range s.reservations {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// aggregate sums repeated products into one line, preserving first-seen order.
//
// NewReservation already guarantees distinct products, so this is a no-op on
// every request that reaches the store through the normal path. It is repeated
// here because phase 1 must check the total a request wants per product, and
// relying on an invariant enforced in another package for the check that
// prevents overselling would make the guarantee only as strong as the weakest
// caller.
func aggregate(lines []domain.ReservationLine) []domain.ReservationLine {
	sums := make([]domain.ReservationLine, 0, len(lines))
	index := make(map[string]int, len(lines))

	for _, line := range lines {
		if at, seen := index[line.ProductID]; seen {
			sums[at].Quantity += line.Quantity
			continue
		}
		index[line.ProductID] = len(sums)
		sums = append(sums, line)
	}
	return sums
}
