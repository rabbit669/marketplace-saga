package memory_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marketplace/services/inventory/internal/adapters/memory"
	"github.com/marketplace/services/inventory/internal/domain"
)

// fakeClock makes expiry a matter of arithmetic rather than sleep. Tests that
// sleep to cross a deadline are slow and flaky, and this suite runs under -race
// where every millisecond of real time is multiplied.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// newStore returns a store with a controllable clock, which is what every test
// below needs in order to talk about TTLs meaningfully.
func newStore(t *testing.T, defaultTTL time.Duration) (*memory.Store, *fakeClock) {
	t.Helper()
	clock := newFakeClock()
	return memory.New(memory.Options{Clock: clock.Now, DefaultTTL: defaultTTL}), clock
}

// mustSetStock fails the test if the write is rejected.
func mustSetStock(t *testing.T, store *memory.Store, productID string, quantity int64) domain.StockItem {
	t.Helper()
	item, err := store.SetStock(context.Background(), productID, quantity)
	if err != nil {
		t.Fatalf("SetStock(%s, %d): %v", productID, quantity, err)
	}
	return item
}

// mustReserve fails the test if the reservation is rejected.
func mustReserve(t *testing.T, store *memory.Store, id string, lines ...domain.ReservationLine) domain.ReserveResult {
	t.Helper()
	res, err := store.Reserve(context.Background(), domain.ReserveRequest{ReservationID: id, OrderID: "order-" + id, Lines: lines})
	if err != nil {
		t.Fatalf("Reserve(%s): %v", id, err)
	}
	return res
}

// reservedOf reports how many units of a product are currently held.
func reservedOf(t *testing.T, store *memory.Store, productID string) int64 {
	t.Helper()
	items, err := store.GetStock(context.Background(), []string{productID})
	if err != nil {
		t.Fatalf("GetStock(%s): %v", productID, err)
	}
	if len(items) == 0 {
		t.Fatalf("product %s has no stock row", productID)
	}
	return items[0].Reserved
}

// ------------------------------------------------------------------- stock

func TestSetStockAndGetStock(t *testing.T) {
	store, _ := newStore(t, 0)
	ctx := context.Background()

	item := mustSetStock(t, store, "p-1", 100)
	if item.Quantity != 100 || item.Reserved != 0 || item.Available() != 100 {
		t.Fatalf("item = %+v, want 100 on hand and free", item)
	}

	found, err := store.GetStock(ctx, []string{"p-1"})
	if err != nil {
		t.Fatalf("GetStock: %v", err)
	}
	if len(found) != 1 || found[0].ProductID != "p-1" {
		t.Fatalf("items = %+v, want the one row", found)
	}
}

func TestGetStockOmitsProductsThatWereNeverStocked(t *testing.T) {
	store, _ := newStore(t, 0)
	mustSetStock(t, store, "p-1", 5)

	// "Never stocked" and "none left" must not look the same to a caller: the
	// first is a catalogue bug, the second is a sold-out product.
	items, err := store.GetStock(context.Background(), []string{"p-1", "p-unknown"})
	if err != nil {
		t.Fatalf("GetStock: %v", err)
	}
	if len(items) != 1 || items[0].ProductID != "p-1" {
		t.Fatalf("items = %+v, want only p-1", items)
	}

	empty, err := store.GetStock(context.Background(), []string{"p-unknown"})
	if err != nil {
		t.Fatalf("GetStock: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("items = %+v, want none", empty)
	}
}

func TestGetStockWithoutFilterReturnsEverythingInAStableOrder(t *testing.T) {
	store, _ := newStore(t, 0)
	mustSetStock(t, store, "p-2", 1)
	mustSetStock(t, store, "p-1", 1)
	mustSetStock(t, store, "p-10", 1)

	items, err := store.GetStock(context.Background(), nil)
	if err != nil {
		t.Fatalf("GetStock: %v", err)
	}

	want := []string{"p-1", "p-10", "p-2"}
	if len(items) != len(want) {
		t.Fatalf("items = %+v, want %d", items, len(want))
	}
	for i, id := range want {
		if items[i].ProductID != id {
			t.Fatalf("items = %+v, want %v: the order must not depend on map iteration", items, want)
		}
	}
}

func TestGetStockCollapsesRepeatedIDs(t *testing.T) {
	store, _ := newStore(t, 0)
	mustSetStock(t, store, "p-1", 5)

	items, err := store.GetStock(context.Background(), []string{"p-1", "p-1", "p-1"})
	if err != nil {
		t.Fatalf("GetStock: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("items = %+v, want one entry", items)
	}
}

func TestSetStockRejectsNegativeAndLeavesTheRowAlone(t *testing.T) {
	store, _ := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 10)

	_, err := store.SetStock(ctx, "p-1", -1)
	if !errors.Is(err, domain.ErrInvalidRequest) {
		t.Fatalf("error = %v, want ErrInvalidRequest", err)
	}

	items, _ := store.GetStock(ctx, []string{"p-1"})
	if items[0].Quantity != 10 {
		t.Fatalf("Quantity = %d, want 10: a rejected write must not change the row", items[0].Quantity)
	}

	if _, err := store.SetStock(ctx, "  ", 5); !errors.Is(err, domain.ErrInvalidRequest) {
		t.Fatalf("blank product id = %v, want ErrInvalidRequest", err)
	}
}

func TestSetStockIsAbsoluteAndKeepsHolds(t *testing.T) {
	store, _ := newStore(t, 0)
	mustSetStock(t, store, "p-1", 10)
	mustReserve(t, store, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 4})

	// An admin re-counts the shelf. The order in flight is not cancelled by it.
	item := mustSetStock(t, store, "p-1", 4)
	if item.Quantity != 4 || item.Reserved != 4 || item.Available() != 0 {
		t.Fatalf("item = %+v, want the hold intact on a corrected shelf", item)
	}
}

// ---------------------------------------------------------------- reserve

func TestReserveHoldsStock(t *testing.T) {
	store, _ := newStore(t, 0)
	mustSetStock(t, store, "p-1", 10)
	mustSetStock(t, store, "p-2", 5)

	result := mustReserve(t, store, "r-1",
		domain.ReservationLine{ProductID: "p-1", Quantity: 4},
		domain.ReservationLine{ProductID: "p-2", Quantity: 5},
	)

	if result.AlreadyExisted {
		t.Error("a first reserve reported AlreadyExisted")
	}
	if result.Reservation.Status != domain.ReservationActive {
		t.Errorf("status = %q, want active", result.Reservation.Status)
	}
	if result.Reservation.ExpiresAt.Sub(result.Reservation.CreatedAt) != domain.DefaultReservationTTL {
		t.Errorf("TTL = %s, want %s", result.Reservation.ExpiresAt.Sub(result.Reservation.CreatedAt), domain.DefaultReservationTTL)
	}
	if got := reservedOf(t, store, "p-1"); got != 4 {
		t.Errorf("p-1 reserved = %d, want 4", got)
	}
	if got := reservedOf(t, store, "p-2"); got != 5 {
		t.Errorf("p-2 reserved = %d, want 5", got)
	}
}

func TestReserveIsAllOrNothing(t *testing.T) {
	store, _ := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 10)
	mustSetStock(t, store, "p-2", 5)

	// The failing line is deliberately last. An implementation that held each
	// line as it checked it would leave p-2 held below, and the saga would then
	// compensate a reservation it believes failed.
	_, err := store.Reserve(ctx, domain.ReserveRequest{
		ReservationID: "r-partial",
		Lines: []domain.ReservationLine{
			{ProductID: "p-2", Quantity: 2},
			{ProductID: "p-1", Quantity: 11},
		},
	})
	if !errors.Is(err, domain.ErrInsufficientStock) {
		t.Fatalf("error = %v, want ErrInsufficientStock", err)
	}

	if got := reservedOf(t, store, "p-2"); got != 0 {
		t.Fatalf("p-2 reserved = %d, want 0: a failed multi-line reserve held its first lines", got)
	}
	if got := reservedOf(t, store, "p-1"); got != 0 {
		t.Fatalf("p-1 reserved = %d, want 0", got)
	}

	// And no reservation was created, so a retry of the same id is a fresh
	// attempt rather than a replay of a failure.
	if _, err := store.GetReservation(ctx, "r-partial"); !errors.Is(err, domain.ErrReservationNotFound) {
		t.Fatalf("GetReservation = %v, want ErrReservationNotFound", err)
	}
}

func TestReserveFailsOnAnUnknownProduct(t *testing.T) {
	store, _ := newStore(t, 0)
	mustSetStock(t, store, "p-1", 10)

	_, err := store.Reserve(context.Background(), domain.ReserveRequest{
		ReservationID: "r-1",
		Lines: []domain.ReservationLine{
			{ProductID: "p-1", Quantity: 1},
			{ProductID: "p-unknown", Quantity: 1},
		},
	})
	if !errors.Is(err, domain.ErrStockNotFound) {
		t.Fatalf("error = %v, want ErrStockNotFound", err)
	}
	if got := reservedOf(t, store, "p-1"); got != 0 {
		t.Fatalf("p-1 reserved = %d, want 0", got)
	}
}

func TestReserveIsIdempotentOnReservationID(t *testing.T) {
	store, _ := newStore(t, 0)
	mustSetStock(t, store, "p-1", 10)

	first := mustReserve(t, store, "order-1", domain.ReservationLine{ProductID: "p-1", Quantity: 4})

	// This is the saga's retry: same deterministic id, same lines, and the
	// answer must be the original hold rather than a second one. A second hold
	// would silently halve the catalogue for every other customer.
	second := mustReserve(t, store, "order-1", domain.ReservationLine{ProductID: "p-1", Quantity: 4})
	if !second.AlreadyExisted {
		t.Error("a replay did not report AlreadyExisted")
	}
	if !second.Reservation.ExpiresAt.Equal(first.Reservation.ExpiresAt) {
		t.Errorf("ExpiresAt = %v, want the original %v: a replay must not extend the hold",
			second.Reservation.ExpiresAt, first.Reservation.ExpiresAt)
	}
	if got := reservedOf(t, store, "p-1"); got != 4 {
		t.Fatalf("p-1 reserved = %d, want 4: stock was held twice", got)
	}

	// A caller that reused an id with different lines is buggy, but the id is
	// the contract: failing here would break the retry safety the whole
	// protocol rests on, so the original reservation is returned instead.
	third := mustReserve(t, store, "order-1", domain.ReservationLine{ProductID: "p-1", Quantity: 9})
	if !third.AlreadyExisted {
		t.Error("a conflicting replay did not report AlreadyExisted")
	}
	if third.Reservation.Lines[0].Quantity != 4 {
		t.Errorf("lines = %+v, want the original hold", third.Reservation.Lines)
	}
	if got := reservedOf(t, store, "p-1"); got != 4 {
		t.Fatalf("p-1 reserved = %d, want 4", got)
	}
}

func TestReserveRejectsInvalidRequestsWithoutHoldingStock(t *testing.T) {
	store, _ := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 10)

	cases := map[string]domain.ReserveRequest{
		"no reservation id": {Lines: []domain.ReservationLine{{ProductID: "p-1", Quantity: 1}}},
		"no lines":          {ReservationID: "r-1"},
		"zero quantity":     {ReservationID: "r-1", Lines: []domain.ReservationLine{{ProductID: "p-1"}}},
		"no product":        {ReservationID: "r-1", Lines: []domain.ReservationLine{{Quantity: 1}}},
		"quantity too big":  {ReservationID: "r-1", Lines: []domain.ReservationLine{{ProductID: "p-1", Quantity: domain.MaxQuantityPerLine + 1}}},
	}

	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := store.Reserve(ctx, req); !errors.Is(err, domain.ErrInvalidRequest) {
				t.Fatalf("error = %v, want ErrInvalidRequest", err)
			}
		})
	}

	if got := reservedOf(t, store, "p-1"); got != 0 {
		t.Fatalf("p-1 reserved = %d, want 0: a rejected reserve held stock", got)
	}
}

func TestReserveUsesTheStoreDefaultTTL(t *testing.T) {
	store, clock := newStore(t, 2*time.Minute)
	mustSetStock(t, store, "p-1", 10)

	res := mustReserve(t, store, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 1})
	if want := clock.Now().Add(2 * time.Minute); !res.Reservation.ExpiresAt.Equal(want) {
		t.Fatalf("ExpiresAt = %v, want the store default %v", res.Reservation.ExpiresAt, want)
	}

	// An explicit TTL wins over the default.
	explicit, err := store.Reserve(context.Background(), domain.ReserveRequest{
		ReservationID: "r-2",
		TTL:           30 * time.Second,
		Lines:         []domain.ReservationLine{{ProductID: "p-1", Quantity: 1}},
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if want := clock.Now().Add(30 * time.Second); !explicit.Reservation.ExpiresAt.Equal(want) {
		t.Fatalf("ExpiresAt = %v, want %v", explicit.Reservation.ExpiresAt, want)
	}
}

func TestReserveWithACancelledContextHoldsNothing(t *testing.T) {
	store, _ := newStore(t, 0)
	mustSetStock(t, store, "p-1", 10)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := store.Reserve(ctx, domain.ReserveRequest{
		ReservationID: "r-1",
		Lines:         []domain.ReservationLine{{ProductID: "p-1", Quantity: 1}},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if got := reservedOf(t, store, "p-1"); got != 0 {
		t.Fatalf("p-1 reserved = %d, want 0: a call that was already cancelled held stock", got)
	}
}

// ----------------------------------------------------------- concurrency

// TestConcurrentReserveNeverOversells is the test this service exists for.
//
// Many goroutines race for a fixed number of units, each with its own
// reservation id, so nothing but the store's own serialisation can stop them
// from all succeeding. The assertions are deliberately exact rather than
// approximate: "no more than the stock was held" is the safety property, and
// "the stock was fully used" is the liveness property. A store that simply
// refused most requests would pass the first and fail the second.
func TestConcurrentReserveNeverOversells(t *testing.T) {
	const (
		stock          = 100
		perReservation = 3
		goroutines     = 300
	)

	store, _ := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", stock)

	var (
		succeeded atomic.Int64
		units     atomic.Int64
		wg        sync.WaitGroup
		start     = make(chan struct{})
	)

	// A watcher samples the store while the race is running. It is what turns
	// "the final total is right" into "the invariant held at every moment":
	// a store that briefly held 103 units and corrected itself afterwards would
	// pass the final check and fail this one.
	stopWatcher := make(chan struct{})
	watcherStopped := make(chan struct{})
	go func() {
		defer close(watcherStopped)
		for {
			select {
			case <-stopWatcher:
				return
			default:
			}

			items, err := store.GetStock(ctx, []string{"p-1"})
			if err != nil {
				t.Errorf("watcher: GetStock: %v", err)
				return
			}
			if len(items) == 0 {
				t.Errorf("watcher: p-1 disappeared")
				return
			}
			if item := items[0]; item.Reserved > item.Quantity || item.Reserved > stock {
				t.Errorf("watcher: reserved %d of %d exceeds the stock of %d", item.Reserved, item.Quantity, stock)
				return
			}
		}
	}()

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start

			res, err := store.Reserve(ctx, domain.ReserveRequest{
				ReservationID: fmt.Sprintf("r-%d", i),
				OrderID:       fmt.Sprintf("order-%d", i),
				Lines:         []domain.ReservationLine{{ProductID: "p-1", Quantity: perReservation}},
			})
			switch {
			case err == nil:
				succeeded.Add(1)
				for _, line := range res.Reservation.Lines {
					units.Add(line.Quantity)
				}
				if res.AlreadyExisted {
					t.Errorf("reservation r-%d is unique, but the store called it a replay", i)
				}
			case errors.Is(err, domain.ErrInsufficientStock):
				// Expected for everyone who arrives after the stock is gone.
			default:
				t.Errorf("reservation r-%d failed unexpectedly: %v", i, err)
			}
		}(i)
	}

	close(start)
	wg.Wait()
	close(stopWatcher)
	<-watcherStopped

	// Exactly floor(100/3) = 33 reservations fit, holding 99 of the 100 units.
	const wantSuccesses = stock / perReservation
	if got := succeeded.Load(); got != wantSuccesses {
		t.Errorf("successful reservations = %d, want exactly %d", got, wantSuccesses)
	}
	if got := units.Load(); got != wantSuccesses*perReservation {
		t.Errorf("units held by successful reservations = %d, want %d", got, wantSuccesses*perReservation)
	}

	items, err := store.GetStock(ctx, []string{"p-1"})
	if err != nil {
		t.Fatalf("GetStock: %v", err)
	}
	if items[0].Reserved != units.Load() {
		t.Errorf("store holds %d units but %d were reported reserved", units.Load(), items[0].Reserved)
	}
	if items[0].Reserved > items[0].Quantity {
		t.Fatalf("oversold: reserved %d of %d", items[0].Reserved, items[0].Quantity)
	}
	if items[0].Available() != stock-wantSuccesses*perReservation {
		t.Errorf("available = %d, want %d", items[0].Available(), stock-wantSuccesses*perReservation)
	}
}

// TestConcurrentReserveWithVaryingQuantities checks the same invariant when the
// requests are not uniform, so no single arithmetic shortcut can satisfy it.
func TestConcurrentReserveWithVaryingQuantities(t *testing.T) {
	const stock = 250

	store, _ := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", stock)

	var (
		units     atomic.Int64
		wg        sync.WaitGroup
		start     = make(chan struct{})
		successes atomic.Int64
	)

	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start

			qty := int64(i%7) + 1
			res, err := store.Reserve(ctx, domain.ReserveRequest{
				ReservationID: fmt.Sprintf("r-%d", i),
				Lines:         []domain.ReservationLine{{ProductID: "p-1", Quantity: qty}},
			})
			if err != nil {
				if !errors.Is(err, domain.ErrInsufficientStock) {
					t.Errorf("reserve r-%d: %v", i, err)
				}
				return
			}
			successes.Add(1)
			if res.Reservation.Lines[0].Quantity != qty {
				t.Errorf("reservation r-%d holds %d units, asked for %d", i, res.Reservation.Lines[0].Quantity, qty)
			}
			units.Add(qty)
		}(i)
	}

	close(start)
	wg.Wait()

	items, err := store.GetStock(ctx, []string{"p-1"})
	if err != nil {
		t.Fatalf("GetStock: %v", err)
	}
	if items[0].Reserved > stock {
		t.Fatalf("oversold: reserved %d of %d", items[0].Reserved, stock)
	}
	if items[0].Reserved != units.Load() {
		t.Fatalf("store reserved %d, successes accounted for %d", items[0].Reserved, units.Load())
	}
	if successes.Load() == 0 {
		t.Fatal("no reservation succeeded: the store refused everything")
	}
}

// TestConcurrentSameIDReservesHoldsOnce covers the other half of the protocol:
// concurrent retries of the *same* reservation must not double-hold.
func TestConcurrentSameIDReservesHoldsOnce(t *testing.T) {
	store, _ := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 10)

	const goroutines = 50

	var (
		firsts  atomic.Int64
		replays atomic.Int64
		wg      sync.WaitGroup
		start   = make(chan struct{})
	)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			res, err := store.Reserve(ctx, domain.ReserveRequest{
				ReservationID: "order-1",
				Lines:         []domain.ReservationLine{{ProductID: "p-1", Quantity: 4}},
			})
			if err != nil {
				t.Errorf("reserve: %v", err)
				return
			}
			if res.AlreadyExisted {
				replays.Add(1)
			} else {
				firsts.Add(1)
			}
		}()
	}

	close(start)
	wg.Wait()

	if got := firsts.Load(); got != 1 {
		t.Errorf("first-time reserves = %d, want exactly 1", got)
	}
	if got := replays.Load(); got != goroutines-1 {
		t.Errorf("replays = %d, want %d", got, goroutines-1)
	}
	if got := reservedOf(t, store, "p-1"); got != 4 {
		t.Fatalf("p-1 reserved = %d, want 4", got)
	}
}

// ---------------------------------------------------------------- commit

func TestCommitIsIdempotentAndDeductsOnce(t *testing.T) {
	store, _ := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 10)
	mustReserve(t, store, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 4})

	res, err := store.Commit(ctx, "r-1")
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if res.Status != domain.ReservationCommitted {
		t.Fatalf("status = %q, want committed", res.Status)
	}

	items, _ := store.GetStock(ctx, []string{"p-1"})
	if items[0].Quantity != 6 || items[0].Reserved != 0 {
		t.Fatalf("stock = %+v, want 6 on hand and nothing held", items[0])
	}

	// The saga's commit step is retried by the recovery worker. A second call
	// is a no-op success, and above all must not deduct a second time.
	if _, err := store.Commit(ctx, "r-1"); err != nil {
		t.Fatalf("repeated Commit: %v", err)
	}
	items, _ = store.GetStock(ctx, []string{"p-1"})
	if items[0].Quantity != 6 {
		t.Fatalf("Quantity = %d after a repeated commit, want 6", items[0].Quantity)
	}
}

func TestCommitOfAnUnknownReservation(t *testing.T) {
	store, _ := newStore(t, 0)

	if _, err := store.Commit(context.Background(), "nope"); !errors.Is(err, domain.ErrReservationNotFound) {
		t.Fatalf("error = %v, want ErrReservationNotFound", err)
	}
}

func TestCommitAfterReleaseIsRefusedAndLosesNoStock(t *testing.T) {
	store, _ := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 10)
	mustReserve(t, store, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 4})

	if _, err := store.Release(ctx, "r-1", "checkout_failed"); err != nil {
		t.Fatalf("Release: %v", err)
	}

	// The units are back in the pool and may already belong to another order;
	// committing them would sell stock twice.
	if _, err := store.Commit(ctx, "r-1"); !errors.Is(err, domain.ErrInvalidRequest) {
		t.Fatalf("error = %v, want ErrInvalidRequest", err)
	}

	items, _ := store.GetStock(ctx, []string{"p-1"})
	if items[0].Quantity != 10 || items[0].Reserved != 0 {
		t.Fatalf("stock = %+v, want the released hold returned and nothing deducted", items[0])
	}
}

func TestCommitAfterExpiryIsRefusedAndRestoresTheStock(t *testing.T) {
	store, clock := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 10)
	mustReserve(t, store, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 4})

	// The worker may not have run yet; the deadline is what decides, not the
	// timer. Committing a lapsed hold would be exactly the oversell this
	// service exists to prevent.
	clock.Advance(domain.DefaultReservationTTL + time.Second)

	if _, err := store.Commit(ctx, "r-1"); !errors.Is(err, domain.ErrInvalidRequest) {
		t.Fatalf("error = %v, want ErrInvalidRequest", err)
	}

	items, _ := store.GetStock(ctx, []string{"p-1"})
	if items[0].Reserved != 0 {
		t.Fatalf("reserved = %d, want 0: touching an overdue hold must return its units", items[0].Reserved)
	}
	if items[0].Quantity != 10 {
		t.Fatalf("quantity = %d, want 10: a refused commit deducted stock", items[0].Quantity)
	}

	res, err := store.GetReservation(ctx, "r-1")
	if err != nil {
		t.Fatalf("GetReservation: %v", err)
	}
	if res.Status != domain.ReservationExpired {
		t.Fatalf("status = %q, want expired", res.Status)
	}
}

// --------------------------------------------------------------- release

func TestReleaseIsIdempotentAndToleratesTheUnknown(t *testing.T) {
	store, _ := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 10)

	// A compensation runs precisely when the caller does not know whether the
	// forward step applied. An unknown id is therefore a success with nothing
	// to do — refusing it would strand the saga on the path that most needs to
	// work.
	unknown, err := store.Release(ctx, "never-existed", "checkout_failed")
	if err != nil {
		t.Fatalf("Release of an unknown reservation: %v", err)
	}
	if unknown.Released || unknown.Reservation != nil {
		t.Fatalf("result = %+v, want nothing released and no reservation", unknown)
	}

	mustReserve(t, store, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 4})

	first, err := store.Release(ctx, "r-1", "checkout_failed:authorize_payment")
	if err != nil {
		t.Fatalf("Release: %v", err)
	}
	if !first.Released || first.Reservation == nil {
		t.Fatalf("result = %+v, want released with the reservation", first)
	}
	if first.Reservation.Status != domain.ReservationReleased {
		t.Errorf("status = %q, want released", first.Reservation.Status)
	}
	if first.Reservation.ReleaseReason != "checkout_failed:authorize_payment" {
		t.Errorf("reason = %q, want the reason recorded", first.Reservation.ReleaseReason)
	}
	if got := reservedOf(t, store, "p-1"); got != 0 {
		t.Fatalf("reserved = %d, want 0", got)
	}

	second, err := store.Release(ctx, "r-1", "checkout_failed")
	if err != nil {
		t.Fatalf("repeated Release: %v", err)
	}
	if second.Released {
		t.Error("a repeated release reported returning stock again")
	}
	if got := reservedOf(t, store, "p-1"); got != 0 {
		t.Fatalf("reserved = %d, want 0", got)
	}
}

func TestReleaseRefusesACommittedReservation(t *testing.T) {
	store, _ := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 10)
	mustReserve(t, store, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 4})
	if _, err := store.Commit(ctx, "r-1"); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if _, err := store.Release(ctx, "r-1", "checkout_failed"); !errors.Is(err, domain.ErrInvalidRequest) {
		t.Fatalf("error = %v, want ErrInvalidRequest", err)
	}

	items, _ := store.GetStock(ctx, []string{"p-1"})
	if items[0].Quantity != 6 {
		t.Fatalf("quantity = %d, want 6: a refused release must not invent stock", items[0].Quantity)
	}
}

func TestReleaseOfAnExpiredHoldSucceedsWithoutReturningStockTwice(t *testing.T) {
	store, clock := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 10)
	mustReserve(t, store, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 4})

	clock.Advance(domain.DefaultReservationTTL + time.Second)

	result, err := store.Release(ctx, "r-1", "checkout_failed")
	if err != nil {
		t.Fatalf("Release: %v", err)
	}
	if result.Released {
		t.Error("reported returning stock that expiry had already returned")
	}
	if got := reservedOf(t, store, "p-1"); got != 0 {
		t.Fatalf("reserved = %d, want 0, and not less than 0", got)
	}
}

// ---------------------------------------------------------------- expiry

func TestExpireReservationsReturnsOnlyTheOverdueActiveOnes(t *testing.T) {
	store, clock := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 30)

	mustReserve(t, store, "r-short", domain.ReservationLine{ProductID: "p-1", Quantity: 5})
	mustReserve(t, store, "r-committed", domain.ReservationLine{ProductID: "p-1", Quantity: 7})
	mustReserve(t, store, "r-released", domain.ReservationLine{ProductID: "p-1", Quantity: 2})

	// One hold outlives the deadline the others share. A TTL is fixed when the
	// hold is taken, so this has to be its own request rather than an extension
	// of an existing one.
	if _, err := store.Reserve(ctx, domain.ReserveRequest{
		ReservationID: "r-long",
		TTL:           time.Hour,
		Lines:         []domain.ReservationLine{{ProductID: "p-1", Quantity: 6}},
	}); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if _, err := store.Commit(ctx, "r-committed"); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if _, err := store.Release(ctx, "r-released", "checkout_failed"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := store.Reserve(ctx, domain.ReserveRequest{
		ReservationID: "r-short-2",
		TTL:           time.Minute,
		Lines:         []domain.ReservationLine{{ProductID: "p-1", Quantity: 1}},
	}); err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	clock.Advance(domain.DefaultReservationTTL + time.Second)

	expired, err := store.ExpireReservations(ctx, clock.Now())
	if err != nil {
		t.Fatalf("ExpireReservations: %v", err)
	}

	got := make([]string, 0, len(expired))
	for _, res := range expired {
		got = append(got, res.ID)
		if res.Status != domain.ReservationExpired {
			t.Errorf("%s status = %q, want expired", res.ID, res.Status)
		}
	}
	// Sorted, because the sweep visits reservations in a stable order; the
	// exact slice is asserted so that a sweep which quietly expired r-long, or
	// an already-committed hold, cannot pass.
	if want := []string{"r-short", "r-short-2"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("expired = %v, want %v: a live, committed or released hold must never be expired", got, want)
	}

	// 30 on hand: 7 committed, everything else returned, so only r-long's 6
	// units are still held.
	if got := reservedOf(t, store, "p-1"); got != 6 {
		t.Fatalf("reserved = %d, want 6 (r-long only)", got)
	}

	// A second sweep found nothing, which is what makes the worker's log lines
	// meaningful: they only appear when something actually happened.
	again, err := store.ExpireReservations(ctx, clock.Now())
	if err != nil {
		t.Fatalf("second ExpireReservations: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("second sweep expired %d reservations, want 0", len(again))
	}
}

func TestExpireReservationsKeepsTheIDClaimed(t *testing.T) {
	store, clock := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 10)
	mustReserve(t, store, "order-1", domain.ReservationLine{ProductID: "p-1", Quantity: 4})

	clock.Advance(domain.DefaultReservationTTL + time.Second)
	if _, err := store.ExpireReservations(ctx, clock.Now()); err != nil {
		t.Fatalf("ExpireReservations: %v", err)
	}

	// A late retry of the same reservation must not take a fresh hold under an
	// id the saga already believes it owns: a compensation for the earlier
	// attempt, arriving after the deadline, would otherwise release the stock
	// the new hold depends on.
	result, err := store.Reserve(ctx, domain.ReserveRequest{
		ReservationID: "order-1",
		Lines:         []domain.ReservationLine{{ProductID: "p-1", Quantity: 4}},
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if !result.AlreadyExisted {
		t.Error("a reserve after expiry created a new hold under the same id")
	}
	if result.Reservation.Status != domain.ReservationExpired {
		t.Errorf("status = %q, want the expired hold", result.Reservation.Status)
	}
	if got := reservedOf(t, store, "p-1"); got != 0 {
		t.Fatalf("reserved = %d, want 0", got)
	}
}

func TestGetReservationIsARead(t *testing.T) {
	store, clock := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 10)
	mustReserve(t, store, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 4})

	clock.Advance(domain.DefaultReservationTTL + time.Second)

	res, err := store.GetReservation(ctx, "r-1")
	if err != nil {
		t.Fatalf("GetReservation: %v", err)
	}
	// A read reports what is recorded, not what it would like to be true;
	// callers see the deadline and can act on it. Expiring here would make GET
	// answer differently depending on which writer happened to run first.
	if res.Status != domain.ReservationActive {
		t.Errorf("status = %q, want the recorded active status", res.Status)
	}
	if !res.IsExpired(clock.Now()) {
		t.Error("the reservation should report itself as overdue")
	}
}

func TestGetReservationUnknown(t *testing.T) {
	store, _ := newStore(t, 0)

	if _, err := store.GetReservation(context.Background(), "nope"); !errors.Is(err, domain.ErrReservationNotFound) {
		t.Fatalf("error = %v, want ErrReservationNotFound", err)
	}
}

func TestReturnedReservationsAreCopies(t *testing.T) {
	store, _ := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 10)

	created := mustReserve(t, store, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 4})

	// A caller mutating what it was handed must not be able to reach into the
	// store's state, which is what would happen if the stored pointer escaped
	// the lock.
	created.Reservation.Lines[0].Quantity = 9

	stored, err := store.GetReservation(ctx, "r-1")
	if err != nil {
		t.Fatalf("GetReservation: %v", err)
	}
	if stored.Lines[0].Quantity != 4 {
		t.Fatalf("stored lines = %+v, want the original hold", stored.Lines)
	}
}
