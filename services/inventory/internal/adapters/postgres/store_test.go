package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/marketplace/pkg/postgres"

	"github.com/marketplace/services/inventory/internal/adapters/memory"
	pgadapter "github.com/marketplace/services/inventory/internal/adapters/postgres"
	"github.com/marketplace/services/inventory/internal/domain"
	"github.com/marketplace/services/inventory/migrations"
)

// These tests drive the adapter against a real PostgreSQL, and they matter more
// here than in any other service.
//
// A stock ledger's failure mode is not an error message, it is a unit sold twice,
// and the properties that prevent it — row locks that serialise two concurrent
// reservations, a transaction that holds none of a request's lines when one of
// them is short, an expiry that returns units exactly once — only exist in the
// presence of a real database. A fake that implements FOR UPDATE by doing
// nothing would pass every single-threaded test in this file and prove nothing.
//
// The store is compared against the in-memory adapter, which is the
// specification: same answers, same errors, same state transitions.
//
// The suite runs against its own database — inventory_test by default — so it can
// truncate freely. It skips itself when no server is reachable, so a machine
// without one still gets a green build.

const defaultTestDSN = "postgres://marketplace:marketplace@localhost:5432/inventory_test?sslmode=disable"

func testDSN() string {
	if dsn := os.Getenv("INVENTORY_TEST_DB_DSN"); dsn != "" {
		return dsn
	}
	return defaultTestDSN
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

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

// newStore returns a store over an empty ledger with a controllable clock, which
// is what every test below needs in order to talk about TTLs meaningfully.
func newStore(t *testing.T, defaultTTL time.Duration) (*pgadapter.Store, *fakeClock, *pgxpool.Pool) {
	t.Helper()

	clock := newFakeClock()
	ctx := context.Background()

	pool, err := postgres.Open(ctx, postgres.Config{
		DSN:            testDSN(),
		Service:        "inventory",
		MaxConns:       32,
		ConnectTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Skipf("no PostgreSQL available for the inventory adapter tests: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := postgres.Migrate(ctx, pool, "inventory", migrations.FS, discardLogger()); err != nil {
		// The server is reachable, so this is a broken migration rather than a
		// missing server, and it must not be skipped.
		t.Fatalf("apply migrations: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE reservations, reservation_lines, stock_items`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	return pgadapter.NewWithOptions(pool, pgadapter.Options{Clock: clock.Now, DefaultTTL: defaultTTL}), clock, pool
}

// mustSetStock fails the test if the write is rejected.
func mustSetStock(t *testing.T, store *pgadapter.Store, productID string, quantity int64) domain.StockItem {
	t.Helper()
	item, err := store.SetStock(context.Background(), productID, quantity)
	if err != nil {
		t.Fatalf("SetStock(%s, %d): %v", productID, quantity, err)
	}
	return item
}

// mustReserve fails the test if the reservation is rejected.
func mustReserve(t *testing.T, store *pgadapter.Store, id string, lines ...domain.ReservationLine) domain.ReserveResult {
	t.Helper()
	res, err := store.Reserve(context.Background(), domain.ReserveRequest{ReservationID: id, OrderID: "order-" + id, Lines: lines})
	if err != nil {
		t.Fatalf("Reserve(%s): %v", id, err)
	}
	return res
}

// reservedOf reports how many units of a product are currently held.
func reservedOf(t *testing.T, store *pgadapter.Store, productID string) int64 {
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

// assertLedgerIsConsistent checks the invariant that ties the two tables
// together: the units held in stock_items.reserved are exactly the units named by
// the lines of the reservations that are still active.
//
// Every path that raises reserved creates an active reservation in the same
// transaction, and every path that lowers it makes the reservation terminal. So
// the two totals must agree, and a disagreement is precisely the bug this service
// exists to prevent — a unit returned twice (reserved lower than the holds), or a
// hold recorded without its units ever being reserved (the reverse).
func assertLedgerIsConsistent(t *testing.T, pool *pgxpool.Pool, productID string) {
	t.Helper()
	ctx := context.Background()

	// Both totals come from one statement, so they come from one snapshot. Two
	// queries would be read at two moments, and a reservation committing between
	// them would look exactly like the corruption this is looking for.
	var (
		held     int64
		reserved int64
	)
	if err := pool.QueryRow(ctx, `
		SELECT
			(SELECT coalesce(sum(l.quantity), 0)
			   FROM reservation_lines l
			   JOIN reservations r ON r.id = l.reservation_id
			  WHERE r.status = 'active' AND l.product_id = $1),
			(SELECT reserved FROM stock_items WHERE product_id = $1)`,
		productID).Scan(&held, &reserved); err != nil {
		t.Errorf("read the ledger for %s: %v", productID, err)
		return
	}

	// Errorf and not Fatalf: this runs from the goroutines that watch a race as
	// well as from the test itself, and only the test goroutine may stop a test.
	if held != reserved {
		t.Errorf("active holds name %d units but the stock row for %s reserved %d: the ledger disagrees with itself",
			held, productID, reserved)
	}
}

// ------------------------------------------------------------------- stock

func TestSetStockAndGetStock(t *testing.T) {
	store, _, _ := newStore(t, 0)
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
	store, _, _ := newStore(t, 0)
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
}

func TestGetStockWithoutFilterReturnsEverythingInAStableOrder(t *testing.T) {
	store, _, _ := newStore(t, 0)
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
			t.Fatalf("items = %+v, want %v", items, want)
		}
	}
}

func TestGetStockCollapsesRepeatedIDs(t *testing.T) {
	store, _, _ := newStore(t, 0)
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
	store, _, _ := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 10)

	// The rule lives in the domain, so a rejected write never reaches the
	// database at all.
	if _, err := store.SetStock(ctx, "p-1", -1); !errors.Is(err, domain.ErrInvalidRequest) {
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
	store, _, _ := newStore(t, 0)
	mustSetStock(t, store, "p-1", 10)
	mustReserve(t, store, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 4})

	// An admin re-counts the shelf. The order in flight is not cancelled by it.
	item := mustSetStock(t, store, "p-1", 4)
	if item.Quantity != 4 || item.Reserved != 4 || item.Available() != 0 {
		t.Fatalf("item = %+v, want the hold intact on a corrected shelf", item)
	}
}

// TestSetStockBelowTheHeldAmountIsAllowedAndFloorsAvailability covers the reason
// there is no `reserved <= quantity` constraint on the table.
//
// A miscounted shelf is a real event, and the holds already granted are promises
// to customers who are mid-checkout rather than cancellations. The row is
// therefore allowed to say "4 held, 2 on hand", and every caller that asks for
// availability is given zero rather than a negative number.
func TestSetStockBelowTheHeldAmountIsAllowedAndFloorsAvailability(t *testing.T) {
	store, _, pool := newStore(t, 0)
	ctx := context.Background()

	mustSetStock(t, store, "p-1", 10)
	mustReserve(t, store, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 6})

	item := mustSetStock(t, store, "p-1", 2)
	if item.Quantity != 2 || item.Reserved != 6 {
		t.Fatalf("item = %+v, want the corrected count and the outstanding hold", item)
	}
	if item.Available() != 0 {
		t.Fatalf("available = %d, want 0: availability is never negative", item.Available())
	}

	// Nothing more can be held, and the refusal is the ordinary sold-out answer
	// rather than an error about the state of the row.
	_, err := store.Reserve(ctx, domain.ReserveRequest{
		ReservationID: "r-2",
		Lines:         []domain.ReservationLine{{ProductID: "p-1", Quantity: 1}},
	})
	if !errors.Is(err, domain.ErrInsufficientStock) {
		t.Fatalf("error = %v, want ErrInsufficientStock", err)
	}

	assertLedgerIsConsistent(t, pool, "p-1")
}

// ---------------------------------------------------------------- reserve

func TestReserveHoldsStock(t *testing.T) {
	store, _, pool := newStore(t, 0)
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
	// The lines come back in the order they were asked for, which is what a
	// caller that prints them expects.
	if len(result.Reservation.Lines) != 2 ||
		result.Reservation.Lines[0].ProductID != "p-1" ||
		result.Reservation.Lines[1].ProductID != "p-2" {
		t.Errorf("lines = %+v, want the requested order", result.Reservation.Lines)
	}
	if got := reservedOf(t, store, "p-1"); got != 4 {
		t.Errorf("p-1 reserved = %d, want 4", got)
	}
	if got := reservedOf(t, store, "p-2"); got != 5 {
		t.Errorf("p-2 reserved = %d, want 5", got)
	}

	assertLedgerIsConsistent(t, pool, "p-1")
	assertLedgerIsConsistent(t, pool, "p-2")
}

func TestReserveIsAllOrNothing(t *testing.T) {
	store, _, pool := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 10)
	mustSetStock(t, store, "p-2", 5)

	// The failing line is deliberately last. An implementation that held each
	// line as it checked it — or one that committed the holds it had made before
	// discovering the short line — would leave p-2 held below, and the saga would
	// then compensate a reservation it believes failed.
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

	assertLedgerIsConsistent(t, pool, "p-1")
	assertLedgerIsConsistent(t, pool, "p-2")
}

func TestReserveFailsOnAnUnknownProduct(t *testing.T) {
	store, _, _ := newStore(t, 0)
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
	store, _, _ := newStore(t, 0)
	mustSetStock(t, store, "p-1", 10)

	first := mustReserve(t, store, "order-1", domain.ReservationLine{ProductID: "p-1", Quantity: 4})

	// This is the saga's retry: same deterministic id, same lines, and the answer
	// must be the original hold rather than a second one. A second hold would
	// silently halve the catalogue for every other customer.
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

	// A caller that reused an id with different lines is buggy, but the id is the
	// contract: failing here would break the retry safety the whole protocol
	// rests on, so the original reservation is returned instead.
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
	store, _, _ := newStore(t, 0)
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
	store, clock, _ := newStore(t, 2*time.Minute)
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
	store, _, _ := newStore(t, 0)
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
// reservation id, so nothing but the store's own locking can stop them from all
// succeeding. The assertions are deliberately exact rather than approximate: "no
// more than the stock was held" is the safety property, and "the stock was fully
// used" is the liveness property. A store that simply refused most requests
// would pass the first and fail the second — and a store that read availability
// without locking would pass both here and fail under load.
func TestConcurrentReserveNeverOversells(t *testing.T) {
	const (
		stock          = 100
		perReservation = 3
		goroutines     = 150
	)

	store, _, pool := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", stock)

	var (
		succeeded atomic.Int64
		units     atomic.Int64
		unwanted  atomic.Int64
		wg        sync.WaitGroup
		start     = make(chan struct{})
	)

	// A watcher samples the store while the race is running. It is what turns
	// "the final total is right" into "the invariant held at every moment": a
	// store that briefly held 103 units and corrected itself afterwards would
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
				// A deadlock or a serialisation failure would land here. The
				// row locks are taken in a fixed order precisely so that this
				// cannot happen, and a failure the saga would have to retry is
				// worth failing the test over.
				unwanted.Add(1)
				t.Errorf("reservation r-%d failed unexpectedly: %v", i, err)
			}
		}(i)
	}

	close(start)
	wg.Wait()
	close(stopWatcher)
	<-watcherStopped

	if got := unwanted.Load(); got != 0 {
		t.Errorf("%d reservations failed with something other than ErrInsufficientStock", got)
	}

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

	assertLedgerIsConsistent(t, pool, "p-1")
}

// TestConcurrentReserveWithVaryingQuantities checks the same invariant when the
// requests are not uniform, so no single arithmetic shortcut can satisfy it.
func TestConcurrentReserveWithVaryingQuantities(t *testing.T) {
	const (
		stock      = 250
		goroutines = 200
	)

	store, _, pool := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", stock)

	var (
		units     atomic.Int64
		successes atomic.Int64
		wg        sync.WaitGroup
		start     = make(chan struct{})
	)

	for i := 0; i < goroutines; i++ {
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

	assertLedgerIsConsistent(t, pool, "p-1")
}

// TestConcurrentSameIDReservesHoldsOnce covers the other half of the protocol:
// concurrent retries of the *same* reservation must not double-hold.
//
// This is the case the primary key on the reservation id exists for. Every
// goroutine takes the path that reads "no such reservation", so all but one go on
// to attempt the insert, and the database — not a preceding SELECT — decides
// which one wins.
func TestConcurrentSameIDReservesHoldsOnce(t *testing.T) {
	store, _, pool := newStore(t, 0)
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

	assertLedgerIsConsistent(t, pool, "p-1")
}

// ---------------------------------------------------------------- commit

func TestCommitIsIdempotentAndDeductsOnce(t *testing.T) {
	store, _, pool := newStore(t, 0)
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

	// The saga's commit step is retried by the recovery worker. A second call is
	// a no-op success, and above all must not deduct a second time.
	if _, err := store.Commit(ctx, "r-1"); err != nil {
		t.Fatalf("repeated Commit: %v", err)
	}
	items, _ = store.GetStock(ctx, []string{"p-1"})
	if items[0].Quantity != 6 {
		t.Fatalf("Quantity = %d after a repeated commit, want 6", items[0].Quantity)
	}

	assertLedgerIsConsistent(t, pool, "p-1")
}

func TestCommitOfAnUnknownReservation(t *testing.T) {
	store, _, _ := newStore(t, 0)

	if _, err := store.Commit(context.Background(), "nope"); !errors.Is(err, domain.ErrReservationNotFound) {
		t.Fatalf("error = %v, want ErrReservationNotFound", err)
	}
}

func TestCommitAfterReleaseIsRefusedAndLosesNoStock(t *testing.T) {
	store, _, pool := newStore(t, 0)
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

	assertLedgerIsConsistent(t, pool, "p-1")
}

// TestCommitAfterExpiryIsRefusedAndRestoresTheStock covers the one refusal that
// also performs a write.
//
// The worker may not have run yet; the deadline is what decides, not the timer.
// Committing a lapsed hold would be exactly the oversell this service exists to
// prevent, so the commit is refused — and because the refusal happens after the
// hold was expired, the expiry has to survive the transaction that reports the
// error. If it were rolled back, the row would still say "active" and a later
// retry would expire it a second time and return the same units twice.
func TestCommitAfterExpiryIsRefusedAndRestoresTheStock(t *testing.T) {
	store, clock, pool := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 10)
	mustReserve(t, store, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 4})

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
		t.Fatalf("status = %q, want expired: the expiry must be durable even though the commit was refused", res.Status)
	}

	// A second refusal must not return the units again.
	if _, err := store.Commit(ctx, "r-1"); !errors.Is(err, domain.ErrInvalidRequest) {
		t.Fatalf("error = %v, want ErrInvalidRequest", err)
	}
	if got := reservedOf(t, store, "p-1"); got != 0 {
		t.Fatalf("reserved = %d after a second refused commit, want 0", got)
	}

	assertLedgerIsConsistent(t, pool, "p-1")
}

// --------------------------------------------------------------- release

func TestReleaseIsIdempotentAndToleratesTheUnknown(t *testing.T) {
	store, _, pool := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 10)

	// A compensation runs precisely when the caller does not know whether the
	// forward step applied. An unknown id is therefore a success with nothing to
	// do — refusing it would strand the saga on the path that most needs to work.
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

	assertLedgerIsConsistent(t, pool, "p-1")
}

func TestReleaseRefusesACommittedReservation(t *testing.T) {
	store, _, pool := newStore(t, 0)
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

	assertLedgerIsConsistent(t, pool, "p-1")
}

func TestReleaseOfAnExpiredHoldSucceedsWithoutReturningStockTwice(t *testing.T) {
	store, clock, pool := newStore(t, 0)
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

	assertLedgerIsConsistent(t, pool, "p-1")
}

// ---------------------------------------------------------------- expiry

func TestExpireReservationsReturnsOnlyTheOverdueActiveOnes(t *testing.T) {
	store, clock, pool := newStore(t, 0)
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
		// The units are needed to sum what a sweep gave back in its log line, so
		// the lines have to come back with the reservation.
		if len(res.Lines) == 0 {
			t.Errorf("%s came back without its lines", res.ID)
		}
	}
	// Sorted by deadline, so the exact slice is asserted and a sweep that quietly
	// expired r-long, or an already-committed hold, cannot pass.
	if want := []string{"r-short", "r-short-2"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("expired = %v, want %v: a live, committed or released hold must never be expired", got, want)
	}

	// 30 on hand: 7 committed, everything else returned, so only r-long's 6 units
	// are still held.
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

	assertLedgerIsConsistent(t, pool, "p-1")
}

func TestExpireReservationsKeepsTheIDClaimed(t *testing.T) {
	store, clock, _ := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 10)
	mustReserve(t, store, "order-1", domain.ReservationLine{ProductID: "p-1", Quantity: 4})

	clock.Advance(domain.DefaultReservationTTL + time.Second)
	if _, err := store.ExpireReservations(ctx, clock.Now()); err != nil {
		t.Fatalf("ExpireReservations: %v", err)
	}

	// A late retry of the same reservation must not take a fresh hold under an id
	// the saga already believes it owns: a compensation for the earlier attempt,
	// arriving after the deadline, would otherwise release the stock the new hold
	// depends on.
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

// TestConcurrentExpireReservationsReturnsEachHoldOnce is the test for the expiry
// statement's whole reason for being one statement.
//
// Four sweeps race over the same set of overdue holds. Exactly one of them may
// return each hold's units: a hold returned twice is stock invented out of
// nothing, and the second return is invisible in the final total because the
// floor at zero hides it. Counting the ids the sweeps report, and checking that
// the ledger still agrees with itself, is what makes a double-return visible.
func TestConcurrentExpireReservationsReturnsEachHoldOnce(t *testing.T) {
	const (
		holds          = 12
		unitsPerHold   = 2
		concurrentRuns = 4
	)

	store, clock, pool := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", holds*unitsPerHold+10)

	for i := 0; i < holds; i++ {
		mustReserve(t, store, fmt.Sprintf("r-%02d", i), domain.ReservationLine{ProductID: "p-1", Quantity: unitsPerHold})
	}

	clock.Advance(domain.DefaultReservationTTL + time.Second)
	now := clock.Now()

	var (
		mu      sync.Mutex
		seen    = make(map[string]int, holds)
		wg      sync.WaitGroup
		start   = make(chan struct{})
		failure atomic.Int64
	)

	for i := 0; i < concurrentRuns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			expired, err := store.ExpireReservations(ctx, now)
			if err != nil {
				failure.Add(1)
				t.Errorf("ExpireReservations: %v", err)
				return
			}

			mu.Lock()
			defer mu.Unlock()
			for _, res := range expired {
				seen[res.ID]++
			}
		}()
	}

	close(start)
	wg.Wait()

	if failure.Load() != 0 {
		t.Fatalf("%d concurrent sweeps failed: they must skip a hold another sweep is working on, not error", failure.Load())
	}

	total := 0
	for id, count := range seen {
		total += count
		if count != 1 {
			t.Errorf("hold %s was expired %d times, want exactly once", id, count)
		}
	}
	if total != holds {
		t.Errorf("sweeps reported %d expired holds in total, want %d", total, holds)
	}

	// Every unit is back exactly once: the holds are gone from the active set and
	// the reserved count is zero, not negative.
	if got := reservedOf(t, store, "p-1"); got != 0 {
		t.Fatalf("reserved = %d after every hold expired, want 0", got)
	}

	assertLedgerIsConsistent(t, pool, "p-1")
}

// TestExpireReservationsRunningWithReserveKeepsTheLedgerHonest runs the sweep
// against a live checkout workload.
//
// Holds are taken and expired at the same time, which is the only arrangement in
// which the two can interleave at all. Two things must hold throughout: no
// reservation fails for a reason other than insufficient stock — an expiry must
// never make a reserve error, only find less available — and the units recorded
// as held stay exactly equal to the units named by active holds, so nothing is
// returned twice and nothing is leaked.
func TestExpireReservationsRunningWithReserveKeepsTheLedgerHonest(t *testing.T) {
	const (
		stock      = 500
		goroutines = 60
		units      = 1
	)

	store, clock, pool := newStore(t, time.Millisecond)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", stock)

	stop := make(chan struct{})
	sweeperDone := make(chan struct{})

	// The clock is advanced as the test runs, so holds expire while other
	// goroutines are taking them: with a frozen clock nothing would ever be due
	// and the race would not exist.
	go func() {
		defer close(sweeperDone)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				clock.Advance(time.Millisecond)
				if _, err := store.ExpireReservations(ctx, clock.Now()); err != nil {
					t.Errorf("ExpireReservations: %v", err)
					return
				}
				assertLedgerIsConsistent(t, pool, "p-1")
			}
		}
	}()

	var (
		succeeded atomic.Int64
		wg        sync.WaitGroup
		start     = make(chan struct{})
	)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start

			_, err := store.Reserve(ctx, domain.ReserveRequest{
				ReservationID: fmt.Sprintf("r-%d", i),
				TTL:           time.Millisecond,
				Lines:         []domain.ReservationLine{{ProductID: "p-1", Quantity: units}},
			})
			switch {
			case err == nil:
				succeeded.Add(1)
			case errors.Is(err, domain.ErrInsufficientStock):
				// Possible if the sweep has not yet returned units that expired
				// a moment ago; the store is not obliged to reorder them.
			default:
				t.Errorf("reserve r-%d: %v", i, err)
			}
		}(i)
	}

	close(start)
	wg.Wait()
	close(stop)
	<-sweeperDone

	if succeeded.Load() == 0 {
		t.Fatal("no reservation succeeded")
	}

	// Everything is overdue now, so a final sweep must return the lot: nothing
	// held, nothing deducted, and the ledger still agreeing with itself.
	clock.Advance(time.Hour)
	if _, err := store.ExpireReservations(ctx, clock.Now()); err != nil {
		t.Fatalf("final ExpireReservations: %v", err)
	}

	items, err := store.GetStock(ctx, []string{"p-1"})
	if err != nil {
		t.Fatalf("GetStock: %v", err)
	}
	if items[0].Reserved != 0 {
		t.Fatalf("reserved = %d after every hold expired, want 0", items[0].Reserved)
	}
	if items[0].Quantity != stock {
		t.Fatalf("quantity = %d, want %d: expiring a hold must not move stock", items[0].Quantity, stock)
	}

	assertLedgerIsConsistent(t, pool, "p-1")
}

// ------------------------------------------------------------------ reads

func TestGetReservationIsARead(t *testing.T) {
	store, clock, _ := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 10)
	mustReserve(t, store, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 4})

	clock.Advance(domain.DefaultReservationTTL + time.Second)

	res, err := store.GetReservation(ctx, "r-1")
	if err != nil {
		t.Fatalf("GetReservation: %v", err)
	}
	// A read reports what is recorded, not what it would like to be true; callers
	// see the deadline and can act on it. Expiring here would make GET answer
	// differently depending on which writer happened to run first.
	if res.Status != domain.ReservationActive {
		t.Errorf("status = %q, want the recorded active status", res.Status)
	}
	if !res.IsExpired(clock.Now()) {
		t.Error("the reservation should report itself as overdue")
	}
}

func TestGetReservationUnknown(t *testing.T) {
	store, _, _ := newStore(t, 0)

	if _, err := store.GetReservation(context.Background(), "nope"); !errors.Is(err, domain.ErrReservationNotFound) {
		t.Fatalf("error = %v, want ErrReservationNotFound", err)
	}
}

func TestReturnedReservationsAreCopies(t *testing.T) {
	store, _, _ := newStore(t, 0)
	ctx := context.Background()
	mustSetStock(t, store, "p-1", 10)

	created := mustReserve(t, store, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 4})

	// A caller mutating what it was handed must not be able to reach into the
	// store's state: every read here builds its own values from the rows it
	// scanned, so this is a check that no code path hands back something shared.
	created.Reservation.Lines[0].Quantity = 9

	stored, err := store.GetReservation(ctx, "r-1")
	if err != nil {
		t.Fatalf("GetReservation: %v", err)
	}
	if stored.Lines[0].Quantity != 4 {
		t.Fatalf("stored lines = %+v, want the original hold", stored.Lines)
	}
}

// TestTheAdaptersAgreeOnTheBasics drives both implementations through the same
// sequence and compares what they say.
//
// The in-memory adapter is the specification for the durable one, so the cheapest
// way to keep them from drifting is to make them answer the same questions. It is
// not a substitute for the tests above — it cannot say anything about locking —
// but it does catch a change in one that was not made in the other.
func TestTheAdaptersAgreeOnTheBasics(t *testing.T) {
	ctx := context.Background()
	durable, clock, _ := newStore(t, time.Minute)
	inMemory := memory.New(memory.Options{Clock: clock.Now, DefaultTTL: time.Minute})

	steps := []struct {
		name string
		run  func(t *testing.T, store domain.Repository)
	}{
		{"set stock", func(t *testing.T, store domain.Repository) {
			item, err := store.SetStock(ctx, "p-1", 10)
			if err != nil {
				t.Fatalf("SetStock: %v", err)
			}
			if item.Quantity != 10 || item.Reserved != 0 {
				t.Fatalf("item = %+v", item)
			}
		}},
		{"reserve", func(t *testing.T, store domain.Repository) {
			res, err := store.Reserve(ctx, domain.ReserveRequest{
				ReservationID: "r-1",
				Lines:         []domain.ReservationLine{{ProductID: "p-1", Quantity: 4}},
			})
			if err != nil {
				t.Fatalf("Reserve: %v", err)
			}
			if res.AlreadyExisted {
				t.Error("a first reserve reported a replay")
			}
		}},
		{"replay", func(t *testing.T, store domain.Repository) {
			res, err := store.Reserve(ctx, domain.ReserveRequest{
				ReservationID: "r-1",
				Lines:         []domain.ReservationLine{{ProductID: "p-1", Quantity: 4}},
			})
			if err != nil {
				t.Fatalf("Reserve: %v", err)
			}
			if !res.AlreadyExisted {
				t.Error("a replay was reported as a first reserve")
			}
		}},
		{"oversell is refused", func(t *testing.T, store domain.Repository) {
			_, err := store.Reserve(ctx, domain.ReserveRequest{
				ReservationID: "r-2",
				Lines:         []domain.ReservationLine{{ProductID: "p-1", Quantity: 7}},
			})
			if !errors.Is(err, domain.ErrInsufficientStock) {
				t.Fatalf("error = %v, want ErrInsufficientStock", err)
			}
		}},
		{"unknown product is refused", func(t *testing.T, store domain.Repository) {
			_, err := store.Reserve(ctx, domain.ReserveRequest{
				ReservationID: "r-3",
				Lines:         []domain.ReservationLine{{ProductID: "nope", Quantity: 1}},
			})
			if !errors.Is(err, domain.ErrStockNotFound) {
				t.Fatalf("error = %v, want ErrStockNotFound", err)
			}
		}},
		{"release", func(t *testing.T, store domain.Repository) {
			result, err := store.Release(ctx, "r-1", "checkout_failed")
			if err != nil {
				t.Fatalf("Release: %v", err)
			}
			if !result.Released || result.Reservation == nil || result.Reservation.ReleaseReason != "checkout_failed" {
				t.Fatalf("result = %+v", result)
			}
		}},
		{"release again", func(t *testing.T, store domain.Repository) {
			result, err := store.Release(ctx, "r-1", "checkout_failed")
			if err != nil {
				t.Fatalf("Release: %v", err)
			}
			if result.Released {
				t.Error("a repeated release reported returning stock")
			}
		}},
		{"unknown release succeeds", func(t *testing.T, store domain.Repository) {
			result, err := store.Release(ctx, "never-existed", "checkout_failed")
			if err != nil {
				t.Fatalf("Release: %v", err)
			}
			if result.Released || result.Reservation != nil {
				t.Fatalf("result = %+v, want nothing to undo", result)
			}
		}},
		{"stock level", func(t *testing.T, store domain.Repository) {
			items, err := store.GetStock(ctx, []string{"p-1"})
			if err != nil {
				t.Fatalf("GetStock: %v", err)
			}
			if len(items) != 1 || items[0].Quantity != 10 || items[0].Reserved != 0 {
				t.Fatalf("items = %+v, want 10 on hand and nothing held", items)
			}
		}},
	}

	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			step.run(t, durable)
			step.run(t, inMemory)
		})
	}
}
