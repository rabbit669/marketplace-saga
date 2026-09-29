package grpc_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/marketplace/pkg/apperr"
	"github.com/marketplace/pkg/grpcx"
	inventoryv1 "github.com/marketplace/pkg/proto/inventory/v1"

	inventorygrpc "github.com/marketplace/services/inventory/internal/adapters/grpc"
	"github.com/marketplace/services/inventory/internal/adapters/memory"
	"github.com/marketplace/services/inventory/internal/domain"
)

// These tests go over a real connection rather than calling the adapter's
// methods directly, because most of what can go wrong at this boundary only
// exists on the wire: a status code the caller branches on, a field the caller
// reads, a nil message the caller dereferences. bufconn gives the real client
// and server stacks — including grpcx's interceptor chain, which is what turns
// a domain error into a status — without opening a port that a concurrently
// running service might already be using.

// testDefaultTTL is the service default applied when a request omits
// ttl_seconds.
const testDefaultTTL = 90 * time.Second

// testClock keeps expiry deterministic: an RPC test that sleeps to cross a
// deadline is a test that fails on a loaded CI box.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type harness struct {
	client inventoryv1.InventoryServiceClient
	store  *memory.Store
	clock  *testClock
}

// newHarness starts the real adapter on a bufconn listener and returns a
// connected client.
func newHarness(t *testing.T, repo domain.Repository, defaultTTL time.Duration) *harness {
	t.Helper()

	// The interception chain logs every call; discarding it keeps a failing
	// test's output readable.
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	adapter, err := inventorygrpc.NewServer(inventorygrpc.Config{
		Repository: repo,
		Logger:     log,
		DefaultTTL: defaultTTL,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	srv := grpcx.NewServer(grpcx.ServerConfig{Name: "inventory", Logger: log})
	adapter.Register(srv)

	lis := bufconn.Listen(1 << 20)
	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Logf("serve: %v", err)
		}
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	})

	return &harness{client: inventoryv1.NewInventoryServiceClient(conn)}
}

// newStoreHarness is the common case: the real adapter over the real store.
func newStoreHarness(t *testing.T) *harness {
	t.Helper()

	clock := newTestClock()
	store := memory.New(memory.Options{Clock: clock.Now, DefaultTTL: testDefaultTTL})

	h := newHarness(t, store, testDefaultTTL)
	h.store = store
	h.clock = clock
	return h
}

func (h *harness) setStock(t *testing.T, productID string, quantity int64) {
	t.Helper()
	if _, err := h.client.SetStock(context.Background(), &inventoryv1.SetStockRequest{
		ProductId: productID,
		Quantity:  quantity,
	}); err != nil {
		t.Fatalf("SetStock(%s, %d): %v", productID, quantity, err)
	}
}

func (h *harness) stockOf(t *testing.T, productID string) *inventoryv1.StockItem {
	t.Helper()
	resp, err := h.client.GetStock(context.Background(), &inventoryv1.GetStockRequest{
		ProductIds: []string{productID},
	})
	if err != nil {
		t.Fatalf("GetStock(%s): %v", productID, err)
	}
	if len(resp.GetItems()) != 1 {
		t.Fatalf("GetStock(%s) returned %d items, want 1", productID, len(resp.GetItems()))
	}
	return resp.GetItems()[0]
}

func lines(pairs ...any) []*inventoryv1.ReservationLine {
	out := make([]*inventoryv1.ReservationLine, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, &inventoryv1.ReservationLine{
			ProductId: pairs[i].(string),
			Quantity:  int64(pairs[i+1].(int)),
		})
	}
	return out
}

// ------------------------------------------------------------------- stock

func TestGetStockOmitsProductsThatWereNeverStocked(t *testing.T) {
	h := newStoreHarness(t)
	h.setStock(t, "p-1", 100)
	h.setStock(t, "p-2", 250)

	resp, err := h.client.GetStock(context.Background(), &inventoryv1.GetStockRequest{
		ProductIds: []string{"p-1", "p-2"},
	})
	if err != nil {
		t.Fatalf("GetStock: %v", err)
	}
	if len(resp.GetItems()) != 2 {
		t.Fatalf("items = %+v, want both products", resp.GetItems())
	}
	if item := resp.GetItems()[0]; item.GetQuantity() != 100 || item.GetReserved() != 0 || item.GetAvailable() != 100 {
		t.Fatalf("p-1 = %+v, want 100 free", item)
	}
	if resp.GetItems()[0].GetUpdatedAt() == nil {
		t.Error("updated_at is missing")
	}

	// The proto's contract: a product this service has never stocked is absent
	// from the response. Returning it as zero would make "unknown" look like
	// "sold out" to the operator reading the screen.
	partial, err := h.client.GetStock(context.Background(), &inventoryv1.GetStockRequest{
		ProductIds: []string{"p-1", "p-never-stocked"},
	})
	if err != nil {
		t.Fatalf("GetStock: %v", err)
	}
	if len(partial.GetItems()) != 1 || partial.GetItems()[0].GetProductId() != "p-1" {
		t.Fatalf("items = %+v, want only p-1", partial.GetItems())
	}
}

func TestGetStockReportsAvailableConsistentlyWithReserved(t *testing.T) {
	h := newStoreHarness(t)
	h.setStock(t, "p-1", 10)

	if _, err := h.client.ReserveStock(context.Background(), &inventoryv1.ReserveStockRequest{
		ReservationId: "r-1",
		OrderId:       "order-1",
		Lines:         lines("p-1", 4),
	}); err != nil {
		t.Fatalf("ReserveStock: %v", err)
	}

	item := h.stockOf(t, "p-1")
	// available is sent rather than left to the caller to subtract: the clamp
	// at zero is a rule, and a client that recomputed it would reimplement it
	// differently.
	if item.GetQuantity() != 10 || item.GetReserved() != 4 || item.GetAvailable() != 6 {
		t.Fatalf("item = %+v, want 10/4/6", item)
	}
}

func TestGetStockRejectsABlankProductID(t *testing.T) {
	h := newStoreHarness(t)

	_, err := h.client.GetStock(context.Background(), &inventoryv1.GetStockRequest{
		ProductIds: []string{"p-1", ""},
	})
	assertCode(t, err, codes.InvalidArgument)
}

func TestSetStockRejectsNegativeQuantity(t *testing.T) {
	h := newStoreHarness(t)
	h.setStock(t, "p-1", 10)

	_, err := h.client.SetStock(context.Background(), &inventoryv1.SetStockRequest{
		ProductId: "p-1",
		Quantity:  -1,
	})
	assertCode(t, err, codes.InvalidArgument)

	if item := h.stockOf(t, "p-1"); item.GetQuantity() != 10 {
		t.Fatalf("quantity = %d after a rejected write, want 10", item.GetQuantity())
	}
}

// -------------------------------------------------------------- reserving

func TestReserveStockRoundTrip(t *testing.T) {
	h := newStoreHarness(t)
	h.setStock(t, "p-1", 10)
	h.setStock(t, "p-2", 5)

	resp, err := h.client.ReserveStock(context.Background(), &inventoryv1.ReserveStockRequest{
		ReservationId: "order-42",
		OrderId:       "order-42",
		Lines:         lines("p-1", 3, "p-2", 5),
		TtlSeconds:    120,
	})
	if err != nil {
		t.Fatalf("ReserveStock: %v", err)
	}

	res := resp.GetReservation()
	if resp.GetAlreadyExisted() {
		t.Error("a first reserve reported already_existed")
	}
	if res.GetReservationId() != "order-42" || res.GetOrderId() != "order-42" {
		t.Errorf("reservation = %+v, want the caller's ids echoed", res)
	}
	if res.GetStatus() != inventoryv1.ReservationStatus_RESERVATION_STATUS_ACTIVE {
		t.Errorf("status = %v, want ACTIVE", res.GetStatus())
	}
	if len(res.GetLines()) != 2 {
		t.Fatalf("lines = %+v, want both", res.GetLines())
	}
	if got := res.GetExpiresAt().AsTime().Sub(res.GetCreatedAt().AsTime()); got != 120*time.Second {
		t.Errorf("ttl = %s, want 120s", got)
	}

	if item := h.stockOf(t, "p-1"); item.GetReserved() != 3 || item.GetAvailable() != 7 {
		t.Errorf("p-1 = %+v, want 3 held of 10", item)
	}
}

func TestReserveStockUsesTheServerDefaultTTL(t *testing.T) {
	h := newStoreHarness(t)
	h.setStock(t, "p-1", 10)

	resp, err := h.client.ReserveStock(context.Background(), &inventoryv1.ReserveStockRequest{
		ReservationId: "r-1",
		Lines:         lines("p-1", 1),
	})
	if err != nil {
		t.Fatalf("ReserveStock: %v", err)
	}

	// ttl_seconds of zero is documented as "the server default", so the value
	// the server was configured with must be what the caller sees back.
	got := resp.GetReservation().GetExpiresAt().AsTime().Sub(resp.GetReservation().GetCreatedAt().AsTime())
	if got != testDefaultTTL {
		t.Fatalf("ttl = %s, want the configured default %s", got, testDefaultTTL)
	}
}

func TestReserveStockIsIdempotentOverTheWire(t *testing.T) {
	h := newStoreHarness(t)
	h.setStock(t, "p-1", 10)

	first, err := h.client.ReserveStock(context.Background(), &inventoryv1.ReserveStockRequest{
		ReservationId: "order-7",
		OrderId:       "order-7",
		Lines:         lines("p-1", 4),
	})
	if err != nil {
		t.Fatalf("ReserveStock: %v", err)
	}

	// The saga retries a step whose outcome it never learned. The retry must
	// come back with the original hold, flagged as a replay.
	second, err := h.client.ReserveStock(context.Background(), &inventoryv1.ReserveStockRequest{
		ReservationId: "order-7",
		OrderId:       "order-7",
		Lines:         lines("p-1", 4),
	})
	if err != nil {
		t.Fatalf("replayed ReserveStock: %v", err)
	}
	if !second.GetAlreadyExisted() {
		t.Error("the replay was not reported as already existing")
	}
	if second.GetReservation().GetExpiresAt().AsTime() != first.GetReservation().GetExpiresAt().AsTime() {
		t.Error("the replay moved the deadline")
	}
	if item := h.stockOf(t, "p-1"); item.GetReserved() != 4 {
		t.Fatalf("reserved = %d, want 4: the replay held stock twice", item.GetReserved())
	}
}

func TestReserveStockFailuresMapToTheRightStatus(t *testing.T) {
	h := newStoreHarness(t)
	h.setStock(t, "p-1", 3)

	t.Run("insufficient stock is a failed precondition", func(t *testing.T) {
		_, err := h.client.ReserveStock(context.Background(), &inventoryv1.ReserveStockRequest{
			ReservationId: "r-1",
			Lines:         lines("p-1", 5),
		})
		assertCode(t, err, codes.FailedPrecondition)
		// The message names the product and the numbers so the gateway can show
		// something better than "failed precondition".
		if !strings.Contains(status.Convert(err).Message(), "p-1") {
			t.Errorf("message %q should name the product", status.Convert(err).Message())
		}
		if item := h.stockOf(t, "p-1"); item.GetReserved() != 0 {
			t.Fatalf("a failed reserve held %d units", item.GetReserved())
		}
	})

	t.Run("unknown product is not found", func(t *testing.T) {
		_, err := h.client.ReserveStock(context.Background(), &inventoryv1.ReserveStockRequest{
			ReservationId: "r-2",
			Lines:         lines("p-1", 1, "p-ghost", 1),
		})
		assertCode(t, err, codes.NotFound)

		// All-or-nothing: the satisfiable line must not be left held.
		if item := h.stockOf(t, "p-1"); item.GetReserved() != 0 {
			t.Fatalf("reserved = %d, want 0 after a failed multi-line reserve", item.GetReserved())
		}
	})

	invalid := map[string]*inventoryv1.ReserveStockRequest{
		"no reservation id": {Lines: lines("p-1", 1)},
		"no lines":          {ReservationId: "r-3"},
		"negative ttl":      {ReservationId: "r-3", Lines: lines("p-1", 1), TtlSeconds: -1},
		"absurd ttl":        {ReservationId: "r-3", Lines: lines("p-1", 1), TtlSeconds: int64(domain.MaxReservationTTL/time.Second) + 1},
	}
	for name, req := range invalid {
		t.Run(name+" is invalid argument", func(t *testing.T) {
			_, err := h.client.ReserveStock(context.Background(), req)
			assertCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestReserveStockAcceptsTheMaximumTTL(t *testing.T) {
	h := newStoreHarness(t)
	h.setStock(t, "p-1", 10)

	resp, err := h.client.ReserveStock(context.Background(), &inventoryv1.ReserveStockRequest{
		ReservationId: "r-1",
		Lines:         lines("p-1", 1),
		TtlSeconds:    int64(domain.MaxReservationTTL / time.Second),
	})
	if err != nil {
		t.Fatalf("ReserveStock at the TTL limit: %v", err)
	}
	if got := resp.GetReservation().GetExpiresAt().AsTime().Sub(resp.GetReservation().GetCreatedAt().AsTime()); got != domain.MaxReservationTTL {
		t.Fatalf("ttl = %s, want %s", got, domain.MaxReservationTTL)
	}
}

// -------------------------------------------------------------- committing

func TestCommitReservationIsIdempotentOverTheWire(t *testing.T) {
	h := newStoreHarness(t)
	h.setStock(t, "p-1", 10)
	if _, err := h.client.ReserveStock(context.Background(), &inventoryv1.ReserveStockRequest{
		ReservationId: "r-1",
		Lines:         lines("p-1", 4),
	}); err != nil {
		t.Fatalf("ReserveStock: %v", err)
	}

	commit := func() *inventoryv1.CommitReservationResponse {
		t.Helper()
		resp, err := h.client.CommitReservation(context.Background(), &inventoryv1.CommitReservationRequest{ReservationId: "r-1"})
		if err != nil {
			t.Fatalf("CommitReservation: %v", err)
		}
		return resp
	}

	if got := commit().GetReservation().GetStatus(); got != inventoryv1.ReservationStatus_RESERVATION_STATUS_COMMITTED {
		t.Fatalf("status = %v, want COMMITTED", got)
	}
	// The recovery worker retries this step; a retry must not deduct twice.
	if got := commit().GetReservation().GetStatus(); got != inventoryv1.ReservationStatus_RESERVATION_STATUS_COMMITTED {
		t.Fatalf("status after a repeated commit = %v, want COMMITTED", got)
	}

	item := h.stockOf(t, "p-1")
	if item.GetQuantity() != 6 || item.GetReserved() != 0 {
		t.Fatalf("item = %+v, want 6 sold-through and nothing held", item)
	}
}

func TestCommitReservationErrors(t *testing.T) {
	h := newStoreHarness(t)
	h.setStock(t, "p-1", 10)

	_, err := h.client.CommitReservation(context.Background(), &inventoryv1.CommitReservationRequest{ReservationId: "ghost"})
	assertCode(t, err, codes.NotFound)

	if _, err := h.client.ReserveStock(context.Background(), &inventoryv1.ReserveStockRequest{
		ReservationId: "r-1",
		Lines:         lines("p-1", 4),
	}); err != nil {
		t.Fatalf("ReserveStock: %v", err)
	}
	if _, err := h.client.ReleaseReservation(context.Background(), &inventoryv1.ReleaseReservationRequest{
		ReservationId: "r-1",
		Reason:        "checkout_failed",
	}); err != nil {
		t.Fatalf("ReleaseReservation: %v", err)
	}

	// The units are back in the pool and may already belong to another order.
	_, err = h.client.CommitReservation(context.Background(), &inventoryv1.CommitReservationRequest{ReservationId: "r-1"})
	assertCode(t, err, codes.InvalidArgument)
}

func TestCommitAfterExpiryIsRefused(t *testing.T) {
	h := newStoreHarness(t)
	h.setStock(t, "p-1", 10)
	if _, err := h.client.ReserveStock(context.Background(), &inventoryv1.ReserveStockRequest{
		ReservationId: "r-1",
		Lines:         lines("p-1", 4),
		TtlSeconds:    60,
	}); err != nil {
		t.Fatalf("ReserveStock: %v", err)
	}

	h.clock.Advance(61 * time.Second)

	_, err := h.client.CommitReservation(context.Background(), &inventoryv1.CommitReservationRequest{ReservationId: "r-1"})
	assertCode(t, err, codes.InvalidArgument)

	if item := h.stockOf(t, "p-1"); item.GetReserved() != 0 {
		t.Fatalf("reserved = %d, want the lapsed hold returned", item.GetReserved())
	}
}

// --------------------------------------------------------------- releasing

func TestReleaseReservationIsIdempotentAndTolerantOverTheWire(t *testing.T) {
	h := newStoreHarness(t)
	h.setStock(t, "p-1", 10)

	// Compensation runs when the forward step's outcome is unknown, so an
	// unknown id must be a success with nothing to do rather than an error.
	unknown, err := h.client.ReleaseReservation(context.Background(), &inventoryv1.ReleaseReservationRequest{
		ReservationId: "never-created",
		Reason:        "checkout_failed",
	})
	if err != nil {
		t.Fatalf("ReleaseReservation of an unknown reservation: %v", err)
	}
	if unknown.GetReleased() {
		t.Error("reported releasing stock for a reservation that never existed")
	}
	if unknown.GetReservation() != nil {
		t.Error("a reservation was reported for an id that was never created")
	}

	if _, err := h.client.ReserveStock(context.Background(), &inventoryv1.ReserveStockRequest{
		ReservationId: "r-1",
		Lines:         lines("p-1", 4),
	}); err != nil {
		t.Fatalf("ReserveStock: %v", err)
	}

	first, err := h.client.ReleaseReservation(context.Background(), &inventoryv1.ReleaseReservationRequest{
		ReservationId: "r-1",
		Reason:        "checkout_failed:authorize_payment",
	})
	if err != nil {
		t.Fatalf("ReleaseReservation: %v", err)
	}
	if !first.GetReleased() || first.GetReservation().GetStatus() != inventoryv1.ReservationStatus_RESERVATION_STATUS_RELEASED {
		t.Fatalf("response = %+v, want RELEASED and released=true", first)
	}
	if item := h.stockOf(t, "p-1"); item.GetReserved() != 0 {
		t.Fatalf("reserved = %d, want 0", item.GetReserved())
	}

	second, err := h.client.ReleaseReservation(context.Background(), &inventoryv1.ReleaseReservationRequest{
		ReservationId: "r-1",
		Reason:        "checkout_failed",
	})
	if err != nil {
		t.Fatalf("repeated ReleaseReservation: %v", err)
	}
	if second.GetReleased() {
		t.Error("a repeated release reported returning stock again")
	}
}

func TestReleaseOfAnExpiredHoldSucceeds(t *testing.T) {
	h := newStoreHarness(t)
	h.setStock(t, "p-1", 10)
	if _, err := h.client.ReserveStock(context.Background(), &inventoryv1.ReserveStockRequest{
		ReservationId: "r-1",
		Lines:         lines("p-1", 4),
		TtlSeconds:    60,
	}); err != nil {
		t.Fatalf("ReserveStock: %v", err)
	}

	h.clock.Advance(2 * time.Minute)

	resp, err := h.client.ReleaseReservation(context.Background(), &inventoryv1.ReleaseReservationRequest{
		ReservationId: "r-1",
		Reason:        "checkout_failed",
	})
	if err != nil {
		t.Fatalf("ReleaseReservation: %v", err)
	}
	if resp.GetReleased() {
		t.Error("reported returning stock that expiry had already returned")
	}
	if item := h.stockOf(t, "p-1"); item.GetReserved() != 0 {
		t.Fatalf("reserved = %d, want 0", item.GetReserved())
	}
}

// ------------------------------------------------------------ reservation

func TestGetReservationReportsTheRecordedState(t *testing.T) {
	h := newStoreHarness(t)
	h.setStock(t, "p-1", 10)
	if _, err := h.client.ReserveStock(context.Background(), &inventoryv1.ReserveStockRequest{
		ReservationId: "r-1",
		OrderId:       "order-1",
		Lines:         lines("p-1", 4),
	}); err != nil {
		t.Fatalf("ReserveStock: %v", err)
	}

	resp, err := h.client.GetReservation(context.Background(), &inventoryv1.GetReservationRequest{ReservationId: "r-1"})
	if err != nil {
		t.Fatalf("GetReservation: %v", err)
	}
	if resp.GetReservation().GetStatus() != inventoryv1.ReservationStatus_RESERVATION_STATUS_ACTIVE {
		t.Fatalf("status = %v, want ACTIVE", resp.GetReservation().GetStatus())
	}

	_, err = h.client.GetReservation(context.Background(), &inventoryv1.GetReservationRequest{ReservationId: "ghost"})
	assertCode(t, err, codes.NotFound)
}

// ------------------------------------------------------------- resilience

// panicRepo explodes the way a bug does. It also embeds a nil Repository, so
// every method it does not override panics as well — which is how a
// half-implemented adapter behaves.
type panicRepo struct {
	domain.Repository
}

func (panicRepo) GetStock(_ context.Context, productIDs []string) ([]domain.StockItem, error) {
	for _, id := range productIDs {
		if id == "boom" {
			panic("repository exploded")
		}
	}
	return []domain.StockItem{{ProductID: "p-1", Quantity: 7}}, nil
}

func TestHandlerPanicIsContained(t *testing.T) {
	h := newHarness(t, panicRepo{}, testDefaultTTL)

	_, err := h.client.GetStock(context.Background(), &inventoryv1.GetStockRequest{ProductIds: []string{"boom"}})
	if err == nil {
		t.Fatal("a panicking handler returned success")
	}

	// Two things are asserted about the failure, and neither is the exact wire
	// code, because that code belongs to pkg/grpcx rather than to this service.
	// grpcx installs its recovery interceptor *outside* its error-mapping one,
	// so a recovered panic arrives as an *apperr.Error that never went through
	// the mapper: the wire code is Unknown while the taxonomy still says
	// internal. Accepting either keeps this test about what this service
	// guarantees — no leak, and no crash — instead of about a package it does
	// not own.
	switch code := status.Code(err); code {
	case codes.Internal, codes.Unknown:
	default:
		t.Fatalf("code = %s, want Internal or Unknown", code)
	}
	if got := apperr.CodeOf(err); got != apperr.CodeInternal {
		t.Errorf("apperr code = %q, want internal", got)
	}
	// The stack trace is logged, never returned: it names internal types and
	// file paths.
	if msg := status.Convert(err).Message(); strings.Contains(msg, "exploded") {
		t.Errorf("message %q leaked the panic", msg)
	}

	// The point of recovery: the process and every other in-flight request
	// survive one misbehaving handler. Without the interceptor this call would
	// never return, because the test binary would be gone.
	ok, err := h.client.GetStock(context.Background(), &inventoryv1.GetStockRequest{ProductIds: []string{"p-1"}})
	if err != nil {
		t.Fatalf("the server did not survive the panic: %v", err)
	}
	if len(ok.GetItems()) != 1 || ok.GetItems()[0].GetQuantity() != 7 {
		t.Fatalf("items = %+v, want the healthy response", ok.GetItems())
	}
}

func TestHalfImplementedRepositoryPanicsAndIsContained(t *testing.T) {
	// panicRepo does not override SetStock, so the embedded nil interface
	// panics. A typed-nil adapter is a realistic mistake, and it must surface as
	// one failed call rather than as a dead process.
	h := newHarness(t, panicRepo{}, testDefaultTTL)

	_, err := h.client.SetStock(context.Background(), &inventoryv1.SetStockRequest{ProductId: "p-1", Quantity: 1})
	if err == nil {
		t.Fatal("a panicking handler returned success")
	}
	if got := apperr.CodeOf(err); got != apperr.CodeInternal {
		t.Fatalf("apperr code = %q, want internal", got)
	}
}

// assertCode fails the test unless err carries the expected gRPC status code.
func assertCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("no error, want %s", want)
	}
	if got := status.Code(err); got != want {
		t.Fatalf("code = %s (%v), want %s", got, err, want)
	}
}
