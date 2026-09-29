package app_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marketplace/pkg/apperr"
	"github.com/marketplace/pkg/money"
	"github.com/marketplace/pkg/resilience"
	"github.com/marketplace/pkg/saga"

	"github.com/marketplace/services/order/internal/app"
	"github.com/marketplace/services/order/internal/domain"
)

// =============================================================================
//  Fakes
//
//  Every port is faked, so these tests exercise the real saga engine, the real
//  checkout definition and the real domain rules — without PostgreSQL, Kafka or
//  a network. That is the payoff of keeping the domain free of infrastructure:
//  the rollback path, which is the part most likely to be wrong, is testable in
//  milliseconds and can therefore actually be tested.
// =============================================================================

type fakeOrders struct {
	mu     sync.Mutex
	orders map[string]*domain.Order
	keys   map[string]string
	// failUpdate, when set, makes Update fail — used to simulate a database
	// problem during compensation.
	failUpdate error
	updates    int
}

func newFakeOrders() *fakeOrders {
	return &fakeOrders{
		orders: make(map[string]*domain.Order),
		keys:   make(map[string]string),
	}
}

// CreateIdempotent mirrors the repository contract: the key and the order are
// claimed under one lock, because that atomicity is the property under test in
// the concurrent-retry case.
func (f *fakeOrders) CreateIdempotent(_ context.Context, o *domain.Order, key string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if existing, ok := f.keys[key]; ok {
		return existing, false, nil
	}
	if _, exists := f.orders[o.ID]; exists {
		return "", false, domain.ErrDuplicateOrder
	}

	cp := *o
	cp.Lines = slices.Clone(o.Lines)
	f.orders[o.ID] = &cp
	f.keys[key] = o.ID
	return o.ID, true, nil
}

func (f *fakeOrders) ReleaseIdempotencyKey(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.keys, key)
	return nil
}

func (f *fakeOrders) Create(_ context.Context, o *domain.Order) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.orders[o.ID]; exists {
		return domain.ErrDuplicateOrder
	}
	cp := *o
	cp.Lines = slices.Clone(o.Lines)
	f.orders[o.ID] = &cp
	return nil
}

func (f *fakeOrders) Get(_ context.Context, id string) (*domain.Order, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	o, ok := f.orders[id]
	if !ok {
		return nil, domain.ErrOrderNotFound
	}
	cp := *o
	cp.Lines = slices.Clone(o.Lines)
	return &cp, nil
}

func (f *fakeOrders) Update(_ context.Context, o *domain.Order) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.updates++
	if f.failUpdate != nil {
		return f.failUpdate
	}

	cp := *o
	cp.Lines = slices.Clone(o.Lines)
	f.orders[o.ID] = &cp
	return nil
}

func (f *fakeOrders) status(id string) domain.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	if o, ok := f.orders[id]; ok {
		return o.Status
	}
	return ""
}

func (f *fakeOrders) get(id string) *domain.Order {
	f.mu.Lock()
	defer f.mu.Unlock()
	o := f.orders[id]
	if o == nil {
		return nil
	}
	cp := *o
	return &cp
}

// ---------------------------------------------------------------- inventory

type reservation struct {
	id      string
	lines   []domain.ReserveLine
	status  string // active | committed | released
	release string // reason
}

type fakeInventory struct {
	mu           sync.Mutex
	reservations map[string]*reservation
	calls        []string

	// failReserve, failCommit and failRelease fail the next n calls.
	failReserve int
	failCommit  int
	failRelease int
	reserveErr  error
}

func newFakeInventory() *fakeInventory {
	return &fakeInventory{
		reservations: make(map[string]*reservation),
		reserveErr:   apperr.Unavailable("inventory.Reserve", "inventory is down"),
	}
}

func (f *fakeInventory) Reserve(_ context.Context, req domain.ReserveRequest) (domain.ReserveResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, "reserve")

	if f.failReserve > 0 {
		f.failReserve--
		return domain.ReserveResult{}, f.reserveErr
	}

	// Idempotent on ReservationID, which is what makes a saga retry safe.
	if existing, ok := f.reservations[req.ReservationID]; ok && existing.status == "active" {
		return domain.ReserveResult{
			ReservationID:  existing.id,
			AlreadyExisted: true,
			ExpiresAt:      time.Now().Add(req.TTL),
		}, nil
	}

	f.reservations[req.ReservationID] = &reservation{
		id:     req.ReservationID,
		lines:  slices.Clone(req.Lines),
		status: "active",
	}
	return domain.ReserveResult{
		ReservationID: req.ReservationID,
		ExpiresAt:     time.Now().Add(req.TTL),
	}, nil
}

func (f *fakeInventory) Commit(_ context.Context, reservationID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, "commit")

	if f.failCommit > 0 {
		f.failCommit--
		return apperr.Unavailable("inventory.Commit", "inventory is down")
	}

	r, ok := f.reservations[reservationID]
	if !ok {
		return apperr.NotFound("inventory.Commit", "reservation %s does not exist", reservationID)
	}
	// Idempotent: committing an already-committed reservation is a no-op, which
	// is what lets the recovery worker retry this step freely.
	if r.status == "active" {
		r.status = "committed"
	}
	return nil
}

func (f *fakeInventory) Release(_ context.Context, reservationID, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, "release")

	if f.failRelease > 0 {
		f.failRelease--
		return apperr.Unavailable("inventory.Release", "inventory is down")
	}

	r, ok := f.reservations[reservationID]
	if !ok {
		// A compensation must tolerate a reservation that was never created:
		// the forward step's outcome was unknown when compensation started.
		return nil
	}
	r.status = "released"
	r.release = reason
	return nil
}

func (f *fakeInventory) reservationFor(id string) *reservation {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.reservations[id]
	if r == nil {
		return nil
	}
	cp := *r
	return &cp
}

func (f *fakeInventory) callLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// ----------------------------------------------------------------- payments

type paymentRecord struct {
	id        string
	status    domain.PaymentStatus
	reason    string
	retryable bool
	voided    bool
}

type fakePayments struct {
	mu       sync.Mutex
	payments map[string]*paymentRecord
	calls    []string

	// decline controls the provider's answer. When it is true the payment is
	// declined; retryable decides whether the saga should try again.
	decline   bool
	retryable bool

	failAuthorize int
	failCapture   int
	failVoid      int
}

func newFakePayments() *fakePayments {
	return &fakePayments{
		payments: make(map[string]*paymentRecord),
		// The default stand-in is a permanent decline: the common case, and the
		// one the rollback path exists for.
		decline: true,
	}
}

func (f *fakePayments) Authorize(_ context.Context, req domain.AuthorizeRequest) (domain.AuthorizeResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, "authorize")

	if f.failAuthorize > 0 {
		f.failAuthorize--
		return domain.AuthorizeResult{}, apperr.Unavailable("payment.Authorize", "provider unreachable")
	}

	// Idempotent on the key: a retry returns the original decision verbatim,
	// including whether it is worth retrying. A replay that dropped the
	// retryable flag would make the saga treat a timeout as a permanent decline
	// on the second attempt and give up early.
	if existing, ok := f.payments[req.IdempotencyKey]; ok {
		return domain.AuthorizeResult{
			PaymentID:      existing.id,
			Status:         existing.status,
			DeclinedReason: existing.reason,
			Retryable:      existing.retryable,
		}, nil
	}

	if f.decline {
		reason := "insufficient_funds"
		if f.retryable {
			reason = "provider_timeout"
		}
		// Real providers issue an identifier for a declined attempt too. That
		// matters here: it is what makes the saga's void step run, and a void
		// against a payment that was never authorized must be a successful
		// no-op rather than an error.
		declinedID := "pay-attempt-" + req.IdempotencyKey
		f.payments[req.IdempotencyKey] = &paymentRecord{
			id:        declinedID,
			status:    domain.PaymentDeclined,
			reason:    reason,
			retryable: f.retryable,
		}
		return domain.AuthorizeResult{
			PaymentID:      declinedID,
			Status:         domain.PaymentDeclined,
			DeclinedReason: reason,
			Retryable:      f.retryable,
		}, nil
	}

	id := "pay-" + req.IdempotencyKey
	f.payments[req.IdempotencyKey] = &paymentRecord{id: id, status: domain.PaymentAuthorized}
	return domain.AuthorizeResult{PaymentID: id, Status: domain.PaymentAuthorized}, nil
}

func (f *fakePayments) Capture(_ context.Context, paymentID, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, "capture")

	if f.failCapture > 0 {
		f.failCapture--
		return apperr.Unavailable("payment.Capture", "provider unreachable")
	}

	for _, p := range f.payments {
		if p.id == paymentID {
			p.status = domain.PaymentCaptured
			return nil
		}
	}
	return apperr.NotFound("payment.Capture", "payment %s does not exist", paymentID)
}

func (f *fakePayments) Void(_ context.Context, paymentID, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, "void")

	if f.failVoid > 0 {
		f.failVoid--
		return apperr.Unavailable("payment.Void", "provider unreachable")
	}

	for _, p := range f.payments {
		if p.id == paymentID {
			p.voided = true
		}
	}
	return nil
}

func (f *fakePayments) callLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

func (f *fakePayments) paymentFor(key string) *paymentRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := f.payments[key]
	if p == nil {
		return nil
	}
	cp := *p
	return &cp
}

// ----------------------------------------------------------------- catalog

type fakeCatalog struct {
	products map[string]domain.Product
	err      error
}

func newFakeCatalog() *fakeCatalog {
	return &fakeCatalog{products: map[string]domain.Product{
		"p-1": {
			ID: "p-1", SKU: "SKU-1", Name: "Espresso machine",
			Price: money.MustNew(19999, money.USD), IsActive: true,
		},
		"p-2": {
			ID: "p-2", SKU: "SKU-2", Name: "Coffee beans 1kg",
			Price: money.MustNew(1499, money.USD), IsActive: true,
		},
		"p-retired": {
			ID: "p-retired", SKU: "SKU-X", Name: "Discontinued grinder",
			Price: money.MustNew(9999, money.USD), IsActive: false,
		},
	}}
}

func (f *fakeCatalog) GetProducts(_ context.Context, ids []string) (map[string]domain.Product, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := make(map[string]domain.Product, len(ids))
	for _, id := range ids {
		if p, ok := f.products[id]; ok {
			out[id] = p
		}
	}
	return out, nil
}

// ------------------------------------------------------------- idempotency

type fakeIdempotency struct {
	mu     sync.Mutex
	keys   map[string]string
	claims int
}

func newFakeIdempotency() *fakeIdempotency {
	return &fakeIdempotency{keys: make(map[string]string)}
}

func (f *fakeIdempotency) Claim(_ context.Context, key, orderID string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.claims++
	if existing, ok := f.keys[key]; ok {
		return existing, false, nil
	}
	f.keys[key] = orderID
	return orderID, true, nil
}

func (f *fakeIdempotency) Release(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.keys, key)
	return nil
}

// ---------------------------------------------------------------- events

type fakeEvents struct {
	mu     sync.Mutex
	events []domain.Event
	err    error
}

func (f *fakeEvents) Publish(_ context.Context, e domain.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return f.err
	}
	f.events = append(f.events, e)
	return nil
}

func (f *fakeEvents) types() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]string, 0, len(f.events))
	for _, e := range f.events {
		out = append(out, e.Type)
	}
	return out
}

// =============================================================================
//  Harness
// =============================================================================

type harness struct {
	service   *app.Service
	engine    *saga.Engine
	store     *saga.MemoryStore
	checkout  *app.CheckoutSaga
	orders    *fakeOrders
	inventory *fakeInventory
	payments  *fakePayments
	catalog   *fakeCatalog
	keys      *fakeIdempotency
	events    *fakeEvents
	clock     time.Time
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	h := &harness{
		orders:    newFakeOrders(),
		inventory: newFakeInventory(),
		payments:  newFakePayments(),
		catalog:   newFakeCatalog(),
		keys:      newFakeIdempotency(),
		events:    &fakeEvents{},
		clock:     time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC),
	}

	// No real sleeping: the retry schedule is not what these tests assert, and
	// a suite that waits on backoff stops being run.
	retry := resilience.RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    time.Millisecond,
		Multiplier:  1,
		Sleep:       func(context.Context, time.Duration) error { return nil },
	}

	var err error
	h.checkout, err = app.NewCheckoutSaga(app.CheckoutConfig{
		Orders:         h.orders,
		Inventory:      h.inventory,
		Payments:       h.payments,
		Events:         h.events,
		Retry:          retry,
		ReservationTTL: 15 * time.Minute,
		Clock:          func() time.Time { return h.clock },
	})
	if err != nil {
		t.Fatalf("NewCheckoutSaga: %v", err)
	}

	h.store = saga.NewMemoryStore()
	h.engine = saga.New(h.store, saga.WithClock(func() time.Time { return h.clock }))

	h.service, err = app.NewService(app.ServiceConfig{
		Orders:   h.orders,
		Catalog:  h.catalog,
		Engine:   h.engine,
		Checkout: h.checkout,
		Clock:    func() time.Time { return h.clock },
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	return h
}

func (h *harness) request(key string) app.CreateOrderRequest {
	return app.CreateOrderRequest{
		CustomerID:         "cust-1",
		IdempotencyKey:     key,
		PaymentMethodToken: "pm_test_token",
		Lines: []domain.LineRequest{
			{ProductID: "p-1", Quantity: 1},
			{ProductID: "p-2", Quantity: 2},
		},
		Shipping: domain.Address{
			Country: "DE", City: "Berlin", Street: "Torstrasse 1",
			PostalCode: "10119", Recipient: "A. Customer",
		},
	}
}

// =============================================================================
//  Tests
// =============================================================================

func TestCreateOrder_HappyPath(t *testing.T) {
	h := newHarness(t)
	h.payments.decline = false

	res, err := h.service.CreateOrder(context.Background(), h.request("key-happy"))
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}

	if res.IdempotentReplay {
		t.Error("IdempotentReplay = true on a first call")
	}
	if res.Order.Status != domain.StatusConfirmed {
		t.Errorf("status = %s, want confirmed", res.Order.Status)
	}
	if res.SagaStatus != saga.StatusCompleted {
		t.Errorf("saga status = %s, want completed", res.SagaStatus)
	}

	// The total is computed from the catalogue: 199.99 + 2 x 14.99.
	want := money.MustNew(19999+2*1499, money.USD)
	if !res.Order.Total.Equal(want) {
		t.Errorf("total = %s, want %s", res.Order.Total, want)
	}

	// Every step ran, in order.
	wantInventory := []string{"reserve", "commit"}
	if got := h.inventory.callLog(); !slices.Equal(got, wantInventory) {
		t.Errorf("inventory calls = %v, want %v", got, wantInventory)
	}
	wantPayments := []string{"authorize", "capture"}
	if got := h.payments.callLog(); !slices.Equal(got, wantPayments) {
		t.Errorf("payment calls = %v, want %v", got, wantPayments)
	}

	// The reservation is committed, not released.
	r := h.inventory.reservationFor(res.Order.ID)
	if r == nil || r.status != "committed" {
		t.Errorf("reservation = %+v, want status committed", r)
	}

	// The order records the handles a compensation would need.
	if res.Order.PaymentID == "" || res.Order.ReservationID == "" {
		t.Errorf("order is missing payment (%q) or reservation (%q) id",
			res.Order.PaymentID, res.Order.ReservationID)
	}

	// Both events were announced.
	wantEvents := []string{app.EventOrderCreated, app.EventOrderConfirmed}
	if got := h.events.types(); !slices.Equal(got, wantEvents) {
		t.Errorf("events = %v, want %v", got, wantEvents)
	}
}

func TestCreateOrder_DeclinedPaymentRollsEverythingBack(t *testing.T) {
	// This is the scenario the whole project exists for: the card is declined
	// after stock has already been reserved.
	h := newHarness(t)
	h.payments.decline = true
	h.payments.retryable = false

	res, err := h.service.CreateOrder(context.Background(), h.request("key-declined"))
	if err == nil {
		t.Fatal("CreateOrder = nil error, want a payment decline")
	}

	// A declined card is the customer's problem to solve, so it is reported as
	// a failed precondition rather than a server error.
	if !apperr.Is(err, apperr.CodeFailedPrecondition) {
		t.Errorf("error code = %q, want %q (err = %v)",
			apperr.CodeOf(err), apperr.CodeFailedPrecondition, err)
	}

	// The order exists and is cancelled — not left pending, and not failed.
	if res.Order == nil {
		t.Fatal("no order returned; the caller needs to know what happened")
	}
	if res.Order.Status != domain.StatusCancelled {
		t.Errorf("status = %s, want cancelled", res.Order.Status)
	}
	if res.SagaStatus != saga.StatusCompensated {
		t.Errorf("saga status = %s, want compensated", res.SagaStatus)
	}
	if res.Order.FailureReason == "" {
		t.Error("FailureReason is empty; a support agent needs to know why")
	}

	// The reserved stock was returned to the pool.
	r := h.inventory.reservationFor(res.Order.ID)
	if r == nil {
		t.Fatal("no reservation record; the reservation should have been created")
	}
	if r.status != "released" {
		t.Errorf("reservation status = %s, want released", r.status)
	}
	if r.release == "" {
		t.Error("release reason is empty; the audit trail should say why")
	}

	// Payment was authorized and then voided, and never captured.
	wantPayments := []string{"authorize", "void"}
	if got := h.payments.callLog(); !slices.Equal(got, wantPayments) {
		t.Errorf("payment calls = %v, want %v", got, wantPayments)
	}
	if slices.Contains(h.payments.callLog(), "capture") {
		t.Error("capture must never run when the payment was declined")
	}

	// Stock was never committed.
	if slices.Contains(h.inventory.callLog(), "commit") {
		t.Error("commit must never run when the payment was declined")
	}

	// The cancellation is announced.
	wantEvents := []string{app.EventOrderCreated, app.EventOrderCancelled}
	if got := h.events.types(); !slices.Equal(got, wantEvents) {
		t.Errorf("events = %v, want %v", got, wantEvents)
	}
}

func TestCreateOrder_RetryableDeclineIsRetriedThenRolledBack(t *testing.T) {
	// A provider timeout is not a decision. The saga retries it, and only gives
	// up — compensating — once the attempts are exhausted.
	h := newHarness(t)
	h.payments.decline = true
	h.payments.retryable = true

	res, err := h.service.CreateOrder(context.Background(), h.request("key-timeout"))
	if err == nil {
		t.Fatal("CreateOrder = nil, want a failure after the retries were exhausted")
	}

	// Three attempts, matching the retry policy, then one void.
	want := []string{"authorize", "authorize", "authorize", "void"}
	if got := h.payments.callLog(); !slices.Equal(got, want) {
		t.Errorf("payment calls = %v, want %v", got, want)
	}

	if res.Order.Status != domain.StatusCancelled {
		t.Errorf("status = %s, want cancelled", res.Order.Status)
	}
}

func TestCreateOrder_IsIdempotent(t *testing.T) {
	// The customer taps "pay" twice because the button did not respond. One
	// order must exist, and the second call must return it rather than create a
	// second one — the classic double-charge.
	h := newHarness(t)
	h.payments.decline = false

	first, err := h.service.CreateOrder(context.Background(), h.request("key-retry"))
	if err != nil {
		t.Fatalf("first CreateOrder: %v", err)
	}

	paymentsBefore := len(h.payments.callLog())
	inventoryBefore := len(h.inventory.callLog())

	second, err := h.service.CreateOrder(context.Background(), h.request("key-retry"))
	if err != nil {
		t.Fatalf("second CreateOrder: %v", err)
	}

	if !second.IdempotentReplay {
		t.Error("IdempotentReplay = false, want true on a replayed call")
	}
	if second.Order.ID != first.Order.ID {
		t.Errorf("second call created order %s, want the original %s", second.Order.ID, first.Order.ID)
	}

	// Critically, the replay did no work: no second charge, no second hold.
	if got := len(h.payments.callLog()); got != paymentsBefore {
		t.Errorf("payment calls grew from %d to %d on a replay", paymentsBefore, got)
	}
	if got := len(h.inventory.callLog()); got != inventoryBefore {
		t.Errorf("inventory calls grew from %d to %d on a replay", inventoryBefore, got)
	}

	if h.orders.get(first.Order.ID) == nil {
		t.Error("the original order is missing")
	}
}

func TestCreateOrder_CompensationFailureMarksTheOrderFailed(t *testing.T) {
	// The payment is declined, and the stock release also fails. The system
	// cannot clean up after itself, and that must be visible rather than
	// silently reported as a clean rollback.
	h := newHarness(t)
	h.payments.decline = true
	// Always fail: the compensation is retried and never succeeds.
	h.inventory.failRelease = 99

	res, err := h.service.CreateOrder(context.Background(), h.request("key-stuck"))
	if err == nil {
		t.Fatal("CreateOrder = nil, want a compensation failure")
	}

	if res.Order.Status != domain.StatusFailed {
		t.Errorf("status = %s, want failed: a human must resolve this", res.Order.Status)
	}

	// The failure is reported as an internal problem, not as a customer error:
	// "your card was declined" would be a lie, since the stock is still held.
	if !apperr.Is(err, apperr.CodeInternal) {
		t.Errorf("error code = %q, want %q", apperr.CodeOf(err), apperr.CodeInternal)
	}

	// The reservation is still held, which is exactly why this needs a human.
	r := h.inventory.reservationFor(res.Order.ID)
	if r == nil || r.status != "active" {
		t.Errorf("reservation = %+v, want it still active so the problem is visible", r)
	}
}

func TestCreateOrder_RejectsUnavailableProductsBeforeDoingAnyWork(t *testing.T) {
	tests := []struct {
		name      string
		productID string
	}{
		{"unknown product", "p-does-not-exist"},
		{"retired product", "p-retired"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.payments.decline = false

			req := h.request("key-" + tc.name)
			req.Lines = []domain.LineRequest{{ProductID: tc.productID, Quantity: 1}}

			_, err := h.service.CreateOrder(context.Background(), req)
			if err == nil {
				t.Fatal("CreateOrder = nil, want a rejection")
			}

			// Pricing happens before the saga starts, so nothing was reserved
			// or charged and no order was written.
			if got := h.inventory.callLog(); len(got) != 0 {
				t.Errorf("inventory was called (%v) for an unavailable product", got)
			}
			if got := h.payments.callLog(); len(got) != 0 {
				t.Errorf("payment was called (%v) for an unavailable product", got)
			}
			if h.orders.get("") != nil {
				t.Error("an order was persisted for an unavailable product")
			}
		})
	}
}

func TestCreateOrder_InvalidRequestsAreRejected(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*app.CreateOrderRequest)
		field  string
	}{
		{"no customer", func(r *app.CreateOrderRequest) { r.CustomerID = "" }, "customer_id"},
		{"no idempotency key", func(r *app.CreateOrderRequest) { r.IdempotencyKey = "" }, "idempotency_key"},
		{"no payment token", func(r *app.CreateOrderRequest) { r.PaymentMethodToken = "" }, "payment_method_token"},
		{"no lines", func(r *app.CreateOrderRequest) { r.Lines = nil }, "lines"},
		{"zero quantity", func(r *app.CreateOrderRequest) {
			r.Lines = []domain.LineRequest{{ProductID: "p-1", Quantity: 0}}
		}, "lines[0]"},
		{"excessive quantity", func(r *app.CreateOrderRequest) {
			r.Lines = []domain.LineRequest{{ProductID: "p-1", Quantity: domain.MaxLineQuantity + 1}}
		}, "lines[0]"},
		{"incomplete address", func(r *app.CreateOrderRequest) { r.Shipping.City = "" }, "shipping_address"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)

			req := h.request("key-invalid")
			tc.mutate(&req)

			_, err := h.service.CreateOrder(context.Background(), req)
			if err == nil {
				t.Fatal("CreateOrder = nil, want a validation error")
			}
			if !apperr.Is(err, apperr.CodeInvalidArgument) {
				t.Errorf("error code = %q, want %q (err = %v)",
					apperr.CodeOf(err), apperr.CodeInvalidArgument, err)
			}

			// The offending field is named, so a client can point at it.
			var appErr *apperr.Error
			if errors.As(err, &appErr) && appErr.Meta["field"] != tc.field {
				t.Errorf("field = %q, want %q", appErr.Meta["field"], tc.field)
			}
		})
	}
}

func TestCreateOrder_ReleasesIdempotencyKeyAfterAPermanentFailure(t *testing.T) {
	// The customer mistyped a product id. Once they correct it they must be
	// able to retry with the same key rather than being told the order already
	// exists.
	h := newHarness(t)

	bad := h.request("key-fixable")
	bad.Lines = []domain.LineRequest{{ProductID: "p-does-not-exist", Quantity: 1}}
	if _, err := h.service.CreateOrder(context.Background(), bad); err == nil {
		t.Fatal("expected the bad request to be rejected")
	}

	h.payments.decline = false

	good := h.request("key-fixable")
	res, err := h.service.CreateOrder(context.Background(), good)
	if err != nil {
		t.Fatalf("retry with a corrected cart failed: %v", err)
	}
	if res.IdempotentReplay {
		t.Error("the corrected cart was treated as a replay of the failed one")
	}
	if res.Order.Status != domain.StatusConfirmed {
		t.Errorf("status = %s, want confirmed", res.Order.Status)
	}
}

func TestGetSagaState_ExposesTheStepHistory(t *testing.T) {
	// The operator story: "why was this order cancelled?" is answered by the
	// saga record, not by grepping five services' logs.
	h := newHarness(t)
	h.payments.decline = true

	res, err := h.service.CreateOrder(context.Background(), h.request("key-inspect"))
	if err == nil {
		t.Fatal("expected the checkout to fail")
	}

	inst, err := h.service.GetSagaState(context.Background(), res.Order.ID)
	if err != nil {
		t.Fatalf("GetSagaState: %v", err)
	}

	if inst.Status != saga.StatusCompensated {
		t.Errorf("saga status = %s, want compensated", inst.Status)
	}
	if inst.FailedStep != app.StepAuthorizePayment {
		t.Errorf("failed step = %q, want %q", inst.FailedStep, app.StepAuthorizePayment)
	}

	states := map[string]saga.StepStatus{}
	for _, step := range inst.Steps {
		states[step.Name] = step.Status
	}

	want := map[string]saga.StepStatus{
		app.StepReserveStock:     saga.StepCompensated,
		app.StepAuthorizePayment: saga.StepFailed,
		app.StepCapturePayment:   saga.StepPending,
		app.StepCommitStock:      saga.StepPending,
		app.StepConfirmOrder:     saga.StepPending,
	}
	for name, wantStatus := range want {
		if states[name] != wantStatus {
			t.Errorf("step %s = %s, want %s", name, states[name], wantStatus)
		}
	}
}

func TestRecovery_ResumesASagaAbandonedMidFlight(t *testing.T) {
	// The process died after reserving stock. Nothing is retrying the saga, and
	// the customer's stock is held — until the recovery worker notices.
	ctx := context.Background()
	h := newHarness(t)
	h.payments.decline = false

	// Build the state a crash would leave behind: the order exists, the
	// reservation is taken, and the saga never moved past step one.
	order, err := domain.NewOrder(domain.NewOrderInput{
		ID:         "order-stuck",
		CustomerID: "cust-1",
		Lines: []domain.Line{{
			ProductID: "p-1", SKU: "SKU-1", Name: "Espresso machine",
			Quantity: 1, UnitPrice: money.MustNew(19999, money.USD),
		}},
		Shipping:  domain.Address{Country: "DE", City: "Berlin", Street: "Torstrasse 1", PostalCode: "10119", Recipient: "A."},
		CreatedAt: h.clock,
	})
	if err != nil {
		t.Fatalf("NewOrder: %v", err)
	}
	if err := h.orders.Create(ctx, order); err != nil {
		t.Fatalf("Create: %v", err)
	}

	def := h.checkout.Definition()
	inst := saga.NewInstance(order.ID, def, h.clock)
	inst.Steps[0].Status = saga.StepCompleted
	inst.Data["reservation_id"] = order.ID
	inst.Data["payment_method_token"] = "pm_test_token"
	inst.UpdatedAt = h.clock

	// The reservation really exists, as it would after a successful step one.
	if _, err := h.inventory.Reserve(ctx, domain.ReserveRequest{
		ReservationID: order.ID, OrderID: order.ID,
		Lines: []domain.ReserveLine{{ProductID: "p-1", Quantity: 1}},
	}); err != nil {
		t.Fatalf("seed reservation: %v", err)
	}
	h.inventory.mu.Lock()
	h.inventory.calls = nil
	h.inventory.mu.Unlock()

	if err := h.store.Create(ctx, inst); err != nil {
		t.Fatalf("store saga: %v", err)
	}

	// Time passes with no progress.
	h.clock = h.clock.Add(10 * time.Minute)

	report, err := h.engine.Recover(ctx, 2*time.Minute, 10, func(name string) (*saga.Definition, bool) {
		if name != app.SagaName {
			return nil, false
		}
		return def, true
	})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if report.Scanned != 1 || report.Resumed != 1 {
		t.Fatalf("report = %+v, want 1 scanned and 1 resumed", report)
	}

	recovered, err := h.engine.Inspect(ctx, order.ID)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if recovered.Status != saga.StatusCompleted {
		t.Fatalf("status = %s, want completed (error: %s)", recovered.Status, recovered.LastError)
	}

	// The critical property: the already-completed step did not run again.
	if got := h.inventory.callLog(); slices.Contains(got, "reserve") {
		t.Errorf("inventory calls = %v, want no second reserve", got)
	}

	// The checkout finished: stock committed, payment captured.
	r := h.inventory.reservationFor(order.ID)
	if r == nil || r.status != "committed" {
		t.Errorf("reservation = %+v, want committed", r)
	}
	if got := h.payments.callLog(); !slices.Contains(got, "capture") {
		t.Errorf("payment calls = %v, want a capture", got)
	}
}

func TestCancelOrder_RejectsConfirmedOrders(t *testing.T) {
	// Cancelling a confirmed order means a refund, which is a different
	// operation with different rules. Reporting success here would tell the
	// customer their money is coming back when it is not.
	h := newHarness(t)
	h.payments.decline = false

	res, err := h.service.CreateOrder(context.Background(), h.request("key-cancel"))
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}

	_, _, err = h.service.CancelOrder(context.Background(), res.Order.ID, "changed my mind")
	if err == nil {
		t.Fatal("CancelOrder on a confirmed order = nil, want a rejection")
	}
	if !apperr.Is(err, apperr.CodeFailedPrecondition) {
		t.Errorf("error code = %q, want %q", apperr.CodeOf(err), apperr.CodeFailedPrecondition)
	}
}

func TestGetOrder_NotFound(t *testing.T) {
	h := newHarness(t)

	_, err := h.service.GetOrder(context.Background(), "order-nope")
	if !apperr.Is(err, apperr.CodeNotFound) {
		t.Errorf("error code = %q, want %q", apperr.CodeOf(err), apperr.CodeNotFound)
	}

	if _, err := h.service.GetOrder(context.Background(), ""); !apperr.Is(err, apperr.CodeInvalidArgument) {
		t.Errorf("empty id gave %q, want %q", apperr.CodeOf(err), apperr.CodeInvalidArgument)
	}
}

func TestCreateOrder_PricesAreSnapshottedNotReferenced(t *testing.T) {
	// A price change between checkout and confirmation must not alter what the
	// customer agreed to pay.
	h := newHarness(t)
	h.payments.decline = false

	res, err := h.service.CreateOrder(context.Background(), h.request("key-price"))
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	original := res.Order.Total

	// Marketing re-prices the product.
	h.catalog.products["p-1"] = domain.Product{
		ID: "p-1", SKU: "SKU-1", Name: "Espresso machine",
		Price: money.MustNew(999, money.USD), IsActive: true,
	}

	stored, err := h.service.GetOrder(context.Background(), res.Order.ID)
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if !stored.Total.Equal(original) {
		t.Errorf("total changed from %s to %s after a catalogue re-price", original, stored.Total)
	}
	if !stored.Lines[0].UnitPrice.Equal(money.MustNew(19999, money.USD)) {
		t.Errorf("line price = %s, want the price at checkout time",
			stored.Lines[0].UnitPrice)
	}
}

func TestCreateOrder_StopsWhenTheEventCannotBeRecorded(t *testing.T) {
	// The outbox write is part of the same transaction as the order. If it
	// cannot be recorded, the saga must not start: a confirmed order whose event
	// was never persisted is a customer who is never notified.
	h := newHarness(t)
	h.payments.decline = false
	h.events.err = errors.New("outbox unavailable")

	_, err := h.service.CreateOrder(context.Background(), h.request("key-noevent"))
	if err == nil {
		t.Fatal("CreateOrder = nil, want a failure")
	}

	if got := h.inventory.callLog(); len(got) != 0 {
		t.Errorf("inventory was called (%v) although the checkout never started", got)
	}
	if got := h.payments.callLog(); len(got) != 0 {
		t.Errorf("payment was called (%v) although the checkout never started", got)
	}
}

func TestCheckoutSaga_DefinitionIsValid(t *testing.T) {
	h := newHarness(t)

	def := h.checkout.Definition()
	if err := def.Validate(); err != nil {
		t.Fatalf("the checkout definition does not validate: %v", err)
	}

	if def.Name != app.SagaName {
		t.Errorf("name = %q, want %q", def.Name, app.SagaName)
	}

	// The step order is the safety argument; assert it explicitly so that a
	// future reordering has to be a deliberate, reviewed change.
	want := []string{
		app.StepReserveStock,
		app.StepAuthorizePayment,
		app.StepCapturePayment,
		app.StepCommitStock,
		app.StepConfirmOrder,
	}

	got := make([]string, 0, len(def.Steps))
	for _, s := range def.Steps {
		got = append(got, s.Name)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("steps = %v, want %v", got, want)
	}

	// Everything before the pivot must be compensatable.
	for _, s := range def.Steps {
		compensatable := s.Compensate != nil
		switch s.Name {
		case app.StepReserveStock, app.StepAuthorizePayment:
			if !compensatable {
				t.Errorf("step %s is before the pivot and must have a compensation", s.Name)
			}
		default:
			if compensatable {
				t.Errorf("step %s is at or after the pivot and must not claim to be compensatable", s.Name)
			}
		}
	}
}

func TestCreateOrder_ConcurrentRetriesCreateOneOrder(t *testing.T) {
	// Two identical requests arriving at once must not both proceed: the
	// idempotency claim is what prevents it, and a check-then-create would race.
	h := newHarness(t)
	h.payments.decline = false

	const attempts = 8

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results []app.CreateOrderResult
		errs    []error
	)

	for i := range attempts {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := h.service.CreateOrder(context.Background(), h.request("key-concurrent"))

			mu.Lock()
			defer mu.Unlock()
			results = append(results, res)
			if err != nil {
				errs = append(errs, fmt.Errorf("attempt %d: %w", i, err))
			}
		}(i)
	}
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("concurrent attempts failed: %v", errs)
	}

	ids := make(map[string]struct{})
	for _, r := range results {
		if r.Order != nil {
			ids[r.Order.ID] = struct{}{}
		}
	}
	if len(ids) != 1 {
		t.Errorf("%d distinct orders created, want exactly 1: %v", len(ids), ids)
	}

	// Only one charge and one hold, regardless of how many callers arrived.
	captures := 0
	for _, c := range h.payments.callLog() {
		if c == "capture" {
			captures++
		}
	}
	if captures != 1 {
		t.Errorf("captures = %d, want exactly 1", captures)
	}
}

// outcomeRecorder captures domain outcomes, so a wiring mistake is caught here
// rather than discovered by an empty panel on a dashboard.
type outcomeRecorder struct {
	mu       sync.Mutex
	outcomes map[string]int
}

func newOutcomeRecorder() *outcomeRecorder {
	return &outcomeRecorder{outcomes: make(map[string]int)}
}

func (o *outcomeRecorder) EventOutcome(kind, outcome string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.outcomes[kind+"/"+outcome]++
}

func (o *outcomeRecorder) count(kind, outcome string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.outcomes[kind+"/"+outcome]
}

func TestCreateOrder_RecordsTheSagaOutcome(t *testing.T) {
	// The metric is the only place the three outcomes are separated, and the
	// three need different responses: completed is success, compensated is the
	// saga working, failed means stock or money is stuck. A counter that is
	// never incremented looks identical to a quiet system.
	tests := []struct {
		name    string
		prepare func(*harness)
		want    string
	}{
		{
			name:    "approved payment completes",
			prepare: func(h *harness) { h.payments.decline = false },
			want:    "completed",
		},
		{
			name:    "declined payment compensates",
			prepare: func(h *harness) { h.payments.decline = true; h.payments.retryable = false },
			want:    "compensated",
		},
		{
			name: "failed compensation is reported separately",
			prepare: func(h *harness) {
				h.payments.decline = true
				h.payments.retryable = false
				h.inventory.failRelease = 99
			},
			want: "failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			recorder := newOutcomeRecorder()

			// Rebuild the service with the recorder attached; the harness builds
			// one without metrics, which is exactly the situation this test
			// exists to make impossible to ship.
			svc, err := app.NewService(app.ServiceConfig{
				Orders:   h.orders,
				Catalog:  h.catalog,
				Engine:   h.engine,
				Checkout: h.checkout,
				Metrics:  recorder,
			})
			if err != nil {
				t.Fatalf("NewService: %v", err)
			}
			h.service = svc

			tc.prepare(h)

			_, _ = h.service.CreateOrder(context.Background(), h.request("outcome-"+tc.want))

			if got := recorder.count("saga_checkout", tc.want); got != 1 {
				t.Errorf("outcome %q recorded %d times, want 1 (all: %v)",
					tc.want, got, recorder.outcomes)
			}
		})
	}
}

func TestCreateOrder_FailureMessageIsFitForACustomer(t *testing.T) {
	// The order service records the provider's reason in the saga's data for
	// exactly this purpose. The step's LastError is a diagnostic and reads
	// "saga <uuid> step authorize_payment: failed after 1 attempt(s): ..." —
	// handing that to a customer leaks identifiers and says "declined" twice.
	h := newHarness(t)
	h.payments.decline = true
	h.payments.retryable = false

	res, err := h.service.CreateOrder(context.Background(), h.request("message-quality"))
	if err == nil {
		t.Fatal("expected the checkout to fail")
	}

	message := apperr.PublicMessage(err)

	// It must say what happened.
	if !strings.Contains(message, "insufficient funds") {
		t.Errorf("message = %q, want the provider's reason in it", message)
	}

	// It must not contain the machinery.
	for _, leak := range []string{"saga ", "step ", "attempt", "failed_precondition", "payment.Authorize"} {
		if strings.Contains(message, leak) {
			t.Errorf("message leaks internal detail %q: %s", leak, message)
		}
	}

	// And it must not repeat itself, which is what wrapping the diagnostic in
	// another "payment was declined:" produced.
	if strings.Count(strings.ToLower(message), "declined") > 1 {
		t.Errorf("message says declined more than once: %s", message)
	}

	if res.Order == nil || res.Order.Status != domain.StatusCancelled {
		t.Errorf("order = %v, want it cancelled and reported", res.Order)
	}
}
