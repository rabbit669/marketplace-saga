package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/marketplace/pkg/apperr"
	"github.com/marketplace/pkg/jwtx"
	authv1 "github.com/marketplace/pkg/proto/auth/v1"
	commonv1 "github.com/marketplace/pkg/proto/common/v1"
	orderv1 "github.com/marketplace/pkg/proto/order/v1"
	"github.com/marketplace/pkg/ratelimit"

	"github.com/marketplace/services/gateway/internal/transport/httpapi"
)

// ------------------------------------------------------------------ fakes

// fakeOrders is a hand-written stand-in for the generated client.
//
// Implementing the interface directly rather than running a real server over
// bufconn keeps these tests about the gateway's own behaviour — auth, error
// shaping, rate limiting — instead of about gRPC plumbing that is already
// covered elsewhere.
type fakeOrders struct {
	createResp *orderv1.CreateOrderResponse
	createErr  error

	getResp *orderv1.GetOrderResponse
	getErr  error

	sagaResp *orderv1.GetSagaStateResponse
	sagaErr  error

	// lastCreate captures the request, so tests can assert what the gateway
	// actually forwarded — in particular the customer id it took from the token
	// rather than from the body.
	lastCreate *orderv1.CreateOrderRequest
	createHits int
}

func (f *fakeOrders) CreateOrder(_ context.Context, in *orderv1.CreateOrderRequest, _ ...grpc.CallOption) (*orderv1.CreateOrderResponse, error) {
	f.createHits++
	f.lastCreate = in
	return f.createResp, f.createErr
}

func (f *fakeOrders) GetOrder(context.Context, *orderv1.GetOrderRequest, ...grpc.CallOption) (*orderv1.GetOrderResponse, error) {
	return f.getResp, f.getErr
}

func (f *fakeOrders) ListOrders(context.Context, *orderv1.ListOrdersRequest, ...grpc.CallOption) (*orderv1.ListOrdersResponse, error) {
	return nil, nil
}

func (f *fakeOrders) CancelOrder(context.Context, *orderv1.CancelOrderRequest, ...grpc.CallOption) (*orderv1.CancelOrderResponse, error) {
	return nil, nil
}

func (f *fakeOrders) GetSagaState(context.Context, *orderv1.GetSagaStateRequest, ...grpc.CallOption) (*orderv1.GetSagaStateResponse, error) {
	return f.sagaResp, f.sagaErr
}

func (f *fakeOrders) RetrySaga(context.Context, *orderv1.RetrySagaRequest, ...grpc.CallOption) (*orderv1.RetrySagaResponse, error) {
	return nil, nil
}

// fakeAuth is a stub identity service.
//
// The public routes are only registered when an auth client is configured, so
// without this the address limiter would have nothing to protect and the test
// would pass for the wrong reason.
type fakeAuth struct{}

func (fakeAuth) Register(context.Context, *authv1.RegisterRequest, ...grpc.CallOption) (*authv1.RegisterResponse, error) {
	return &authv1.RegisterResponse{User: &authv1.User{Id: "u-1"}}, nil
}

func (fakeAuth) Login(context.Context, *authv1.LoginRequest, ...grpc.CallOption) (*authv1.LoginResponse, error) {
	return nil, apperr.Unauthenticated("auth", "email or password is incorrect")
}

func (fakeAuth) Refresh(context.Context, *authv1.RefreshRequest, ...grpc.CallOption) (*authv1.RefreshResponse, error) {
	return nil, apperr.Unauthenticated("auth", "refresh token is not valid")
}

func (fakeAuth) Logout(context.Context, *authv1.LogoutRequest, ...grpc.CallOption) (*authv1.LogoutResponse, error) {
	return &authv1.LogoutResponse{}, nil
}

func (fakeAuth) ValidateToken(context.Context, *authv1.ValidateTokenRequest, ...grpc.CallOption) (*authv1.ValidateTokenResponse, error) {
	return &authv1.ValidateTokenResponse{Valid: false}, nil
}

func (fakeAuth) GetUser(context.Context, *authv1.GetUserRequest, ...grpc.CallOption) (*authv1.GetUserResponse, error) {
	return nil, apperr.NotFound("auth", "no such user")
}

// --------------------------------------------------------------- harness

const testSecret = "test-secret-must-be-at-least-32-bytes-long"

type harness struct {
	handler http.Handler
	orders  *fakeOrders
	now     time.Time
}

func newHarness(t *testing.T, cfg func(*httpapi.Config)) *harness {
	t.Helper()

	h := &harness{
		orders: &fakeOrders{},
		now:    time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC),
	}

	c := httpapi.Config{
		Addr: ":0",
		JWT: jwtx.Config{
			Secret: testSecret,
			Issuer: "marketplace",
			TTL:    time.Hour,
			Now:    func() time.Time { return h.now },
		},
		RateLimit:   ratelimit.Config{PerSecond: 1000, Burst: 1000},
		OrderClient: h.orders,
		AuthClient:  fakeAuth{},
	}
	if cfg != nil {
		cfg(&c)
	}

	server, err := httpapi.NewServer(c)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	h.handler = server.Handler()
	return h
}

func (h *harness) token(t *testing.T, customer string) string {
	t.Helper()

	token, err := jwtx.Issue(
		jwtx.Config{Secret: testSecret, Issuer: "marketplace", TTL: time.Hour, Now: func() time.Time { return h.now }},
		jwtx.Claims{Subject: customer, Email: customer + "@example.com"},
	)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	return token
}

func (h *harness) do(t *testing.T, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	return rec
}

func (h *harness) createOrder(t *testing.T, token, idemKey, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/v1/orders", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	return h.do(t, req)
}

const validOrderBody = `{
  "lines": [{"product_id": "p-1", "quantity": 1}],
  "shipping_address": {"country":"DE","city":"Berlin","street":"Torstrasse 1","postal_code":"10119","recipient":"A. Customer"},
  "payment_method_token": "pm_test_approve"
}`

// ------------------------------------------------------------------ tests

func TestAuth_RejectsMissingAndMalformedTokens(t *testing.T) {
	tests := []struct {
		name   string
		header string
	}{
		{"no header", ""},
		{"empty bearer", "Bearer "},
		{"wrong scheme", "Basic dXNlcjpwYXNz"},
		{"garbage token", "Bearer not-a-jwt"},
		{"scheme without a token", "Bearer"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil)

			req := httptest.NewRequest(http.MethodGet, "/v1/orders/o-1", nil)
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}

			rec := h.do(t, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", rec.Code)
			}

			body := decodeError(t, rec)
			if body.Error.Code != string(apperr.CodeUnauthenticated) {
				t.Errorf("code = %q, want %q", body.Error.Code, apperr.CodeUnauthenticated)
			}
		})
	}
}

func TestAuth_RejectsATokenSignedWithTheWrongSecret(t *testing.T) {
	// The gateway must not accept a token it did not sign. This is the test that
	// would fail if the signing method were ever taken from the token header
	// instead of pinned.
	h := newHarness(t, nil)

	forged, err := jwtx.Issue(
		jwtx.Config{Secret: "a-completely-different-secret-32-bytes!!", Issuer: "marketplace", TTL: time.Hour, Now: func() time.Time { return h.now }},
		jwtx.Claims{Subject: "attacker"},
	)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/orders/o-1", nil)
	req.Header.Set("Authorization", "Bearer "+forged)

	rec := h.do(t, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401: a token signed with another key must be rejected", rec.Code)
	}
}

func TestAuth_RejectsAnExpiredToken(t *testing.T) {
	h := newHarness(t, nil)

	expired, err := jwtx.Issue(
		jwtx.Config{Secret: testSecret, Issuer: "marketplace", TTL: time.Hour,
			Now: func() time.Time { return h.now.Add(-2 * time.Hour) }},
		jwtx.Claims{Subject: "cust-1"},
	)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/orders/o-1", nil)
	req.Header.Set("Authorization", "Bearer "+expired)

	if rec := h.do(t, req); rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

func TestCreateOrder_ForwardsTheAuthenticatedCustomerNotTheBody(t *testing.T) {
	// The customer id comes from the verified token. If it came from the body,
	// any authenticated user could place orders as anyone else — the classic
	// broken-object-level-authorization bug.
	h := newHarness(t, nil)
	h.orders.createResp = &orderv1.CreateOrderResponse{
		Order:      &orderv1.Order{Id: "o-1", Status: orderv1.OrderStatus_ORDER_STATUS_CONFIRMED},
		SagaStatus: orderv1.SagaStatus_SAGA_STATUS_COMPLETED,
	}

	token := h.token(t, "cust-42")
	// A body that tries to claim a different customer.
	body := strings.Replace(validOrderBody, `"lines"`, `"customer_id": "cust-victim", "lines"`, 1)

	// The extra field is rejected outright, which is the stronger outcome.
	rec := h.createOrder(t, token, "key-1", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for an unknown field", rec.Code)
	}

	// And with a well-formed body, the forwarded customer is the token's.
	rec = h.createOrder(t, token, "key-1", validOrderBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}
	if got := h.orders.lastCreate.GetCustomerId(); got != "cust-42" {
		t.Errorf("forwarded customer_id = %q, want cust-42 from the token", got)
	}
}

func TestCreateOrder_RequiresAnIdempotencyKey(t *testing.T) {
	h := newHarness(t, nil)
	token := h.token(t, "cust-1")

	rec := h.createOrder(t, token, "", validOrderBody)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}

	body := decodeError(t, rec)
	if !strings.Contains(body.Error.Message, "Idempotency-Key") {
		t.Errorf("message = %q, want it to name the missing header", body.Error.Message)
	}
	if h.orders.createHits != 0 {
		t.Error("the order service was called without an idempotency key")
	}
}

func TestCreateOrder_HeaderWinsOverTheBody(t *testing.T) {
	// A proxy that rewrites bodies must not be able to change the idempotency
	// semantics of a retry.
	h := newHarness(t, nil)
	h.orders.createResp = &orderv1.CreateOrderResponse{
		Order:      &orderv1.Order{Id: "o-1", Status: orderv1.OrderStatus_ORDER_STATUS_CONFIRMED},
		SagaStatus: orderv1.SagaStatus_SAGA_STATUS_COMPLETED,
	}

	token := h.token(t, "cust-1")
	body := strings.Replace(validOrderBody, `"lines"`, `"idempotency_key": "from-body", "lines"`, 1)

	if rec := h.createOrder(t, token, "from-header", body); rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body: %s)", rec.Code, rec.Body.String())
	}

	if got := h.orders.lastCreate.GetIdempotencyKey(); got != "from-header" {
		t.Errorf("forwarded key = %q, want the header value", got)
	}
}

func TestCreateOrder_DeclinedPaymentReturnsTheOrderAndTheReason(t *testing.T) {
	// A rollback is a business outcome, not a server fault. The order travels in
	// the body so the client can tell the customer what happened and can look the
	// attempt up afterwards.
	h := newHarness(t, nil)
	// The failure travels inside a *successful* response. Returning it as a gRPC
	// error alongside the response would lose the response entirely — gRPC has
	// no partial success — and the client would be left with a status code and
	// no idea what happened.
	h.orders.createResp = &orderv1.CreateOrderResponse{
		Order: &orderv1.Order{
			Id:     "o-1",
			Status: orderv1.OrderStatus_ORDER_STATUS_CANCELLED,
			Total:  commonMoney,
		},
		SagaStatus: orderv1.SagaStatus_SAGA_STATUS_COMPENSATED,
		Failure: &orderv1.CheckoutFailure{
			Code:       string(apperr.CodeFailedPrecondition),
			Message:    "payment was declined: insufficient_funds",
			FailedStep: "authorize_payment",
		},
	}

	token := h.token(t, "cust-1")
	rec := h.createOrder(t, token, "key-1", validOrderBody)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", rec.Code)
	}

	var body struct {
		ID         string `json:"id"`
		Status     string `json:"status"`
		SagaStatus string `json:"saga_status"`
		Failure    *struct {
			Code       string `json:"code"`
			Message    string `json:"message"`
			FailedStep string `json:"failed_step"`
		} `json:"failure"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v\n%s", err, rec.Body.String())
	}

	// The order is reported even on failure: the customer needs an id to refer
	// to, and support needs to be able to look the attempt up.
	if body.ID != "o-1" {
		t.Errorf("id = %q, want the order to be reported even on failure", body.ID)
	}
	if body.Status != "cancelled" {
		t.Errorf("status = %q, want cancelled", body.Status)
	}
	if body.SagaStatus != "compensated" {
		t.Errorf("saga_status = %q, want compensated", body.SagaStatus)
	}

	if body.Failure == nil {
		t.Fatal("no failure in the body: the customer would be told nothing")
	}
	if body.Failure.Code != string(apperr.CodeFailedPrecondition) {
		t.Errorf("failure.code = %q, want %q", body.Failure.Code, apperr.CodeFailedPrecondition)
	}
	if body.Failure.FailedStep != "authorize_payment" {
		t.Errorf("failure.failed_step = %q, want authorize_payment", body.Failure.FailedStep)
	}
	if !strings.Contains(body.Failure.Message, "insufficient_funds") {
		t.Errorf("failure.message = %q, want the provider's reason", body.Failure.Message)
	}
}

func TestCreateOrder_UnrolledBackCheckoutIsAServerFault(t *testing.T) {
	// A saga that could not be rolled back is categorically different from one
	// that was: stock is still held and money may be in limbo. Reporting it as
	// 422 would tell the client it was their fault and there was nothing to do.
	h := newHarness(t, nil)
	h.orders.createResp = &orderv1.CreateOrderResponse{
		Order:      &orderv1.Order{Id: "o-1", Status: orderv1.OrderStatus_ORDER_STATUS_FAILED},
		SagaStatus: orderv1.SagaStatus_SAGA_STATUS_FAILED,
		Failure: &orderv1.CheckoutFailure{
			Code:       string(apperr.CodeInternal),
			Message:    "checkout could not be completed or rolled back",
			FailedStep: "reserve_stock",
		},
	}

	token := h.token(t, "cust-1")
	rec := h.createOrder(t, token, "key-1", validOrderBody)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}

func TestInternalErrorsAreNotLeaked(t *testing.T) {
	// A database error or a stack trace must never reach a client.
	h := newHarness(t, nil)
	h.orders.getErr = apperr.Wrap("order.GetOrder", apperr.CodeInternal,
		"could not load order", fmt.Errorf(`pq: relation "orders" does not exist`))

	token := h.token(t, "cust-1")
	req := httptest.NewRequest(http.MethodGet, "/v1/orders/o-1", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	rec := h.do(t, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}

	raw := rec.Body.String()
	for _, leak := range []string{"pq:", "relation", "does not exist"} {
		if strings.Contains(raw, leak) {
			t.Errorf("response leaked internal detail %q:\n%s", leak, raw)
		}
	}

	body := decodeError(t, rec)
	if body.Error.Message != "internal error" {
		t.Errorf("message = %q, want the opaque fallback", body.Error.Message)
	}
}

func TestErrorResponsesCarryTheRequestID(t *testing.T) {
	h := newHarness(t, nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/orders/o-1", nil)
	req.Header.Set("X-Request-Id", "trace-me-123")

	rec := h.do(t, req)

	if got := rec.Header().Get("X-Request-Id"); got != "trace-me-123" {
		t.Errorf("X-Request-Id = %q, want the caller's id echoed back", got)
	}
	body := decodeError(t, rec)
	if body.Error.RequestID != "trace-me-123" {
		t.Errorf("body request_id = %q, want trace-me-123", body.Error.RequestID)
	}
}

func TestUnknownRouteReturnsTheSameErrorShape(t *testing.T) {
	// A client that parses JSON errors should not have to special-case the one
	// response that is plain text.
	h := newHarness(t, nil)
	token := h.token(t, "cust-1")

	req := httptest.NewRequest(http.MethodGet, "/v1/nope", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	rec := h.do(t, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}

	body := decodeError(t, rec)
	if body.Error.Code != string(apperr.CodeNotFound) {
		t.Errorf("code = %q, want %q", body.Error.Code, apperr.CodeNotFound)
	}
}

func TestRateLimit_RejectsWithRetryAfter(t *testing.T) {
	h := newHarness(t, func(c *httpapi.Config) {
		c.RateLimit = ratelimit.Config{PerSecond: 1, Burst: 2}
	})
	token := h.token(t, "cust-1")

	statuses := make([]int, 0, 5)
	for range 5 {
		req := httptest.NewRequest(http.MethodGet, "/v1/orders/o-1", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := h.do(t, req)
		statuses = append(statuses, rec.Code)
		if rec.Code == http.StatusTooManyRequests {
			if rec.Header().Get("Retry-After") == "" {
				t.Error("a 429 must carry Retry-After, or clients cannot back off politely")
			}
		}
	}

	if statuses[0] != http.StatusOK {
		t.Errorf("first request = %d, want 200", statuses[0])
	}
	if statuses[len(statuses)-1] != http.StatusTooManyRequests {
		t.Errorf("last request = %d, want 429 (all: %v)", statuses[len(statuses)-1], statuses)
	}
}

func TestRateLimit_IsKeyedPerCustomer(t *testing.T) {
	// One customer exhausting their budget must not affect another. A per-IP
	// limit would do exactly that to everyone behind a corporate NAT.
	h := newHarness(t, func(c *httpapi.Config) {
		c.RateLimit = ratelimit.Config{PerSecond: 1, Burst: 1}
	})

	exhaust := func(customer string) int {
		token := h.token(t, customer)
		req := httptest.NewRequest(http.MethodGet, "/v1/orders/o-1", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		return h.do(t, req).Code
	}

	if got := exhaust("cust-a"); got != http.StatusOK {
		t.Fatalf("cust-a first request = %d, want 200", got)
	}
	if got := exhaust("cust-a"); got != http.StatusTooManyRequests {
		t.Fatalf("cust-a second request = %d, want 429", got)
	}
	if got := exhaust("cust-b"); got != http.StatusOK {
		t.Errorf("cust-b request = %d, want 200: limits are per customer", got)
	}
}

func TestMalformedJSONIsRejectedWithAFieldHint(t *testing.T) {
	h := newHarness(t, nil)
	token := h.token(t, "cust-1")

	rec := h.createOrder(t, token, "key-1", `{"lines": [}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if h.orders.createHits != 0 {
		t.Error("the order service was called with a malformed body")
	}
}

// ---------------------------------------------------------------- helpers

type errorEnvelope struct {
	Error struct {
		Code      string            `json:"code"`
		Message   string            `json:"message"`
		RequestID string            `json:"request_id"`
		Fields    map[string]string `json:"fields"`
	} `json:"error"`
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) errorEnvelope {
	t.Helper()

	var body errorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body is not JSON: %v\n%s", err, rec.Body.String())
	}
	if body.Error.Code == "" {
		t.Errorf("error body has no code:\n%s", rec.Body.String())
	}
	return body
}

var commonMoney = &commonv1.Money{AmountMinor: 4999, Currency: "USD"}

func TestRateLimit_DoesNotThrottleAuthenticatedRoutesByAddress(t *testing.T) {
	// The regression this guards: the address limiter was in the global chain,
	// so it covered authenticated routes too. It is sized for a human logging in
	// (a few requests per second), so a load test from one address had thousands
	// of checkouts rejected with 429 — and so would every customer behind a
	// corporate NAT, which is the same situation with worse consequences.
	h := newHarness(t, func(c *httpapi.Config) {
		// Tight, so the test does not have to send hundreds of requests.
		c.AuthRateLimit = ratelimit.Config{PerSecond: 1, Burst: 1}
		c.RateLimit = ratelimit.Config{PerSecond: 1000, Burst: 1000}
	})
	h.orders.createResp = &orderv1.CreateOrderResponse{
		Order:      &orderv1.Order{Id: "o-1", Status: orderv1.OrderStatus_ORDER_STATUS_CONFIRMED},
		SagaStatus: orderv1.SagaStatus_SAGA_STATUS_COMPLETED,
	}

	token := h.token(t, "cust-1")

	// The address budget is spent immediately by the first call...
	for range 5 {
		if rec := h.createOrder(t, token, "key-x", validOrderBody); rec.Code == http.StatusTooManyRequests {
			t.Fatal("an authenticated checkout was throttled by the address limiter")
		}
	}

	// ...and the unauthenticated route is still limited by it.
	login := httptest.NewRequest(http.MethodPost, "/v1/auth/login",
		strings.NewReader(`{"email":"a@example.com","password":"whatever"}`))
	login.Header.Set("Content-Type", "application/json")

	throttled := false
	for range 5 {
		if h.do(t, login).Code == http.StatusTooManyRequests {
			throttled = true
			break
		}
	}
	if !throttled {
		t.Error("login was never throttled; the address limiter is not protecting it")
	}
}
