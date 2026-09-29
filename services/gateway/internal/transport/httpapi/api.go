package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/marketplace/pkg/apperr"
	"github.com/marketplace/pkg/jwtx"
	authv1 "github.com/marketplace/pkg/proto/auth/v1"
	catalogv1 "github.com/marketplace/pkg/proto/catalog/v1"
	orderv1 "github.com/marketplace/pkg/proto/order/v1"
	"github.com/marketplace/pkg/ratelimit"
	"github.com/redis/go-redis/v9"
)

// Config configures the HTTP edge.
type Config struct {
	// Addr is the listen address, e.g. ":8080".
	Addr string
	// JWT verifies access tokens.
	JWT jwtx.Config
	// RateLimit is the per-customer budget, applied to authenticated routes.
	RateLimit ratelimit.Config
	// Redis, when set, makes both limiters shared across replicas. Without it
	// each replica keeps its own buckets and the effective allowance is the
	// configured one multiplied by the number of replicas.
	Redis redis.UniversalClient
	// AuthRateLimit is the per-address budget for unauthenticated routes.
	//
	// There are two limiters because they defend against different things.
	// Login has no customer to key on and is the endpoint an attacker
	// brute-forces, so it is limited by address. The rest of the API is limited
	// by customer, so that one account cannot exhaust another's budget — which
	// is what a per-address limit does to everyone behind a shared address.
	AuthRateLimit ratelimit.Config
	// RequestTimeout bounds a single request end to end.
	RequestTimeout time.Duration
	// ShutdownTimeout bounds draining on shutdown.
	ShutdownTimeout time.Duration
	Logger          *slog.Logger
	// Metrics receives one observation per request. Optional.
	Metrics HTTPMetrics
	// OrderClient is the connection to the order service.
	OrderClient orderv1.OrderServiceClient
	// CatalogClient lists products for the demo storefront.
	CatalogClient catalogv1.CatalogServiceClient
	// AuthClient is the connection to the identity service. It is used only for
	// registration and login; token verification stays local, because a network
	// hop on every request would make the auth service a hard dependency of
	// every call rather than only of the two that need it.
	AuthClient authv1.AuthServiceClient
}

// Server is the HTTP edge.
type Server struct {
	cfg         Config
	log         *slog.Logger
	orders      orderv1.OrderServiceClient
	auth        authv1.AuthServiceClient
	catalog     catalogv1.CatalogServiceClient
	limiter     ratelimit.Limiter
	authLimiter ratelimit.Limiter
}

// NewServer builds the edge.
func NewServer(cfg Config) (*Server, error) {
	if cfg.OrderClient == nil {
		return nil, errors.New("gateway: an order service client is required")
	}
	if cfg.Addr == "" {
		cfg.Addr = ":8080"
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 10 * time.Second
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 15 * time.Second
	}
	if cfg.AuthRateLimit.PerSecond <= 0 {
		// Sized for a human logging in, not for a service: enough that a shared
		// office address is not locked out, few enough that online guessing is
		// hopeless.
		cfg.AuthRateLimit = ratelimit.Config{PerSecond: 5, Burst: 15}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if err := cfg.JWT.Validate(); err != nil {
		return nil, err
	}

	return &Server{
		cfg:         cfg,
		log:         cfg.Logger,
		orders:      cfg.OrderClient,
		auth:        cfg.AuthClient,
		catalog:     cfg.CatalogClient,
		limiter:     newLimiter(cfg.Redis, cfg.RateLimit, "customer"),
		authLimiter: newLimiter(cfg.Redis, cfg.AuthRateLimit, "address"),
	}, nil
}

// Handler returns the routed, middleware-wrapped HTTP handler.
//
// The middleware order is the argument, and it is fixed here rather than at each
// route so it cannot drift. Outermost first:
//
//	request id  — everything after it can log a correlation id
//	recovery    — a panic anywhere below becomes a 500
//	logging     — records the final status, including 500s from recovery
//	timeout     — bounds the handler, not the middleware
//
// Rate limiting is not in this chain. It is applied per route, because the two
// limiters key on different things: the address for the unauthenticated routes,
// the customer for everything behind authentication. A single global limiter can
// only key on one of them, and keying on the address throttles every customer
// behind a shared one to the allowance meant for a person logging in.
//
// Authentication is not here: it is applied to the routes that need it, so that
// registration and login stay reachable.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Authentication is applied per route rather than globally, because
	// registration and login are by definition reachable without a token. The
	// rate limiter still covers them: login is the endpoint an attacker
	// brute-forces, so it needs the limiter more than the others do.
	// Wrapping each route rather than the mux: the route *pattern* is what a
	// metric must be labelled by, and it is only available on the request once
	// the mux has matched. Labelling by path instead would create one time
	// series per order id.
	route := func(h http.Handler) http.Handler {
		return withMetrics(s.cfg.Metrics, s.cfg.Metrics)(h)
	}

	requireAuth := func(h http.Handler) http.Handler {
		// Rate limiting sits inside authentication so that the bucket is keyed
		// by the verified customer rather than by an address several customers
		// may share.
		return withAuth(s.cfg.JWT, s.log)(withRateLimit(s.limiter, "customer")(h))
	}

	// Public. These are limited by address, because there is no caller identity
	// yet — and login is the endpoint an attacker brute-forces.
	//
	// The address limiter is applied *here* rather than to the whole mux. Putting
	// it in the global chain made it cover authenticated routes too, and since it
	// is sized for a human logging in rather than for a service, a checkout load
	// test from a single address had 11k of its requests rejected with 429. That
	// is also what every real client behind a corporate NAT would have hit.
	if s.auth != nil {
		mux.Handle("POST /v1/auth/register", route(withRateLimit(s.authLimiter, "address")(s.handleRegister())))
		mux.Handle("POST /v1/auth/login", route(withRateLimit(s.authLimiter, "address")(s.handleLogin())))
		mux.Handle("POST /v1/auth/refresh", route(withRateLimit(s.authLimiter, "address")(s.handleRefresh())))
	}

	// The demo storefront, and the product listing it needs. Both are public:
	// a shop window does not require a token, and the page itself is static.
	s.registerUIRoutes(mux, route)

	// Authenticated.
	mux.Handle("POST /v1/orders", route(requireAuth(s.handleCreateOrder())))
	mux.Handle("GET /v1/orders/{orderID}", route(requireAuth(s.handleGetOrder())))
	mux.Handle("GET /v1/orders/{orderID}/saga", route(requireAuth(s.handleGetSagaState())))
	mux.Handle("POST /v1/orders/{orderID}/cancel", route(requireAuth(s.handleCancelOrder())))

	// A catch-all that answers in the same error shape as everything else. The
	// default net/http 404 is plain text, and a client that parses JSON errors
	// should not have to special-case the one response it cannot parse.
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, apperr.NotFound("gateway", "no route matches %s %s", r.Method, r.URL.Path))
	}))

	return chain(mux,
		withRequestID(s.log),
		withRecovery(s.log),
		withLogging(s.log),
		withTimeout(s.cfg.RequestTimeout),
	)
}

// Serve runs the edge until ctx is cancelled, then drains in-flight requests.
func (s *Server) Serve(ctx context.Context) error {
	srv := &http.Server{
		Addr:    s.cfg.Addr,
		Handler: s.Handler(),

		// ReadHeaderTimeout is the one that matters for a public listener: it
		// bounds how long a client may hold a connection open by dribbling out
		// headers, which is the cheapest slowloris there is.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       90 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}

	// A janitor for the rate limiter's buckets. An attacker sending requests
	// with a fresh key each time grows the map without bound, which turns the
	// protection into the attack.
	go s.reapLimiter(ctx)

	errCh := make(chan error, 1)
	go func() {
		s.log.Info("http server listening", "addr", s.cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("gateway: listen: %w", err)
		}
	}()

	select {
	case err := <-errCh:
		return err

	case <-ctx.Done():
		s.log.Info("http server draining", "grace", s.cfg.ShutdownTimeout)
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("gateway: shutdown: %w", err)
	}

	s.log.Info("http server stopped")
	return nil
}

func (s *Server) reapLimiter(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Redis expires its own buckets; this only does anything for the
			// in-memory implementation.
			if removed, ok := s.limiter.(interface{ Reap(time.Duration) int }); ok {
				if n := removed.Reap(10 * time.Minute); n > 0 {
					s.log.Debug("reaped idle rate limit buckets", "count", n)
				}
			}
		}
	}
}

// newLimiter builds the shared limiter when Redis is configured, and a
// per-process one otherwise.
//
// The in-memory fallback stays meaningful even with Redis: it is what the shared
// limiter degrades to if Redis becomes unreachable, so a cache outage cannot
// take the edge down with it.
func newLimiter(client redis.UniversalClient, cfg ratelimit.Config, scope string) ratelimit.Limiter {
	local := ratelimit.NewMemory(cfg)
	if client == nil {
		return local
	}

	shared, err := ratelimit.NewRedis(ratelimit.RedisConfig{
		Client:    client,
		PerSecond: cfg.PerSecond,
		Burst:     cfg.Burst,
		Fallback:  local,
	})
	if err != nil {
		// Falling back is better than refusing to start: the service is still
		// correct, only its limit becomes per-replica.
		return local
	}
	_ = scope
	return shared
}

// ---------------------------------------------------------------- handlers

// createOrderRequest is the public shape, deliberately not the generated proto.
//
// The wire contract between services is free to change; this is not. Keeping a
// separate DTO is what stops an internal field rename from becoming a breaking
// change for every client, and it is where public naming conventions — snake
// case, minor-unit amounts — are enforced.
type createOrderRequest struct {
	Lines              []orderLineRequest `json:"lines"`
	ShippingAddress    addressDTO         `json:"shipping_address"`
	PaymentMethodToken string             `json:"payment_method_token"`
	// IdempotencyKey is optional here because the header is the convention; when
	// both are present the header wins, so a proxy that rewrites bodies cannot
	// change the idempotency semantics.
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

type orderLineRequest struct {
	ProductID string `json:"product_id"`
	Quantity  int64  `json:"quantity"`
}

type addressDTO struct {
	Country    string `json:"country"`
	City       string `json:"city"`
	Street     string `json:"street"`
	PostalCode string `json:"postal_code"`
	Recipient  string `json:"recipient"`
}

type moneyDTO struct {
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
}

type orderLineResponse struct {
	ProductID string   `json:"product_id"`
	SKU       string   `json:"sku,omitempty"`
	Name      string   `json:"name,omitempty"`
	Quantity  int64    `json:"quantity"`
	UnitPrice moneyDTO `json:"unit_price"`
	LineTotal moneyDTO `json:"line_total"`
}

type orderResponse struct {
	ID              string              `json:"id"`
	Status          string              `json:"status"`
	Lines           []orderLineResponse `json:"lines"`
	Total           moneyDTO            `json:"total"`
	ShippingAddress addressDTO          `json:"shipping_address"`
	PaymentID       string              `json:"payment_id,omitempty"`
	ReservationID   string              `json:"reservation_id,omitempty"`
	// SagaStatus is the orchestrator's own status, which is not the same as the
	// order's: an order can be cancelled because the saga rolled back cleanly,
	// or failed because it could not.
	SagaStatus string `json:"saga_status,omitempty"`
	// Failure is present when the checkout did not complete.
	Failure *checkoutFailureDTO `json:"failure,omitempty"`
	// IdempotentReplay says this response describes an order created by an
	// earlier call with the same idempotency key.
	IdempotentReplay bool `json:"idempotent_replay,omitempty"`
}

type sagaStepResponse struct {
	Name        string `json:"name"`
	Status      string `json:"status"`
	Attempts    int    `json:"attempts"`
	Error       string `json:"error,omitempty"`
	Compensated bool   `json:"compensated,omitempty"`
	StartedAt   string `json:"started_at,omitempty"`
	FinishedAt  string `json:"finished_at,omitempty"`
}

type sagaStateResponse struct {
	OrderID    string             `json:"order_id"`
	Status     string             `json:"status"`
	FailedStep string             `json:"failed_step,omitempty"`
	LastError  string             `json:"last_error,omitempty"`
	Attempts   int                `json:"attempts"`
	Steps      []sagaStepResponse `json:"steps"`
}

func (s *Server) handleCreateOrder() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req createOrderRequest
		if err := decodeJSON(w, r, &req); err != nil {
			writeError(w, r, err)
			return
		}

		// The header is the convention and takes precedence over the body, so
		// that the idempotency of a retry cannot be altered by whatever
		// serialises the body.
		idempotencyKey := r.Header.Get("Idempotency-Key")
		if idempotencyKey == "" {
			idempotencyKey = req.IdempotencyKey
		}
		if idempotencyKey == "" {
			writeError(w, r, apperr.InvalidArgument("gateway",
				"an Idempotency-Key header is required: without it a retried checkout creates a second order"))
			return
		}

		lines := make([]*orderv1.OrderLineRequest, 0, len(req.Lines))
		for _, l := range req.Lines {
			lines = append(lines, &orderv1.OrderLineRequest{
				ProductId: l.ProductID,
				Quantity:  l.Quantity,
			})
		}

		resp, err := s.orders.CreateOrder(r.Context(), &orderv1.CreateOrderRequest{
			CustomerId:         CustomerIDFrom(r.Context()),
			Lines:              lines,
			ShippingAddress:    addressToProto(req.ShippingAddress),
			IdempotencyKey:     idempotencyKey,
			PaymentMethodToken: req.PaymentMethodToken,
		})

		if err != nil {
			// A transport-level failure: the order service could not be reached,
			// or it returned a status rather than a business outcome.
			writeError(w, r, err)
			return
		}

		order := resp.GetOrder()
		if order == nil {
			// No order and no error means the contract was violated. Saying so
			// is better than returning an empty 201 that a client would treat
			// as a successful purchase.
			writeError(w, r, apperr.Internal("gateway", "the order service returned no order"))
			return
		}

		body := toOrderResponse(order)
		body.SagaStatus = sagaStatusString(resp.GetSagaStatus())
		body.IdempotentReplay = resp.GetIdempotentReplay()

		// A rolled-back checkout arrives as a *successful* response carrying a
		// failure, because gRPC discards the response when the status is not OK.
		// The HTTP status is therefore chosen here, from the classification the
		// order service sent — so the same failure maps to the same status
		// whether it was raised locally or on the far side of the call.
		if failure := resp.GetFailure(); failure != nil {
			body.Failure = &checkoutFailureDTO{
				Code:       failure.GetCode(),
				Message:    failure.GetMessage(),
				FailedStep: failure.GetFailedStep(),
				Retryable:  failure.GetRetryable(),
			}

			s.log.WarnContext(r.Context(), "checkout did not complete",
				"order_id", body.ID,
				"order_status", body.Status,
				"saga_status", body.SagaStatus,
				"failed_step", failure.GetFailedStep(),
				"code", failure.GetCode(),
			)

			writeJSON(w, r, apperr.HTTPStatusForCode(apperr.Code(failure.GetCode())), body)
			return
		}

		status := http.StatusCreated
		if resp.GetIdempotentReplay() {
			// A replay created nothing, so 200 rather than 201.
			status = http.StatusOK
		}

		writeJSON(w, r, status, body)
	})
}

func (s *Server) handleGetOrder() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		orderID := r.PathValue("orderID")

		resp, err := s.orders.GetOrder(r.Context(), &orderv1.GetOrderRequest{OrderId: orderID})
		if err != nil {
			writeError(w, r, err)
			return
		}

		writeJSON(w, r, http.StatusOK, toOrderResponse(resp.GetOrder()))
	})
}

func (s *Server) handleGetSagaState() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		orderID := r.PathValue("orderID")

		resp, err := s.orders.GetSagaState(r.Context(), &orderv1.GetSagaStateRequest{OrderId: orderID})
		if err != nil {
			writeError(w, r, err)
			return
		}

		steps := make([]sagaStepResponse, 0, len(resp.GetSteps()))
		for _, step := range resp.GetSteps() {
			steps = append(steps, sagaStepResponse{
				Name:        step.GetName(),
				Status:      stepStatusString(step.GetStatus()),
				Attempts:    int(step.GetAttempt()),
				Error:       step.GetError(),
				Compensated: step.GetCompensated(),
				StartedAt:   timeString(timeFromProto(step.GetStartedAt())),
				FinishedAt:  timeString(timeFromProto(step.GetFinishedAt())),
			})
		}

		writeJSON(w, r, http.StatusOK, sagaStateResponse{
			OrderID:   resp.GetOrderId(),
			Status:    sagaStatusString(resp.GetStatus()),
			Attempts:  int(resp.GetAttempts()),
			LastError: resp.GetLastError(),
			Steps:     steps,
		})
	})
}

func (s *Server) handleCancelOrder() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		orderID := r.PathValue("orderID")

		var body struct {
			Reason string `json:"reason"`
		}
		// An empty body is allowed: the reason is nice to have, not required.
		if r.ContentLength > 0 {
			if err := decodeJSON(w, r, &body); err != nil {
				writeError(w, r, err)
				return
			}
		}

		resp, err := s.orders.CancelOrder(r.Context(), &orderv1.CancelOrderRequest{
			OrderId: orderID,
			Reason:  body.Reason,
		})
		if err != nil {
			writeError(w, r, err)
			return
		}

		out := toOrderResponse(resp.GetOrder())
		out.SagaStatus = sagaStatusString(resp.GetSagaStatus())
		writeJSON(w, r, http.StatusOK, out)
	})
}

// ------------------------------------------------------------ auth routes

type registerRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
	FullName string `json:"full_name"`
}

type userResponse struct {
	ID            string   `json:"id"`
	Email         string   `json:"email"`
	FullName      string   `json:"full_name"`
	Roles         []string `json:"roles"`
	EmailVerified bool     `json:"email_verified"`
}

type tokenResponse struct {
	AccessToken  string       `json:"access_token"`
	RefreshToken string       `json:"refresh_token,omitempty"`
	TokenType    string       `json:"token_type"`
	ExpiresIn    int64        `json:"expires_in"`
	User         userResponse `json:"user,omitempty"`
}

func (s *Server) handleRegister() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req registerRequest
		if err := decodeJSON(w, r, &req); err != nil {
			writeError(w, r, err)
			return
		}

		resp, err := s.auth.Register(r.Context(), &authv1.RegisterRequest{
			Email:    req.Email,
			Password: req.Password,
			FullName: req.FullName,
		})
		if err != nil {
			writeError(w, r, err)
			return
		}

		writeJSON(w, r, http.StatusCreated, toUserResponse(resp.GetUser()))
	})
}

func (s *Server) handleLogin() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Email    string `json:"email"`
			Password string `json:"password"`
			Client   string `json:"client,omitempty"`
		}
		if err := decodeJSON(w, r, &req); err != nil {
			writeError(w, r, err)
			return
		}

		resp, err := s.auth.Login(r.Context(), &authv1.LoginRequest{
			Email:    req.Email,
			Password: req.Password,
			Client:   req.Client,
		})
		if err != nil {
			writeError(w, r, err)
			return
		}

		writeJSON(w, r, http.StatusOK, tokenResponse{
			AccessToken:  resp.GetAccessToken(),
			RefreshToken: resp.GetRefreshToken(),
			TokenType:    "Bearer",
			ExpiresIn:    resp.GetExpiresIn(),
			User:         toUserResponse(resp.GetUser()),
		})
	})
}

func (s *Server) handleRefresh() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RefreshToken string `json:"refresh_token"`
		}
		if err := decodeJSON(w, r, &req); err != nil {
			writeError(w, r, err)
			return
		}

		resp, err := s.auth.Refresh(r.Context(), &authv1.RefreshRequest{
			RefreshToken: req.RefreshToken,
		})
		if err != nil {
			writeError(w, r, err)
			return
		}

		// The new refresh token is returned because the old one is now consumed.
		// A client that keeps the old value will be logged out on its next
		// attempt, which is the reuse detection working as designed.
		writeJSON(w, r, http.StatusOK, tokenResponse{
			AccessToken:  resp.GetAccessToken(),
			RefreshToken: resp.GetRefreshToken(),
			TokenType:    "Bearer",
			ExpiresIn:    resp.GetExpiresIn(),
		})
	})
}

func toUserResponse(u *authv1.User) userResponse {
	if u == nil {
		return userResponse{}
	}

	roles := make([]string, 0, len(u.GetRoles()))
	for _, r := range u.GetRoles() {
		switch r {
		case authv1.Role_ROLE_CUSTOMER:
			roles = append(roles, "customer")
		case authv1.Role_ROLE_ADMIN:
			roles = append(roles, "admin")
		}
	}

	return userResponse{
		ID:            u.GetId(),
		Email:         u.GetEmail(),
		FullName:      u.GetFullName(),
		Roles:         roles,
		EmailVerified: u.GetEmailVerified(),
	}
}
