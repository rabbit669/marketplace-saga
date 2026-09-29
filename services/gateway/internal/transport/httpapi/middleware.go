package httpapi

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/marketplace/pkg/apperr"
	"github.com/marketplace/pkg/idgen"
	"github.com/marketplace/pkg/jwtx"
	"github.com/marketplace/pkg/ratelimit"
)

// ctxKey is an unexported type so that no other package can collide with these
// context keys. Using a plain string key would let any dependency overwrite the
// authenticated caller.
type ctxKey int

const (
	ctxKeyRequestID ctxKey = iota
	ctxKeyClaims
)

// RequestIDFrom returns the request id, or "" when there is none.
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(ctxKeyRequestID).(string)
	return id
}

// ClaimsFrom returns the authenticated caller.
func ClaimsFrom(ctx context.Context) (jwtx.Claims, bool) {
	c, ok := ctx.Value(ctxKeyClaims).(jwtx.Claims)
	return c, ok
}

// CustomerIDFrom returns the authenticated customer id, or "".
func CustomerIDFrom(ctx context.Context) string {
	c, ok := ClaimsFrom(ctx)
	if !ok {
		return ""
	}
	return c.Subject
}

// Middleware is a standard HTTP decorator.
type Middleware func(http.Handler) http.Handler

// chain applies middleware so that the first listed runs outermost.
func chain(h http.Handler, mw ...Middleware) http.Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}

// withRequestID assigns every request an id, honouring an inbound one.
//
// A client-supplied id is accepted because it is what makes a request traceable
// across a system the caller also operates. It is bounded and never trusted for
// anything but correlation.
func withRequestID(log *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := strings.TrimSpace(r.Header.Get("X-Request-Id"))
			if id == "" || len(id) > 128 {
				id = idgen.New()
			}

			ctx := context.WithValue(r.Context(), ctxKeyRequestID, id)
			w.Header().Set("X-Request-Id", id)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// withRecovery turns a panic into a 500 rather than a dropped connection.
//
// Without it, a nil dereference in one handler kills the process and every other
// in-flight request with it. The stack goes to the log; the caller gets a
// generic error and the request id needed to find it.
func withRecovery(log *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log.ErrorContext(r.Context(), "panic recovered in HTTP handler",
						"panic", rec,
						"method", r.Method,
						"path", r.URL.Path,
						"request_id", RequestIDFrom(r.Context()),
						"stack", string(debug.Stack()),
					)
					writeError(w, r, apperr.Internal("gateway", "internal error"))
				}
			}()

			next.ServeHTTP(w, r)
		})
	}
}

// statusRecorder captures the response status for logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	n, err := s.ResponseWriter.Write(b)
	s.bytes += n
	return n, err
}

// withLogging records one line per request.
func withLogging(log *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w}

			next.ServeHTTP(rec, r)

			if rec.status == 0 {
				rec.status = http.StatusOK
			}

			attrs := []any{
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"duration", time.Since(start),
				"bytes", rec.bytes,
				"request_id", RequestIDFrom(r.Context()),
			}
			if customer := CustomerIDFrom(r.Context()); customer != "" {
				attrs = append(attrs, "customer_id", customer)
			}

			switch {
			case rec.status >= 500:
				log.ErrorContext(r.Context(), "request", attrs...)
			case rec.status >= 400:
				log.WarnContext(r.Context(), "request", attrs...)
			default:
				log.InfoContext(r.Context(), "request", attrs...)
			}
		})
	}
}

// withTimeout bounds handler execution.
//
// The gateway's own deadline must be shorter than the client's patience and
// longer than the slowest dependency call, so that the request fails with an
// explained 504 rather than a dropped connection the client cannot interpret.
func withTimeout(d time.Duration) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel()

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// withAuth verifies the bearer token and puts the caller in the context.
//
// Every route behind it can therefore assume an authenticated customer id
// exists, which is why handlers never check for one themselves — a missing check
// in one handler is exactly the bug this arrangement removes.
func withAuth(cfg jwtx.Config, log *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			header := r.Header.Get("Authorization")
			if header == "" {
				writeError(w, r, apperr.Unauthenticated("gateway", "an Authorization header is required"))
				return
			}

			scheme, token, found := strings.Cut(header, " ")
			if !found || !strings.EqualFold(scheme, "bearer") {
				writeError(w, r, apperr.Unauthenticated("gateway", "Authorization must use the Bearer scheme"))
				return
			}

			claims, err := jwtx.Verify(cfg, token)
			if err != nil {
				// The reason is logged but not returned: telling a caller that a
				// token was "expired" rather than "forged" is a small oracle, and
				// they are indistinguishable to anyone legitimate.
				log.WarnContext(r.Context(), "token rejected",
					"error", err.Error(),
					"request_id", RequestIDFrom(r.Context()),
				)
				writeError(w, r, apperr.Unauthenticated("gateway", "the access token is not valid"))
				return
			}

			ctx := context.WithValue(r.Context(), ctxKeyClaims, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// withRateLimit limits requests, keyed by whichever scope the caller names.
//
// "customer" keys on the authenticated subject and must therefore run after
// authentication. "address" keys on the client address and is the only option
// before authentication, where no identity exists yet.
func withRateLimit(limiter ratelimit.Limiter, scope string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var key string
			if scope == "customer" {
				key = CustomerIDFrom(r.Context())
			}
			if key == "" {
				key = clientIP(r)
			}

			allowed, retryAfter := limiter.Allow(r.Context(), key)
			if !allowed {
				seconds := int(retryAfter.Seconds()) + 1
				w.Header().Set("Retry-After", strconv.Itoa(seconds))
				writeError(w, r, apperr.New("gateway", apperr.CodeResourceExhausted,
					"too many requests; retry in "+strconv.Itoa(seconds)+"s"))
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// clientIP extracts the caller's address, honouring a single proxy hop.
//
// Only the first entry of X-Forwarded-For is used, and only because the gateway
// is deployed behind exactly one trusted proxy. Trusting the whole header would
// let a client choose its own rate-limit key by sending a forged value.
func clientIP(r *http.Request) string {
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
		if first, _, found := strings.Cut(forwarded, ","); found {
			return strings.TrimSpace(first)
		}
		return strings.TrimSpace(forwarded)
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// HTTPMetrics records one observation per request.
//
// An interface rather than a Prometheus type, so that this package has no
// opinion about the metrics backend and a test can pass a stub.
type HTTPMetrics interface {
	ObserveHTTP(ctx context.Context, route, method string, status int, duration time.Duration)
}

// withMetrics records the outcome of every request.
//
// The route is read from the request's matched pattern, which is why this
// wrapper is applied per route rather than around the mux. A metric labelled
// with the raw path creates a new time series per order id and exhausts memory —
// the most common way an observability setup takes down the system it watches.
func withMetrics(m HTTPMetrics, _ HTTPMetrics) Middleware {
	return func(next http.Handler) http.Handler {
		if m == nil {
			return next
		}

		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w}

			next.ServeHTTP(rec, r)

			status := rec.status
			if status == 0 {
				status = http.StatusOK
			}

			// r.Pattern is populated by the ServeMux once a route has matched.
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			m.ObserveHTTP(r.Context(), route, r.Method, status, time.Since(start))
		})
	}
}
