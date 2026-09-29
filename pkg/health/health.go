// Package health exposes liveness and readiness endpoints for every service.
//
// The two probes answer different questions and conflating them causes outages:
//
//	/healthz  liveness  — "is this process wedged?" Restarting is the fix.
//	/readyz   readiness — "should traffic be routed here?" Removing from the
//	                      load balancer is the fix, restarting usually is not.
//
// A service whose database is briefly unreachable should stay live and go
// unready; restarting it would only add a cold start to the outage.
package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/marketplace/pkg/version"
)

// Check reports whether a dependency is usable. Returning a non-nil error
// marks the service unready; the error text is included in the probe response.
type Check func(ctx context.Context) error

// Checker aggregates named readiness checks.
type Checker struct {
	mu      sync.RWMutex
	checks  map[string]Check
	started time.Time
	// checkTimeout bounds a single check so one slow dependency cannot hang a
	// probe until the orchestrator's own timeout fires.
	checkTimeout time.Duration
}

// New returns a Checker with no registered checks.
func New() *Checker {
	return &Checker{
		checks:       make(map[string]Check),
		started:      time.Now(),
		checkTimeout: 2 * time.Second,
	}
}

// Register adds or replaces a named check. Registering from multiple
// goroutines is safe.
func (c *Checker) Register(name string, fn Check) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checks[name] = fn
}

// Ready runs every check concurrently and returns a map of failures, keyed by
// check name. An empty map means ready.
func (c *Checker) Ready(ctx context.Context) map[string]string {
	c.mu.RLock()
	checks := make(map[string]Check, len(c.checks))
	for name, fn := range c.checks {
		checks[name] = fn
	}
	c.mu.RUnlock()

	if len(checks) == 0 {
		return nil
	}

	var (
		mu   sync.Mutex
		wg   sync.WaitGroup
		fail = make(map[string]string)
	)

	for name, fn := range checks {
		wg.Add(1)
		go func(name string, fn Check) {
			defer wg.Done()

			cctx, cancel := context.WithTimeout(ctx, c.checkTimeout)
			defer cancel()

			if err := fn(cctx); err != nil {
				mu.Lock()
				fail[name] = err.Error()
				mu.Unlock()
			}
		}(name, fn)
	}
	wg.Wait()

	return fail
}

// Uptime returns how long this process has been running.
func (c *Checker) Uptime() time.Duration { return time.Since(c.started) }

type probeResponse struct {
	Status   string            `json:"status"`
	Service  string            `json:"service,omitempty"`
	Version  string            `json:"version,omitempty"`
	Uptime   string            `json:"uptime"`
	Failures map[string]string `json:"failures,omitempty"`
}

// Handler returns an http.Handler serving the probe endpoints:
//
//	GET /healthz  liveness
//	GET /readyz   readiness
//	GET /version  build metadata
func (c *Checker) Handler(service string) http.Handler {
	return c.HandlerWithRoutes(service, nil)
}

// HandlerWithRoutes serves the probes plus any extra routes.
//
// Extra routes exist so that /metrics shares the probe port. One port per
// service means one thing to configure in a scrape file and in a container
// health check, and it makes "the service is not being scraped" impossible to
// confuse with "the service is down".
func (c *Checker) HandlerWithRoutes(service string, extra map[string]http.Handler) http.Handler {
	mux := http.NewServeMux()

	for pattern, handler := range extra {
		mux.Handle(pattern, handler)
	}

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, probeResponse{
			Status:  "ok",
			Service: service,
			Version: version.Short(),
			Uptime:  c.Uptime().Round(time.Second).String(),
		})
	})

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		fail := c.Ready(r.Context())

		status := http.StatusOK
		label := "ready"
		if len(fail) > 0 {
			status = http.StatusServiceUnavailable
			label = "not_ready"
		}
		writeJSON(w, status, probeResponse{
			Status:   label,
			Service:  service,
			Version:  version.Short(),
			Uptime:   c.Uptime().Round(time.Second).String(),
			Failures: fail,
		})
	})

	mux.HandleFunc("GET /version", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{
			"service": service,
			"version": version.String(),
		})
	})

	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// Server runs the probe endpoints on addr until ctx is cancelled.
type Server struct {
	addr    string
	checker *Checker
	service string
	log     *slog.Logger
	extra   map[string]http.Handler
}

// WithRoutes mounts additional handlers on the probe port, such as /metrics.
func (s *Server) WithRoutes(routes map[string]http.Handler) *Server {
	if s == nil {
		return nil
	}
	s.extra = routes
	return s
}

// NewServer builds a probe server. It returns nil when addr is empty, which
// lets a service disable the probe listener with a single empty config value.
func NewServer(addr, service string, checker *Checker, log *slog.Logger) *Server {
	if addr == "" {
		return nil
	}
	if log == nil {
		log = slog.Default()
	}
	return &Server{addr: addr, checker: checker, service: service, log: log}
}

// Run serves until ctx is cancelled, then shuts down gracefully. A non-nil
// error is returned only for unexpected listener failures; a clean shutdown
// returns nil.
func (s *Server) Run(ctx context.Context) error {
	if s == nil {
		return nil
	}

	srv := &http.Server{
		Addr:              s.addr,
		Handler:           s.checker.HandlerWithRoutes(s.service, s.extra),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		s.log.Info("health server listening", slog.String("addr", s.addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("health server: %w", err)
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("health server shutdown: %w", err)
		}
		s.log.Info("health server stopped")
		return nil
	}
}

// SortedFailures returns failure names in a stable order, for deterministic
// logs and tests.
func SortedFailures(fail map[string]string) []string {
	names := make([]string, 0, len(fail))
	for name := range fail {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
