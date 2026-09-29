// Package metrics exposes Prometheus metrics for every service.
//
// The metrics are chosen around one question: when a checkout is slow, which
// service and which call is responsible? That is the RED method — rate, errors,
// duration — applied per gRPC method, plus the Go runtime collectors, plus a
// small number of domain counters that answer questions the transport cannot.
//
// Deliberately absent: per-request labels such as customer id or order id. A
// label with unbounded values turns a metric into a memory leak, and Prometheus
// was never the right place to answer "what happened to this one order" — the
// saga record and the traces are.
package metrics

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc/codes"
)

// Metrics owns a service's registry and its collectors.
//
// The registry is per service rather than the global default, so that a test can
// build one without inheriting or polluting another's series.
type Metrics struct {
	registry *prometheus.Registry
	service  string

	grpcRequests   *prometheus.CounterVec
	grpcDuration   *prometheus.HistogramVec
	grpcInFlight   *prometheus.GaugeVec
	httpRequests   *prometheus.CounterVec
	httpDuration   *prometheus.HistogramVec
	domainCounters *prometheus.CounterVec
	domainGauges   *prometheus.GaugeVec
}

// New builds a registry for one service.
func New(service string) *Metrics {
	registry := prometheus.NewRegistry()

	m := &Metrics{
		registry: registry,
		service:  service,

		grpcRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "grpc_server_requests_total",
			Help: "Total gRPC calls handled, by method and status code.",
		}, []string{"service", "method", "code"}),

		grpcDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "grpc_server_duration_seconds",
			Help: "gRPC call latency in seconds, by method.",
			// Buckets from 1 ms to 10 s. The low end matters because the
			// interesting regression is usually a dependency that went from
			// 2 ms to 200 ms, and the high end because a saga step with retries
			// can legitimately take seconds.
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"service", "method"}),

		grpcInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "grpc_server_in_flight_requests",
			Help: "gRPC calls currently being handled.",
		}, []string{"service"}),

		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_server_requests_total",
			Help: "Total HTTP requests handled, by route, method and status code.",
		}, []string{"service", "method", "route", "status"}),

		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_server_duration_seconds",
			Help:    "HTTP request latency in seconds, by route.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"service", "method", "route"}),

		domainCounters: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "marketplace_events_total",
			Help: "Domain events by kind, for example saga terminal statuses.",
		}, []string{"service", "kind", "outcome"}),

		domainGauges: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "marketplace_state",
			Help: "Point-in-time counts of domain state, for example pending outbox events.",
		}, []string{"service", "kind"}),
	}

	registry.MustRegister(
		m.grpcRequests, m.grpcDuration, m.grpcInFlight,
		m.httpRequests, m.httpDuration,
		m.domainCounters, m.domainGauges,
	)

	// The runtime and process collectors are the cheapest way to answer "is this
	// a leak or is it load?": goroutine count and heap size separate a stuck
	// service from a busy one, and open file descriptors catch a connection leak
	// before it becomes an outage.
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	return m
}

// Handler returns the /metrics endpoint.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		// A scrape that fails to gather one collector should still return the
		// rest, rather than turning a metric bug into a monitoring outage.
		ErrorHandling: promhttp.ContinueOnError,
	})
}

// Registry exposes the registry, for tests.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// --------------------------------------------------------------- transport

// ObserveRPC implements the grpcx.Metrics port.
//
// It records the outcome even for a call that failed, which is the point: an
// error rate computed only from successful calls is always zero.
func (m *Metrics) ObserveRPC(_ context.Context, method string, code codes.Code, duration time.Duration) {
	m.grpcRequests.WithLabelValues(m.service, method, code.String()).Inc()
	m.grpcDuration.WithLabelValues(m.service, method).Observe(duration.Seconds())
}

// RPCStarted and RPCFinished track in-flight calls.
func (m *Metrics) RPCStarted()  { m.grpcInFlight.WithLabelValues(m.service).Inc() }
func (m *Metrics) RPCFinished() { m.grpcInFlight.WithLabelValues(m.service).Dec() }

// ObserveHTTP records one HTTP request.
//
// The route is the pattern rather than the path — "/v1/orders/{orderID}", not
// "/v1/orders/01a0ed...". Labelling by path would create a new time series per
// order and exhaust memory within a day; this is the single most common way a
// metrics setup takes down the system it was meant to observe.
func (m *Metrics) ObserveHTTP(ctx context.Context, route, method string, status int, duration time.Duration) {
	if route == "" {
		route = "unmatched"
	}

	statusLabel := strconv.Itoa(status)
	m.httpRequests.WithLabelValues(m.service, method, route, statusLabel).Inc()
	m.httpDuration.WithLabelValues(m.service, method, route).Observe(duration.Seconds())
}

// WithRoute returns a context carrying the route pattern, so that an HTTP
// middleware can record the pattern rather than the raw path.
func WithRoute(ctx context.Context, route string) context.Context {
	return context.WithValue(ctx, routeKey{}, route)
}

// RouteFrom returns the route pattern recorded in the context.
func RouteFrom(ctx context.Context) string {
	route, _ := ctx.Value(routeKey{}).(string)
	return route
}

type routeKey struct{}

// ---------------------------------------------------------------- domain

// EventOutcome counts a domain event by kind and outcome.
//
// Kind is bounded by the code — "saga_checkout" — and outcome by the small set
// of terminal states, so this cannot grow without bound.
func (m *Metrics) EventOutcome(kind, outcome string) {
	m.domainCounters.WithLabelValues(m.service, kind, outcome).Inc()
}

// SetState publishes a point-in-time count.
func (m *Metrics) SetState(kind string, value float64) {
	m.domainGauges.WithLabelValues(m.service, kind).Set(value)
}
