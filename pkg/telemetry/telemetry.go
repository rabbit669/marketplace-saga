// Package telemetry wires OpenTelemetry into a service.
//
// The point of tracing here is not the dashboards. It is that a checkout touches
// five services, and when one is slow there is otherwise no way to answer "where
// did the time go?" without reading five sets of logs and guessing at the
// ordering. A trace answers it in one view, provided two things are true:
//
//   - **The context must be propagated across every hop.** A trace that stops at
//     a service boundary shows one service's work and calls it the whole story,
//     which is worse than no trace because it looks complete. gRPC propagates it
//     automatically through metadata, and the Kafka path carries it explicitly
//     in the outbox row and the record header.
//
//   - **Sampling must be decided once, at the edge.** If each service sampled
//     independently, a five-service checkout would produce traces with holes in
//     them. The sampler is therefore configurable and defaults to sampling
//     everything in development, where the goal is to see the flow rather than
//     to control cost.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Config describes how to instrument a service.
type Config struct {
	// ServiceName identifies the service in the trace UI.
	ServiceName string
	// Version is the build, attached to every span so that a regression can be
	// tied to a deploy.
	Version string
	// Env is dev, staging or prod.
	Env string
	// Endpoint is the OTLP gRPC collector, e.g. "jaeger:4317".
	Endpoint string
	// Enabled turns tracing off entirely. Zero-value spans are cheap, but a
	// service that cannot reach a collector should not spend its shutdown
	// budget waiting for one.
	Enabled bool
	// SampleRatio is the fraction of traces to record, in [0, 1].
	SampleRatio float64
	// Insecure skips TLS to the collector. True inside a cluster network, where
	// TLS is terminated by the mesh, and false anywhere else.
	Insecure bool
	Logger   *slog.Logger
}

// Provider owns the tracer provider and its exporter.
type Provider struct {
	tp     *sdktrace.TracerProvider
	log    *slog.Logger
	closer func(context.Context) error
}

// New builds and installs a tracer provider.
//
// The global provider is set even when tracing is disabled: a zero provider that
// returns non-recording spans means instrumentation code never has to check
// whether tracing is on, which removes the branch that would eventually be got
// wrong.
func New(ctx context.Context, cfg Config) (*Provider, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.SampleRatio < 0 {
		cfg.SampleRatio = 0
	}
	if cfg.SampleRatio > 1 {
		cfg.SampleRatio = 1
	}

	if !cfg.Enabled || cfg.Endpoint == "" {
		cfg.Logger.Info("tracing is disabled", "service", cfg.ServiceName)
		return &Provider{tp: sdktrace.NewTracerProvider(), log: cfg.Logger}, nil
	}

	opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.Endpoint)}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}

	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		// A missing collector must not stop a service from running. Losing
		// traces is a visibility problem; refusing to start is an outage, and
		// choosing the outage would be the wrong trade every time.
		cfg.Logger.Warn("could not create the trace exporter, continuing without tracing",
			"endpoint", cfg.Endpoint, "error", err.Error())
		return &Provider{tp: sdktrace.NewTracerProvider(), log: cfg.Logger}, nil
	}

	// Built with resource.New rather than merging resource.Default() with a
	// hand-made resource. Merging the two fails whenever their schema URLs
	// differ, and the schema URL comes from whichever semconv package each side
	// was compiled against — so it breaks on an SDK upgrade, silently, by
	// falling back to the default resource. The symptom is every span arriving
	// under `unknown_service:<binary>` and the trace UI listing a service nobody
	// recognises.
	//
	// The attribute keys are written as strings for the same reason: importing a
	// semconv package would pin this file to one schema version, which is
	// exactly the coupling that caused the problem. These three keys are stable
	// in the specification.
	res, err := resource.New(ctx,
		// OTEL_RESOURCE_ATTRIBUTES lets an operator attach a cluster or region
		// without rebuilding the image.
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(
			attribute.String("service.name", cfg.ServiceName),
			attribute.String("service.version", cfg.Version),
			attribute.String("deployment.environment", cfg.Env),
		),
	)
	if err != nil {
		cfg.Logger.Warn("could not build the trace resource, spans will carry only the defaults",
			"error", err.Error())
		res = resource.Default()
	}

	var sampler sdktrace.Sampler
	switch {
	case cfg.SampleRatio >= 1:
		sampler = sdktrace.AlwaysSample()
	case cfg.SampleRatio <= 0:
		sampler = sdktrace.NeverSample()
	default:
		// ParentBased keeps a trace whole: once the edge decides to record it,
		// every downstream service records its part. Sampling independently per
		// service is what produces traces with missing middle sections.
		sampler = sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			// A short batch delay: a trace that arrives 30 seconds after the
			// request is of little use while watching a deploy.
			sdktrace.WithBatchTimeout(2*time.Second),
			sdktrace.WithMaxExportBatchSize(512),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	otel.SetTracerProvider(tp)

	// W3C tracecontext plus baggage. The propagator is what carries the trace
	// across a service boundary; the Kafka path uses the same format so that a
	// consumer joins the producing trace rather than starting a new one.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	cfg.Logger.Info("tracing enabled",
		"service", cfg.ServiceName,
		"endpoint", cfg.Endpoint,
		"sample_ratio", cfg.SampleRatio,
	)

	return &Provider{
		tp:  tp,
		log: cfg.Logger,
		closer: func(ctx context.Context) error {
			// Flushing on shutdown matters: the spans for the last requests
			// before a deploy are exactly the ones someone will want to look at.
			return tp.Shutdown(ctx)
		},
	}, nil
}

// Shutdown flushes pending spans.
func (p *Provider) Shutdown(ctx context.Context) {
	if p == nil {
		return
	}
	if p.closer == nil {
		return
	}

	// A bounded flush, on a context that is not the cancelled one: the caller's
	// deadline has usually just expired when this runs.
	flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if err := p.closer(flushCtx); err != nil && !errors.Is(err, context.Canceled) {
		p.log.Warn("could not flush traces", "error", err.Error())
	}
}

// Tracer returns a tracer for the configured service.
func Tracer() trace.Tracer {
	return otel.Tracer("marketplace")
}

// EnvFromOS reads the shared tracing settings from the environment.
//
// It exists so that every service configures tracing identically. Seven services
// reading the same variables by hand is seven chances for one of them to
// disagree about the endpoint or the sampler.
func EnvFromOS(serviceName, version, env string) Config {
	return Config{
		ServiceName: serviceName,
		Version:     version,
		Env:         env,
		Endpoint:    os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		Enabled:     !strings.EqualFold(os.Getenv("OTEL_ENABLED"), "false"),
		SampleRatio: parseRatio(os.Getenv("OTEL_TRACES_SAMPLER_RATIO")),
		Insecure:    true,
	}
}

func parseRatio(raw string) float64 {
	switch strings.TrimSpace(raw) {
	case "":
		return 1
	case "0":
		return 0
	case "1":
		return 1
	}

	var f float64
	if _, err := fmt.Sscanf(raw, "%f", &f); err != nil {
		return 1
	}
	return f
}

// SpanAttributes returns the attributes every span in this platform carries.
func SpanAttributes(service, env string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("service.name", service),
		attribute.String("deployment.environment", env),
	}
}
