// Package bootstrap gives every service the same startup and shutdown path.
//
// Seven services that each hand-roll their own signal handling, logger setup
// and probe server will drift apart, and the differences will be discovered
// during an incident. Instead each main is a short function that hands
// bootstrap a callback, and the lifecycle is defined once:
//
//   - parse -version and -config flags
//   - optionally load .env (local development only; real environments win)
//   - build the structured logger
//   - start the health/readiness listener
//   - run the service until SIGINT/SIGTERM
//   - give the service a bounded window to shut down cleanly
package bootstrap

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/marketplace/pkg/config"
	"github.com/marketplace/pkg/health"
	"github.com/marketplace/pkg/logger"
	"github.com/marketplace/pkg/metrics"
	"github.com/marketplace/pkg/telemetry"
	"github.com/marketplace/pkg/version"
)

// DefaultShutdownTimeout bounds how long in-flight work may finish after a
// termination signal. Kubernetes sends SIGKILL after its own grace period, so
// exceeding this only produces an uglier death, never more drained requests.
const DefaultShutdownTimeout = 15 * time.Second

// Options describes the invariant part of a service's identity.
type Options struct {
	// Service is the service name, e.g. "order". Used in logs and probes.
	Service string
	// EnvPrefix scopes environment lookups, e.g. "ORDER" -> ORDER_GRPC_ADDR.
	EnvPrefix string
	// DefaultHealthAddr is used when <PREFIX>_HEALTH_ADDR is unset.
	DefaultHealthAddr string
	// ShutdownTimeout overrides DefaultShutdownTimeout when positive.
	ShutdownTimeout time.Duration
}

// Runtime is the dependency bundle handed to a service's Main function.
type Runtime struct {
	Service string
	Env     string
	Log     *slog.Logger
	Health  *health.Checker
	// Config reads the service's own environment variables. Parse errors are
	// collected, not fatal, until Err is checked.
	Config *config.Loader
	// Metrics is this service's Prometheus registry, served on the probe port.
	Metrics *metrics.Metrics
	// Telemetry is the tracer provider. It is shut down after the service stops,
	// so that spans for the requests that were in flight are flushed rather than
	// lost with the process.
	Telemetry *telemetry.Provider
	// Shared reads unprefixed environment variables — settings that describe the
	// platform rather than the service, such as retry budgets and the OTLP
	// endpoint. Every service names them the same way so that changing one value
	// changes every service's behaviour together.
	Shared *config.Loader
}

// Main is the service entry point. It is expected to block until ctx is
// cancelled and then return; returning an error aborts the process with a
// non-zero exit status.
type Main func(ctx context.Context, rt *Runtime) error

// Run executes the standard service lifecycle around main.
func Run(opts Options, main Main) error {
	showVersion := flag.Bool("version", false, "print build information and exit")
	envFile := flag.String("env-file", "", "load environment variables from this file if it exists")
	healthCheck := flag.Bool("health-check", false, "probe the local readiness endpoint and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("%s %s\n", opts.Service, version.String())
		return nil
	}

	if *envFile == "" {
		// Convenience for local runs; missing file is not an error.
		*envFile = ".env"
	}
	if err := config.LoadDotEnv(*envFile); err != nil {
		return fmt.Errorf("load env file: %w", err)
	}

	// Container images are distroless and have no shell, so the image cannot
	// ship a curl or wget based health check. The binary probes itself instead:
	// exit status is the health signal, and the readiness body is echoed to
	// stderr where `docker inspect` and the orchestrator can see it.
	if *healthCheck {
		return runHealthCheck(opts)
	}

	loader := config.New(opts.EnvPrefix)
	shared := config.New("")

	env := loader.String("ENV", loader.String("APP_ENV", "dev"))
	log := logger.New(logger.Config{
		Level:   loader.String("LOG_LEVEL", "info"),
		Format:  logger.Format(loader.String("LOG_FORMAT", "json")),
		Service: opts.Service,
		Version: version.Short(),
		Env:     env,
	})

	healthAddr := loader.String("HEALTH_ADDR", opts.DefaultHealthAddr)

	// Signals are wired before anything else starts so that a SIGTERM arriving
	// during startup is not lost.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ctx = logger.WithLogger(ctx, log)

	// Tracing is initialised before the service starts so that the very first
	// request is instrumented. A failure to reach the collector is logged and
	// not fatal: losing traces is a visibility problem, while refusing to boot
	// is an outage.
	tracing, terr := telemetry.New(ctx, telemetry.Config{
		ServiceName: opts.Service,
		Version:     version.Short(),
		Env:         env,
		// Shared, unprefixed: tracing is a platform concern and every service
		// names it the same way.
		Endpoint:    shared.String("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		Enabled:     shared.Bool("OTEL_ENABLED", false),
		SampleRatio: shared.Float64("OTEL_TRACES_SAMPLER_RATIO", 1),
		Insecure:    true,
		Logger:      log,
	})
	if terr != nil {
		log.Warn("tracing could not be initialised", slog.String("error", terr.Error()))
	}
	if tracing != nil {
		defer tracing.Shutdown(context.WithoutCancel(ctx))
	}

	rt := &Runtime{
		Service:   opts.Service,
		Env:       env,
		Log:       log,
		Health:    health.New(),
		Config:    loader,
		Shared:    shared,
		Telemetry: tracing,
		Metrics:   metrics.New(opts.Service),
	}

	// service, version and env are already attached to every record by the
	// logger's handler. Repeating them here would emit duplicate JSON keys,
	// which strict log processors reject and humans find confusing.
	log.Info("starting service",
		slog.String("health_addr", healthAddr),
	)

	probeSrv := health.NewServer(healthAddr, opts.Service, rt.Health, log)
	if rt.Metrics != nil {
		// /metrics lives on the probe port rather than a second listener: one
		// port to scrape, one port to health-check, and no way for a service to
		// be up but silently unscrapeable.
		probeSrv = probeSrv.WithRoutes(map[string]http.Handler{
			"GET /metrics": rt.Metrics.Handler(),
		})
	}

	probeErr := make(chan error, 1)
	if probeSrv != nil {
		go func() { probeErr <- probeSrv.Run(ctx) }()
	}

	mainErr := make(chan error, 1)
	go func() { mainErr <- main(ctx, rt) }()

	timeout := opts.ShutdownTimeout
	if timeout <= 0 {
		timeout = DefaultShutdownTimeout
	}

	select {
	case err := <-mainErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error("service failed", slog.String("error", err.Error()))
			stop()
			drainProbe(probeErr, timeout)
			return err
		}
		stop()
		drainProbe(probeErr, timeout)
		log.Info("service stopped")
		return nil

	case err := <-probeErr:
		if err != nil {
			log.Error("health server failed", slog.String("error", err.Error()))
			stop()
			return err
		}
		// Probe server exited cleanly on its own; keep serving traffic.
		return <-mainErr

	case <-ctx.Done():
		log.Info("shutdown signal received", slog.Duration("grace", timeout))
	}

	// A second signal during draining means "stop now": operators expect
	// Ctrl-C twice to be an immediate exit.
	stop()

	drainProbe(probeErr, timeout)

	select {
	case err := <-mainErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error("service failed during shutdown", slog.String("error", err.Error()))
			return err
		}
	case <-time.After(timeout):
		log.Warn("graceful shutdown timed out", slog.Duration("timeout", timeout))
	}

	log.Info("service stopped")
	return nil
}

// drainProbe waits briefly for the health server to finish, so that log output
// stays ordered and the port is released before the process exits.
func drainProbe(errCh <-chan error, timeout time.Duration) {
	select {
	case err := <-errCh:
		if err != nil {
			fmt.Fprintf(os.Stderr, "health server: %v\n", err)
		}
	case <-time.After(timeout):
	}
}

// healthCheckTimeout bounds a single probe. It is deliberately shorter than the
// healthcheck timeout configured in the orchestrator so that the process, not
// the runtime, reports the failure and the reason is visible in the logs.
const healthCheckTimeout = 3 * time.Second

// runHealthCheck queries this process's own /readyz endpoint and returns a
// non-nil error when the service is not ready, which becomes a non-zero exit
// status. The response body is written to stderr so that the failing checks are
// readable in `docker inspect --format '{{json .State.Health}}'`.
func runHealthCheck(opts Options) error {
	loader := config.New(opts.EnvPrefix)

	addr := loader.String("HEALTH_ADDR", opts.DefaultHealthAddr)
	if addr == "" {
		return errors.New("health check requested but no health address is configured")
	}

	// Listen addresses are host:port; ":9191" means every interface, so the
	// probe must dial loopback explicitly.
	host := addr
	if strings.HasPrefix(host, ":") {
		host = "127.0.0.1" + host
	}
	url := "http://" + host + "/readyz"

	ctx, cancel := context.WithTimeout(context.Background(), healthCheckTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("health check: build request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("health check: %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "%s: %s\n", url, strings.TrimSpace(string(body)))
		return fmt.Errorf("health check: %s returned %d", url, resp.StatusCode)
	}
	return nil
}
