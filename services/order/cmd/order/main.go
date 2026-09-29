// Command order is the checkout saga orchestrator.
//
// It is the only service that knows the shape of a checkout. Every other service
// exposes capabilities — hold stock, authorize a card — and this one decides the
// order they are used in, what happens when one of them fails, and how to undo
// the ones that already succeeded.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"github.com/marketplace/pkg/bootstrap"
	"github.com/marketplace/pkg/config"
	"github.com/marketplace/pkg/grpcx"
	"github.com/marketplace/pkg/kafka"
	"github.com/marketplace/pkg/postgres"
	orderv1 "github.com/marketplace/pkg/proto/order/v1"
	"github.com/marketplace/pkg/resilience"
	"github.com/marketplace/pkg/saga"

	"github.com/marketplace/services/order/internal/adapters/clients"
	grpcserver "github.com/marketplace/services/order/internal/adapters/grpcserver"
	"github.com/marketplace/services/order/internal/adapters/memory"
	"github.com/marketplace/services/order/internal/adapters/outbox"
	pgadapter "github.com/marketplace/services/order/internal/adapters/postgres"
	"github.com/marketplace/services/order/internal/app"
	"github.com/marketplace/services/order/internal/domain"
	"github.com/marketplace/services/order/migrations"
)

func main() {
	err := bootstrap.Run(bootstrap.Options{
		Service:           "order",
		EnvPrefix:         "ORDER",
		DefaultHealthAddr: ":9194",
	}, run)
	if err != nil {
		fmt.Fprintf(os.Stderr, "order: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, rt *bootstrap.Runtime) error {
	retry := retryPolicy(rt.Shared)

	// ---------------------------------------------------------------- clients
	//
	// One connection per dependency, created once and reused. A connection per
	// request would pay a TCP and HTTP/2 handshake every time and, worse, would
	// defeat the circuit breaker: the breaker's state only means anything if it
	// is shared across calls.
	deps := map[string]string{
		"catalog":   rt.Shared.String("CATALOG_GRPC_TARGET", "catalog:9092"),
		"inventory": rt.Shared.String("INVENTORY_GRPC_TARGET", "inventory:9093"),
		"payment":   rt.Shared.String("PAYMENT_GRPC_TARGET", "payment:9095"),
	}

	conns := make(map[string]*grpc.ClientConn, len(deps))
	for name, target := range deps {
		conn, err := grpcx.Dial(ctx, grpcx.ClientConfig{
			Name:    name,
			Target:  target,
			Logger:  rt.Log,
			Timeout: rt.Shared.Duration("GRPC_CALL_TIMEOUT", 5*time.Second),
			Retry:   retry,
			Breaker: resilience.NewCircuitBreaker(resilience.BreakerConfig{
				Name:             name,
				FailureThreshold: rt.Shared.Int("CIRCUIT_BREAKER_THRESHOLD", 5),
				OpenTimeout:      rt.Shared.Duration("CIRCUIT_BREAKER_TIMEOUT", 30*time.Second),
				// A caller's mistake must not trip the breaker: a rejected
				// request says nothing about whether the dependency is healthy.
				IsFailure: func(err error) bool {
					return resilience.RetryableByCode(err)
				},
			}),
		})
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		conns[name] = conn
	}

	// Every config value has now been read, so a malformed one is reported once
	// with the whole list rather than one restart at a time.
	if err := rt.Config.Err(); err != nil {
		return err
	}
	if err := rt.Shared.Err(); err != nil {
		return err
	}

	// ------------------------------------------------------------------ state
	//
	// PostgreSQL when a DSN is configured, in-memory otherwise. The fallback is
	// not laziness: it keeps the service runnable and the saga testable without
	// a database, which is what lets a unit test exercise the rollback path in
	// milliseconds. Production always has a DSN, and the warning below is loud
	// enough that nobody mistakes the two.
	var (
		orderStore domain.Repository
		sagaStore  saga.Store
		events     domain.EventPublisher
		pgStore    *pgadapter.Store
	)

	dsn := rt.Config.String("DB_DSN", "")
	if dsn == "" {
		rt.Log.Warn("ORDER_DB_DSN is not set: running with in-memory storage, which loses every order on restart")

		orderStore = memory.NewOrderStore()
		sagaStore = saga.NewMemoryStore()
	} else {
		pool, err := postgres.Open(ctx, postgres.Config{
			DSN:     dsn,
			Service: "order",
			Logger:  rt.Log,
		})
		if err != nil {
			return err
		}
		defer pool.Close()

		if err := postgres.Migrate(ctx, pool, "order", migrations.FS, rt.Log); err != nil {
			return err
		}

		pgStore = pgadapter.New(pool)
		orderStore = pgStore
		sagaStore = pgadapter.NewSagaStore(pool)
		events = pgStore

		rt.Health.Register("database", postgres.Health(pool))

		// The outbox is only meaningful with durable storage: it exists to make
		// a state change and its event commit together, which an in-memory store
		// cannot offer.
		poller, err := outbox.New(outbox.Config{
			Store:       pgStore,
			Producer:    mustProducer(rt),
			Logger:      rt.Log,
			Interval:    rt.Shared.Duration("OUTBOX_POLL_INTERVAL", time.Second),
			BatchSize:   rt.Shared.Int("OUTBOX_BATCH_SIZE", 100),
			MaxAttempts: rt.Shared.Int("OUTBOX_MAX_ATTEMPTS", 10),
		})
		if err != nil {
			return err
		}
		go func() {
			if err := poller.Run(ctx); err != nil {
				rt.Log.Error("outbox poller stopped", "error", err.Error())
			}
		}()
	}

	engine := saga.New(sagaStore, saga.WithLogger(rt.Log))

	checkout, err := app.NewCheckoutSaga(app.CheckoutConfig{
		Orders:         orderStore,
		Events:         events,
		Inventory:      clients.NewInventoryClient(conns["inventory"]),
		Payments:       clients.NewPaymentClient(conns["payment"]),
		Logger:         rt.Log,
		Retry:          retry,
		ReservationTTL: rt.Config.Duration("RESERVATION_TTL", 15*time.Minute),
	})
	if err != nil {
		return err
	}

	service, err := app.NewService(app.ServiceConfig{
		Orders:   orderStore,
		Catalog:  clients.NewCatalogClient(conns["catalog"]),
		Engine:   engine,
		Checkout: checkout,
		Logger:   rt.Log,
		// Saga outcomes are the domain metric that matters most: a compensated
		// checkout is the system working, a failed one is a page.
		Metrics: rt.Metrics,
	})
	if err != nil {
		return err
	}

	// -------------------------------------------------------- recovery worker
	//
	// A process that dies mid-saga leaves it in StatusRunning forever: nothing
	// is retrying it, and the stock it reserved stays held. This closes that gap.
	go recoverStuckSagas(ctx, rt, engine, checkout)

	rt.Health.Register("saga_engine", func(context.Context) error { return nil })

	// The outbox backlog is the metric that catches a stuck publisher before a
	// customer notices. A gauge rather than a counter because the useful
	// question is "how far behind are we now", not "how many ever".
	if pgStore != nil {
		go publishOutboxDepth(ctx, rt, pgStore.Pool())
	}

	// ----------------------------------------------------------------- server
	grpcServer, err := grpcserver.New(service)
	if err != nil {
		return err
	}

	addr := rt.Config.String("GRPC_ADDR", ":9094")
	srv := grpcx.NewServer(grpcx.ServerConfig{Name: "order", Logger: rt.Log, Metrics: rt.Metrics})
	orderv1.RegisterOrderServiceServer(srv, grpcServer)

	rt.Log.Info("order service ready",
		"grpc_addr", addr,
		"catalog", deps["catalog"],
		"inventory", deps["inventory"],
		"payment", deps["payment"],
	)

	return grpcx.Serve(ctx, srv, addr, rt.Log, 15*time.Second)
}

// recoverStuckSagas periodically drives abandoned sagas to a terminal state.
//
// One goroutine per instance, with no coordination between instances: the saga
// store's optimistic locking is what prevents two workers from running the same
// saga, and a version conflict is treated as "someone else has it" rather than a
// failure. That is why this needs no leader election.
func recoverStuckSagas(
	ctx context.Context,
	rt *bootstrap.Runtime,
	engine *saga.Engine,
	checkout *app.CheckoutSaga,
) {
	interval := rt.Config.Duration("RECOVERY_INTERVAL",
		rt.Shared.Duration("SAGA_RECOVERY_INTERVAL", 30*time.Second))
	stuckAfter := rt.Config.Duration("STUCK_AFTER",
		rt.Shared.Duration("SAGA_STUCK_AFTER", 2*time.Minute))

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	rt.Log.Info("saga recovery worker started",
		"interval", interval,
		"stuck_after", stuckAfter,
	)

	for {
		select {
		case <-ctx.Done():
			rt.Log.Info("saga recovery worker stopped")
			return

		case <-ticker.C:
			report, err := engine.Recover(ctx, stuckAfter, 50, func(name string) (*saga.Definition, bool) {
				if name != app.SagaName {
					// A saga whose definition is no longer deployed is skipped
					// rather than failed, so that retiring a flow does not fill
					// the alert channel with work nobody can perform.
					return nil, false
				}
				return checkout.Definition(), true
			})
			if err != nil {
				rt.Log.Error("saga recovery pass failed", "error", err.Error())
				continue
			}
			if report.Scanned > 0 {
				rt.Log.Info("saga recovery pass",
					"scanned", report.Scanned,
					"resumed", report.Resumed,
					"failed", report.Failed,
					"skipped", report.Skipped,
				)
			}
		}
	}
}

// publishOutboxDepth samples the outbox backlog onto a gauge.
func publishOutboxDepth(ctx context.Context, rt *bootstrap.Runtime, pool *pgxpool.Pool) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var pending int64
			if err := pool.QueryRow(ctx,
				`SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&pending); err != nil {
				continue
			}
			// A gauge that stops being updated is itself a signal, so a failure
			// here is left to the next tick rather than logged on every pass.
			rt.Metrics.SetState("outbox_pending", float64(pending))
		}
	}
}

// mustProducer connects the Kafka producer used by the outbox poller.
//
// The panic is deliberate: this is only called on the durable path, where an
// outbox with no way to publish would silently accumulate events forever. A
// service that starts and quietly never delivers anything is worse than one that
// refuses to start.
func mustProducer(rt *bootstrap.Runtime) *kafka.Producer {
	producer, err := kafka.NewProducer(kafka.Config{
		Brokers:     rt.Shared.StringSlice("KAFKA_BROKERS", []string{"kafka:9092"}),
		TopicPrefix: rt.Shared.String("KAFKA_TOPIC_PREFIX", "marketplace"),
		ClientID:    "order",
		Logger:      rt.Log,
	})
	if err != nil {
		panic(fmt.Sprintf("order: cannot create the outbox producer: %v", err))
	}
	return producer
}

// retryPolicy reads the shared resilience settings.
//
// The defaults are deliberately modest: four attempts over roughly a second is
// enough to ride out a rolling restart, and short enough that a genuinely broken
// dependency is reported quickly instead of holding a customer at a spinner.
func retryPolicy(shared *config.Loader) resilience.RetryPolicy {
	return resilience.RetryPolicy{
		MaxAttempts: shared.Int("RESILIENCE_RETRY_MAX_ATTEMPTS", 4),
		BaseDelay:   shared.Duration("RESILIENCE_RETRY_BASE_DELAY", 50*time.Millisecond),
		MaxDelay:    shared.Duration("RESILIENCE_RETRY_MAX_DELAY", 2*time.Second),
		Multiplier:  2,
		Jitter:      0.2,
	}
}
