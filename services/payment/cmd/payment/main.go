// Command payment is the payment provider adapter.
//
// It owns no business rules of its own beyond idempotency: it records what an
// external provider decided, and classifies declines as permanent or transient
// so that the checkout saga knows whether to retry or to compensate.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/marketplace/pkg/bootstrap"
	"github.com/marketplace/pkg/grpcx"
	"github.com/marketplace/pkg/postgres"
	paymentv1 "github.com/marketplace/pkg/proto/payment/v1"

	grpcserver "github.com/marketplace/services/payment/internal/adapters/grpc"
	"github.com/marketplace/services/payment/internal/adapters/memory"
	pgadapter "github.com/marketplace/services/payment/internal/adapters/postgres"
	"github.com/marketplace/services/payment/internal/adapters/provider"
	"github.com/marketplace/services/payment/internal/app"
	"github.com/marketplace/services/payment/internal/domain"
	"github.com/marketplace/services/payment/migrations"
)

func main() {
	err := bootstrap.Run(bootstrap.Options{
		Service:           "payment",
		EnvPrefix:         "PAYMENT",
		DefaultHealthAddr: ":9195",
	}, run)
	if err != nil {
		fmt.Fprintf(os.Stderr, "payment: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, rt *bootstrap.Runtime) error {
	// Every config value is read before any of them is acted on, so a malformed
	// one is reported once with the whole picture rather than one restart at a
	// time.
	dsn := rt.Config.String("DB_DSN", "")
	failureRate := rt.Config.Float64("FAILURE_RATE", 0.15)
	latency := time.Duration(rt.Config.Int("LATENCY_MS", 50)) * time.Millisecond
	grpcAddr := rt.Config.String("GRPC_ADDR", ":9095")

	if err := rt.Config.Err(); err != nil {
		return err
	}

	// ------------------------------------------------------------------ state
	//
	// PostgreSQL when a DSN is configured, in-memory otherwise.
	//
	// The fallback is not laziness. Idempotency is the one rule this service
	// owns, and being able to run a whole checkout without a database is what
	// makes the retry and compensation paths cheap to exercise; a demo that
	// needs a database to start is a demo nobody runs. Production always sets
	// PAYMENT_DB_DSN, and the warning below is loud enough that the two are
	// never confused.
	//
	// The difference matters more here than anywhere else: the in-memory store
	// keeps the idempotency claim in a map, so a restart forgets every key and a
	// retry that arrives afterwards charges the customer a second time.
	var (
		payments domain.Repository
		pgStore  *pgadapter.Store
	)

	if dsn == "" {
		rt.Log.Warn("PAYMENT_DB_DSN is not set: running with in-memory storage, " +
			"which forgets every idempotency key on restart and would let a retry charge twice")

		payments = memory.New()
	} else {
		pool, err := postgres.Open(ctx, postgres.Config{
			DSN:     dsn,
			Service: "payment",
			Logger:  rt.Log,
		})
		if err != nil {
			return err
		}
		defer pool.Close()

		// The service migrates its own schema at startup, under an advisory
		// lock that keeps two replicas from racing on the same DDL. The
		// alternative is a migration step in the deploy pipeline, which in
		// practice means a deploy that starts before its schema exists.
		if err := postgres.Migrate(ctx, pool, "payment", migrations.FS, rt.Log); err != nil {
			return err
		}

		pgStore = pgadapter.New(pool)
		payments = pgStore
	}

	// The failure rate is deliberately non-zero in development. The checkout
	// saga's compensation path is the part of this system most likely to be
	// wrong, and a demo where every payment succeeds never exercises it.
	providerImpl := provider.NewSimulated(provider.SimulatedConfig{
		FailureRate: failureRate,
		Latency:     latency,
	})

	service, err := app.NewService(app.ServiceConfig{
		Payments: payments,
		Provider: providerImpl,
		Logger:   rt.Log,
	})
	if err != nil {
		return err
	}

	// Readiness follows the store. With a DSN the probe is a real query, so an
	// instance whose database has gone away leaves the load balancer instead of
	// failing every authorization; the in-memory fallback has nothing to probe,
	// and its check exists so that the health endpoint keeps the same shape
	// either way.
	if pgStore != nil {
		rt.Health.Register("database", postgres.Health(pgStore.Pool()))
	} else {
		rt.Health.Register("repository", func(context.Context) error { return nil })
	}

	grpcServer, err := grpcserver.New(service)
	if err != nil {
		return err
	}

	srv := grpcx.NewServer(grpcx.ServerConfig{Name: "payment", Logger: rt.Log, Metrics: rt.Metrics})
	paymentv1.RegisterPaymentServiceServer(srv, grpcServer)

	rt.Log.Info("payment service ready",
		"grpc_addr", grpcAddr,
		"failure_rate", failureRate,
		"durable", pgStore != nil,
	)

	return grpcx.Serve(ctx, srv, grpcAddr, rt.Log, 10*time.Second)
}
