// Command inventory is the stock levels and the reservation lifecycle.
//
// The process lifecycle (signals, logging, probes) lives in pkg/bootstrap; this
// file is responsible only for wiring the service's own dependencies and
// running them until shutdown: the repository, the gRPC adapter that exposes
// it, and the background worker that gives expired holds back to the pool.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/marketplace/pkg/bootstrap"
	"github.com/marketplace/pkg/grpcx"
	"github.com/marketplace/pkg/postgres"

	inventorygrpc "github.com/marketplace/services/inventory/internal/adapters/grpc"
	"github.com/marketplace/services/inventory/internal/adapters/memory"
	pgadapter "github.com/marketplace/services/inventory/internal/adapters/postgres"
	"github.com/marketplace/services/inventory/internal/domain"
	"github.com/marketplace/services/inventory/migrations"
)

const (
	// defaultGRPCAddr matches INVENTORY_GRPC_ADDR in .env and the compose file.
	defaultGRPCAddr = ":9093"

	// expiryInterval is how often overdue holds are swept.
	//
	// Thirty seconds is a compromise between two costs that both fall on the
	// customer: sweeping more often buys nothing, because a hold that expires
	// between ticks is still returned long before any human notices, while
	// sweeping less often means a stock level shown on a product page can be
	// stale for minutes after an abandoned checkout. The sweep is O(holds) in
	// memory and follows the same "stay well under the TTL" rule a persistent
	// adapter would need.
	expiryInterval = 30 * time.Second
)

// demoStock is the seed applied in development so that the checkout demo works
// on a fresh container. The ids match the catalogue fixture.
var demoStock = []struct {
	productID string
	quantity  int64
}{
	{"p-1", 100},
	{"p-2", 250},
}

func main() {
	err := bootstrap.Run(bootstrap.Options{
		Service:           "inventory",
		EnvPrefix:         "INVENTORY",
		DefaultHealthAddr: ":9193",
	}, run)
	if err != nil {
		fmt.Fprintf(os.Stderr, "inventory: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, rt *bootstrap.Runtime) error {
	cfg := rt.Config

	grpcAddr := cfg.String("GRPC_ADDR", defaultGRPCAddr)
	ttl := cfg.Duration("RESERVATION_TTL", domain.DefaultReservationTTL)
	// Demo seeding is on in development only. In any other environment a
	// fabricated stock level would be indistinguishable from a real one, and
	// the first real order would be fulfilled from it.
	seedDemo := cfg.Bool("SEED_DEMO", rt.Env == "dev")

	// Checked once, after every value has been read, so a misconfigured service
	// reports all of its problems in one restart rather than one per restart.
	if err := cfg.Err(); err != nil {
		return err
	}
	if ttl <= 0 {
		return fmt.Errorf("inventory: INVENTORY_RESERVATION_TTL must be positive, got %s", ttl)
	}

	// ------------------------------------------------------------------ state
	//
	// PostgreSQL when a DSN is configured, in-memory otherwise. The fallback is
	// not laziness: it keeps the service runnable, and the whole reservation
	// protocol testable, without a database — which is what lets a test drive
	// the awkward paths, like a compensation arriving for a hold that never
	// existed, in milliseconds. Production always has a DSN, and the warning
	// below is loud enough that nobody mistakes the two.
	var repo domain.Repository

	dsn := cfg.String("DB_DSN", "")
	if dsn == "" {
		rt.Log.Warn("INVENTORY_DB_DSN is not set: running with in-memory storage, which loses every stock level and every hold on restart")

		repo = memory.New(memory.Options{DefaultTTL: ttl})
	} else {
		pool, err := postgres.Open(ctx, postgres.Config{
			DSN:     dsn,
			Service: "inventory",
			Logger:  rt.Log,
		})
		if err != nil {
			return err
		}
		defer pool.Close()

		if err := postgres.Migrate(ctx, pool, "inventory", migrations.FS, rt.Log); err != nil {
			return err
		}

		repo = pgadapter.NewWithOptions(pool, pgadapter.Options{DefaultTTL: ttl})

		rt.Health.Register("database", postgres.Health(pool))
	}

	if seedDemo {
		if err := seedDemoStock(ctx, repo, rt.Log); err != nil {
			return err
		}
	}

	// Readiness answers "should traffic be routed here?", so the check is a
	// real read of the path every request takes rather than a flag that is set
	// once. It is registered against the port, so it exercises whichever adapter
	// this process was started with.
	rt.Health.Register("repository", func(ctx context.Context) error {
		_, err := repo.GetStock(ctx, nil)
		return err
	})

	srv := grpcx.NewServer(grpcx.ServerConfig{Name: "inventory", Logger: rt.Log, Metrics: rt.Metrics})

	adapter, err := inventorygrpc.NewServer(inventorygrpc.Config{
		Repository: repo,
		Logger:     rt.Log,
		DefaultTTL: ttl,
	})
	if err != nil {
		return err
	}
	adapter.Register(srv)

	// The worker gets its own cancellable context rather than the service's, so
	// that it is stopped on the way out of this function even when grpcx.Serve
	// returns early (a port already in use, for instance) and the service
	// context is never cancelled.
	workerCtx, stopWorker := context.WithCancel(ctx)

	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		runExpiryWorker(workerCtx, repo, rt.Log, expiryInterval)
	}()

	rt.Log.Info("inventory ready",
		slog.String("grpc_addr", grpcAddr),
		slog.Duration("reservation_ttl", ttl),
		slog.Bool("postgres", dsn != ""),
		slog.Bool("seeded_demo_stock", seedDemo),
	)

	// A drain of zero selects grpcx's own default. The server is asked to stop
	// gracefully because a checkout that is mid-reserve must get a real answer
	// rather than a connection reset: an unknown outcome is what forces the
	// saga into a compensation it may not need.
	err = grpcx.Serve(ctx, srv, grpcAddr, rt.Log, 0)

	stopWorker()
	workers.Wait()

	return err
}

// seedDemoStock applies the development fixture.
//
// SetStock is an absolute write, which is exactly what a seed wants: running
// this twice leaves the same levels rather than doubling them.
func seedDemoStock(ctx context.Context, store domain.Repository, log *slog.Logger) error {
	for _, seed := range demoStock {
		item, err := store.SetStock(ctx, seed.productID, seed.quantity)
		if err != nil {
			return fmt.Errorf("inventory: seed stock for %s: %w", seed.productID, err)
		}
		log.Info("seeded demo stock",
			slog.String("product_id", item.ProductID),
			slog.Int64("quantity", item.Quantity),
		)
	}
	return nil
}

// runExpiryWorker releases overdue holds until ctx is cancelled.
//
// This is the safety net under the checkout saga. A saga that crashes, or whose
// process is killed mid-flight, never runs its compensation, and without this
// worker the stock it held would stay held forever — a slow leak that shows up
// as products that are mysteriously never in stock.
func runExpiryWorker(ctx context.Context, store domain.Repository, log *slog.Logger, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	log.Info("expiry worker started", slog.Duration("interval", every))

	for {
		select {
		case <-ctx.Done():
			// Returns before the store is discarded; the holds that were due
			// are released by their own deadline on the next start.
			log.Info("expiry worker stopped")
			return

		case now := <-ticker.C:
			expired, err := store.ExpireReservations(ctx, now)
			switch {
			case errors.Is(err, context.Canceled):
				log.Info("expiry worker stopped")
				return
			case err != nil:
				// A failed sweep is not worth stopping the process for: the
				// holds it did not release stay bounded by their own deadline,
				// and the next tick tries again.
				log.Error("expiry sweep failed", slog.String("error", err.Error()))
				continue
			}
			logExpiredReservations(ctx, log, expired)
		}
	}
}

// logExpiredReservations reports what a sweep gave back.
//
// Expiry is otherwise invisible: no request caused it and no customer will ask
// about it, so these lines are the only evidence that a hold existed at all.
// The reservation is logged individually as well as counted, because "which
// order was stuck" is the question asked during an incident, and a bare count
// cannot answer it.
func logExpiredReservations(ctx context.Context, log *slog.Logger, expired []domain.Reservation) {
	if len(expired) == 0 {
		return
	}

	var units int64
	for _, res := range expired {
		for _, line := range res.Lines {
			units += line.Quantity
		}
		log.InfoContext(ctx, "reservation expired",
			slog.String("reservation_id", res.ID),
			slog.String("order_id", res.OrderID),
			slog.Int("lines", len(res.Lines)),
			slog.Time("created_at", res.CreatedAt),
			slog.Time("expired_at", res.ExpiresAt),
		)
	}

	log.InfoContext(ctx, "expired holds released",
		slog.Int("reservations", len(expired)),
		slog.Int64("units", units),
	)
}
