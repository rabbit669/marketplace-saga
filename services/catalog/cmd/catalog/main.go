// Command catalog is the product catalogue: what exists, what it costs, and
// whether it is on sale.
//
// The process lifecycle (signals, logging, probes) lives in pkg/bootstrap; this
// file is responsible only for wiring the service's own dependencies and
// running them until shutdown.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/marketplace/pkg/bootstrap"
	"github.com/marketplace/pkg/grpcx"
	"github.com/marketplace/pkg/money"
	"github.com/marketplace/pkg/postgres"
	catalogv1 "github.com/marketplace/pkg/proto/catalog/v1"

	cataloggrpc "github.com/marketplace/services/catalog/internal/adapters/grpc"
	"github.com/marketplace/services/catalog/internal/adapters/memory"
	pgadapter "github.com/marketplace/services/catalog/internal/adapters/postgres"
	"github.com/marketplace/services/catalog/internal/domain"
	"github.com/marketplace/services/catalog/migrations"
)

// DefaultGRPCAddr is where the catalogue listens when CATALOG_GRPC_ADDR is
// unset. It matches the target the other services dial in the compose file.
const DefaultGRPCAddr = ":9092"

func main() {
	err := bootstrap.Run(bootstrap.Options{
		Service:           "catalog",
		EnvPrefix:         "CATALOG",
		DefaultHealthAddr: ":9192",
	}, run)
	if err != nil {
		fmt.Fprintf(os.Stderr, "catalog: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, rt *bootstrap.Runtime) error {
	addr := rt.Config.String("GRPC_ADDR", DefaultGRPCAddr)
	// Demo data defaults to on in development and off everywhere else: seeded
	// products are convenient locally, and indistinguishable from real ones in a
	// staging catalogue somebody might be shown.
	seedDemo := rt.Config.Bool("SEED_DEMO", rt.Env == "dev")
	if err := rt.Config.Err(); err != nil {
		return fmt.Errorf("catalog: configuration: %w", err)
	}

	// The fixture is built before a store is chosen, so a hardcoded product that
	// fails validation is reported without a database connection having been
	// opened and closed first.
	var demo []domain.Product
	if seedDemo {
		products, err := demoProducts()
		if err != nil {
			// Hardcoded fixtures that fail validation are a programming error,
			// not a configuration problem: failing at startup beats serving a
			// catalogue that is quietly missing the products the demo expects.
			return fmt.Errorf("catalog: build demo products: %w", err)
		}
		demo = products
	}

	// ------------------------------------------------------------------ state
	//
	// PostgreSQL when a DSN is configured, in-memory otherwise. The fallback is
	// not laziness: it keeps the service runnable, and the gRPC surface and the
	// pricing contract testable, without a database — which is what lets a test
	// drive a real client against a real server in milliseconds. Production
	// always has a DSN, and the warning below is loud enough that nobody
	// mistakes the two.
	var (
		repo   domain.Repository
		total  int
		seeded int
	)

	dsn := rt.Config.String("DB_DSN", "")
	if dsn == "" {
		rt.Log.Warn("CATALOG_DB_DSN is not set: running with in-memory storage, which loses the whole catalogue on restart")

		store := memory.New()
		if err := seedDemoProducts(store.Seed, demo); err != nil {
			return err
		}
		repo = store
		total = store.Len()
		seeded = len(demo)
	} else {
		pool, err := postgres.Open(ctx, postgres.Config{
			DSN:     dsn,
			Service: "catalog",
			Logger:  rt.Log,
		})
		if err != nil {
			return err
		}
		defer pool.Close()

		if err := postgres.Migrate(ctx, pool, "catalog", migrations.FS, rt.Log); err != nil {
			return err
		}

		pgStore := pgadapter.New(pool)
		repo = pgStore

		// The fixture is applied only to an empty catalogue. Its ids and SKUs are
		// fixed and Create is not idempotent — deliberately, since it is the
		// create path a caller uses — so seeding a database that already holds
		// the demo products would fail on its own data and refuse to start.
		empty, err := pgStore.Count(ctx)
		if err != nil {
			return err
		}
		if empty == 0 {
			if err := seedDemoProducts(func(products ...domain.Product) error {
				return pgStore.Seed(ctx, products...)
			}, demo); err != nil {
				return err
			}
			seeded = len(demo)
		}
		if total, err = pgStore.Count(ctx); err != nil {
			return err
		}

		rt.Health.Register("database", postgres.Health(pool))
	}

	// The readiness check performs a real read rather than reporting a flag. A
	// store that exists but cannot serve a page is not ready, and a probe that
	// only asserted "the field is non-nil" would keep this process in the load
	// balancer while every request failed. It is registered against the port, so
	// it exercises whichever adapter this process was started with.
	rt.Health.Register("repository", func(ctx context.Context) error {
		if _, err := repo.List(ctx, domain.ListFilter{PageSize: 1}); err != nil {
			return fmt.Errorf("catalog repository is not usable: %w", err)
		}
		return nil
	})

	server, err := cataloggrpc.New(cataloggrpc.Config{
		Repository: repo,
		Logger:     rt.Log,
	})
	if err != nil {
		return fmt.Errorf("catalog: %w", err)
	}

	grpcServer := grpcx.NewServer(grpcx.ServerConfig{
		Name:   rt.Service,
		Logger: rt.Log,
	})
	catalogv1.RegisterCatalogServiceServer(grpcServer, server)

	rt.Log.Info("catalog service ready",
		slog.String("grpc_addr", addr),
		slog.Bool("postgres", dsn != ""),
		slog.Int("products", total),
		slog.Int("seeded_products", seeded),
	)

	// Serve returns once ctx is cancelled, after draining in-flight calls, so an
	// order that is being priced when the signal arrives still gets an answer.
	return grpcx.Serve(ctx, grpcServer, addr, rt.Log, 0)
}

// seedDemoProducts applies the fixture through whichever store was chosen.
//
// The stores are handed the write as a function rather than being reached into:
// the two adapters take the same products and differ only in whether they need a
// context, and this keeps that difference in one place instead of in the startup
// path. A seed that cannot be applied stops the service, because a half-seeded
// catalogue produces a demo that fails in the middle of a checkout.
func seedDemoProducts(seed func(...domain.Product) error, products []domain.Product) error {
	if len(products) == 0 {
		return nil
	}
	if err := seed(products...); err != nil {
		return fmt.Errorf("catalog: seed demo products: %w", err)
	}
	return nil
}

// demoProducts builds the local fixture catalogue.
//
// The products go through domain.NewProduct rather than being assembled field by
// field, so the demo data obeys exactly the rules a real create request does. A
// fixture that only works because it bypasses validation is a fixture that hides
// the bug it was written to exercise.
func demoProducts() ([]domain.Product, error) {
	specs := []struct {
		// id is fixed rather than generated, and matches the inventory service's
		// seeded stock. Demo fixtures exist to make a flow demonstrable, and a
		// flow where the catalogue's product ids do not exist in the stock ledger
		// is not demonstrable — it fails at the first reservation.
		id          string
		sku         string
		name        string
		description string
		category    string
		priceMinor  int64
		attributes  map[string]string
		// active is false for one fixture, so that ListProducts with
		// only_active set and the sale-state flag both have something to
		// demonstrate as soon as the service is started.
		active bool
	}{
		{
			id:          "p-1",
			sku:         "KETTLE-01",
			name:        "Stainless Steel Kettle",
			description: "1.7 litre brushed steel kettle with a removable limescale filter.",
			category:    "kitchen",
			priceMinor:  3999,
			attributes:  map[string]string{"colour": "steel", "capacity": "1.7l"},
			active:      true,
		},
		{
			id:          "p-2",
			sku:         "MUG-02",
			name:        "Stoneware Mug, Set of 4",
			description: "Four 350 ml stoneware mugs, dishwasher and microwave safe.",
			category:    "kitchen",
			priceMinor:  2450,
			attributes:  map[string]string{"colour": "slate", "pieces": "4"},
			active:      true,
		},
		{
			id:          "p-3",
			sku:         "TEE-03",
			name:        "Organic Cotton T-Shirt",
			description: "Mid-weight organic cotton t-shirt with a reinforced collar.",
			category:    "apparel",
			priceMinor:  1990,
			attributes:  map[string]string{"colour": "navy", "size": "M", "material": "organic cotton"},
			active:      true,
		},
		{
			id:          "p-4",
			sku:         "SOCK-04",
			name:        "Merino Hiking Socks",
			description: "Cushioned merino wool socks with a reinforced heel and toe.",
			category:    "apparel",
			priceMinor:  1450,
			attributes:  map[string]string{"colour": "charcoal", "size": "39-42"},
			active:      true,
		},
		{
			id:          "p-5",
			sku:         "LAMP-05",
			name:        "Adjustable Desk Lamp",
			description: "Warm-white LED desk lamp with a weighted base and a USB-C port.",
			category:    "lighting",
			priceMinor:  5495,
			attributes:  map[string]string{"colour": "black", "power": "usb-c"},
			active:      true,
		},
		{
			id:          "p-6",
			sku:         "NOTE-06",
			name:        "Hardcover Notebook",
			description: "A5 hardcover notebook, 192 dot-grid pages, elastic closure. Superseded by NOTE-07.",
			category:    "stationery",
			priceMinor:  1250,
			attributes:  map[string]string{"colour": "forest", "pages": "192"},
			active:      false,
		},
	}

	now := time.Now()
	products := make([]domain.Product, 0, len(specs))
	for _, spec := range specs {
		product, err := domain.NewProduct(domain.NewProductInput{
			ID:          spec.id,
			SKU:         spec.sku,
			Name:        spec.name,
			Description: spec.description,
			Price:       money.MustNew(spec.priceMinor, money.USD),
			CategoryID:  spec.category,
			Attributes:  spec.attributes,
			CreatedAt:   now,
		})
		if err != nil {
			return nil, fmt.Errorf("demo product %s: %w", spec.sku, err)
		}
		// NewProduct always yields an on-sale product, because that is what a
		// create request means. Seeding is the one place that legitimately wants
		// an off-sale row, so the state is set here, explicitly, rather than by
		// weakening the constructor.
		product.IsActive = spec.active
		products = append(products, *product)
	}
	return products, nil
}
