// Command auth is the identity service: users, credentials and token issuance.
//
// It is the only service that sees a password, and the only one that signs a
// token. Every other service treats a token as proof of identity, which keeps
// the consequences of a logging mistake confined to one place.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/marketplace/pkg/bootstrap"
	"github.com/marketplace/pkg/grpcx"
	"github.com/marketplace/pkg/jwtx"
	"github.com/marketplace/pkg/postgres"
	authv1 "github.com/marketplace/pkg/proto/auth/v1"

	grpcserver "github.com/marketplace/services/auth/internal/adapters/grpc"
	"github.com/marketplace/services/auth/internal/adapters/memory"
	pgadapter "github.com/marketplace/services/auth/internal/adapters/postgres"
	"github.com/marketplace/services/auth/internal/app"
	"github.com/marketplace/services/auth/internal/domain"
	"github.com/marketplace/services/auth/migrations"
)

func main() {
	err := bootstrap.Run(bootstrap.Options{
		Service:           "auth",
		EnvPrefix:         "AUTH",
		DefaultHealthAddr: ":9191",
	}, run)
	if err != nil {
		fmt.Fprintf(os.Stderr, "auth: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, rt *bootstrap.Runtime) error {
	// JWT settings are read unprefixed: the gateway verifies tokens this service
	// signs, so a prefix mismatch would let the two disagree about the issuer.
	jwtCfg := jwtx.Config{
		Secret: rt.Shared.String("JWT_SECRET", ""),
		Issuer: rt.Shared.String("JWT_ISSUER", "marketplace"),
		TTL:    rt.Shared.Duration("JWT_ACCESS_TTL", 15*time.Minute),
	}

	// Every config value is read before any of them is acted on, so a malformed
	// one is reported once with the whole picture rather than one restart at a
	// time.
	dsn := rt.Config.String("DB_DSN", "")
	refreshTTL := rt.Shared.Duration("JWT_REFRESH_TTL", 30*24*time.Hour)
	grpcAddr := rt.Config.String("GRPC_ADDR", ":9091")

	if err := rt.Config.Err(); err != nil {
		return err
	}
	if err := rt.Shared.Err(); err != nil {
		return err
	}

	// ------------------------------------------------------------------ state
	//
	// PostgreSQL when a DSN is configured, in-memory otherwise.
	//
	// The fallback is not laziness: it keeps the service runnable and the login
	// path testable without a database. Production always sets AUTH_DB_DSN, and
	// the warning below is loud enough that the two are never confused.
	//
	// Nothing is more durable than an account, though, which is why this
	// particular fallback is the one that matters most: on the in-memory store
	// every restart erases every user, and everyone who registered since the
	// last deploy finds their credentials simply gone.
	var (
		users   domain.Repository
		pgStore *pgadapter.Store
	)

	if dsn == "" {
		rt.Log.Warn("AUTH_DB_DSN is not set: running with in-memory storage, " +
			"which erases every account and every session on restart")

		users = memory.New()
	} else {
		pool, err := postgres.Open(ctx, postgres.Config{
			DSN:     dsn,
			Service: "auth",
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
		if err := postgres.Migrate(ctx, pool, "auth", migrations.FS, rt.Log); err != nil {
			return err
		}

		pgStore = pgadapter.New(pool)
		users = pgStore
	}

	service, err := app.New(app.Config{
		Users:      users,
		JWT:        jwtCfg,
		Logger:     rt.Log,
		RefreshTTL: refreshTTL,
	})
	if err != nil {
		return fmt.Errorf("auth: %w (is JWT_SECRET set to at least %d bytes?)", err, jwtx.MinSecretLength)
	}

	// Readiness follows the store. With a DSN the probe is a real query, so an
	// instance whose database has gone away leaves the load balancer instead of
	// failing every login; the in-memory fallback has nothing to probe, and its
	// check exists so that the health endpoint keeps the same shape either way.
	if pgStore != nil {
		rt.Health.Register("database", postgres.Health(pgStore.Pool()))
	} else {
		rt.Health.Register("repository", func(context.Context) error { return nil })
	}

	grpcServer, err := grpcserver.New(service)
	if err != nil {
		return err
	}

	srv := grpcx.NewServer(grpcx.ServerConfig{Name: "auth", Logger: rt.Log, Metrics: rt.Metrics})
	authv1.RegisterAuthServiceServer(srv, grpcServer)

	rt.Log.Info("auth service ready",
		"grpc_addr", grpcAddr,
		"refresh_ttl", refreshTTL,
		"durable", pgStore != nil,
	)

	return grpcx.Serve(ctx, srv, grpcAddr, rt.Log, 10*time.Second)
}
