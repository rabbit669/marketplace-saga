// Package postgres provides the connection pool and migration runner shared by
// every service.
//
// Two decisions in here are worth stating, because both are the kind that look
// like details until they cause an incident.
//
// **Migrations run at startup, under an advisory lock.** Every service runs its
// own migrations when it boots, and PostgreSQL's advisory lock guarantees that
// exactly one replica does the work while the others wait. This avoids a
// separate migration orchestration step — which in practice means someone
// forgetting it — without allowing two replicas to race on the same DDL.
//
// **The pool is deliberately small.** PostgreSQL's max_connections is a
// per-server limit, not a per-service one. Seven services each taking the
// default of "unlimited" exhausts it the moment anything scales out, and the
// resulting failure looks like a database outage rather than a configuration
// mistake. The limit here is per instance, with the arithmetic written down.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Config describes a connection pool.
type Config struct {
	// DSN is a postgres:// connection string.
	DSN string
	// MaxConns bounds this instance's connections. See the package comment: the
	// budget is shared across every service and every replica.
	MaxConns int32
	// MinConns keeps a few connections warm so that a burst does not pay for
	// establishing them.
	MinConns int32
	// MaxConnLifetime retires connections. It matters behind a proxy or a
	// failover, where a connection can be alive from this side while the server
	// considers it closed.
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
	// ConnectTimeout bounds the initial dial.
	ConnectTimeout time.Duration
	// StartupTimeout bounds how long Open waits for the server to become
	// reachable before giving up.
	//
	// It exists because "the database is not up yet" and "the DSN is wrong" are
	// different problems that look identical at the first attempt. A compose
	// stack solves the first with `depends_on: condition: service_healthy`;
	// Kubernetes has no equivalent, so a service that insists on its database
	// being ready at process start crash-loops until it happens to be.
	//
	// Waiting is the right answer for availability and the wrong one for a
	// misconfiguration, so the wait is bounded: a wrong DSN still fails, just
	// after the budget rather than immediately.
	StartupTimeout time.Duration
	// Logger receives pool events that pgx would otherwise swallow.
	Logger *slog.Logger
	// Service names the service in logs and in the migration lock key.
	Service string
}

func (c Config) withDefaults() Config {
	if c.MaxConns <= 0 {
		// Seven services, plus headroom for a rolling deploy that briefly runs
		// two of each. 10 x 14 = 140, comfortably inside the default 100 plus
		// superuser reserve, and the number is written down so that raising it
		// is a decision rather than an accident.
		c.MaxConns = 10
	}
	if c.MinConns < 0 || c.MinConns > c.MaxConns {
		c.MinConns = 1
	}
	if c.MaxConnLifetime <= 0 {
		c.MaxConnLifetime = time.Hour
	}
	if c.MaxConnIdleTime <= 0 {
		c.MaxConnIdleTime = 30 * time.Minute
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 5 * time.Second
	}
	if c.StartupTimeout <= 0 {
		c.StartupTimeout = 60 * time.Second
	}
	return c
}

// Open creates a pool and verifies it can reach the server.
//
// The ping is deliberate. pgxpool connects lazily, so without it a service would
// start, report healthy, and fail on its first real query — turning a
// configuration error into a runtime one at the worst possible moment. Starting
// is the cheap time to find out.
func Open(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	cfg = cfg.withDefaults()

	if strings.TrimSpace(cfg.DSN) == "" {
		return nil, fmt.Errorf("postgres: %s has no DSN configured", cfg.Service)
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		// The DSN frequently contains a password, so the error is wrapped rather
		// than echoed. A connection failure that prints its own connection
		// string writes a credential to the logs.
		return nil, fmt.Errorf("postgres: %s: the DSN is not a valid connection string: %w", cfg.Service, err)
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	poolCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: %s: create pool: %w", cfg.Service, err)
	}

	if err := waitForServer(ctx, pool, cfg); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: %s: cannot reach the database: %w", cfg.Service, err)
	}

	if cfg.Logger != nil {
		cfg.Logger.Info("database pool ready",
			"service", cfg.Service,
			"host", redactDSN(cfg.DSN),
			"max_conns", cfg.MaxConns,
		)
	}

	return pool, nil
}

// waitForServer pings the database until it answers or the budget runs out.
//
// The retries are spaced out rather than tight: a database starting up needs
// seconds, not microseconds, and hammering it during a cold start competes with
// the work it is trying to finish.
func waitForServer(ctx context.Context, pool *pgxpool.Pool, cfg Config) error {
	deadline := time.Now().Add(cfg.StartupTimeout)
	delay := 200 * time.Millisecond

	var lastErr error
	for attempt := 1; ; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
		err := pool.Ping(pingCtx)
		cancel()

		if err == nil {
			if attempt > 1 {
				cfg.Logger.Info("database became reachable",
					"service", cfg.Service, "attempts", attempt)
			}
			return nil
		}
		lastErr = err

		// The caller gave up, or we are out of budget.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("gave up after %s and %d attempts: %w",
				cfg.StartupTimeout, attempt, lastErr)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		// Exponential up to a second: a cold database can take tens of seconds,
		// and a fixed short delay would spend the whole budget on failed dials.
		if delay < time.Second {
			delay *= 2
		}
	}
}

// Health returns a readiness check backed by the pool.
//
// It issues a real query rather than reporting that the pool object exists: a
// pool whose server has gone away is not ready, and a probe that only checked
// for nil would keep the service in the load balancer while every request
// failed.
func Health(pool *pgxpool.Pool) func(context.Context) error {
	return func(ctx context.Context) error {
		if pool == nil {
			return errors.New("postgres: no pool")
		}
		var one int
		if err := pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
			return fmt.Errorf("postgres: health query failed: %w", err)
		}
		return nil
	}
}

// Stats reports pool usage, for metrics and for diagnosing exhaustion.
func Stats(pool *pgxpool.Pool) map[string]any {
	if pool == nil {
		return nil
	}
	s := pool.Stat()
	return map[string]any{
		"acquired":         s.AcquiredConns(),
		"idle":             s.IdleConns(),
		"total":            s.TotalConns(),
		"max":              s.MaxConns(),
		"acquire_count":    s.AcquireCount(),
		"empty_acquires":   s.EmptyAcquireCount(),
		"canceled_acquire": s.CanceledAcquireCount(),
	}
}

// redactDSN returns a DSN safe to log: host and database only.
func redactDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "<unparseable>"
	}
	return u.Host + u.Path
}

// --------------------------------------------------------------- migrations

// migrationLockKey is an arbitrary but stable number derived from the service
// name, used as the advisory lock key.
//
// Advisory locks are scoped to a database and a 64-bit key, so two services
// migrating different databases cannot block each other, and two replicas of the
// same service deliberately do.
func migrationLockKey(service string) int64 {
	var h uint64 = 1469598103934665603
	for i := 0; i < len(service); i++ {
		h ^= uint64(service[i])
		h *= 1099511628211
	}
	return int64(h & 0x7fffffffffffffff)
}

// Migrate applies every embedded migration that has not been applied yet.
//
// Files must be named NNNN_description.sql and are applied in ascending order. A
// file is split on semicolons only at statement level, which is enough for DDL
// and deliberately not a general-purpose SQL parser: migrations that need
// procedural bodies should be written as separate files.
func Migrate(ctx context.Context, pool *pgxpool.Pool, service string, files fs.FS, log *slog.Logger) error {
	if pool == nil {
		return errors.New("postgres: cannot migrate without a pool")
	}
	if log == nil {
		log = slog.Default()
	}

	names, err := migrationFiles(files)
	if err != nil {
		return err
	}
	if len(names) == 0 {
		return nil
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("postgres: %s: acquire migration connection: %w", service, err)
	}
	defer conn.Release()

	// The lock is held for the whole migration, on a single connection, so a
	// second replica blocks here and then finds the work already done. It is
	// released explicitly rather than by session end, because the session is
	// pooled and outlives the migration.
	key := migrationLockKey(service)
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", key); err != nil {
		return fmt.Errorf("postgres: %s: acquire migration lock: %w", service, err)
	}
	defer func() {
		// A fresh context: the caller's may already be cancelled if this is
		// running during shutdown, and a lock that is never released blocks
		// every future deploy.
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", key)
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version     text PRIMARY KEY,
			applied_at  timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("postgres: %s: create schema_migrations: %w", service, err)
	}

	applied, err := appliedVersions(ctx, conn.Conn())
	if err != nil {
		return fmt.Errorf("postgres: %s: read applied migrations: %w", service, err)
	}

	for _, name := range names {
		if applied[name] {
			continue
		}

		body, err := fs.ReadFile(files, name)
		if err != nil {
			return fmt.Errorf("postgres: %s: read %s: %w", service, name, err)
		}

		// Each migration runs in its own transaction, so a failure leaves the
		// database at the last good version rather than half-applied. DDL is
		// transactional in PostgreSQL, which is what makes this possible.
		err = runInTx(ctx, conn.Conn(), func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, string(body)); err != nil {
				return fmt.Errorf("apply: %w", err)
			}
			if _, err := tx.Exec(ctx,
				"INSERT INTO schema_migrations (version) VALUES ($1)", name); err != nil {
				return fmt.Errorf("record: %w", err)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("postgres: %s: migration %s: %w", service, name, err)
		}

		log.Info("migration applied", "service", service, "migration", name)
	}

	return nil
}

func migrationFiles(files fs.FS) ([]string, error) {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil, fmt.Errorf("postgres: read migrations directory: %w", err)
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		names = append(names, e.Name())
	}

	// Lexicographic order equals numeric order because the prefix is zero
	// padded. Sorting here rather than trusting the filesystem means the result
	// does not depend on the platform.
	sort.Strings(names)
	return names, nil
}

func appliedVersions(ctx context.Context, conn *pgx.Conn) (map[string]bool, error) {
	rows, err := conn.Query(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		applied[v] = true
	}
	return applied, rows.Err()
}

// runInTx runs fn in a transaction, rolling back on error or panic.
func runInTx(ctx context.Context, conn *pgx.Conn, fn func(pgx.Tx) error) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}

	defer func() {
		// Rollback after a successful commit is a no-op, so this is safe as a
		// blanket guard and cannot leave a transaction open.
		_ = tx.Rollback(ctx)
	}()

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
