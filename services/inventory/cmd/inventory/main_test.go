package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marketplace/pkg/bootstrap"
	"github.com/marketplace/pkg/config"
	"github.com/marketplace/pkg/health"

	"github.com/marketplace/services/inventory/internal/adapters/memory"
	"github.com/marketplace/services/inventory/internal/domain"
)

// These tests cover the wiring the rest of the service cannot see: the seed
// fixture, the sweep that gives expired holds back, and the startup checks. The
// rules themselves are tested in `domain`, and the repository in
// `adapters/memory`.

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// testClock makes the expiry worker's deadline a matter of arithmetic.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// waitFor polls until cond holds, so a test never has to guess how long a
// background goroutine takes to notice something.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

func TestSeedDemoStock(t *testing.T) {
	ctx := context.Background()
	store := memory.New(memory.Options{})

	if err := seedDemoStock(ctx, store, discardLogger()); err != nil {
		t.Fatalf("seedDemoStock: %v", err)
	}

	// The checkout demo depends on these exact ids and levels, because the
	// catalogue fixture and the demo script both name them.
	want := map[string]int64{"p-1": 100, "p-2": 250}
	for productID, quantity := range want {
		items, err := store.GetStock(ctx, []string{productID})
		if err != nil {
			t.Fatalf("GetStock(%s): %v", productID, err)
		}
		if len(items) != 1 {
			t.Fatalf("%s was not seeded", productID)
		}
		if items[0].Quantity != quantity {
			t.Errorf("%s quantity = %d, want %d", productID, items[0].Quantity, quantity)
		}
	}

	// Seeding is an absolute write, so a second pass must not double the levels.
	if err := seedDemoStock(ctx, store, discardLogger()); err != nil {
		t.Fatalf("second seedDemoStock: %v", err)
	}
	items, _ := store.GetStock(ctx, []string{"p-1"})
	if items[0].Quantity != 100 {
		t.Fatalf("p-1 quantity = %d after seeding twice, want 100", items[0].Quantity)
	}
}

func TestSeedDemoStockPropagatesFailures(t *testing.T) {
	// A seed that cannot be applied must stop the service: a half-seeded
	// catalogue produces a demo that fails in the middle of checkout, which is
	// far more confusing than a container that refuses to start.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := seedDemoStock(ctx, memory.New(memory.Options{}), discardLogger()); err == nil {
		t.Fatal("seeding with a cancelled context reported success")
	}
}

func TestExpiryWorkerReleasesAndLogsOverdueHolds(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	store := memory.New(memory.Options{Clock: clock.Now, DefaultTTL: time.Minute})

	if _, err := store.SetStock(ctx, "p-1", 10); err != nil {
		t.Fatalf("SetStock: %v", err)
	}
	if _, err := store.Reserve(ctx, domain.ReserveRequest{
		ReservationID: "order-1",
		OrderID:       "order-1",
		Lines:         []domain.ReservationLine{{ProductID: "p-1", Quantity: 4}},
		TTL:           time.Minute,
	}); err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	// The hold is overdue before the worker even starts: the worker notices
	// deadlines, it does not create them.
	clock.Advance(2 * time.Minute)

	logs := &syncBuffer{}
	log := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	workerCtx, stopWorker := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		runExpiryWorker(workerCtx, store, log, 5*time.Millisecond)
	}()

	waitFor(t, 3*time.Second, "the overdue hold to be released", func() bool {
		items, err := store.GetStock(ctx, []string{"p-1"})
		return err == nil && len(items) == 1 && items[0].Reserved == 0
	})

	stopWorker()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("the expiry worker did not stop when its context was cancelled")
	}

	output := logs.String()
	// Expiry is invisible otherwise: no request caused it and no customer will
	// ask about it, so the log line is the only evidence a hold was ever frozen.
	for _, want := range []string{"reservation expired", "order-1", "expired holds released", "expiry worker stopped"} {
		if !strings.Contains(output, want) {
			t.Errorf("log output does not contain %q:\n%s", want, output)
		}
	}
}

func TestExpiryWorkerStopsPromptlyWithNothingToDo(t *testing.T) {
	logs := &syncBuffer{}
	log := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		// A long interval: the only way this returns is through cancellation,
		// which is what a shutdown has to be able to rely on.
		runExpiryWorker(ctx, memory.New(memory.Options{}), log, time.Hour)
	}()

	cancel()

	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("the expiry worker ignored cancellation")
	}
}

func TestRunRejectsBadConfiguration(t *testing.T) {
	cases := map[string]struct {
		env  string
		valu string
	}{
		"unparseable duration": {env: "INVENTORY_RESERVATION_TTL", valu: "fortnight"},
		"non-positive ttl":     {env: "INVENTORY_RESERVATION_TTL", valu: "0s"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv(tc.env, tc.valu)

			err := run(context.Background(), testRuntime())
			if err == nil {
				t.Fatal("run accepted a bad configuration")
			}
			if !strings.Contains(err.Error(), "RESERVATION_TTL") {
				t.Errorf("error %q should name the setting", err)
			}
		})
	}
}

func TestRunStartsAndStopsCleanly(t *testing.T) {
	// Port 0 lets the kernel pick a free one, so this test cannot collide with
	// a service that happens to be running on 9093.
	t.Setenv("INVENTORY_GRPC_ADDR", "127.0.0.1:0")
	t.Setenv("INVENTORY_SEED_DEMO", "false")
	// Cleared, not merely left unset: this asserts the in-memory path starts and
	// stops cleanly, and a developer with a DSN exported would otherwise be
	// running a different test — one that needs a database to be up.
	t.Setenv("INVENTORY_DB_DSN", "")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, testRuntime()) }()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v, want a clean shutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after its context was cancelled")
	}
}

// testRuntime builds the dependency bundle bootstrap would hand to run.
func testRuntime() *bootstrap.Runtime {
	return &bootstrap.Runtime{
		Service: "inventory",
		Env:     "dev",
		Log:     discardLogger(),
		Health:  health.New(),
		Config:  config.New("INVENTORY"),
	}
}
