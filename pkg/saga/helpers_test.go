package saga_test

import (
	"io"
	"log/slog"
	"sync"
	"time"
)

// discardLogger returns a logger that produces no output, so that test runs are
// readable and the engine's own logging does not become the thing under test.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// testClock is a manually advanced clock, so that "time has passed" can be
// asserted without a test that sleeps for minutes.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func newTestClock() *testClock {
	return &testClock{t: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}
