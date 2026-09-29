// Package outbox publishes events recorded in the outbox table.
//
// The table exists because a state change and its event cannot be written
// atomically to two systems (ADR-0002). This poller closes the loop: it reads
// unpublished rows in order and publishes them, so the event is eventually
// delivered even though the two writes were never simultaneous.
//
// Delivery is at-least-once, by construction. A crash between publishing and
// marking the row published causes a duplicate, and that is the correct trade:
// a duplicate is absorbed by the consumer's deduplication, while a lost event is
// a customer who is never notified.
package outbox

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/marketplace/pkg/kafka"

	pgstore "github.com/marketplace/services/order/internal/adapters/postgres"
)

// EventStore is the part of the order repository the poller needs.
//
// An interface rather than the concrete store, so that the poller can be tested
// without a database and cannot accidentally grow a dependency on the rest of
// the repository.
type EventStore interface {
	// PendingEvents returns unpublished events in insertion order.
	PendingEvents(ctx context.Context, limit int) ([]pgstore.OutboxEvent, error)
	// MarkPublished records that an event reached the broker.
	MarkPublished(ctx context.Context, ids ...string) error
	// MarkFailed records a delivery attempt that failed.
	MarkFailed(ctx context.Context, id string, cause error) error
}

// Config configures the poller.
type Config struct {
	Store    EventStore
	Producer *kafka.Producer
	Logger   *slog.Logger

	// Interval between passes. It is the delay between committing an order and
	// the rest of the system learning about it, so it trades latency against
	// query load.
	Interval time.Duration
	// BatchSize bounds one pass, so that a large backlog does not hold a
	// transaction or a goroutine for an unbounded time.
	BatchSize int
	// MaxAttempts after which an event is left alone and reported. A poison
	// message must not be retried forever, and it must not be dropped silently
	// either.
	MaxAttempts int
}

func (c Config) withDefaults() Config {
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Interval <= 0 {
		c.Interval = time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 10
	}
	return c
}

// Poller moves events from the outbox to Kafka.
type Poller struct {
	cfg Config
}

// New builds a poller.
func New(cfg Config) (*Poller, error) {
	if cfg.Store == nil {
		return nil, errors.New("outbox: a store is required")
	}
	if cfg.Producer == nil {
		return nil, errors.New("outbox: a producer is required")
	}
	return &Poller{cfg: cfg.withDefaults()}, nil
}

// Run polls until ctx is cancelled.
//
// There is no leader election and no coordination between replicas. Two pollers
// reading the same rows would both publish, which produces a duplicate — and
// duplicates are already a normal, handled outcome. Adding coordination to
// prevent something the consumers must tolerate anyway would be complexity
// without a benefit.
func (p *Poller) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.cfg.Interval)
	defer ticker.Stop()

	p.cfg.Logger.Info("outbox poller started", "interval", p.cfg.Interval, "batch_size", p.cfg.BatchSize)

	for {
		select {
		case <-ctx.Done():
			p.cfg.Logger.Info("outbox poller stopped")
			return nil

		case <-ticker.C:
			published, err := p.pollOnce(ctx)
			if err != nil {
				// A failure here is usually the database or the broker being
				// briefly unavailable. Logging and trying again next tick is the
				// whole recovery strategy, and the outbox keeps the events safe
				// in the meantime.
				p.cfg.Logger.Error("outbox pass failed", "error", err.Error())
				continue
			}
			if published > 0 {
				p.cfg.Logger.Info("outbox drained", "published", published)
			}
		}
	}
}

// pollOnce publishes one batch and reports how many events were delivered.
func (p *Poller) pollOnce(ctx context.Context) (int, error) {
	events, err := p.cfg.Store.PendingEvents(ctx, p.cfg.BatchSize)
	if err != nil {
		return 0, err
	}

	published := 0
	for _, event := range events {
		if ctx.Err() != nil {
			return published, ctx.Err()
		}

		if event.Attempts >= p.cfg.MaxAttempts {
			// Left in place rather than deleted. An event that cannot be
			// published is an incident, and a row that stays visible is how an
			// operator finds out about it.
			continue
		}

		// The event id travels as a header so that the consumer can deduplicate
		// without parsing the payload. It has to be readable before the body is
		// deserialised, because the decision to skip happens first.
		headers := map[string]string{
			"event_id":   event.ID,
			"event_type": event.Type,
		}
		// The trace context captured when the event was created, not now. See
		// the field's comment: this is the difference between a consumer joining
		// the checkout's trace and starting one of its own.
		if event.TraceParent != "" {
			headers["traceparent"] = event.TraceParent
		}

		if err := p.cfg.Producer.PublishEvent(ctx, event.Type, event.AggregateID, event.Payload, headers); err != nil {
			if merr := p.cfg.Store.MarkFailed(ctx, event.ID, err); merr != nil {
				p.cfg.Logger.Error("could not record an outbox failure",
					"event_id", event.ID, "error", merr.Error())
			}
			p.cfg.Logger.Warn("event could not be published, will retry",
				"event_id", event.ID,
				"event_type", event.Type,
				"attempts", event.Attempts+1,
				"error", err.Error(),
			)
			continue
		}

		if err := p.cfg.Store.MarkPublished(ctx, event.ID); err != nil {
			// The event is already on the broker; failing to record that means
			// it will be published again. That is a duplicate, not a loss, and
			// the consumer handles it.
			p.cfg.Logger.Error("published an event but could not mark it published",
				"event_id", event.ID, "error", err.Error())
			continue
		}

		published++
	}

	return published, nil
}
