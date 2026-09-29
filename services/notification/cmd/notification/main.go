// Command notification turns domain events into customer notifications.
//
// It does not decide what happened — the order service already decided — it
// decides which of those facts the customer needs to hear about, and in what
// words. That is a smaller job than it sounds, and it is why this service can be
// redeployed and rewritten without touching the checkout flow.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/marketplace/pkg/bootstrap"
	"github.com/marketplace/pkg/kafka"
	"github.com/marketplace/pkg/postgres"

	psdedup "github.com/marketplace/services/notification/internal/adapters/postgres"
	"github.com/marketplace/services/notification/internal/adapters/sender"
	"github.com/marketplace/services/notification/internal/app"
	"github.com/marketplace/services/notification/migrations"
)

func main() {
	err := bootstrap.Run(bootstrap.Options{
		Service:           "notification",
		EnvPrefix:         "NOTIFICATION",
		DefaultHealthAddr: ":9196",
	}, run)
	if err != nil {
		fmt.Fprintf(os.Stderr, "notification: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, rt *bootstrap.Runtime) error {
	shared := rt.Shared

	prefix := shared.String("KAFKA_TOPIC_PREFIX", "marketplace")
	brokers := shared.StringSlice("KAFKA_BROKERS", []string{"kafka:9092"})

	service, err := app.New(app.Config{
		Sender: sender.NewLog(rt.Log),
		Logger: rt.Log,
	})
	if err != nil {
		return err
	}

	// ------------------------------------------------------------ deduplication
	//
	// Delivery is at-least-once, so a redelivery is normal and a consumer that
	// cannot tell one from a first attempt will send the same email twice. The
	// record lives in PostgreSQL rather than in memory because a restart must
	// not forget what it has already sent.
	var deduper kafka.Deduper
	if dsn := rt.Config.String("DB_DSN", ""); dsn != "" {
		pool, err := postgres.Open(ctx, postgres.Config{DSN: dsn, Service: "notification", Logger: rt.Log})
		if err != nil {
			return err
		}
		defer pool.Close()

		if err := postgres.Migrate(ctx, pool, "notification", migrations.FS, rt.Log); err != nil {
			return err
		}
		deduper = psdedup.NewDeduper(pool)
		rt.Health.Register("database", postgres.Health(pool))

		go pruneProcessedEvents(ctx, rt, psdedup.NewDeduper(pool))
	} else {
		rt.Log.Warn("NOTIFICATION_DB_DSN is not set: deduplication is in memory, so a restart may resend a notification")
		deduper = kafka.NewMemoryDeduper()
	}

	// ----------------------------------------------------------------- consumer
	//
	// Two topics, not all of them. `order.created` is internal progress the
	// customer did not ask to hear about; subscribing only to the two events
	// that produce a notification means the service cannot accidentally start
	// sending more.
	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Config: kafka.Config{
			Brokers:     brokers,
			TopicPrefix: prefix,
			ClientID:    "notification",
			Logger:      rt.Log,
		},
		Group:   shared.String("KAFKA_CONSUMER_GROUP", prefix+".notification.v1"),
		Topics:  []string{prefix + ".order.confirmed", prefix + ".order.cancelled"},
		Handler: handleEvent(service),
		Deduper: deduper,
	})
	if err != nil {
		return err
	}

	rt.Log.Info("notification service ready", "brokers", brokers, "topic_prefix", prefix)
	return consumer.Run(ctx)
}

// handleEvent adapts the Kafka handler to the service.
func handleEvent(service *app.Service) kafka.Handler {
	return func(ctx context.Context, event kafka.Event) error {
		// The event type comes from the header rather than from the topic name,
		// so that renaming a topic does not change what the payload means.
		eventType := event.Header("event_type")
		if eventType == "" {
			eventType = event.Topic
		}
		return service.Handle(ctx, eventType, event.Value)
	}
}

// pruneProcessedEvents trims the deduplication table.
//
// Redelivery happens within seconds; keeping a day of ids is orders of magnitude
// more than the window that matters, and without a bound the table grows by one
// row per event forever.
func pruneProcessedEvents(ctx context.Context, rt *bootstrap.Runtime, deduper *psdedup.Deduper) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			removed, err := deduper.ForgetOlderThan(ctx, 24*time.Hour)
			if err != nil {
				rt.Log.Error("could not prune processed events", "error", err.Error())
				continue
			}
			if removed > 0 {
				rt.Log.Info("pruned processed events", "count", removed)
			}
		}
	}
}
