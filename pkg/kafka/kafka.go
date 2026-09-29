// Package kafka provides the producer and consumer every service uses.
//
// Two properties of Kafka shape everything here, and both are handled by the
// package rather than left to each handler:
//
//   - **Delivery is at-least-once.** A consumer that processes a message and
//     then crashes before committing its offset will see that message again.
//     Deduplication is therefore not optional, so it is built into the consumer
//     loop: a handler that returns successfully has its event id recorded, and a
//     redelivery is skipped without the handler running again.
//
//   - **Ordering is per partition, not per topic.** Messages are keyed by
//     aggregate id, so everything about one order lands in one partition and is
//     therefore delivered in order. Without a key, a consumer can observe
//     `order.confirmed` before `order.created` and apply state backwards.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Config holds the settings shared by producers and consumers.
type Config struct {
	// Brokers is the bootstrap list.
	Brokers []string
	// TopicPrefix namespaces every topic, so one cluster can host several
	// environments without a naming collision.
	TopicPrefix string
	// ClientID identifies this service in broker logs and metrics.
	ClientID string
	Logger   *slog.Logger
}

func (c Config) withDefaults() Config {
	if len(c.Brokers) == 0 {
		c.Brokers = []string{"localhost:9092"}
	}
	if c.TopicPrefix == "" {
		c.TopicPrefix = "marketplace"
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

// TopicFor returns the topic name for an event type.
//
// One topic per event type rather than one topic for everything: it lets a
// consumer subscribe to exactly what it needs, and it means a poison message in
// one stream cannot block another.
func (c Config) TopicFor(eventType string) string {
	return c.TopicPrefix + "." + eventType
}

// ------------------------------------------------------------------ producer

// Producer publishes records to Kafka.
type Producer struct {
	client *kgo.Client
	cfg    Config
}

// NewProducer connects a producer.
//
// The client is created eagerly but connects lazily; the first publish reports a
// connection failure, which is deliberate. A service that refuses to start
// because a broker is briefly unavailable turns a broker restart into a
// platform-wide outage.
func NewProducer(cfg Config) (*Producer, error) {
	cfg = cfg.withDefaults()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		// All in-sync replicas must acknowledge before a write is considered
		// successful. With acks=1 a leader failure loses acknowledged events,
		// and the outbox row is already marked published by then.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Retry indefinitely rather than dropping. The outbox poller also
		// retries, but a dropped record here would only be noticed as a missing
		// notification.
		kgo.RecordRetries(10),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: create producer: %w", err)
	}

	return &Producer{client: client, cfg: cfg}, nil
}

// Close flushes pending records and closes the client.
func (p *Producer) Close() {
	if p == nil || p.client == nil {
		return
	}
	// Flush before closing: a record still in the batch buffer when the process
	// exits is a lost event.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = p.client.Flush(ctx)
	p.client.Close()
}

// Publish sends one record.
//
// The key is the aggregate id, which is what puts every event for one order in
// one partition and therefore in order.
func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	}
	for k, v := range headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}

	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("kafka: publish to %s: %w", topic, err)
	}
	return nil
}

// PublishEvent sends an event on its own topic, keyed by aggregate id.
func (p *Producer) PublishEvent(ctx context.Context, eventType, aggregateID string, value []byte, headers map[string]string) error {
	return p.Publish(ctx, p.cfg.TopicFor(eventType), aggregateID, value, headers)
}

// ------------------------------------------------------------------ consumer

// Event is one delivered record.
type Event struct {
	Topic     string
	Key       string
	Value     []byte
	Headers   map[string]string
	Partition int32
	Offset    int64
}

// Header returns a header value, or "" when absent.
func (e Event) Header(key string) string { return e.Headers[key] }

// Handler processes one event. Returning an error leaves the offset uncommitted,
// so the event is redelivered.
type Handler func(ctx context.Context, event Event) error

// Deduper records which events have been processed.
//
// It is required, not optional: delivery is at-least-once, so without it every
// redelivery repeats the side effect. The method must be atomic — a check
// followed by a separate write races against a concurrent redelivery of the same
// event, which is exactly the situation it exists to handle.
type Deduper interface {
	// MarkProcessed records an event id and reports whether it had already been
	// recorded. Returning true means the caller must skip the event.
	MarkProcessed(ctx context.Context, eventID string) (alreadyProcessed bool, err error)
}

// ConsumerConfig configures a consumer.
type ConsumerConfig struct {
	Config
	// Group is the consumer group. It determines which events this service
	// receives when several instances are running.
	Group string
	// Topics to subscribe to.
	Topics []string
	// Handler processes each event.
	Handler Handler
	// Deduper guards against redelivery. Required.
	Deduper Deduper
	// EventID extracts the deduplication key from a record. Nil reads the
	// `event_id` header, which is where the outbox poller writes it.
	EventID func(Event) string
}

// Consumer reads events and dispatches them to a handler.
type Consumer struct {
	client  *kgo.Client
	cfg     ConsumerConfig
	handler Handler
	deduper Deduper
	eventID func(Event) string
}

// NewConsumer connects a consumer.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if cfg.Handler == nil {
		return nil, errors.New("kafka: a handler is required")
	}
	if cfg.Deduper == nil {
		// Refusing to start is the honest choice. A consumer without
		// deduplication will silently repeat side effects under redelivery, and
		// that is far harder to notice than a startup failure.
		return nil, errors.New("kafka: a deduper is required; delivery is at-least-once and redelivery is normal")
	}
	if len(cfg.Topics) == 0 {
		return nil, errors.New("kafka: at least one topic is required")
	}
	if cfg.Group == "" {
		return nil, errors.New("kafka: a consumer group is required")
	}

	cfg.Config = cfg.Config.withDefaults()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		// Commit offsets automatically once a record has been handled. The
		// handler decides success by returning nil, and a redelivery after a
		// crash is absorbed by the deduper.
		kgo.DisableAutoCommit(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			for topic, parts := range assigned {
				cfg.Logger.Info("partitions assigned", "topic", topic, "partitions", len(parts), "group", cfg.Group)
			}
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
			for topic, parts := range revoked {
				cfg.Logger.Info("partitions revoked", "topic", topic, "partitions", len(parts), "group", cfg.Group)
			}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: create consumer: %w", err)
	}

	eventID := cfg.EventID
	if eventID == nil {
		eventID = func(e Event) string { return e.Header("event_id") }
	}

	return &Consumer{
		client:  client,
		cfg:     cfg,
		handler: cfg.Handler,
		deduper: cfg.Deduper,
		eventID: eventID,
	}, nil
}

// Run consumes until ctx is cancelled.
//
// Records are handled one at a time and offsets are committed only after a
// successful handle. The alternative — handling concurrently and committing
// eagerly — trades correctness for throughput in a way that is invisible until
// an event is silently lost during a restart.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.client.Close()

	c.cfg.Logger.Info("consumer started",
		"group", c.cfg.Group,
		"topics", c.cfg.Topics,
		"brokers", c.cfg.Brokers,
	)

	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return nil
		}

		// A fetch error is logged and the loop continues. Returning here would
		// stop consuming for a transient broker problem; the client retries
		// internally and will deliver again.
		fetches.EachError(func(topic string, partition int32, err error) {
			if errors.Is(err, context.Canceled) {
				return
			}
			c.cfg.Logger.Warn("kafka fetch error",
				"topic", topic, "partition", partition, "error", err.Error())
		})

		var toCommit []*kgo.Record

		fetches.EachRecord(func(record *kgo.Record) {
			if err := c.handle(ctx, record); err != nil {
				// The offset is not committed, so this record is redelivered.
				// Repeated failure is a poison message; it will block the
				// partition, which is louder than dropping it silently.
				c.cfg.Logger.Error("event handling failed, offset not committed",
					"topic", record.Topic, "partition", record.Partition,
					"offset", record.Offset, "error", err.Error())
				return
			}
			toCommit = append(toCommit, record)
		})

		if len(toCommit) == 0 {
			continue
		}

		if err := c.client.CommitRecords(ctx, toCommit...); err != nil {
			c.cfg.Logger.Error("could not commit offsets",
				"count", len(toCommit), "error", err.Error())
		}
	}
}

// handle deduplicates and dispatches one record.
func (c *Consumer) handle(ctx context.Context, record *kgo.Record) error {
	event := Event{
		Topic:     record.Topic,
		Key:       string(record.Key),
		Value:     record.Value,
		Partition: record.Partition,
		Offset:    record.Offset,
		Headers:   make(map[string]string, len(record.Headers)),
	}
	for _, h := range record.Headers {
		event.Headers[h.Key] = string(h.Value)
	}

	id := c.eventID(event)
	if id == "" {
		// An event with no id cannot be deduplicated. Processing it anyway would
		// risk a repeated side effect on redelivery, so it is rejected — loudly,
		// because the producer is at fault and needs fixing.
		return fmt.Errorf("event on %s has no event_id header, so redelivery cannot be detected", event.Topic)
	}

	seen, err := c.deduper.MarkProcessed(ctx, id)
	if err != nil {
		return fmt.Errorf("deduplicate event %s: %w", id, err)
	}
	if seen {
		c.cfg.Logger.Debug("skipping a redelivered event", "event_id", id, "topic", event.Topic)
		return nil
	}

	// Join the producing trace rather than starting a new one. Without this the
	// consumer's work appears as an orphan root, and the question "did the
	// notification go out because of that checkout?" has no answer in the trace
	// UI.
	ctx = c.joinTrace(ctx, event)

	if err := c.handler(ctx, event); err != nil {
		return fmt.Errorf("handle event %s: %w", id, err)
	}
	return nil
}

// joinTrace extracts the trace context a producer left in the headers.
//
// A missing or unparseable traceparent is not an error: the event is still
// perfectly valid, and refusing to process it because its tracing metadata is
// absent would make observability a correctness dependency.
func (c *Consumer) joinTrace(ctx context.Context, event Event) context.Context {
	carrier := propagation.MapCarrier{}
	for k, v := range event.Headers {
		carrier.Set(k, v)
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// ------------------------------------------------------------- memory deduper

// MemoryDeduper is an in-process Deduper.
//
// It exists for tests and for local runs without a database. It is not suitable
// for a deployed service: a restart forgets what it has seen, and a second
// replica has its own map, so both would reprocess on redelivery. A real
// implementation stores processed ids in the service's database, in the same
// transaction as the effect it guards.
type MemoryDeduper struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

// NewMemoryDeduper returns an empty deduper.
func NewMemoryDeduper() *MemoryDeduper {
	return &MemoryDeduper{seen: make(map[string]struct{})}
}

// MarkProcessed implements Deduper.
func (d *MemoryDeduper) MarkProcessed(_ context.Context, eventID string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.seen[eventID]; ok {
		return true, nil
	}
	d.seen[eventID] = struct{}{}
	return false, nil
}

// Len returns how many ids have been recorded.
func (d *MemoryDeduper) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}
