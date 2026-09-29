# ADR-0002: Transactional outbox instead of dual writes

- **Status:** Accepted
- **Date:** 2026-01-15
- **Deciders:** backend team

## Context

Services must both change their own state and tell the rest of the system about
it. When the order service confirms an order, it has to:

1. write `status = confirmed` to `order_db`, and
2. publish `order.confirmed` to Kafka, so that notification sends a receipt.

These are two different systems. There is no transaction spanning them.

The naive implementation fails in both directions:

```go
// The dual write.
if err := db.UpdateStatus(order, "confirmed"); err != nil {
    return err
}
if err := kafka.Publish(OrderConfirmed{...}); err != nil {
    return err
}
```

- If the process dies between the two calls, the order is confirmed and nobody
  is ever notified. The customer paid and received nothing.
- If Kafka accepts the message and the commit then fails, we publish a
  confirmation for an order that does not exist.

Neither failure is rare enough to ignore: process restarts, deploys and broker
restarts all produce them.

## Decision

We write the event to an `outbox` table **in the same database transaction** as
the state change, and a separate poller publishes outbox rows to Kafka
afterwards.

```sql
BEGIN;
  UPDATE orders SET status = 'confirmed' WHERE id = $1;
  INSERT INTO outbox (id, aggregate_id, event_type, payload, occurred_at)
       VALUES ($2, $1, 'order.confirmed', $3, now());
COMMIT;
```

The poller then reads unpublished rows in order, publishes them, and marks them
published.

This gives **at-least-once** delivery. It cannot give exactly-once, and we do not
pretend otherwise — see consequences.

## Rationale

**One transaction, one atomicity guarantee.** The state change and the intent to
publish it succeed or fail together. There is no window in which one happened
and the other did not.

**The database is already the source of truth.** The outbox is an ordinary
table, so it participates in the same backup, replication and point-in-time
recovery as the business data. A separate message log would be a second thing to
lose.

**Ordering is preserved per aggregate.** The poller reads rows by insertion
sequence, so `order.created` is always published before `order.confirmed` for
the same order. Kafka's per-partition ordering then preserves it for consumers
keyed by order id.

**It keeps the failure inside the service.** If Kafka is down, the outbox simply
grows. The service keeps serving requests, and publishing resumes when the
broker returns. The alternative — publishing inline — turns a broker outage into
a checkout outage.

## Consequences

### Positive

- No lost events from a crash between commit and publish.
- No phantom events for rolled-back state changes.
- A broker outage degrades notification delivery rather than checkout.
- The outbox table doubles as an audit log of everything published.

### Negative

- **Consumers must be idempotent.** Delivery is at-least-once, so a consumer will
  see duplicates after a crash between publish and mark-published. Every consumer
  therefore stores processed `event_id`s and ignores repeats. This is not
  optional, and it is enforced in `pkg/kafka` rather than left to each handler.
- **Added latency.** Events are published on the poll interval (1s by default)
  rather than immediately. Acceptable for notifications; it would not be for a
  request/response path, and this pattern is deliberately not used for one.
- **The outbox grows.** A retention job deletes published rows older than the
  longest plausible consumer outage.
- **A table that is not a domain concept.** The outbox is infrastructure living
  in the service's schema, which slightly muddies "one database per service means
  one bounded context". We accept the trade-off: the alternative is losing
  events.

## Alternatives considered

**Publish inside the request, accept the risk.** Rejected: the failure mode is a
customer who paid and was never told, which is a support ticket and a chargeback.

**Kafka transactions with a transactional producer.** Rejected: it makes the
*Kafka* write atomic with other Kafka writes, not with the database commit. The
database is the system that must not diverge.

**Change data capture (Debezium) on the WAL.** This is the strongest alternative
and we would pick it if we were already running Debezium. It removes polling
latency and the outbox table entirely. Rejected for now because it adds a
connector, a Kafka Connect cluster and a schema-evolution dependency to a
project whose point is the saga logic — it moves the interesting failure modes
somewhere they cannot be tested in-process. The outbox keeps the mechanism
visible and unit-testable.

**Listen to yourself (publish, then consume your own event to update state).**
Rejected: it inverts the source of truth and makes reads inconsistent for an
unbounded window.
