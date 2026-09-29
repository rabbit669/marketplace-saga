# ADR-0005: One database per service, and no cross-service foreign keys

- **Status:** Accepted
- **Date:** 2026-01-15

## Context

Five services need persistence. The cheapest way to build this is one PostgreSQL
instance with one schema, tables joined by foreign keys, and services reading
whichever tables they need.

That is genuinely cheaper — right up to the first time two services need to
change the same table on different schedules, or one service's slow query takes
down another's checkout.

## Decision

Each service owns a **separate logical database** on the shared PostgreSQL
instance, with its own credentials and its own migration history. A service may
read and write only its own database.

**No foreign key ever crosses a database boundary.** Where one service needs to
reference another's entity, it stores the identifier as an opaque value.

## Rationale

**The boundary must be enforced by something, or it will not hold.** A schema
convention does not survive a deadline. Separate databases make a cross-service
join fail at the database level, which is the only enforcement that does not
depend on discipline.

**Independent deployability is the point of the split.** If two services share a
table, a migration for one is a migration for both, and "deploy the order service
alone" becomes impossible. That is the specific problem microservices exist to
solve, and a shared table reintroduces it.

**Referential integrity between services is the saga's job.** This is the
counter-intuitive part and the reason it is written down. A foreign key would
give us database-enforced consistency, but only within one transaction — and
across services we have no such transaction, so we would get a constraint that
cannot be satisfied atomically. The consistency model here is *eventual*, and it
is maintained by compensations, not by constraints (see ADR-0001).

**Identifiers are opaque.** The order service stores `product_id` as a string. It
does not assume it is a UUID, an integer, or that the row exists. A service that
dereferences another service's identifier in a query has taken a hidden
dependency on that service's schema.

**One instance, many databases.** Running five PostgreSQL servers locally would
be wasteful and would drown the actual lesson in container orchestration. The
`database-per-service` *rule* is what matters; the physical placement is a
deployment detail that can change without touching code, because nothing joins
across databases.

## Consequences

### Positive

- Cross-service coupling fails loudly at development time.
- Each service migrates on its own schedule.
- Backup, restore and retention are per-service.
- Extracting a service to its own server later is a connection-string change.

### Negative

- **No joins across services.** A screen that needs orders plus product names
  makes two calls, and the composing service must handle a missing product. In
  practice the order service copies the fields it needs (name, price) into the
  order line at checkout, which is the right call anyway: an order must record
  the price the customer agreed to, not today's price.
- **No foreign key means orphans are possible.** A product deleted while an order
  references it leaves a dangling id. Handled by treating the reference as
  optional and degrading gracefully, not by adding back a constraint that cannot
  work.
- **More connection pools.** Five services × one pool each; sized explicitly
  because PostgreSQL's `max_connections` is a shared, easily exhausted resource.

## Alternatives considered

**Single shared schema with foreign keys.** Rejected: it is a distributed
monolith with all the coordination costs and none of the benefits.

**Separate database servers per service from day one.** Rejected for local
development only — it multiplies resource use without changing any code. The
architecture is identical either way, which is precisely why the rule is
enforced at runtime rather than by topology.

**Event sourcing as the store of record.** Rejected: it is a much larger
commitment (schema evolution of events, projections, replay) than this problem
requires, and the outbox already gives us the event log we actually need.
