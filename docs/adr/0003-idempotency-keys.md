# ADR-0003: Idempotency keys on every retryable command

- **Status:** Accepted
- **Date:** 2026-01-15
- **Deciders:** backend team

## Context

The saga retries steps. So does the retry policy inside each client, the HTTP
client at the edge, and the customer who taps "Place order" twice when the
button does not respond immediately.

A retry is not a repeat of a known outcome. It is the resolution of an
**unknown** one:

```
client ──POST /orders──▶ order service
                              │
                              ├─ writes the order
                              │
                              ╳  connection drops (timeout, deploy, OOM)
                              
client: "did that work?"  — nobody knows
```

The client cannot distinguish "the request never arrived" from "it arrived, was
processed, and the response was lost". Both look identical. Retrying is the only
safe action, and it must not create a second order or charge the card twice.

This is not a theoretical concern: it is the single most common way a naive
checkout implementation double-charges customers.

## Decision

Every command that crosses a process boundary and may be retried requires a
caller-supplied **idempotency key**. Three places enforce this:

1. **`CreateOrder`** requires `idempotency_key`. The first call stores the key
   with the created order id; a retry returns the original order and sets
   `idempotent_replay = true`, rather than creating a second order.

2. **`AuthorizePayment`** requires `idempotency_key`. Two calls with the same key
   return the same payment record. This mirrors what every real provider
   (Stripe, Adyen, Braintree) exposes, because they face the same problem.

3. **Saga reservation ids are deterministic.** The order service passes its own
   order id as the inventory `reservation_id`. The reservation is therefore
   naturally idempotent under a saga retry without a separate key: reserving for
   order `o-42` twice returns the first reservation.

Keys are stored in the service's own database, in the same transaction as the
effect they describe, with a unique constraint. The mechanism is the same
outbox argument: the key and the effect must commit together or the guarantee is
worthless.

## Rationale

**"At least once" is a property of the transport, not a choice.** Retries will
happen. The only question is whether they are safe.

**A unique constraint is the enforcement.** Not a lookup-then-insert, which races
under concurrency; a `UNIQUE` index where the losing insert is caught and
resolved by reading the winner's row. The database is the only component that
can make this decision atomically.

**Keys are scoped, not global.** An idempotency key identifies one attempt at one
operation by one caller, so it is scoped to `(caller, operation, key)`. Otherwise
one customer's key could suppress another's order.

**The key expires, but not quickly.** Keys are retained longer than the maximum
possible retry window — 24 hours is the industry norm and what we use. Retaining
them forever turns the table into an unbounded log; expiring them after minutes
reopens the double-charge window it exists to close.

## Consequences

### Positive

- The retry storm after a network blip is harmless rather than expensive.
- Customers can safely retry a timed-out checkout, which is what they will do
  regardless of what the API documents.
- The stored response is returned verbatim on replay, so a client that retries a
  successful request gets the same success response, not an error.

### Negative

- **The key is now part of the API contract.** Callers must generate one and keep
  it stable across retries. A client that generates a fresh key per attempt gets
  no protection, so the gateway defaults to deriving one from the request body
  hash when the header is absent.
- **A failed request must not be cached as a failure** if the failure was
  transient and the caller might retry successfully. Only terminal outcomes
  (success, or a permanent decline) are stored as replayable; transient failures
  release the key.
- **Storage with a retention policy.** Modest, but it is a new table and a new
  cleanup job per service that needs it.

## Alternatives considered

**Deduplicate on a natural key.** Rejected: there is no natural key for "this
particular checkout attempt". Two genuine attempts to order the same two items
are different orders.

**Advisory locks around the operation.** Rejected: they prevent concurrent
duplicates but not sequential ones — exactly the case a retry produces.

**Let the client sort it out (compare before create).** Rejected: the client has
strictly less information than the server, and the check-then-act pattern races.

**Idempotent-by-design operations with no key.** This is what we do for
`ReserveStock`, but it only works where a natural deterministic key exists. For
"create an order" there is none, so the caller must supply one.
