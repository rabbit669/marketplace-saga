# ADR-0001: Orchestrated saga instead of choreographed events

- **Status:** Accepted
- **Date:** 2026-01-15
- **Deciders:** backend team

## Context

Checking out a cart writes to three services and calls one external provider:

1. `order` creates the order row.
2. `inventory` reserves stock.
3. `payment` authorizes the card.
4. `order` marks the order confirmed.

Each service owns its own database. There is no transaction spanning them, and
there is no distributed transaction manager in the platform. When step 3 is
declined, steps 1 and 2 have already committed and must be undone by running new
business operations — a release of the reservation and a cancellation of the
order.

Two established ways to coordinate this exist:

- **Choreography.** Each service publishes an event; the next service reacts.
  There is no central coordinator.
- **Orchestration.** One service owns the flow and tells the others what to do,
  recording each step's outcome.

We need to choose one and commit to it, because the choice shapes where failures
are diagnosed for the life of the system.

## Decision

We use an **orchestrated saga**, implemented in `pkg/saga` and owned by the
`order` service. The order service executes the steps in order and, on failure,
executes each completed step's compensation in reverse order.

The engine persists the saga after every step transition, so a process that dies
mid-saga resumes rather than restarts, and a payment that was already authorized
is never authorized twice.

## Rationale

**The failure path is the product.** Checkout succeeds almost always; what
distinguishes an implementation is what it does when it does not. With
orchestration, the entire failure path — which steps ran, what triggered the
rollback, which compensations succeeded — is readable in one place. With
choreography it is an emergent property of five consumers, and reconstructing
"why was this booking rolled back?" means reading five services' logs and
reasoning about event ordering.

**The flow is fixed and short.** Choreography earns its keep when steps are
added by different teams independently and the process is genuinely open-ended.
Checkout has three steps, all owned by one team, and the business requires them
in a specific order. Orchestration's usual cost — a coordinator that must know
about every participant — buys us nothing to avoid here, because that coupling
already exists in the business rules.

**Compensation needs global knowledge.** Deciding *whether* to compensate
requires knowing which steps completed. In choreography, no participant knows
that. Each would have to infer it from the events it happens to have seen, which
is exactly the fragile reasoning that causes double refunds.

**Recoverability is explicit.** The orchestrator owns a durable saga record, so
a recovery worker can find sagas that stopped progressing and drive them to a
terminal state. In a choreographed system there is no such record; a lost
message leaves the system in a state nothing is responsible for noticing.

## Consequences

### Positive

- The failure path is testable in isolation. `pkg/saga` is covered by tests for
  crash-resume, reverse-order compensation, partial compensation failure and a
  definition that changes mid-flight — none of which need a database or a broker.
- Compensation runs from persisted state, so a restart between steps does not
  lose the information needed to undo them.
- Operators get one place to look: `GetSagaState` returns every step, its
  attempt count and its error.

### Negative

- The `order` service must know the shape of the checkout flow, including the
  commands other services accept. Adding a step means changing the orchestrator.
- The orchestrator is on the critical path for checkout. It is stateless and
  horizontally scalable, but its availability matters more than it would in a
  choreographed design.
- Every participant must expose a compensation for its step, which is real work
  that is easy to postpone and must not be.

## Alternatives considered

**Choreography with Kafka events only.** Rejected for the reasons above. It also
makes the rollback path untestable end-to-end without running the whole cluster.

**Two-phase commit (XA).** Rejected. It requires a transaction coordinator and
participants that support prepared transactions; PostgreSQL does, Kafka and the
payment provider do not. It also holds locks for the duration of the protocol,
so a slow participant blocks unrelated traffic. Most importantly, 2PC cannot
enlist an external payment provider at all, which is the participant that
actually matters.

**A single shared database with one transaction.** Rejected: it reintroduces the
coupling that the service split exists to remove, and still cannot include the
payment provider.

**Retry until it works, with no compensation.** Rejected: a declined card is a
permanent failure, not a transient one. Retrying it forever would hold stock
indefinitely.
