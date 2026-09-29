# Architecture

Three views of the same system, at three levels of zoom. The point of showing all
three is that a microservice architecture is only defensible if you can describe
what each service is for and why the boundary is where it is — a single diagram
of boxes and arrows invites the question "why not one service?" and answers none
of it.

- [Level 1 — System context](#level-1--system-context): who uses this, and what it depends on
- [Level 2 — Containers](#level-2--containers): the services and their stores
- [Level 3 — The checkout flow](#level-3--the-checkout-flow): the sequence that the whole project exists for

---

## Level 1 — System context

```mermaid
C4Context
    title System context

    Person(customer, "Customer", "Buys things. Uses a browser or a mobile app.")
    Person(operator, "Operator", "Investigates a stuck checkout and retries it.")

    System(marketplace, "Marketplace", "Takes orders, holds stock and charges cards. Guarantees that a checkout either completes or leaves nothing behind.")

    System_Ext(psp, "Payment provider", "Authorizes and captures card payments. Can decline, and can time out.")
    System_Ext(email, "Email provider", "Delivers customer notifications.")

    Rel(customer, marketplace, "Places orders", "HTTPS/JSON")
    Rel(operator, marketplace, "Inspects and retries sagas", "HTTPS/JSON")
    Rel(marketplace, psp, "Authorizes, captures, voids", "Provider API")
    Rel(marketplace, email, "Sends receipts and cancellations", "Provider API")
```

The only interesting thing here is what is **outside** the boundary. The payment
provider can decline or time out, and neither is under our control — which is
precisely why the system needs compensations rather than a rollback statement.

---

## Level 2 — Containers

```mermaid
C4Container
    title Containers

    Person(customer, "Customer")

    Container_Boundary(platform, "Marketplace") {
        Container(gateway, "API gateway", "Go, HTTP", "Verifies tokens, limits rate, translates REST to gRPC. The only service published on the host network.")
        Container(auth, "Auth", "Go, gRPC", "Registers users, verifies passwords, issues and rotates tokens.")
        Container(order, "Order", "Go, gRPC", "Owns the checkout saga: step order, compensation, recovery.")
        Container(catalog, "Catalog", "Go, gRPC", "Products and prices. The source of the checkout price snapshot.")
        Container(inventory, "Inventory", "Go, gRPC", "Stock levels and the two-phase reservation lifecycle.")
        Container(payment, "Payment", "Go, gRPC", "Provider adapter. Idempotency and decline classification.")
        Container(notification, "Notification", "Go, Kafka consumer", "Turns domain events into customer messages.")

        ContainerDb(orderdb, "order_db", "PostgreSQL", "Orders, saga state, idempotency keys, outbox.")
        ContainerDb(authdb, "auth_db", "PostgreSQL", "Users, hashed credentials, refresh tokens.")
        ContainerDb(catalogdb, "catalog_db", "PostgreSQL", "Products.")
        ContainerDb(invdb, "inventory_db", "PostgreSQL", "Stock and reservations.")
        ContainerDb(paydb, "payment_db", "PostgreSQL", "Payments and idempotency claims.")
        ContainerDb(notifdb, "notification_db", "PostgreSQL", "Processed event ids, for deduplication.")

        Container(kafka, "Kafka", "Event log", "Domain events. At-least-once, ordered per order id.")
        Container(redis, "Redis", "Cache", "Cache and rate-limit state. Losing it is survivable.")
    }

    System_Ext(psp, "Payment provider")

    Rel(customer, gateway, "HTTPS/JSON")
    Rel(gateway, auth, "ValidateToken, Login", "gRPC")
    Rel(gateway, order, "CreateOrder, GetOrder, GetSagaState", "gRPC")

    Rel(order, catalog, "GetProductsBatch — price the cart", "gRPC")
    Rel(order, inventory, "Reserve, Commit, Release", "gRPC")
    Rel(order, payment, "Authorize, Capture, Void", "gRPC")

    Rel(order, orderdb, "Reads and writes", "SQL")
    Rel(auth, authdb, "Reads and writes", "SQL")
    Rel(catalog, catalogdb, "Reads and writes", "SQL")
    Rel(inventory, invdb, "Reads and writes", "SQL")
    Rel(payment, paydb, "Reads and writes", "SQL")
    Rel(notification, notifdb, "Deduplicates events", "SQL")

    Rel(payment, psp, "Authorizes and captures", "Provider API")
    Rel(order, kafka, "Publishes via the outbox", "Kafka")
    Rel(kafka, notification, "Delivers events", "Kafka")
    Rel(gateway, redis, "Rate-limit buckets", "Redis")

    UpdateLayoutConfig($c4ShapeInRow="3", $c4BoundaryInRow="1")
```

**Why these boundaries.**

| Service | Owns | Why it is separate |
|---|---|---|
| `auth` | Credentials and tokens | The only service that ever sees a password, so the blast radius of a logging mistake is one service |
| `order` | The checkout flow | The only service that knows the *shape* of a checkout; every other service exposes a capability |
| `catalog` | Prices | Pricing changes far more often than checkout, and on a different schedule |
| `inventory` | Stock | The one place that must never oversell, so it holds a lock and nobody else does |
| `payment` | The provider relationship | Swapping providers must not touch the checkout logic |
| `notification` | What the customer is told | Async, slow, and allowed to fail without affecting a sale |

No foreign key crosses a database boundary. Referential integrity between services
is the saga's job — see [ADR-0005](../adr/0005-database-per-service.md).

---

## Level 3 — The checkout flow

The saga, including the path that matters. Everything before `capture_payment` can
be undone; nothing after it can.

```mermaid
sequenceDiagram
    autonumber
    participant C as Customer
    participant G as Gateway
    participant O as Order (saga)
    participant Cat as Catalog
    participant I as Inventory
    participant P as Payment
    participant DB as order_db
    participant K as Kafka
    participant N as Notification

    C->>G: POST /v1/orders (Idempotency-Key)
    G->>O: CreateOrder

    O->>Cat: GetProductsBatch
    Cat-->>O: prices
    Note over O,DB: order + idempotency key commit<br/>in ONE transaction

    rect rgb(235, 245, 255)
    Note over O,P: Reversible steps — everything here has a compensation
    O->>I: ReserveStock(reservation_id = order_id)
    I-->>O: held
    O->>P: AuthorizePayment(idempotency_key = order_id)
    P-->>O: approved
    end

    rect rgb(255, 240, 235)
    Note over O,I: The pivot — from here there is no way back
    O->>P: CapturePayment
    O->>I: CommitReservation
    O->>DB: MarkConfirmed
    end

    O->>DB: outbox: order.confirmed
    O-->>G: 201 with the order
    G-->>C: 201

    DB->>K: outbox poller publishes
    K->>N: order.confirmed
    N->>N: deduplicate by event_id
    Note over N: email sent
```

And the same flow when the card is refused:

```mermaid
sequenceDiagram
    autonumber
    participant O as Order (saga)
    participant I as Inventory
    participant P as Payment
    participant DB as order_db
    participant K as Kafka
    participant N as Notification

    O->>I: ReserveStock
    I-->>O: held
    O->>P: AuthorizePayment
    P-->>O: DECLINED (insufficient funds, permanent)

    Note over O: No retry — a declined card cannot succeed on a second attempt

    rect rgb(255, 245, 235)
    Note over O,I: Compensation, in reverse order
    O->>P: VoidPayment — a no-op, the provider never authorized
    O->>I: ReleaseReservation — the hold goes back to the pool
    end

    O->>DB: MarkCancelled + outbox: order.cancelled
    O-->>O: 422 with the order AND the reason

    DB->>K: outbox poller publishes
    K->>N: order.cancelled
    Note over N: "you have not been charged"
```

`capture_payment`, `commit_stock` and `confirm_order` never run. That is the
property the whole project exists to demonstrate, and `make demo` shows it against
the running system rather than asserting it in a test.

---

## Cross-cutting

```mermaid
flowchart LR
    subgraph request["One checkout request"]
        direction TB
        A[Gateway span] --> B[Order span]
        B --> C[Catalog span]
        B --> D[Inventory span]
        B --> E[Payment span]
    end

    subgraph async["Same trace, after the response"]
        direction TB
        F[outbox row carries<br/>the traceparent] --> G[consumer joins<br/>the same trace]
    end

    request -.->|traceparent captured at event creation| async
```

Two things that are easy to get wrong and are deliberate here:

- **The error taxonomy is transport-neutral.** `pkg/apperr` classifies a failure
  once; the gRPC and HTTP layers each translate it. Domain code imports neither
  `grpc` nor `net/http`.
- **The trace context is captured where the event is created, not where it is
  published.** The outbox poller runs seconds later on its own goroutine; injecting
  the context there would attach the event to the poller and break the causal
  chain. See [ADR-0002](../adr/0002-transactional-outbox.md).
