-- Order service schema.
--
-- Three tables with different jobs, which is why they are separate rather than
-- one wide row: the aggregate, the command log that makes retries safe, and the
-- saga record that makes rollback possible. A single table would make each of
-- them harder to reason about and impossible to retain on its own schedule.

-- ---------------------------------------------------------------- orders
--
-- One row per order. The total and the line prices are stored, not derived: an
-- order must record the amount the customer agreed to pay, and joining to the
-- catalogue to recompute it would change the answer whenever marketing edits a
-- price.
CREATE TABLE IF NOT EXISTS orders (
    id              text PRIMARY KEY,
    customer_id     text NOT NULL,
    status          text NOT NULL,

    total_minor     bigint NOT NULL,
    currency        text NOT NULL,

    ship_country    text NOT NULL,
    ship_city       text NOT NULL,
    ship_street     text NOT NULL,
    ship_postal     text NOT NULL,
    ship_recipient  text NOT NULL,

    payment_id      text NOT NULL DEFAULT '',
    reservation_id  text NOT NULL DEFAULT '',
    failure_reason  text NOT NULL DEFAULT '',

    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL,

    -- Optimistic concurrency. Two saga workers must not interleave writes to the
    -- same order, and this is what detects the attempt.
    version         bigint NOT NULL DEFAULT 1,

    CONSTRAINT orders_status_known CHECK (
        status IN ('pending', 'confirmed', 'cancelled', 'failed')
    ),
    CONSTRAINT orders_total_positive CHECK (total_minor > 0)
);

-- The customer's order history is the only listing the API offers, and it is
-- always newest-first. The id is a UUID v7, so it is already time-ordered and
-- the index doubles as the sort.
CREATE INDEX IF NOT EXISTS orders_customer_idx
    ON orders (customer_id, id DESC);

CREATE TABLE IF NOT EXISTS order_lines (
    order_id          text NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    line_no           integer NOT NULL,

    product_id        text NOT NULL,
    sku               text NOT NULL,
    name              text NOT NULL,
    quantity          bigint NOT NULL,

    unit_price_minor  bigint NOT NULL,
    currency          text NOT NULL,

    PRIMARY KEY (order_id, line_no),
    CONSTRAINT order_lines_quantity_positive CHECK (quantity > 0)
);

-- ------------------------------------------------------------------ idempotency
--
-- A retried checkout must return the original order rather than placing a second
-- one. The primary key is the mechanism: a read followed by an insert races
-- under exactly the concurrent retries this exists to make safe, so the
-- uniqueness has to come from the database.
--
-- Rows expire. A key kept forever turns this into an unbounded log, and one
-- expired too soon reopens the double-charge window it closes; 24 hours covers
-- every plausible client retry.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key         text PRIMARY KEY,
    order_id    text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idempotency_keys_created_idx
    ON idempotency_keys (created_at);

-- ---------------------------------------------------------------------- sagas
--
-- The saga's steps and shared data are JSONB rather than columns because their
-- shape belongs to the definition, and the definition changes between
-- deployments. A saga that is mid-flight when a step is added must still load,
-- so the record is deliberately schema-light.
CREATE TABLE IF NOT EXISTS sagas (
    id           text PRIMARY KEY,
    name         text NOT NULL,
    status       text NOT NULL,
    steps        jsonb NOT NULL DEFAULT '[]'::jsonb,
    data         jsonb NOT NULL DEFAULT '{}'::jsonb,
    attempts     integer NOT NULL DEFAULT 0,
    last_error   text NOT NULL DEFAULT '',
    failed_step  text NOT NULL DEFAULT '',
    started_at   timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL,
    version      bigint NOT NULL DEFAULT 1,

    CONSTRAINT sagas_status_known CHECK (
        status IN ('running', 'completed', 'compensating', 'compensated', 'failed')
    )
);

-- The recovery worker's query: non-terminal sagas that have stopped moving.
-- Partial, because terminal sagas are never scanned for and would otherwise
-- dominate the index as the table grows.
CREATE INDEX IF NOT EXISTS sagas_stuck_idx
    ON sagas (updated_at)
    WHERE status NOT IN ('completed', 'compensated', 'failed');

-- --------------------------------------------------------------------- outbox
--
-- The event is written in the same transaction as the state change that produced
-- it. Publishing to a broker directly would leave a window where the database
-- committed and the message did not — an order nobody is ever notified about.
-- Delivery is at-least-once, so consumers deduplicate.
CREATE TABLE IF NOT EXISTS outbox (
    id            text PRIMARY KEY,
    aggregate_id  text NOT NULL,
    event_type    text NOT NULL,
    payload       bytea NOT NULL,

    occurred_at   timestamptz NOT NULL,
    published_at  timestamptz,
    attempts      integer NOT NULL DEFAULT 0,
    last_error    text NOT NULL DEFAULT '',

    CONSTRAINT outbox_attempts_non_negative CHECK (attempts >= 0)
);

-- The poller's query: unpublished rows in insertion order. Partial, so the index
-- stays small as published rows accumulate.
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox (occurred_at, id)
    WHERE published_at IS NULL;
