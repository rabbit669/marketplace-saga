-- Inventory service schema.
--
-- Three tables, because the service has three different things to remember: how
-- many units exist, what has been promised, and what a promise contained. The
-- split is not decoration. Storing the held quantity as a column of the stock row
-- is what lets a reservation be checked and applied with a row lock on one row,
-- and storing the lines separately is what lets a multi-line hold be
-- all-or-nothing in one transaction.

-- ------------------------------------------------------------- stock levels
--
-- One row per product. `reserved` is the number of units promised to active
-- reservations, and it is never stored as a difference: "5 on hand, all of them
-- spoken for" and "0 on hand" are different answers to an operator, and only the
-- first tells them an order is waiting.
CREATE TABLE IF NOT EXISTS stock_items (
    product_id  text PRIMARY KEY,
    quantity    bigint NOT NULL DEFAULT 0,
    reserved    bigint NOT NULL DEFAULT 0,
    updated_at  timestamptz NOT NULL,

    CONSTRAINT stock_items_quantity_non_negative CHECK (quantity >= 0),
    CONSTRAINT stock_items_reserved_non_negative CHECK (reserved >= 0)

    -- There is deliberately no CHECK (reserved <= quantity).
    --
    -- It looks like the invariant this service exists to protect, and it is not
    -- one: an operator may set an absolute on-hand count below what is already
    -- held — the shelf was miscounted, or written off — and the holds already
    -- granted are promises to customers who are mid-checkout, not cancellations.
    -- SetStock does not touch `reserved`, so such a row is legitimate and the
    -- constraint would reject a correct write. Availability is floored at zero
    -- wherever it is computed instead, and the guarantee that matters — a hold is
    -- only ever granted against availability that the row lock says is there —
    -- is enforced by the reserve statement itself.
);

-- ------------------------------------------------------------ reservations
--
-- A reservation is a promise: these units, for this order, until this deadline.
-- The id is the caller's — the order service passes its order id — and the
-- primary key is what makes a retried reservation idempotent. A read followed by
-- an insert would race under exactly the concurrent retries this exists to make
-- safe, so the claim has to come from the database.
--
-- Terminal rows are kept rather than deleted. The saga that owns one may still
-- ask about it and must be told "expired" rather than "unknown", and the id must
-- stay claimed so that a late retry cannot take a fresh hold under an id the saga
-- already believes it owns.
CREATE TABLE IF NOT EXISTS reservations (
    id              text PRIMARY KEY,
    order_id        text NOT NULL DEFAULT '',
    status          text NOT NULL,
    release_reason  text NOT NULL DEFAULT '',

    created_at      timestamptz NOT NULL,
    expires_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL,

    CONSTRAINT reservations_status_known CHECK (
        status IN ('active', 'committed', 'released', 'expired')
    ),
    CONSTRAINT reservations_deadline_after_creation CHECK (expires_at >= created_at)
);

-- The expiry worker's query: active holds whose deadline has passed. Partial,
-- because terminal rows are never swept for and would otherwise dominate the
-- index as they accumulate — which they do, by design, at one row per checkout.
CREATE INDEX IF NOT EXISTS reservations_due_idx
    ON reservations (expires_at)
    WHERE status = 'active';

-- ---------------------------------------------------------- reservation lines
--
-- One row per product a hold covers. line_no exists so that the order a caller
-- asked for survives a round trip: the reservation is stored and returned with
-- its lines in the order the request listed them, and nothing here is allowed to
-- depend on the order a database happened to return rows in.
CREATE TABLE IF NOT EXISTS reservation_lines (
    reservation_id  text NOT NULL REFERENCES reservations (id) ON DELETE CASCADE,
    line_no         integer NOT NULL,

    product_id      text NOT NULL,
    quantity        bigint NOT NULL,

    PRIMARY KEY (reservation_id, line_no),

    -- One line per product per reservation. The domain merges repeated products
    -- before a request reaches storage, and this makes that a fact rather than a
    -- convention: every path that returns held units iterates the lines, and two
    -- lines for one product would return the same units twice.
    CONSTRAINT reservation_lines_product_once UNIQUE (reservation_id, product_id),

    CONSTRAINT reservation_lines_quantity_positive CHECK (quantity > 0)
);
