-- Payment service schema.
--
-- One table. A payment is a record of what an external provider decided, not a
-- graph of related things, so a second table would buy nothing. What matters
-- here is not the shape of the row but its constraints: the idempotency
-- guarantee the checkout saga leans on is the unique index below, not a check
-- in application code, because a check in application code is a read that
-- another request can race between.
CREATE TABLE IF NOT EXISTS payments (
    id                    text NOT NULL,

    -- The order this payment was taken for. Not unique: a second authorize for
    -- the same order carries a different idempotency key and represents a
    -- genuine retry after a failure, so the history is kept and the newest
    -- record wins on lookup.
    order_id              text NOT NULL,

    -- The amount the provider was asked to hold, in minor units with its
    -- currency. Stored rather than recomputed from the order: an authorization
    -- is a promise about a specific sum, and a later price change must not
    -- appear to rewrite what the customer agreed to pay.
    amount_minor          bigint NOT NULL,
    currency              text NOT NULL,

    status                text NOT NULL,

    -- The caller's key, unique across the service. This is the load-bearing
    -- column: a retry presents the same key, and the insert that carries it
    -- either claims the key or loses to the request that already did.
    idempotency_key       text NOT NULL,

    -- The provider's token for the card. The service never sees a card number,
    -- which is the point of holding a token instead.
    payment_method_token  text NOT NULL DEFAULT '',

    -- Set only when the provider declined; empty otherwise.
    decline_reason        text NOT NULL DEFAULT '',

    -- The provider's own identifier, for reconciling against its dashboard when
    -- a customer disputes a charge.
    provider_reference    text NOT NULL DEFAULT '',

    -- Accumulated refunds, so that partial refunds can be applied without ever
    -- exceeding what was captured.
    refunded_minor        bigint NOT NULL DEFAULT 0,

    created_at            timestamptz NOT NULL,
    updated_at            timestamptz NOT NULL,

    -- The claim that makes concurrent retries safe. Two authorize calls with the
    -- same key must produce one payment and one charge, and a client retry after
    -- a timeout is precisely the moment they arrive at once. Uniqueness from the
    -- database is what settles that race; a SELECT followed by an INSERT lets
    -- both callers see the key as free and both reach the provider.
    CONSTRAINT payments_pkey PRIMARY KEY (id),
    CONSTRAINT payments_idempotency_key_key UNIQUE (idempotency_key),

    -- A coarse second net under the domain's own rules. These deliberately do
    -- not reproduce Rehydrate(): they catch the row a manual edit or a buggy
    -- older writer actually produces, and leave the subtler rules — a declined
    -- payment without a reason, for instance — to the read path, which reports
    -- them with a message that names the payment.
    CONSTRAINT payments_status_known CHECK (
        status IN ('pending', 'authorized', 'captured', 'declined', 'voided', 'refunded')
    ),
    CONSTRAINT payments_decline_reason_known CHECK (
        decline_reason IN (
            '', 'insufficient_funds', 'card_expired', 'fraud_suspected',
            'provider_timeout', 'provider_unavailable'
        )
    ),
    CONSTRAINT payments_idempotency_key_present CHECK (idempotency_key <> ''),
    CONSTRAINT payments_amount_positive CHECK (amount_minor > 0),
    CONSTRAINT payments_refunded_within_capture CHECK (
        refunded_minor >= 0 AND refunded_minor <= amount_minor
    )
);

-- The recovery worker's query: it knows the order but not whether an earlier
-- authorize attempt landed, so it asks for that order's newest payment. The
-- column order matches the store's ORDER BY exactly — newest first, with the id
-- as a tiebreaker — so the lookup is an index scan rather than a sort of every
-- payment the order ever had.
CREATE INDEX IF NOT EXISTS payments_order_created_idx
    ON payments (order_id, created_at DESC, id DESC);
