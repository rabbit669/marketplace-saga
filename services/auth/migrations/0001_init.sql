-- Auth service schema.
--
-- Two tables. The account and the refresh-token chains descended from its logins
-- are separate because their lifetimes are: a chain is rotated on every refresh
-- and kept after revocation as the record of a leak, while the account outlives
-- all of it. Both are small, and each is dominated by one question — "is this
-- address taken?" and "is this token still usable?" — so the constraints and
-- indexes below are chosen for those.

CREATE TABLE IF NOT EXISTS users (
    id              text NOT NULL,

    -- The address in its normalised form: lower-cased and trimmed, exactly as
    -- domain.NormaliseEmail produces it. Storing the raw form would let
    -- `Alice@example.com` and `alice@example.com` become two accounts, and the
    -- unique constraint below would then guarantee nothing that matters.
    --
    -- That normalisation is the store's job rather than a CHECK such as
    -- `email = lower(btrim(email))`, and the reason is a disagreement about
    -- what "lower" means: PostgreSQL follows the database collation while Go
    -- follows Unicode simple case mapping, so for a handful of addresses the
    -- two differ and the constraint would reject a real person's registration.
    -- It would close a hole only a manual edit opens, at the price of a hard
    -- failure on the happy path. A row written outside the store is caught on
    -- the read path instead, which refuses to load an address it cannot
    -- normalise.
    email           text NOT NULL,

    full_name       text NOT NULL,

    -- A bcrypt hash, never a password. The cost is part of the hash, so raising
    -- BcryptCost later does not invalidate existing rows.
    password_hash   text NOT NULL,

    -- Roles as an array: the set is read whole on every authorization decision
    -- and never queried by role, so a join table would buy nothing.
    roles           text[] NOT NULL DEFAULT '{}',

    email_verified  boolean NOT NULL DEFAULT false,

    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL,

    -- Registration is where two requests for the same address genuinely race:
    -- a double-clicked signup form, a retried request, or two instances behind
    -- a load balancer. This is what makes exactly one of them win. A SELECT
    -- before the INSERT lets both callers observe the address as free, which is
    -- how one person ends up with two accounts and two password hashes.
    CONSTRAINT users_pkey PRIMARY KEY (id),
    CONSTRAINT users_email_key UNIQUE (email),
    CONSTRAINT users_email_present CHECK (email <> '')
);

CREATE TABLE IF NOT EXISTS refresh_tokens (
    id          text NOT NULL,

    user_id     text NOT NULL REFERENCES users (id) ON DELETE CASCADE,

    -- The SHA-256 of the token's secret, and the only representation of the
    -- credential that is ever written here. Refresh tokens are 256 bits of
    -- randomness rather than passwords, so a fast hash is the right one: there
    -- is no dictionary to attack, and a stolen dump yields nothing that can be
    -- presented to the API.
    --
    -- The id is stored as given: it identifies the row, it is not the secret,
    -- and a token can only be exchanged by presenting the secret whose hash
    -- matches this column.
    token_hash  text NOT NULL,

    -- Groups the tokens descended from one login. Rotation keeps the family, so
    -- a reuse detected three rotations later still names the chain it came from
    -- and the whole chain can be revoked in one statement.
    family_id   text NOT NULL,

    issued_at   timestamptz NOT NULL,
    expires_at  timestamptz NOT NULL,

    -- NULL until the token is consumed, explicitly revoked, or caught in a
    -- family revocation. NULL rather than a sentinel date so that "still live"
    -- is a fact the database can filter on rather than a value every query has
    -- to remember.
    revoked_at  timestamptz
);

-- RevokeFamily's statement: one indexed UPDATE across the family. It is the
-- response to a detected reuse, so it has to reach every token of the chain,
-- including the ones issued after the leak.
CREATE INDEX IF NOT EXISTS refresh_tokens_family_idx
    ON refresh_tokens (family_id);
