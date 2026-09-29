-- Catalog service schema.
--
-- One table. The catalogue is the simplest aggregate in the platform — a product
-- has no children, no history and no lifecycle beyond a sale flag — so a second
-- table would buy nothing and cost a join on every price lookup, which is the
-- service's hottest read.

CREATE TABLE IF NOT EXISTS products (
    id           text PRIMARY KEY,
    sku          text NOT NULL,
    name         text NOT NULL,
    description  text NOT NULL DEFAULT '',

    -- The price is stored as minor units and a currency, never a float: an
    -- order copies this number into its total, and a rounding difference would
    -- surface as a customer being charged an amount nobody agreed to.
    price_minor  bigint NOT NULL,
    currency     text NOT NULL,

    category_id  text NOT NULL DEFAULT '',

    -- The attribute map is schema-flexible by design (colour, size, material),
    -- so it is jsonb rather than a table of key/value rows or a column per
    -- attribute. It is data a shopper reads, never something the database
    -- filters or joins on.
    attributes   jsonb NOT NULL DEFAULT '{}'::jsonb,

    is_active    boolean NOT NULL DEFAULT true,

    created_at   timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL,

    CONSTRAINT products_price_non_negative CHECK (price_minor >= 0),
    -- A coarse second net under the domain's rule that a name is required. It
    -- is deliberately not an attempt to reproduce Validate(): btrim() strips
    -- spaces, not every Unicode blank, so a name of a tab still passes here and
    -- is caught by Rehydrate on the way out. The constraint exists to stop the
    -- obviously empty row, which is the one a manual edit actually produces.
    CONSTRAINT products_name_not_blank CHECK (length(btrim(name)) > 0)
);

-- The SKU is the catalogue's natural key, and this index is what enforces it.
--
-- A read-then-insert would be the obvious implementation and would be wrong:
-- two concurrent creates of the same SKU both find nothing, both insert, and
-- the catalogue ends up with two products that customers cannot tell apart —
-- which product the order service then prices is up to whichever row it reads
-- first. Uniqueness has to be a fact the database maintains inside the write,
-- which is exactly what a unique index is, so the adapter inserts and maps the
-- resulting unique violation onto domain.ErrDuplicateSKU.
--
-- The index is named explicitly because the adapter tells the two uniqueness
-- failures apart by that name: a clash here is a duplicate SKU, a clash on the
-- primary key is a duplicate id, and the caller is told which.
--
-- Uniqueness is on the *normalised* SKU ("WIDGET-01"), so the adapter
-- normalises before writing. Storing the raw value instead would make
-- "widget-01" and "WIDGET-01" two products, which is precisely the duplicate
-- the domain's NormalizeSKU exists to prevent.
CREATE UNIQUE INDEX IF NOT EXISTS products_sku_key ON products (sku);

-- Listing pages are walked in ascending id order, and an id is a UUID v7, so
-- ascending id is also creation order. The primary key already provides that
-- order; this index exists for the filtered listing — "kitchen, newest first" —
-- where it lets PostgreSQL find the category's rows without scanning the
-- catalogue.
CREATE INDEX IF NOT EXISTS products_category_idx ON products (category_id, id);
