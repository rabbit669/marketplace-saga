-- =============================================================================
--  Initial database layout.
--
--  One database per service. This is not decoration: it makes accidental
--  cross-service joins impossible rather than merely discouraged, and it means
--  a service can be extracted to its own server without a migration project.
--
--  No foreign keys ever cross these boundaries. Referential integrity between
--  services is maintained by the saga, not by the database.
-- =============================================================================

CREATE DATABASE auth_db;
CREATE DATABASE catalog_db;
CREATE DATABASE inventory_db;
CREATE DATABASE order_db;
CREATE DATABASE payment_db;
CREATE DATABASE notification_db;

\connect auth_db
COMMENT ON DATABASE auth_db IS 'auth service: users, credentials, refresh tokens';

\connect catalog_db
COMMENT ON DATABASE catalog_db IS 'catalog service: products and categories';

\connect inventory_db
COMMENT ON DATABASE inventory_db IS 'inventory service: stock levels and reservations';

\connect order_db
COMMENT ON DATABASE order_db IS 'order service: orders, saga state, outbox';

\connect payment_db
COMMENT ON DATABASE payment_db IS 'payment service: payments, idempotency records, outbox';

\connect notification_db
COMMENT ON DATABASE notification_db IS 'notification service: processed event ids, for deduplication';
