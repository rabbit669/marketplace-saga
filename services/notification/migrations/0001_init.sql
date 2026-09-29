-- Notification service schema.
--
-- One table, and it exists because Kafka delivery is at-least-once: a consumer
-- that processes an event and then dies before committing its offset sees that
-- event again. Without a record of what has been handled, the customer receives
-- the same email twice.
--
-- The id is the event id, and it is the primary key rather than an indexed
-- column: deduplication has to be decided by a constraint, because a check
-- followed by a write races against a concurrent redelivery of the same event.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     text PRIMARY KEY,
    event_type   text NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
);

-- Retention scans delete by age. Delivery is at-least-once but redelivery
-- happens within seconds, so a day of history is orders of magnitude more than
-- the window that matters.
CREATE INDEX IF NOT EXISTS processed_events_age_idx
    ON processed_events (processed_at);
