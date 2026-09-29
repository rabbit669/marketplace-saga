-- The outbox carries the trace context of the request that produced each event.
--
-- It cannot be recovered later: the poller runs seconds afterwards, on its own
-- goroutine, with no relationship to the checkout. Capturing the traceparent at
-- creation is what lets a consumer join the original trace instead of starting a
-- disconnected one — and a trace with a gap in the middle is more misleading
-- than no trace at all.
ALTER TABLE outbox
    ADD COLUMN IF NOT EXISTS trace_parent text NOT NULL DEFAULT '';
