// Package postgres is the notification service's durable storage.
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Deduper implements kafka.Deduper against PostgreSQL.
//
// It shares the store with nothing else, because the only thing this service
// persists is the record of what it has already handled.
type Deduper struct {
	pool *pgxpool.Pool
}

// NewDeduper returns a deduper backed by the pool.
func NewDeduper(pool *pgxpool.Pool) *Deduper { return &Deduper{pool: pool} }

// MarkProcessed implements kafka.Deduper.
//
// The decision is made by the primary key, not by a preceding SELECT. An
// `INSERT ... ON CONFLICT DO NOTHING` reports through its affected-row count
// whether this caller was the first, and two concurrent redeliveries of the same
// event cannot both see zero rows — which a read-then-write would allow, and
// which is exactly the case this exists to handle.
func (d *Deduper) MarkProcessed(ctx context.Context, eventID string) (bool, error) {
	tag, err := d.pool.Exec(ctx,
		`INSERT INTO processed_events (event_id, event_type) VALUES ($1, '')
		 ON CONFLICT (event_id) DO NOTHING`, eventID)
	if err != nil {
		return false, fmt.Errorf("record processed event %s: %w", eventID, err)
	}

	// No rows affected means the insert was a conflict: somebody already
	// handled it.
	return tag.RowsAffected() == 0, nil
}

// ForgetOlderThan deletes deduplication records past the retention window.
func (d *Deduper) ForgetOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	tag, err := d.pool.Exec(ctx,
		`DELETE FROM processed_events WHERE processed_at < now() - $1::interval`, age.String())
	if err != nil {
		return 0, fmt.Errorf("prune processed events: %w", err)
	}
	return tag.RowsAffected(), nil
}
