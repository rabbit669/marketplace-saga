// Package postgres is the order service's durable storage.
//
// It implements the domain's Repository and IdempotencyStore, the saga engine's
// Store, and the transactional outbox. They live together because they must
// share transactions: an order and its outbox row commit together or not at all,
// and an idempotency key and the order it names are the same guarantee.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/marketplace/pkg/money"
	"github.com/marketplace/pkg/saga"

	"github.com/marketplace/services/order/internal/domain"
)

// Store is the order service's PostgreSQL adapter.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a store backed by the pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Pool exposes the pool so that the outbox poller and health checks can use it.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// ------------------------------------------------------------------ orders

// Create implements domain.Repository.
//
// The order and its lines go in one transaction. A partially written order —
// header without lines — would be an order whose total contradicts its contents,
// and Rehydrate would reject it on every subsequent read.
func (s *Store) Create(ctx context.Context, order *domain.Order) error {
	if order == nil {
		return fmt.Errorf("%w: order is nil", domain.ErrOrderNotFound)
	}

	return s.inTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO orders (
				id, customer_id, status, total_minor, currency,
				ship_country, ship_city, ship_street, ship_postal, ship_recipient,
				payment_id, reservation_id, failure_reason, created_at, updated_at, version
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$14,1)`,
			order.ID, order.CustomerID, string(order.Status),
			order.Total.Amount(), string(order.Total.Currency()),
			order.Shipping.Country, order.Shipping.City, order.Shipping.Street,
			order.Shipping.PostalCode, order.Shipping.Recipient,
			order.PaymentID, order.ReservationID, order.FailureReason,
			order.CreatedAt,
		)
		if err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("%w: %s", domain.ErrDuplicateOrder, order.ID)
			}
			return fmt.Errorf("insert order: %w", err)
		}

		return insertLines(ctx, tx, order)
	})
}

// CreateIdempotent implements domain.Repository.
//
// Both writes happen in one transaction. The key is inserted first, and a unique
// violation on it means another caller already claimed the key — in which case
// its order is committed too, because the insert and the claim were one
// transaction there as well. That is what makes the replay safe to read
// immediately.
func (s *Store) CreateIdempotent(ctx context.Context, order *domain.Order, key string) (string, bool, error) {
	if order == nil {
		return "", false, fmt.Errorf("%w: order is nil", domain.ErrOrderNotFound)
	}

	var existingOrderID string
	created := false

	err := s.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`INSERT INTO idempotency_keys (key, order_id) VALUES ($1, $2)
			 ON CONFLICT (key) DO NOTHING`, key, order.ID)
		if err != nil {
			return fmt.Errorf("claim idempotency key: %w", err)
		}

		if tag.RowsAffected() == 0 {
			// The key was already taken. The other caller's order is visible
			// because it committed in the same transaction as its claim.
			if err := tx.QueryRow(ctx,
				`SELECT order_id FROM idempotency_keys WHERE key = $1`, key).Scan(&existingOrderID); err != nil {
				return fmt.Errorf("read existing idempotency claim: %w", err)
			}
			return nil
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO orders (
				id, customer_id, status, total_minor, currency,
				ship_country, ship_city, ship_street, ship_postal, ship_recipient,
				payment_id, reservation_id, failure_reason, created_at, updated_at, version
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$14,1)`,
			order.ID, order.CustomerID, string(order.Status),
			order.Total.Amount(), string(order.Total.Currency()),
			order.Shipping.Country, order.Shipping.City, order.Shipping.Street,
			order.Shipping.PostalCode, order.Shipping.Recipient,
			order.PaymentID, order.ReservationID, order.FailureReason,
			order.CreatedAt,
		); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("%w: %s", domain.ErrDuplicateOrder, order.ID)
			}
			return fmt.Errorf("insert order: %w", err)
		}

		if err := insertLines(ctx, tx, order); err != nil {
			return err
		}

		created = true
		return nil
	})
	if err != nil {
		return "", false, err
	}

	if !created {
		return existingOrderID, false, nil
	}
	return order.ID, true, nil
}

// ReleaseIdempotencyKey implements domain.Repository.
func (s *Store) ReleaseIdempotencyKey(ctx context.Context, key string) error {
	if _, err := s.pool.Exec(ctx, `DELETE FROM idempotency_keys WHERE key = $1`, key); err != nil {
		return fmt.Errorf("release idempotency key: %w", err)
	}
	return nil
}

// DeleteExpiredKeys removes claims older than the retention window.
func (s *Store) DeleteExpiredKeys(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM idempotency_keys WHERE created_at < now() - $1::interval`,
		olderThan.String())
	if err != nil {
		return 0, fmt.Errorf("delete expired idempotency keys: %w", err)
	}
	return tag.RowsAffected(), nil
}

// Get implements domain.Repository.
func (s *Store) Get(ctx context.Context, id string) (*domain.Order, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, customer_id, status, total_minor, currency,
		       ship_country, ship_city, ship_street, ship_postal, ship_recipient,
		       payment_id, reservation_id, failure_reason, created_at, updated_at
		  FROM orders WHERE id = $1`, id)

	order, err := scanOrder(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", domain.ErrOrderNotFound, id)
		}
		return nil, fmt.Errorf("select order %s: %w", id, err)
	}

	lines, err := s.loadLines(ctx, id)
	if err != nil {
		return nil, err
	}
	order.Lines = lines

	// Re-validated on the way out. A migration, a manual edit or a bug in an
	// older writer can leave a row that violates the current rules, and the read
	// path is where every consumer benefits from catching it.
	rebuilt, err := domain.Rehydrate(*order)
	if err != nil {
		return nil, fmt.Errorf("order %s: stored row is invalid: %w", id, err)
	}
	return rebuilt, nil
}

// Update implements domain.Repository with optimistic concurrency.
//
// The version is incremented in the same statement as the write, so two workers
// cannot both believe they saved. A caller that loses the race gets a conflict
// rather than silently overwriting the other's changes.
func (s *Store) Update(ctx context.Context, order *domain.Order) error {
	if order == nil {
		return fmt.Errorf("%w: order is nil", domain.ErrOrderNotFound)
	}

	tag, err := s.pool.Exec(ctx, `
		UPDATE orders SET
			status = $2, payment_id = $3, reservation_id = $4, failure_reason = $5,
			updated_at = $6, version = version + 1
		WHERE id = $1`,
		order.ID, string(order.Status), order.PaymentID, order.ReservationID,
		order.FailureReason, order.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("update order %s: %w", order.ID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", domain.ErrOrderNotFound, order.ID)
	}
	return nil
}

func insertLines(ctx context.Context, tx pgx.Tx, order *domain.Order) error {
	for i, l := range order.Lines {
		_, err := tx.Exec(ctx, `
			INSERT INTO order_lines (
				order_id, line_no, product_id, sku, name, quantity, unit_price_minor, currency
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
			order.ID, i, l.ProductID, l.SKU, l.Name, l.Quantity,
			l.UnitPrice.Amount(), string(l.UnitPrice.Currency()),
		)
		if err != nil {
			return fmt.Errorf("insert order line %d: %w", i, err)
		}
	}
	return nil
}

func (s *Store) loadLines(ctx context.Context, orderID string) ([]domain.Line, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT product_id, sku, name, quantity, unit_price_minor, currency
		  FROM order_lines WHERE order_id = $1 ORDER BY line_no`, orderID)
	if err != nil {
		return nil, fmt.Errorf("select order lines for %s: %w", orderID, err)
	}
	defer rows.Close()

	var lines []domain.Line
	for rows.Next() {
		var (
			l        domain.Line
			minor    int64
			currency string
		)
		if err := rows.Scan(&l.ProductID, &l.SKU, &l.Name, &l.Quantity, &minor, &currency); err != nil {
			return nil, fmt.Errorf("scan order line: %w", err)
		}

		price, err := money.New(minor, money.Currency(currency))
		if err != nil {
			return nil, fmt.Errorf("order %s: stored line price is invalid: %w", orderID, err)
		}
		l.UnitPrice = price
		lines = append(lines, l)
	}
	return lines, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanOrder(row rowScanner) (*domain.Order, error) {
	var (
		o        domain.Order
		status   string
		minor    int64
		currency string
	)

	err := row.Scan(
		&o.ID, &o.CustomerID, &status, &minor, &currency,
		&o.Shipping.Country, &o.Shipping.City, &o.Shipping.Street,
		&o.Shipping.PostalCode, &o.Shipping.Recipient,
		&o.PaymentID, &o.ReservationID, &o.FailureReason,
		&o.CreatedAt, &o.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	o.Status = domain.Status(status)

	total, err := money.New(minor, money.Currency(currency))
	if err != nil {
		return nil, fmt.Errorf("stored total is invalid: %w", err)
	}
	o.Total = total

	return &o, nil
}

// ------------------------------------------------------------- idempotency

// -------------------------------------------------------------- saga store

// SagaStore implements saga.Store.
type SagaStore struct {
	pool *pgxpool.Pool
}

// NewSagaStore returns a saga store backed by the pool.
func NewSagaStore(pool *pgxpool.Pool) *SagaStore { return &SagaStore{pool: pool} }

// Create implements saga.Store.
func (s *SagaStore) Create(ctx context.Context, inst *saga.Instance) error {
	steps, err := json.Marshal(inst.Steps)
	if err != nil {
		return fmt.Errorf("saga %s: encode steps: %w", inst.ID, err)
	}
	data, err := json.Marshal(inst.Data)
	if err != nil {
		return fmt.Errorf("saga %s: encode data: %w", inst.ID, err)
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO sagas (id, name, status, steps, data, attempts, last_error, failed_step,
		                   started_at, updated_at, version)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,1)`,
		inst.ID, inst.Name, string(inst.Status), steps, data,
		inst.Attempts, inst.LastError, inst.FailedStep, inst.StartedAt, inst.UpdatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: %s", saga.ErrAlreadyExists, inst.ID)
		}
		return fmt.Errorf("insert saga %s: %w", inst.ID, err)
	}

	inst.Version = 1
	return nil
}

// Get implements saga.Store.
func (s *SagaStore) Get(ctx context.Context, id string) (*saga.Instance, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, name, status, steps, data, attempts, last_error, failed_step,
		       started_at, updated_at, version
		  FROM sagas WHERE id = $1`, id)

	inst, err := scanSaga(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", saga.ErrNotFound, id)
		}
		return nil, fmt.Errorf("select saga %s: %w", id, err)
	}
	return inst, nil
}

// Save implements saga.Store with optimistic concurrency.
//
// The version check is in the WHERE clause rather than a preceding SELECT, so
// the comparison and the write are one atomic operation. A read followed by a
// write would let two workers both pass the check and both save.
func (s *SagaStore) Save(ctx context.Context, inst *saga.Instance) error {
	steps, err := json.Marshal(inst.Steps)
	if err != nil {
		return fmt.Errorf("saga %s: encode steps: %w", inst.ID, err)
	}
	data, err := json.Marshal(inst.Data)
	if err != nil {
		return fmt.Errorf("saga %s: encode data: %w", inst.ID, err)
	}

	tag, err := s.pool.Exec(ctx, `
		UPDATE sagas SET
			status = $2, steps = $3, data = $4, attempts = $5,
			last_error = $6, failed_step = $7, updated_at = $8, version = version + 1
		WHERE id = $1 AND version = $9`,
		inst.ID, string(inst.Status), steps, data, inst.Attempts,
		inst.LastError, inst.FailedStep, inst.UpdatedAt, inst.Version)
	if err != nil {
		return fmt.Errorf("update saga %s: %w", inst.ID, err)
	}
	if tag.RowsAffected() == 0 {
		// Either the row is gone or someone else moved the version on. Both mean
		// "this worker no longer owns the saga", which the caller treats the
		// same way.
		return fmt.Errorf("%w: %s", saga.ErrVersionConflict, inst.ID)
	}

	inst.Version++
	return nil
}

// ListStuck implements saga.Store.
func (s *SagaStore) ListStuck(ctx context.Context, olderThan time.Time, limit int) ([]*saga.Instance, error) {
	if limit <= 0 {
		limit = 50
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, name, status, steps, data, attempts, last_error, failed_step,
		       started_at, updated_at, version
		  FROM sagas
		 WHERE updated_at < $1
		   AND status NOT IN ('completed', 'compensated', 'failed')
		 ORDER BY updated_at
		 LIMIT $2`, olderThan, limit)
	if err != nil {
		return nil, fmt.Errorf("list stuck sagas: %w", err)
	}
	defer rows.Close()

	var out []*saga.Instance
	for rows.Next() {
		inst, err := scanSaga(rows)
		if err != nil {
			return nil, fmt.Errorf("scan stuck saga: %w", err)
		}
		out = append(out, inst)
	}
	return out, rows.Err()
}

func scanSaga(row rowScanner) (*saga.Instance, error) {
	var (
		inst     saga.Instance
		status   string
		stepsRaw []byte
		dataRaw  []byte
	)

	err := row.Scan(
		&inst.ID, &inst.Name, &status, &stepsRaw, &dataRaw,
		&inst.Attempts, &inst.LastError, &inst.FailedStep,
		&inst.StartedAt, &inst.UpdatedAt, &inst.Version,
	)
	if err != nil {
		return nil, err
	}

	inst.Status = saga.Status(status)

	if err := json.Unmarshal(stepsRaw, &inst.Steps); err != nil {
		return nil, fmt.Errorf("saga %s: stored steps are not readable: %w", inst.ID, err)
	}
	if err := json.Unmarshal(dataRaw, &inst.Data); err != nil {
		return nil, fmt.Errorf("saga %s: stored data is not readable: %w", inst.ID, err)
	}
	if inst.Data == nil {
		inst.Data = map[string]string{}
	}

	return &inst, nil
}

// ------------------------------------------------------------------ outbox

// OutboxEvent is a row in the outbox.
type OutboxEvent struct {
	ID          string
	AggregateID string
	Type        string
	Payload     []byte
	OccurredAt  time.Time
	TraceParent string
	Attempts    int
}

// Publish implements domain.EventPublisher.
//
// It writes to the outbox table, not to a broker. This method is called inside
// the same transaction as the state change that produced the event, which is the
// entire point: the two commit together or not at all, so there is no window in
// which an order is confirmed and its event is lost.
func (s *Store) Publish(ctx context.Context, event domain.Event) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO outbox (id, aggregate_id, event_type, payload, occurred_at, trace_parent)
		VALUES ($1,$2,$3,$4,$5,$6)`,
		event.ID, event.AggregateID, event.Type, event.Payload, event.OccurredAt, event.TraceParent)
	if err != nil {
		return fmt.Errorf("insert outbox event %s: %w", event.ID, err)
	}
	return nil
}

// PendingEvents returns unpublished events in insertion order.
//
// Ordered by (occurred_at, id) so that events for one aggregate are handled in
// the order they were produced. Out-of-order delivery within an aggregate is
// what makes consumers apply stale state.
func (s *Store) PendingEvents(ctx context.Context, limit int) ([]OutboxEvent, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, aggregate_id, event_type, payload, occurred_at, trace_parent, attempts
		  FROM outbox
		 WHERE published_at IS NULL
		 ORDER BY occurred_at, id
		 LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("select pending outbox events: %w", err)
	}
	defer rows.Close()

	var out []OutboxEvent
	for rows.Next() {
		var e OutboxEvent
		if err := rows.Scan(&e.ID, &e.AggregateID, &e.Type, &e.Payload, &e.OccurredAt, &e.TraceParent, &e.Attempts); err != nil {
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// MarkPublished records that an event reached the broker.
func (s *Store) MarkPublished(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids)
	if err != nil {
		return fmt.Errorf("mark outbox events published: %w", err)
	}
	return nil
}

// MarkFailed records a publish attempt that failed, so that a poison message is
// visible rather than retried forever in silence.
func (s *Store) MarkFailed(ctx context.Context, id string, cause error) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE outbox SET attempts = attempts + 1, last_error = $2 WHERE id = $1`,
		id, cause.Error())
	if err != nil {
		return fmt.Errorf("record outbox failure: %w", err)
	}
	return nil
}

// ----------------------------------------------------------------- helpers

// inTx runs fn in a transaction.
func (s *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// isUniqueViolation reports whether err is a PostgreSQL unique constraint
// violation (SQLSTATE 23505).
//
// It checks the code rather than matching on message text, which changes between
// server versions and locales.
func isUniqueViolation(err error) bool {
	var pgErr interface{ SQLState() string }
	return errors.As(err, &pgErr) && pgErr.SQLState() == "23505"
}
