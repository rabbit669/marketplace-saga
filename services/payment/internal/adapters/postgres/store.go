// Package postgres is the payment service's durable storage.
//
// It implements the domain's Repository. The two rules that make the checkout
// saga's retries safe — one payment per id, one payment per idempotency key —
// are enforced by the schema rather than by the code here, because both are
// questions about concurrent writers and only the database can answer those
// atomically. Everything in this file is bookkeeping around that.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/marketplace/pkg/money"

	"github.com/marketplace/services/payment/internal/domain"
)

// The constraint names the store distinguishes between. The migration declares
// each of them explicitly, because they are a contract: a renamed constraint
// would silently turn "this idempotency key is already claimed" — an expected
// outcome the caller replays — into a generic insert failure.
const (
	constraintPaymentID  = "payments_pkey"
	constraintPaymentKey = "payments_idempotency_key_key"
)

// pgUniqueViolation is SQLSTATE 23505.
const pgUniqueViolation = "23505"

// Store is the payment service's PostgreSQL adapter.
type Store struct {
	pool *pgxpool.Pool
}

// The store is a domain.Repository, checked here rather than at the call site so
// that a signature drift fails the build instead of a customer's checkout.
var _ domain.Repository = (*Store)(nil)

// New returns a store backed by the pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Pool exposes the pool so that the health check and metrics can use it.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// ----------------------------------------------------------------- payments

// Create implements domain.Repository.
//
// The idempotency key is claimed by the insert itself. The unique index on
// idempotency_key makes claiming and storing one atomic act, so of any number of
// concurrent retries carrying the same key exactly one gets past this method —
// and the others are turned away here, before any of them has reached the
// provider. A SELECT followed by an INSERT would let every caller observe the
// key as free, which is the double charge this exists to prevent.
func (s *Store) Create(ctx context.Context, p *domain.Payment) error {
	if p == nil {
		return fmt.Errorf("%w: payment is nil", domain.ErrInvalidPayment)
	}

	_, err := s.pool.Exec(ctx, `
		INSERT INTO payments (
			id, order_id, amount_minor, currency, status, idempotency_key,
			payment_method_token, decline_reason, provider_reference,
			refunded_minor, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		p.ID, p.OrderID, p.Amount.Amount(), string(p.Amount.Currency()), string(p.Status),
		p.IdempotencyKey, p.PaymentMethodToken, string(p.DeclineReason),
		p.ProviderReference, p.RefundedMinor, p.CreatedAt, p.UpdatedAt,
	)
	if err != nil {
		// The two unique constraints mean different things to the caller: a
		// taken key is an expected replay, a taken id is a bug upstream. They
		// are told apart by constraint name rather than collapsed into one
		// error, because the service decides whether to replay on that
		// difference.
		switch violatedConstraint(err) {
		case constraintPaymentKey:
			return fmt.Errorf("%w: %s", domain.ErrDuplicateIdempotencyKey, p.IdempotencyKey)
		case constraintPaymentID:
			return fmt.Errorf("%w: payment %s already exists", domain.ErrInvalidPayment, p.ID)
		}
		return fmt.Errorf("insert payment %s: %w", p.ID, err)
	}

	return nil
}

// GetByID implements domain.Repository.
func (s *Store) GetByID(ctx context.Context, id string) (*domain.Payment, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, order_id, amount_minor, currency, status, idempotency_key,
		       payment_method_token, decline_reason, provider_reference,
		       refunded_minor, created_at, updated_at
		  FROM payments WHERE id = $1`, id)

	p, err := scanPayment(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", domain.ErrPaymentNotFound, id)
		}
		return nil, fmt.Errorf("select payment %s: %w", id, err)
	}

	return rehydrate(p)
}

// GetByIdempotencyKey implements domain.Repository.
//
// It answers "what did the first call with this key produce?", which is what a
// retry that lost the claim asks. The lookup is served by the same unique index
// that makes the claim atomic.
func (s *Store) GetByIdempotencyKey(ctx context.Context, key string) (*domain.Payment, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, order_id, amount_minor, currency, status, idempotency_key,
		       payment_method_token, decline_reason, provider_reference,
		       refunded_minor, created_at, updated_at
		  FROM payments WHERE idempotency_key = $1`, key)

	p, err := scanPayment(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: no payment for idempotency key %s",
				domain.ErrPaymentNotFound, key)
		}
		return nil, fmt.Errorf("select payment by idempotency key: %w", err)
	}

	return rehydrate(p)
}

// GetByOrderID implements domain.Repository.
//
// One payment per order is an expectation of this service rather than a
// constraint on the table, so the newest record wins — the in-memory store
// orders by CreatedAt and this expresses the same rule in SQL. The id breaks
// ties: it is a UUIDv7, so it is itself time-ordered, and without it a
// sub-microsecond difference in the caller's clock would leave the answer down
// to physical row order.
func (s *Store) GetByOrderID(ctx context.Context, orderID string) (*domain.Payment, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, order_id, amount_minor, currency, status, idempotency_key,
		       payment_method_token, decline_reason, provider_reference,
		       refunded_minor, created_at, updated_at
		  FROM payments
		 WHERE order_id = $1
		 ORDER BY created_at DESC, id DESC
		 LIMIT 1`, orderID)

	p, err := scanPayment(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: no payment for order %s", domain.ErrPaymentNotFound, orderID)
		}
		return nil, fmt.Errorf("select payment for order %s: %w", orderID, err)
	}

	return rehydrate(p)
}

// Update implements domain.Repository.
//
// The idempotency key is immutable, and the rule is enforced as part of the
// WHERE clause rather than by reading the row first: a read followed by a write
// leaves a window in which the key could change underneath, and the whole point
// of the key is that it cannot move without releasing a claim the caller still
// believes it holds.
//
// Fields that describe the original transaction — the order, the amount and its
// currency, the created instant — are not updated. Nothing in the domain rewrites
// them after construction, and allowing it through this method would let a buggy
// caller restate what the customer agreed to pay.
func (s *Store) Update(ctx context.Context, p *domain.Payment) error {
	if p == nil {
		return fmt.Errorf("%w: payment is nil", domain.ErrInvalidPayment)
	}

	tag, err := s.pool.Exec(ctx, `
		UPDATE payments SET
			status = $2, decline_reason = $3, provider_reference = $4,
			payment_method_token = $5, refunded_minor = $6, updated_at = $7
		WHERE id = $1 AND idempotency_key = $8`,
		p.ID, string(p.Status), string(p.DeclineReason), p.ProviderReference,
		p.PaymentMethodToken, p.RefundedMinor, p.UpdatedAt, p.IdempotencyKey,
	)
	if err != nil {
		return fmt.Errorf("update payment %s: %w", p.ID, err)
	}
	if tag.RowsAffected() > 0 {
		return nil
	}

	// Nothing matched. Either the payment is gone or the caller is trying to
	// move it to a different key, and the caller needs to know which — one is a
	// missing record, the other is a rejected request.
	return s.explainUpdateMiss(ctx, p)
}

// Delete implements domain.Repository.
//
// Deleting the row releases the idempotency key with it, because the payment row
// *is* the claim: the unique index cannot be satisfied twice, and removing the
// row removes the claim in the same statement. That is what lets a caller whose
// provider call never produced a decision retry with the same key instead of
// being told forever that a payment is already in progress.
//
// A payment that does not exist is not an error. The only caller is a
// compensation, which runs precisely when it cannot know whether the forward
// step applied.
func (s *Store) Delete(ctx context.Context, id string) error {
	if _, err := s.pool.Exec(ctx, `DELETE FROM payments WHERE id = $1`, id); err != nil {
		return fmt.Errorf("delete payment %s: %w", id, err)
	}
	return nil
}

// ------------------------------------------------------------------ helpers

// rehydrate re-runs the domain's rules over a row read back from storage.
//
// A migration, a manual edit or a bug in an older writer can leave a row that
// violates the rules this version of the domain enforces, and the read path is
// where every consumer benefits from catching it: a corrupt payment that reached
// the checkout saga would look like a business decision rather than a data
// problem.
func rehydrate(p *domain.Payment) (*domain.Payment, error) {
	rebuilt, err := domain.Rehydrate(*p)
	if err != nil {
		return nil, fmt.Errorf("payment %s: stored row is invalid: %w", p.ID, err)
	}
	return rebuilt, nil
}

// explainUpdateMiss says why an update matched nothing.
//
// The read only classifies a failure the write has already decided; it cannot
// make the update succeed and it never decides state, so a concurrent change
// between the two statements can at worst mislabel the reason. The alternative —
// reading first and writing second — would introduce the race the WHERE clause
// exists to avoid.
func (s *Store) explainUpdateMiss(ctx context.Context, p *domain.Payment) error {
	var storedKey string
	err := s.pool.QueryRow(ctx,
		`SELECT idempotency_key FROM payments WHERE id = $1`, p.ID).Scan(&storedKey)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("%w: %s", domain.ErrPaymentNotFound, p.ID)
	case err != nil:
		return fmt.Errorf("update payment %s: %w", p.ID, err)
	default:
		return fmt.Errorf("%w: payment %s cannot change its idempotency key",
			domain.ErrInvalidPayment, p.ID)
	}
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanPayment(row rowScanner) (*domain.Payment, error) {
	var (
		p        domain.Payment
		minor    int64
		currency string
		status   string
		reason   string
	)

	err := row.Scan(
		&p.ID, &p.OrderID, &minor, &currency, &status, &p.IdempotencyKey,
		&p.PaymentMethodToken, &reason, &p.ProviderReference, &p.RefundedMinor,
		&p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	p.Status = domain.Status(status)
	p.DeclineReason = domain.DeclineReason(reason)

	amount, err := money.New(minor, money.Currency(currency))
	if err != nil {
		// money.New rejects anything that is not a three-letter code, and the
		// CHECK constraints cannot state that. An amount stored in a form the
		// money package cannot construct fails here rather than silently
		// becoming the zero value, which has no currency and is not an amount
		// anyone can act on.
		return nil, fmt.Errorf("stored amount is invalid: %w", err)
	}
	p.Amount = amount

	return &p, nil
}

// violatedConstraint returns the constraint a unique violation broke, or "" when
// err is not a unique violation.
//
// It reads the SQLSTATE and the constraint name rather than matching on message
// text, which changes between server versions and is localised.
func violatedConstraint(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
		return pgErr.ConstraintName
	}
	return ""
}
