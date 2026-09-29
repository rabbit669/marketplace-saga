// Package app holds the order service's use cases.
//
// It orchestrates: it loads domain objects, calls ports, and records what
// happened. It contains no business rules of its own — those live in `domain` —
// and no infrastructure — those live in `adapters`. What is left here is the
// sequence, and that is the thing worth reading.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/marketplace/pkg/apperr"
	"github.com/marketplace/pkg/resilience"
	"github.com/marketplace/pkg/saga"

	"github.com/marketplace/services/order/internal/domain"
)

// Step names. These appear in saga records, logs and the operator API, so they
// are part of the observable interface and must not be renamed casually: a saga
// that is mid-flight when a deployment renames a step becomes unreadable.
const (
	StepReserveStock     = "reserve_stock"
	StepAuthorizePayment = "authorize_payment"
	StepCapturePayment   = "capture_payment"
	StepCommitStock      = "commit_stock"
	StepConfirmOrder     = "confirm_order"
)

// Session keys. Values written by a step and read by its compensation must be
// persisted, because the compensation may run in a different process after a
// restart.
const (
	keyReservationID  = "reservation_id"
	keyPaymentID      = "payment_id"
	keyPaymentToken   = "payment_method_token"
	keyDeclinedReason = "declined_reason"
)

// SagaName identifies the checkout definition.
const SagaName = "checkout"

// CheckoutConfig carries the saga's dependencies.
type CheckoutConfig struct {
	Orders    domain.Repository
	Inventory domain.InventoryClient
	Payments  domain.PaymentClient
	Events    domain.EventPublisher

	Logger *slog.Logger
	Clock  func() time.Time

	// ReservationTTL bounds how long stock stays held if the saga never
	// finishes. It is the safety net beneath the recovery worker: a stuck saga
	// is fixed within seconds, but a saga whose service is gone for an hour must
	// not hold a customer's stock for that hour.
	ReservationTTL time.Duration

	// Retry applies to each step. A zero value means one attempt per step with
	// no delay, which is only appropriate in tests.
	Retry resilience.RetryPolicy
}

// CheckoutSaga builds the checkout saga definition.
//
// The step order encodes the whole safety argument, so it is worth stating
// plainly:
//
//  1. reserve_stock      reversible  (compensation: release the hold)
//  2. authorize_payment  reversible  (compensation: void the hold)
//  3. capture_payment    THE PIVOT   (no compensation — see below)
//  4. commit_stock       irreversible, idempotent
//  5. confirm_order      local, idempotent
//
// Everything that can be undone happens before anything that cannot. Capture is
// the pivot because reversing it is not a compensation but a refund: a new
// business transaction, visible to the customer, with its own rules and its own
// fees. Treating it as an undo would be a comforting lie.
//
// Steps 4 and 5 have no compensations either, but they are idempotent and are
// retried by the recovery worker, so a failure there is a delay rather than a
// permanent inconsistency.
type CheckoutSaga struct {
	cfg CheckoutConfig
}

// NewCheckoutSaga returns a checkout saga wired to its dependencies.
func NewCheckoutSaga(cfg CheckoutConfig) (*CheckoutSaga, error) {
	if cfg.Orders == nil {
		return nil, fmt.Errorf("checkout saga: orders repository is required")
	}
	if cfg.Inventory == nil {
		return nil, fmt.Errorf("checkout saga: inventory client is required")
	}
	if cfg.Payments == nil {
		return nil, fmt.Errorf("checkout saga: payment client is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.ReservationTTL <= 0 {
		cfg.ReservationTTL = 15 * time.Minute
	}

	return &CheckoutSaga{cfg: cfg}, nil
}

// Definition returns the saga definition for the engine.
func (c *CheckoutSaga) Definition() *saga.Definition {
	return &saga.Definition{
		Name:  SagaName,
		Retry: c.cfg.Retry,
		Steps: []saga.Step{
			{
				Name:       StepReserveStock,
				Execute:    c.reserveStock,
				Compensate: c.releaseStock,
				// A reserve that times out may still have taken the hold, so the
				// compensation runs even when the step reports failure. Releasing
				// a reservation that was never created is a no-op.
				CompensateOnFailure: true,
			},
			{
				Name:       StepAuthorizePayment,
				Execute:    c.authorizePayment,
				Compensate: c.voidPayment,
				// The same reasoning, and this is the step where it matters
				// most: an authorize that timed out may have placed a hold on
				// the customer's card. Voiding an authorization that does not
				// exist is a no-op, so running it is strictly safer than
				// assuming the call never landed.
				CompensateOnFailure: true,
			},
			{
				// The pivot. No Compensate: after this point the customer's
				// money has moved and undoing it is a refund, not a rollback.
				Name:    StepCapturePayment,
				Execute: c.capturePayment,
			},
			{
				Name:    StepCommitStock,
				Execute: c.commitStock,
			},
			{
				Name:    StepConfirmOrder,
				Execute: c.confirmOrder,
			},
		},
	}
}

// ---------------------------------------------------------------- forward

// reserveStock holds the ordered units.
//
// The reservation id is the order id. That choice is what makes this step safe
// to retry: a second attempt returns the existing hold instead of taking a
// second one, so a saga that retried after a timeout does not leave the
// customer unable to buy anything else.
func (c *CheckoutSaga) reserveStock(ctx context.Context, s *saga.Session) error {
	order, err := c.cfg.Orders.Get(ctx, s.ID())
	if err != nil {
		return fmt.Errorf("reserve stock: load order: %w", err)
	}

	lines := make([]domain.ReserveLine, 0, len(order.Lines))
	for _, l := range order.Lines {
		lines = append(lines, domain.ReserveLine{ProductID: l.ProductID, Quantity: l.Quantity})
	}

	result, err := c.cfg.Inventory.Reserve(ctx, domain.ReserveRequest{
		ReservationID: order.ID,
		OrderID:       order.ID,
		Lines:         lines,
		TTL:           c.cfg.ReservationTTL,
	})
	if err != nil {
		return fmt.Errorf("reserve stock for order %s: %w", order.ID, err)
	}

	// Recorded in both places on purpose. The session is what a compensation
	// reads after a restart; the order is what a support agent reads.
	s.Set(keyReservationID, result.ReservationID)

	order.RecordReservation(result.ReservationID)
	if err := c.cfg.Orders.Update(ctx, order); err != nil {
		return fmt.Errorf("reserve stock: persist reservation: %w", err)
	}

	c.cfg.Logger.InfoContext(ctx, "stock reserved",
		slog.String("order_id", order.ID),
		slog.String("reservation_id", result.ReservationID),
		slog.Bool("replayed", result.AlreadyExisted),
	)
	return nil
}

// releaseStock returns a hold to the pool. It is the compensation for
// reserveStock.
func (c *CheckoutSaga) releaseStock(ctx context.Context, s *saga.Session) error {
	reservationID, err := s.Require(keyReservationID)
	if err != nil {
		return err
	}

	reason := "checkout_failed"
	if failed := s.FailedStep(); failed != "" {
		reason = "checkout_failed:" + failed
	}

	if err := c.cfg.Inventory.Release(ctx, reservationID, reason); err != nil {
		return fmt.Errorf("release reservation %s: %w", reservationID, err)
	}

	order, err := c.cfg.Orders.Get(ctx, s.ID())
	if err == nil {
		order.RecordReservation("")
		if uerr := c.cfg.Orders.Update(ctx, order); uerr != nil {
			// The hold is already released, which is what matters. Failing the
			// compensation here would trigger a retry that has nothing to do.
			c.cfg.Logger.WarnContext(ctx, "released stock but could not clear the reservation id",
				slog.String("order_id", order.ID),
				slog.String("error", uerr.Error()),
			)
		}
	}

	c.cfg.Logger.InfoContext(ctx, "stock released",
		slog.String("order_id", s.ID()),
		slog.String("reservation_id", reservationID),
		slog.String("reason", reason),
	)
	return nil
}

// authorizePayment holds the funds.
func (c *CheckoutSaga) authorizePayment(ctx context.Context, s *saga.Session) error {
	order, err := c.cfg.Orders.Get(ctx, s.ID())
	if err != nil {
		return fmt.Errorf("authorize payment: load order: %w", err)
	}

	token, err := s.Require(keyPaymentToken)
	if err != nil {
		return err
	}

	result, err := c.cfg.Payments.Authorize(ctx, domain.AuthorizeRequest{
		OrderID: order.ID,
		Amount:  order.Total,
		// The order id is the idempotency key. A retried authorize therefore
		// returns the original authorization rather than placing a second hold
		// on the customer's card.
		IdempotencyKey:     order.ID,
		PaymentMethodToken: token,
	})
	if err != nil {
		return fmt.Errorf("authorize payment for order %s: %w", order.ID, err)
	}

	if result.Status == domain.PaymentDeclined {
		// Persist the provider's reason so the customer sees something more
		// useful than "payment failed".
		s.Set(keyDeclinedReason, result.DeclinedReason)

		// The provider's id is recorded even on decline: a decline is a real
		// record that support may need to look up.
		if result.PaymentID != "" {
			s.Set(keyPaymentID, result.PaymentID)
		}

		if !result.Retryable {
			// Retrying a declined card cannot succeed and only delays the
			// customer's answer, so it is classified as permanent.
			return apperr.FailedPrecondition("payment.Authorize",
				"payment declined: %s", result.DeclinedReason)
		}
		return apperr.Unavailable("payment.Authorize",
			"payment provider unavailable: %s", result.DeclinedReason)
	}

	s.Set(keyPaymentID, result.PaymentID)

	order.RecordPayment(result.PaymentID)
	if uerr := c.cfg.Orders.Update(ctx, order); uerr != nil {
		return fmt.Errorf("authorize payment: persist payment id: %w", uerr)
	}

	c.cfg.Logger.InfoContext(ctx, "payment authorized",
		slog.String("order_id", order.ID),
		slog.String("payment_id", result.PaymentID),
	)
	return nil
}

// voidPayment cancels an authorization that was never captured. It is the
// compensation for authorizePayment.
func (c *CheckoutSaga) voidPayment(ctx context.Context, s *saga.Session) error {
	paymentID, ok := s.Get(keyPaymentID)
	if !ok || paymentID == "" {
		// The provider declined before issuing an id, so there is no hold to
		// cancel. This is a successful compensation, not a failure: a
		// compensation must tolerate the forward step never having applied.
		c.cfg.Logger.InfoContext(ctx, "no payment authorization to void",
			slog.String("order_id", s.ID()),
		)
		return nil
	}

	reason := "checkout_failed"
	if failed := s.FailedStep(); failed != "" {
		reason = "checkout_failed:" + failed
	}

	if err := c.cfg.Payments.Void(ctx, paymentID, reason); err != nil {
		return fmt.Errorf("void payment %s: %w", paymentID, err)
	}

	c.cfg.Logger.InfoContext(ctx, "payment authorization voided",
		slog.String("order_id", s.ID()),
		slog.String("payment_id", paymentID),
	)
	return nil
}

// capturePayment takes the held funds. This is the pivot: there is no way back
// from here, which is why nothing that can fail permanently runs after it.
func (c *CheckoutSaga) capturePayment(ctx context.Context, s *saga.Session) error {
	paymentID, err := s.Require(keyPaymentID)
	if err != nil {
		return err
	}

	if err := c.cfg.Payments.Capture(ctx, paymentID, s.ID()); err != nil {
		return fmt.Errorf("capture payment %s: %w", paymentID, err)
	}

	c.cfg.Logger.InfoContext(ctx, "payment captured",
		slog.String("order_id", s.ID()),
		slog.String("payment_id", paymentID),
	)
	return nil
}

// commitStock converts the hold into a permanent deduction.
func (c *CheckoutSaga) commitStock(ctx context.Context, s *saga.Session) error {
	reservationID, err := s.Require(keyReservationID)
	if err != nil {
		return err
	}

	if err := c.cfg.Inventory.Commit(ctx, reservationID); err != nil {
		return fmt.Errorf("commit reservation %s: %w", reservationID, err)
	}

	c.cfg.Logger.InfoContext(ctx, "stock committed",
		slog.String("order_id", s.ID()),
		slog.String("reservation_id", reservationID),
	)
	return nil
}

// confirmOrder marks the order confirmed and announces it.
//
// The status update and the event are written through the same repository call
// by the outbox adapter, so a confirmed order always has a matching event. See
// ADR-0002.
func (c *CheckoutSaga) confirmOrder(ctx context.Context, s *saga.Session) error {
	order, err := c.cfg.Orders.Get(ctx, s.ID())
	if err != nil {
		return fmt.Errorf("confirm order: load: %w", err)
	}

	if err := order.MarkConfirmed(c.cfg.Clock()); err != nil {
		return err
	}
	if err := c.cfg.Orders.Update(ctx, order); err != nil {
		return fmt.Errorf("confirm order: persist: %w", err)
	}

	if c.cfg.Events != nil {
		if err := c.publishOrderConfirmed(ctx, order); err != nil {
			return err
		}
	}

	c.cfg.Logger.InfoContext(ctx, "order confirmed",
		slog.String("order_id", order.ID),
		slog.String("total", order.Total.String()),
	)
	return nil
}

// --------------------------------------------------------------- helpers

// CancellationReason turns a saga outcome into a reason a support agent can act
// on, rather than a stack trace.
func CancellationReason(inst *saga.Instance) string {
	switch {
	case inst == nil:
		return "checkout failed"
	case inst.FailedStep == "":
		return "checkout was rolled back"
	default:
		return "checkout failed at step " + inst.FailedStep
	}
}
