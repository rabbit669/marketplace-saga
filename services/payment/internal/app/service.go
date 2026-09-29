// Package app holds the payment service's use cases.
//
// The orchestrating idea is that the idempotency key is claimed *before* the
// provider is called, by writing a pending payment whose key is unique. That
// ordering is what makes concurrent retries safe: a second caller cannot get
// past the write, so it can never reach the provider and charge twice.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/marketplace/pkg/idgen"
	"github.com/marketplace/pkg/money"

	"github.com/marketplace/services/payment/internal/domain"
)

// Service implements the payment use cases.
type Service struct {
	payments domain.Repository
	provider domain.Provider

	clock func() time.Time
	log   *slog.Logger
}

// ServiceConfig carries the service's dependencies.
type ServiceConfig struct {
	Payments domain.Repository
	Provider domain.Provider
	Logger   *slog.Logger
	Clock    func() time.Time
}

// NewService wires the use cases.
func NewService(cfg ServiceConfig) (*Service, error) {
	if cfg.Payments == nil {
		return nil, errors.New("payment service: repository is required")
	}
	if cfg.Provider == nil {
		return nil, errors.New("payment service: provider is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}

	return &Service{
		payments: cfg.Payments,
		provider: cfg.Provider,
		clock:    cfg.Clock,
		log:      cfg.Logger,
	}, nil
}

// AuthorizeRequest asks for funds to be held.
type AuthorizeRequest struct {
	OrderID            string
	Amount             money.Money
	IdempotencyKey     string
	PaymentMethodToken string
}

// AuthorizeResult is the outcome of an authorization.
type AuthorizeResult struct {
	// Payment is the payment record. When its Status is declined, the provider
	// refused — which is a *successful* call reporting a business outcome, not
	// an error. The proto models it this way because the payment object carries
	// the reason, and it matches how real providers behave.
	//
	// Callers must therefore check the status. Forgetting to is the one way this
	// design can bite, which is why the checkout saga's adapter does it in one
	// place and nowhere else.
	Payment *domain.Payment
	// IdempotentReplay is true when this call returned a payment produced by an
	// earlier call with the same key.
	IdempotentReplay bool
}

// Authorize holds funds on the customer's payment method.
func (s *Service) Authorize(ctx context.Context, req AuthorizeRequest) (AuthorizeResult, error) {
	if req.OrderID == "" {
		return AuthorizeResult{}, domain.AsAppError(
			fmt.Errorf("%w: order_id is required", domain.ErrInvalidPayment))
	}
	if req.IdempotencyKey == "" {
		return AuthorizeResult{}, domain.AsAppError(
			fmt.Errorf("%w: idempotency_key is required", domain.ErrInvalidPayment))
	}
	if !req.Amount.IsPositive() {
		return AuthorizeResult{}, domain.AsAppError(
			fmt.Errorf("%w: amount must be positive", domain.ErrInvalidPayment))
	}
	if req.PaymentMethodToken == "" {
		return AuthorizeResult{}, domain.AsAppError(
			fmt.Errorf("%w: payment_method_token is required", domain.ErrInvalidPayment))
	}

	payment, err := domain.NewPayment(domain.NewPaymentInput{
		ID:                 idgen.New(),
		OrderID:            req.OrderID,
		Amount:             req.Amount,
		IdempotencyKey:     req.IdempotencyKey,
		PaymentMethodToken: req.PaymentMethodToken,
		Now:                s.clock(),
	})
	if err != nil {
		return AuthorizeResult{}, domain.AsAppError(err)
	}

	// Claim the key by writing the payment in its pending state. If two callers
	// race, exactly one wins this write and the other is turned away here,
	// before either has touched the provider.
	if err := s.payments.Create(ctx, payment); err != nil {
		if errors.Is(err, domain.ErrDuplicateIdempotencyKey) {
			return s.replayAuthorize(ctx, req.IdempotencyKey)
		}
		return AuthorizeResult{}, fmt.Errorf("authorize: claim idempotency key: %w", err)
	}

	decision, err := s.provider.Authorize(ctx, domain.AuthorizeRequest{
		OrderID:            req.OrderID,
		Amount:             req.Amount,
		IdempotencyKey:     req.IdempotencyKey,
		PaymentMethodToken: req.PaymentMethodToken,
	})
	if err != nil {
		// The provider was never reached, so nothing happened and the key must
		// be released. Leaving the pending record behind would make every future
		// retry with this key fail as "already in progress" — turning a transient
		// outage into a permanently stuck checkout.
		s.releaseClaim(ctx, payment)
		return AuthorizeResult{}, domain.AsAppError(fmt.Errorf("%w: %w", domain.ErrProviderUnavailable, err))
	}

	now := s.clock()
	if decision.Approved {
		err = payment.MarkAuthorized(decision.ProviderReference, now)
	} else {
		err = payment.MarkDeclined(decision.DeclineReason, decision.ProviderReference, now)
	}
	if err != nil {
		s.releaseClaim(ctx, payment)
		return AuthorizeResult{}, domain.AsAppError(err)
	}

	if err := s.payments.Update(ctx, payment); err != nil {
		return AuthorizeResult{}, fmt.Errorf("authorize: persist decision: %w", err)
	}

	s.log.InfoContext(ctx, "payment authorized",
		slog.String("payment_id", payment.ID),
		slog.String("order_id", payment.OrderID),
		slog.String("status", string(payment.Status)),
		slog.String("amount", payment.Amount.String()),
	)

	return AuthorizeResult{Payment: payment}, nil
}

// replayAuthorize returns the payment an earlier call with this key produced.
func (s *Service) replayAuthorize(ctx context.Context, key string) (AuthorizeResult, error) {
	existing, err := s.payments.GetByIdempotencyKey(ctx, key)
	if err != nil {
		return AuthorizeResult{}, fmt.Errorf("authorize: load existing payment for key: %w", err)
	}

	if existing.Status == domain.StatusPending {
		// The first call is still waiting on the provider. Its outcome is
		// unknown, so replaying it is impossible and proceeding would risk a
		// second charge. Failing loudly is the only honest answer.
		return AuthorizeResult{}, domain.AsAppError(
			fmt.Errorf("%w: payment %s", domain.ErrPaymentInProgress, existing.ID))
	}

	s.log.InfoContext(ctx, "payment authorization replayed",
		slog.String("payment_id", existing.ID),
		slog.String("order_id", existing.OrderID),
		slog.String("status", string(existing.Status)),
	)

	return AuthorizeResult{Payment: existing, IdempotentReplay: true}, nil
}

// releaseClaim removes a pending payment whose provider call never produced a
// decision, freeing the idempotency key for a retry.
func (s *Service) releaseClaim(ctx context.Context, payment *domain.Payment) {
	if err := s.payments.Delete(ctx, payment.ID); err != nil {
		// The key stays claimed, which will block retries with it. Worth a loud
		// log because the symptom (a permanently stuck checkout) appears much
		// later than the cause.
		s.log.ErrorContext(ctx, "could not release an idempotency claim after a provider failure",
			slog.String("payment_id", payment.ID),
			slog.String("idempotency_key", payment.IdempotencyKey),
			slog.String("error", err.Error()),
		)
	}
}

// Capture takes funds that were previously authorized.
//
// It is idempotent on the payment id rather than on a supplied key: capturing
// twice is the same operation applied twice to the same record, and the second
// application is a no-op. A separate key would add nothing.
func (s *Service) Capture(ctx context.Context, paymentID string) (*domain.Payment, error) {
	if paymentID == "" {
		return nil, domain.AsAppError(fmt.Errorf("%w: payment_id is required", domain.ErrInvalidPayment))
	}

	payment, err := s.payments.GetByID(ctx, paymentID)
	if err != nil {
		return nil, domain.AsAppError(err)
	}

	if err := payment.MarkCaptured(s.clock()); err != nil {
		return nil, domain.AsAppError(err)
	}
	if err := s.payments.Update(ctx, payment); err != nil {
		return nil, fmt.Errorf("capture: persist: %w", err)
	}

	s.log.InfoContext(ctx, "payment captured",
		slog.String("payment_id", payment.ID),
		slog.String("order_id", payment.OrderID),
	)

	return payment, nil
}

// Void cancels an authorization that was never captured.
//
// A payment that does not exist is not an error. This runs as a saga
// compensation, and a compensation executes precisely when the caller does not
// know whether the forward step applied — so "there is nothing to void" must be
// indistinguishable from success.
func (s *Service) Void(ctx context.Context, paymentID, reason string) (*domain.Payment, error) {
	if paymentID == "" {
		return nil, nil
	}

	payment, err := s.payments.GetByID(ctx, paymentID)
	if err != nil {
		if errors.Is(err, domain.ErrPaymentNotFound) {
			s.log.InfoContext(ctx, "nothing to void: payment does not exist",
				slog.String("payment_id", paymentID),
				slog.String("reason", reason),
			)
			return nil, nil
		}
		return nil, domain.AsAppError(err)
	}

	if err := payment.MarkVoided(s.clock()); err != nil {
		return nil, domain.AsAppError(err)
	}
	if err := s.payments.Update(ctx, payment); err != nil {
		return nil, fmt.Errorf("void: persist: %w", err)
	}

	s.log.InfoContext(ctx, "payment authorization voided",
		slog.String("payment_id", payment.ID),
		slog.String("reason", reason),
	)

	return payment, nil
}

// Refund returns captured funds.
//
// It is deliberately not wired into the checkout saga's compensation. A refund
// is a new business transaction, visible to the customer and carrying its own
// rules and fees — not an undo. It is exposed for the separate, human-driven
// flow that handles cancellations after capture.
func (s *Service) Refund(ctx context.Context, paymentID string, amount money.Money, reason string) (*domain.Payment, error) {
	if paymentID == "" {
		return nil, domain.AsAppError(fmt.Errorf("%w: payment_id is required", domain.ErrInvalidPayment))
	}

	payment, err := s.payments.GetByID(ctx, paymentID)
	if err != nil {
		return nil, domain.AsAppError(err)
	}

	if !amount.IsValid() || amount.IsZero() {
		// No amount means "refund everything still refundable", which is the
		// common case and the one a caller is least likely to compute correctly
		// themselves.
		remaining := payment.Amount.Amount() - payment.RefundedMinor
		if remaining <= 0 {
			return payment, nil
		}
		amount = money.MustNew(remaining, payment.Amount.Currency())
	}

	if err := payment.MarkRefunded(amount, s.clock()); err != nil {
		return nil, domain.AsAppError(err)
	}
	if err := s.payments.Update(ctx, payment); err != nil {
		return nil, fmt.Errorf("refund: persist: %w", err)
	}

	s.log.InfoContext(ctx, "payment refunded",
		slog.String("payment_id", payment.ID),
		slog.String("amount", amount.String()),
		slog.String("reason", reason),
	)

	return payment, nil
}

// Get loads a payment by id, or by order id when no id is given.
func (s *Service) Get(ctx context.Context, paymentID, orderID string) (*domain.Payment, error) {
	switch {
	case paymentID != "":
		payment, err := s.payments.GetByID(ctx, paymentID)
		return payment, domain.AsAppError(err)

	case orderID != "":
		payment, err := s.payments.GetByOrderID(ctx, orderID)
		return payment, domain.AsAppError(err)

	default:
		return nil, domain.AsAppError(
			fmt.Errorf("%w: either payment_id or order_id is required", domain.ErrInvalidPayment))
	}
}
