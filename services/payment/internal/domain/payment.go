// Package domain holds the payment service's rules.
//
// This service is an adapter for an external payment provider, so its domain is
// unusual: most of the interesting logic is about faithfully recording what the
// provider decided, rather than deciding anything itself. Two things it does
// own, and that the checkout saga depends on:
//
//  1. **Idempotency.** Two authorize calls with the same key must return the
//     same payment. Providers require this because a timeout is not a failure
//     but an unknown, and retrying without a key charges the customer twice.
//  2. **Retryability.** A decline is classified as permanent or transient, and
//     that classification travels back to the caller. Retrying an insufficient
//     funds decline cannot succeed and only delays the customer's answer.
package domain

import (
	"context"
	"fmt"
	"time"

	"github.com/marketplace/pkg/money"
)

// Status is the lifecycle state of a payment.
type Status string

const (
	// StatusPending means the provider has been asked but has not answered yet.
	// It exists so that the idempotency key can be claimed before the network
	// call, which is what makes two concurrent retries safe.
	StatusPending Status = "pending"
	// StatusAuthorized means funds are held but not taken. Compensatable.
	StatusAuthorized Status = "authorized"
	// StatusCaptured means funds were taken. Terminal, and not compensatable:
	// undoing it is a refund, a different business operation.
	StatusCaptured Status = "captured"
	// StatusDeclined means the provider refused. Terminal.
	StatusDeclined Status = "declined"
	// StatusVoided means a hold was cancelled before capture. Terminal.
	StatusVoided Status = "voided"
	// StatusRefunded means captured funds were returned. Terminal.
	StatusRefunded Status = "refunded"
)

// Terminal reports whether the payment has stopped moving.
func (s Status) Terminal() bool {
	switch s {
	case StatusCaptured, StatusDeclined, StatusVoided, StatusRefunded:
		return true
	default:
		return false
	}
}

// Valid reports whether s is a known status, guarding against a value read from
// storage that this version of the code does not understand.
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusAuthorized, StatusCaptured, StatusDeclined, StatusVoided, StatusRefunded:
		return true
	default:
		return false
	}
}

// DeclineReason explains why a provider refused, and encodes whether trying
// again could possibly help.
type DeclineReason string

const (
	// Permanent declines. Retrying these wastes the caller's latency budget and
	// the customer's patience, and in a saga it delays the compensation that
	// releases their reserved stock.
	ReasonInsufficientFunds DeclineReason = "insufficient_funds"
	ReasonCardExpired       DeclineReason = "card_expired"
	ReasonFraudSuspected    DeclineReason = "fraud_suspected"

	// Transient declines. The request may well succeed on a second attempt.
	ReasonProviderTimeout     DeclineReason = "provider_timeout"
	ReasonProviderUnavailable DeclineReason = "provider_unavailable"
)

// Retryable reports whether a decline of this kind is worth another attempt.
//
// This is a property of the reason, not of the caller's optimism, which is why
// it lives here rather than in a retry policy somewhere upstream.
func (r DeclineReason) Retryable() bool {
	switch r {
	case ReasonProviderTimeout, ReasonProviderUnavailable:
		return true
	default:
		return false
	}
}

// Valid reports whether r is a known reason.
func (r DeclineReason) Valid() bool {
	switch r {
	case ReasonInsufficientFunds, ReasonCardExpired, ReasonFraudSuspected,
		ReasonProviderTimeout, ReasonProviderUnavailable:
		return true
	default:
		return false
	}
}

// Payment is the aggregate root.
type Payment struct {
	ID      string
	OrderID string
	Amount  money.Money
	Status  Status

	// IdempotencyKey is the caller's key. It is unique across the service, and
	// the uniqueness is what makes concurrent retries safe — enforced by the
	// storage layer rather than by a check-then-write, which would race.
	IdempotencyKey string

	// PaymentMethodToken is the provider's token for the card. The service never
	// sees a card number.
	PaymentMethodToken string

	// DeclineReason is set when Status is declined.
	DeclineReason DeclineReason

	// ProviderReference is the provider's own identifier, useful when
	// reconciling against a real provider's dashboard.
	ProviderReference string

	// RefundedMinor tracks how much of the captured amount has been returned,
	// so that partial refunds can be accumulated without exceeding the capture.
	RefundedMinor int64

	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewPayment validates and builds a pending payment.
//
// The amount must be strictly positive: authorizing zero or a negative sum is
// always a bug upstream, and letting it through would create a payment record
// that no reconciliation can make sense of.
func NewPayment(in NewPaymentInput) (*Payment, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: id is required", ErrInvalidPayment)
	}
	if in.OrderID == "" {
		return nil, fmt.Errorf("%w: order_id is required", ErrInvalidPayment)
	}
	if in.IdempotencyKey == "" {
		return nil, fmt.Errorf("%w: idempotency_key is required", ErrInvalidPayment)
	}
	if !in.Amount.IsValid() {
		return nil, fmt.Errorf("%w: amount has no currency", ErrInvalidPayment)
	}
	if !in.Amount.IsPositive() {
		return nil, fmt.Errorf("%w: amount must be positive, got %s", ErrInvalidPayment, in.Amount)
	}

	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}

	return &Payment{
		ID:                 in.ID,
		OrderID:            in.OrderID,
		Amount:             in.Amount,
		Status:             StatusPending,
		IdempotencyKey:     in.IdempotencyKey,
		PaymentMethodToken: in.PaymentMethodToken,
		CreatedAt:          now,
		UpdatedAt:          now,
	}, nil
}

// NewPaymentInput is the input to NewPayment.
type NewPaymentInput struct {
	ID                 string
	OrderID            string
	Amount             money.Money
	IdempotencyKey     string
	PaymentMethodToken string
	Now                time.Time
}

// MarkAuthorized records that the provider is holding the funds.
func (p *Payment) MarkAuthorized(providerRef string, now time.Time) error {
	if p.Status != StatusPending {
		return fmt.Errorf("%w: cannot authorize a payment in status %s", ErrInvalidTransition, p.Status)
	}
	p.Status = StatusAuthorized
	p.ProviderReference = providerRef
	p.UpdatedAt = now
	return nil
}

// MarkDeclined records the provider's refusal and its reason.
func (p *Payment) MarkDeclined(reason DeclineReason, providerRef string, now time.Time) error {
	if p.Status != StatusPending {
		return fmt.Errorf("%w: cannot decline a payment in status %s", ErrInvalidTransition, p.Status)
	}
	if !reason.Valid() {
		return fmt.Errorf("%w: unknown decline reason %q", ErrInvalidPayment, reason)
	}
	p.Status = StatusDeclined
	p.DeclineReason = reason
	p.ProviderReference = providerRef
	p.UpdatedAt = now
	return nil
}

// MarkCaptured records that the funds were taken. It is idempotent: capturing an
// already captured payment succeeds, because the saga retries this step and a
// retry after a timeout must not fail.
func (p *Payment) MarkCaptured(now time.Time) error {
	switch p.Status {
	case StatusCaptured:
		return nil
	case StatusAuthorized:
		p.Status = StatusCaptured
		p.UpdatedAt = now
		return nil
	default:
		return fmt.Errorf("%w: cannot capture a payment in status %s", ErrInvalidTransition, p.Status)
	}
}

// MarkVoided cancels an authorization that was never captured.
//
// It is deliberately tolerant: voiding a declined payment, an already voided
// payment, or one that was never authorized all succeed. A compensation runs
// exactly when the caller does not know whether the forward step applied, so
// requiring the caller to know would defeat the purpose.
//
// Voiding a *captured* payment is refused. That is not a compensation failure —
// it is the caller asking for the wrong operation, and the money has moved.
func (p *Payment) MarkVoided(now time.Time) error {
	switch p.Status {
	case StatusVoided, StatusDeclined:
		// Nothing to undo, which is a successful outcome for a compensation.
		return nil
	case StatusAuthorized:
		p.Status = StatusVoided
		p.UpdatedAt = now
		return nil
	case StatusCaptured, StatusRefunded:
		return fmt.Errorf("%w: payment %s was captured and cannot be voided; a refund is required",
			ErrInvalidTransition, p.ID)
	default:
		return fmt.Errorf("%w: cannot void a payment in status %s", ErrInvalidTransition, p.Status)
	}
}

// MarkRefunded returns captured funds, in full or in part.
func (p *Payment) MarkRefunded(amount money.Money, now time.Time) error {
	if p.Status != StatusCaptured && p.Status != StatusRefunded {
		return fmt.Errorf("%w: cannot refund a payment in status %s", ErrInvalidTransition, p.Status)
	}
	if !amount.IsValid() || !amount.IsPositive() {
		return fmt.Errorf("%w: refund amount must be positive", ErrInvalidPayment)
	}
	if amount.Currency() != p.Amount.Currency() {
		return fmt.Errorf("%w: refund in %s does not match payment currency %s",
			ErrInvalidPayment, amount.Currency(), p.Amount.Currency())
	}

	refundable := p.Amount.Amount() - p.RefundedMinor
	if amount.Amount() > refundable {
		return fmt.Errorf("%w: refund of %s exceeds the refundable %d minor units",
			ErrInvalidPayment, amount, refundable)
	}

	p.RefundedMinor += amount.Amount()
	if p.RefundedMinor == p.Amount.Amount() {
		p.Status = StatusRefunded
	}
	p.UpdatedAt = now
	return nil
}

// IsCompensatable reports whether the payment can still be released back to the
// customer by a saga compensation.
func (p *Payment) IsCompensatable() bool {
	return p.Status == StatusAuthorized || p.Status == StatusPending
}

// Rehydrate rebuilds a payment from storage, re-running validation.
func Rehydrate(p Payment) (*Payment, error) {
	if p.ID == "" || p.OrderID == "" || p.IdempotencyKey == "" {
		return nil, fmt.Errorf("%w: stored payment is missing its identifiers", ErrInvalidPayment)
	}
	if !p.Status.Valid() {
		return nil, fmt.Errorf("%w: stored payment %s has unknown status %q", ErrInvalidPayment, p.ID, p.Status)
	}
	if !p.Amount.IsValid() || !p.Amount.IsPositive() {
		return nil, fmt.Errorf("%w: stored payment %s has an invalid amount", ErrInvalidPayment, p.ID)
	}
	if p.Status == StatusDeclined && !p.DeclineReason.Valid() {
		return nil, fmt.Errorf("%w: stored payment %s is declined without a reason", ErrInvalidPayment, p.ID)
	}
	if p.RefundedMinor < 0 || p.RefundedMinor > p.Amount.Amount() {
		return nil, fmt.Errorf("%w: stored payment %s has %d refunded of %d",
			ErrInvalidPayment, p.ID, p.RefundedMinor, p.Amount.Amount())
	}

	cp := p
	return &cp, nil
}

// AuthorizeRequest asks the provider to hold funds.
type AuthorizeRequest struct {
	OrderID            string
	Amount             money.Money
	IdempotencyKey     string
	PaymentMethodToken string
}

// ProviderDecision is the provider's answer to an authorization request.
type ProviderDecision struct {
	Approved          bool
	ProviderReference string
	DeclineReason     DeclineReason
}

// Provider is the external payment provider.
//
// It is a port so that the simulated implementation used in development and
// tests can be replaced by a real one without touching the service's logic. The
// simulation is not a test double: it runs in the deployed service, and its
// failure rate is configuration.
type Provider interface {
	// Authorize asks the provider to hold funds. Implementations must be safe to
	// call more than once for the same key.
	Authorize(ctx context.Context, req AuthorizeRequest) (ProviderDecision, error)
}

// Repository persists payments.
type Repository interface {
	// Create stores a payment and claims its idempotency key.
	//
	// It must fail with ErrDuplicateIdempotencyKey when the key is taken, and
	// that failure must come from a uniqueness constraint rather than a prior
	// read: a check-then-write races under exactly the concurrent retries this
	// method exists to make safe.
	Create(ctx context.Context, p *Payment) error

	// GetByID loads a payment by its own id.
	GetByID(ctx context.Context, id string) (*Payment, error)

	// GetByIdempotencyKey loads the payment a previous call with this key
	// produced.
	GetByIdempotencyKey(ctx context.Context, key string) (*Payment, error)

	// GetByOrderID loads the payment for an order. The saga recovery worker uses
	// it when it does not know whether an earlier authorize attempt landed.
	GetByOrderID(ctx context.Context, orderID string) (*Payment, error)

	// Update saves a changed payment.
	Update(ctx context.Context, p *Payment) error

	// Delete removes a payment and releases its idempotency key.
	//
	// It exists for one narrow case: a payment whose provider call never
	// produced a decision. Nothing happened, so the pending record must go, or
	// the key stays claimed and every retry with it fails as "in progress" —
	// turning a transient provider outage into a permanently stuck checkout.
	Delete(ctx context.Context, id string) error
}
