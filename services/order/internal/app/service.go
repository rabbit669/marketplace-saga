package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/marketplace/pkg/apperr"
	"github.com/marketplace/pkg/idgen"
	"github.com/marketplace/pkg/saga"

	"github.com/marketplace/services/order/internal/domain"
)

// Metrics receives domain outcomes.
//
// An interface rather than a Prometheus type so that this package stays free of
// the metrics backend, and so that a test can assert on outcomes without
// scraping a registry.
type Metrics interface {
	EventOutcome(kind, outcome string)
}

// Service implements the order service's use cases.
type Service struct {
	orders   domain.Repository
	catalog  domain.CatalogClient
	engine   *saga.Engine
	checkout *CheckoutSaga

	clock   func() time.Time
	log     *slog.Logger
	metrics Metrics
}

// ServiceConfig carries the service's dependencies.
type ServiceConfig struct {
	Orders   domain.Repository
	Catalog  domain.CatalogClient
	Engine   *saga.Engine
	Checkout *CheckoutSaga

	Logger  *slog.Logger
	Clock   func() time.Time
	Metrics Metrics
}

// NewService wires the use cases.
func NewService(cfg ServiceConfig) (*Service, error) {
	switch {
	case cfg.Orders == nil:
		return nil, errors.New("order service: orders repository is required")
	case cfg.Catalog == nil:
		return nil, errors.New("order service: catalog client is required")
	case cfg.Engine == nil:
		return nil, errors.New("order service: saga engine is required")
	case cfg.Checkout == nil:
		return nil, errors.New("order service: checkout saga is required")
	}

	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}

	return &Service{
		orders:   cfg.Orders,
		catalog:  cfg.Catalog,
		engine:   cfg.Engine,
		checkout: cfg.Checkout,
		clock:    cfg.Clock,
		log:      cfg.Logger,
		metrics:  cfg.Metrics,
	}, nil
}

// CreateOrderRequest is a customer's checkout attempt.
type CreateOrderRequest struct {
	CustomerID string
	Lines      []domain.LineRequest
	Shipping   domain.Address

	// IdempotencyKey is required. It is what makes a retried checkout return the
	// original order rather than placing a second one — the single most common
	// way a naive checkout double-charges a customer. See ADR-0003.
	IdempotencyKey string

	// PaymentMethodToken is the provider's token for the card. The order service
	// never sees a card number, and this token is a provider reference rather
	// than a PAN, so storing it is not a PCI concern.
	PaymentMethodToken string
}

// Validate checks the request's shape before anything is persisted.
func (r CreateOrderRequest) Validate() error {
	if strings.TrimSpace(r.CustomerID) == "" {
		return domain.Invalid("customer_id", "is required")
	}
	if strings.TrimSpace(r.IdempotencyKey) == "" {
		return domain.Invalid("idempotency_key", "is required: without it a retried checkout creates a second order")
	}
	if strings.TrimSpace(r.PaymentMethodToken) == "" {
		return domain.Invalid("payment_method_token", "is required")
	}
	if len(r.Lines) == 0 {
		return domain.Invalid("lines", "an order must have at least one line")
	}
	if len(r.Lines) > domain.MaxLines {
		return domain.Invalid("lines", "an order may have at most %d lines, got %d", domain.MaxLines, len(r.Lines))
	}
	for i, l := range r.Lines {
		if err := l.Validate(); err != nil {
			return domain.Invalid(fmt.Sprintf("lines[%d]", i), "%s", err.Error())
		}
	}
	if err := r.Shipping.Validate(); err != nil {
		return domain.Invalid("shipping_address", "%s", err.Error())
	}
	return nil
}

// CreateOrderResult reports the outcome of a checkout.
type CreateOrderResult struct {
	Order *domain.Order
	// SagaStatus is the terminal status the saga reached. A compensated saga is
	// a successful outcome for the system: nothing is left applied.
	SagaStatus saga.Status
	// IdempotentReplay is true when this call returned an order created by an
	// earlier call with the same idempotency key.
	IdempotentReplay bool
}

// CreateOrder validates a cart, prices it, persists the order and runs the
// checkout saga.
//
// The saga runs synchronously, so the caller learns the outcome — confirmed or
// rolled back — from this call rather than polling. That suits checkout, where
// the customer is waiting either way. A longer flow would start the saga and
// return immediately; `GetSagaState` exists for exactly that.
func (s *Service) CreateOrder(ctx context.Context, req CreateOrderRequest) (CreateOrderResult, error) {
	if err := req.Validate(); err != nil {
		return CreateOrderResult{}, domain.AsAppError(err)
	}

	result, err := s.checkoutOrder(ctx, req)

	// A validation failure happens before anything is claimed, so there is
	// nothing to release. A permanent failure after the order exists must keep
	// its claim: the order is real, and releasing the key would let a retry
	// create a second one.
	if err != nil && isPermanent(err) && result.Order == nil {
		if relErr := s.orders.ReleaseIdempotencyKey(ctx, req.IdempotencyKey); relErr != nil {
			s.log.ErrorContext(ctx, "could not release idempotency key after a permanent failure",
				slog.String("error", relErr.Error()),
			)
		}
	}

	return result, err
}

// checkoutOrder prices the cart, stores the order and runs the saga.
func (s *Service) checkoutOrder(ctx context.Context, req CreateOrderRequest) (CreateOrderResult, error) {
	// Pricing happens first because it is a read: it can be repeated safely, and
	// it must not sit inside the atomic claim, where a slow catalogue call would
	// hold the key against every concurrent retry.
	lines, err := s.priceCart(ctx, req.Lines)
	if err != nil {
		return CreateOrderResult{}, err
	}

	orderID := idgen.New()

	order, err := domain.NewOrder(domain.NewOrderInput{
		ID:         orderID,
		CustomerID: req.CustomerID,
		Lines:      lines,
		Shipping:   req.Shipping,
		CreatedAt:  s.clock(),
	})
	if err != nil {
		return CreateOrderResult{}, domain.AsAppError(err)
	}

	// One atomic step: the order and its idempotency key commit together, or the
	// guarantee that a retry returns the original order is worthless. Splitting
	// them leaves a window in which a concurrent retry sees the key claimed but
	// finds no order.
	existingID, created, err := s.orders.CreateIdempotent(ctx, order, req.IdempotencyKey)
	if err != nil {
		return CreateOrderResult{}, fmt.Errorf("create order: %w", err)
	}
	if !created {
		existing, gerr := s.orders.Get(ctx, existingID)
		if gerr != nil {
			return CreateOrderResult{}, fmt.Errorf("load order %s for idempotency key: %w", existingID, gerr)
		}
		s.log.InfoContext(ctx, "checkout replayed from idempotency key",
			slog.String("order_id", existing.ID),
			slog.String("status", string(existing.Status)),
		)
		return CreateOrderResult{Order: existing, IdempotentReplay: true}, nil
	}

	if err := s.checkout.publishOrderCreated(ctx, order); err != nil {
		// The order exists and the saga has not started. Leaving it pending
		// would strand it, so the failure is reported and the order is
		// cancelled rather than silently abandoned.
		s.cancelAfterSetupFailure(ctx, order, err)
		return CreateOrderResult{}, fmt.Errorf("publish order.created: %w", err)
	}

	def := s.checkout.Definition()

	inst, sagaErr := s.engine.StartWith(ctx, order.ID, def, map[string]string{
		keyPaymentToken: req.PaymentMethodToken,
	})

	reconciled, err := s.applySagaOutcome(ctx, order, inst)
	if err != nil {
		return CreateOrderResult{}, err
	}

	status := saga.StatusRunning
	if inst != nil {
		status = inst.Status
	}

	result := CreateOrderResult{Order: reconciled, SagaStatus: status}

	// The caller-facing error is derived from the saga's *outcome*, not from
	// whether the engine returned an error. A saga that rolled back cleanly is
	// not an engine failure — the engine did exactly what it was asked — but it
	// is still a failed checkout, and the customer has to be told why.
	//
	// The order travels alongside the error in every case. "Your payment was
	// declined" is a business outcome, not a malformed request: the caller needs
	// the order it refers to — its id, its final status and the reason — not
	// just a code.
	switch {
	case inst == nil:
		return result, sagaErr
	case inst.Status == saga.StatusCompleted || inst.Status == saga.StatusRunning || inst.Status == saga.StatusCompensating:
		return result, sagaErr
	case inst.Status == saga.StatusCompensated:
		return result, checkoutFailure(inst, sagaErr).WithMeta("failed_step", inst.FailedStep)
	default: // saga.StatusFailed
		return result, apperr.Wrap("order.CreateOrder", apperr.CodeInternal,
			"checkout could not be completed or rolled back", sagaErr)
	}
}

// applySagaOutcome brings the order's status in line with what the saga did.
//
// The saga's steps update the order on the happy path — confirm_order marks it
// confirmed — but a failed saga leaves the order pending, and a pending order
// for a declined card is a lie that a support agent will eventually act on. This
// is what makes the stored status match reality in every case.
//
// It returns only infrastructure failures. A rolled-back checkout is a
// *successful* outcome for the system — nothing is left applied — so it is not
// an error here; the caller decides how to phrase it, and RetrySaga treats it as
// the expected result.
func (s *Service) applySagaOutcome(ctx context.Context, order *domain.Order, inst *saga.Instance) (*domain.Order, error) {
	if inst == nil {
		return order, nil
	}

	s.countOutcome(inst.Status)

	switch inst.Status {
	case saga.StatusCompleted:
		// Already confirmed by the confirm_order step; re-read so the caller
		// sees what that step wrote rather than the pre-saga snapshot.
		return s.reload(ctx, order.ID)

	case saga.StatusCompensated:
		if err := order.MarkCancelled(CancellationReason(inst), s.clock()); err != nil {
			return order, err
		}
		if err := s.orders.Update(ctx, order); err != nil {
			return order, fmt.Errorf("mark order cancelled: %w", err)
		}
		if perr := s.checkout.publishOrderCancelled(ctx, order, inst.FailedStep, order.FailureReason); perr != nil {
			s.log.ErrorContext(ctx, "could not publish order.cancelled",
				slog.String("order_id", order.ID),
				slog.String("error", perr.Error()),
			)
		}

		s.log.InfoContext(ctx, "checkout rolled back",
			slog.String("order_id", order.ID),
			slog.String("failed_step", inst.FailedStep),
		)
		return order, nil

	case saga.StatusFailed:
		if err := order.MarkFailed(CancellationReason(inst), s.clock()); err != nil {
			return order, err
		}
		if err := s.orders.Update(ctx, order); err != nil {
			return order, fmt.Errorf("mark order failed: %w", err)
		}

		s.log.ErrorContext(ctx, "checkout needs manual intervention",
			slog.String("order_id", order.ID),
			slog.String("error", inst.LastError),
		)
		return order, nil

	default:
		return order, nil
	}
}

// countOutcome records a terminal saga result.
//
// The three outcomes are counted separately because they need different
// responses: a completed saga is success, a compensated one is a business
// failure the product team watches, and a failed one means money or stock is
// stuck and somebody should be paged. A single "failed" counter would hide the
// distinction that matters.
func (s *Service) countOutcome(status saga.Status) {
	if s.metrics == nil {
		return
	}
	s.metrics.EventOutcome("saga_checkout", string(status))
}

// reload re-reads an order, so the caller sees what the steps wrote.
func (s *Service) reload(ctx context.Context, id string) (*domain.Order, error) {
	order, err := s.orders.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("reload order %s: %w", id, err)
	}
	return order, nil
}

// cancelAfterSetupFailure marks an order cancelled when the saga never ran.
func (s *Service) cancelAfterSetupFailure(ctx context.Context, order *domain.Order, cause error) {
	if err := order.MarkCancelled("could not start checkout: "+cause.Error(), s.clock()); err != nil {
		return
	}
	if err := s.orders.Update(ctx, order); err != nil {
		s.log.ErrorContext(ctx, "could not cancel an order whose checkout failed to start",
			slog.String("order_id", order.ID),
			slog.String("error", err.Error()),
		)
	}
}

// priceCart fetches prices and builds the order's lines.
//
// Prices come from the catalogue here and are copied into the order, so a later
// price change cannot alter what the customer agreed to pay.
func (s *Service) priceCart(ctx context.Context, reqLines []domain.LineRequest) ([]domain.Line, error) {
	ids := make([]string, 0, len(reqLines))
	for _, l := range reqLines {
		ids = append(ids, l.ProductID)
	}

	products, err := s.catalog.GetProducts(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("price cart: %w", err)
	}

	lines := make([]domain.Line, 0, len(reqLines))
	for _, req := range reqLines {
		product, ok := products[req.ProductID]
		if !ok {
			return nil, domain.AsAppError(fmt.Errorf("%w: product %s does not exist",
				domain.ErrProductUnavailable, req.ProductID))
		}
		if !product.IsActive {
			return nil, domain.AsAppError(fmt.Errorf("%w: product %s is no longer on sale",
				domain.ErrProductUnavailable, req.ProductID))
		}
		if !product.Price.IsValid() || product.Price.IsNegative() {
			return nil, fmt.Errorf("price cart: product %s has an invalid price", req.ProductID)
		}

		lines = append(lines, domain.Line{
			ProductID: product.ID,
			SKU:       product.SKU,
			Name:      product.Name,
			Quantity:  req.Quantity,
			UnitPrice: product.Price,
		})
	}

	return lines, nil
}

// GetOrder loads an order.
func (s *Service) GetOrder(ctx context.Context, id string) (*domain.Order, error) {
	if strings.TrimSpace(id) == "" {
		return nil, domain.AsAppError(domain.Invalid("order_id", "is required"))
	}

	order, err := s.orders.Get(ctx, id)
	if err != nil {
		if errors.Is(err, domain.ErrOrderNotFound) {
			return nil, domain.AsAppError(err)
		}
		return nil, fmt.Errorf("get order %s: %w", id, err)
	}
	return order, nil
}

// GetSagaState returns the saga's progress, for polling and for operators.
func (s *Service) GetSagaState(ctx context.Context, orderID string) (*saga.Instance, error) {
	if strings.TrimSpace(orderID) == "" {
		return nil, domain.AsAppError(domain.Invalid("order_id", "is required"))
	}

	inst, err := s.engine.Inspect(ctx, orderID)
	if err != nil {
		return nil, fmt.Errorf("get saga state for %s: %w", orderID, err)
	}
	return inst, nil
}

// RetrySaga drives a saga that could not finish to a terminal state.
//
// This is the operator escape hatch. A saga ends up StatusFailed when a
// compensation could not be applied — the blocker is usually a dependency being
// down — and nothing else will ever retry it. Once the blocker is gone, this
// finishes the rollback.
func (s *Service) RetrySaga(ctx context.Context, orderID string) (*saga.Instance, error) {
	if strings.TrimSpace(orderID) == "" {
		return nil, domain.AsAppError(domain.Invalid("order_id", "is required"))
	}

	order, err := s.orders.Get(ctx, orderID)
	if err != nil {
		return nil, domain.AsAppError(err)
	}

	inst, sagaErr := s.engine.Resume(ctx, orderID, s.checkout.Definition())

	// A rolled-back checkout is the expected result of a retry, so the outcome
	// is applied and only infrastructure failures are reported.
	if _, aerr := s.applySagaOutcome(ctx, order, inst); aerr != nil {
		return inst, aerr
	}

	if sagaErr != nil && inst != nil && inst.Status == saga.StatusFailed {
		return inst, sagaErr
	}
	return inst, nil
}

// CancelOrder rolls back a checkout at the customer's request.
func (s *Service) CancelOrder(ctx context.Context, orderID, reason string) (*domain.Order, saga.Status, error) {
	order, err := s.GetOrder(ctx, orderID)
	if err != nil {
		return nil, "", err
	}

	switch order.Status {
	case domain.StatusConfirmed:
		// Cancelling a confirmed order is a refund and a stock return, not a
		// saga rollback: the checkout already completed and its steps are no
		// longer compensatable. That is a different flow with different rules,
		// and pretending otherwise would produce a "cancelled" order whose
		// money was never returned.
		return nil, "", apperr.FailedPrecondition("order.CancelOrder",
			"order %s is already confirmed; cancelling it requires a refund, not a rollback", orderID)
	case domain.StatusCancelled, domain.StatusFailed:
		return order, saga.StatusCompensated, nil
	}

	// The saga is still running or the process died mid-flight. Stopping it
	// safely means letting the engine drive it to a terminal state rather than
	// writing a status here, which would race with the steps.
	inst, err := s.engine.Resume(ctx, orderID, s.checkout.Definition())
	if err != nil {
		return nil, "", fmt.Errorf("cancel order %s: %w", orderID, err)
	}

	order, err = s.reload(ctx, orderID)
	return order, inst.Status, err
}

// isPermanent reports whether an error will fail the same way on a retry.
func isPermanent(err error) bool {
	if err == nil {
		return false
	}
	switch apperr.CodeOf(err) {
	case apperr.CodeInvalidArgument,
		apperr.CodeNotFound,
		apperr.CodeAlreadyExists,
		apperr.CodeFailedPrecondition,
		apperr.CodeConflict,
		apperr.CodePermissionDenied,
		apperr.CodeUnauthenticated:
		return true
	default:
		return false
	}
}

// checkoutFailure turns a rolled-back saga into a caller-facing error.
//
// The status code reflects who can act on it: a declined card is the customer's
// problem to solve, while an unreachable payment provider is ours.
func checkoutFailure(inst *saga.Instance, sagaErr error) *apperr.Error {
	reason := reasonFromSaga(inst)

	if inst != nil && inst.FailedStep == StepAuthorizePayment {
		if errors.Is(sagaErr, saga.ErrCompensationFailed) {
			return apperr.Wrap("order.CreateOrder", apperr.CodeInternal,
				"checkout failed and could not be fully rolled back", sagaErr)
		}
		return apperr.FailedPrecondition("order.CreateOrder", "payment was declined: %s", reason)
	}

	if errors.Is(sagaErr, saga.ErrCompensationFailed) {
		return apperr.Wrap("order.CreateOrder", apperr.CodeInternal,
			"checkout failed and could not be fully rolled back", sagaErr)
	}

	return apperr.FailedPrecondition("order.CreateOrder", "checkout could not be completed: %s", reason)
}

// reasonFromSaga extracts the most specific reason available.
func reasonFromSaga(inst *saga.Instance) string {
	if inst == nil {
		return "unknown"
	}
	for _, step := range inst.Steps {
		if step.LastError != "" {
			return step.LastError
		}
	}
	if inst.LastError != "" {
		return inst.LastError
	}
	return "unknown"
}
