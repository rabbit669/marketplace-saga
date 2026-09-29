package clients

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/marketplace/pkg/apperr"
	paymentv1 "github.com/marketplace/pkg/proto/payment/v1"
	"github.com/marketplace/pkg/protox"

	"github.com/marketplace/services/order/internal/domain"
)

// PaymentClient implements domain.PaymentClient over gRPC.
type PaymentClient struct {
	client paymentv1.PaymentServiceClient
}

// NewPaymentClient wraps a connection to the payment service.
func NewPaymentClient(conn grpc.ClientConnInterface) *PaymentClient {
	return &PaymentClient{client: paymentv1.NewPaymentServiceClient(conn)}
}

// Authorize asks the provider to hold funds.
//
// A decline arrives as a *successful* response carrying a declined payment, not
// as an error. That is how real providers behave and how the contract models it,
// because the payment object is where the reason lives. The consequence is that
// this adapter must inspect the status — and it is the only place in the order
// service that has to, which is why the proto carries the retryability flag
// rather than making every consumer re-derive it from the reason.
func (c *PaymentClient) Authorize(ctx context.Context, req domain.AuthorizeRequest) (domain.AuthorizeResult, error) {
	amount, err := prox.MoneyToProto(req.Amount)
	if err != nil {
		return domain.AuthorizeResult{}, fmt.Errorf("payment.Authorize: %w", err)
	}

	resp, err := c.client.AuthorizePayment(ctx, &paymentv1.AuthorizePaymentRequest{
		OrderId:            req.OrderID,
		Amount:             amount,
		IdempotencyKey:     req.IdempotencyKey,
		PaymentMethodToken: req.PaymentMethodToken,
	})
	if err != nil {
		return domain.AuthorizeResult{}, apperr.FromGRPC("payment.Authorize", err)
	}

	payment := resp.GetPayment()
	if payment == nil {
		return domain.AuthorizeResult{}, apperr.Internal("payment.Authorize",
			"payment service returned success without a payment")
	}

	result := domain.AuthorizeResult{
		PaymentID:   payment.GetId(),
		ProviderRef: payment.GetProviderReference(),
		Retryable:   resp.GetRetryable(),
	}

	switch payment.GetStatus() {
	case paymentv1.PaymentStatus_PAYMENT_STATUS_AUTHORIZED:
		result.Status = domain.PaymentAuthorized
	case paymentv1.PaymentStatus_PAYMENT_STATUS_DECLINED:
		result.Status = domain.PaymentDeclined
		result.DeclinedReason = declineReasonString(payment.GetDeclinedReason())
	default:
		// Any other status here means the provider answered something the saga
		// has no branch for. Reporting it as a failure is safer than guessing,
		// because the alternatives are either charging on an unknown state or
		// silently treating it as approval.
		return domain.AuthorizeResult{}, apperr.Internal("payment.Authorize",
			"payment %s returned unexpected status %s", payment.GetId(), payment.GetStatus())
	}

	return result, nil
}

// Capture takes previously authorized funds.
//
// This is the saga's pivot: there is no compensation for it, because undoing a
// capture is a refund — a new business transaction, not a rollback.
func (c *PaymentClient) Capture(ctx context.Context, paymentID, idempotencyKey string) error {
	_, err := c.client.CapturePayment(ctx, &paymentv1.CapturePaymentRequest{
		PaymentId:      paymentID,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return apperr.FromGRPC("payment.Capture", err)
	}
	return nil
}

// Void cancels an authorization that was never captured.
//
// Called as a saga compensation, so a payment that does not exist is a
// successful no-op: the compensation runs when the forward step's outcome is
// unknown, and "there was nothing to void" is one of the possible outcomes.
func (c *PaymentClient) Void(ctx context.Context, paymentID, reason string) error {
	_, err := c.client.VoidPayment(ctx, &paymentv1.VoidPaymentRequest{
		PaymentId: paymentID,
		Reason:    reason,
	})
	if err != nil {
		return apperr.FromGRPC("payment.Void", err)
	}
	return nil
}

// declineReasonString renders the wire enum for logs and for the customer-facing
// message. The order service does not branch on it — retryability comes from the
// service that owns the provider relationship.
func declineReasonString(r paymentv1.DeclinedReason) string {
	switch r {
	case paymentv1.DeclinedReason_DECLINED_REASON_INSUFFICIENT_FUNDS:
		return "insufficient_funds"
	case paymentv1.DeclinedReason_DECLINED_REASON_CARD_EXPIRED:
		return "card_expired"
	case paymentv1.DeclinedReason_DECLINED_REASON_FRAUD_SUSPECTED:
		return "fraud_suspected"
	case paymentv1.DeclinedReason_DECLINED_REASON_PROVIDER_TIMEOUT:
		return "provider_timeout"
	case paymentv1.DeclinedReason_DECLINED_REASON_PROVIDER_UNAVAILABLE:
		return "provider_unavailable"
	default:
		return "unknown"
	}
}
