package grpcserver

import (
	"context"
	"errors"
	"fmt"

	"github.com/marketplace/pkg/apperr"
	"github.com/marketplace/pkg/money"
	commonv1 "github.com/marketplace/pkg/proto/common/v1"
	paymentv1 "github.com/marketplace/pkg/proto/payment/v1"
	"github.com/marketplace/pkg/protox"

	"github.com/marketplace/services/payment/internal/app"
	"github.com/marketplace/services/payment/internal/domain"
)

// Server implements the generated paymentv1.PaymentServiceServer.
//
// Its only job is translation: it turns wire types into domain requests, calls
// the use cases, and turns the results back. It contains no rules, which is what
// keeps the interesting behaviour testable without a network.
type Server struct {
	paymentv1.UnimplementedPaymentServiceServer

	service *app.Service
}

// New builds the transport adapter.
func New(service *app.Service) (*Server, error) {
	if service == nil {
		return nil, errors.New("payment grpc server: service is required")
	}
	return &Server{service: service}, nil
}

// AuthorizePayment implements paymentv1.PaymentServiceServer.
//
// A decline is returned as a normal response carrying a payment in the declined
// state, not as an error. That mirrors how real providers behave and matches the
// proto, where PaymentStatus and DeclinedReason live on the payment itself. The
// consequence is that callers must inspect the status — which the checkout saga
// does in exactly one place.
func (s *Server) AuthorizePayment(ctx context.Context, req *paymentv1.AuthorizePaymentRequest) (*paymentv1.AuthorizePaymentResponse, error) {
	amount, err := wireMoney(req.GetAmount())
	if err != nil {
		return nil, err
	}

	result, err := s.service.Authorize(ctx, app.AuthorizeRequest{
		OrderID:            req.GetOrderId(),
		Amount:             amount,
		IdempotencyKey:     req.GetIdempotencyKey(),
		PaymentMethodToken: req.GetPaymentMethodToken(),
	})
	if err != nil {
		return nil, err
	}

	payment, err := toProtoPayment(result.Payment)
	if err != nil {
		return nil, err
	}

	return &paymentv1.AuthorizePaymentResponse{
		Payment:          payment,
		IdempotentReplay: result.IdempotentReplay,
		// The retryability of the decline is computed here, where the provider's
		// reasons are understood, and carried on the wire so that consumers do
		// not each re-derive it.
		Retryable: result.Payment.Status == domain.StatusDeclined &&
			result.Payment.DeclineReason.Retryable(),
	}, nil
}

// CapturePayment implements paymentv1.PaymentServiceServer.
func (s *Server) CapturePayment(ctx context.Context, req *paymentv1.CapturePaymentRequest) (*paymentv1.CapturePaymentResponse, error) {
	payment, err := s.service.Capture(ctx, req.GetPaymentId())
	if err != nil {
		return nil, err
	}

	proto, err := toProtoPayment(payment)
	if err != nil {
		return nil, err
	}
	return &paymentv1.CapturePaymentResponse{Payment: proto}, nil
}

// VoidPayment implements paymentv1.PaymentServiceServer.
//
// A payment that does not exist produces a response with no payment rather than
// an error. This is a saga compensation, and a compensation runs exactly when
// the caller does not know whether the forward step applied, so "there was
// nothing to undo" must be indistinguishable from success.
func (s *Server) VoidPayment(ctx context.Context, req *paymentv1.VoidPaymentRequest) (*paymentv1.VoidPaymentResponse, error) {
	payment, err := s.service.Void(ctx, req.GetPaymentId(), req.GetReason())
	if err != nil {
		return nil, err
	}

	proto, err := toProtoPayment(payment)
	if err != nil {
		return nil, err
	}
	return &paymentv1.VoidPaymentResponse{Payment: proto}, nil
}

// RefundPayment implements paymentv1.PaymentServiceServer.
func (s *Server) RefundPayment(ctx context.Context, req *paymentv1.RefundPaymentRequest) (*paymentv1.RefundPaymentResponse, error) {
	// A zero amount means "refund everything refundable"; the use case decides
	// how much that is, because only it sees the payment's history.
	var amount money.Money
	if req.GetAmount() != nil {
		parsed, err := wireMoney(req.GetAmount())
		if err != nil {
			return nil, err
		}
		amount = parsed
	}

	payment, err := s.service.Refund(ctx, req.GetPaymentId(), amount, req.GetReason())
	if err != nil {
		return nil, err
	}

	proto, err := toProtoPayment(payment)
	if err != nil {
		return nil, err
	}
	return &paymentv1.RefundPaymentResponse{Payment: proto}, nil
}

// GetPayment implements paymentv1.PaymentServiceServer.
func (s *Server) GetPayment(ctx context.Context, req *paymentv1.GetPaymentRequest) (*paymentv1.GetPaymentResponse, error) {
	payment, err := s.service.Get(ctx, req.GetPaymentId(), req.GetOrderId())
	if err != nil {
		return nil, err
	}

	proto, err := toProtoPayment(payment)
	if err != nil {
		return nil, err
	}
	return &paymentv1.GetPaymentResponse{Payment: proto}, nil
}

// wireMoney converts a wire amount, reporting a malformed one as a caller error
// rather than an internal failure. A client that sends an amount without a
// currency has made a mistake; it is not our server breaking.
func wireMoney(m *commonv1.Money) (money.Money, error) {
	amount, err := prox.MoneyFromProto(m)
	if err != nil {
		return money.Money{}, apperr.InvalidArgument("payment", "%s", err.Error())
	}
	return amount, nil
}

func toProtoPayment(p *domain.Payment) (*paymentv1.Payment, error) {
	if p == nil {
		// Void on a payment that never existed is a successful no-op, so there
		// is genuinely no payment to report.
		return nil, nil
	}

	amount, err := prox.MoneyToProto(p.Amount)
	if err != nil {
		return nil, fmt.Errorf("payment %s: %w", p.ID, err)
	}

	return &paymentv1.Payment{
		Id:                p.ID,
		OrderId:           p.OrderID,
		Amount:            amount,
		Status:            toProtoStatus(p.Status),
		DeclinedReason:    toProtoDeclineReason(p.DeclineReason),
		ProviderReference: p.ProviderReference,
		CreatedAt:         prox.TimeToProto(p.CreatedAt),
		UpdatedAt:         prox.TimeToProto(p.UpdatedAt),
	}, nil
}

func toProtoStatus(s domain.Status) paymentv1.PaymentStatus {
	switch s {
	case domain.StatusPending:
		// Not representable on the wire: a payment only ever reaches a caller
		// after the provider has answered. Unspecified is the honest answer
		// rather than pretending it is authorized.
		return paymentv1.PaymentStatus_PAYMENT_STATUS_UNSPECIFIED
	case domain.StatusAuthorized:
		return paymentv1.PaymentStatus_PAYMENT_STATUS_AUTHORIZED
	case domain.StatusCaptured:
		return paymentv1.PaymentStatus_PAYMENT_STATUS_CAPTURED
	case domain.StatusDeclined:
		return paymentv1.PaymentStatus_PAYMENT_STATUS_DECLINED
	case domain.StatusVoided:
		return paymentv1.PaymentStatus_PAYMENT_STATUS_VOIDED
	case domain.StatusRefunded:
		return paymentv1.PaymentStatus_PAYMENT_STATUS_REFUNDED
	default:
		return paymentv1.PaymentStatus_PAYMENT_STATUS_UNSPECIFIED
	}
}

func toProtoDeclineReason(r domain.DeclineReason) paymentv1.DeclinedReason {
	switch r {
	case domain.ReasonInsufficientFunds:
		return paymentv1.DeclinedReason_DECLINED_REASON_INSUFFICIENT_FUNDS
	case domain.ReasonCardExpired:
		return paymentv1.DeclinedReason_DECLINED_REASON_CARD_EXPIRED
	case domain.ReasonFraudSuspected:
		return paymentv1.DeclinedReason_DECLINED_REASON_FRAUD_SUSPECTED
	case domain.ReasonProviderTimeout:
		return paymentv1.DeclinedReason_DECLINED_REASON_PROVIDER_TIMEOUT
	case domain.ReasonProviderUnavailable:
		return paymentv1.DeclinedReason_DECLINED_REASON_PROVIDER_UNAVAILABLE
	default:
		return paymentv1.DeclinedReason_DECLINED_REASON_UNSPECIFIED
	}
}

// FromProtoDeclineReason converts a wire reason to the domain's, for callers
// that need to react to a specific decline.
func FromProtoDeclineReason(r paymentv1.DeclinedReason) domain.DeclineReason {
	switch r {
	case paymentv1.DeclinedReason_DECLINED_REASON_INSUFFICIENT_FUNDS:
		return domain.ReasonInsufficientFunds
	case paymentv1.DeclinedReason_DECLINED_REASON_CARD_EXPIRED:
		return domain.ReasonCardExpired
	case paymentv1.DeclinedReason_DECLINED_REASON_FRAUD_SUSPECTED:
		return domain.ReasonFraudSuspected
	case paymentv1.DeclinedReason_DECLINED_REASON_PROVIDER_TIMEOUT:
		return domain.ReasonProviderTimeout
	case paymentv1.DeclinedReason_DECLINED_REASON_PROVIDER_UNAVAILABLE:
		return domain.ReasonProviderUnavailable
	default:
		return ""
	}
}
