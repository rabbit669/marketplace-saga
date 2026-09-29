// Package grpcserver exposes the order service over gRPC.
//
// It translates between the wire contract and the use cases, and does nothing
// else. Every decision — what a valid cart is, when to compensate, what a
// decline means — belongs to the domain and application layers, which is why
// those can be tested without a network and why this file is mostly field
// copying.
package grpcserver

import (
	"context"
	"errors"

	"github.com/marketplace/pkg/apperr"
	"github.com/marketplace/pkg/resilience"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/marketplace/pkg/proto/common/v1"
	orderv1 "github.com/marketplace/pkg/proto/order/v1"
	"github.com/marketplace/pkg/protox"
	"github.com/marketplace/pkg/saga"

	"github.com/marketplace/services/order/internal/app"
	"github.com/marketplace/services/order/internal/domain"
)

// Server implements orderv1.OrderServiceServer.
type Server struct {
	orderv1.UnimplementedOrderServiceServer

	service *app.Service
}

// New builds the transport adapter.
func New(service *app.Service) (*Server, error) {
	if service == nil {
		return nil, errors.New("order grpc server: service is required")
	}
	return &Server{service: service}, nil
}

// CreateOrder implements orderv1.OrderServiceServer.
//
// The response carries the order even when the checkout failed. "Your payment
// was declined" is a business outcome, not a malformed request, and the caller
// needs the order it refers to — its id, its final status and the reason — in
// order to say anything useful to the customer.
func (s *Server) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
	lines := make([]domain.LineRequest, 0, len(req.GetLines()))
	for _, l := range req.GetLines() {
		lines = append(lines, domain.LineRequest{
			ProductID: l.GetProductId(),
			Quantity:  l.GetQuantity(),
		})
	}

	shipping := domain.Address{
		Country:    req.GetShippingAddress().GetCountry(),
		City:       req.GetShippingAddress().GetCity(),
		Street:     req.GetShippingAddress().GetStreet(),
		PostalCode: req.GetShippingAddress().GetPostalCode(),
		Recipient:  req.GetShippingAddress().GetRecipient(),
	}

	result, err := s.service.CreateOrder(ctx, app.CreateOrderRequest{
		CustomerID:         req.GetCustomerId(),
		Lines:              lines,
		Shipping:           shipping,
		IdempotencyKey:     req.GetIdempotencyKey(),
		PaymentMethodToken: req.GetPaymentMethodToken(),
	})

	// The order is translated even when err is non-nil, because a rolled-back
	// checkout still produced one. Returning a bare error would leave the caller
	// with nothing to show and no way to look the attempt up afterwards.
	resp := &orderv1.CreateOrderResponse{
		IdempotentReplay: result.IdempotentReplay,
		SagaStatus:       toProtoSagaStatus(result.SagaStatus),
		SagaCompleted:    result.SagaStatus.Terminal(),
	}
	if result.Order != nil {
		order, terr := toProtoOrder(result.Order)
		if terr != nil {
			return nil, terr
		}
		resp.Order = order
	}

	if err != nil {
		// A gRPC call has no partial success: returning a non-OK status
		// alongside a response makes the server discard the response, so the
		// caller would never see the order. The failure is therefore carried in
		// the response and the HTTP layer decides what status it deserves.
		resp.Failure = toProtoFailure(err, result.SagaStatus)
	}
	return resp, nil
}

// toProtoFailure renders a checkout failure for the wire.
//
// The classification travels with the message because the gateway maps the code
// to an HTTP status, and re-deriving it from the text would be guesswork.
func toProtoFailure(err error, status saga.Status) *orderv1.CheckoutFailure {
	if err == nil {
		return nil
	}

	failure := &orderv1.CheckoutFailure{
		Code:    string(apperr.CodeOf(err)),
		Message: apperr.PublicMessage(err),
	}

	// A saga that could not be rolled back is a different problem from one that
	// rolled back cleanly: the first needs a human, the second is a normal
	// business outcome. They must not be reported alike.
	if status == saga.StatusFailed {
		failure.Code = string(apperr.CodeInternal)
	}

	// Whether trying again could help is a property of the classification, and
	// the client cannot derive it from the message. A transient provider outage
	// and a declined card must not look alike to a client that decides whether
	// to offer the customer a retry.
	failure.Retryable = resilience.RetryableByCode(err)

	var appErr *apperr.Error
	if errors.As(err, &appErr) && len(appErr.Meta) > 0 {
		failure.FailedStep = appErr.Meta["failed_step"]
	}

	return failure
}

// GetOrder implements orderv1.OrderServiceServer.
func (s *Server) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	order, err := s.service.GetOrder(ctx, req.GetOrderId())
	if err != nil {
		return nil, err
	}

	proto, err := toProtoOrder(order)
	if err != nil {
		return nil, err
	}
	return &orderv1.GetOrderResponse{Order: proto}, nil
}

// ListOrders implements orderv1.OrderServiceServer.
//
// It is not implemented yet, and says so rather than returning an empty list.
// An empty list is indistinguishable from "this customer has no orders", which
// is a fact a caller would act on.
func (s *Server) ListOrders(context.Context, *orderv1.ListOrdersRequest) (*orderv1.ListOrdersResponse, error) {
	// Unimplemented rather than an empty list: an empty list is
	// indistinguishable from "this customer has no orders", which is a fact a
	// caller would act on. This is the one place a handler builds a status
	// directly, because "unimplemented" is a transport-level fact about the
	// server rather than a business outcome.
	return nil, status.Error(codes.Unimplemented, "ListOrders is not implemented yet")
}

// CancelOrder implements orderv1.OrderServiceServer.
func (s *Server) CancelOrder(ctx context.Context, req *orderv1.CancelOrderRequest) (*orderv1.CancelOrderResponse, error) {
	order, status, err := s.service.CancelOrder(ctx, req.GetOrderId(), req.GetReason())
	if err != nil {
		return nil, err
	}

	proto, err := toProtoOrder(order)
	if err != nil {
		return nil, err
	}
	return &orderv1.CancelOrderResponse{
		Order:      proto,
		SagaStatus: toProtoSagaStatus(status),
	}, nil
}

// GetSagaState implements orderv1.OrderServiceServer.
//
// This is the operator's window into a checkout: every step, how many attempts
// it took and what went wrong. It is what answers "why was this order
// cancelled?" without reading five services' logs.
func (s *Server) GetSagaState(ctx context.Context, req *orderv1.GetSagaStateRequest) (*orderv1.GetSagaStateResponse, error) {
	inst, err := s.service.GetSagaState(ctx, req.GetOrderId())
	if err != nil {
		return nil, err
	}
	return toProtoSagaState(inst), nil
}

// RetrySaga implements orderv1.OrderServiceServer.
//
// The operator escape hatch for a saga whose compensation failed: once the
// blocker is gone, this drives it to a terminal state.
func (s *Server) RetrySaga(ctx context.Context, req *orderv1.RetrySagaRequest) (*orderv1.RetrySagaResponse, error) {
	inst, err := s.service.RetrySaga(ctx, req.GetOrderId())
	if err != nil {
		return nil, err
	}
	return &orderv1.RetrySagaResponse{Status: toProtoSagaStatus(inst.Status)}, nil
}

// ------------------------------------------------------------- conversion

func toProtoOrder(o *domain.Order) (*orderv1.Order, error) {
	if o == nil {
		return nil, nil
	}

	lines := make([]*orderv1.OrderLine, 0, len(o.Lines))
	for _, l := range o.Lines {
		unitPrice, err := prox.MoneyToProto(l.UnitPrice)
		if err != nil {
			return nil, err
		}
		lineTotal, err := prox.MoneyToProto(l.UnitPrice.Mul(l.Quantity))
		if err != nil {
			return nil, err
		}
		lines = append(lines, &orderv1.OrderLine{
			ProductId: l.ProductID,
			Sku:       l.SKU,
			Name:      l.Name,
			Quantity:  l.Quantity,
			UnitPrice: unitPrice,
			LineTotal: lineTotal,
		})
	}

	total, err := prox.MoneyToProto(o.Total)
	if err != nil {
		return nil, err
	}

	return &orderv1.Order{
		Id:         o.ID,
		CustomerId: o.CustomerID,
		Status:     toProtoOrderStatus(o.Status),
		Lines:      lines,
		Total:      total,
		ShippingAddress: &commonv1.Address{
			Country:    o.Shipping.Country,
			City:       o.Shipping.City,
			Street:     o.Shipping.Street,
			PostalCode: o.Shipping.PostalCode,
			Recipient:  o.Shipping.Recipient,
		},
		PaymentId:     o.PaymentID,
		ReservationId: o.ReservationID,
	}, nil
}

func toProtoOrderStatus(s domain.Status) orderv1.OrderStatus {
	switch s {
	case domain.StatusPending:
		return orderv1.OrderStatus_ORDER_STATUS_PENDING
	case domain.StatusConfirmed:
		return orderv1.OrderStatus_ORDER_STATUS_CONFIRMED
	case domain.StatusCancelled:
		return orderv1.OrderStatus_ORDER_STATUS_CANCELLED
	case domain.StatusFailed:
		return orderv1.OrderStatus_ORDER_STATUS_FAILED
	default:
		return orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED
	}
}

func toProtoSagaStatus(s saga.Status) orderv1.SagaStatus {
	switch s {
	case saga.StatusRunning:
		return orderv1.SagaStatus_SAGA_STATUS_RUNNING
	case saga.StatusCompleted:
		return orderv1.SagaStatus_SAGA_STATUS_COMPLETED
	case saga.StatusCompensating:
		return orderv1.SagaStatus_SAGA_STATUS_COMPENSATING
	case saga.StatusCompensated:
		return orderv1.SagaStatus_SAGA_STATUS_COMPENSATED
	case saga.StatusFailed:
		return orderv1.SagaStatus_SAGA_STATUS_FAILED
	default:
		return orderv1.SagaStatus_SAGA_STATUS_UNSPECIFIED
	}
}

func toProtoStepStatus(s saga.StepStatus) orderv1.StepStatus {
	switch s {
	case saga.StepPending:
		return orderv1.StepStatus_STEP_STATUS_PENDING
	case saga.StepCompleted:
		return orderv1.StepStatus_STEP_STATUS_COMPLETED
	case saga.StepFailed:
		return orderv1.StepStatus_STEP_STATUS_FAILED
	case saga.StepCompensated:
		return orderv1.StepStatus_STEP_STATUS_COMPENSATED
	case saga.StepCompensationFailed:
		return orderv1.StepStatus_STEP_STATUS_COMPENSATION_FAILED
	default:
		return orderv1.StepStatus_STEP_STATUS_UNSPECIFIED
	}
}

func toProtoSagaState(inst *saga.Instance) *orderv1.GetSagaStateResponse {
	if inst == nil {
		return nil
	}

	steps := make([]*orderv1.SagaStep, 0, len(inst.Steps))
	for _, s := range inst.Steps {
		steps = append(steps, &orderv1.SagaStep{
			Name:        s.Name,
			Status:      toProtoStepStatus(s.Status),
			Attempt:     int32(s.Attempts),
			Error:       s.LastError,
			Compensated: s.Compensated,
			StartedAt:   prox.TimeToProto(s.StartedAt),
			FinishedAt:  prox.TimeToProto(s.FinishedAt),
		})
	}

	return &orderv1.GetSagaStateResponse{
		SagaId:    inst.ID,
		OrderId:   inst.ID,
		Status:    toProtoSagaStatus(inst.Status),
		Steps:     steps,
		Attempts:  int32(inst.Attempts),
		LastError: inst.LastError,
		StartedAt: prox.TimeToProto(inst.StartedAt),
		UpdatedAt: prox.TimeToProto(inst.UpdatedAt),
	}
}
