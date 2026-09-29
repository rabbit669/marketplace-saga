package httpapi

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/marketplace/pkg/proto/common/v1"
	orderv1 "github.com/marketplace/pkg/proto/order/v1"
)

// This file converts between the generated proto types the gateway receives and
// the JSON shapes it returns.
//
// The two are deliberately different. The proto is an internal contract between
// services and may change freely; the JSON is a public contract and may not.
// Deriving the public response from the proto automatically — with protobuf's
// own JSON mapping, say — would couple them, so a field renamed internally would
// silently rename itself in the API.

// addressToProto converts the public address shape to the wire type.
func addressToProto(a addressDTO) *commonv1.Address {
	return &commonv1.Address{
		Country:    a.Country,
		City:       a.City,
		Street:     a.Street,
		PostalCode: a.PostalCode,
		Recipient:  a.Recipient,
	}
}

// addressFromProto converts the wire type back to the public shape.
func addressFromProto(a *commonv1.Address) addressDTO {
	if a == nil {
		return addressDTO{}
	}
	return addressDTO{
		Country:    a.GetCountry(),
		City:       a.GetCity(),
		Street:     a.GetStreet(),
		PostalCode: a.GetPostalCode(),
		Recipient:  a.GetRecipient(),
	}
}

// moneyFromProto converts an amount to the public shape.
//
// Amounts are published as integer minor units with a currency, never as a
// decimal number. A JSON number is a double to most clients, and a double cannot
// represent 0.1 exactly — which is how a total computed correctly on the server
// arrives at the client a cent off.
func moneyFromProto(m *commonv1.Money) moneyDTO {
	if m == nil {
		return moneyDTO{}
	}
	return moneyDTO{
		AmountMinor: m.GetAmountMinor(),
		Currency:    m.GetCurrency(),
	}
}

// toOrderResponse builds the public order representation.
func toOrderResponse(o *orderv1.Order) orderResponse {
	if o == nil {
		return orderResponse{}
	}

	lines := make([]orderLineResponse, 0, len(o.GetLines()))
	for _, l := range o.GetLines() {
		lines = append(lines, orderLineResponse{
			ProductID: l.GetProductId(),
			SKU:       l.GetSku(),
			Name:      l.GetName(),
			Quantity:  l.GetQuantity(),
			UnitPrice: moneyFromProto(l.GetUnitPrice()),
			LineTotal: moneyFromProto(l.GetLineTotal()),
		})
	}

	return orderResponse{
		ID:              o.GetId(),
		Status:          orderStatusString(o.GetStatus()),
		Lines:           lines,
		Total:           moneyFromProto(o.GetTotal()),
		ShippingAddress: addressFromProto(o.GetShippingAddress()),
		PaymentID:       o.GetPaymentId(),
		ReservationID:   o.GetReservationId(),
	}
}

// checkoutFailureDTO explains why a checkout did not complete.
//
// It travels in the body of a failed create, alongside the order itself, because
// the caller needs both: the id to look the attempt up later, and the reason to
// say something useful to the customer.
type checkoutFailureDTO struct {
	// Code is the transport-neutral classification, e.g. "failed_precondition".
	Code string `json:"code"`
	// Message is safe to show a user.
	Message string `json:"message"`
	// FailedStep names the saga step that triggered the rollback.
	FailedStep string `json:"failed_step,omitempty"`
	// Retryable says whether the same request might succeed later.
	Retryable bool `json:"retryable,omitempty"`
}

// orderStatusString renders the order status for the API.
//
// Explicit mapping rather than the enum's own name: the proto name is part of an
// internal contract that can be renamed, while these strings are what clients
// switch on.
func orderStatusString(s orderv1.OrderStatus) string {
	switch s {
	case orderv1.OrderStatus_ORDER_STATUS_PENDING:
		return "pending"
	case orderv1.OrderStatus_ORDER_STATUS_CONFIRMED:
		return "confirmed"
	case orderv1.OrderStatus_ORDER_STATUS_CANCELLED:
		return "cancelled"
	case orderv1.OrderStatus_ORDER_STATUS_FAILED:
		return "failed"
	default:
		return "unspecified"
	}
}

func sagaStatusString(s orderv1.SagaStatus) string {
	switch s {
	case orderv1.SagaStatus_SAGA_STATUS_RUNNING:
		return "running"
	case orderv1.SagaStatus_SAGA_STATUS_COMPLETED:
		return "completed"
	case orderv1.SagaStatus_SAGA_STATUS_COMPENSATING:
		return "compensating"
	case orderv1.SagaStatus_SAGA_STATUS_COMPENSATED:
		return "compensated"
	case orderv1.SagaStatus_SAGA_STATUS_FAILED:
		return "failed"
	default:
		return ""
	}
}

func stepStatusString(s orderv1.StepStatus) string {
	switch s {
	case orderv1.StepStatus_STEP_STATUS_PENDING:
		return "pending"
	case orderv1.StepStatus_STEP_STATUS_COMPLETED:
		return "completed"
	case orderv1.StepStatus_STEP_STATUS_FAILED:
		return "failed"
	case orderv1.StepStatus_STEP_STATUS_COMPENSATED:
		return "compensated"
	case orderv1.StepStatus_STEP_STATUS_COMPENSATION_FAILED:
		return "compensation_failed"
	default:
		return "unspecified"
	}
}

// timeString renders a timestamp in RFC 3339, which is what every client
// library parses without configuration. The zero time renders as empty rather
// than "0001-01-01T00:00:00Z", which is a value no consumer wants to special
// case.
func timeString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// timeFromProto converts a wire timestamp to a Go time, returning the zero time
// for nil. The zero time is the honest answer for an unset field: substituting
// time.Now would invent a fact, and a caller can test for zero.
func timeFromProto(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}
