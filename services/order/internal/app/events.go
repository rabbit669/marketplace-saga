package app

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/marketplace/pkg/idgen"
	"github.com/marketplace/pkg/money"
	commonv1 "github.com/marketplace/pkg/proto/common/v1"
	eventsv1 "github.com/marketplace/pkg/proto/events/v1"

	"github.com/marketplace/services/order/internal/domain"
)

// Event type names, as they appear on the wire. Consumers route on these, so
// they are a compatibility surface: renaming one silently stops delivery.
const (
	EventOrderCreated   = "order.created"
	EventOrderConfirmed = "order.confirmed"
	EventOrderCancelled = "order.cancelled"
)

// This file is the only place in the order service where an event's wire
// representation is constructed. checkout.go orchestrates and never serialises;
// keeping the two apart means a schema change touches one file rather than
// threading protobuf imports through the saga steps.

// publishOrderCreated announces a new order.
//
// It is published after the order is persisted and the saga has started, so a
// consumer that reacts to it can always load the order it names.
func (c *CheckoutSaga) publishOrderCreated(ctx context.Context, order *domain.Order) error {
	lines := make([]*eventsv1.OrderLinePayload, 0, len(order.Lines))
	for _, l := range order.Lines {
		price, err := toProtoMoney(l.UnitPrice)
		if err != nil {
			return fmt.Errorf("order %s line %s: %w", order.ID, l.ProductID, err)
		}
		lines = append(lines, &eventsv1.OrderLinePayload{
			ProductId: l.ProductID,
			Sku:       l.SKU,
			Quantity:  l.Quantity,
			UnitPrice: price,
		})
	}

	total, err := toProtoMoney(order.Total)
	if err != nil {
		return fmt.Errorf("order %s total: %w", order.ID, err)
	}

	return c.publish(ctx, order, EventOrderCreated, &eventsv1.Envelope_OrderCreated{
		OrderCreated: &eventsv1.OrderCreated{
			OrderId:    order.ID,
			CustomerId: order.CustomerID,
			Lines:      lines,
			Total:      total,
		},
	})
}

// publishOrderConfirmed announces a completed checkout.
func (c *CheckoutSaga) publishOrderConfirmed(ctx context.Context, order *domain.Order) error {
	total, err := toProtoMoney(order.Total)
	if err != nil {
		return fmt.Errorf("order %s total: %w", order.ID, err)
	}

	return c.publish(ctx, order, EventOrderConfirmed, &eventsv1.Envelope_OrderConfirmed{
		OrderConfirmed: &eventsv1.OrderConfirmed{
			OrderId:    order.ID,
			CustomerId: order.CustomerID,
			PaymentId:  order.PaymentID,
			Total:      total,
		},
	})
}

// publishOrderCancelled announces a rolled-back checkout.
//
// It carries the step that failed as well as a human-readable reason, so that a
// consumer can distinguish "the card was declined" from "we could not reach the
// payment provider" without parsing prose.
func (c *CheckoutSaga) publishOrderCancelled(ctx context.Context, order *domain.Order, failedStep, reason string) error {
	// The cancelled payload deliberately carries no amount: the money never
	// moved, and including a total invites a consumer to treat it as a refund.
	return c.publish(ctx, order, EventOrderCancelled, &eventsv1.Envelope_OrderCancelled{
		OrderCancelled: &eventsv1.OrderCancelled{
			OrderId:    order.ID,
			CustomerId: order.CustomerID,
			FailedStep: failedStep,
			Reason:     reason,
		},
	})
}

// publish builds the envelope and hands it to the outbox.
//
// The event id is generated here rather than by the broker: consumers
// deduplicate on it, and a broker-assigned id is not available to the code that
// writes the outbox row inside the business transaction.
func (c *CheckoutSaga) publish(
	ctx context.Context,
	order *domain.Order,
	eventType string,
	payload any,
) error {
	if c.cfg.Events == nil {
		return nil
	}

	eventID := idgen.New()
	now := c.cfg.Clock()

	envelope := &eventsv1.Envelope{
		EventId:   eventID,
		EventType: eventType,
		// The saga id is the order id in this service, which is what lets a
		// consumer that sees several events for one checkout order them.
		SagaId:        order.ID,
		CorrelationId: order.ID,
		SchemaVersion: 1,
		OccurredAt:    timestamppb.New(now),
	}

	switch p := payload.(type) {
	case *eventsv1.Envelope_OrderCreated:
		envelope.Payload = p
	case *eventsv1.Envelope_OrderConfirmed:
		envelope.Payload = p
	case *eventsv1.Envelope_OrderCancelled:
		envelope.Payload = p
	default:
		return fmt.Errorf("order %s: unsupported event payload %T for %s", order.ID, payload, eventType)
	}

	encoded, err := proto.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("order %s: marshal %s: %w", order.ID, eventType, err)
	}

	event := domain.Event{
		ID:          eventID,
		Type:        eventType,
		AggregateID: order.ID,
		Payload:     encoded,
		OccurredAt:  now,
		TraceParent: traceParentFrom(ctx),
	}

	if err := c.cfg.Events.Publish(ctx, event); err != nil {
		return fmt.Errorf("order %s: publish %s: %w", order.ID, eventType, err)
	}

	c.cfg.Logger.InfoContext(ctx, "event published",
		"event_type", eventType,
		"event_id", eventID,
		"order_id", order.ID,
	)
	return nil
}

// traceParentFrom extracts the W3C traceparent from the request context.
//
// The carrier is a map rather than gRPC metadata because the destination is a
// database column: the value has to survive until the poller publishes it, by
// which time the original context is long gone.
func traceParentFrom(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// toProtoMoney converts the domain's money type to its wire representation.
//
// The conversion is explicit rather than a shared type because the domain must
// not depend on protobuf: a domain that can only express values the wire format
// supports is a domain shaped by its transport.
func toProtoMoney(m money.Money) (*commonv1.Money, error) {
	if !m.IsValid() {
		return nil, fmt.Errorf("amount has no currency")
	}
	return &commonv1.Money{
		AmountMinor: m.Amount(),
		Currency:    string(m.Currency()),
	}, nil
}
