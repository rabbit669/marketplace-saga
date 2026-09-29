// Package app turns domain events into notifications.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/marketplace/pkg/idgen"
	commonv1 "github.com/marketplace/pkg/proto/common/v1"
	eventsv1 "github.com/marketplace/pkg/proto/events/v1"
)

// Notification is a message that would be sent to a customer.
type Notification struct {
	ID         string
	EventID    string
	EventType  string
	OrderID    string
	CustomerID string
	Channel    string
	Subject    string
	Body       string
	CreatedAt  time.Time
}

// Sender delivers a notification.
//
// It is a port because the real implementation is an email or push provider, and
// the point of this service is what it decides to send, not how it is
// transmitted. The development implementation writes to the log.
type Sender interface {
	Send(ctx context.Context, n Notification) error
}

// Service consumes domain events and sends the notifications they imply.
type Service struct {
	sender Sender
	log    *slog.Logger
	clock  func() time.Time

	mu   sync.Mutex
	sent map[string]Notification
	// skipped counts events that produced no notification, so a test can prove
	// the service is discriminating rather than merely quiet.
	skipped int
}

// Config configures the service.
type Config struct {
	Sender Sender
	Logger *slog.Logger
	Clock  func() time.Time
}

// New builds the service.
func New(cfg Config) (*Service, error) {
	if cfg.Sender == nil {
		return nil, errors.New("notification: a sender is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	return &Service{
		sender: cfg.Sender,
		log:    cfg.Logger,
		clock:  cfg.Clock,
		sent:   make(map[string]Notification),
	}, nil
}

// Handle decodes an event envelope and sends whatever it implies.
//
// Not every event produces a notification, and that is the interesting part of
// this service: `order.created` is an implementation detail the customer did not
// ask to hear about, while `order.confirmed` and `order.cancelled` are both
// things they must be told. Sending on every event would train customers to
// ignore the ones that matter.
func (s *Service) Handle(ctx context.Context, eventType string, payload []byte) error {
	envelope := &eventsv1.Envelope{}
	if err := protoUnmarshal(payload, envelope); err != nil {
		return fmt.Errorf("decode event envelope: %w", err)
	}

	n, ok := s.compose(envelope)
	if !ok {
		s.mu.Lock()
		s.skipped++
		s.mu.Unlock()

		s.log.DebugContext(ctx, "event produced no notification", "event_type", eventType)
		return nil
	}

	if err := s.sender.Send(ctx, n); err != nil {
		return fmt.Errorf("send notification for order %s: %w", n.OrderID, err)
	}

	s.mu.Lock()
	s.sent[n.EventID] = n
	s.mu.Unlock()

	s.log.InfoContext(ctx, "notification sent",
		"order_id", n.OrderID,
		"customer_id", n.CustomerID,
		"event_type", n.EventType,
		"subject", n.Subject,
	)
	return nil
}

// compose decides what, if anything, to tell the customer.
func (s *Service) compose(e *eventsv1.Envelope) (Notification, bool) {
	base := Notification{
		ID:        newID(),
		EventID:   e.GetEventId(),
		EventType: e.GetEventType(),
		Channel:   "email",
		CreatedAt: s.clock(),
	}

	switch payload := e.GetPayload().(type) {
	case *eventsv1.Envelope_OrderConfirmed:
		base.OrderID = payload.OrderConfirmed.GetOrderId()
		base.CustomerID = payload.OrderConfirmed.GetCustomerId()
		base.Subject = "Your order is confirmed"
		base.Body = fmt.Sprintf(
			"Order %s has been confirmed. %s will be taken from your card. Payment reference %s.",
			base.OrderID, formatMoney(payload.OrderConfirmed.GetTotal()), payload.OrderConfirmed.GetPaymentId(),
		)
		return base, true

	case *eventsv1.Envelope_OrderCancelled:
		base.OrderID = payload.OrderCancelled.GetOrderId()
		base.CustomerID = payload.OrderCancelled.GetCustomerId()
		base.Subject = "Your order could not be completed"
		// The reason is passed through as the step that failed rather than as
		// the provider's error text. "checkout_failed:authorize_payment" tells
		// support where to look, and the advice to the customer is the same
		// either way.
		base.Body = fmt.Sprintf(
			"Order %s was cancelled and you have not been charged. Nothing was reserved. Reference: %s.",
			base.OrderID, payload.OrderCancelled.GetFailedStep(),
		)
		return base, true

	default:
		// order.created, stock.reserved and the payment events are internal
		// progress markers. The customer does not need a message for each one,
		// and sending them would bury the two that matter.
		return Notification{}, false
	}
}

// Sent returns the notifications sent so far, for assertions in tests.
func (s *Service) Sent() []Notification {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]Notification, 0, len(s.sent))
	for _, n := range s.sent {
		out = append(out, n)
	}
	return out
}

// Skipped returns how many events produced no notification.
func (s *Service) Skipped() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.skipped
}

// protoUnmarshal decodes a protobuf message.
//
// Wrapped in a one-line helper so the rest of the file reads as business logic
// rather than as serialisation.
func protoUnmarshal(data []byte, msg proto.Message) error {
	return proto.Unmarshal(data, msg)
}

// newID mints a notification id.
func newID() string { return idgen.New() }

// formatMoney renders a wire amount for a message body.
func formatMoney(m *commonv1.Money) string {
	if m == nil {
		return "the amount due"
	}
	units := m.GetAmountMinor()
	whole := units / 100
	cents := units % 100
	return fmt.Sprintf("%d.%02d %s", whole, cents, m.GetCurrency())
}
