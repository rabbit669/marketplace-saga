// Package sender contains notification delivery adapters.
package sender

import (
	"context"
	"log/slog"

	"github.com/marketplace/services/notification/internal/app"
)

// LogSender writes notifications to the log instead of sending them.
//
// It is a real adapter, not a test double: nothing else in the system needs a
// mail provider in order to be exercised end to end, and a demo that silently
// depends on an external account is a demo that breaks when the account does.
type LogSender struct {
	log *slog.Logger
}

// NewLog returns a sender that logs.
func NewLog(log *slog.Logger) *LogSender {
	if log == nil {
		log = slog.Default()
	}
	return &LogSender{log: log}
}

// Send implements app.Sender.
func (s *LogSender) Send(ctx context.Context, n app.Notification) error {
	s.log.InfoContext(ctx, "EMAIL",
		"to", n.CustomerID,
		"subject", n.Subject,
		"body", n.Body,
		"order_id", n.OrderID,
		"event_id", n.EventID,
	)
	return nil
}
