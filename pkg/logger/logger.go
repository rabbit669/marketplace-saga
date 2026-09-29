// Package logger builds the structured logger every service logs through.
//
// Two properties matter in a distributed system and are enforced here:
//
//   - Logs are structured (slog), never formatted strings, so that a central
//     collector can index fields such as order_id and saga_id.
//   - Every line carries the trace and span id when one is in scope, which is
//     what lets you jump from a log line to the Jaeger trace that produced it.
package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// Format selects the output encoding.
type Format string

const (
	// FormatJSON emits one JSON object per line. Use in deployed environments.
	FormatJSON Format = "json"
	// FormatText emits human readable output. Use locally.
	FormatText Format = "text"
)

// Config describes how to build a logger.
type Config struct {
	// Level is one of debug, info, warn, error. Unknown values fall back to info.
	Level string
	// Format is "json" or "text"; anything else falls back to JSON.
	Format Format
	// Service is attached to every record as service=<name>.
	Service string
	// Version is attached to every record as version=<build>.
	Version string
	// Env is attached to every record as env=<dev|staging|prod>.
	Env string
	// Output defaults to os.Stdout.
	Output io.Writer
}

type ctxKey struct{}

// New builds a logger from cfg, tagging every record with the service
// identity so that logs from many services can share one index.
func New(cfg Config) *slog.Logger {
	out := cfg.Output
	if out == nil {
		out = os.Stdout
	}

	opts := &slog.HandlerOptions{Level: ParseLevel(cfg.Level)}

	var h slog.Handler
	if cfg.Format == FormatText {
		h = slog.NewTextHandler(out, opts)
	} else {
		h = slog.NewJSONHandler(out, opts)
	}

	attrs := make([]slog.Attr, 0, 3)
	if cfg.Service != "" {
		attrs = append(attrs, slog.String("service", cfg.Service))
	}
	if cfg.Version != "" {
		attrs = append(attrs, slog.String("version", cfg.Version))
	}
	if cfg.Env != "" {
		attrs = append(attrs, slog.String("env", cfg.Env))
	}
	if len(attrs) > 0 {
		h = h.WithAttrs(attrs)
	}

	return slog.New(h)
}

// ParseLevel converts a textual level to slog.Level, defaulting to info.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error", "err":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// WithLogger stores l in ctx so that downstream code can retrieve it without
// threading a logger through every function signature.
func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, l)
}

// FromContext returns the logger stored in ctx, enriched with the trace and
// span id of the active span. It falls back to slog.Default when no logger was
// stored, so it is always safe to call.
func FromContext(ctx context.Context) *slog.Logger {
	l, ok := ctx.Value(ctxKey{}).(*slog.Logger)
	if !ok || l == nil {
		l = slog.Default()
	}

	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return l
	}
	return l.With(
		slog.String("trace_id", sc.TraceID().String()),
		slog.String("span_id", sc.SpanID().String()),
	)
}

// TraceAttr returns the trace_id attribute for manual use, or an empty slice
// when no valid span is in scope.
func TraceAttr(ctx context.Context) []any {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return nil
	}
	return []any{slog.String("trace_id", sc.TraceID().String())}
}
