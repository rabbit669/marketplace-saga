package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestNew_AttachesServiceMetadataToEveryRecord(t *testing.T) {
	var buf bytes.Buffer

	log := New(Config{
		Level:   "info",
		Format:  FormatJSON,
		Service: "order",
		Version: "v1.2.3",
		Env:     "staging",
		Output:  &buf,
	})

	log.Info("order created", slog.String("order_id", "o-1"))

	fields := decodeSingleRecord(t, &buf)

	for key, want := range map[string]string{
		"service":  "order",
		"version":  "v1.2.3",
		"env":      "staging",
		"order_id": "o-1",
		"msg":      "order created",
	} {
		if got, _ := fields[key].(string); got != want {
			t.Errorf("%s = %v, want %q", key, fields[key], want)
		}
	}
}

func TestNew_EmitsNoDuplicateKeys(t *testing.T) {
	// A handler that attaches attributes plus a caller that repeats them
	// produces duplicate JSON keys. Strict log processors reject such lines, and
	// the ones that tolerate them silently keep the last value, which makes the
	// duplicate invisible until it matters.
	var buf bytes.Buffer

	log := New(Config{
		Level:   "debug",
		Format:  FormatJSON,
		Service: "gateway",
		Version: "dev",
		Env:     "dev",
		Output:  &buf,
	})

	log.Info("starting service", slog.String("health_addr", ":9180"))
	log.Debug("plain")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d log lines, want 2", len(lines))
	}

	for _, line := range lines {
		var raw map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			t.Fatalf("log line is not valid JSON: %v\n%s", err, line)
		}

		// json.Unmarshal into a map silently collapses duplicates, so the check
		// has to be on the raw key list.
		keys := rawKeys(t, line)
		seen := make(map[string]int, len(keys))
		for _, k := range keys {
			seen[k]++
		}
		for k, n := range seen {
			if n > 1 {
				t.Errorf("key %q appears %d times in: %s", k, n, line)
			}
		}
	}
}

func TestNew_RespectsLevel(t *testing.T) {
	var buf bytes.Buffer

	log := New(Config{Level: "warn", Format: FormatJSON, Output: &buf})

	log.Debug("debug message")
	log.Info("info message")
	log.Warn("warn message")

	out := buf.String()
	if strings.Contains(out, "debug message") || strings.Contains(out, "info message") {
		t.Errorf("records below the configured level were emitted:\n%s", out)
	}
	if !strings.Contains(out, "warn message") {
		t.Errorf("the warn record is missing:\n%s", out)
	}
}

func TestNew_TextFormatIsReadable(t *testing.T) {
	var buf bytes.Buffer

	log := New(Config{Level: "info", Format: FormatText, Service: "auth", Output: &buf})
	log.Info("user registered")

	out := buf.String()
	if strings.HasPrefix(strings.TrimSpace(out), "{") {
		t.Errorf("text format produced JSON: %s", out)
	}
	if !strings.Contains(out, "user registered") || !strings.Contains(out, "auth") {
		t.Errorf("text output is missing expected content: %s", out)
	}
}

func TestNew_DefaultsToJSONOnStdout(t *testing.T) {
	// A zero Config must still produce a usable logger rather than panicking.
	log := New(Config{})
	if log == nil {
		t.Fatal("New(Config{}) = nil")
	}
	log.Info("no output writer configured")
}

func TestParseLevel(t *testing.T) {
	tests := map[string]slog.Level{
		"debug":    slog.LevelDebug,
		"DEBUG":    slog.LevelDebug,
		" info ":   slog.LevelInfo,
		"warn":     slog.LevelWarn,
		"warning":  slog.LevelWarn,
		"error":    slog.LevelError,
		"err":      slog.LevelError,
		"":         slog.LevelInfo,
		"nonsense": slog.LevelInfo,
	}

	for in, want := range tests {
		if got := ParseLevel(in); got != want {
			t.Errorf("ParseLevel(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestWithLogger_FromContext(t *testing.T) {
	var buf bytes.Buffer
	custom := New(Config{Level: "info", Format: FormatJSON, Service: "custom", Output: &buf})

	ctx := WithLogger(context.Background(), custom)
	FromContext(ctx).Info("hello")

	if !strings.Contains(buf.String(), `"service":"custom"`) {
		t.Errorf("FromContext did not return the stored logger: %s", buf.String())
	}
}

func TestFromContext_FallsBackToDefault(t *testing.T) {
	// Every call site can use FromContext without checking whether a logger was
	// installed, which removes a whole class of nil dereference.
	if got := FromContext(context.Background()); got == nil {
		t.Fatal("FromContext on an empty context = nil, want slog.Default()")
	}
}

func TestFromContext_AddsTraceCorrelation(t *testing.T) {
	var buf bytes.Buffer

	log := New(Config{Level: "info", Format: FormatJSON, Service: "order", Output: &buf})
	ctx := WithLogger(context.Background(), log)

	// A valid span context is what makes a log line findable from a trace.
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
		SpanID:     trace.SpanID{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18},
		TraceFlags: trace.FlagsSampled,
	})
	ctx = trace.ContextWithSpanContext(ctx, sc)

	FromContext(ctx).Info("reservation created")

	fields := decodeSingleRecord(t, &buf)

	wantTrace := "0102030405060708090a0b0c0d0e0f10"
	if got, _ := fields["trace_id"].(string); got != wantTrace {
		t.Errorf("trace_id = %v, want %q", fields["trace_id"], wantTrace)
	}
	if got, _ := fields["span_id"].(string); got != "1112131415161718" {
		t.Errorf("span_id = %v, want %q", fields["span_id"], "1112131415161718")
	}
}

func TestFromContext_OmitsTraceFieldsWithoutASpan(t *testing.T) {
	var buf bytes.Buffer

	log := New(Config{Level: "info", Format: FormatJSON, Service: "order", Output: &buf})
	ctx := WithLogger(context.Background(), log)

	FromContext(ctx).Info("no span here")

	fields := decodeSingleRecord(t, &buf)
	if _, present := fields["trace_id"]; present {
		t.Errorf("trace_id must be absent when there is no span, got %v", fields["trace_id"])
	}
}

func TestTraceAttr(t *testing.T) {
	if got := TraceAttr(context.Background()); got != nil {
		t.Errorf("TraceAttr without a span = %v, want nil", got)
	}

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x0a},
		SpanID:     trace.SpanID{0x0b},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	if got := TraceAttr(ctx); len(got) != 1 {
		t.Errorf("TraceAttr with a span = %v, want one attribute", got)
	}
}

// ------------------------------------------------------------- helpers

func decodeSingleRecord(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()

	line := strings.TrimSpace(buf.String())
	if line == "" {
		t.Fatal("no log output was produced")
	}
	if i := strings.IndexByte(line, '\n'); i >= 0 {
		line = line[:i]
	}

	var fields map[string]any
	if err := json.Unmarshal([]byte(line), &fields); err != nil {
		t.Fatalf("output is not a single JSON object: %v\n%s", err, line)
	}
	return fields
}

// rawKeys returns the top-level keys in order, duplicates included, by scanning
// the object one value at a time.
func rawKeys(t *testing.T, line string) []string {
	t.Helper()

	dec := json.NewDecoder(strings.NewReader(line))

	tok, err := dec.Token()
	if err != nil {
		t.Fatalf("read opening token: %v", err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		t.Fatalf("expected a JSON object, got %v", tok)
	}

	var keys []string
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			t.Fatalf("read key: %v", err)
		}
		key, ok := keyTok.(string)
		if !ok {
			t.Fatalf("expected a string key, got %v", keyTok)
		}
		keys = append(keys, key)

		var discard any
		if err := dec.Decode(&discard); err != nil {
			t.Fatalf("skip value for %q: %v", key, err)
		}
	}
	return keys
}
