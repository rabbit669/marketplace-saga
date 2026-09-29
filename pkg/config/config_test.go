package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoader_PrefixAndDefaults(t *testing.T) {
	t.Setenv("ORDER_GRPC_ADDR", ":9094")

	l := New("order")

	if got := l.String("GRPC_ADDR", ":0"); got != ":9094" {
		t.Errorf("String(GRPC_ADDR) = %q, want %q", got, ":9094")
	}
	if got := l.String("MISSING", "fallback"); got != "fallback" {
		t.Errorf("String(MISSING) = %q, want %q", got, "fallback")
	}
	if err := l.Err(); err != nil {
		t.Errorf("Err() = %v, want nil", err)
	}
}

func TestLoader_EmptyValueFallsBackToDefault(t *testing.T) {
	// An explicitly empty variable is treated as unset. This matters for
	// generated compose files, which happily export KEY= for undefined values.
	t.Setenv("ORDER_GRPC_ADDR", "")

	l := New("ORDER")
	if got := l.String("GRPC_ADDR", ":9094"); got != ":9094" {
		t.Errorf("String(GRPC_ADDR) = %q, want the default %q", got, ":9094")
	}
}

func TestLoader_NoPrefix(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")

	l := New("")
	if got := l.String("LOG_LEVEL", "info"); got != "debug" {
		t.Errorf("String(LOG_LEVEL) = %q, want %q", got, "debug")
	}
}

func TestLoader_TypedAccessors(t *testing.T) {
	t.Setenv("T_N", "42")
	t.Setenv("T_N64", "9007199254740993")
	t.Setenv("T_F", "0.15")
	t.Setenv("T_B", "true")
	t.Setenv("T_D", "250ms")
	t.Setenv("T_LIST", "a:1, b:2 ,,c:3")
	t.Setenv("T_PORT", "8080")

	l := New("")

	if got := l.Int("T_N", 0); got != 42 {
		t.Errorf("Int = %d, want 42", got)
	}
	if got := l.Int64("T_N64", 0); got != 9007199254740993 {
		t.Errorf("Int64 = %d, want 9007199254740993", got)
	}
	if got := l.Float64("T_F", 0); got != 0.15 {
		t.Errorf("Float64 = %v, want 0.15", got)
	}
	if got := l.Bool("T_B", false); !got {
		t.Error("Bool = false, want true")
	}
	if got := l.Duration("T_D", 0); got != 250*time.Millisecond {
		t.Errorf("Duration = %v, want 250ms", got)
	}
	if got := l.Port("T_PORT", 0); got != 8080 {
		t.Errorf("Port = %d, want 8080", got)
	}

	got := l.StringSlice("T_LIST", nil)
	want := []string{"a:1", "b:2", "c:3"}
	if len(got) != len(want) {
		t.Fatalf("StringSlice = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("StringSlice = %v, want %v", got, want)
		}
	}

	if err := l.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
}

func TestLoader_AccumulatesEveryParseError(t *testing.T) {
	// This is the point of the Loader: one restart should reveal every bad
	// value, not just the first one the parser happens to reach.
	t.Setenv("T_N", "not-a-number")
	t.Setenv("T_B", "maybe")
	t.Setenv("T_D", "soon")
	t.Setenv("T_PORT", "99999")

	l := New("")
	l.Int("T_N", 0)
	l.Bool("T_B", false)
	l.Duration("T_D", 0)
	l.Port("T_PORT", 0)

	err := l.Err()
	if err == nil {
		t.Fatal("Err() = nil, want the accumulated parse errors")
	}

	msg := err.Error()
	for _, key := range []string{"T_N", "T_B", "T_D", "T_PORT"} {
		if !strings.Contains(msg, key) {
			t.Errorf("Err() message does not mention %s:\n%s", key, msg)
		}
	}
}

func TestLoader_RequiredString(t *testing.T) {
	l := New("AUTH")
	got := l.RequiredString("JWT_SECRET")
	if got != "" {
		t.Errorf("RequiredString = %q, want empty", got)
	}
	if err := l.Err(); err == nil {
		t.Fatal("Err() = nil, want an error for the missing required key")
	} else if !strings.Contains(err.Error(), "AUTH_JWT_SECRET") {
		t.Errorf("error should name the full variable, got: %v", err)
	}
}

func TestLoader_PortRejectsOutOfRange(t *testing.T) {
	t.Setenv("T_PORT", "70000")

	l := New("")
	if got := l.Port("T_PORT", 1234); got != 1234 {
		t.Errorf("Port = %d, want the default 1234", got)
	}
	if l.Err() == nil {
		t.Error("Err() = nil, want an out-of-range error")
	}
}

func TestParseDotEnvLine(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		wantKey string
		wantVal string
		wantOK  bool
	}{
		{"simple", "KEY=value", "KEY", "value", true},
		{"spaces", "  KEY = value  ", "KEY", "value", true},
		{"export idiom", "export KEY=value", "KEY", "value", true},
		{"double quotes", `KEY="a b"`, "KEY", "a b", true},
		{"single quotes", `KEY='a b'`, "KEY", "a b", true},
		{"empty value", "KEY=", "KEY", "", true},
		{"value with equals", "KEY=a=b", "KEY", "a=b", true},
		{"comment", "# KEY=value", "", "", false},
		{"blank", "   ", "", "", false},
		{"no equals", "JUSTAWORD", "", "", false},
		{"empty key", "=value", "", "", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key, val, ok := parseDotEnvLine(tc.line)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if key != tc.wantKey || val != tc.wantVal {
				t.Errorf("got (%q, %q), want (%q, %q)", key, val, tc.wantKey, tc.wantVal)
			}
		})
	}
}

func TestLoadDotEnv_ExistingEnvironmentWins(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	content := "# a comment\nFROM_FILE=file-value\nALREADY_SET=from-file\n\nEMPTY=\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write env file: %v", err)
	}

	t.Setenv("ALREADY_SET", "from-environment")

	if err := LoadDotEnv(path); err != nil {
		t.Fatalf("LoadDotEnv: %v", err)
	}

	if got := os.Getenv("FROM_FILE"); got != "file-value" {
		t.Errorf("FROM_FILE = %q, want %q", got, "file-value")
	}
	if got := os.Getenv("ALREADY_SET"); got != "from-environment" {
		t.Errorf("ALREADY_SET = %q, want the environment to win: %q", got, "from-environment")
	}
	if got, ok := os.LookupEnv("EMPTY"); !ok || got != "" {
		t.Errorf("EMPTY = (%q, %v), want (\"\", true)", got, ok)
	}
}

func TestLoadDotEnv_MissingFileIsNotAnError(t *testing.T) {
	// Containers receive configuration from the orchestrator; there is no .env
	// inside the image and its absence must not stop the process from booting.
	if err := LoadDotEnv(filepath.Join(t.TempDir(), "does-not-exist")); err != nil {
		t.Errorf("LoadDotEnv(missing) = %v, want nil", err)
	}
}
