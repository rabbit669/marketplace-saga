// Package config provides environment-based configuration loading.
//
// The design goal is that a misconfigured service fails at startup with *all*
// of its problems listed at once, rather than one per restart. Every accessor
// records a parse error instead of panicking or silently substituting a
// default; callers check Err() once, after the whole config struct is built.
package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Loader reads environment variables, optionally behind a common prefix, and
// accumulates parse failures until Err is called.
//
// A Loader is not safe for concurrent use; build configuration on a single
// goroutine during startup.
type Loader struct {
	prefix string
	errs   []error
}

// New returns a Loader whose keys are prefixed. For New("ORDER"), the key
// "GRPC_ADDR" resolves to the environment variable ORDER_GRPC_ADDR.
// An empty prefix reads bare keys.
func New(prefix string) *Loader {
	return &Loader{prefix: strings.ToUpper(strings.TrimSpace(prefix))}
}

// key resolves a short key to its full environment variable name.
func (l *Loader) key(k string) string {
	k = strings.ToUpper(strings.TrimSpace(k))
	if l.prefix == "" {
		return k
	}
	return l.prefix + "_" + k
}

func (l *Loader) errf(key, format string, args ...any) {
	l.errs = append(l.errs, fmt.Errorf("%s: %s", l.key(key), fmt.Sprintf(format, args...)))
}

// Err returns a single error joining every problem found so far, or nil.
func (l *Loader) Err() error {
	if len(l.errs) == 0 {
		return nil
	}
	return errors.Join(l.errs...)
}

// raw returns the value for key and whether it was set to a non-empty string.
func (l *Loader) raw(k string) (string, bool) {
	v, ok := os.LookupEnv(l.key(k))
	v = strings.TrimSpace(v)
	return v, ok && v != ""
}

// String returns the value for key, or def when unset or empty.
func (l *Loader) String(k, def string) string {
	if v, ok := l.raw(k); ok {
		return v
	}
	return def
}

// RequiredString returns the value for key and records an error when it is
// missing. Use it for values with no safe default, such as credentials.
func (l *Loader) RequiredString(k string) string {
	v, ok := l.raw(k)
	if !ok {
		l.errf(k, "is required but not set")
	}
	return v
}

// StringSlice splits a comma-separated value, trimming spaces and dropping
// empty entries. Unset or empty yields def.
func (l *Loader) StringSlice(k string, def []string) []string {
	v, ok := l.raw(k)
	if !ok {
		return def
	}

	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

// Int returns the value for key parsed as an int, or def when unset.
func (l *Loader) Int(k string, def int) int {
	v, ok := l.raw(k)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.errf(k, "must be an integer, got %q", v)
		return def
	}
	return n
}

// Int64 returns the value for key parsed as an int64, or def when unset.
func (l *Loader) Int64(k string, def int64) int64 {
	v, ok := l.raw(k)
	if !ok {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		l.errf(k, "must be an integer, got %q", v)
		return def
	}
	return n
}

// Float64 returns the value for key parsed as a float64, or def when unset.
func (l *Loader) Float64(k string, def float64) float64 {
	v, ok := l.raw(k)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		l.errf(k, "must be a number, got %q", v)
		return def
	}
	return f
}

// Bool returns the value for key parsed as a bool, or def when unset.
func (l *Loader) Bool(k string, def bool) bool {
	v, ok := l.raw(k)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		l.errf(k, "must be a boolean, got %q", v)
		return def
	}
	return b
}

// Duration returns the value for key parsed as a Go duration ("250ms", "2m"),
// or def when unset.
func (l *Loader) Duration(k string, def time.Duration) time.Duration {
	v, ok := l.raw(k)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l.errf(k, "must be a duration such as 250ms or 2m, got %q", v)
		return def
	}
	return d
}

// Port returns the value for key parsed as a TCP port, or def when unset.
func (l *Loader) Port(k string, def int) int {
	n := l.Int(k, def)
	if n < 0 || n > 65535 {
		l.errf(k, "must be a valid port (0-65535), got %d", n)
		return def
	}
	return n
}

// LoadDotEnv reads a KEY=VALUE file and exports any key that is not already
// present in the environment. Existing values always win, so a real
// environment overrides the file — the behaviour everyone expects from .env.
//
// A missing file is not an error: containers get their configuration from the
// orchestrator, not from a checked-out file.
func LoadDotEnv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("open env file %s: %w", path, err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	line := 0
	for sc.Scan() {
		line++
		key, val, ok := parseDotEnvLine(sc.Text())
		if !ok {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		if err := os.Setenv(key, val); err != nil {
			return fmt.Errorf("set %s from %s:%d: %w", key, path, line, err)
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("read env file %s: %w", path, err)
	}
	return nil
}

// parseDotEnvLine parses one line of a .env file. It reports false for blanks,
// comments and anything that is not a KEY=VALUE assignment.
func parseDotEnvLine(s string) (key, val string, ok bool) {
	s = strings.TrimSpace(s)
	if s == "" || strings.HasPrefix(s, "#") {
		return "", "", false
	}
	// Tolerate the common `export KEY=VALUE` shell idiom.
	s = strings.TrimPrefix(s, "export ")

	k, v, found := strings.Cut(s, "=")
	if !found {
		return "", "", false
	}
	k = strings.TrimSpace(k)
	if k == "" {
		return "", "", false
	}
	v = strings.TrimSpace(v)

	// Strip one layer of matching quotes; anything inside is taken literally.
	if len(v) >= 2 {
		if (v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'') {
			v = v[1 : len(v)-1]
		}
	}
	return k, v, true
}
