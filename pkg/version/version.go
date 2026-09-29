// Package version exposes build metadata for every service binary.
//
// Values are injected at build time via -ldflags (see the Makefile). When a
// binary is built without those flags — `go run`, `go test`, a plain
// `go build` — we fall back to the VCS information the Go toolchain embeds,
// so `service --version` is never a bare "dev" when it does not have to be.
package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// Injected via -ldflags at release build time.
var (
	Version = ""
	Commit  = ""
	Date    = ""
)

// String returns a single-line human readable version, e.g.
// "v1.4.0 (a1b2c3d) built 2026-01-02T15:04:05Z go1.25.0 darwin/arm64".
func String() string {
	v, c, d := resolve()
	return fmt.Sprintf("%s (%s) built %s %s %s", v, c, d, runtime.Version(), runtime.GOOS+"/"+runtime.GOARCH)
}

// Short returns just the version and commit, for log fields.
func Short() string {
	v, c, _ := resolve()
	return v + "+" + c
}

// resolve returns the ldflags values when present, falling back to the VCS
// stamps embedded by the Go toolchain.
func resolve() (version, commit, date string) {
	version, commit, date = Version, Commit, Date

	if version != "" && commit != "" && date != "" {
		return version, commit, date
	}

	info, ok := debug.ReadBuildInfo()
	if !ok {
		return orDefault(version, "dev"), orDefault(commit, "unknown"), orDefault(date, "unknown")
	}

	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			if commit == "" {
				commit = truncate(s.Value, 7)
			}
		case "vcs.time":
			if date == "" {
				date = s.Value
			}
		case "vcs.modified":
			if s.Value == "true" && commit != "" {
				commit += "-dirty"
			}
		}
	}

	if version == "" {
		version = info.Main.Version
	}

	return orDefault(version, "dev"), orDefault(commit, "unknown"), orDefault(date, "unknown")
}

func orDefault(v, def string) string {
	if v == "" || v == "(devel)" {
		return def
	}
	return v
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
