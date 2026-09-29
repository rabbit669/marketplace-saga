// Command devtoken prints an access token for local development.
//
// The gateway requires a bearer token on every /v1 route, and the auth service
// that would normally issue one is not part of the demo path. Rather than
// weakening the gateway with a development bypass — which is exactly the kind of
// affordance that survives into production — this mints a real token with the
// same code path the auth service will use.
//
// It refuses to run outside a development environment, because a tool whose
// whole purpose is to hand out credentials should not be one environment
// variable away from doing so in production.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/marketplace/pkg/config"
	"github.com/marketplace/pkg/jwtx"
)

func main() {
	customer := flag.String("customer", "cust-demo", "customer id to put in the token subject")
	email := flag.String("email", "demo@example.com", "email claim")
	ttl := flag.Duration("ttl", 24*time.Hour, "token lifetime")
	envFile := flag.String("env-file", ".env", "file to read JWT_SECRET from, if the environment does not set it")
	flag.Parse()

	if err := config.LoadDotEnv(*envFile); err != nil {
		fmt.Fprintf(os.Stderr, "devtoken: %v\n", err)
		os.Exit(1)
	}

	env := os.Getenv("APP_ENV")
	if env == "" {
		env = os.Getenv("ENV")
	}
	switch env {
	case "", "dev", "development", "local":
	default:
		fmt.Fprintf(os.Stderr,
			"devtoken: refusing to run with APP_ENV=%q.\n"+
				"This tool mints credentials and is meant for local development only.\n", env)
		os.Exit(1)
	}

	cfg := jwtx.Config{
		Secret: os.Getenv("JWT_SECRET"),
		Issuer: envOrDefault("JWT_ISSUER", "marketplace"),
		TTL:    *ttl,
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "devtoken: %v\n", err)
		os.Exit(1)
	}

	token, err := jwtx.Issue(cfg, jwtx.Claims{
		Subject: *customer,
		Email:   *email,
		Roles:   []string{"customer"},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "devtoken: %v\n", err)
		os.Exit(1)
	}

	// The token goes to stdout alone so that it can be captured in a shell
	// variable; everything else goes to stderr.
	fmt.Fprintln(os.Stderr, "development access token for", *customer)
	fmt.Println(token)
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
