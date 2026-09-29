// Package jwtx issues and verifies the access tokens used at the API edge.
//
// The gateway verifies tokens; the auth service issues them. Both sides share
// this package so that a claim added on one side cannot be silently ignored on
// the other.
//
// JWT verification has two classic ways to go wrong, and both are handled
// explicitly here:
//
//   - **Algorithm confusion.** A verifier that trusts the token's own `alg`
//     header can be talked into accepting a token signed with a different
//     algorithm — most famously `none`, or an RSA public key used as an HMAC
//     secret. The signing method is therefore pinned by the caller and the
//     header is never consulted for the decision.
//   - **Missing expiry.** A token with no `exp` is valid forever. Expiry is
//     required rather than optional, so that a leaked token has a bounded life.
package jwtx

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Claims is the token payload the platform uses.
type Claims struct {
	// Subject is the user id. It is what the gateway forwards downstream as the
	// authenticated caller.
	Subject string
	Email   string
	Roles   []string
	Issuer  string

	IssuedAt  time.Time
	ExpiresAt time.Time
}

// Sentinel errors, so callers can distinguish "expired" from "forged" in logs
// without matching on message text.
var (
	ErrMalformed   = errors.New("token is malformed")
	ErrExpired     = errors.New("token has expired")
	ErrNotYetValid = errors.New("token is not valid yet")
	ErrWrongIssuer = errors.New("token was issued by someone else")
	ErrWrongMethod = errors.New("token uses an unsupported signing algorithm")
)

// Config describes how tokens are signed and validated.
type Config struct {
	// Secret is the HMAC key. It must be at least 32 bytes: a short key is
	// brute-forceable offline, and an attacker who can forge tokens can be any
	// customer they like.
	Secret string
	// Issuer is written into `iss` and required on verification.
	Issuer string
	// TTL is the access token lifetime.
	TTL time.Duration
	// Now overrides the clock, for tests.
	Now func() time.Time
}

func (c Config) withDefaults() Config {
	if c.Issuer == "" {
		c.Issuer = "marketplace"
	}
	if c.TTL <= 0 {
		c.TTL = 15 * time.Minute
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

// MinSecretLength is the shortest HMAC key accepted. 256 bits is the output size
// of SHA-256, so a shorter key is the weakest link in the construction.
const MinSecretLength = 32

// Validate reports whether the configuration is usable to sign tokens.
func (c Config) Validate() error {
	if len(c.Secret) < MinSecretLength {
		return fmt.Errorf("jwt: secret must be at least %d bytes, got %d", MinSecretLength, len(c.Secret))
	}
	return nil
}

// Issue signs an access token.
func Issue(cfg Config, claims Claims) (string, error) {
	cfg = cfg.withDefaults()

	if err := cfg.Validate(); err != nil {
		return "", err
	}
	if claims.Subject == "" {
		return "", errors.New("jwt: subject is required")
	}

	now := cfg.Now()
	if claims.Issuer == "" {
		claims.Issuer = cfg.Issuer
	}
	if claims.IssuedAt.IsZero() {
		claims.IssuedAt = now
	}
	if claims.ExpiresAt.IsZero() {
		claims.ExpiresAt = now.Add(cfg.TTL)
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":   claims.Subject,
		"email": claims.Email,
		"roles": claims.Roles,
		"iss":   claims.Issuer,
		"iat":   claims.IssuedAt.Unix(),
		"exp":   claims.ExpiresAt.Unix(),
	})

	signed, err := token.SignedString([]byte(cfg.Secret))
	if err != nil {
		return "", fmt.Errorf("jwt: sign: %w", err)
	}
	return signed, nil
}

// Verify validates a token and returns its claims.
func Verify(cfg Config, token string) (Claims, error) {
	cfg = cfg.withDefaults()

	if err := cfg.Validate(); err != nil {
		return Claims{}, err
	}

	token = strings.TrimSpace(token)
	if token == "" {
		return Claims{}, ErrMalformed
	}

	var claims jwt.MapClaims

	parsed, err := jwt.ParseWithClaims(token, &claims, func(t *jwt.Token) (any, error) {
		// The signing method is checked against a fixed expectation rather than
		// read from the token. Trusting the header is what makes algorithm
		// confusion attacks possible.
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("%w: %v", ErrWrongMethod, t.Header["alg"])
		}
		if t.Method.Alg() != jwt.SigningMethodHS256.Alg() {
			return nil, fmt.Errorf("%w: %s", ErrWrongMethod, t.Method.Alg())
		}
		return []byte(cfg.Secret), nil
	},
		// The clock is injected so that expiry is testable without sleeping.
		jwt.WithTimeFunc(cfg.Now),
		jwt.WithIssuer(cfg.Issuer),
		jwt.WithExpirationRequired(),
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
	)
	if err != nil {
		return Claims{}, translateError(err)
	}
	if !parsed.Valid {
		return Claims{}, ErrMalformed
	}

	subject, err := claims.GetSubject()
	if err != nil || subject == "" {
		return Claims{}, fmt.Errorf("%w: missing subject", ErrMalformed)
	}

	out := Claims{Subject: subject}
	if v, ok := claims["email"].(string); ok {
		out.Email = v
	}
	if v, ok := claims["iss"].(string); ok {
		out.Issuer = v
	}
	if raw, ok := claims["roles"].([]any); ok {
		for _, r := range raw {
			if s, ok := r.(string); ok {
				out.Roles = append(out.Roles, s)
			}
		}
	}
	if v, ok := claims["iat"].(float64); ok {
		out.IssuedAt = time.Unix(int64(v), 0)
	}
	if v, ok := claims["exp"].(float64); ok {
		out.ExpiresAt = time.Unix(int64(v), 0)
	}

	return out, nil
}

// translateError maps the library's errors onto this package's sentinels, so
// callers do not have to import jwt just to tell an expired token from a forged
// one.
func translateError(err error) error {
	switch {
	case errors.Is(err, jwt.ErrTokenExpired):
		return fmt.Errorf("%w: %w", ErrExpired, err)
	case errors.Is(err, jwt.ErrTokenNotValidYet):
		return fmt.Errorf("%w: %w", ErrNotYetValid, err)
	case errors.Is(err, jwt.ErrTokenInvalidIssuer):
		return fmt.Errorf("%w: %w", ErrWrongIssuer, err)
	default:
		return fmt.Errorf("%w: %w", ErrMalformed, err)
	}
}
