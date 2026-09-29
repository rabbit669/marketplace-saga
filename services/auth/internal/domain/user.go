// Package domain holds the auth service's rules.
//
// This is the only service that ever sees a password. Everything else treats a
// token as proof of identity and never looks at a credential, which keeps the
// blast radius of a logging mistake small: there is exactly one service whose
// code could accidentally write a password somewhere.
package domain

import (
	"context"
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// BcryptCost is deliberately above the library default of 10.
//
// The cost decides how long a brute-force attempt takes, and it is the only
// defence left once a password database leaks. 12 is roughly 250ms on current
// hardware: slow enough to make offline cracking expensive, fast enough that a
// login is not noticeably delayed. It is a trade-off that must be revisited as
// hardware improves, which is why it is a named constant rather than a literal.
const BcryptCost = 12

// MinPasswordLength is the only composition rule enforced.
//
// Length beats character classes: "correct horse battery staple" is stronger
// than "P@ssw0rd" and far easier to remember, while class requirements mostly
// produce `Password1!` and a sticky note.
const MinPasswordLength = 12

// MaxPasswordLength guards against a denial of service specific to bcrypt: it
// hashes only the first 72 bytes, and some implementations will happily spend
// time hashing a megabyte first.
const MaxPasswordLength = 72

// User is an authenticated principal.
type User struct {
	ID            string
	Email         string
	FullName      string
	PasswordHash  string
	Roles         []string
	EmailVerified bool
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Public returns the user without its credential, for anything that leaves the
// service. A method rather than a struct tag, so that forgetting to strip the
// hash is a compile-time choice rather than an oversight.
func (u User) Public() User {
	out := u
	out.PasswordHash = ""
	return out
}

// HasRole reports whether the user holds a role.
func (u User) HasRole(role string) bool {
	for _, r := range u.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// NormaliseEmail lower-cases and trims an address.
//
// The local part of an address is technically case-sensitive, but no mail
// provider in practice treats it that way, and treating `Alice@x.com` and
// `alice@x.com` as different accounts lets one person register twice.
func NormaliseEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

// RefreshToken is a long-lived credential that can be exchanged for a new
// access token.
type RefreshToken struct {
	// ID is the token's own identifier, stored hashed, so that a stolen
	// database cannot be replayed against the API.
	ID     string
	UserID string
	// Hash is the SHA-256 of the token value. Refresh tokens are high-entropy
	// random strings, not passwords, so a fast hash is correct here: there is
	// nothing to brute-force.
	Hash string
	// FamilyID groups tokens descended from one login. Rotating a refresh token
	// keeps the family, so revoking a family kills a stolen chain.
	FamilyID  string
	IssuedAt  time.Time
	ExpiresAt time.Time
	// RevokedAt is set when the token is used or explicitly revoked. A reused
	// revoked token means the chain was stolen, and the whole family is dropped.
	RevokedAt time.Time
}

// Terminal reports whether the token can no longer be exchanged.
func (t RefreshToken) Terminal() bool { return !t.RevokedAt.IsZero() }

// Valid reports whether the token may be exchanged at the given time.
func (t RefreshToken) Valid(now time.Time) bool {
	return !t.Terminal() && now.Before(t.ExpiresAt)
}

// Sentinel errors.
var (
	ErrUserNotFound        = errors.New("user not found")
	ErrEmailTaken          = errors.New("email is already registered")
	ErrInvalidCredentials  = errors.New("email or password is incorrect")
	ErrWeakPassword        = errors.New("password does not meet the requirements")
	ErrInvalidEmail        = errors.New("email address is not valid")
	ErrRefreshTokenInvalid = errors.New("refresh token is not valid")
)

// ValidateEmail checks an address parses and normalises it.
func ValidateEmail(raw string) (string, error) {
	email := NormaliseEmail(raw)
	if email == "" {
		return "", fmt.Errorf("%w: it is required", ErrInvalidEmail)
	}
	addr, err := mail.ParseAddress(email)
	if err != nil || addr.Address != email {
		return "", fmt.Errorf("%w: %q", ErrInvalidEmail, raw)
	}
	return email, nil
}

// HashPassword validates and hashes a password.
func HashPassword(plain string) (string, error) {
	if err := ValidatePassword(plain); err != nil {
		return "", err
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(plain), BcryptCost)
	if err != nil {
		return "", fmt.Errorf("hash password: %w", err)
	}
	return string(hash), nil
}

// ValidatePassword reports whether a password is acceptable.
func ValidatePassword(plain string) error {
	if len(plain) < MinPasswordLength {
		return fmt.Errorf("%w: it must be at least %d characters", ErrWeakPassword, MinPasswordLength)
	}
	if len(plain) > MaxPasswordLength {
		return fmt.Errorf("%w: it must be at most %d bytes", ErrWeakPassword, MaxPasswordLength)
	}
	return nil
}

// VerifyPassword compares a plaintext password against a stored hash.
//
// bcrypt's comparison is constant-time with respect to the hash, and it embeds
// the salt and cost, so nothing else is needed here.
func VerifyPassword(hash, plain string) error {
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(plain)); err != nil {
		return ErrInvalidCredentials
	}
	return nil
}

// Repository persists users and refresh tokens.
type Repository interface {
	// CreateUser stores a new user, failing with ErrEmailTaken when the
	// normalised email exists. The uniqueness must come from a constraint
	// rather than a prior read, or two concurrent registrations both succeed.
	CreateUser(ctx context.Context, u *User) error
	GetUser(ctx context.Context, id string) (*User, error)
	GetUserByEmail(ctx context.Context, email string) (*User, error)

	CreateRefreshToken(ctx context.Context, t *RefreshToken) error
	GetRefreshToken(ctx context.Context, id string) (*RefreshToken, error)
	// RevokeRefreshToken consumes a token that has just been exchanged. Rotation
	// depends on it: a refresh token is single-use, and a second presentation is
	// what reveals that the chain leaked.
	RevokeRefreshToken(ctx context.Context, id string) error
	// RevokeFamily revokes every token descended from one login. It is the
	// response to a reused token, which means the chain leaked.
	RevokeFamily(ctx context.Context, familyID string) error
}
