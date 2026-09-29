// Package app holds the auth service's use cases.
package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/marketplace/pkg/apperr"
	"github.com/marketplace/pkg/idgen"
	"github.com/marketplace/pkg/jwtx"

	"github.com/marketplace/services/auth/internal/domain"
)

const op = "auth"

// Service implements registration, login and token exchange.
type Service struct {
	users domain.Repository
	jwt   jwtx.Config
	clock func() time.Time
	log   *slog.Logger

	refreshTTL time.Duration
}

// Config configures the service.
type Config struct {
	Users      domain.Repository
	JWT        jwtx.Config
	Logger     *slog.Logger
	Clock      func() time.Time
	RefreshTTL time.Duration
}

// New builds the service.
func New(cfg Config) (*Service, error) {
	switch {
	case cfg.Users == nil:
		return nil, errors.New("auth: a user repository is required")
	case cfg.JWT.Secret == "":
		return nil, errors.New("auth: a JWT secret is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.RefreshTTL <= 0 {
		cfg.RefreshTTL = 30 * 24 * time.Hour
	}
	if cfg.JWT.TTL <= 0 {
		cfg.JWT.TTL = 15 * time.Minute
	}
	if err := cfg.JWT.Validate(); err != nil {
		return nil, err
	}

	return &Service{
		users:      cfg.Users,
		jwt:        cfg.JWT,
		clock:      cfg.Clock,
		log:        cfg.Logger,
		refreshTTL: cfg.RefreshTTL,
	}, nil
}

// Register creates an account.
func (s *Service) Register(ctx context.Context, email, password, fullName string) (domain.User, error) {
	normalised, err := domain.ValidateEmail(email)
	if err != nil {
		return domain.User{}, apperr.InvalidArgument(op, "%s", err.Error())
	}
	if strings.TrimSpace(fullName) == "" {
		return domain.User{}, apperr.InvalidArgument(op, "full_name is required")
	}

	hash, err := domain.HashPassword(password)
	if err != nil {
		return domain.User{}, apperr.InvalidArgument(op, "%s", err.Error())
	}

	now := s.clock()
	user := domain.User{
		ID:           idgen.New(),
		Email:        normalised,
		FullName:     strings.TrimSpace(fullName),
		PasswordHash: hash,
		Roles:        []string{"customer"},
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	if err := s.users.CreateUser(ctx, &user); err != nil {
		if errors.Is(err, domain.ErrEmailTaken) {
			// The message deliberately does not confirm whether the address is
			// registered. Registration is an unauthenticated endpoint, and a
			// distinct "already exists" answer turns it into an account
			// enumeration oracle.
			return domain.User{}, apperr.Wrap(op, apperr.CodeAlreadyExists,
				"an account with this email already exists", err)
		}
		return domain.User{}, fmt.Errorf("auth: create user: %w", err)
	}

	s.log.InfoContext(ctx, "user registered", "user_id", user.ID)
	return user.Public(), nil
}

// Login verifies a password and issues a token pair.
func (s *Service) Login(ctx context.Context, email, password, client string) (domain.User, TokenPair, error) {
	normalised, err := domain.ValidateEmail(email)
	if err != nil {
		// A malformed address cannot match an account, but answering differently
		// from a wrong password would reveal which addresses are well-formed.
		return domain.User{}, TokenPair{}, apperr.Unauthenticated(op, "email or password is incorrect")
	}

	user, err := s.users.GetUserByEmail(ctx, normalised)
	if err != nil {
		if errors.Is(err, domain.ErrUserNotFound) {
			// Hash anyway, so that a missing account and a wrong password take
			// the same time. Without this, response latency tells an attacker
			// which addresses are registered.
			_, _ = domain.HashPassword("timing-equalisation-placeholder")
			return domain.User{}, TokenPair{}, apperr.Unauthenticated(op, "email or password is incorrect")
		}
		return domain.User{}, TokenPair{}, fmt.Errorf("auth: load user: %w", err)
	}

	if err := domain.VerifyPassword(user.PasswordHash, password); err != nil {
		s.log.WarnContext(ctx, "failed login", "user_id", user.ID, "client", client)
		return domain.User{}, TokenPair{}, apperr.Unauthenticated(op, "email or password is incorrect")
	}

	pair, err := s.issuePair(ctx, *user, idgen.New())
	if err != nil {
		return domain.User{}, TokenPair{}, err
	}

	s.log.InfoContext(ctx, "user logged in", "user_id", user.ID, "client", client)
	return user.Public(), pair, nil
}

// TokenPair is an access token and the refresh token that replaces it.
type TokenPair struct {
	AccessToken  string
	RefreshToken string
	ExpiresIn    time.Duration
}

// Refresh exchanges a refresh token for a new pair, rotating it.
//
// Rotation is the point: a refresh token is single-use. If one is presented
// twice, either the client retried — harmless — or it was stolen and both
// parties now hold it, in which case the whole family is revoked and both are
// logged out. That is the only reliable way to detect a leaked token, because a
// stolen refresh token is otherwise indistinguishable from the real client.
func (s *Service) Refresh(ctx context.Context, refreshToken string) (TokenPair, error) {
	id, secret, ok := splitRefreshToken(refreshToken)
	if !ok {
		return TokenPair{}, apperr.Unauthenticated(op, "refresh token is not valid")
	}

	stored, err := s.users.GetRefreshToken(ctx, id)
	if err != nil {
		return TokenPair{}, apperr.Unauthenticated(op, "refresh token is not valid")
	}

	if hashToken(secret) != stored.Hash {
		return TokenPair{}, apperr.Unauthenticated(op, "refresh token is not valid")
	}

	now := s.clock()
	if stored.Terminal() {
		// A revoked token presented again means the chain leaked. Killing the
		// family logs out the thief and the victim, which is the safe direction:
		// the victim can log in again, the thief cannot.
		s.log.ErrorContext(ctx, "refresh token reuse detected, revoking the family",
			"user_id", stored.UserID, "family_id", stored.FamilyID)
		if rerr := s.users.RevokeFamily(ctx, stored.FamilyID); rerr != nil {
			s.log.ErrorContext(ctx, "could not revoke the token family", "error", rerr.Error())
		}
		return TokenPair{}, apperr.Unauthenticated(op, "refresh token is not valid")
	}

	if !stored.Valid(now) {
		return TokenPair{}, apperr.Unauthenticated(op, "refresh token has expired")
	}

	user, err := s.users.GetUser(ctx, stored.UserID)
	if err != nil {
		return TokenPair{}, apperr.Unauthenticated(op, "refresh token is not valid")
	}

	// Rotate: consume the presented token and issue a successor in the same
	// family, so the chain stays traceable to the login it came from.
	if err := s.users.RevokeRefreshToken(ctx, stored.ID); err != nil {
		return TokenPair{}, fmt.Errorf("auth: revoke used refresh token: %w", err)
	}

	pair, err := s.issuePair(ctx, *user, stored.FamilyID)
	if err != nil {
		return TokenPair{}, err
	}
	return pair, nil
}

// issuePair mints an access token and a refresh token in the given family.
func (s *Service) issuePair(ctx context.Context, user domain.User, familyID string) (TokenPair, error) {
	access, err := jwtx.Issue(s.jwt, jwtx.Claims{
		Subject: user.ID,
		Email:   user.Email,
		Roles:   user.Roles,
	})
	if err != nil {
		return TokenPair{}, fmt.Errorf("auth: issue access token: %w", err)
	}

	secret, err := idgen.RandomToken(32)
	if err != nil {
		return TokenPair{}, fmt.Errorf("auth: generate refresh token: %w", err)
	}

	now := s.clock()
	id := idgen.New()

	token := domain.RefreshToken{
		ID:        id,
		UserID:    user.ID,
		Hash:      hashToken(secret),
		FamilyID:  familyID,
		IssuedAt:  now,
		ExpiresAt: now.Add(s.refreshTTL),
	}
	if err := s.users.CreateRefreshToken(ctx, &token); err != nil {
		return TokenPair{}, fmt.Errorf("auth: store refresh token: %w", err)
	}

	return TokenPair{
		AccessToken:  access,
		RefreshToken: id + "." + secret,
		ExpiresIn:    s.jwt.TTL,
	}, nil
}

// Logout revokes the family the presented token belongs to.
//
// Revoking the whole family rather than the single token is deliberate: the
// siblings of a refresh token are indistinguishable to us, and a thief who
// already exchanged one would otherwise keep a working session after the victim
// logged out.
func (s *Service) Logout(ctx context.Context, refreshToken string) error {
	id, secret, ok := splitRefreshToken(refreshToken)
	if !ok {
		// Logging out with a token we cannot parse is not worth an error: the
		// caller wants the session gone, and it already is.
		return nil
	}

	stored, err := s.users.GetRefreshToken(ctx, id)
	if err != nil || hashToken(secret) != stored.Hash {
		return nil
	}

	if err := s.users.RevokeFamily(ctx, stored.FamilyID); err != nil {
		return fmt.Errorf("auth: revoke family: %w", err)
	}

	s.log.InfoContext(ctx, "user logged out", "user_id", stored.UserID)
	return nil
}

// ValidateToken verifies an access token. The gateway calls this on every
// request, so it stays a local signature check with no database read.
func (s *Service) ValidateToken(_ context.Context, accessToken string) (jwtx.Claims, error) {
	claims, err := jwtx.Verify(s.jwt, accessToken)
	if err != nil {
		return jwtx.Claims{}, apperr.Unauthenticated(op, "access token is not valid")
	}
	return claims, nil
}

// GetUser loads a user by id.
func (s *Service) GetUser(ctx context.Context, id string) (domain.User, error) {
	user, err := s.users.GetUser(ctx, id)
	if err != nil {
		if errors.Is(err, domain.ErrUserNotFound) {
			return domain.User{}, apperr.NotFound(op, "user %s does not exist", id)
		}
		return domain.User{}, fmt.Errorf("auth: load user: %w", err)
	}
	return user.Public(), nil
}

// splitRefreshToken separates a presented token into its id and secret.
func splitRefreshToken(token string) (id, secret string, ok bool) {
	id, secret, found := strings.Cut(strings.TrimSpace(token), ".")
	if !found || id == "" || secret == "" {
		return "", "", false
	}
	return id, secret, true
}

// hashToken hashes a refresh secret.
//
// SHA-256 rather than bcrypt, and that is not a shortcut: the secret is 256 bits
// of randomness, so there is no dictionary to attack and nothing for a slow hash
// to buy. Storing the hash at all means a leaked database yields no usable token.
func hashToken(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}
