// Package postgres is the auth service's durable storage.
//
// It implements the domain's Repository. Two rules in here are the ones that
// matter, and both are enforced by the schema rather than by this code: one
// account per normalised address, and one row per refresh-token family that can
// be revoked in a single statement. Everything else is bookkeeping around them.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/marketplace/services/auth/internal/domain"
)

// The constraint names the store distinguishes between. The migration declares
// each of them explicitly, because they are a contract: a renamed constraint
// would turn "this address is already registered" — an expected answer that the
// caller reports as such — into a generic insert failure.
const (
	constraintUserID    = "users_pkey"
	constraintUserEmail = "users_email_key"
	constraintTokenID   = "refresh_tokens_pkey"
)

// pgUniqueViolation is SQLSTATE 23505.
const pgUniqueViolation = "23505"

// Store is the auth service's PostgreSQL adapter.
type Store struct {
	pool *pgxpool.Pool
}

// The store is a domain.Repository, checked here rather than at the call site so
// that a signature drift fails the build instead of a login.
var _ domain.Repository = (*Store)(nil)

// New returns a store backed by the pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Pool exposes the pool so that the health check and metrics can use it.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// -------------------------------------------------------------------- users

// CreateUser implements domain.Repository.
//
// The address is normalised *here*, not only wherever the caller happens to do
// it. The uniqueness rule is "one account per address", and it only holds if
// every writer stores the same form of the address; a caller that skipped
// normalisation would be able to register `Alice@example.com` beside an existing
// `alice@example.com`, and no constraint would notice.
//
// The uniqueness itself comes from the unique index on that column, so the claim
// is atomic: of any number of concurrent registrations only one commits. A
// SELECT followed by an INSERT would let every caller observe the address as
// free, which is exactly how one person ends up with two accounts.
func (s *Store) CreateUser(ctx context.Context, u *domain.User) error {
	if u == nil {
		return errors.New("create user: the user is nil")
	}

	email := domain.NormaliseEmail(u.Email)

	// A nil slice encodes as SQL NULL, which the column's NOT NULL rejects. A
	// user with no roles is an empty role set, not a missing value.
	roles := u.Roles
	if roles == nil {
		roles = []string{}
	}

	_, err := s.pool.Exec(ctx, `
		INSERT INTO users (
			id, email, full_name, password_hash, roles, email_verified, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		u.ID, email, u.FullName, u.PasswordHash, roles, u.EmailVerified,
		u.CreatedAt, u.UpdatedAt,
	)
	if err != nil {
		switch violatedConstraint(err) {
		case constraintUserEmail:
			return fmt.Errorf("%w: %s", domain.ErrEmailTaken, email)
		case constraintUserID:
			// A taken id is a bug upstream rather than a duplicate account, but
			// the caller's only sane response is the same: registration failed.
			return fmt.Errorf("%w: id %s is taken", domain.ErrEmailTaken, u.ID)
		}
		return fmt.Errorf("insert user %s: %w", u.ID, err)
	}

	return nil
}

// GetUser implements domain.Repository.
func (s *Store) GetUser(ctx context.Context, id string) (*domain.User, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, email, full_name, password_hash, roles, email_verified, created_at, updated_at
		  FROM users WHERE id = $1`, id)

	u, err := scanUser(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", domain.ErrUserNotFound, id)
		}
		return nil, fmt.Errorf("select user %s: %w", id, err)
	}

	return revalidateUser(u)
}

// GetUserByEmail implements domain.Repository.
//
// The lookup normalises its argument for the same reason the write does: the
// stored column holds the normalised form, so comparing anything else against it
// would miss. That is what makes `ALICE@example.com` find the account registered
// as `alice@example.com` instead of reporting it as unknown.
func (s *Store) GetUserByEmail(ctx context.Context, email string) (*domain.User, error) {
	normalised := domain.NormaliseEmail(email)

	row := s.pool.QueryRow(ctx, `
		SELECT id, email, full_name, password_hash, roles, email_verified, created_at, updated_at
		  FROM users WHERE email = $1`, normalised)

	u, err := scanUser(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", domain.ErrUserNotFound, normalised)
		}
		return nil, fmt.Errorf("select user by email: %w", err)
	}

	return revalidateUser(u)
}

// revalidateUser re-runs the domain's rules over a row read back from storage.
//
// The email is the identity a user authenticates with, so a stored address that
// no longer parses — or one that is not in the normalised form the unique index
// is built on — describes an account that cannot be found by the login path and
// whose uniqueness is therefore not actually guaranteed. It is reported as a
// load failure, deliberately *not* as ErrUserNotFound: answering "no such user"
// would turn a corrupt row into a wrong-password, which is a support ticket
// instead of an alert.
func revalidateUser(u *domain.User) (*domain.User, error) {
	normalised, err := domain.ValidateEmail(u.Email)
	if err != nil {
		return nil, fmt.Errorf("user %s: stored address is unusable: %w", u.ID, err)
	}
	if normalised != u.Email {
		return nil, fmt.Errorf(
			"user %s: stored address %q is not normalised; the uniqueness of addresses is not guaranteed",
			u.ID, u.Email)
	}
	return u, nil
}

// ----------------------------------------------------------- refresh tokens

// CreateRefreshToken implements domain.Repository.
//
// Only the hash is written. The secret half of the token exists in the response
// to the caller and nowhere else, so a dump of this table cannot be replayed
// against the API — which is the property that makes a leaked backup survivable.
// The id is stored as given because it is an identifier rather than a secret: a
// token can only be exchanged by presenting the secret whose hash matches.
func (s *Store) CreateRefreshToken(ctx context.Context, t *domain.RefreshToken) error {
	if t == nil {
		return errors.New("create refresh token: the token is nil")
	}

	_, err := s.pool.Exec(ctx, `
		INSERT INTO refresh_tokens (id, user_id, token_hash, family_id, issued_at, expires_at, revoked_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		t.ID, t.UserID, t.Hash, t.FamilyID, t.IssuedAt, t.ExpiresAt, nullTime(t.RevokedAt),
	)
	if err != nil {
		if violatedConstraint(err) == constraintTokenID {
			return fmt.Errorf("%w: refresh token %s already exists", domain.ErrRefreshTokenInvalid, t.ID)
		}
		return fmt.Errorf("insert refresh token %s: %w", t.ID, err)
	}

	return nil
}

// GetRefreshToken implements domain.Repository.
//
// The row that comes back carries the hash, and the caller compares the hash of
// the presented secret against it. Nothing here can decide whether the token is
// authentic on its own, which is the point: the secret never travels to the
// database, so an operator reading this table — or a backup of it — learns
// nothing they can exchange for a session.
func (s *Store) GetRefreshToken(ctx context.Context, id string) (*domain.RefreshToken, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, user_id, token_hash, family_id, issued_at, expires_at, revoked_at
		  FROM refresh_tokens WHERE id = $1`, id)

	t, err := scanToken(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", domain.ErrRefreshTokenInvalid, id)
		}
		return nil, fmt.Errorf("select refresh token %s: %w", id, err)
	}

	return revalidateToken(t)
}

// RevokeRefreshToken implements domain.Repository.
//
// Idempotent by construction: the statement only touches a token that is still
// live, and touching nothing is a success. Rotation calls this on every refresh,
// so a retry of a refresh, or a second call for a token that has already been
// consumed, must not turn into a failure — a spurious error here logs a user out
// of a session that was working.
func (s *Store) RevokeRefreshToken(ctx context.Context, id string) error {
	if _, err := s.pool.Exec(ctx, `
		UPDATE refresh_tokens SET revoked_at = now()
		 WHERE id = $1 AND revoked_at IS NULL`, id); err != nil {
		return fmt.Errorf("revoke refresh token %s: %w", id, err)
	}
	return nil
}

// RevokeFamily implements domain.Repository.
//
// One statement, and that is the whole design of this method. Revoking a family
// is the response to a refresh token being presented twice, which is the only
// evidence this service ever gets that a chain leaked; a revocation that
// stopped half-way would log the victim out and leave the thief holding a token
// that still works, which is worse than not noticing the reuse at all. A single
// UPDATE is atomic — it either revokes every token of the family or fails — and
// concurrent revocations of the same family cannot interleave, because the
// second one simply finds nothing left to do.
//
// `revoked_at IS NULL` keeps the first revocation's timestamp, so the moment the
// leak was detected survives every later call, including the logout that
// typically follows it. A family nobody holds a token for is not an error: the
// caller's intent is already satisfied.
func (s *Store) RevokeFamily(ctx context.Context, familyID string) error {
	if familyID == "" {
		// An empty family names nothing. Guarding on it rather than issuing the
		// statement keeps a bug upstream — a zero-valued token struct — from
		// revoking every row that happens to have an empty family id too.
		return nil
	}

	if _, err := s.pool.Exec(ctx, `
		UPDATE refresh_tokens SET revoked_at = now()
		 WHERE family_id = $1 AND revoked_at IS NULL`, familyID); err != nil {
		return fmt.Errorf("revoke refresh token family %s: %w", familyID, err)
	}
	return nil
}

// ------------------------------------------------------------------ helpers

type rowScanner interface {
	Scan(dest ...any) error
}

func scanUser(row rowScanner) (*domain.User, error) {
	var u domain.User

	err := row.Scan(
		&u.ID, &u.Email, &u.FullName, &u.PasswordHash, &u.Roles,
		&u.EmailVerified, &u.CreatedAt, &u.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	if u.Roles == nil {
		// An empty role set, not an absent one: HasRole must answer false, and
		// a caller ranging over the slice must not have to nil-check it.
		u.Roles = []string{}
	}

	return &u, nil
}

func scanToken(row rowScanner) (*domain.RefreshToken, error) {
	var (
		t domain.RefreshToken
		// NULL and the domain's zero time mean the same thing — still live — so
		// the column is read through a pointer and folded onto the zero value.
		revokedAt *time.Time
	)

	err := row.Scan(
		&t.ID, &t.UserID, &t.Hash, &t.FamilyID,
		&t.IssuedAt, &t.ExpiresAt, &revokedAt,
	)
	if err != nil {
		return nil, err
	}

	if revokedAt != nil {
		t.RevokedAt = *revokedAt
	}

	return &t, nil
}

// revalidateToken checks that a stored token row is usable at all.
//
// The domain's own validator, RefreshToken.Valid(now), is deliberately not
// applied here: whether a token has expired depends on the caller's clock, and
// the service distinguishes "expired" from "reused" — one ends the session, the
// other revokes the whole family. What is checked is what no caller could
// recover from: a row missing one of its identifiers can never be matched
// against a presented token and can never be swept up by a family revocation, so
// it is a corrupt row rather than an unauthenticated request.
func revalidateToken(t *domain.RefreshToken) (*domain.RefreshToken, error) {
	if t.ID == "" || t.UserID == "" || t.Hash == "" || t.FamilyID == "" {
		return nil, fmt.Errorf("%w: stored token %s is missing an identifier",
			domain.ErrRefreshTokenInvalid, t.ID)
	}
	return t, nil
}

// nullTime maps the domain's zero time onto SQL NULL.
//
// A token that has not been revoked has no revocation instant, and a sentinel
// date would be a lie that a query like "what was revoked in the last hour"
// would believe.
func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// violatedConstraint returns the constraint a unique violation broke, or "" when
// err is not a unique violation.
//
// It reads the SQLSTATE and the constraint name rather than matching on message
// text, which changes between server versions and is localised.
func violatedConstraint(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
		return pgErr.ConstraintName
	}
	return ""
}
