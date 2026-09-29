package app_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marketplace/pkg/apperr"
	"github.com/marketplace/pkg/jwtx"

	"github.com/marketplace/services/auth/internal/adapters/memory"
	"github.com/marketplace/services/auth/internal/app"
	"github.com/marketplace/services/auth/internal/domain"
)

const testSecret = "test-secret-must-be-at-least-32-bytes-long"

func newService(t *testing.T) (*app.Service, *memory.Store) {
	t.Helper()

	store := memory.New()
	svc, err := app.New(app.Config{
		Users: store,
		JWT:   jwtx.Config{Secret: testSecret, Issuer: "marketplace", TTL: 15 * time.Minute},
	})
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	return svc, store
}

const goodPassword = "correct-horse-battery"

func TestRegister_ThenLogin(t *testing.T) {
	ctx := context.Background()
	svc, _ := newService(t)

	user, err := svc.Register(ctx, "Alice@Example.com", goodPassword, "Alice")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	// The address is normalised, so a second registration in different case is
	// the same account rather than a duplicate.
	if user.Email != "alice@example.com" {
		t.Errorf("email = %q, want it lower-cased", user.Email)
	}
	if user.PasswordHash != "" {
		t.Error("Register returned a password hash; it must never leave the service")
	}

	logged, pair, err := svc.Login(ctx, "ALICE@example.com", goodPassword, "test")
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if logged.ID != user.ID {
		t.Errorf("logged in as %s, want %s", logged.ID, user.ID)
	}
	if pair.AccessToken == "" || pair.RefreshToken == "" {
		t.Fatal("Login returned an incomplete token pair")
	}
	if strings.Contains(pair.AccessToken, goodPassword) {
		t.Error("the access token contains the password")
	}
}

func TestRegister_RejectsDuplicateEmailRegardlessOfCase(t *testing.T) {
	ctx := context.Background()
	svc, _ := newService(t)

	if _, err := svc.Register(ctx, "bob@example.com", goodPassword, "Bob"); err != nil {
		t.Fatalf("first Register: %v", err)
	}

	_, err := svc.Register(ctx, "BOB@Example.COM", goodPassword, "Bob Again")
	if !apperr.Is(err, apperr.CodeAlreadyExists) {
		t.Errorf("code = %q, want %q", apperr.CodeOf(err), apperr.CodeAlreadyExists)
	}
}

func TestRegister_ConcurrentSameEmailCreatesOneAccount(t *testing.T) {
	// The uniqueness has to come from an atomic claim. A read followed by a
	// separate write lets both callers observe the address as free.
	ctx := context.Background()
	svc, _ := newService(t)

	const attempts = 12
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		succeeded int
	)

	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := svc.Register(ctx, "race@example.com", goodPassword, "Race"); err == nil {
				mu.Lock()
				succeeded++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if succeeded != 1 {
		t.Errorf("successful registrations = %d, want exactly 1", succeeded)
	}
}

func TestRegister_RejectsWeakInput(t *testing.T) {
	tests := []struct {
		name     string
		email    string
		password string
		fullName string
	}{
		{"short password", "a@example.com", "short", "A"},
		{"missing password", "a@example.com", "", "A"},
		{"invalid email", "not-an-email", goodPassword, "A"},
		{"missing email", "", goodPassword, "A"},
		{"missing name", "a@example.com", goodPassword, "   "},
		{"oversized password", "a@example.com", strings.Repeat("x", domain.MaxPasswordLength+1), "A"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, store := newService(t)

			_, err := svc.Register(context.Background(), tc.email, tc.password, tc.fullName)
			if !apperr.Is(err, apperr.CodeInvalidArgument) {
				t.Errorf("code = %q, want %q (err = %v)",
					apperr.CodeOf(err), apperr.CodeInvalidArgument, err)
			}
			if users, _ := store.Counts(); users != 0 {
				t.Errorf("%d users were stored for an invalid registration", users)
			}
		})
	}
}

func TestLogin_DoesNotRevealWhetherTheAccountExists(t *testing.T) {
	// A distinguishable answer here turns login into an account enumeration
	// oracle, which is the first step of a credential-stuffing campaign.
	ctx := context.Background()
	svc, _ := newService(t)

	if _, err := svc.Register(ctx, "real@example.com", goodPassword, "Real"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	_, _, wrongPassword := svc.Login(ctx, "real@example.com", "not-the-password", "test")
	_, _, noSuchUser := svc.Login(ctx, "ghost@example.com", goodPassword, "test")

	if wrongPassword == nil || noSuchUser == nil {
		t.Fatal("both logins should fail")
	}
	if wrongPassword.Error() != noSuchUser.Error() {
		t.Errorf("messages differ:\n  wrong password: %v\n  no such user:   %v", wrongPassword, noSuchUser)
	}
	if apperr.CodeOf(wrongPassword) != apperr.CodeOf(noSuchUser) {
		t.Errorf("codes differ: %q vs %q", apperr.CodeOf(wrongPassword), apperr.CodeOf(noSuchUser))
	}
}

func TestRefresh_RotatesAndDetectsReuse(t *testing.T) {
	// This is the whole reason refresh tokens are single-use. A second
	// presentation of the same token means either a client retried or the token
	// leaked; the two are indistinguishable, so the safe reading is taken and
	// the family is revoked.
	ctx := context.Background()
	svc, _ := newService(t)

	if _, err := svc.Register(ctx, "carol@example.com", goodPassword, "Carol"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	_, pair, err := svc.Login(ctx, "carol@example.com", goodPassword, "test")
	if err != nil {
		t.Fatalf("Login: %v", err)
	}

	rotated, err := svc.Refresh(ctx, pair.RefreshToken)
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if rotated.RefreshToken == pair.RefreshToken {
		t.Error("Refresh returned the same refresh token; it must rotate")
	}

	// The original is now consumed.
	if _, err := svc.Refresh(ctx, pair.RefreshToken); err == nil {
		t.Error("a reused refresh token was accepted")
	}

	// And the reuse killed the successor too, so a thief holding it is logged
	// out along with the victim.
	if _, err := svc.Refresh(ctx, rotated.RefreshToken); err == nil {
		t.Error("after a reuse was detected, the successor token still worked")
	}
}

func TestRefresh_RejectsGarbage(t *testing.T) {
	ctx := context.Background()
	svc, _ := newService(t)

	for _, token := range []string{"", "no-dot", ".", "id.", ".secret", "unknown.unknown"} {
		if _, err := svc.Refresh(ctx, token); err == nil {
			t.Errorf("Refresh(%q) = nil, want a rejection", token)
		}
	}
}

func TestValidateToken_AcceptsIssuedAndRejectsForged(t *testing.T) {
	ctx := context.Background()
	svc, _ := newService(t)

	if _, err := svc.Register(ctx, "dave@example.com", goodPassword, "Dave"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	user, pair, err := svc.Login(ctx, "dave@example.com", goodPassword, "test")
	if err != nil {
		t.Fatalf("Login: %v", err)
	}

	claims, err := svc.ValidateToken(ctx, pair.AccessToken)
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if claims.Subject != user.ID {
		t.Errorf("subject = %q, want %q", claims.Subject, user.ID)
	}

	// A token signed with another key must not be accepted.
	forged, err := jwtx.Issue(
		jwtx.Config{Secret: "a-different-secret-at-least-32-bytes-long!", Issuer: "marketplace"},
		jwtx.Claims{Subject: "attacker"},
	)
	if err != nil {
		t.Fatalf("Issue forged: %v", err)
	}
	if _, err := svc.ValidateToken(ctx, forged); err == nil {
		t.Error("a token signed with another key was accepted")
	}
}

func TestLogout_RevokesTheWholeFamily(t *testing.T) {
	ctx := context.Background()
	svc, _ := newService(t)

	if _, err := svc.Register(ctx, "erin@example.com", goodPassword, "Erin"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	_, pair, err := svc.Login(ctx, "erin@example.com", goodPassword, "test")
	if err != nil {
		t.Fatalf("Login: %v", err)
	}

	successor, err := svc.Refresh(ctx, pair.RefreshToken)
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	if err := svc.Logout(ctx, successor.RefreshToken); err != nil {
		t.Fatalf("Logout: %v", err)
	}

	if _, err := svc.Refresh(ctx, successor.RefreshToken); err == nil {
		t.Error("a refresh token still worked after logout")
	}
}

func TestGetUser_NotFound(t *testing.T) {
	ctx := context.Background()
	svc, _ := newService(t)

	_, err := svc.GetUser(ctx, "nobody")
	if !apperr.Is(err, apperr.CodeNotFound) {
		t.Errorf("code = %q, want %q", apperr.CodeOf(err), apperr.CodeNotFound)
	}
}

func TestHashPassword_IsSaltedPerCall(t *testing.T) {
	// Two accounts with the same password must not share a hash, or one cracked
	// password reveals every account that uses it.
	a, err := domain.HashPassword(goodPassword)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	b, err := domain.HashPassword(goodPassword)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if a == b {
		t.Error("the same password produced the same hash twice; it is not salted")
	}

	if err := domain.VerifyPassword(a, goodPassword); err != nil {
		t.Errorf("VerifyPassword on its own hash = %v, want nil", err)
	}
	if err := domain.VerifyPassword(a, "something else"); !errors.Is(err, domain.ErrInvalidCredentials) {
		t.Errorf("VerifyPassword on a wrong password = %v, want ErrInvalidCredentials", err)
	}
}
