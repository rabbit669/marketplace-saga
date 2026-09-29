package postgres_test

// These tests run against a real PostgreSQL, because the guarantees they check
// are properties of the schema rather than of this Go code: whether two
// concurrent registrations can both take the same address is decided by a unique
// index, and whether a family revocation can leave a token behind is decided by
// how many statements it is.
//
// The scratch database (AUTH_TEST_DB_DSN, default auth_test) is used rather than
// the service's own: the tests truncate tables and write rows no service would
// ever write. When no database is reachable the suite skips, so `go test ./...`
// still passes on a laptop with nothing running.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/marketplace/pkg/apperr"
	"github.com/marketplace/pkg/idgen"
	"github.com/marketplace/pkg/jwtx"
	"github.com/marketplace/pkg/postgres"

	pgadapter "github.com/marketplace/services/auth/internal/adapters/postgres"
	"github.com/marketplace/services/auth/internal/app"
	"github.com/marketplace/services/auth/internal/domain"
	"github.com/marketplace/services/auth/migrations"
)

const testSecret = "test-secret-must-be-at-least-32-bytes-long"

// testDSN points at a scratch database, never at auth_db.
func testDSN() string {
	if dsn := os.Getenv("AUTH_TEST_DB_DSN"); dsn != "" {
		return dsn
	}
	// localhost, not the compose service name: the tests run on the host while
	// the PostgreSQL they talk to runs in a container.
	return "postgres://marketplace:marketplace@localhost:5432/auth_test?sslmode=disable"
}

// fixture is a store over empty tables.
type fixture struct {
	store *pgadapter.Store
	pool  *pgxpool.Pool
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	ctx := context.Background()

	// A silent logger: the migration runner logs every applied migration, which
	// is useful in a deploy and noise in a test.
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	pool, err := postgres.Open(ctx, postgres.Config{DSN: testDSN(), Service: "auth_test"})
	if err != nil {
		t.Skipf("PostgreSQL is not available, skipping the durability tests: %v", err)
	}
	t.Cleanup(pool.Close)

	// The real migrations, not a test schema. A store tested against a schema
	// that drifted from the deployed one is a store tested against nothing.
	if err := postgres.Migrate(ctx, pool, "auth_test", migrations.FS, log); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Every test starts from empty tables, so a uniqueness assertion is about
	// this test's rows and not about what a previous test left behind.
	if _, err := pool.Exec(ctx, `TRUNCATE users CASCADE`); err != nil {
		t.Fatalf("truncate users: %v", err)
	}

	return &fixture{store: pgadapter.New(pool), pool: pool}
}

// createUser registers an account through the store, which is how every test
// gets the user a refresh token must belong to.
func (f *fixture) createUser(t *testing.T, id, email string) *domain.User {
	t.Helper()

	u := &domain.User{
		ID:           id,
		Email:        email,
		FullName:     "Test User",
		PasswordHash: "$2a$12$not-a-real-bcrypt-hash-for-a-store-test",
		Roles:        []string{"customer"},
		CreatedAt:    epoch,
		UpdatedAt:    epoch,
	}
	if err := f.store.CreateUser(context.Background(), u); err != nil {
		t.Fatalf("CreateUser(%s): %v", email, err)
	}
	return u
}

func (f *fixture) countUsers(t *testing.T) int {
	t.Helper()

	var n int
	if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM users`).Scan(&n); err != nil {
		t.Fatalf("count users: %v", err)
	}
	return n
}

func (f *fixture) countLiveTokens(t *testing.T, familyID string) int {
	t.Helper()

	var n int
	err := f.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM refresh_tokens WHERE family_id = $1 AND revoked_at IS NULL`,
		familyID).Scan(&n)
	if err != nil {
		t.Fatalf("count live tokens: %v", err)
	}
	return n
}

var epoch = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// token builds a refresh token whose Hash is the digest of its secret, the way
// the service does it.
func token(id, userID, familyID, secret string, issuedAt time.Time) *domain.RefreshToken {
	return &domain.RefreshToken{
		ID:        id,
		UserID:    userID,
		Hash:      hashSecret(secret),
		FamilyID:  familyID,
		IssuedAt:  issuedAt,
		ExpiresAt: issuedAt.Add(30 * 24 * time.Hour),
	}
}

// hashSecret mirrors the service's own hashing, so that the tests store and
// verify exactly what the service would.
func hashSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// -------------------------------------------------------------------- users

func TestStore_CreateUserStoresTheNormalisedAddress(t *testing.T) {
	// The unique index is on the stored column, so it only guarantees "one
	// account per person" if every writer stores the same form of the address.
	// The store therefore normalises rather than trusting its caller to.
	ctx := context.Background()
	f := newFixture(t)

	f.createUser(t, "u-1", "  Alice@Example.COM  ")

	got, err := f.store.GetUser(ctx, "u-1")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if got.Email != "alice@example.com" {
		t.Errorf("stored email = %q, want it normalised to alice@example.com", got.Email)
	}

	// And a login in any case finds that one account.
	for _, query := range []string{"alice@example.com", "ALICE@EXAMPLE.COM", " Alice@Example.com "} {
		found, err := f.store.GetUserByEmail(ctx, query)
		if err != nil {
			t.Fatalf("GetUserByEmail(%q): %v", query, err)
		}
		if found.ID != "u-1" {
			t.Errorf("GetUserByEmail(%q) returned %s, want u-1", query, found.ID)
		}
	}
}

func TestStore_CreateUserRejectsADuplicateEmail(t *testing.T) {
	// A different case is the same address, so it is the same account. Only the
	// unique index can decide that: this call has to fail even though nothing in
	// this process read the table first.
	ctx := context.Background()
	f := newFixture(t)

	f.createUser(t, "u-1", "bob@example.com")

	err := f.store.CreateUser(ctx, &domain.User{
		ID: "u-2", Email: "BOB@Example.COM", FullName: "Bob Again",
		PasswordHash: "another-hash",
		CreatedAt:    epoch, UpdatedAt: epoch,
	})
	if !errors.Is(err, domain.ErrEmailTaken) {
		t.Fatalf("second CreateUser = %v, want ErrEmailTaken", err)
	}

	if got := f.countUsers(t); got != 1 {
		t.Errorf("stored users = %d, want 1", got)
	}
}

func TestStore_CreateUserRejectsADuplicateID(t *testing.T) {
	// A taken id is a bug upstream rather than a duplicate account, but the
	// caller's only sane response is the same one, so the store answers the way
	// the in-memory implementation does.
	ctx := context.Background()
	f := newFixture(t)

	f.createUser(t, "u-1", "first@example.com")

	err := f.store.CreateUser(ctx, &domain.User{
		ID: "u-1", Email: "second@example.com", FullName: "Someone Else",
		PasswordHash: "another-hash",
		CreatedAt:    epoch, UpdatedAt: epoch,
	})
	if !errors.Is(err, domain.ErrEmailTaken) {
		t.Fatalf("CreateUser with a duplicate id = %v, want ErrEmailTaken", err)
	}
}

func TestStore_ConcurrentRegistrationsCreateOneAccount(t *testing.T) {
	// Two requests for the same address racing is not hypothetical: it is a
	// double-clicked signup form, a retried request, or two replicas of this
	// service behind a load balancer. If both get past CreateUser, one person
	// ends up with two accounts and two password hashes, and whichever one they
	// log into decides what the other can see.
	//
	// The addresses are deliberately cased differently, so an implementation
	// that normalised after the insert instead of before it fails here too.
	ctx := context.Background()
	f := newFixture(t)

	const attempts = 24

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		created   int
		duplicate int
		other     []error
	)

	// A barrier, so the goroutines pile up on the insert instead of queueing
	// politely behind each other's round trips.
	start := make(chan struct{})

	for i := range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			email := "Race@Example.com"
			if i%2 == 1 {
				email = "rAcE@eXaMpLe.COM"
			}
			err := f.store.CreateUser(ctx, &domain.User{
				ID: fmt.Sprintf("u-%d", i), Email: email, FullName: "Race",
				PasswordHash: "hash", CreatedAt: epoch, UpdatedAt: epoch,
			})

			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				created++
			case errors.Is(err, domain.ErrEmailTaken):
				duplicate++
			default:
				other = append(other, err)
			}
		}()
	}

	close(start)
	wg.Wait()

	if len(other) > 0 {
		t.Fatalf("unexpected errors: %v", other)
	}
	if created != 1 {
		t.Errorf("registrations that succeeded = %d, want exactly 1", created)
	}
	if duplicate != attempts-1 {
		t.Errorf("duplicate rejections = %d, want %d", duplicate, attempts-1)
	}
	if got := f.countUsers(t); got != 1 {
		t.Errorf("stored users = %d, want exactly 1", got)
	}
}

func TestStore_UserRoundTripsRolesAndFlags(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)

	want := &domain.User{
		ID:            "u-1",
		Email:         "admin@example.com",
		FullName:      "Ada Admin",
		PasswordHash:  "$2a$12$hash",
		Roles:         []string{"customer", "admin"},
		EmailVerified: true,
		CreatedAt:     epoch,
		UpdatedAt:     epoch.Add(time.Minute),
	}
	if err := f.store.CreateUser(ctx, want); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	got, err := f.store.GetUserByEmail(ctx, "admin@example.com")
	if err != nil {
		t.Fatalf("GetUserByEmail: %v", err)
	}

	if !slices.Equal(got.Roles, want.Roles) {
		t.Errorf("roles = %v, want %v", got.Roles, want.Roles)
	}
	if !got.EmailVerified {
		t.Error("email_verified did not round-trip")
	}
	if got.FullName != want.FullName || got.PasswordHash != want.PasswordHash {
		t.Errorf("user did not round-trip: %+v", got)
	}
	if !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Errorf("updated_at = %s, want %s", got.UpdatedAt, want.UpdatedAt)
	}
	if got.Email == "" || got.ID == "" {
		t.Errorf("identifiers were lost: %+v", got)
	}
}

func TestStore_UsersWithoutRolesRoundTripAsAnEmptySet(t *testing.T) {
	// A NULL array would make HasRole and any caller ranging over the slice
	// behave differently from an empty one, for no reason a reader could guess.
	ctx := context.Background()
	f := newFixture(t)

	if err := f.store.CreateUser(ctx, &domain.User{
		ID: "u-1", Email: "noroles@example.com", FullName: "No Roles",
		PasswordHash: "hash", Roles: nil, CreatedAt: epoch, UpdatedAt: epoch,
	}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	got, err := f.store.GetUser(ctx, "u-1")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if got.Roles == nil {
		t.Fatal("roles came back nil; an empty set must not become an absent one")
	}
	if got.HasRole("admin") {
		t.Error("a user with no roles claims to have one")
	}
}

func TestStore_ReadsThatMatchNothing(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)

	if _, err := f.store.GetUser(ctx, "nope"); !errors.Is(err, domain.ErrUserNotFound) {
		t.Errorf("GetUser = %v, want ErrUserNotFound", err)
	}
	if _, err := f.store.GetUserByEmail(ctx, "nobody@example.com"); !errors.Is(err, domain.ErrUserNotFound) {
		t.Errorf("GetUserByEmail = %v, want ErrUserNotFound", err)
	}
	if _, err := f.store.GetRefreshToken(ctx, "nope"); !errors.Is(err, domain.ErrRefreshTokenInvalid) {
		t.Errorf("GetRefreshToken = %v, want ErrRefreshTokenInvalid", err)
	}
}

func TestStore_ReadsRevalidateTheStoredRow(t *testing.T) {
	// The address is what a user authenticates with, so a row whose address is
	// unusable — hand-edited, or written by an older version of this service —
	// describes an account that can never be found. It must be reported as a
	// load failure and, in particular, not as "no such user": answering that
	// would turn a corrupt row into a wrong password, which is a support ticket
	// instead of an alert.
	ctx := context.Background()
	f := newFixture(t)

	tests := []struct {
		name  string
		email string
	}{
		{"address that is not an address", "not-an-email"},
		{"address that is not normalised", "Alice@Example.com"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := f.pool.Exec(ctx, `
				INSERT INTO users (id, email, full_name, password_hash, roles, email_verified, created_at, updated_at)
				VALUES ('u-bad', $1, 'Bad Row', 'hash', '{customer}', false, now(), now())`,
				tc.email); err != nil {
				t.Fatalf("insert corrupt row: %v", err)
			}

			_, err := f.store.GetUser(ctx, "u-bad")
			if err == nil {
				t.Fatal("GetUser accepted a corrupt row; it must fail loudly at read time")
			}
			if errors.Is(err, domain.ErrUserNotFound) {
				t.Errorf("error = %v, want it not to be a not-found: the row is there", err)
			}

			if _, err := f.pool.Exec(ctx, `DELETE FROM users`); err != nil {
				t.Fatalf("cleanup: %v", err)
			}
		})
	}
}

// ----------------------------------------------------------- refresh tokens

func TestStore_RefreshTokensAreStoredHashed(t *testing.T) {
	// The secret half of a refresh token must never reach the database. It is
	// the credential itself: anyone holding a copy of this table — an operator,
	// a backup, a leaked replica — would otherwise be able to mint sessions for
	// every logged-in user.
	ctx := context.Background()
	f := newFixture(t)

	f.createUser(t, "u-1", "carol@example.com")

	secret, err := idgen.RandomToken(32)
	if err != nil {
		t.Fatalf("RandomToken: %v", err)
	}
	want := token("t-1", "u-1", "f-1", secret, epoch)

	if err := f.store.CreateRefreshToken(ctx, want); err != nil {
		t.Fatalf("CreateRefreshToken: %v", err)
	}

	// The row stores the digest and nothing else.
	var storedHash string
	err = f.pool.QueryRow(ctx,
		`SELECT token_hash FROM refresh_tokens WHERE id = $1`, "t-1").Scan(&storedHash)
	if err != nil {
		t.Fatalf("select token hash: %v", err)
	}
	if storedHash != hashSecret(secret) {
		t.Errorf("stored hash = %q, want the SHA-256 of the secret", storedHash)
	}

	// The secret appears in no column, in any form. This is the assertion that
	// fails if someone ever "helpfully" stores the raw token alongside its hash.
	var leaked int
	err = f.pool.QueryRow(ctx, `
		SELECT count(*) FROM refresh_tokens
		 WHERE token_hash = $1 OR id = $1 OR family_id = $1 OR user_id = $1`,
		secret).Scan(&leaked)
	if err != nil {
		t.Fatalf("scan for the raw secret: %v", err)
	}
	if leaked != 0 {
		t.Errorf("the raw refresh secret is stored in %d row(s)", leaked)
	}

	// What comes back is the hash, which the caller compares against the digest
	// of the presented secret.
	got, err := f.store.GetRefreshToken(ctx, "t-1")
	if err != nil {
		t.Fatalf("GetRefreshToken: %v", err)
	}
	if got.Hash != hashSecret(secret) {
		t.Errorf("returned hash = %q, want the stored digest", got.Hash)
	}
	if got.UserID != "u-1" || got.FamilyID != "f-1" {
		t.Errorf("token did not round-trip: %+v", got)
	}
	if got.Terminal() {
		t.Error("a freshly issued token came back revoked")
	}
	if !got.Valid(epoch.Add(time.Hour)) {
		t.Error("a freshly issued token is not usable; its timestamps did not round-trip")
	}
	// And it is unusable against a secret it does not belong to.
	if got.Hash == hashSecret(secret+"x") {
		t.Error("the stored hash matches a different secret")
	}
}

func TestStore_RevokedTokensRoundTrip(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)

	f.createUser(t, "u-1", "dave@example.com")
	if err := f.store.CreateRefreshToken(ctx, token("t-1", "u-1", "f-1", "secret", epoch)); err != nil {
		t.Fatalf("CreateRefreshToken: %v", err)
	}
	if err := f.store.RevokeRefreshToken(ctx, "t-1"); err != nil {
		t.Fatalf("RevokeRefreshToken: %v", err)
	}

	got, err := f.store.GetRefreshToken(ctx, "t-1")
	if err != nil {
		t.Fatalf("GetRefreshToken: %v", err)
	}
	if !got.Terminal() {
		t.Error("revoked_at did not round-trip; a consumed token would look usable")
	}
	if got.Valid(time.Now().Add(time.Hour)) {
		t.Error("a revoked token is still reported as valid")
	}
}

func TestStore_RevokeRefreshTokenIsIdempotent(t *testing.T) {
	// Rotation revokes on every refresh, so this call arrives on retries and on
	// duplicate requests. Turning a harmless second call into an error would log
	// a user out of a session that was working.
	ctx := context.Background()
	f := newFixture(t)

	f.createUser(t, "u-1", "erin@example.com")
	if err := f.store.CreateRefreshToken(ctx, token("t-1", "u-1", "f-1", "secret", epoch)); err != nil {
		t.Fatalf("CreateRefreshToken: %v", err)
	}

	if err := f.store.RevokeRefreshToken(ctx, "t-1"); err != nil {
		t.Fatalf("first RevokeRefreshToken: %v", err)
	}

	var first time.Time
	if err := f.pool.QueryRow(ctx,
		`SELECT revoked_at FROM refresh_tokens WHERE id = $1`, "t-1").Scan(&first); err != nil {
		t.Fatalf("read revoked_at: %v", err)
	}

	time.Sleep(5 * time.Millisecond)

	if err := f.store.RevokeRefreshToken(ctx, "t-1"); err != nil {
		t.Errorf("second RevokeRefreshToken = %v, want nil", err)
	}

	var second time.Time
	if err := f.pool.QueryRow(ctx,
		`SELECT revoked_at FROM refresh_tokens WHERE id = $1`, "t-1").Scan(&second); err != nil {
		t.Fatalf("read revoked_at: %v", err)
	}
	if !second.Equal(first) {
		t.Errorf("the second revocation moved revoked_at from %s to %s; the first detection is the one worth keeping",
			first, second)
	}

	// An unknown token is not an error either: the caller wanted it gone, and it
	// is.
	if err := f.store.RevokeRefreshToken(ctx, "never-existed"); err != nil {
		t.Errorf("RevokeRefreshToken of an unknown token = %v, want nil", err)
	}
}

func TestStore_RevokeFamilyRevokesEveryTokenInTheFamily(t *testing.T) {
	// This is the response to a refresh token being presented twice, which is
	// the only evidence this service ever gets that a chain leaked. Logging the
	// victim out while leaving the thief a working token is worse than not
	// noticing the reuse at all, so the revocation has to be all-or-nothing.
	ctx := context.Background()
	f := newFixture(t)

	f.createUser(t, "u-1", "frank@example.com")

	// Three rotations of one login, in one family.
	for i := range 3 {
		id := fmt.Sprintf("t-%d", i)
		if err := f.store.CreateRefreshToken(ctx, token(id, "u-1", "family-a", "secret-"+id, epoch)); err != nil {
			t.Fatalf("CreateRefreshToken %s: %v", id, err)
		}
	}
	// And a second login, which must be left alone: revoking it too would log
	// out a session that has nothing to do with the leak.
	if err := f.store.CreateRefreshToken(ctx, token("t-other", "u-1", "family-b", "secret-other", epoch)); err != nil {
		t.Fatalf("CreateRefreshToken other family: %v", err)
	}

	if err := f.store.RevokeFamily(ctx, "family-a"); err != nil {
		t.Fatalf("RevokeFamily: %v", err)
	}

	if live := f.countLiveTokens(t, "family-a"); live != 0 {
		t.Errorf("live tokens in the revoked family = %d, want 0: a thief would still hold one", live)
	}
	if live := f.countLiveTokens(t, "family-b"); live != 1 {
		t.Errorf("live tokens in the untouched family = %d, want 1", live)
	}

	// Every token of the family is individually revoked, not just counted.
	for i := range 3 {
		got, err := f.store.GetRefreshToken(ctx, fmt.Sprintf("t-%d", i))
		if err != nil {
			t.Fatalf("GetRefreshToken t-%d: %v", i, err)
		}
		if !got.Terminal() {
			t.Errorf("token t-%d survived the family revocation", i)
		}
	}

	// Repeating it is a no-op, and keeps the timestamp of the first detection.
	var first time.Time
	if err := f.pool.QueryRow(ctx,
		`SELECT revoked_at FROM refresh_tokens WHERE id = 't-0'`).Scan(&first); err != nil {
		t.Fatalf("read revoked_at: %v", err)
	}

	time.Sleep(5 * time.Millisecond)

	if err := f.store.RevokeFamily(ctx, "family-a"); err != nil {
		t.Errorf("second RevokeFamily = %v, want nil", err)
	}

	var second time.Time
	if err := f.pool.QueryRow(ctx,
		`SELECT revoked_at FROM refresh_tokens WHERE id = 't-0'`).Scan(&second); err != nil {
		t.Fatalf("read revoked_at: %v", err)
	}
	if !second.Equal(first) {
		t.Errorf("a later family revocation moved revoked_at from %s to %s", first, second)
	}

	// An empty family name revokes nothing at all, rather than every row whose
	// family id happens to be empty too.
	if err := f.store.RevokeFamily(ctx, ""); err != nil {
		t.Errorf("RevokeFamily with an empty id = %v, want nil", err)
	}
	if live := f.countLiveTokens(t, "family-b"); live != 1 {
		t.Errorf("live tokens in the untouched family = %d, want 1", live)
	}
}

// ------------------------------------------------------- through the service

func newService(t *testing.T, store domain.Repository) *app.Service {
	t.Helper()

	svc, err := app.New(app.Config{
		Users:  store,
		JWT:    jwtx.Config{Secret: testSecret, Issuer: "marketplace", TTL: 15 * time.Minute},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	return svc
}

const goodPassword = "correct-horse-battery"

func TestService_ConcurrentRegistrationsCreateOneAccount(t *testing.T) {
	// The same race as the store test above, but observed through the service:
	// one password hash is written, one account exists, and everyone who lost is
	// told the address is taken rather than being handed an account of their
	// own. The password hashing is deliberately part of this, because it is the
	// expensive step that makes the timing window wide.
	ctx := context.Background()
	f := newFixture(t)
	svc := newService(t, f.store)

	const attempts = 12

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		succeeded int
		other     []error
	)

	start := make(chan struct{})
	for i := range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			// Mixed case on purpose: registration and login must agree on which
			// addresses are the same person.
			email := "Race@Example.com"
			if i%2 == 1 {
				email = "rAcE@eXaMpLe.COM"
			}
			_, err := svc.Register(ctx, email, goodPassword, "Race")

			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				succeeded++
			case apperr.Is(err, apperr.CodeAlreadyExists):
			default:
				other = append(other, err)
			}
		}()
	}

	close(start)
	wg.Wait()

	if len(other) > 0 {
		t.Fatalf("unexpected errors: %v", other)
	}
	if succeeded != 1 {
		t.Errorf("successful registrations = %d, want exactly 1", succeeded)
	}
	if got := f.countUsers(t); got != 1 {
		t.Errorf("stored users = %d, want exactly 1", got)
	}

	// The account that was created is usable, which is what says the loser was
	// rejected rather than the winner being lost.
	if _, _, err := svc.Login(ctx, "race@example.com", goodPassword, "test"); err != nil {
		t.Fatalf("Login after a registration race: %v", err)
	}
}

func TestService_ReuseDetectionRevokesTheFamilyAcrossRestarts(t *testing.T) {
	// The scenario the family exists for, run against the database instead of a
	// map: a token is rotated, the superseded token is presented again, and the
	// successor must stop working — on another instance of the service, because
	// the thief is not necessarily talking to the same one the victim is.
	ctx := context.Background()
	f := newFixture(t)

	svc1 := newService(t, f.store)

	if _, err := svc1.Register(ctx, "grace@example.com", goodPassword, "Grace"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	_, pair, err := svc1.Login(ctx, "grace@example.com", goodPassword, "test")
	if err != nil {
		t.Fatalf("Login: %v", err)
	}

	rotated, err := svc1.Refresh(ctx, pair.RefreshToken)
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	// A second instance over the same database: the state that matters is in
	// PostgreSQL, not in the process.
	svc2 := newService(t, f.store)

	// The thief presents the token the victim already rotated, which is the
	// reuse signal.
	if _, err := svc2.Refresh(ctx, pair.RefreshToken); err == nil {
		t.Fatal("a reused refresh token was accepted")
	}

	// The victim's successor is dead too, which is the price of the safe
	// direction: the victim can log in again, the thief cannot.
	if _, err := svc2.Refresh(ctx, rotated.RefreshToken); err == nil {
		t.Error("after a reuse was detected, the successor token still worked")
	}

	// And that is a fact about the rows, not about which process noticed.
	var live int
	if err := f.pool.QueryRow(ctx, `
		SELECT count(*) FROM refresh_tokens t
		  JOIN users u ON u.id = t.user_id
		 WHERE u.email = 'grace@example.com' AND t.revoked_at IS NULL`).Scan(&live); err != nil {
		t.Fatalf("count live tokens: %v", err)
	}
	if live != 0 {
		t.Errorf("live refresh tokens after a detected reuse = %d, want 0", live)
	}
}

// ------------------------------------------------------------------- schema

func TestStore_TheEmailIndexIsUnique(t *testing.T) {
	// The tests above would fail anyway, but they would fail with a confusing
	// "two rows appeared" message. This one names the missing guarantee, because
	// the index is what registration leans on and a later migration that dropped
	// it should say so out loud.
	ctx := context.Background()
	f := newFixture(t)

	var unique bool
	err := f.pool.QueryRow(ctx, `
		SELECT indisunique
		  FROM pg_index i
		  JOIN pg_class c ON c.oid = i.indexrelid
		 WHERE c.relname = 'users_email_key'`).Scan(&unique)
	if err != nil {
		t.Fatalf("the unique index on users.email is missing: %v", err)
	}
	if !unique {
		t.Error("the index on users.email is not unique, so nothing prevents two accounts for one person")
	}
}

func TestStore_TheTokenHashColumnIsPresentAndNotNull(t *testing.T) {
	// A guard on the shape of the table rather than on a behaviour. The lookup
	// the service performs compares the digest of the presented secret against
	// this column, so a hash that could be absent would be a token that can never
	// be exchanged and never be attributed to a user.
	ctx := context.Background()
	f := newFixture(t)

	var (
		columnType string
		nullable   string
	)
	err := f.pool.QueryRow(ctx, `
		SELECT data_type, is_nullable FROM information_schema.columns
		 WHERE table_name = 'refresh_tokens' AND column_name = 'token_hash'`).
		Scan(&columnType, &nullable)
	if err != nil {
		t.Fatalf("the token_hash column is missing: %v", err)
	}
	if columnType != "text" {
		t.Errorf("token_hash is %s, want text: it holds a hex digest", columnType)
	}
	if nullable != "NO" {
		t.Errorf("token_hash is nullable (%s); a token with no hash can never be validated", nullable)
	}
}
