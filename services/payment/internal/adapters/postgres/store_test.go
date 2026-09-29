package postgres_test

// These tests run against a real PostgreSQL, because the guarantees they check
// are not properties of this Go code: they are properties of the schema. Whether
// two concurrent retries can both claim one idempotency key is decided by a
// unique index, and a fake database would only ever confirm what the fake was
// told to do.
//
// The scratch database (PAYMENT_TEST_DB_DSN, default payment_test) is used
// rather than the service's own: the tests truncate tables and write rows no
// service would ever write. When no database is reachable the suite skips, so
// `go test ./...` still passes on a laptop with nothing running.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/marketplace/pkg/apperr"
	"github.com/marketplace/pkg/money"
	"github.com/marketplace/pkg/postgres"

	pgadapter "github.com/marketplace/services/payment/internal/adapters/postgres"
	"github.com/marketplace/services/payment/internal/app"
	"github.com/marketplace/services/payment/internal/domain"
	"github.com/marketplace/services/payment/migrations"
)

// testDSN points at a scratch database, never at payment_db.
func testDSN() string {
	if dsn := os.Getenv("PAYMENT_TEST_DB_DSN"); dsn != "" {
		return dsn
	}
	// localhost, not the compose service name: the tests run on the host while
	// the PostgreSQL they talk to runs in a container.
	return "postgres://marketplace:marketplace@localhost:5432/payment_test?sslmode=disable"
}

// fixture is a store over an empty payments table.
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

	pool, err := postgres.Open(ctx, postgres.Config{DSN: testDSN(), Service: "payment_test"})
	if err != nil {
		t.Skipf("PostgreSQL is not available, skipping the durability tests: %v", err)
	}
	t.Cleanup(pool.Close)

	// The real migrations, not a test schema. A store tested against a schema
	// that drifted from the deployed one is a store tested against nothing.
	if err := postgres.Migrate(ctx, pool, "payment_test", migrations.FS, log); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Every test starts from an empty table, so a uniqueness assertion is about
	// this test's rows and not about what a previous test left behind.
	if _, err := pool.Exec(ctx, `TRUNCATE payments`); err != nil {
		t.Fatalf("truncate payments: %v", err)
	}

	return &fixture{store: pgadapter.New(pool), pool: pool}
}

func (f *fixture) count(t *testing.T) int {
	t.Helper()

	var n int
	if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM payments`).Scan(&n); err != nil {
		t.Fatalf("count payments: %v", err)
	}
	return n
}

// payment builds a pending payment, which is the state the service writes when
// it claims a key.
func payment(id, orderID, key string, at time.Time) *domain.Payment {
	return &domain.Payment{
		ID:                 id,
		OrderID:            orderID,
		Amount:             money.MustNew(4999, money.USD),
		Status:             domain.StatusPending,
		IdempotencyKey:     key,
		PaymentMethodToken: "pm_token",
		CreatedAt:          at,
		UpdatedAt:          at,
	}
}

var epoch = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// ------------------------------------------------------------ create and read

func TestStore_CreateRoundTripsEveryField(t *testing.T) {
	// Everything the domain models must survive the trip through SQL, including
	// the parts the service only reads back when a customer disputes a charge.
	ctx := context.Background()
	f := newFixture(t)

	p := payment("p-1", "o-1", "k-1", epoch)
	p.Status = domain.StatusDeclined
	p.DeclineReason = domain.ReasonInsufficientFunds
	p.ProviderReference = "psp_declined"
	p.RefundedMinor = 0

	if err := f.store.Create(ctx, p); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := f.store.GetByID(ctx, "p-1")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}

	if got.OrderID != p.OrderID || got.IdempotencyKey != p.IdempotencyKey {
		t.Errorf("identifiers did not round-trip: %+v", got)
	}
	if got.Status != p.Status {
		t.Errorf("status = %s, want %s", got.Status, p.Status)
	}
	if got.DeclineReason != p.DeclineReason {
		t.Errorf("decline reason = %q, want %q", got.DeclineReason, p.DeclineReason)
	}
	if got.ProviderReference != "psp_declined" {
		t.Errorf("provider reference = %q, want it preserved for reconciliation", got.ProviderReference)
	}
	if got.PaymentMethodToken != "pm_token" {
		t.Errorf("payment method token = %q, want it preserved", got.PaymentMethodToken)
	}
	if !got.Amount.Equal(p.Amount) {
		t.Errorf("amount = %s, want %s", got.Amount, p.Amount)
	}
	if !got.CreatedAt.Equal(epoch) {
		t.Errorf("created_at = %s, want %s", got.CreatedAt, epoch)
	}
}

func TestStore_ReadsThatMatchNothing(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)

	if _, err := f.store.GetByID(ctx, "nope"); !errors.Is(err, domain.ErrPaymentNotFound) {
		t.Errorf("GetByID = %v, want ErrPaymentNotFound", err)
	}
	if _, err := f.store.GetByIdempotencyKey(ctx, "nope"); !errors.Is(err, domain.ErrPaymentNotFound) {
		t.Errorf("GetByIdempotencyKey = %v, want ErrPaymentNotFound", err)
	}
	if _, err := f.store.GetByOrderID(ctx, "nope"); !errors.Is(err, domain.ErrPaymentNotFound) {
		t.Errorf("GetByOrderID = %v, want ErrPaymentNotFound", err)
	}
}

// ------------------------------------------------------------ idempotency key

func TestStore_CreateRejectsADuplicateIdempotencyKey(t *testing.T) {
	// The claim comes from the unique index, so the second insert has to fail
	// even though nothing in this process saw the first one.
	ctx := context.Background()
	f := newFixture(t)

	if err := f.store.Create(ctx, payment("p-1", "o-1", "shared", epoch)); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	err := f.store.Create(ctx, payment("p-2", "o-2", "shared", epoch))
	if !errors.Is(err, domain.ErrDuplicateIdempotencyKey) {
		t.Fatalf("second Create = %v, want ErrDuplicateIdempotencyKey", err)
	}
	// The rejection must reach the service as a replay, not as an ambiguous
	// failure, or the caller cannot tell "already done" from "try again".
	if errors.Is(err, domain.ErrInvalidPayment) {
		t.Error("a taken idempotency key was reported as an invalid payment")
	}

	if got := f.count(t); got != 1 {
		t.Errorf("stored payments = %d, want 1", got)
	}

	// The key still resolves to the payment that won it, which is what the
	// retry's replay reads.
	existing, err := f.store.GetByIdempotencyKey(ctx, "shared")
	if err != nil {
		t.Fatalf("GetByIdempotencyKey: %v", err)
	}
	if existing.ID != "p-1" {
		t.Errorf("key resolves to %s, want the original p-1", existing.ID)
	}
}

func TestStore_CreateRejectsADuplicateIDDistinctly(t *testing.T) {
	// A taken id and a taken key are different bugs and the service reacts
	// differently to them, so the store must not collapse them into one error.
	ctx := context.Background()
	f := newFixture(t)

	if err := f.store.Create(ctx, payment("p-1", "o-1", "k-1", epoch)); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	err := f.store.Create(ctx, payment("p-1", "o-2", "k-2", epoch))
	if !errors.Is(err, domain.ErrInvalidPayment) {
		t.Fatalf("Create with a duplicate id = %v, want ErrInvalidPayment", err)
	}
	if errors.Is(err, domain.ErrDuplicateIdempotencyKey) {
		t.Error("a duplicate id was reported as a duplicate idempotency key")
	}
}

func TestStore_ConcurrentCreatesClaimTheKeyOnce(t *testing.T) {
	// This is the test the whole design exists for. A client that times out
	// retries, and the retries arrive at the same instant; if two of them get
	// past Create, two of them reach the provider, and the customer is charged
	// twice.
	//
	// A check-then-write passes every sequential test ever written and fails
	// this one, which is why the claim is the insert itself.
	ctx := context.Background()
	f := newFixture(t)

	const attempts = 24

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		claimed   int
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

			p := payment(fmt.Sprintf("p-%d", i), "o-1", "the-key", epoch)
			err := f.store.Create(ctx, p)

			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				claimed++
			case errors.Is(err, domain.ErrDuplicateIdempotencyKey):
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
	if claimed != 1 {
		t.Errorf("creates that claimed the key = %d, want exactly 1", claimed)
	}
	if duplicate != attempts-1 {
		t.Errorf("duplicate rejections = %d, want %d", duplicate, attempts-1)
	}

	// The number that decides whether anyone was charged twice.
	if got := f.count(t); got != 1 {
		t.Errorf("stored payments = %d, want exactly 1", got)
	}
}

func TestStore_DeleteReleasesTheKey(t *testing.T) {
	// A payment whose provider call never produced a decision must not keep the
	// key claimed, or a transient outage becomes a permanently stuck checkout:
	// every retry with that key would be answered "already in progress".
	ctx := context.Background()
	f := newFixture(t)

	if err := f.store.Create(ctx, payment("p-1", "o-1", "k-1", epoch)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := f.store.Delete(ctx, "p-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, err := f.store.GetByID(ctx, "p-1"); !errors.Is(err, domain.ErrPaymentNotFound) {
		t.Errorf("GetByID after Delete = %v, want ErrPaymentNotFound", err)
	}

	// The same key now works, which is the retry path after an outage.
	retry := payment("p-2", "o-1", "k-1", epoch.Add(time.Second))
	if err := f.store.Create(ctx, retry); err != nil {
		t.Fatalf("Create after Delete = %v, want the key to be free again", err)
	}

	// Deleting something that is not there is a no-op, because the only caller
	// is a compensation that cannot know whether the forward step applied.
	if err := f.store.Delete(ctx, "never-existed"); err != nil {
		t.Errorf("Delete of an unknown payment = %v, want nil", err)
	}
}

// -------------------------------------------------------------------- update

func TestStore_UpdatePersistsTheDecision(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)

	if err := f.store.Create(ctx, payment("p-1", "o-1", "k-1", epoch)); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := f.store.GetByID(ctx, "p-1")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}

	at := epoch.Add(time.Second)
	if err := got.MarkAuthorized("psp_ok", at); err != nil {
		t.Fatalf("MarkAuthorized: %v", err)
	}
	if err := f.store.Update(ctx, got); err != nil {
		t.Fatalf("Update: %v", err)
	}

	again, err := f.store.GetByID(ctx, "p-1")
	if err != nil {
		t.Fatalf("second GetByID: %v", err)
	}
	if again.Status != domain.StatusAuthorized {
		t.Errorf("status = %s, want authorized", again.Status)
	}
	if again.ProviderReference != "psp_ok" {
		t.Errorf("provider reference = %q, want psp_ok", again.ProviderReference)
	}
	if !again.UpdatedAt.Equal(at) {
		t.Errorf("updated_at = %s, want %s", again.UpdatedAt, at)
	}
	if !again.CreatedAt.Equal(epoch) {
		t.Errorf("created_at = %s, want it unchanged at %s", again.CreatedAt, epoch)
	}
}

func TestStore_UpdateRefusesToMoveTheIdempotencyKey(t *testing.T) {
	// Moving the key would silently release one claim and take another, which is
	// exactly the guarantee the key exists to provide.
	ctx := context.Background()
	f := newFixture(t)

	if err := f.store.Create(ctx, payment("p-1", "o-1", "k-1", epoch)); err != nil {
		t.Fatalf("Create: %v", err)
	}

	moved := payment("p-1", "o-1", "k-2", epoch)
	err := f.store.Update(ctx, moved)
	if !errors.Is(err, domain.ErrInvalidPayment) {
		t.Fatalf("Update with a changed key = %v, want ErrInvalidPayment", err)
	}
	if errors.Is(err, domain.ErrPaymentNotFound) {
		t.Error("a refused key change was reported as a missing payment")
	}

	// The stored key is untouched, so the original claim still holds.
	stored, err := f.store.GetByIdempotencyKey(ctx, "k-1")
	if err != nil {
		t.Fatalf("GetByIdempotencyKey: %v", err)
	}
	if stored.ID != "p-1" {
		t.Errorf("the original key now resolves to %s", stored.ID)
	}
}

func TestStore_UpdateOfAMissingPaymentIsNotFound(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)

	err := f.store.Update(ctx, payment("ghost", "o-1", "k-1", epoch))
	if !errors.Is(err, domain.ErrPaymentNotFound) {
		t.Errorf("Update of a missing payment = %v, want ErrPaymentNotFound", err)
	}
}

// ------------------------------------------------------------- order lookups

func TestStore_GetByOrderIDReturnsTheNewestRecord(t *testing.T) {
	// One payment per order is an expectation of the service rather than a
	// constraint, so the read has to pick, and it picks the way the in-memory
	// store does: the newest record wins.
	ctx := context.Background()
	f := newFixture(t)

	// Inserted out of order on purpose: the answer must come from the ORDER BY
	// and not from insertion order.
	older := payment("p-old", "o-1", "k-old", epoch)
	newer := payment("p-new", "o-1", "k-new", epoch.Add(time.Minute))
	for _, p := range []*domain.Payment{newer, older} {
		if err := f.store.Create(ctx, p); err != nil {
			t.Fatalf("Create %s: %v", p.ID, err)
		}
	}
	// Another order's payment must not be able to answer for this one.
	if err := f.store.Create(ctx, payment("p-other", "o-2", "k-other", epoch.Add(time.Hour))); err != nil {
		t.Fatalf("Create other order: %v", err)
	}

	got, err := f.store.GetByOrderID(ctx, "o-1")
	if err != nil {
		t.Fatalf("GetByOrderID: %v", err)
	}
	if got.ID != "p-new" {
		t.Errorf("GetByOrderID returned %s, want the newest record p-new", got.ID)
	}
}

func TestStore_GetByOrderIDBreaksTiesOnTheID(t *testing.T) {
	// A clock with a coarser resolution than the writer, or two writes in the
	// same instant, would otherwise leave the answer to physical row order. The
	// id is a UUIDv7, so it is itself time-ordered and makes the newest-wins
	// rule total rather than merely likely.
	ctx := context.Background()
	f := newFixture(t)

	for _, id := range []string{"p-a", "p-b"} {
		if err := f.store.Create(ctx, payment(id, "o-1", "k-"+id, epoch)); err != nil {
			t.Fatalf("Create %s: %v", id, err)
		}
	}

	got, err := f.store.GetByOrderID(ctx, "o-1")
	if err != nil {
		t.Fatalf("GetByOrderID: %v", err)
	}
	if got.ID != "p-b" {
		t.Errorf("GetByOrderID returned %s, want p-b: identical timestamps must still order deterministically", got.ID)
	}
}

// ------------------------------------------------------------- read validation

// insertRaw writes a row the store would never write, so that the read path can
// be shown what it does with one. Every row used here passes the CHECK
// constraints on purpose: the net being tested is the domain's, not the
// database's.
func (f *fixture) insertRaw(t *testing.T, row string) {
	t.Helper()

	if _, err := f.pool.Exec(context.Background(), `
		INSERT INTO payments (
			id, order_id, amount_minor, currency, status, idempotency_key,
			payment_method_token, decline_reason, provider_reference,
			refunded_minor, created_at, updated_at
		) VALUES `+row); err != nil {
		t.Fatalf("insert corrupt row: %v", err)
	}

	t.Cleanup(func() {
		if _, err := f.pool.Exec(context.Background(), `DELETE FROM payments`); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	})
}

func TestStore_ReadsRevalidateTheStoredRow(t *testing.T) {
	// The database constraints are a coarse net; the domain's rules are finer.
	// A row that violates them — written by a migration, a manual edit or a bug
	// in an older writer — must fail loudly on the way out, because a corrupt
	// payment that reached the saga would look like a business decision rather
	// than a data problem.
	ctx := context.Background()
	f := newFixture(t)

	// Declined, with no reason recorded. No CHECK constraint states this rule,
	// because it is the domain's: a decline that cannot be explained cannot be
	// classified as retryable or permanent, which is the whole answer the saga
	// needs. It is also exactly what a buggy older writer would leave behind.
	f.insertRaw(t, `('p-bad', 'o-bad', 1000, 'USD', 'declined', 'k-bad', '', '', '', 0, now(), now())`)

	_, err := f.store.GetByID(ctx, "p-bad")
	if err == nil {
		t.Fatal("GetByID accepted a corrupt row; it must fail loudly at read time")
	}
	if !errors.Is(err, domain.ErrInvalidPayment) {
		t.Errorf("GetByID error = %v, want it to wrap ErrInvalidPayment", err)
	}

	// The order lookup reads the same rows, so it must not be the way around
	// the check.
	if _, err := f.store.GetByOrderID(ctx, "o-bad"); err == nil {
		t.Error("GetByOrderID accepted a corrupt row")
	}
}

func TestStore_ReadsReportAnUnreadableAmount(t *testing.T) {
	// A currency the money package cannot construct fails while scanning, before
	// the domain ever sees the row. It must still be a loud failure and, in
	// particular, not "payment not found": the row exists, and telling the saga
	// it does not would send it down the path that authorizes a second payment.
	ctx := context.Background()
	f := newFixture(t)

	f.insertRaw(t, `('p-bad', 'o-bad', 1000, 'us', 'pending', 'k-bad', '', '', '', 0, now(), now())`)

	_, err := f.store.GetByID(ctx, "p-bad")
	if err == nil {
		t.Fatal("GetByID accepted a row whose amount cannot be read")
	}
	if errors.Is(err, domain.ErrPaymentNotFound) {
		t.Errorf("error = %v, want it not to be a not-found: the row is there", err)
	}
}

// ------------------------------------------------------- through the service

// countingProvider records how many times the service reached the provider.
// That count, not the answer it returned, is what says whether a customer was
// charged twice.
type countingProvider struct {
	mu    sync.Mutex
	calls int
	delay time.Duration
}

func (c *countingProvider) Authorize(ctx context.Context, _ domain.AuthorizeRequest) (domain.ProviderDecision, error) {
	c.mu.Lock()
	c.calls++
	delay := c.delay
	c.mu.Unlock()

	if delay > 0 {
		select {
		case <-ctx.Done():
			return domain.ProviderDecision{}, ctx.Err()
		case <-time.After(delay):
		}
	}

	return domain.ProviderDecision{Approved: true, ProviderReference: "psp_ok"}, nil
}

func (c *countingProvider) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func TestService_ConcurrentRetriesReachTheProviderOnce(t *testing.T) {
	// The store test above proves the key can only be claimed once. This one
	// proves the service turns that claim into the behaviour the checkout saga
	// depends on: one payment, one provider call, and every other retry either
	// told to come back or handed the original decision.
	ctx := context.Background()
	f := newFixture(t)

	provider := &countingProvider{delay: 100 * time.Millisecond}

	service, err := app.NewService(app.ServiceConfig{
		Payments: f.store,
		Provider: provider,
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	const attempts = 12

	req := app.AuthorizeRequest{
		OrderID:            "o-race",
		Amount:             money.MustNew(4999, money.USD),
		IdempotencyKey:     "the-key",
		PaymentMethodToken: "pm_token",
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		created int
		replays int
		other   []error
	)

	start := make(chan struct{})
	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			result, err := service.Authorize(ctx, req)

			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil && !result.IdempotentReplay:
				created++
			case err == nil && result.IdempotentReplay:
				replays++
			case apperr.Is(err, apperr.CodeConflict):
				// "A payment with this key is already in progress" is the
				// correct answer while the first call is still with the
				// provider: its outcome is not known, so it cannot be replayed.
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
		t.Errorf("authorizations that created a payment = %d, want exactly 1", created)
	}
	if created+replays != 1 {
		t.Errorf("calls that got a payment back = %d, want exactly 1", created+replays)
	}

	// The number that matters: one key, one charge.
	if got := provider.callCount(); got != 1 {
		t.Errorf("provider calls = %d, want exactly 1", got)
	}
	if got := f.count(t); got != 1 {
		t.Errorf("stored payments = %d, want 1", got)
	}
}

func TestService_ReplayAfterARestartFindsTheStoredPayment(t *testing.T) {
	// The in-memory store forgets every key when the process dies, which is why
	// the durable one exists. A retry that arrives after a restart must find the
	// original decision rather than reach the provider again, so the replay path
	// is exercised through a second service instance over the same database.
	ctx := context.Background()
	f := newFixture(t)

	first := &countingProvider{}
	svc1, err := app.NewService(app.ServiceConfig{Payments: f.store, Provider: first})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	req := app.AuthorizeRequest{
		OrderID:            "o-1",
		Amount:             money.MustNew(4999, money.USD),
		IdempotencyKey:     "k-restart",
		PaymentMethodToken: "pm_token",
	}

	original, err := svc1.Authorize(ctx, req)
	if err != nil {
		t.Fatalf("first Authorize: %v", err)
	}

	// A new process, the same database.
	second := &countingProvider{}
	svc2, err := app.NewService(app.ServiceConfig{Payments: f.store, Provider: second})
	if err != nil {
		t.Fatalf("NewService (restart): %v", err)
	}

	replayed, err := svc2.Authorize(ctx, req)
	if err != nil {
		t.Fatalf("Authorize after restart: %v", err)
	}

	if !replayed.IdempotentReplay {
		t.Error("IdempotentReplay = false, want the stored payment replayed")
	}
	if replayed.Payment.ID != original.Payment.ID {
		t.Errorf("replay produced %s, want the original %s", replayed.Payment.ID, original.Payment.ID)
	}
	if got := second.callCount(); got != 0 {
		t.Errorf("provider calls after a restart = %d, want 0: the key was already claimed", got)
	}
}

// ------------------------------------------------------------------- schema

func TestStore_TheIdempotencyIndexIsUnique(t *testing.T) {
	// The tests above would fail anyway, but they would fail with a confusing
	// "two rows appeared" message. This one names the missing guarantee, because
	// the index is what the whole service leans on and a later migration that
	// dropped it should say so out loud.
	ctx := context.Background()
	f := newFixture(t)

	var unique bool
	err := f.pool.QueryRow(ctx, `
		SELECT indisunique
		  FROM pg_index i
		  JOIN pg_class c ON c.oid = i.indexrelid
		 WHERE c.relname = 'payments_idempotency_key_key'`).Scan(&unique)
	if err != nil {
		t.Fatalf("the unique index on idempotency_key is missing: %v", err)
	}
	if !unique {
		t.Error("the index on idempotency_key is not unique, so nothing prevents a double charge")
	}
}
