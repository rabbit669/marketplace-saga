package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/marketplace/pkg/grpcx"
	"github.com/marketplace/pkg/money"
	"github.com/marketplace/pkg/postgres"
	catalogv1 "github.com/marketplace/pkg/proto/catalog/v1"
	commonv1 "github.com/marketplace/pkg/proto/common/v1"

	cataloggrpc "github.com/marketplace/services/catalog/internal/adapters/grpc"
	pgadapter "github.com/marketplace/services/catalog/internal/adapters/postgres"
	"github.com/marketplace/services/catalog/internal/domain"
	"github.com/marketplace/services/catalog/migrations"
)

// usd builds a wire amount. Every price in these tests is in one currency, so
// the helper exists only to keep the assertions readable.
func usd(minor int64) *commonv1.Money {
	return &commonv1.Money{AmountMinor: minor, Currency: "USD"}
}

// These tests drive the adapter against a real PostgreSQL.
//
// A store that talks to a database cannot be verified by a fake one. The
// properties that matter here are the ones only the server can provide: a unique
// index that rejects a duplicate under concurrency, a cursor that pages through
// a live table, a SET clause built from a caller's field mask, and a read path
// that rejects a row a migration corrupted. A stub would happily agree with
// whatever the code believed about all four.
//
// The suite runs against its own database — catalog_test by default — so it can
// truncate freely without touching a service's data. It skips itself when no
// server is reachable, so a machine without one still gets a green build, and it
// is short enough to run on every commit because the alternative is a suite that
// nobody runs.

// defaultTestDSN points at the throwaway database this suite owns. The override
// exists for CI, where the server is not on localhost.
const defaultTestDSN = "postgres://marketplace:marketplace@localhost:5432/catalog_test?sslmode=disable"

func testDSN() string {
	if dsn := os.Getenv("CATALOG_TEST_DB_DSN"); dsn != "" {
		return dsn
	}
	return defaultTestDSN
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// testStore returns a store over an empty catalogue, or skips the test.
//
// Skipping rather than failing is deliberate: a unit test suite that cannot run
// without infrastructure stops being run. Failing is reserved for the case that
// matters — a server that answers but whose schema will not apply.
func testStore(t *testing.T) (*pgadapter.Store, *pgxpool.Pool) {
	t.Helper()

	ctx := context.Background()
	pool, err := postgres.Open(ctx, postgres.Config{
		DSN:            testDSN(),
		Service:        "catalog",
		MaxConns:       16,
		ConnectTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Skipf("no PostgreSQL available for the catalog adapter tests: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := postgres.Migrate(ctx, pool, "catalog", migrations.FS, discardLogger()); err != nil {
		// The server is reachable, so this is a broken migration rather than a
		// missing server: it must not be skipped, because a suite that quietly
		// skips a failing migration is a suite that passes on a broken schema.
		t.Fatalf("apply migrations: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE products`); err != nil {
		t.Fatalf("truncate products: %v", err)
	}

	return pgadapter.New(pool), pool
}

// ---------------------------------------------------------------- fixtures

// fixture builds a valid product. Ids and SKUs are supplied by the caller so
// that a test can control the order products sort in, which is what pagination
// depends on.
func fixture(t *testing.T, id, sku, name string, priceMinor int64, opts ...func(*domain.NewProductInput)) domain.Product {
	t.Helper()

	in := domain.NewProductInput{
		ID:          id,
		SKU:         sku,
		Name:        name,
		Description: name + " description",
		Price:       money.MustNew(priceMinor, money.USD),
		CreatedAt:   time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	for _, opt := range opts {
		opt(&in)
	}

	product, err := domain.NewProduct(in)
	if err != nil {
		t.Fatalf("fixture %s: %v", sku, err)
	}
	return *product
}

func withCategory(category string) func(*domain.NewProductInput) {
	return func(in *domain.NewProductInput) { in.CategoryID = category }
}

func withDescription(description string) func(*domain.NewProductInput) {
	return func(in *domain.NewProductInput) { in.Description = description }
}

func withAttributes(attrs map[string]string) func(*domain.NewProductInput) {
	return func(in *domain.NewProductInput) { in.Attributes = attrs }
}

// productID produces a well-formed, ordered id. Zero padding keeps the lexical
// order equal to the numeric one, which is the property pagination relies on.
func productID(n int) string { return fmt.Sprintf("0192f0a1-0000-7000-8000-%012d", n) }

func mustCreate(t *testing.T, store *pgadapter.Store, product domain.Product) {
	t.Helper()
	if err := store.Create(context.Background(), &product); err != nil {
		t.Fatalf("Create(%s): %v", product.SKU, err)
	}
}

// ------------------------------------------------------------------- create

func TestCreate_ThenGetRoundTrips(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	product := fixture(t, productID(1), "KETTLE-01", "Kettle", 3999,
		withCategory("kitchen"), withAttributes(map[string]string{"colour": "steel"}))
	mustCreate(t, store, product)

	got, err := store.Get(ctx, product.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	// Field by field, because a column written to the wrong place — a currency
	// read back as an amount — is exactly the kind of bug a scan order gets
	// wrong silently.
	if got.ID != product.ID || got.SKU != "KETTLE-01" || got.Name != "Kettle" {
		t.Errorf("got %+v, want the product that was stored", got)
	}
	if got.Description != product.Description {
		t.Errorf("description = %q, want %q", got.Description, product.Description)
	}
	if got.Price.Amount() != 3999 || got.Price.Currency() != money.USD {
		t.Errorf("price = %s, want 3999 USD", got.Price)
	}
	if got.CategoryID != "kitchen" || !got.IsActive {
		t.Errorf("got %+v, want the category and the sale state preserved", got)
	}
	if got.Attributes["colour"] != "steel" {
		t.Errorf("attributes = %v, want the stored map back", got.Attributes)
	}
	if !got.CreatedAt.Equal(product.CreatedAt) || !got.UpdatedAt.Equal(product.UpdatedAt) {
		t.Errorf("timestamps = %v/%v, want %v/%v", got.CreatedAt, got.UpdatedAt, product.CreatedAt, product.UpdatedAt)
	}
}

func TestCreate_NormalisesTheSKUBeforeStoringIt(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	// A product built through NewProduct is already normalised; one assembled by
	// hand is not, and the unique index compares what is stored. Storing the raw
	// value would let "widget-01" and "WIDGET-01" coexist as two products.
	product := fixture(t, productID(1), "KETTLE-01", "Kettle", 3999)
	product.SKU = "  kettle-01  "
	if err := store.Create(ctx, &product); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := store.Get(ctx, productID(1))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.SKU != "KETTLE-01" {
		t.Fatalf("stored SKU = %q, want the normalised form", got.SKU)
	}
}

func TestCreate_RejectsADuplicateSKU(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	mustCreate(t, store, fixture(t, productID(1), "KETTLE-01", "Kettle", 3999))

	// A different id, the same SKU written differently: normalisation is what
	// makes the natural key trustworthy, and the index is what enforces it.
	second := fixture(t, productID(2), "MUG-02", "Mug", 2450)
	second.SKU = " kettle-01 "
	err := store.Create(ctx, &second)
	if !errors.Is(err, domain.ErrDuplicateSKU) {
		t.Fatalf("error = %v, want ErrDuplicateSKU", err)
	}

	got, err := store.GetBySKU(ctx, "KETTLE-01")
	if err != nil {
		t.Fatalf("GetBySKU: %v", err)
	}
	if got.ID != productID(1) {
		t.Fatalf("the SKU now belongs to %s, want the original product", got.ID)
	}
}

func TestCreate_RejectsADuplicateID(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	mustCreate(t, store, fixture(t, productID(1), "KETTLE-01", "Kettle", 3999))

	// A different SKU under the same id. The two uniqueness failures are
	// reported separately on purpose: this one is a caller that reused an
	// identifier it was given, which is a bug on its side rather than a
	// business conflict.
	second := fixture(t, productID(1), "MUG-02", "Mug", 2450)
	if err := store.Create(ctx, &second); !errors.Is(err, domain.ErrDuplicateProduct) {
		t.Fatalf("error = %v, want ErrDuplicateProduct", err)
	}
}

func TestCreate_RejectsAnInvalidProductWithoutTouchingTheTable(t *testing.T) {
	store, pool := testStore(t)
	ctx := context.Background()

	product := fixture(t, productID(1), "KETTLE-01", "Kettle", 3999)
	product.Price = money.Money{} // no currency: not something that can be charged
	if err := store.Create(ctx, &product); err == nil {
		t.Fatal("a product with no currency was accepted")
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM products`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatalf("the table holds %d rows after a rejected create, want 0", count)
	}
}

// TestCreate_ConcurrentDuplicateSKUsIsRejectedByTheIndex is the test that proves
// duplicate detection is the index's job.
//
// Every goroutine inserts a different product id with the same SKU, so nothing
// but the database can stop them from all succeeding. A read-then-insert — the
// obvious implementation — finds nothing in all of them, and the catalogue ends
// up holding as many products as there were goroutines, all claiming the same
// natural key and possibly at different prices.
func TestCreate_ConcurrentDuplicateSKUsIsRejectedByTheIndex(t *testing.T) {
	store, pool := testStore(t)
	ctx := context.Background()

	const goroutines = 16

	// The fixtures are built here rather than inside the goroutines: they fail
	// the test on a validation error, and t.Fatalf may only be called from the
	// goroutine running the test.
	products := make([]domain.Product, 0, goroutines)
	for i := 0; i < goroutines; i++ {
		products = append(products,
			fixture(t, productID(i+1), "KETTLE-01", fmt.Sprintf("Kettle %d", i), 3999))
	}

	var (
		created   atomic.Int64
		duplicate atomic.Int64
		wg        sync.WaitGroup
		start     = make(chan struct{})
	)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start

			product := products[i]
			switch err := store.Create(ctx, &product); {
			case err == nil:
				created.Add(1)
			case errors.Is(err, domain.ErrDuplicateSKU):
				duplicate.Add(1)
			default:
				t.Errorf("create %d: %v", i, err)
			}
		}(i)
	}

	close(start)
	wg.Wait()

	if got := created.Load(); got != 1 {
		t.Errorf("creates that succeeded = %d, want exactly 1", got)
	}
	if got := duplicate.Load(); got != goroutines-1 {
		t.Errorf("duplicate rejections = %d, want %d", got, goroutines-1)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM products WHERE sku = 'KETTLE-01'`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("the table holds %d rows with SKU KETTLE-01, want 1", count)
	}
}

// ------------------------------------------------------------------ lookup

func TestGetBySKU_MatchesRegardlessOfCase(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	mustCreate(t, store, fixture(t, productID(1), "KETTLE-01", "Kettle", 3999))

	for _, sku := range []string{"KETTLE-01", "kettle-01", " Kettle-01 "} {
		got, err := store.GetBySKU(ctx, sku)
		if err != nil {
			t.Fatalf("GetBySKU(%q): %v", sku, err)
		}
		if got.ID != productID(1) {
			t.Errorf("GetBySKU(%q) returned %s, want %s", sku, got.ID, productID(1))
		}
	}
}

func TestGet_UnknownProductIsNotFound(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	if _, err := store.Get(ctx, "nope"); !errors.Is(err, domain.ErrProductNotFound) {
		t.Errorf("Get error = %v, want ErrProductNotFound", err)
	}
	if _, err := store.GetBySKU(ctx, "NOPE-01"); !errors.Is(err, domain.ErrProductNotFound) {
		t.Errorf("GetBySKU error = %v, want ErrProductNotFound", err)
	}
}

func TestGetBySKU_RejectsAMalformedSKU(t *testing.T) {
	store, _ := testStore(t)

	// The malformed SKU is a caller error and must be reported as one. Sending
	// it to the database instead would produce a lookup that simply finds
	// nothing, which the caller cannot tell apart from a product that does not
	// exist.
	_, err := store.GetBySKU(context.Background(), "not a sku")
	var validation *domain.ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("error = %v, want a *domain.ValidationError", err)
	}
}

// --------------------------------------------------------------- read path

// TestReadPath_RejectsACorruptedRow is what makes Rehydrate worth running on the
// way out of storage.
//
// The row below is perfectly readable and violates a rule the domain enforces —
// a product updated before it was created. A migration, a manual edit or a bug
// in an older writer produces exactly this, and the read path is the only place
// every consumer would otherwise silently inherit it.
func TestReadPath_RejectsACorruptedRow(t *testing.T) {
	store, pool := testStore(t)
	ctx := context.Background()

	mustCreate(t, store, fixture(t, productID(1), "KETTLE-01", "Kettle", 3999))

	if _, err := pool.Exec(ctx,
		`UPDATE products SET updated_at = created_at - interval '1 day' WHERE id = $1`, productID(1)); err != nil {
		t.Fatalf("corrupt the row: %v", err)
	}

	_, err := store.Get(ctx, productID(1))
	if !errors.Is(err, domain.ErrInvalidProduct) {
		t.Fatalf("Get error = %v, want ErrInvalidProduct", err)
	}

	_, err = store.List(ctx, domain.ListFilter{PageSize: 10})
	if !errors.Is(err, domain.ErrInvalidProduct) {
		t.Fatalf("List error = %v, want ErrInvalidProduct", err)
	}
}

func TestReadPath_RejectsAStoredPriceWithNoKnownCurrency(t *testing.T) {
	store, pool := testStore(t)
	ctx := context.Background()

	mustCreate(t, store, fixture(t, productID(1), "KETTLE-01", "Kettle", 3999))

	// The column accepts any text; the domain accepts only an ISO-4217 code. The
	// read path is where the difference is caught, rather than letting an amount
	// in a currency this platform cannot settle reach an order.
	if _, err := pool.Exec(ctx,
		`UPDATE products SET currency = 'usd' WHERE id = $1`, productID(1)); err != nil {
		t.Fatalf("corrupt the row: %v", err)
	}

	_, err := store.Get(ctx, productID(1))
	if err == nil {
		t.Fatal("a stored amount in an unknown currency was read successfully")
	}
	// Pinned to the intended failure, so the test cannot pass because of an
	// unrelated one.
	if !strings.Contains(err.Error(), "stored price is invalid") {
		t.Fatalf("error = %v, want the stored price to be reported as invalid", err)
	}
}

// ------------------------------------------------------------------- batch

func TestGetBatch_ReturnsOnlyTheIDsThatExist(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	mustCreate(t, store, fixture(t, productID(1), "KETTLE-01", "Kettle", 3999))
	mustCreate(t, store, fixture(t, productID(2), "MUG-02", "Mug", 2450))

	// A repeated id and a miss, which is what a cart that mentions an item twice
	// and a deleted product looks like. A miss must be absent, never a
	// zero-valued product: the caller compares sizes to detect it, and a product
	// priced at nothing would quietly make the cart total too low.
	products, err := store.GetBatch(ctx, []string{productID(2), "does-not-exist", productID(1), productID(2)})
	if err != nil {
		t.Fatalf("GetBatch: %v", err)
	}
	if len(products) != 2 {
		t.Fatalf("got %d products, want 2", len(products))
	}
	if products[0].ID != productID(1) || products[1].ID != productID(2) {
		t.Fatalf("ids = %s, %s, want them in ascending id order", products[0].ID, products[1].ID)
	}

	empty, err := store.GetBatch(ctx, nil)
	if err != nil {
		t.Fatalf("GetBatch(nil): %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("an empty request returned %d products", len(empty))
	}

	missing, err := store.GetBatch(ctx, []string{"nope", "  "})
	if err != nil {
		t.Fatalf("GetBatch of unknown ids: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("unknown ids returned %d products, want none", len(missing))
	}
}

// ------------------------------------------------------------ pagination

func TestList_PaginationVisitsEveryProductExactlyOnce(t *testing.T) {
	const total = 25
	const pageSize = 7

	store, _ := testStore(t)
	ctx := context.Background()

	for i := 1; i <= total; i++ {
		mustCreate(t, store, fixture(t, productID(i), fmt.Sprintf("SKU-%03d", i), fmt.Sprintf("Product %d", i), 1000+int64(i)))
	}

	seen := make(map[string]int, total)
	token := ""
	pages := 0

	for {
		result, err := store.List(ctx, domain.ListFilter{PageSize: pageSize, PageToken: token})
		if err != nil {
			t.Fatalf("List page %d: %v", pages, err)
		}
		if len(result.Products) > pageSize {
			t.Fatalf("page %d returned %d products, want at most %d", pages, len(result.Products), pageSize)
		}
		if result.TotalSize != total {
			t.Errorf("TotalSize = %d, want %d", result.TotalSize, total)
		}

		for _, product := range result.Products {
			seen[product.ID]++
		}
		pages++

		if result.NextPageToken == "" {
			break
		}
		// The token is the id of the last product returned, which is what makes
		// "resume after this point" stable while the catalogue is edited.
		if last := result.Products[len(result.Products)-1].ID; result.NextPageToken != last {
			t.Fatalf("NextPageToken = %q, want the last returned id %q", result.NextPageToken, last)
		}
		token = result.NextPageToken

		if pages > total {
			t.Fatal("pagination did not terminate: the token never came back empty")
		}
	}

	if len(seen) != total {
		t.Errorf("walked %d distinct products, want %d", len(seen), total)
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("product %s was returned %d times, want exactly once", id, count)
		}
	}
	if pages != 4 {
		t.Errorf("pages = %d, want 4 (25 products at 7 per page)", pages)
	}
}

func TestList_LastPageHasNoToken(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		mustCreate(t, store, fixture(t, productID(i), fmt.Sprintf("SKU-%03d", i), "Product", 1000))
	}

	result, err := store.List(ctx, domain.ListFilter{PageSize: 10})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(result.Products) != 3 {
		t.Fatalf("got %d products, want 3", len(result.Products))
	}
	if result.NextPageToken != "" {
		t.Errorf("NextPageToken = %q, want empty on the last page", result.NextPageToken)
	}
}

func TestList_CursorIsPositionalNotALookup(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	for i := 1; i <= 6; i++ {
		mustCreate(t, store, fixture(t, productID(i), fmt.Sprintf("SKU-%03d", i), "Product", 1000))
	}

	// A cursor naming a product that is not in the catalogue must still mean
	// "resume after this point". Treating the token as a lookup would either
	// restart the walk or return nothing, and a caller holding a token has no way
	// to tell which happened.
	between := productID(2) + "5"
	result, err := store.List(ctx, domain.ListFilter{PageSize: 10, PageToken: between})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(result.Products) != 4 {
		t.Fatalf("got %d products after the cursor, want 4", len(result.Products))
	}
	if result.Products[0].ID != productID(3) {
		t.Errorf("first product after the cursor = %s, want %s", result.Products[0].ID, productID(3))
	}

	// A cursor past the end is an empty page, not an error and not a restart —
	// and the total still describes the whole match set, not the page.
	past := productID(99)
	result, err = store.List(ctx, domain.ListFilter{PageSize: 10, PageToken: past})
	if err != nil {
		t.Fatalf("List past the end: %v", err)
	}
	if len(result.Products) != 0 || result.NextPageToken != "" {
		t.Errorf("cursor past the end returned %d products and token %q, want nothing", len(result.Products), result.NextPageToken)
	}
	if result.TotalSize != 6 {
		t.Errorf("TotalSize = %d, want 6: it counts matches, not the page", result.TotalSize)
	}
}

func TestList_ClampsThePageSize(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	for i := 1; i <= 150; i++ {
		mustCreate(t, store, fixture(t, productID(i), fmt.Sprintf("SKU-%03d", i), "Product", 1000))
	}

	tests := []struct {
		name      string
		requested int
		want      int
	}{
		{name: "unset falls back to the default", requested: 0, want: domain.DefaultPageSize},
		{name: "negative falls back to the default", requested: -5, want: domain.DefaultPageSize},
		{name: "kept when in range", requested: 33, want: 33},
		{name: "clamped to the maximum", requested: 1000, want: domain.MaxPageSize},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := store.List(ctx, domain.ListFilter{PageSize: tc.requested})
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if len(result.Products) != tc.want {
				t.Errorf("got %d products for PageSize %d, want %d", len(result.Products), tc.requested, tc.want)
			}
		})
	}
}

// --------------------------------------------------------------- filtering

func TestList_FiltersMatchTheInMemoryAdapter(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	mustCreate(t, store, fixture(t, productID(1), "KETTLE-01", "Steel Kettle", 3999, withCategory("kitchen")))
	mustCreate(t, store, fixture(t, productID(2), "MUG-02", "Stoneware Mug", 2450, withCategory("kitchen")))
	retired := fixture(t, productID(3), "NOTE-06", "Hardcover Notebook", 1250, withCategory("stationery"))
	retired.IsActive = false
	mustCreate(t, store, retired)
	mustCreate(t, store, fixture(t, productID(4), "LAMP-05", "Adjustable Lamp", 5495, withCategory("lighting")))

	tests := []struct {
		name   string
		filter domain.ListFilter
		want   []string
	}{
		{
			name:   "no filter returns everything",
			filter: domain.ListFilter{PageSize: 10},
			want:   []string{productID(1), productID(2), productID(3), productID(4)},
		},
		{
			name:   "category",
			filter: domain.ListFilter{CategoryID: "kitchen", PageSize: 10},
			want:   []string{productID(1), productID(2)},
		},
		{
			name:   "only active",
			filter: domain.ListFilter{OnlyActive: true, PageSize: 10},
			want:   []string{productID(1), productID(2), productID(4)},
		},
		{
			name:   "query matches the name, case insensitively",
			filter: domain.ListFilter{Query: "KETTLE", PageSize: 10},
			want:   []string{productID(1)},
		},
		{
			name:   "query matches the SKU, which is what support searches by",
			filter: domain.ListFilter{Query: "note-06", PageSize: 10},
			want:   []string{productID(3)},
		},
		{
			name:   "query matches the description",
			filter: domain.ListFilter{Query: "stoneware mug description", PageSize: 10},
			want:   []string{productID(2)},
		},
		{
			name:   "category and query together",
			filter: domain.ListFilter{CategoryID: "kitchen", Query: "mug", PageSize: 10},
			want:   []string{productID(2)},
		},
		{
			name:   "whitespace is not a filter",
			filter: domain.ListFilter{CategoryID: "  ", Query: "   ", PageSize: 10},
			want:   []string{productID(1), productID(2), productID(3), productID(4)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := store.List(ctx, tc.filter)
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			got := make([]string, 0, len(result.Products))
			for _, product := range result.Products {
				got = append(got, product.ID)
			}
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("ids = %v, want %v", got, tc.want)
			}
			if result.TotalSize != len(tc.want) {
				t.Errorf("TotalSize = %d, want %d", result.TotalSize, len(tc.want))
			}
		})
	}
}

// TestList_QueryTreatsWildcardsLiterally guards the choice of strpos() over
// LIKE.
//
// A shopper searching for "50%" or "A_B" means those characters literally. A
// LIKE pattern built by concatenating the query would treat them as wildcards
// and match products the caller never asked for — which is the wrong answer, and
// one that looks like a relevance bug rather than an escaping one.
func TestList_QueryTreatsWildcardsLiterally(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	mustCreate(t, store, fixture(t, productID(1), "CABLE-A_B", "Cable A_B", 1000))
	mustCreate(t, store, fixture(t, productID(2), "CABLE-AXB", "Cable AXB", 1000))
	mustCreate(t, store, fixture(t, productID(3), "SALE-50", "Half price, 50% off", 1000))
	mustCreate(t, store, fixture(t, productID(4), "SALE-99", "Almost free", 1000))

	for _, tc := range []struct {
		query string
		want  []string
	}{
		{query: "A_B", want: []string{productID(1)}},
		{query: "50%", want: []string{productID(3)}},
	} {
		result, err := store.List(ctx, domain.ListFilter{Query: tc.query, PageSize: 10})
		if err != nil {
			t.Fatalf("List(%q): %v", tc.query, err)
		}
		got := make([]string, 0, len(result.Products))
		for _, product := range result.Products {
			got = append(got, product.ID)
		}
		if strings.Join(got, ",") != strings.Join(tc.want, ",") {
			t.Errorf("query %q matched %v, want %v: a wildcard in a search term is a character", tc.query, got, tc.want)
		}
	}
}

// ------------------------------------------------------------------- update

func TestUpdate_ReplacesEveryField(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	mustCreate(t, store, fixture(t, productID(1), "KETTLE-01", "Kettle", 3999, withCategory("kitchen")))

	updated := fixture(t, productID(1), "kettle-99", "Kettle, Third Edition", 4599, withCategory("outlet"))
	updated.IsActive = false
	updated.UpdatedAt = updated.CreatedAt.Add(time.Hour)
	if err := store.Update(ctx, &updated); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, err := store.Get(ctx, productID(1))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.SKU != "KETTLE-99" || got.Name != "Kettle, Third Edition" || got.Price.Amount() != 4599 {
		t.Errorf("got %+v, want every field replaced", got)
	}
	if got.CategoryID != "outlet" || got.IsActive {
		t.Errorf("got %+v, want the category and sale state replaced", got)
	}

	// The old SKU must stop resolving, or the catalogue answers with a product
	// that no longer has that SKU.
	if _, err := store.GetBySKU(ctx, "KETTLE-01"); !errors.Is(err, domain.ErrProductNotFound) {
		t.Fatalf("the old SKU still resolves: %v", err)
	}
}

func TestUpdate_UnknownProductAndDuplicateSKU(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	mustCreate(t, store, fixture(t, productID(1), "KETTLE-01", "Kettle", 3999))
	mustCreate(t, store, fixture(t, productID(2), "MUG-02", "Mug", 2450))

	unknown := fixture(t, productID(9), "LAMP-05", "Lamp", 5495)
	if err := store.Update(ctx, &unknown); !errors.Is(err, domain.ErrProductNotFound) {
		t.Fatalf("error = %v, want ErrProductNotFound", err)
	}

	// Renaming one product onto another's SKU is a business conflict, and the
	// rejected write must leave both rows exactly as they were.
	taken := fixture(t, productID(2), "KETTLE-01", "Mug", 2450)
	if err := store.Update(ctx, &taken); !errors.Is(err, domain.ErrDuplicateSKU) {
		t.Fatalf("error = %v, want ErrDuplicateSKU", err)
	}
	got, err := store.Get(ctx, productID(2))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.SKU != "MUG-02" {
		t.Fatalf("SKU = %q after a rejected update, want it untouched", got.SKU)
	}
}

// ------------------------------------------------------------ field masks

func TestUpdateFields_AppliesOnlyTheMaskedFields(t *testing.T) {
	store, pool := testStore(t)
	ctx := context.Background()

	mustCreate(t, store, fixture(t, productID(1), "KETTLE-01", "Kettle", 3999,
		withCategory("kitchen"), withAttributes(map[string]string{"colour": "steel"})))

	patched := fixture(t, productID(1), "IGNORED-99", "Kettle, Second Edition", 1111, withCategory("outlet"))
	patched.Attributes = map[string]string{"colour": "rust"}
	patched.IsActive = false
	patched.UpdatedAt = patched.CreatedAt.Add(time.Hour)

	stored, err := store.UpdateFields(ctx, &patched, []string{"name"})
	if err != nil {
		t.Fatalf("UpdateFields: %v", err)
	}
	if stored.Name != "Kettle, Second Edition" {
		t.Errorf("name = %q, want the new one", stored.Name)
	}

	// Read back through the adapter as well: the returned product comes from the
	// same statement, and a test that only trusted it would not notice a SET
	// clause that wrote the wrong column.
	got, err := store.Get(ctx, productID(1))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.SKU != "KETTLE-01" || got.Price.Amount() != 3999 || got.CategoryID != "kitchen" {
		t.Errorf("got %+v, want everything the mask did not name untouched", got)
	}
	if !got.IsActive || got.Attributes["colour"] != "steel" {
		t.Errorf("got %+v, want the sale state and attributes untouched", got)
	}
	if !got.UpdatedAt.After(got.CreatedAt) {
		t.Errorf("updated_at = %v, want it advanced: a caller cannot tell whether a copy is current otherwise", got.UpdatedAt)
	}

	// The price and the currency travel together: a mask naming one of them
	// without the other would leave an amount denominated in the wrong currency.
	if _, err := store.UpdateFields(ctx, &patched, []string{"price"}); err != nil {
		t.Fatalf("UpdateFields(price): %v", err)
	}
	var minor int64
	var currency string
	if err := pool.QueryRow(ctx,
		`SELECT price_minor, currency FROM products WHERE id = $1`, productID(1)).Scan(&minor, &currency); err != nil {
		t.Fatalf("read price: %v", err)
	}
	if minor != 1111 || currency != "USD" {
		t.Fatalf("price = %d %s, want 1111 USD", minor, currency)
	}
}

func TestUpdateFields_EmptyMaskAppliesEverything(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	mustCreate(t, store, fixture(t, productID(1), "KETTLE-01", "Kettle", 3999, withCategory("kitchen")))

	patched := fixture(t, productID(1), "kettle-99", "Kettle, Third Edition", 4599, withCategory("outlet"))
	patched.IsActive = false
	patched.Attributes = map[string]string{"colour": "rust"}

	if _, err := store.UpdateFields(ctx, &patched, nil); err != nil {
		t.Fatalf("UpdateFields: %v", err)
	}

	got, err := store.Get(ctx, productID(1))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.SKU != "KETTLE-99" || got.Name != "Kettle, Third Edition" || got.Price.Amount() != 4599 ||
		got.CategoryID != "outlet" || got.IsActive || got.Attributes["colour"] != "rust" {
		t.Fatalf("got %+v, want every field applied", got)
	}
}

func TestUpdateFields_RejectsAnUnknownFieldBeforeItReachesTheStatement(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	mustCreate(t, store, fixture(t, productID(1), "KETTLE-01", "Kettle", 3999))

	patched := fixture(t, productID(1), "KETTLE-01", "Kettle", 3999)
	patched.UpdatedAt = patched.CreatedAt.Add(time.Hour)

	// The mask is caller-supplied text and the column names in the SET clause
	// are interpolated, so the only safe treatment of an unknown name is to
	// refuse it. This one is also a SQL fragment, which is exactly what a store
	// that translated the mask into SQL by string concatenation would execute.
	for _, mask := range [][]string{
		{"nmae"},
		{"id"},
		{"name; DROP TABLE products"},
		{"name", "price_minor"},
	} {
		_, err := store.UpdateFields(ctx, &patched, mask)
		var validation *domain.ValidationError
		if !errors.As(err, &validation) {
			t.Fatalf("mask %v: error = %v, want a *domain.ValidationError", mask, err)
		}
		if validation.Field != "update_mask" {
			t.Errorf("mask %v: field = %q, want update_mask", mask, validation.Field)
		}
	}

	// Nothing was executed, and the stored row is untouched.
	got, err := store.Get(ctx, productID(1))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "Kettle" || !got.UpdatedAt.Equal(got.CreatedAt) {
		t.Fatalf("got %+v, want the row untouched", got)
	}
}

func TestUpdateFields_UnknownProductIsNotFound(t *testing.T) {
	store, _ := testStore(t)

	product := fixture(t, productID(1), "KETTLE-01", "Kettle", 3999)
	if _, err := store.UpdateFields(context.Background(), &product, []string{"name"}); !errors.Is(err, domain.ErrProductNotFound) {
		t.Fatalf("error = %v, want ErrProductNotFound", err)
	}
}

// TestUpdateFields_ConcurrentMaskedUpdatesDoNotLoseEachOther is the reason the
// masked write exists.
//
// One writer renames, another re-prices, and both run against the same rows at
// the same time. Update writes every column, so the loser of the race would
// write back the copy of the other's field it read before the other committed
// and silently revert it — a lost update that no retry notices, because both
// callers were told they succeeded. Because each masked update only names its
// own columns, the two changes are independent and both must survive.
func TestUpdateFields_ConcurrentMaskedUpdatesDoNotLoseEachOther(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	const products = 8
	const rounds = 12

	ids := make([]string, 0, products)
	for i := 1; i <= products; i++ {
		mustCreate(t, store, fixture(t, productID(i), fmt.Sprintf("SKU-%03d", i), "Original name", 1000))
		ids = append(ids, productID(i))
	}

	var wg sync.WaitGroup
	start := make(chan struct{})

	// The renaming writer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for round := 0; round < rounds; round++ {
			for _, id := range ids {
				product, err := store.Get(ctx, id)
				if err != nil {
					t.Errorf("renamer: Get(%s): %v", id, err)
					return
				}
				product.Name = "Renamed"
				if _, err := store.UpdateFields(ctx, product, []string{"name"}); err != nil {
					t.Errorf("renamer: UpdateFields(%s): %v", id, err)
					return
				}
			}
		}
	}()

	// The re-pricing writer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for round := 0; round < rounds; round++ {
			for _, id := range ids {
				product, err := store.Get(ctx, id)
				if err != nil {
					t.Errorf("pricer: Get(%s): %v", id, err)
					return
				}
				product.Price = money.MustNew(9999, money.USD)
				if _, err := store.UpdateFields(ctx, product, []string{"price"}); err != nil {
					t.Errorf("pricer: UpdateFields(%s): %v", id, err)
					return
				}
			}
		}
	}()

	close(start)
	wg.Wait()

	for _, id := range ids {
		got, err := store.Get(ctx, id)
		if err != nil {
			t.Fatalf("Get(%s): %v", id, err)
		}
		if got.Name != "Renamed" {
			t.Errorf("%s: name = %q, want the rename to have survived", id, got.Name)
		}
		if got.Price.Amount() != 9999 {
			t.Errorf("%s: price = %d, want the re-pricing to have survived", id, got.Price.Amount())
		}
	}
}

// -------------------------------------------------------------------- seed

func TestSeed_GoesThroughTheSameRulesAsACreate(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	first := fixture(t, productID(1), "KETTLE-01", "Kettle", 3999)
	second := fixture(t, productID(2), "KETTLE-01", "Kettle again", 3999)

	// A fixture that collides must fail loudly at startup rather than produce a
	// catalogue whose contents contradict its index.
	if err := store.Seed(ctx, first, second); !errors.Is(err, domain.ErrDuplicateSKU) {
		t.Fatalf("error = %v, want ErrDuplicateSKU", err)
	}
}

// ------------------------------------------------------- transport

// TestTheTransportAdapterUpgradesToTheDurableStore runs the real gRPC adapter
// over the real store.
//
// The adapter serves two optional capabilities when the repository offers them —
// a batch read for pricing a cart, and a masked update that writes one column —
// and falls back to a loop and a read-modify-write when it does not. Both paths
// answer the same requests, so nothing else in this suite would notice if the
// upgrade silently stopped happening: the loss would be an N+1 query pattern and,
// for the masked update, the lost update the mask exists to prevent. This is the
// test that pins it.
func TestTheTransportAdapterUpgradesToTheDurableStore(t *testing.T) {
	store, pool := testStore(t)
	ctx := context.Background()

	// The capabilities the adapter looks for are interfaces, and an interface is
	// satisfied by method signature: a change to either would be a compile error
	// here instead of a silent downgrade at runtime.
	if _, ok := any(store).(domain.FieldMaskedUpdater); !ok {
		t.Fatal("the store no longer offers UpdateFields, so the adapter would fall back to a replacing update")
	}
	if _, ok := any(store).(domain.BatchGetter); !ok {
		t.Fatal("the store no longer offers GetBatch, so pricing a cart would be one query per line")
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, err := cataloggrpc.New(cataloggrpc.Config{Repository: store, Logger: log})
	if err != nil {
		t.Fatalf("cataloggrpc.New: %v", err)
	}

	grpcServer := grpcx.NewServer(grpcx.ServerConfig{Name: "catalog", Logger: log})
	catalogv1.RegisterCatalogServiceServer(grpcServer, server)

	listener := bufconn.Listen(1 << 20)
	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("serve: %v", err)
		}
	}()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := catalogv1.NewCatalogServiceClient(conn)

	first, err := client.CreateProduct(ctx, &catalogv1.CreateProductRequest{
		Sku: "KETTLE-01", Name: "Kettle", Price: usd(3999), CategoryId: "kitchen",
	})
	if err != nil {
		t.Fatalf("CreateProduct: %v", err)
	}
	second, err := client.CreateProduct(ctx, &catalogv1.CreateProductRequest{
		Sku: "MUG-02", Name: "Mug", Price: usd(2450), CategoryId: "kitchen",
	})
	if err != nil {
		t.Fatalf("CreateProduct: %v", err)
	}

	// A cart that names an item twice and a product that no longer exists: the
	// answer holds what exists, and nothing for the miss.
	batch, err := client.GetProductsBatch(ctx, &catalogv1.GetProductsBatchRequest{
		ProductIds: []string{first.GetProduct().GetId(), "does-not-exist", second.GetProduct().GetId()},
	})
	if err != nil {
		t.Fatalf("GetProductsBatch: %v", err)
	}
	if len(batch.GetProducts()) != 2 {
		t.Fatalf("batch returned %d products, want 2", len(batch.GetProducts()))
	}

	// A mask naming one field, with the rest of the message at its zero value.
	updated, err := client.UpdateProduct(ctx, &catalogv1.UpdateProductRequest{
		ProductId:  first.GetProduct().GetId(),
		UpdateMask: []string{"name"},
		Product:    &catalogv1.Product{Name: "Kettle, Second Edition"},
	})
	if err != nil {
		t.Fatalf("UpdateProduct: %v", err)
	}
	if updated.GetProduct().GetName() != "Kettle, Second Edition" {
		t.Errorf("name = %q, want the new one", updated.GetProduct().GetName())
	}
	if updated.GetProduct().GetPrice().GetAmountMinor() != 3999 || updated.GetProduct().GetSku() != "KETTLE-01" {
		t.Errorf("product = %+v, want everything the mask did not name untouched", updated.GetProduct())
	}

	// And the write really did land in the database, not only in the answer.
	got, err := store.Get(ctx, first.GetProduct().GetId())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "Kettle, Second Edition" || got.Price.Amount() != 3999 {
		t.Fatalf("stored product = %+v, want the masked update applied", got)
	}

	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM products`).Scan(&rows); err != nil {
		t.Fatalf("count: %v", err)
	}
	if rows != 2 {
		t.Fatalf("the catalogue holds %d products, want 2", rows)
	}
}
