package memory_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/marketplace/pkg/money"

	"github.com/marketplace/services/catalog/internal/adapters/memory"
	"github.com/marketplace/services/catalog/internal/domain"
)

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

func mustCreate(t *testing.T, store *memory.Store, product domain.Product) {
	t.Helper()
	if err := store.Create(context.Background(), &product); err != nil {
		t.Fatalf("Create(%s): %v", product.SKU, err)
	}
}

// ------------------------------------------------------------------- create

func TestCreate_ThenGetRoundTrips(t *testing.T) {
	store := memory.New()
	product := fixture(t, productID(1), "KETTLE-01", "Kettle", 3999,
		withCategory("kitchen"), withAttributes(map[string]string{"colour": "steel"}))

	if err := store.Create(context.Background(), &product); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := store.Get(context.Background(), product.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.SKU != "KETTLE-01" || got.Name != "Kettle" || got.CategoryID != "kitchen" {
		t.Errorf("got %+v, want the product that was stored", got)
	}
	if !got.Price.Equal(money.MustNew(3999, money.USD)) {
		t.Errorf("price = %s, want 39.99 USD", got.Price)
	}
	if got.Attributes["colour"] != "steel" {
		t.Errorf("attributes = %v, want the stored attributes", got.Attributes)
	}

	if store.Len() != 1 {
		t.Errorf("Len() = %d, want 1", store.Len())
	}
}

func TestCreate_RejectsDuplicateSKUWhateverItsCaseOrSpacing(t *testing.T) {
	// This is the whole reason SKUs are normalised before they are indexed: to
	// every human involved these three strings are one product, and a uniqueness
	// rule that catches only the first is not a uniqueness rule.
	variants := []string{"KETTLE-01", "kettle-01", "  Kettle-01  "}

	store := memory.New()
	mustCreate(t, store, fixture(t, productID(1), "KETTLE-01", "Kettle", 3999))

	for i, sku := range variants {
		duplicate := fixture(t, productID(i+2), "PLACEHOLDER", "Another Kettle", 4200)
		duplicate.SKU = sku

		err := store.Create(context.Background(), &duplicate)
		if !errors.Is(err, domain.ErrDuplicateSKU) {
			t.Errorf("Create(%q) error = %v, want ErrDuplicateSKU", sku, err)
		}
	}

	if store.Len() != 1 {
		t.Errorf("Len() = %d, want 1: a rejected create must not store anything", store.Len())
	}
}

func TestCreate_RejectsDuplicateID(t *testing.T) {
	store := memory.New()
	mustCreate(t, store, fixture(t, productID(1), "KETTLE-01", "Kettle", 3999))

	clash := fixture(t, productID(1), "MUG-02", "Mug", 2450)
	if err := store.Create(context.Background(), &clash); !errors.Is(err, domain.ErrDuplicateProduct) {
		t.Fatalf("error = %v, want ErrDuplicateProduct", err)
	}
}

func TestCreate_RejectsAProductThatWasNotBuiltByAConstructor(t *testing.T) {
	store := memory.New()

	// A value assembled by hand bypasses every rule in the domain. The store
	// re-checks the conditions its own index depends on rather than trusting a
	// promise made in a comment.
	handmade := domain.Product{
		ID:        productID(1),
		SKU:       "KETTLE-01",
		Name:      "Kettle",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := store.Create(context.Background(), &handmade); err == nil {
		t.Fatal("Create accepted a product with no currency")
	}

	unnormalised := fixture(t, productID(2), "KETTLE-01", "Kettle", 3999)
	mustCreate(t, store, unnormalised)

	// A second product whose SKU only collides after normalisation must still be
	// caught, which means the check is done on the normalised form.
	lowercase := fixture(t, productID(3), "KETTLE-01", "Kettle Again", 3999)
	lowercase.SKU = "kettle-01"
	if err := store.Create(context.Background(), &lowercase); !errors.Is(err, domain.ErrDuplicateSKU) {
		t.Fatalf("error = %v, want ErrDuplicateSKU", err)
	}
}

func TestCreate_StoresTheNormalisedSKU(t *testing.T) {
	store := memory.New()
	product := fixture(t, productID(1), "KETTLE-01", "Kettle", 3999)
	product.SKU = " kettle-01 "

	mustCreate(t, store, product)

	got, err := store.Get(context.Background(), product.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.SKU != "KETTLE-01" {
		t.Errorf("stored sku = %q, want %q", got.SKU, "KETTLE-01")
	}
}

// -------------------------------------------------------------- isolation

func TestReadsAndWritesAreDeepCopied(t *testing.T) {
	store := memory.New()
	product := fixture(t, productID(1), "KETTLE-01", "Kettle", 3999,
		withAttributes(map[string]string{"colour": "steel"}))

	mustCreate(t, store, product)

	// Mutating the value the caller still holds must not reach the store.
	product.Attributes["colour"] = "rust"
	product.Name = "Changed After Create"

	got, err := store.Get(context.Background(), product.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "Kettle" || got.Attributes["colour"] != "steel" {
		t.Fatalf("stored product was mutated through the caller's copy: %+v", got)
	}

	// And mutating what was read must not reach the store either.
	got.Name = "Changed After Get"
	got.Attributes["colour"] = "rust"

	again, err := store.Get(context.Background(), product.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if again.Name != "Kettle" || again.Attributes["colour"] != "steel" {
		t.Fatalf("stored product was mutated through the returned copy: %+v", again)
	}
}

func TestList_ReturnsCopies(t *testing.T) {
	store := memory.New()
	mustCreate(t, store, fixture(t, productID(1), "KETTLE-01", "Kettle", 3999,
		withAttributes(map[string]string{"colour": "steel"})))

	result, err := store.List(context.Background(), domain.ListFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	result.Products[0].Attributes["colour"] = "rust"

	again, err := store.List(context.Background(), domain.ListFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if again.Products[0].Attributes["colour"] != "steel" {
		t.Error("List exposes the stored attribute map to its caller")
	}
}

// ---------------------------------------------------------------- lookup

func TestGetBySKU_MatchesRegardlessOfCase(t *testing.T) {
	store := memory.New()
	mustCreate(t, store, fixture(t, productID(1), "KETTLE-01", "Kettle", 3999))

	for _, sku := range []string{"KETTLE-01", "kettle-01", " Kettle-01 "} {
		got, err := store.GetBySKU(context.Background(), sku)
		if err != nil {
			t.Fatalf("GetBySKU(%q): %v", sku, err)
		}
		if got.ID != productID(1) {
			t.Errorf("GetBySKU(%q) returned %s, want %s", sku, got.ID, productID(1))
		}
	}
}

func TestGet_UnknownProductIsNotFound(t *testing.T) {
	store := memory.New()

	if _, err := store.Get(context.Background(), "nope"); !errors.Is(err, domain.ErrProductNotFound) {
		t.Errorf("Get error = %v, want ErrProductNotFound", err)
	}
	if _, err := store.GetBySKU(context.Background(), "nope"); !errors.Is(err, domain.ErrProductNotFound) {
		t.Errorf("GetBySKU error = %v, want ErrProductNotFound", err)
	}
}

func TestGetBySKU_RejectsAMalformedSKU(t *testing.T) {
	store := memory.New()

	_, err := store.GetBySKU(context.Background(), "not a sku")
	var validation *domain.ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("error = %v, want a *domain.ValidationError", err)
	}
}

// ------------------------------------------------------------ pagination

func TestList_PaginationVisitsEveryProductExactlyOnce(t *testing.T) {
	const total = 25
	const pageSize = 7

	store := memory.New()
	want := make(map[string]bool, total)
	for i := 1; i <= total; i++ {
		product := fixture(t, productID(i), fmt.Sprintf("SKU-%03d", i), fmt.Sprintf("Product %d", i), 1000+int64(i))
		mustCreate(t, store, product)
		want[product.ID] = true
	}

	seen := make(map[string]int, total)
	token := ""
	pages := 0

	for {
		result, err := store.List(context.Background(), domain.ListFilter{PageSize: pageSize, PageToken: token})
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
		if result.NextPageToken != result.Products[len(result.Products)-1].ID {
			t.Fatalf("NextPageToken = %q, want the last returned id %q",
				result.NextPageToken, result.Products[len(result.Products)-1].ID)
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
	for id := range want {
		if seen[id] == 0 {
			t.Errorf("product %s was never returned", id)
		}
	}
	if pages != 4 {
		t.Errorf("pages = %d, want 4 (25 products at 7 per page)", pages)
	}
}

func TestList_PageSizeIsClamped(t *testing.T) {
	store := memory.New()
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
			result, err := store.List(context.Background(), domain.ListFilter{PageSize: tc.requested})
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if len(result.Products) != tc.want {
				t.Errorf("got %d products for PageSize %d, want %d", len(result.Products), tc.requested, tc.want)
			}
		})
	}
}

func TestList_LastPageHasNoToken(t *testing.T) {
	store := memory.New()
	for i := 1; i <= 3; i++ {
		mustCreate(t, store, fixture(t, productID(i), fmt.Sprintf("SKU-%03d", i), "Product", 1000))
	}

	result, err := store.List(context.Background(), domain.ListFilter{PageSize: 10})
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
	store := memory.New()
	for i := 1; i <= 6; i++ {
		mustCreate(t, store, fixture(t, productID(i), fmt.Sprintf("SKU-%03d", i), "Product", 1000))
	}

	// A cursor naming a product that is not in the store must still mean "resume
	// after this point". Treating the token as a lookup would either restart the
	// walk or return nothing, and a caller holding a token has no way to tell
	// which happened.
	between := productID(2) + "5"

	result, err := store.List(context.Background(), domain.ListFilter{PageSize: 10, PageToken: between})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(result.Products) != 4 {
		t.Fatalf("got %d products after the cursor, want 4", len(result.Products))
	}
	if result.Products[0].ID != productID(3) {
		t.Errorf("first product after the cursor = %s, want %s", result.Products[0].ID, productID(3))
	}

	// A cursor past the end is an empty page, not an error and not a restart.
	past := productID(99)
	result, err = store.List(context.Background(), domain.ListFilter{PageSize: 10, PageToken: past})
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

// --------------------------------------------------------------- filtering

func TestList_Filters(t *testing.T) {
	store := memory.New()
	mustCreate(t, store, fixture(t, productID(1), "KETTLE-01", "Steel Kettle", 3999, withCategory("kitchen")))
	mustCreate(t, store, fixture(t, productID(2), "MUG-02", "Stoneware Mug", 2450, withCategory("kitchen")))
	mustCreate(t, store, fixture(t, productID(3), "TEE-03", "Cotton T-Shirt", 1990, withCategory("apparel")))
	mustCreate(t, store, fixture(t, productID(4), "LAMP-05", "Desk Lamp", 5495, withCategory("lighting")))

	// The retired product must be findable by category and name, but not by a
	// filter that asks for what is on sale.
	retired := fixture(t, productID(5), "NOTE-06", "Hardcover Notebook", 1250, withCategory("stationery"))
	retired.IsActive = false
	mustCreate(t, store, retired)

	// One product's description mentions merino, which is what a shopper types.
	mustCreate(t, store, fixture(t, productID(6), "SOCK-04", "Hiking Socks", 1450,
		withCategory("apparel"), withDescription("Cushioned merino wool socks")))

	tests := []struct {
		name   string
		filter domain.ListFilter
		want   []string
	}{
		{
			name:   "no filter returns everything",
			filter: domain.ListFilter{PageSize: 50},
			want:   []string{productID(1), productID(2), productID(3), productID(4), productID(5), productID(6)},
		},
		{
			name:   "only active hides the retired product",
			filter: domain.ListFilter{PageSize: 50, OnlyActive: true},
			want:   []string{productID(1), productID(2), productID(3), productID(4), productID(6)},
		},
		{
			name:   "category",
			filter: domain.ListFilter{PageSize: 50, CategoryID: "kitchen"},
			want:   []string{productID(1), productID(2)},
		},
		{
			name:   "category and only active",
			filter: domain.ListFilter{PageSize: 50, CategoryID: "apparel", OnlyActive: true},
			want:   []string{productID(3), productID(6)},
		},
		{
			name:   "query on the name is case insensitive",
			filter: domain.ListFilter{PageSize: 50, Query: "kettle"},
			want:   []string{productID(1)},
		},
		{
			name:   "query on the description",
			filter: domain.ListFilter{PageSize: 50, Query: "MERINO"},
			want:   []string{productID(6)},
		},
		{
			name:   "query on the sku",
			filter: domain.ListFilter{PageSize: 50, Query: "lamp-05"},
			want:   []string{productID(4)},
		},
		{
			name:   "a whitespace-only category is not a filter",
			filter: domain.ListFilter{PageSize: 50, CategoryID: "   "},
			want:   []string{productID(1), productID(2), productID(3), productID(4), productID(5), productID(6)},
		},
		{
			name:   "nothing matches",
			filter: domain.ListFilter{PageSize: 50, Query: "telescope"},
			want:   nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := store.List(context.Background(), tc.filter)
			if err != nil {
				t.Fatalf("List: %v", err)
			}

			got := make([]string, 0, len(result.Products))
			for _, product := range result.Products {
				got = append(got, product.ID)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v, want %v (in the store's stable order)", got, tc.want)
				}
			}
			if result.TotalSize != len(tc.want) {
				t.Errorf("TotalSize = %d, want %d", result.TotalSize, len(tc.want))
			}
		})
	}
}

// ------------------------------------------------------------------ update

func TestUpdate_ReplacesTheProductAndMovesTheSKUIndex(t *testing.T) {
	store := memory.New()
	product := fixture(t, productID(1), "KETTLE-01", "Kettle", 3999, withCategory("kitchen"))
	mustCreate(t, store, product)

	product.SKU = "KETTLE-02"
	product.Name = "Kettle, Second Edition"
	product.Price = money.MustNew(4299, money.USD)

	if err := store.Update(context.Background(), &product); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, err := store.Get(context.Background(), product.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "Kettle, Second Edition" || !got.Price.Equal(money.MustNew(4299, money.USD)) {
		t.Errorf("stored product = %+v, want the updated values", got)
	}

	// The old SKU must be free again, and the new one must resolve. A stale
	// index entry would make the catalogue answer with a product that no longer
	// has that SKU.
	if _, err := store.GetBySKU(context.Background(), "KETTLE-01"); !errors.Is(err, domain.ErrProductNotFound) {
		t.Errorf("old SKU still resolves: %v", err)
	}
	if _, err := store.GetBySKU(context.Background(), "KETTLE-02"); err != nil {
		t.Errorf("new SKU does not resolve: %v", err)
	}

	// The id, and therefore the pagination cursor, is unchanged.
	if store.Len() != 1 {
		t.Errorf("Len() = %d, want 1", store.Len())
	}
}

func TestUpdate_RejectsUnknownProductAndTakenSKU(t *testing.T) {
	store := memory.New()
	first := fixture(t, productID(1), "KETTLE-01", "Kettle", 3999)
	second := fixture(t, productID(2), "MUG-02", "Mug", 2450)
	mustCreate(t, store, first)
	mustCreate(t, store, second)

	unknown := fixture(t, productID(99), "GHOST-01", "Ghost", 1000)
	if err := store.Update(context.Background(), &unknown); !errors.Is(err, domain.ErrProductNotFound) {
		t.Errorf("Update(unknown) error = %v, want ErrProductNotFound", err)
	}

	clash := second
	clash.SKU = "kettle-01"
	if err := store.Update(context.Background(), &clash); !errors.Is(err, domain.ErrDuplicateSKU) {
		t.Errorf("Update(duplicate sku) error = %v, want ErrDuplicateSKU", err)
	}

	// A rejected update must leave the store exactly as it was.
	got, err := store.Get(context.Background(), second.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.SKU != "MUG-02" {
		t.Errorf("sku = %q, want the value from before the rejected update", got.SKU)
	}
	if _, err := store.GetBySKU(context.Background(), "MUG-02"); err != nil {
		t.Errorf("the rejected update corrupted the SKU index: %v", err)
	}
}

func TestUpdate_KeepingTheSameSKUIsAllowed(t *testing.T) {
	store := memory.New()
	product := fixture(t, productID(1), "KETTLE-01", "Kettle", 3999)
	mustCreate(t, store, product)

	product.Name = "Kettle, Renamed"
	if err := store.Update(context.Background(), &product); err != nil {
		t.Fatalf("Update with an unchanged SKU: %v", err)
	}
	if _, err := store.GetBySKU(context.Background(), "KETTLE-01"); err != nil {
		t.Errorf("SKU stopped resolving after an update that kept it: %v", err)
	}
}

// -------------------------------------------------------------------- seed

func TestSeed_GoesThroughTheSameUniquenessRules(t *testing.T) {
	store := memory.New()

	err := store.Seed(
		fixture(t, productID(1), "KETTLE-01", "Kettle", 3999),
		fixture(t, productID(2), "kettle-01", "Another Kettle", 4200),
	)
	if !errors.Is(err, domain.ErrDuplicateSKU) {
		t.Fatalf("Seed error = %v, want ErrDuplicateSKU naming the colliding SKU", err)
	}
	if store.Len() != 1 {
		t.Errorf("Len() = %d, want 1: the first fixture is stored, the colliding one is not", store.Len())
	}
}

// ------------------------------------------------------------- concurrency

func TestConcurrentCreateReadAndList(t *testing.T) {
	// This test exists to be run with -race. A read path that forgot the lock,
	// or a write path that handed the stored map to a caller, is invisible in a
	// single-goroutine test and immediate in production.
	const (
		writers        = 8
		perWriter      = 20
		readers        = 4
		readerAttempts = 50
	)

	store := memory.New()
	ctx := context.Background()

	// Fixtures are built before the goroutines start: a testing.T must not be
	// used for assertions from a goroutine other than the one running the test.
	fixtures := make([]domain.Product, 0, writers*perWriter)
	for n := 0; n < writers*perWriter; n++ {
		fixtures = append(fixtures, fixture(t, productID(n+1), fmt.Sprintf("SKU-%04d", n), fmt.Sprintf("Product %d", n), 1000,
			withAttributes(map[string]string{"colour": "steel"})))
	}

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		errs   []error
		record = func(err error) {
			if err == nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			errs = append(errs, err)
		}
	)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				product := fixtures[w*perWriter+i]
				record(store.Create(ctx, &product))
			}
		}(w)
	}

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < readerAttempts; i++ {
				result, err := store.List(ctx, domain.ListFilter{PageSize: domain.MaxPageSize})
				record(err)
				for _, product := range result.Products {
					// Mutating what was read is legal for a caller, and must not
					// disturb a concurrent writer under the race detector.
					product.Attributes["touched"] = "yes"
					product.Name = "touched"
				}
				if _, err := store.GetBySKU(ctx, "SKU-0001"); err != nil && !errors.Is(err, domain.ErrProductNotFound) {
					record(err)
				}
			}
		}()
	}

	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("%d operations failed, first: %v", len(errs), errs[0])
	}
	if store.Len() != writers*perWriter {
		t.Errorf("Len() = %d, want %d", store.Len(), writers*perWriter)
	}
}

func TestOperationsHonourContextCancellation(t *testing.T) {
	store := memory.New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	product := fixture(t, productID(1), "KETTLE-01", "Kettle", 3999)

	if err := store.Create(ctx, &product); !errors.Is(err, context.Canceled) {
		t.Errorf("Create error = %v, want context.Canceled", err)
	}
	if _, err := store.Get(ctx, product.ID); !errors.Is(err, context.Canceled) {
		t.Errorf("Get error = %v, want context.Canceled", err)
	}
	if _, err := store.List(ctx, domain.ListFilter{}); !errors.Is(err, context.Canceled) {
		t.Errorf("List error = %v, want context.Canceled", err)
	}
}
