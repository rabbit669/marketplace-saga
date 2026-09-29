package grpc_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/marketplace/pkg/grpcx"
	catalogv1 "github.com/marketplace/pkg/proto/catalog/v1"
	commonv1 "github.com/marketplace/pkg/proto/common/v1"

	cataloggrpc "github.com/marketplace/services/catalog/internal/adapters/grpc"
	"github.com/marketplace/services/catalog/internal/adapters/memory"
	"github.com/marketplace/services/catalog/internal/domain"
)

// The tests below drive a real gRPC client against a real gRPC server over an
// in-process listener. Nothing above the store is faked: the request is
// serialised, the interceptor chain runs, the domain validates, and an error
// travels back as a status. That is the layer where a handler that forgot to map
// its error — turning every "not found" into a 500 — actually shows up, and it is
// invisible to a test that calls the handler method directly.

// stepClock returns a timestamp one second later on every read, so that a test
// can assert on audit timestamps without sleeping and without depending on timer
// resolution.
type stepClock struct{ current time.Time }

func newStepClock() *stepClock {
	return &stepClock{current: time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC)}
}

func (c *stepClock) now() time.Time {
	c.current = c.current.Add(time.Second)
	return c.current
}

type harness struct {
	client catalogv1.CatalogServiceClient
}

// newHarness starts a server on a buffer-backed listener and returns a client
// wired to it.
func newHarness(t *testing.T) *harness {
	t.Helper()
	return newHarnessWithRepository(t, memory.New())
}

// newHarnessWithRepository is newHarness with the store supplied by the test, so
// that a repository that misbehaves can be driven through the real interceptor
// chain and observed as a caller sees it.
func newHarnessWithRepository(t *testing.T, repo domain.Repository) *harness {
	t.Helper()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	clock := newStepClock()

	server, err := cataloggrpc.New(cataloggrpc.Config{
		Repository: repo,
		Logger:     log,
		Clock:      clock.now,
	})
	if err != nil {
		t.Fatalf("cataloggrpc.New: %v", err)
	}

	// The production interceptor chain, so the error mapping under test is the
	// one that runs in the service.
	grpcServer := grpcx.NewServer(grpcx.ServerConfig{Name: "catalog", Logger: log})
	catalogv1.RegisterCatalogServiceServer(grpcServer, server)

	listener := bufconn.Listen(1 << 20)
	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("serve: %v", err)
		}
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		grpcServer.Stop()
	})

	return &harness{client: catalogv1.NewCatalogServiceClient(conn)}
}

// usd builds a wire amount. Every price in these tests is in one currency, so
// the helper exists only to keep the assertions readable.
func usd(minor int64) *commonv1.Money {
	return &commonv1.Money{AmountMinor: minor, Currency: "USD"}
}

// create makes a product through the API, which is how a test stays on the
// public contract instead of reaching into the store.
func (h *harness) create(t *testing.T, sku, name string, minor int64, opts ...func(*catalogv1.CreateProductRequest)) *catalogv1.Product {
	t.Helper()

	req := &catalogv1.CreateProductRequest{
		Sku:         sku,
		Name:        name,
		Description: name + " description",
		Price:       usd(minor),
		CategoryId:  "kitchen",
		Attributes:  map[string]string{"colour": "steel"},
	}
	for _, opt := range opts {
		opt(req)
	}

	resp, err := h.client.CreateProduct(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateProduct(%s): %v", sku, err)
	}
	return resp.GetProduct()
}

// codeOf asserts that a call failed and returns its status code.
func codeOf(t *testing.T, err error) codes.Code {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	return status.Code(err)
}

// ------------------------------------------------------------------- create

func TestCreateProduct_HappyPath(t *testing.T) {
	h := newHarness(t)

	product := h.create(t, "  kettle-01 ", "Stainless Steel Kettle", 3999,
		func(req *catalogv1.CreateProductRequest) {
			req.Attributes = map[string]string{"colour": "steel", "capacity": "1.7l"}
			req.CategoryId = "kitchen"
		})

	if product.GetId() == "" {
		t.Error("id is empty: the server must mint one")
	}
	if product.GetSku() != "KETTLE-01" {
		t.Errorf("sku = %q, want the normalised %q", product.GetSku(), "KETTLE-01")
	}
	if product.GetName() != "Stainless Steel Kettle" {
		t.Errorf("name = %q", product.GetName())
	}
	if product.GetPrice().GetAmountMinor() != 3999 || product.GetPrice().GetCurrency() != "USD" {
		t.Errorf("price = %v, want 3999 USD", product.GetPrice())
	}
	if product.GetAttributes()["capacity"] != "1.7l" {
		t.Errorf("attributes = %v, want the ones that were sent", product.GetAttributes())
	}
	if !product.GetActive() {
		t.Error("a newly created product must be on sale")
	}
	if product.GetAudit().GetCreatedAt() == nil || product.GetAudit().GetUpdatedAt() == nil {
		t.Error("audit timestamps are missing: a caller cannot tell when a price was set")
	}

	// The product must be readable back through the same API.
	got, err := h.client.GetProduct(context.Background(), &catalogv1.GetProductRequest{ProductId: product.GetId()})
	if err != nil {
		t.Fatalf("GetProduct: %v", err)
	}
	if got.GetProduct().GetSku() != "KETTLE-01" {
		t.Errorf("read back sku = %q", got.GetProduct().GetSku())
	}
}

func TestCreateProduct_RejectsInvalidInput(t *testing.T) {
	h := newHarness(t)

	tests := []struct {
		name string
		req  *catalogv1.CreateProductRequest
	}{
		{
			name: "no sku",
			req:  &catalogv1.CreateProductRequest{Name: "Kettle", Price: usd(3999)},
		},
		{
			name: "no name",
			req:  &catalogv1.CreateProductRequest{Sku: "KETTLE-01", Price: usd(3999)},
		},
		{
			name: "no price",
			req:  &catalogv1.CreateProductRequest{Sku: "KETTLE-01", Name: "Kettle"},
		},
		{
			name: "negative price",
			req:  &catalogv1.CreateProductRequest{Sku: "KETTLE-01", Name: "Kettle", Price: usd(-1)},
		},
		{
			name: "currency that is not an ISO-4217 code",
			req: &catalogv1.CreateProductRequest{
				Sku: "KETTLE-01", Name: "Kettle",
				Price: &commonv1.Money{AmountMinor: 3999, Currency: "US"},
			},
		},
		{
			name: "malformed sku",
			req:  &catalogv1.CreateProductRequest{Sku: "KETTLE 01", Name: "Kettle", Price: usd(3999)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := h.client.CreateProduct(context.Background(), tc.req)
			if code := codeOf(t, err); code != codes.InvalidArgument {
				t.Fatalf("code = %s (%v), want InvalidArgument", code, err)
			}
			if resp != nil {
				t.Errorf("a rejected create returned %v", resp)
			}
		})
	}
}

func TestCreateProduct_AcceptsALowercaseCurrency(t *testing.T) {
	h := newHarness(t)

	// ISO-4217 codes are case-insensitive by convention; rejecting "usd" would
	// be pedantry that produces a confusing error for a caller.
	product := h.create(t, "KETTLE-01", "Kettle", 3999, func(req *catalogv1.CreateProductRequest) {
		req.Price = &commonv1.Money{AmountMinor: 3999, Currency: "usd"}
	})
	if product.GetPrice().GetCurrency() != "USD" {
		t.Errorf("currency = %q, want the ISO-4217 form USD", product.GetPrice().GetCurrency())
	}
}

func TestCreateProduct_DuplicateSKUIsAlreadyExists(t *testing.T) {
	h := newHarness(t)
	h.create(t, "KETTLE-01", "Kettle", 3999)

	_, err := h.client.CreateProduct(context.Background(), &catalogv1.CreateProductRequest{
		Sku:   "kettle-01",
		Name:  "Another Kettle",
		Price: usd(4200),
	})
	if code := codeOf(t, err); code != codes.AlreadyExists {
		t.Fatalf("code = %s (%v), want AlreadyExists", code, err)
	}
}

// ---------------------------------------------------------------- retrieval

func TestGetProduct_UnknownProductIsNotFound(t *testing.T) {
	h := newHarness(t)

	resp, err := h.client.GetProduct(context.Background(), &catalogv1.GetProductRequest{ProductId: "does-not-exist"})
	if code := codeOf(t, err); code != codes.NotFound {
		t.Fatalf("code = %s (%v), want NotFound", code, err)
	}
	if resp != nil {
		t.Errorf("a failed get returned %v", resp)
	}
}

func TestGetProductBySKU_IsCaseInsensitive(t *testing.T) {
	h := newHarness(t)
	created := h.create(t, "KETTLE-01", "Kettle", 3999)

	for _, sku := range []string{"KETTLE-01", "kettle-01", " Kettle-01 "} {
		resp, err := h.client.GetProductBySKU(context.Background(), &catalogv1.GetProductBySKURequest{Sku: sku})
		if err != nil {
			t.Fatalf("GetProductBySKU(%q): %v", sku, err)
		}
		if resp.GetProduct().GetId() != created.GetId() {
			t.Errorf("GetProductBySKU(%q) returned %s, want %s", sku, resp.GetProduct().GetId(), created.GetId())
		}
	}

	_, err := h.client.GetProductBySKU(context.Background(), &catalogv1.GetProductBySKURequest{Sku: "MUG-02"})
	if code := codeOf(t, err); code != codes.NotFound {
		t.Errorf("code = %s, want NotFound", code)
	}
}

func TestListProducts_PaginatesThroughTheWholeCatalogue(t *testing.T) {
	h := newHarness(t)

	stored := make(map[string]bool, 5)
	for i := 1; i <= 5; i++ {
		product := h.create(t, fmt.Sprintf("SKU-%02d", i), fmt.Sprintf("Product %d", i), 1000+int64(i))
		stored[product.GetId()] = true
	}

	seen := make(map[string]int, 5)
	token := ""
	for page := 0; ; page++ {
		resp, err := h.client.ListProducts(context.Background(), &catalogv1.ListProductsRequest{
			Page: &commonv1.PageRequest{PageSize: 2, PageToken: token},
		})
		if err != nil {
			t.Fatalf("ListProducts page %d: %v", page, err)
		}
		if len(resp.GetProducts()) > 2 {
			t.Fatalf("page %d returned %d products, want at most 2", page, len(resp.GetProducts()))
		}
		if resp.GetPage().GetTotalSize() != 5 {
			t.Errorf("total_size = %d, want 5", resp.GetPage().GetTotalSize())
		}
		for _, product := range resp.GetProducts() {
			seen[product.GetId()]++
		}

		token = resp.GetPage().GetNextPageToken()
		if token == "" {
			break
		}
		if page > 5 {
			t.Fatal("pagination did not terminate")
		}
	}

	if len(seen) != 5 {
		t.Fatalf("walked %d distinct products, want 5", len(seen))
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("product %s was returned %d times", id, count)
		}
	}
	for id := range stored {
		if seen[id] == 0 {
			t.Errorf("product %s was never returned", id)
		}
	}
}

func TestListProducts_AppliesFilters(t *testing.T) {
	h := newHarness(t)

	h.create(t, "KETTLE-01", "Steel Kettle", 3999)
	h.create(t, "MUG-02", "Stoneware Mug", 2450)
	retired := h.create(t, "NOTE-06", "Hardcover Notebook", 1250, func(req *catalogv1.CreateProductRequest) {
		req.CategoryId = "stationery"
	})

	// Take the third product off sale through the API, so the filter has
	// something real to exclude.
	if _, err := h.client.UpdateProduct(context.Background(), &catalogv1.UpdateProductRequest{
		ProductId:  retired.GetId(),
		UpdateMask: []string{"active"},
		Product:    &catalogv1.Product{Active: false},
	}); err != nil {
		t.Fatalf("UpdateProduct: %v", err)
	}

	resp, err := h.client.ListProducts(context.Background(), &catalogv1.ListProductsRequest{OnlyActive: true})
	if err != nil {
		t.Fatalf("ListProducts: %v", err)
	}
	if len(resp.GetProducts()) != 2 {
		t.Fatalf("got %d active products, want 2", len(resp.GetProducts()))
	}
	for _, product := range resp.GetProducts() {
		if !product.GetActive() {
			t.Errorf("product %s is not active but came back from an only_active listing", product.GetSku())
		}
	}

	resp, err = h.client.ListProducts(context.Background(), &catalogv1.ListProductsRequest{CategoryId: "kitchen", Query: "kettle"})
	if err != nil {
		t.Fatalf("ListProducts: %v", err)
	}
	if len(resp.GetProducts()) != 1 || resp.GetProducts()[0].GetSku() != "KETTLE-01" {
		t.Errorf("category+query listing returned %v, want just KETTLE-01", resp.GetProducts())
	}
}

// ------------------------------------------------------------------- update

func TestUpdateProduct_AppliesOnlyTheMaskedFields(t *testing.T) {
	h := newHarness(t)
	created := h.create(t, "KETTLE-01", "Steel Kettle", 3999)

	// A mask naming one field, with the rest of the message left at its zero
	// value: this is exactly the request that a full-replacement update would
	// quietly turn into a free, unnamed, uncategorised product.
	resp, err := h.client.UpdateProduct(context.Background(), &catalogv1.UpdateProductRequest{
		ProductId:  created.GetId(),
		UpdateMask: []string{"name"},
		Product:    &catalogv1.Product{Name: "Kettle, Second Edition"},
	})
	if err != nil {
		t.Fatalf("UpdateProduct: %v", err)
	}

	updated := resp.GetProduct()
	if updated.GetName() != "Kettle, Second Edition" {
		t.Errorf("name = %q, want the new one", updated.GetName())
	}
	if updated.GetSku() != "KETTLE-01" {
		t.Errorf("sku = %q, want it untouched", updated.GetSku())
	}
	if updated.GetPrice().GetAmountMinor() != 3999 {
		t.Errorf("price = %d, want it untouched at 3999", updated.GetPrice().GetAmountMinor())
	}
	if updated.GetCategoryId() != "kitchen" {
		t.Errorf("category = %q, want it untouched", updated.GetCategoryId())
	}
	if updated.GetAttributes()["colour"] != "steel" {
		t.Errorf("attributes = %v, want them untouched", updated.GetAttributes())
	}
	if !updated.GetActive() {
		t.Error("active = false, want it untouched: the mask did not name it")
	}

	// The timestamp must move, otherwise a caller cannot tell whether the price
	// it copied into an order is still current.
	if !updated.GetAudit().GetUpdatedAt().AsTime().After(created.GetAudit().GetUpdatedAt().AsTime()) {
		t.Error("updated_at did not advance")
	}
}

func TestUpdateProduct_EmptyMaskAppliesEverything(t *testing.T) {
	h := newHarness(t)
	created := h.create(t, "KETTLE-01", "Steel Kettle", 3999)

	resp, err := h.client.UpdateProduct(context.Background(), &catalogv1.UpdateProductRequest{
		ProductId: created.GetId(),
		// No mask at all.
		Product: &catalogv1.Product{
			Sku:        "kettle-99",
			Name:       "Kettle, Third Edition",
			Price:      usd(4599),
			CategoryId: "outlet",
			Attributes: map[string]string{"colour": "rust"},
			Active:     false,
		},
	})
	if err != nil {
		t.Fatalf("UpdateProduct: %v", err)
	}

	updated := resp.GetProduct()
	if updated.GetSku() != "KETTLE-99" {
		t.Errorf("sku = %q, want the normalised new SKU", updated.GetSku())
	}
	if updated.GetPrice().GetAmountMinor() != 4599 {
		t.Errorf("price = %d, want 4599", updated.GetPrice().GetAmountMinor())
	}
	if updated.GetCategoryId() != "outlet" || updated.GetActive() {
		t.Errorf("product = %+v, want every field applied", updated)
	}
	if updated.GetAttributes()["colour"] != "rust" {
		t.Errorf("attributes = %v, want them replaced wholesale", updated.GetAttributes())
	}

	// The old SKU must stop resolving, or the catalogue answers with a product
	// that no longer has that SKU.
	_, err = h.client.GetProductBySKU(context.Background(), &catalogv1.GetProductBySKURequest{Sku: "KETTLE-01"})
	if code := codeOf(t, err); code != codes.NotFound {
		t.Error("the old SKU still resolves after a rename")
	}
}

func TestUpdateProduct_MaskCanLeaveTheSaleStateAlone(t *testing.T) {
	h := newHarness(t)
	created := h.create(t, "KETTLE-01", "Steel Kettle", 3999)

	// A price change that does not mention "active" must not take the product
	// off sale, even though the request leaves that field at its zero value.
	resp, err := h.client.UpdateProduct(context.Background(), &catalogv1.UpdateProductRequest{
		ProductId:  created.GetId(),
		UpdateMask: []string{"price"},
		Product:    &catalogv1.Product{Price: usd(3599)},
	})
	if err != nil {
		t.Fatalf("UpdateProduct: %v", err)
	}
	if !resp.GetProduct().GetActive() {
		t.Error("a price-only update took the product off sale")
	}
	if resp.GetProduct().GetPrice().GetAmountMinor() != 3599 {
		t.Errorf("price = %d, want 3599", resp.GetProduct().GetPrice().GetAmountMinor())
	}
}

func TestUpdateProduct_RejectsAnUnknownMaskField(t *testing.T) {
	h := newHarness(t)
	created := h.create(t, "KETTLE-01", "Steel Kettle", 3999)

	// A typo in a mask would otherwise be an update that reports success and
	// changes nothing, which is the hardest kind of bug to notice from outside.
	_, err := h.client.UpdateProduct(context.Background(), &catalogv1.UpdateProductRequest{
		ProductId:  created.GetId(),
		UpdateMask: []string{"nmae"},
		Product:    &catalogv1.Product{Name: "Kettle"},
	})
	if code := codeOf(t, err); code != codes.InvalidArgument {
		t.Fatalf("code = %s (%v), want InvalidArgument", code, err)
	}
}

func TestUpdateProduct_RejectsAnImmutableId(t *testing.T) {
	h := newHarness(t)
	created := h.create(t, "KETTLE-01", "Steel Kettle", 3999)

	_, err := h.client.UpdateProduct(context.Background(), &catalogv1.UpdateProductRequest{
		ProductId:  created.GetId(),
		UpdateMask: []string{"id"},
		Product:    &catalogv1.Product{Id: "somewhere-else"},
	})
	if code := codeOf(t, err); code != codes.InvalidArgument {
		t.Fatalf("code = %s (%v), want InvalidArgument: an id is immutable", code, err)
	}
}

func TestUpdateProduct_RejectsAnEmptyName(t *testing.T) {
	h := newHarness(t)
	created := h.create(t, "KETTLE-01", "Steel Kettle", 3999)

	_, err := h.client.UpdateProduct(context.Background(), &catalogv1.UpdateProductRequest{
		ProductId:  created.GetId(),
		UpdateMask: []string{"name"},
		Product:    &catalogv1.Product{Name: "   "},
	})
	if code := codeOf(t, err); code != codes.InvalidArgument {
		t.Fatalf("code = %s (%v), want InvalidArgument", code, err)
	}

	// The rejected update must not have changed anything.
	got, err := h.client.GetProduct(context.Background(), &catalogv1.GetProductRequest{ProductId: created.GetId()})
	if err != nil {
		t.Fatalf("GetProduct: %v", err)
	}
	if got.GetProduct().GetName() != "Steel Kettle" {
		t.Errorf("name = %q, want the stored value", got.GetProduct().GetName())
	}
}

func TestUpdateProduct_UnknownProductIsNotFound(t *testing.T) {
	h := newHarness(t)

	_, err := h.client.UpdateProduct(context.Background(), &catalogv1.UpdateProductRequest{
		ProductId:  "does-not-exist",
		UpdateMask: []string{"name"},
		Product:    &catalogv1.Product{Name: "Kettle"},
	})
	if code := codeOf(t, err); code != codes.NotFound {
		t.Fatalf("code = %s (%v), want NotFound", code, err)
	}
}

// -------------------------------------------------------------------- batch

func TestGetProductsBatch_PricesACartAndOmitsMisses(t *testing.T) {
	h := newHarness(t)
	first := h.create(t, "KETTLE-01", "Kettle", 3999)
	second := h.create(t, "MUG-02", "Mug", 2450)

	resp, err := h.client.GetProductsBatch(context.Background(), &catalogv1.GetProductsBatchRequest{
		ProductIds: []string{first.GetId(), second.GetId(), "does-not-exist", first.GetId()},
	})
	if err != nil {
		t.Fatalf("GetProductsBatch: %v", err)
	}

	if len(resp.GetProducts()) != 2 {
		t.Fatalf("got %d products, want 2: a miss must be absent, not zero-filled", len(resp.GetProducts()))
	}
	if _, ok := resp.GetProducts()["does-not-exist"]; ok {
		t.Error("a product that does not exist came back anyway")
	}
	if got := resp.GetProducts()[first.GetId()]; got.GetPrice().GetAmountMinor() != 3999 {
		t.Errorf("KETTLE-01 price = %d, want 3999", got.GetPrice().GetAmountMinor())
	}
	if got := resp.GetProducts()[second.GetId()]; got.GetSku() != "MUG-02" {
		t.Errorf("MUG-02 = %+v", got)
	}
}

func TestGetProductsBatch_RejectsOversizedBatches(t *testing.T) {
	h := newHarness(t)

	ids := make([]string, cataloggrpc.MaxBatchSize+1)
	for i := range ids {
		ids[i] = fmt.Sprintf("product-%d", i)
	}

	resp, err := h.client.GetProductsBatch(context.Background(), &catalogv1.GetProductsBatchRequest{ProductIds: ids})
	if code := codeOf(t, err); code != codes.InvalidArgument {
		t.Fatalf("code = %s (%v), want InvalidArgument", code, err)
	}
	if resp != nil {
		t.Errorf("an oversized batch returned %v", resp)
	}

	// Exactly at the limit is accepted: the boundary must not be off by one.
	_, err = h.client.GetProductsBatch(context.Background(),
		&catalogv1.GetProductsBatchRequest{ProductIds: ids[:cataloggrpc.MaxBatchSize]})
	if err != nil {
		t.Fatalf("a batch of exactly %d ids was rejected: %v", cataloggrpc.MaxBatchSize, err)
	}
}

func TestGetProductsBatch_EmptyRequestIsAnEmptyAnswer(t *testing.T) {
	h := newHarness(t)

	resp, err := h.client.GetProductsBatch(context.Background(), &catalogv1.GetProductsBatchRequest{})
	if err != nil {
		t.Fatalf("GetProductsBatch: %v", err)
	}
	if len(resp.GetProducts()) != 0 {
		t.Errorf("got %d products for an empty request", len(resp.GetProducts()))
	}
}

// ------------------------------------------------------------------ wiring

func TestNew_RequiresARepository(t *testing.T) {
	if _, err := cataloggrpc.New(cataloggrpc.Config{}); err == nil {
		t.Fatal("New accepted a nil repository: the failure would surface as a 500 on the first request instead of at startup")
	}
}

func TestNew_DefaultsItsDependencies(t *testing.T) {
	server, err := cataloggrpc.New(cataloggrpc.Config{Repository: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// With nothing injected the defaults must still be usable: an empty id would
	// collide on the second create, and a missing timestamp would make an audit
	// trail impossible.
	resp, err := server.CreateProduct(context.Background(), &catalogv1.CreateProductRequest{
		Sku:   "KETTLE-01",
		Name:  "Kettle",
		Price: usd(3999),
	})
	if err != nil {
		t.Fatalf("CreateProduct: %v", err)
	}
	if resp.GetProduct().GetId() == "" {
		t.Error("the default id generator produced an empty id")
	}
	if resp.GetProduct().GetAudit().GetCreatedAt() == nil {
		t.Error("the default clock produced no timestamp")
	}
}

// failingRepository stands in for a store that is down.
type failingRepository struct {
	domain.Repository
}

func (failingRepository) Get(context.Context, string) (*domain.Product, error) {
	return nil, errors.New("connection refused to postgres-primary.internal:5432")
}

func TestUnexpectedStoreErrorsDoNotLeakInternals(t *testing.T) {
	// Driven through the real server and client, so the assertion is about what
	// a caller receives and not about what the handler returned internally.
	h := newHarnessWithRepository(t, failingRepository{})

	_, err := h.client.GetProduct(context.Background(), &catalogv1.GetProductRequest{ProductId: "x"})
	if code := codeOf(t, err); code != codes.Internal {
		t.Fatalf("code = %s, want Internal", code)
	}

	// The message is what the caller sees; it must not name an internal host.
	if msg := status.Convert(err).Message(); strings.Contains(msg, "postgres-primary") {
		t.Errorf("message = %q leaks internal detail", msg)
	}
}
