// Package grpc adapts the catalog domain to the CatalogService contract.
//
// It is the only place in this service where a protobuf message exists. The
// conversion runs in both directions here — proto to domain on the way in,
// domain to proto on the way out — so that the domain never learns the wire
// format, and a change to the .proto cannot reach the pricing rules that the
// whole platform depends on.
//
// Errors leave as domain errors: they are mapped onto the shared apperr taxonomy
// and turned into gRPC statuses by the server interceptor in pkg/grpcx. No
// handler in this file constructs a status.
package grpc

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/marketplace/pkg/apperr"
	"github.com/marketplace/pkg/idgen"
	"github.com/marketplace/pkg/money"
	catalogv1 "github.com/marketplace/pkg/proto/catalog/v1"
	commonv1 "github.com/marketplace/pkg/proto/common/v1"

	"github.com/marketplace/services/catalog/internal/domain"
)

// op namespaces errors raised by this adapter, matching the domain's own op so
// that a caller cannot tell — and does not need to care — which layer rejected
// the request.
const op = "catalog"

// MaxBatchSize is the largest GetProductsBatch the server accepts.
//
// The limit is enforced here rather than left to the caller's discretion because
// the RPC exists to price one cart: without a ceiling, a single request could
// ask for the entire catalogue and hold a connection while it is assembled.
// Exceeding it is a caller bug, so it is an InvalidArgument and not a partial
// answer that silently drops products.
const MaxBatchSize = 100

// updatableFields lists the product fields a caller may change through
// UpdateProduct, in the order they are reported in an error message.
//
// It is the domain's list rather than a copy of it. The storage adapter that can
// apply a mask as a single write validates the very same names before they reach
// a SQL statement, and two lists would eventually disagree — at which point this
// service accepts a field on the way in and silently ignores it on the way to
// storage, which is the failure the closed list exists to prevent.
var updatableFields = domain.UpdatableFields

// Config carries the adapter's dependencies.
type Config struct {
	// Repository is the product store. Required.
	Repository domain.Repository
	// Logger receives handler-level records. Defaults to slog.Default.
	Logger *slog.Logger
	// Clock supplies timestamps. Zero means time.Now. It is injectable so that
	// a test can assert on audit timestamps without sleeping.
	Clock func() time.Time
	// NewID mints product ids. Zero means idgen.New, which returns a UUID v7 —
	// time-ordered, which is what the store's pagination relies on.
	NewID func() string
}

// Server implements catalogv1.CatalogServiceServer.
type Server struct {
	catalogv1.UnimplementedCatalogServiceServer

	repo  domain.Repository
	log   *slog.Logger
	clock func() time.Time
	newID func() string
}

// New builds the adapter, failing fast on a missing dependency.
//
// A nil repository would not surface until the first request, at which point the
// gateway would report an internal error for every call while the process looks
// healthy. Refusing to start is a much shorter path to the actual problem.
func New(cfg Config) (*Server, error) {
	if cfg.Repository == nil {
		return nil, errors.New("catalog grpc server: repository is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.NewID == nil {
		cfg.NewID = idgen.New
	}

	return &Server{
		repo:  cfg.Repository,
		log:   cfg.Logger,
		clock: cfg.Clock,
		newID: cfg.NewID,
	}, nil
}

var _ catalogv1.CatalogServiceServer = (*Server)(nil)

// CreateProduct adds a product to the catalogue.
func (s *Server) CreateProduct(ctx context.Context, req *catalogv1.CreateProductRequest) (*catalogv1.CreateProductResponse, error) {
	price, err := priceFromProto(req.GetPrice())
	if err != nil {
		return nil, domain.AsAppError(err)
	}

	// The id is minted here, in the adapter, and not in the domain: it comes
	// from a UUID v7 generator, which is infrastructure the domain must not
	// reach for.
	product, err := domain.NewProduct(domain.NewProductInput{
		ID:          s.newID(),
		SKU:         req.GetSku(),
		Name:        req.GetName(),
		Description: req.GetDescription(),
		Price:       price,
		CategoryID:  req.GetCategoryId(),
		Attributes:  cloneAttributes(req.GetAttributes()),
		CreatedAt:   s.clock(),
	})
	if err != nil {
		return nil, domain.AsAppError(err)
	}

	if err := s.repo.Create(ctx, product); err != nil {
		return nil, domain.AsAppError(err)
	}

	s.log.InfoContext(ctx, "product created",
		slog.String("product_id", product.ID),
		slog.String("sku", product.SKU),
	)
	return &catalogv1.CreateProductResponse{Product: productToProto(*product)}, nil
}

// GetProduct returns one product by id.
func (s *Server) GetProduct(ctx context.Context, req *catalogv1.GetProductRequest) (*catalogv1.GetProductResponse, error) {
	product, err := s.repo.Get(ctx, req.GetProductId())
	if err != nil {
		return nil, domain.AsAppError(err)
	}
	return &catalogv1.GetProductResponse{Product: productToProto(*product)}, nil
}

// GetProductBySKU returns one product by SKU.
func (s *Server) GetProductBySKU(ctx context.Context, req *catalogv1.GetProductBySKURequest) (*catalogv1.GetProductBySKUResponse, error) {
	// The repository normalises the SKU, so "widget-01" reaches the product
	// stored as "WIDGET-01" without this adapter having to guess the rule.
	product, err := s.repo.GetBySKU(ctx, req.GetSku())
	if err != nil {
		return nil, domain.AsAppError(err)
	}
	return &catalogv1.GetProductBySKUResponse{Product: productToProto(*product)}, nil
}

// ListProducts returns one page of the catalogue.
func (s *Server) ListProducts(ctx context.Context, req *catalogv1.ListProductsRequest) (*catalogv1.ListProductsResponse, error) {
	filter := domain.ListFilter{
		CategoryID: req.GetCategoryId(),
		Query:      req.GetQuery(),
		OnlyActive: req.GetOnlyActive(),
		PageSize:   int(req.GetPage().GetPageSize()),
		PageToken:  req.GetPage().GetPageToken(),
	}

	result, err := s.repo.List(ctx, filter)
	if err != nil {
		return nil, domain.AsAppError(err)
	}

	products := make([]*catalogv1.Product, 0, len(result.Products))
	for _, product := range result.Products {
		products = append(products, productToProto(product))
	}

	return &catalogv1.ListProductsResponse{
		Products: products,
		Page: &commonv1.PageResponse{
			NextPageToken: result.NextPageToken,
			TotalSize:     int32(result.TotalSize),
		},
	}, nil
}

// UpdateProduct applies a field mask to an existing product.
//
// The mask is what makes a partial update safe. Without it every update would be
// a full replacement, and a caller that wanted to rename a product would have to
// send its price back too — which means a concurrent re-pricing would be
// silently reverted by a rename. With it, only the named fields are touched, and
// an empty mask still means "apply everything" for callers that do send the
// whole product.
func (s *Server) UpdateProduct(ctx context.Context, req *catalogv1.UpdateProductRequest) (*catalogv1.UpdateProductResponse, error) {
	mask, err := parseMask(req.GetUpdateMask())
	if err != nil {
		return nil, domain.AsAppError(err)
	}

	// Read-modify-write against the stored product rather than against the
	// request alone: a caller that omits a field must keep the stored value, and
	// a mask naming "active" must not reset the price to the zero value that a
	// missing price message would have produced.
	existing, err := s.repo.Get(ctx, req.GetProductId())
	if err != nil {
		return nil, domain.AsAppError(err)
	}

	src := req.GetProduct()
	updated := existing.Clone()

	if mask.has("sku") {
		sku, err := domain.NormalizeSKU(src.GetSku())
		if err != nil {
			return nil, domain.AsAppError(err)
		}
		updated.SKU = sku
	}
	if mask.has("name") {
		updated.Name = src.GetName()
	}
	if mask.has("description") {
		updated.Description = src.GetDescription()
	}
	if mask.has("price") {
		price, err := priceFromProto(src.GetPrice())
		if err != nil {
			return nil, domain.AsAppError(err)
		}
		updated.Price = price
	}
	if mask.has("category_id") {
		updated.CategoryID = src.GetCategoryId()
	}
	if mask.has("attributes") {
		// Replaced wholesale, not merged: a merge would make removing an
		// attribute impossible, and the mask already gives the caller a way to
		// say "leave the attributes alone".
		updated.Attributes = cloneAttributes(src.GetAttributes())
	}
	if mask.has("active") {
		updated.IsActive = src.GetActive()
	}

	// Re-validated because a patch is a product no constructor has seen: it can
	// leave the name empty or push the price negative one field at a time, each
	// step individually plausible.
	if err := updated.Validate(); err != nil {
		return nil, domain.AsAppError(err)
	}
	updated.UpdatedAt = s.clock()

	// A repository that can apply the mask as one write is preferred, and is
	// what a durable one offers. The read-modify-write above has a real weakness
	// that no amount of care in this handler removes: it writes every field
	// back, so a rename that runs alongside a re-pricing writes its own stale
	// copy of the price and silently reverts the other change. Both paths end
	// with the stored product, so the answer a caller sees is the same either
	// way — one of them simply cannot undo a concurrent edit to a field it was
	// not asked to touch.
	stored := &updated
	if updater, ok := s.repo.(domain.FieldMaskedUpdater); ok {
		saved, err := updater.UpdateFields(ctx, &updated, req.GetUpdateMask())
		if err != nil {
			return nil, domain.AsAppError(err)
		}
		stored = saved
	} else if err := s.repo.Update(ctx, &updated); err != nil {
		return nil, domain.AsAppError(err)
	}

	s.log.InfoContext(ctx, "product updated",
		slog.String("product_id", stored.ID),
		slog.Any("fields", req.GetUpdateMask()),
	)
	return &catalogv1.UpdateProductResponse{Product: productToProto(*stored)}, nil
}

// GetProductsBatch prices a whole cart in one round trip.
func (s *Server) GetProductsBatch(ctx context.Context, req *catalogv1.GetProductsBatchRequest) (*catalogv1.GetProductsBatchResponse, error) {
	ids := req.GetProductIds()
	if len(ids) > MaxBatchSize {
		return nil, apperr.InvalidArgument(op+".GetProductsBatch",
			"at most %d product ids may be requested at once, got %d", MaxBatchSize, len(ids))
	}

	// A repository that can answer the whole set in one round trip is preferred:
	// pricing a twenty-line cart should not be twenty queries. It applies the
	// same rule as the loop below — a product that does not exist is absent from
	// the answer rather than zero-valued — so the caller cannot tell which path
	// served it.
	if batcher, ok := s.repo.(domain.BatchGetter); ok {
		products, err := batcher.GetBatch(ctx, ids)
		if err != nil {
			return nil, domain.AsAppError(err)
		}
		found := make(map[string]*catalogv1.Product, len(products))
		for _, product := range products {
			found[product.ID] = productToProto(product)
		}
		return &catalogv1.GetProductsBatchResponse{Products: found}, nil
	}

	products := make(map[string]*catalogv1.Product, len(ids))
	for _, id := range ids {
		product, err := s.repo.Get(ctx, id)
		switch {
		case errors.Is(err, domain.ErrProductNotFound):
			// Deliberately omitted rather than zero-filled. The caller compares
			// the size of the answer with the size of the request to detect a
			// miss; a zero-valued product would instead look like an item priced
			// at nothing, and the cart total would quietly come out too low.
			continue
		case err != nil:
			return nil, domain.AsAppError(err)
		}
		// Keyed by the stored id, so a duplicate id in the request collapses to
		// one entry instead of overwriting with an identical one.
		products[product.ID] = productToProto(*product)
	}

	return &catalogv1.GetProductsBatchResponse{Products: products}, nil
}

// fieldMask is the set of fields a caller asked to change.
type fieldMask map[string]bool

// has reports whether a field should be applied.
//
// An empty mask means "apply everything", which is the documented proto
// semantics and the behaviour a caller sending a whole product expects. Folding
// that rule into has() keeps it from being repeated — and eventually forgotten —
// at each of the seven assignments in UpdateProduct.
func (m fieldMask) has(field string) bool {
	return len(m) == 0 || m[field]
}

// parseMask validates a caller-supplied field mask.
func parseMask(fields []string) (fieldMask, error) {
	mask := make(fieldMask, len(fields))
	for _, field := range fields {
		name := strings.TrimSpace(field)
		if name == "" {
			// A blank entry is how an unset optional field arrives from some
			// clients. It names nothing, so skipping it changes no behaviour.
			continue
		}
		if !isUpdatable(name) {
			return nil, domain.Invalid("update_mask",
				"%q is not an updatable field; updatable fields are %s", name, strings.Join(updatableFields, ", "))
		}
		mask[name] = true
	}
	return mask, nil
}

func isUpdatable(name string) bool {
	return domain.IsUpdatableField(name)
}

// ------------------------------------------------------------ conversions

// productToProto converts a domain product to its wire representation.
func productToProto(product domain.Product) *catalogv1.Product {
	return &catalogv1.Product{
		Id:          product.ID,
		Sku:         product.SKU,
		Name:        product.Name,
		Description: product.Description,
		Price:       moneyToProto(product.Price),
		CategoryId:  product.CategoryID,
		Attributes:  cloneAttributes(product.Attributes),
		Active:      product.IsActive,
		Audit:       auditToProto(product.CreatedAt, product.UpdatedAt),
	}
}

// moneyToProto converts an amount. An unset currency cannot occur on a validated
// product, so the conversion is a pure mapping rather than a second validation
// point.
func moneyToProto(amount money.Money) *commonv1.Money {
	return &commonv1.Money{
		AmountMinor: amount.Amount(),
		Currency:    string(amount.Currency()),
	}
}

// priceFromProto converts a wire amount, rejecting one that is absent or that
// names a currency the platform does not settle in.
//
// The currency is upcased first so that a client sending "usd" is accepted: the
// ISO-4217 code is case-insensitive by convention, and rejecting it would be
// pedantry with a confusing error message.
func priceFromProto(amount *commonv1.Money) (money.Money, error) {
	if amount == nil {
		return money.Money{}, domain.Invalid("price", "is required")
	}

	currency := money.Currency(strings.ToUpper(strings.TrimSpace(amount.GetCurrency())))
	price, err := money.New(amount.GetAmountMinor(), currency)
	if err != nil {
		return money.Money{}, domain.Invalid("price", "%s", err)
	}
	return price, nil
}

// auditToProto converts the timestamps. They are only set when they are
// meaningful, so a caller can tell "never updated" from "updated at the epoch".
func auditToProto(createdAt, updatedAt time.Time) *commonv1.AuditInfo {
	audit := &commonv1.AuditInfo{}
	if !createdAt.IsZero() {
		audit.CreatedAt = timestamppb.New(createdAt)
	}
	if !updatedAt.IsZero() {
		audit.UpdatedAt = timestamppb.New(updatedAt)
	}
	return audit
}

// cloneAttributes copies the wire attribute map into a Go one.
//
// proto3 does not distinguish an absent map from an empty one, so nil and empty
// are treated alike: the domain then sees no attributes either way.
func cloneAttributes(attrs map[string]string) map[string]string {
	if len(attrs) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(attrs))
	for k, v := range attrs {
		cloned[k] = v
	}
	return cloned
}
