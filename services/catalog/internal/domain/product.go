// Package domain holds the catalog service's business rules.
//
// As in the order service, this package imports nothing from the outside world:
// no database driver, no gRPC, not even this repository's own infrastructure
// packages. The single exception is the shared `money` value type, which is
// itself dependency-free.
//
// The payoff is concrete rather than stylistic. The catalogue is the source of
// prices, and every price the platform charges is copied out of it, so the
// rules here — what a product is, what makes one valid, what makes one
// unsellable — must hold no matter which adapter is underneath. Keeping them
// free of protobuf also means the wire contract can gain a field without this
// package noticing, and that a product can be validated in a unit test without
// a network.
package domain

import (
	"fmt"
	"strings"
	"time"

	"github.com/marketplace/pkg/money"
)

// SKU is the catalogue's natural key, so its shape is restricted rather than
// free-form: it is what a warehouse operator types into a scanner, what appears
// in a URL when support pulls up a product, and what deduplication compares.
// A SKU containing an exotic space would look identical to a plain one on
// screen while sorting and comparing differently.
const (
	// MaxSKULength bounds the key so it stays usable in the places a SKU is
	// printed and indexed.
	MaxSKULength = 64
	// MaxNameLength bounds the display name. The in-memory store holds whole
	// products, so an unbounded string is an unbounded allocation per row.
	MaxNameLength = 200
	// MaxAttributeCount bounds the free-form attribute map. It is deliberately
	// schema-flexible, which is precisely why it needs a ceiling: a
	// schema-flexible field with no limit is a place to hide a large write.
	MaxAttributeCount = 50
)

// Product is the catalogue's aggregate root: a sellable item with a price and a
// sale state.
//
// It can be built only through NewProduct or Rehydrate, both of which validate.
// A Product assembled by hand and written to storage would bypass every rule in
// this file, so the repository accepts only constructor output.
type Product struct {
	// ID is the product's identity. It is an opaque string here — the domain
	// does not assume a UUID, it only relies on ids sorting by creation time
	// because the id generator produces UUID v7.
	ID string
	// SKU is the human-facing natural key. It is always stored normalised; see
	// NormalizeSKU for why.
	SKU string
	// Name is what the customer sees. It is stored verbatim, because trimming
	// inside a value type would silently rewrite a name on every read.
	Name string
	// Description is free text and may be empty.
	Description string
	// Price is the amount the customer pays. It is never a float and always
	// carries a currency: an amount without one cannot be charged.
	Price money.Money
	// CategoryID groups products for browsing. It is optional and opaque.
	CategoryID string
	// Attributes carries schema-flexible facts such as colour or size. It is a
	// map, so every copy of a Product must copy it too — see Clone.
	Attributes map[string]string
	// IsActive is the sale state. An inactive product still exists: it keeps its
	// id, price and history, it is simply not sellable right now. Hiding it from
	// the catalogue is a business decision taken elsewhere (ListProducts and its
	// OnlyActive filter), not something the aggregate enforces.
	IsActive bool
	// CreatedAt and UpdatedAt are the audit timestamps. They are part of the
	// aggregate rather than a storage detail because callers see them: an
	// order's price snapshot is only auditable if the catalogue can say when the
	// price it copied was last changed.
	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewProductInput is everything needed to create a product.
//
// It is a struct rather than a long parameter list so that adding a field does
// not break every call site, and so that a caller cannot silently transpose two
// adjacent string arguments.
type NewProductInput struct {
	// ID is chosen by the caller. Generating it here would drag an
	// infrastructure dependency (a random source, a clock, a uuid library) into
	// a package that is deliberately free of them, and would make the
	// constructors untestable without one.
	ID          string
	SKU         string
	Name        string
	Description string
	Price       money.Money
	CategoryID  string
	Attributes  map[string]string
	// CreatedAt is the creation time; zero means "now", which is convenient in
	// tests and explicit in callers that already have a clock.
	CreatedAt time.Time
}

// NewProduct builds a validated, on-sale product.
//
// New products are always active. A caller cannot create something that is
// invisible to shoppers: taking a product off sale is a deliberate later action
// (UpdateProduct with IsActive), and treating "created but not yet on sale" as
// the default would make a typo in a create request look like a product that
// silently never appears.
func NewProduct(in NewProductInput) (*Product, error) {
	now := in.CreatedAt
	if now.IsZero() {
		now = time.Now()
	}

	product := Product{
		ID:          strings.TrimSpace(in.ID),
		SKU:         in.SKU,
		Name:        in.Name,
		Description: in.Description,
		Price:       in.Price,
		CategoryID:  in.CategoryID,
		Attributes:  cloneAttributes(in.Attributes),
		IsActive:    true,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	sku, err := NormalizeSKU(product.SKU)
	if err != nil {
		return nil, err
	}
	product.SKU = sku

	if err := product.Validate(); err != nil {
		return nil, err
	}
	return &product, nil
}

// Rehydrate rebuilds a product from storage.
//
// It re-runs the same rules as NewProduct and adds the checks that only make
// sense for a stored row: a row with no audit timestamps, or one that claims to
// have been updated before it was created, was written by a buggy migration or
// edited by hand. Catching that at read time turns silent corruption into an
// error at the point it is introduced, instead of letting a nonsensical
// timestamp reach an order's audit trail.
//
// Unlike NewProduct it does not force IsActive: a stored product is allowed to
// be off sale, which is the whole point of storing the flag.
func Rehydrate(p Product) (*Product, error) {
	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidProduct, err)
	}
	if p.CreatedAt.IsZero() {
		return nil, fmt.Errorf("%w: product %s has no creation timestamp", ErrInvalidProduct, p.ID)
	}
	if p.UpdatedAt.Before(p.CreatedAt) {
		return nil, fmt.Errorf("%w: product %s was updated at %s but created at %s",
			ErrInvalidProduct, p.ID, p.UpdatedAt.Format(time.RFC3339Nano), p.CreatedAt.Format(time.RFC3339Nano))
	}

	sku, err := NormalizeSKU(p.SKU)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidProduct, err)
	}

	rebuilt := p.Clone()
	rebuilt.SKU = sku
	return &rebuilt, nil
}

// Validate reports whether the product satisfies every rule that must hold for
// it to be sellable.
//
// It is exported because the update path has to re-validate a product it
// patched in place: a field mask can change one field in isolation, so the
// result of applying it is a product that no constructor has ever seen.
func (p Product) Validate() error {
	if strings.TrimSpace(p.ID) == "" {
		return Invalid("id", "is required")
	}
	if _, err := NormalizeSKU(p.SKU); err != nil {
		return err
	}
	if strings.TrimSpace(p.Name) == "" {
		return Invalid("name", "is required")
	}
	if len(p.Name) > MaxNameLength {
		return Invalid("name", "must be at most %d characters, got %d", MaxNameLength, len(p.Name))
	}
	if !p.Price.IsValid() {
		// The zero value of Money has no currency, which is what a price that
		// was never set looks like. Treating it as zero would put the product
		// on sale for free, so it is rejected instead of defaulted.
		return Invalid("price", "must have a currency, for example USD")
	}
	if p.Price.IsNegative() {
		// A negative price is a refund dressed as a product. Nothing in this
		// service is allowed to produce one; the order service is where money
		// legitimately moves backwards.
		return Invalid("price", "must not be negative, got %s", p.Price)
	}
	if len(p.Attributes) > MaxAttributeCount {
		return Invalid("attributes", "holds at most %d entries, got %d", MaxAttributeCount, len(p.Attributes))
	}
	for key := range p.Attributes {
		if strings.TrimSpace(key) == "" {
			return Invalid("attributes", "contains an entry with an empty key")
		}
	}
	return nil
}

// Clone returns a deep copy.
//
// A plain value copy is not enough: Attributes is a map, so two Product values
// can share it, and a caller that mutates the map it received would be editing
// state it does not own. The repository leans on this for every read and write.
func (p Product) Clone() Product {
	cloned := p
	cloned.Attributes = cloneAttributes(p.Attributes)
	return cloned
}

// NormalizeSKU canonicalises a SKU and rejects a malformed one.
//
// Normalisation is what makes the natural key trustworthy. "widget-01",
// " WIDGET-01 " and "Widget-01" are the same product to everybody except a
// database, and a uniqueness constraint that only catches one of them is not a
// uniqueness constraint. Uppercasing is the documented behaviour, not an
// accident, so the returned value is what gets stored.
func NormalizeSKU(raw string) (string, error) {
	sku := strings.ToUpper(strings.TrimSpace(raw))
	if sku == "" {
		return "", Invalid("sku", "is required")
	}
	if len(sku) > MaxSKULength {
		return "", Invalid("sku", "must be at most %d characters, got %d", MaxSKULength, len(sku))
	}
	for _, r := range sku {
		if !isSKURune(r) {
			return "", Invalid("sku", "%q is not allowed in a SKU; use letters, digits and '-', '_', '.', '/'", r)
		}
	}
	return sku, nil
}

// isSKURune reports whether r may appear in a SKU. The set is the one that
// survives a round trip through a URL, a CSV export and a log line unchanged;
// anything else is rejected loudly rather than stored and discovered later.
func isSKURune(r rune) bool {
	switch {
	case r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return true
	case r == '-', r == '_', r == '.', r == '/':
		return true
	default:
		return false
	}
}

// cloneAttributes copies a map, treating nil as nil so that an absent attribute
// set stays absent rather than becoming an allocated empty one.
func cloneAttributes(attrs map[string]string) map[string]string {
	if attrs == nil {
		return nil
	}
	cloned := make(map[string]string, len(attrs))
	for k, v := range attrs {
		cloned[k] = v
	}
	return cloned
}
