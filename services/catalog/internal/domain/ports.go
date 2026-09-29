package domain

import (
	"context"
	"slices"
)

// The interface in this file is the catalog service's port. The domain defines
// what it needs; adapters (an in-memory store today, PostgreSQL later) provide
// it.
//
// The dependency therefore points inward: `adapters` imports `domain`, never the
// reverse. That is what keeps the business rules testable without a database,
// and it is why the same Repository can be backed by a map in tests and by SQL
// in production without a line of business logic changing.

// Pagination bounds.
//
// They live in the domain rather than in an adapter because they are part of the
// contract every caller depends on: the order service sizes its cart requests
// against DefaultPageSize, and the batch RPC rejects anything above MaxPageSize.
// An adapter that invented its own limits would make those guarantees false.
const (
	// DefaultPageSize is used when a caller does not ask for a specific size.
	// Without it, "give me everything" would be the default request and the
	// first large catalogue would answer it slowly.
	DefaultPageSize = 20
	// MaxPageSize is the largest page a caller can ask for, whatever it asks.
	MaxPageSize = 100
)

// ListFilter narrows a listing.
//
// Every field is optional: an empty value means "do not filter on this", which
// is spelled out in the proto contract as well, so a gateway that forwards an
// unset field does not accidentally filter on the empty string.
type ListFilter struct {
	// CategoryID selects one category. An empty value selects all of them.
	CategoryID string
	// Query is a free-text match. It is deliberately not a query language: the
	// catalogue is small enough that a case-insensitive substring match over
	// name, SKU and description is both sufficient and predictable for callers.
	Query string
	// OnlyActive excludes products that are not on sale.
	OnlyActive bool
	// PageSize is the requested page size. It is clamped by the repository to
	// [1, MaxPageSize]; zero means DefaultPageSize.
	PageSize int
	// PageToken resumes a previous listing. It is the id of the last product the
	// caller received — see the Repository.List contract for why an opaque id
	// rather than an offset.
	PageToken string
}

// ListResult is one page of products.
type ListResult struct {
	// Products is the page itself, in the repository's stable order.
	Products []Product
	// NextPageToken is the id to pass to the next call, or empty when this was
	// the last page.
	NextPageToken string
	// TotalSize is how many products match the filter, ignoring pagination. It
	// lets a caller render "showing 20 of 137" without walking the collection.
	TotalSize int
}

// ClampPageSize applies the pagination bounds. It is exported so that a
// transport adapter can echo the effective page size back to a caller without
// duplicating the rule.
func ClampPageSize(requested int) int {
	switch {
	case requested <= 0:
		return DefaultPageSize
	case requested > MaxPageSize:
		return MaxPageSize
	default:
		return requested
	}
}

// Repository stores products.
//
// Every method takes a context so that a caller's deadline and cancellation
// propagate to the store, which is what stops a slow query outliving the request
// that needed it.
//
// Implementations must deep-copy: a value handed in must not be able to change
// stored state afterwards, and a value handed out must not be able to change it
// either. A Product carries a map, so a shallow copy is not enough.
type Repository interface {
	// Create stores a new product.
	//
	// It returns ErrDuplicateSKU when the (normalised) SKU is taken, and
	// ErrDuplicateProduct when the id is.
	Create(ctx context.Context, product *Product) error

	// Get loads a product by id, returning ErrProductNotFound when it is absent.
	Get(ctx context.Context, id string) (*Product, error)

	// GetBySKU loads a product by its normalised SKU, so "widget-01" and
	// "WIDGET-01 " address the same product. It returns ErrProductNotFound when
	// nothing matches and a *ValidationError when the SKU itself is malformed.
	GetBySKU(ctx context.Context, sku string) (*Product, error)

	// List returns one page of products matching the filter.
	//
	// The ordering must be stable across calls, and the cursor must be
	// positional: an id and not an offset. An offset is wrong for a live
	// catalogue — inserting or deleting a product shifts every later page, so a
	// caller walking the collection either sees a product twice or never at all.
	//
	// NextPageToken is the id of the last product in the page, or empty when the
	// page is the last one.
	List(ctx context.Context, filter ListFilter) (ListResult, error)

	// Update replaces an existing product, returning ErrProductNotFound when the
	// id is unknown and ErrDuplicateSKU when the new SKU belongs to another
	// product.
	Update(ctx context.Context, product *Product) error
}

// UpdatableFields lists the product fields a field mask may name, in the order
// they are reported in an error message.
//
// The list is closed on purpose, and it is the one thing in this file that names
// a field the way the API spells it. A field mask is strings, so a typo — "nmae"
// — would otherwise be an update that reports success and changes nothing, which
// is the hardest kind of bug to notice from the caller's side.
//
// It lives in the domain rather than in the transport adapter because two
// adapters must agree on it: the gRPC layer rejects an unknown field before the
// request is served, and a storage adapter that can apply a mask as a single
// write must reject it again before the name reaches a SQL statement. A second
// copy of the list would be a second source of truth, and the day they diverge
// the service accepts a field it then silently ignores.
var UpdatableFields = []string{"sku", "name", "description", "price", "category_id", "attributes", "active"}

// IsUpdatableField reports whether a field mask may name this field.
func IsUpdatableField(name string) bool {
	return slices.Contains(UpdatableFields, name)
}

// FieldMaskedUpdater is an optional extension of Repository.
//
// Update replaces a product wholesale, which is the honest signature for an
// aggregate that was read, patched in the caller's memory and written back. It
// is also the reason a repository may offer this second operation: a
// read-modify-write cannot express "change the name and leave the price alone",
// so a caller that renames a product while another re-prices it has its own copy
// of the price written back, silently reverting the other change. An adapter
// that can apply the mask as one write — a single UPDATE whose SET clause is
// built from the same mask — makes the two updates touch disjoint columns and
// the lost update impossible.
//
// The implementation must write only the named fields, must leave the rest of
// the stored row untouched, and must return the stored product as it stands
// after the write. An empty mask keeps the documented meaning of "apply
// everything".
type FieldMaskedUpdater interface {
	UpdateFields(ctx context.Context, product *Product, fields []string) (*Product, error)
}

// BatchGetter is an optional extension of Repository.
//
// Pricing a cart asks for a handful of products by id, and Repository offers
// only Get. An adapter backed by a network can answer the whole set in one
// round trip, and pricing a twenty-line cart should not be twenty of them.
//
// The result is the products that exist: an id the catalogue does not know is
// absent from the answer rather than an error, because that is what the batch
// RPC promises its caller — it compares the size of the answer with the size of
// the request to detect a miss. Implementations must not invent a zero-valued
// product for a miss; a product priced at nothing would quietly make the cart
// total too low.
type BatchGetter interface {
	GetBatch(ctx context.Context, ids []string) ([]Product, error)
}
