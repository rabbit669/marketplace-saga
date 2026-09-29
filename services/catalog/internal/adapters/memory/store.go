// Package memory is an in-process implementation of domain.Repository.
//
// It exists so that the service is runnable, and therefore exercisable
// end-to-end, before a database is involved: the gRPC surface, the validation
// rules and the pagination contract can all be driven by a real client against
// a real server in a test that takes milliseconds. The SQL implementation will
// be a second adapter behind the same port, and nothing above this package
// changes when it lands.
//
// The trade-off is explicit: state is lost on restart and a single process
// holds the whole catalogue. That is acceptable for a demo stack and is exactly
// why the port, not this package, defines the semantics.
package memory

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/marketplace/services/catalog/internal/domain"
)

// Store is a concurrency-safe, in-memory product repository.
//
// The mutex is an RWMutex because the read path (Get, GetBySKU, List) dominates:
// every cart priced by the order service reads, and only catalogue edits write.
// A plain Mutex would serialise the reads that the whole platform depends on.
type Store struct {
	mu sync.RWMutex

	// products is keyed by product id.
	products map[string]domain.Product

	// skuIndex maps a normalised SKU to a product id, which is what makes
	// duplicate detection a map lookup rather than a scan.
	//
	// It is a separate index, and not a scan over products, because uniqueness
	// is a rule rather than a query: a scan would be correct here and wrong the
	// moment the store is replaced by anything concurrent.
	skuIndex map[string]string

	// ids holds every product id in ascending order.
	//
	// Ids are UUID v7, so ascending id order is creation order. Keeping the
	// slice sorted makes pagination a slice walk instead of a sort on every
	// List call, and — more importantly — gives the cursor a total order to be
	// positional against, which is what makes "resume after this id" stable
	// while the catalogue is being edited.
	ids []string
}

// New returns an empty store.
func New() *Store {
	return &Store{
		products: make(map[string]domain.Product),
		skuIndex: make(map[string]string),
	}
}

// Store must satisfy the port the rest of the service depends on.
var _ domain.Repository = (*Store)(nil)

// Create stores a new product.
func (s *Store) Create(ctx context.Context, product *domain.Product) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	sku, err := validateForWrite(product)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.products[product.ID]; exists {
		return fmt.Errorf("%w: %s", domain.ErrDuplicateProduct, product.ID)
	}
	if owner, taken := s.skuIndex[sku]; taken {
		return fmt.Errorf("%w: %s is already used by product %s", domain.ErrDuplicateSKU, sku, owner)
	}

	stored := product.Clone()
	// Stored normalised so that the index key and the stored value can never
	// disagree; a mismatch would defeat duplicate detection on the next write.
	stored.SKU = sku

	s.products[stored.ID] = stored
	s.skuIndex[sku] = stored.ID
	s.insertID(stored.ID)

	return nil
}

// Get loads a product by id.
func (s *Store) Get(ctx context.Context, id string) (*domain.Product, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	product, ok := s.products[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", domain.ErrProductNotFound, id)
	}
	return clonePtr(product), nil
}

// GetBySKU loads a product by its normalised SKU.
func (s *Store) GetBySKU(ctx context.Context, sku string) (*domain.Product, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	normalised, err := domain.NormalizeSKU(sku)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	id, ok := s.skuIndex[normalised]
	if !ok {
		return nil, fmt.Errorf("%w: sku %s", domain.ErrProductNotFound, normalised)
	}

	product, ok := s.products[id]
	if !ok {
		// Unreachable while the index and the map are updated together. It is
		// reported as not-found rather than as corruption because a dangling
		// index entry and a deleted product are indistinguishable to a caller.
		return nil, fmt.Errorf("%w: sku %s", domain.ErrProductNotFound, normalised)
	}
	return clonePtr(product), nil
}

// List returns one page of products matching the filter.
func (s *Store) List(ctx context.Context, filter domain.ListFilter) (domain.ListResult, error) {
	if err := ctx.Err(); err != nil {
		return domain.ListResult{}, err
	}

	size := domain.ClampPageSize(filter.PageSize)

	s.mu.RLock()
	defer s.mu.RUnlock()

	// TotalSize counts every match, not just the page, so it is computed over
	// the whole collection. That is a full scan; a SQL adapter would do the same
	// with a COUNT, and the caller uses the number to render "showing 20 of 137".
	total := 0
	for _, id := range s.ids {
		if matches(s.products[id], filter) {
			total++
		}
	}

	// The cursor is resolved by position, not by lookup: a token naming a
	// product that has since been deleted must not restart the walk or skip
	// everything after it. Scanning forward past every id <= the token gives the
	// same answer whether or not that product still exists.
	start, _ := slices.BinarySearch(s.ids, filter.PageToken)
	for start < len(s.ids) && s.ids[start] <= filter.PageToken {
		start++
	}

	// One extra match is collected to decide whether a further page exists. It
	// is cheaper and more honest than predicting from TotalSize, which knows
	// nothing about where the cursor sits.
	page := make([]domain.Product, 0, size+1)
	for i := start; i < len(s.ids) && len(page) <= size; i++ {
		product, ok := s.products[s.ids[i]]
		if !ok || !matches(product, filter) {
			continue
		}
		page = append(page, product.Clone())
	}

	var next string
	if len(page) > size {
		page = page[:size]
		next = page[size-1].ID
	}

	return domain.ListResult{
		Products:      page,
		NextPageToken: next,
		TotalSize:     total,
	}, nil
}

// Update replaces an existing product.
func (s *Store) Update(ctx context.Context, product *domain.Product) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	sku, err := validateForWrite(product)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.products[product.ID]
	if !ok {
		return fmt.Errorf("%w: %s", domain.ErrProductNotFound, product.ID)
	}
	// An id is immutable once stored: it is the map key and the pagination
	// cursor, so letting it move would invalidate tokens callers already hold.
	// A caller that got this far passed the same id it read, so the only way to
	// reach here with a clash is a bug elsewhere in the process.
	if owner, taken := s.skuIndex[sku]; taken && owner != product.ID {
		return fmt.Errorf("%w: %s is already used by product %s", domain.ErrDuplicateSKU, sku, owner)
	}

	stored := product.Clone()
	stored.SKU = sku

	// The old SKU is released only after the new one is known to be free, so a
	// rejected update leaves the index exactly as it was.
	if existing.SKU != sku {
		delete(s.skuIndex, existing.SKU)
	}
	s.skuIndex[sku] = stored.ID
	s.products[stored.ID] = stored

	return nil
}

// Seed inserts demo products.
//
// It is a thin wrapper over Create on purpose: demo data goes through the same
// uniqueness checks as a real write, so a fixture that collides fails loudly at
// startup instead of producing a store whose contents contradict its index.
func (s *Store) Seed(products ...domain.Product) error {
	for i := range products {
		product := products[i]
		if err := s.Create(context.Background(), &product); err != nil {
			return fmt.Errorf("seed product %d (%s): %w", i, product.SKU, err)
		}
	}
	return nil
}

// Len returns how many products are stored.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.products)
}

// validateForWrite re-checks the invariants this store's indexes depend on.
//
// The port asks callers to pass constructor output, but "the caller promised" is
// not an invariant. The SKU index is keyed by the normalised SKU, so an
// unnormalised value slipping through would silently create a second entry for a
// product that already exists — the one failure this store must not have.
func validateForWrite(product *domain.Product) (string, error) {
	if product == nil {
		return "", domain.Invalid("product", "is required")
	}
	sku, err := domain.NormalizeSKU(product.SKU)
	if err != nil {
		return "", err
	}
	if !product.Price.IsValid() {
		return "", domain.Invalid("price", "must have a currency, for example USD")
	}
	return sku, nil
}

// insertID keeps the id slice sorted. Ids are UUID v7, so this is insertion in
// creation order, which is the order a catalogue is normally browsed in.
func (s *Store) insertID(id string) {
	at, found := slices.BinarySearch(s.ids, id)
	if found {
		return
	}
	s.ids = slices.Insert(s.ids, at, id)
}

// matches reports whether a product satisfies every filter the caller set.
func matches(product domain.Product, filter domain.ListFilter) bool {
	if filter.OnlyActive && !product.IsActive {
		return false
	}
	// Trimmed so that a gateway forwarding an unset optional field as " " does
	// not silently filter on a category named " ".
	if category := strings.TrimSpace(filter.CategoryID); category != "" && product.CategoryID != category {
		return false
	}

	query := strings.ToLower(strings.TrimSpace(filter.Query))
	if query == "" {
		return true
	}
	// Description participates because a shopper searching a catalogue expects
	// a word that only appears in the description to find the product; SKU
	// participates because support searches by it.
	return strings.Contains(strings.ToLower(product.Name), query) ||
		strings.Contains(strings.ToLower(product.SKU), query) ||
		strings.Contains(strings.ToLower(product.Description), query)
}

// clonePtr returns a pointer to a deep copy, so that a caller mutating what it
// read cannot reach the stored value.
func clonePtr(product domain.Product) *domain.Product {
	cloned := product.Clone()
	return &cloned
}
