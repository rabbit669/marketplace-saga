// Package postgres is the catalog service's durable storage.
//
// It implements the domain's Repository, plus the two optional ports the
// transport adapter uses when it can: a masked update, which makes a partial
// edit a single SQL write instead of a read-modify-write, and a batch read,
// which prices a whole cart in one round trip. They live in this package
// because they are storage capabilities and nothing in the domain depends on
// them being present — an adapter that offers only Repository is still a
// complete catalogue.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/marketplace/pkg/money"

	"github.com/marketplace/services/catalog/internal/domain"
)

// Statement fragments shared by the read paths.
//
// The column list is written once so that every SELECT, and the RETURNING clause
// of the masked update, scan in the same order: a column added in one place and
// forgotten in another would be a silent shift in every field after it.
const (
	productColumns = `id, sku, name, description, price_minor, currency,
	                  category_id, attributes, is_active, created_at, updated_at`

	selectProducts = `SELECT ` + productColumns + ` FROM products`
)

// Names the adapter matches unique violations against. They are the names given
// in migrations/0001_init.sql: PostgreSQL reports the violated constraint or
// unique index, which is what lets a duplicate SKU be told apart from a
// duplicate id without a preceding SELECT.
const (
	skuIndexName    = "products_sku_key"
	productsPKName  = "products_pkey"
	uniqueViolation = "23505"
)

// Store is the catalog service's PostgreSQL adapter.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a store backed by the pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Pool exposes the pool so that health checks and the readiness probe can use
// it.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// The adapter must satisfy the port the rest of the service depends on, and the
// two optional ports the transport layer upgrades to when they exist.
var (
	_ domain.Repository         = (*Store)(nil)
	_ domain.FieldMaskedUpdater = (*Store)(nil)
	_ domain.BatchGetter        = (*Store)(nil)
)

// ------------------------------------------------------------------ writes

// Create stores a new product.
//
// Uniqueness is decided by the database, not by this method. A SELECT for the
// SKU followed by an INSERT is the natural implementation and is wrong: two
// concurrent creates of the same SKU both find nothing and both insert, and the
// catalogue ends up holding two products the customer cannot tell apart — with
// the price the order service charges depending on which row it happens to read.
// The insert is therefore attempted unconditionally and the unique violation it
// may raise is translated into the domain's error, which is the only form of
// this check that is atomic with the write.
func (s *Store) Create(ctx context.Context, product *domain.Product) error {
	sku, err := validateForWrite(product)
	if err != nil {
		return err
	}

	attributes, err := encodeAttributes(product.Attributes)
	if err != nil {
		return fmt.Errorf("product %s: %w", product.ID, err)
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO products (
			id, sku, name, description, price_minor, currency,
			category_id, attributes, is_active, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		product.ID, sku, product.Name, product.Description,
		product.Price.Amount(), string(product.Price.Currency()),
		product.CategoryID, attributes, product.IsActive,
		product.CreatedAt, product.UpdatedAt,
	)
	if err != nil {
		if duplicate := translateUniqueViolation(err, product.ID, sku); duplicate != nil {
			return duplicate
		}
		return fmt.Errorf("insert product %s: %w", product.ID, err)
	}
	return nil
}

// Update replaces an existing product.
func (s *Store) Update(ctx context.Context, product *domain.Product) error {
	sku, err := validateForWrite(product)
	if err != nil {
		return err
	}

	attributes, err := encodeAttributes(product.Attributes)
	if err != nil {
		return fmt.Errorf("product %s: %w", product.ID, err)
	}

	tag, err := s.pool.Exec(ctx, `
		UPDATE products SET
			sku = $2, name = $3, description = $4,
			price_minor = $5, currency = $6, category_id = $7,
			attributes = $8, is_active = $9, updated_at = $10
		WHERE id = $1`,
		product.ID, sku, product.Name, product.Description,
		product.Price.Amount(), string(product.Price.Currency()),
		product.CategoryID, attributes, product.IsActive, product.UpdatedAt,
	)
	if err != nil {
		if duplicate := translateUniqueViolation(err, product.ID, sku); duplicate != nil {
			return duplicate
		}
		return fmt.Errorf("update product %s: %w", product.ID, err)
	}
	// No row means the id is unknown. It is not a distinction the caller can
	// make beforehand: another writer may have deleted the product since it was
	// read, and "not found" is the honest answer either way.
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", domain.ErrProductNotFound, product.ID)
	}
	return nil
}

// UpdateFields applies a field mask to a stored product in one statement.
//
// It implements domain.FieldMaskedUpdater. Update writes every column, which
// means the caller must first read the product, patch it and send the whole
// thing back; a rename that runs alongside a re-pricing therefore writes its own
// stale copy of the price and silently reverts the other change. Here only the
// columns the mask names appear in the SET clause, so two callers editing
// different fields touch different columns and neither can undo the other.
//
// The mask is validated before any SQL is built. A field mask is caller-supplied
// text, and the only safe way to put it in a statement is to map it onto a
// column through a closed set — an unknown name is rejected as a bad request,
// never interpolated and never silently dropped. The column names that do reach
// the statement come from that closed set and never from the request.
//
// The updated row is returned from the same statement, so the caller answers
// with what is stored rather than with what it hoped it had stored.
func (s *Store) UpdateFields(ctx context.Context, product *domain.Product, fields []string) (*domain.Product, error) {
	if product == nil {
		return nil, domain.Invalid("product", "is required")
	}

	mask, err := parseMask(fields)
	if err != nil {
		return nil, err
	}
	sku, err := validateForWrite(product)
	if err != nil {
		return nil, err
	}

	attributes, err := encodeAttributes(product.Attributes)
	if err != nil {
		return nil, fmt.Errorf("product %s: %w", product.ID, err)
	}

	// $1 is the id, so the assignments start at $2. The order is the one the
	// domain lists the fields in and not the order the caller sent them: the
	// same update then produces the same statement text every time, which keeps
	// the statement cache honest and makes two requests comparable in a log.
	args := []any{product.ID}
	assignments := make([]string, 0, len(mask)+2)

	for _, field := range domain.UpdatableFields {
		if !mask[field] {
			continue
		}
		setters, err := fieldAssignments(field, product, attributes)
		if err != nil {
			return nil, err
		}
		for _, setter := range setters {
			assignments = append(assignments, fmt.Sprintf("%s = $%d", setter.column, len(args)+1))
			args = append(args, setter.value)
		}
	}

	// The audit timestamp always moves, even when the mask names no data field:
	// a caller that copied a price out of this product needs to be able to tell
	// whether the copy is still current, and an update that leaves updated_at
	// untouched is indistinguishable from no update at all.
	assignments = append(assignments, fmt.Sprintf("updated_at = $%d", len(args)+1))
	args = append(args, product.UpdatedAt)

	query := `UPDATE products SET ` + strings.Join(assignments, ", ") +
		` WHERE id = $1 RETURNING ` + productColumns

	updated, err := scanProduct(s.pool.QueryRow(ctx, query, args...))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", domain.ErrProductNotFound, product.ID)
		}
		if duplicate := translateUniqueViolation(err, product.ID, sku); duplicate != nil {
			return nil, duplicate
		}
		return nil, fmt.Errorf("update product %s: %w", product.ID, err)
	}

	// Validated on the way out, like every other read: the row that was just
	// written is the row the caller is about to act on.
	rebuilt, err := domain.Rehydrate(*updated)
	if err != nil {
		return nil, fmt.Errorf("product %s: stored row is invalid: %w", product.ID, err)
	}
	return rebuilt, nil
}

// ------------------------------------------------------------------- reads

// Get loads a product by id.
func (s *Store) Get(ctx context.Context, id string) (*domain.Product, error) {
	product, err := scanProduct(s.pool.QueryRow(ctx, selectProducts+` WHERE id = $1`, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", domain.ErrProductNotFound, id)
		}
		return nil, fmt.Errorf("select product %s: %w", id, err)
	}
	return rehydrate(product)
}

// GetBySKU loads a product by its normalised SKU.
//
// The SKU is normalised here and not by the caller, because normalisation is
// what makes the natural key trustworthy: the column holds "WIDGET-01" and a
// lookup for " widget-01 " has to find it. A malformed SKU is a caller error and
// is rejected before the query, so it never reaches the database as a lookup
// that simply finds nothing.
func (s *Store) GetBySKU(ctx context.Context, sku string) (*domain.Product, error) {
	normalised, err := domain.NormalizeSKU(sku)
	if err != nil {
		return nil, err
	}

	product, err := scanProduct(s.pool.QueryRow(ctx, selectProducts+` WHERE sku = $1`, normalised))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: sku %s", domain.ErrProductNotFound, normalised)
		}
		return nil, fmt.Errorf("select product by sku %s: %w", normalised, err)
	}
	return rehydrate(product)
}

// GetBatch loads the products with these ids, in ascending id order.
//
// It implements domain.BatchGetter. Ids the catalogue does not know are simply
// absent from the answer, which is what the batch RPC promises: it exists to
// price a cart, and a miss must not become a zero-priced line. The ids are
// de-duplicated because the request may repeat one, and a repeated id must not
// become a repeated product.
func (s *Store) GetBatch(ctx context.Context, ids []string) ([]domain.Product, error) {
	unique := uniqueIDs(ids)
	if len(unique) == 0 {
		return nil, nil
	}

	rows, err := s.pool.Query(ctx, selectProducts+` WHERE id = ANY($1) ORDER BY id`, unique)
	if err != nil {
		return nil, fmt.Errorf("select %d products: %w", len(unique), err)
	}
	defer rows.Close()

	products := make([]domain.Product, 0, len(unique))
	for rows.Next() {
		product, err := scanProduct(rows)
		if err != nil {
			return nil, fmt.Errorf("scan product: %w", err)
		}
		rebuilt, err := rehydrate(product)
		if err != nil {
			return nil, err
		}
		products = append(products, *rebuilt)
	}
	return products, rows.Err()
}

// List returns one page of products matching the filter.
//
// The cursor is positional, not a lookup: the clause is "id > token", so a token
// naming a product that has since been deleted still resumes exactly where the
// caller stopped instead of restarting the walk or returning nothing.
//
// The count and the page are read in one read-only REPEATABLE READ transaction.
// Two independent statements would each see their own snapshot, and a write
// landing between them would produce a page and a total that contradict each
// other; the in-memory adapter gets consistency from holding its lock across
// both, and a durable adapter owes its callers the same answer.
func (s *Store) List(ctx context.Context, filter domain.ListFilter) (domain.ListResult, error) {
	size := domain.ClampPageSize(filter.PageSize)
	where, args := filterPredicate(filter)

	// The cursor narrows the page and nothing else. TotalSize describes the whole
	// match set — it is what a caller renders as "showing 20 of 137" — so
	// counting after the cursor would report the remainder of the walk instead.
	pageWhere := where
	pageArgs := append([]any{}, args...)
	if token := strings.TrimSpace(filter.PageToken); token != "" {
		pageArgs = append(pageArgs, token)
		cursor := fmt.Sprintf("id > $%d", len(pageArgs))
		if pageWhere == "" {
			pageWhere = "WHERE " + cursor
		} else {
			pageWhere += " AND " + cursor
		}
	}

	var result domain.ListResult

	err := s.inReadTx(ctx, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM products `+where, args...).Scan(&result.TotalSize); err != nil {
			return fmt.Errorf("count products: %w", err)
		}

		// One row more than the page is fetched to decide whether a further page
		// exists. It is cheaper and more honest than predicting from TotalSize,
		// which knows nothing about where the cursor sits.
		pageArgs = append(pageArgs, size+1)
		query := selectProducts + ` ` + pageWhere +
			fmt.Sprintf(` ORDER BY id LIMIT $%d`, len(pageArgs))

		rows, err := tx.Query(ctx, query, pageArgs...)
		if err != nil {
			return fmt.Errorf("select products: %w", err)
		}
		defer rows.Close()

		page := make([]domain.Product, 0, size+1)
		for rows.Next() {
			product, err := scanProduct(rows)
			if err != nil {
				return fmt.Errorf("scan product: %w", err)
			}
			rebuilt, err := rehydrate(product)
			if err != nil {
				return err
			}
			page = append(page, *rebuilt)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		// The token is the id of the last product actually returned, or empty
		// when there is nothing after this page. A caller that passed the token
		// back gets the next page and no duplicates.
		if len(page) > size {
			page = page[:size]
			result.NextPageToken = page[size-1].ID
		}
		result.Products = page
		return nil
	})
	if err != nil {
		return domain.ListResult{}, err
	}
	return result, nil
}

// Count reports how many products are stored. It exists for the startup log and
// for tests, not for a request path.
func (s *Store) Count(ctx context.Context) (int, error) {
	var total int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM products`).Scan(&total); err != nil {
		return 0, fmt.Errorf("count products: %w", err)
	}
	return total, nil
}

// Seed inserts demo products.
//
// It is a thin wrapper over Create on purpose: demo data goes through the same
// uniqueness rules as a real write, so a fixture that collides fails loudly at
// startup instead of producing a catalogue whose contents contradict its index.
func (s *Store) Seed(ctx context.Context, products ...domain.Product) error {
	for i := range products {
		product := products[i]
		if err := s.Create(ctx, &product); err != nil {
			return fmt.Errorf("seed product %d (%s): %w", i, product.SKU, err)
		}
	}
	return nil
}

// ----------------------------------------------------------------- helpers

// filterPredicate builds the WHERE clause for the filter itself.
//
// The cursor is deliberately not part of it: the total is counted with this
// clause and the page is selected with this clause plus the cursor, so the two
// describe the same match set.
//
// Text is trimmed in Go rather than in SQL so that the rule matches the
// in-memory adapter exactly: it treats " " as "no filter", and a gateway that
// forwards an unset optional field as whitespace must not silently filter on a
// category named " ".
func filterPredicate(filter domain.ListFilter) (string, []any) {
	conditions := make([]string, 0, 4)
	args := make([]any, 0, 4)

	if category := strings.TrimSpace(filter.CategoryID); category != "" {
		args = append(args, category)
		conditions = append(conditions, fmt.Sprintf("category_id = $%d", len(args)))
	}
	if filter.OnlyActive {
		conditions = append(conditions, "is_active")
	}
	if query := strings.ToLower(strings.TrimSpace(filter.Query)); query != "" {
		// strpos() rather than LIKE: a shopper searching for "50%" or "a_b"
		// means those characters literally, and a LIKE pattern would treat them
		// as wildcards and quietly match products they never asked for. The
		// substring match over name, SKU and description is the same rule the
		// in-memory adapter applies: SKU participates because support searches
		// by it, description because a word that only appears there should
		// still find the product.
		args = append(args, query)
		at := fmt.Sprintf("$%d", len(args))
		conditions = append(conditions, fmt.Sprintf(
			"(strpos(lower(name), %s) > 0 OR strpos(lower(sku), %s) > 0 OR strpos(lower(description), %s) > 0)",
			at, at, at))
	}

	if len(conditions) == 0 {
		return "", args
	}
	return "WHERE " + strings.Join(conditions, " AND "), args
}

// parseMask validates a caller-supplied field mask and returns it as a set.
//
// An empty mask means "apply everything", which is the documented contract of
// the API and the behaviour a caller sending a whole product expects. Folding
// that rule in here keeps it from being repeated — and eventually forgotten — at
// each place a field is read from the mask.
func parseMask(fields []string) (map[string]bool, error) {
	mask := make(map[string]bool, len(fields))
	for _, field := range fields {
		name := strings.TrimSpace(field)
		if name == "" {
			// A blank entry is how an unset optional field arrives from some
			// clients. It names nothing, so skipping it changes no behaviour.
			continue
		}
		if !domain.IsUpdatableField(name) {
			return nil, domain.Invalid("update_mask",
				"%q is not an updatable field; updatable fields are %s",
				name, strings.Join(domain.UpdatableFields, ", "))
		}
		mask[name] = true
	}

	if len(mask) == 0 {
		for _, field := range domain.UpdatableFields {
			mask[field] = true
		}
	}
	return mask, nil
}

// assignment is one column of a masked update's SET clause.
type assignment struct {
	column string
	value  any
}

// fieldAssignments maps one mask field to the columns it writes.
//
// The mapping is a closed switch and not a string transformation on the caller's
// input: the column names are interpolated into a statement, so they must be
// values this program controls. An unrecognised field is a programming error by
// the time it gets here — parseMask has already rejected it — and it is reported
// rather than guessed at.
//
// A field can map to more than one column, which is why this returns a slice:
// "price" is an amount and a currency, and writing one without the other would
// leave a row whose price is denominated in whatever currency the other caller
// happened to set.
func fieldAssignments(field string, product *domain.Product, attributes []byte) ([]assignment, error) {
	switch field {
	case "sku":
		// Normalised on the way in, so the unique index compares what the
		// domain considers the same SKU.
		sku, err := domain.NormalizeSKU(product.SKU)
		if err != nil {
			return nil, err
		}
		return []assignment{{"sku", sku}}, nil
	case "name":
		return []assignment{{"name", product.Name}}, nil
	case "description":
		return []assignment{{"description", product.Description}}, nil
	case "price":
		return []assignment{
			{"price_minor", product.Price.Amount()},
			{"currency", string(product.Price.Currency())},
		}, nil
	case "category_id":
		return []assignment{{"category_id", product.CategoryID}}, nil
	case "attributes":
		return []assignment{{"attributes", attributes}}, nil
	case "active":
		return []assignment{{"is_active", product.IsActive}}, nil
	default:
		return nil, domain.Invalid("update_mask", "%q is not an updatable field", field)
	}
}

// validateForWrite re-checks the invariants this store's columns and indexes
// depend on.
//
// The port asks callers to pass constructor output, but "the caller promised" is
// not an invariant. The unique index is keyed by the normalised SKU, so an
// unnormalised value slipping through would store a second row for a product
// that already exists — the one failure this store must not have.
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

// rehydrate runs the domain's validation over a row read from storage.
//
// A migration, a manual edit or a bug in an older writer can leave a row that
// violates the current rules, and the read path is where every consumer — the
// gateway, the order service pricing a cart — benefits from catching it. Failing
// here turns silent corruption into an error at the point it is introduced.
func rehydrate(product *domain.Product) (*domain.Product, error) {
	rebuilt, err := domain.Rehydrate(*product)
	if err != nil {
		return nil, fmt.Errorf("product %s: stored row is invalid: %w", product.ID, err)
	}
	return rebuilt, nil
}

// translateUniqueViolation maps a SQLSTATE 23505 error onto the domain error it
// means, or returns nil when err is not a uniqueness clash.
//
// The constraint name is what makes the two cases distinguishable: a clash on
// the SKU index is a business conflict the caller resolves by choosing another
// SKU, while a clash on the primary key means the caller reused an id it was
// given, which is a bug on its side. Reporting either as the other sends support
// looking in the wrong place.
func translateUniqueViolation(err error, productID, sku string) error {
	switch uniqueViolationConstraint(err) {
	case skuIndexName:
		return fmt.Errorf("%w: %s is already used by another product", domain.ErrDuplicateSKU, sku)
	case productsPKName:
		return fmt.Errorf("%w: %s", domain.ErrDuplicateProduct, productID)
	default:
		return nil
	}
}

// uniqueViolationConstraint returns the name of the constraint or unique index a
// uniqueness violation broke, or "" when err is not one.
//
// It reads the SQLSTATE and the constraint name rather than matching on message
// text, which changes between server versions and locales.
func uniqueViolationConstraint(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		return pgErr.ConstraintName
	}
	return ""
}

// inReadTx runs fn in a read-only transaction whose statements share one
// snapshot.
func (s *Store) inReadTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return fmt.Errorf("begin read transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// rowScanner is satisfied by both a pool row and an open result set, so that one
// scan function serves the single-row and the paged reads.
type rowScanner interface {
	Scan(dest ...any) error
}

// scanProduct reads one row in the column order of productColumns.
func scanProduct(row rowScanner) (*domain.Product, error) {
	var (
		product    domain.Product
		minor      int64
		currency   string
		attributes []byte
	)

	err := row.Scan(
		&product.ID, &product.SKU, &product.Name, &product.Description,
		&minor, &currency, &product.CategoryID, &attributes,
		&product.IsActive, &product.CreatedAt, &product.UpdatedAt,
	)
	if err != nil {
		// pgx.ErrNoRows passes through untouched so that each caller can turn it
		// into the not-found error it means for its own operation.
		return nil, err
	}

	price, err := money.New(minor, money.Currency(currency))
	if err != nil {
		return nil, fmt.Errorf("product %s: stored price is invalid: %w", product.ID, err)
	}
	product.Price = price

	attrs, err := decodeAttributes(attributes)
	if err != nil {
		return nil, fmt.Errorf("product %s: %w", product.ID, err)
	}
	product.Attributes = attrs

	return &product, nil
}

// encodeAttributes renders the attribute map for storage.
//
// An absent map is stored as an empty object rather than NULL: the column is
// NOT NULL, and a caller reading a row never has to decide whether "no
// attributes" arrived as NULL, as '{}' or as an empty string.
func encodeAttributes(attrs map[string]string) ([]byte, error) {
	if len(attrs) == 0 {
		return []byte("{}"), nil
	}
	encoded, err := json.Marshal(attrs)
	if err != nil {
		return nil, fmt.Errorf("encode attributes: %w", err)
	}
	return encoded, nil
}

// decodeAttributes reverses it.
//
// An empty object decodes to nil rather than to an allocated empty map. proto3
// cannot distinguish an absent map from an empty one, the transport adapter
// already converges the two, and returning nil keeps a product that has been
// through storage byte-identical to one that has not — so a test comparing two
// products does not fail because one of them allocated a map.
func decodeAttributes(raw []byte) (map[string]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var attrs map[string]string
	if err := json.Unmarshal(raw, &attrs); err != nil {
		return nil, fmt.Errorf("stored attributes are not readable: %w", err)
	}
	if len(attrs) == 0 {
		return nil, nil
	}
	return attrs, nil
}

// uniqueIDs strips blanks and repeats, preserving the order they were asked for
// so that a caller's own ordering is not silently rearranged.
func uniqueIDs(ids []string) []string {
	unique := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if strings.TrimSpace(id) == "" {
			continue
		}
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	return unique
}
