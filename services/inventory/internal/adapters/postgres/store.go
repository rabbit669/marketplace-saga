// Package postgres is the inventory service's durable storage.
//
// It implements the domain's Repository against PostgreSQL. The interesting part
// is not the SQL; it is the locking, because the guarantee that matters here —
// stock is never sold twice — cannot be recovered from after the fact. A refund
// is a human apologising to a customer, so the store has to make overselling
// impossible rather than unlikely.
//
// # Why a row lock is enough
//
// The in-memory adapter holds one mutex across a two-phase check-then-apply, and
// that is the whole of its safety argument. The same argument is available here
// with row locks, because PostgreSQL holds a lock until the end of the
// transaction and gives every writer of a row the same lock:
//
//	begin
//	  select ... from stock_items where product_id = any($1)
//	    order by product_id for update          -- lock every row this request touches
//	  for every line: assert quantity - reserved >= quantity(line)   (phase 1, read)
//	  for every line: update stock_items set reserved = reserved + n
//	                  where product_id = $p and quantity - reserved >= n  (phase 2, write)
//	commit                                      -- locks released here
//
// Let free(p) = quantity(p) - reserved(p). Between phase 1 and phase 2 no other
// transaction can change free(p) for any product this request touches: it would
// have to write one of these rows, and writing one requires the very row lock
// this transaction holds. So the value phase 1 observed is still the value phase
// 2 acts on, and a request is only applied when the stock was sufficient at the
// moment of application. Every other mutation — commit, release, expiry —
// either lowers reserved(p) or lowers quantity(p) and reserved(p) together, so
// none of them can raise free(p) above what phase 1 saw. By induction the sum of
// the holds taken for a product never exceeds its quantity.
//
// The second half of that — the `quantity - reserved >= n` predicate on the
// update, with its affected-row count checked — is deliberate duplication. Under
// the lock it can only ever succeed; it is there so that the write carries its
// own precondition, and a future change that drops the lock degrades into a
// refused request rather than into an oversell.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/marketplace/services/inventory/internal/domain"
)

// options configures a Store.
type Options struct {
	// Clock supplies the current time. Injecting it keeps expiry a matter of
	// arithmetic in tests rather than of sleeping. Defaults to time.Now.
	Clock func() time.Time
	// DefaultTTL is applied when a reserve request does not ask for a TTL of its
	// own. Zero falls back to domain.DefaultReservationTTL.
	DefaultTTL time.Duration
}

// Store is the inventory service's PostgreSQL adapter.
type Store struct {
	pool *pgxpool.Pool

	clock      func() time.Time
	defaultTTL time.Duration
}

// New returns a store backed by the pool, using the system clock and the
// domain's default reservation TTL.
func New(pool *pgxpool.Pool) *Store { return NewWithOptions(pool, Options{}) }

// NewWithOptions returns a store with the supplied clock and TTL.
func NewWithOptions(pool *pgxpool.Pool, opts Options) *Store {
	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}
	return &Store{pool: pool, clock: clock, defaultTTL: opts.DefaultTTL}
}

// Pool exposes the pool so that health checks can use it.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Store must satisfy the port the rest of the service depends on.
var _ domain.Repository = (*Store)(nil)

// pool and transaction share just enough of the pgx API for one implementation
// of each read to serve both. The reserve path has to read inside a transaction
// — the idempotency check and the stock lock are the same decision — while the
// plain reads have no reason to open one.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// ------------------------------------------------------------------- stock

// GetStock returns the requested rows, in the order asked for.
//
// Unknown products are dropped rather than zero-filled: the proto promises
// "absent means never stocked", and an operator cannot tell an empty shelf from a
// product this service has never heard of if both come back as 0. Repeats are
// collapsed, and an empty request means every stocked product, which is what the
// admin view needs.
func (s *Store) GetStock(ctx context.Context, productIDs []string) ([]domain.StockItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if len(productIDs) == 0 {
		return s.queryStock(ctx, s.pool, `
			SELECT product_id, quantity, reserved, updated_at
			  FROM stock_items
			 ORDER BY product_id`)
	}

	ids := uniqueIDs(productIDs)
	items, err := s.queryStock(ctx, s.pool, `
		SELECT product_id, quantity, reserved, updated_at
		  FROM stock_items
		 WHERE product_id = ANY($1)`, ids)
	if err != nil {
		return nil, err
	}

	// Re-ordered into the order they were asked for. The query had to sort to be
	// deterministic, but a caller that asked for three products and got them
	// back in a different order has to sort its own request to line them up, and
	// that is a small tax paid on every call to save the store nothing.
	byID := make(map[string]domain.StockItem, len(items))
	for _, item := range items {
		byID[item.ProductID] = item
	}
	ordered := make([]domain.StockItem, 0, len(ids))
	for _, id := range ids {
		if item, ok := byID[id]; ok {
			ordered = append(ordered, item)
		}
	}
	return ordered, nil
}

// SetStock sets an absolute on-hand quantity and returns the resulting row.
//
// It is a single upsert rather than a read followed by an insert or an update:
// two admins setting the same product concurrently would otherwise both find no
// row, both insert, and leave one of them reporting an error about a product it
// had just created.
//
// Reserved is left alone, which is the whole point of listing only quantity in
// the update. An admin adjusting a stock count is describing the warehouse, not
// cancelling orders; the holds already granted were promises and are still
// honoured, even if the corrected count is lower than what is held. See the note
// on stock_items in migrations/0001_init.sql.
func (s *Store) SetStock(ctx context.Context, productID string, quantity int64) (domain.StockItem, error) {
	if err := ctx.Err(); err != nil {
		return domain.StockItem{}, err
	}

	now := s.clock()

	// Built through the domain so that the rules — a product id is required, a
	// quantity is not negative — are the same ones the in-memory adapter
	// applies, and so that a rejected write never reaches the database.
	item, err := domain.NewStockItem(productID, quantity, now)
	if err != nil {
		return domain.StockItem{}, err
	}

	row := s.pool.QueryRow(ctx, `
		INSERT INTO stock_items (product_id, quantity, reserved, updated_at)
		VALUES ($1, $2, 0, $3)
		ON CONFLICT (product_id) DO UPDATE
			SET quantity = EXCLUDED.quantity, updated_at = EXCLUDED.updated_at
		RETURNING product_id, quantity, reserved, updated_at`,
		item.ProductID, item.Quantity, item.UpdatedAt)

	stored, err := scanStockItem(row)
	if err != nil {
		return domain.StockItem{}, fmt.Errorf("set stock for %s: %w", productID, err)
	}
	return stored, nil
}

// --------------------------------------------------------- reservations

// Reserve holds stock, idempotently and all-or-nothing.
//
// # Idempotency
//
// The reservation id is claimed by inserting the reservation row. A unique
// violation on the primary key means another transaction already owns the id,
// and — because that transaction only becomes visible once it commits — the
// claim is proof that its hold is committed too. The caller is answered with the
// stored reservation and AlreadyExisted=true. That is what makes a saga retry
// safe: the second attempt returns the original hold instead of taking a second
// set of units, which would silently halve the catalogue.
//
// The claim happens before any stock is examined, and that ordering is the
// point. A replay must succeed with the original answer even if the catalogue
// has moved on since: the units it holds are already spoken for, and re-checking
// them against today's availability would fail a reservation that is perfectly
// valid.
//
// # Why the lock is where it is
//
// Take the request's products in ascending id order, lock every one of them with
// SELECT ... FOR UPDATE, and hold those locks until this transaction ends — that
// is, until after the holds have been written and the transaction commits. The
// ascending order is not cosmetic: two requests that overlap on two products
// take their locks in the same order, so neither can be holding what the other
// waits for, and the deadlock PostgreSQL would otherwise have to break up cannot
// form. The lock is what makes phase 1 and phase 2 one decision instead of two —
// the argument is written out in the package comment.
func (s *Store) Reserve(ctx context.Context, req domain.ReserveRequest) (domain.ReserveResult, error) {
	if err := ctx.Err(); err != nil {
		return domain.ReserveResult{}, err
	}

	ttl := req.TTL
	if ttl <= 0 {
		// A zero here is a deliberate "use the service default"; the domain
		// applies DefaultReservationTTL if this store has none configured.
		ttl = s.defaultTTL
	}

	now := s.clock()

	pending, err := domain.NewReservation(domain.NewReservationInput{
		ID:      req.ReservationID,
		OrderID: req.OrderID,
		Lines:   req.Lines,
		TTL:     ttl,
		Now:     now,
	})
	if err != nil {
		return domain.ReserveResult{}, err
	}

	// NewReservation has already merged repeated products, so these lines are
	// already one per product. The aggregation is repeated anyway, because the
	// check-then-hold sequence below is this service's entire safety property and
	// it must not rest on a guarantee made in another layer: two lines of the
	// same product checked separately would each pass against the same free
	// units.
	holds := aggregate(pending.Lines)

	var result domain.ReserveResult

	err = s.inTx(ctx, func(tx pgx.Tx) error {
		switch existing, err := loadReservation(ctx, tx, pending.ID); {
		case err == nil:
			// The ordinary replay: no insert is attempted, so no transaction is
			// aborted and the answer costs one read.
			result = domain.ReserveResult{Reservation: *existing, AlreadyExisted: true}
			return nil
		case !errors.Is(err, domain.ErrReservationNotFound):
			return err
		}

		if err := insertReservation(ctx, tx, pending); err != nil {
			return err
		}

		stock, err := lockStock(ctx, tx, holds)
		if err != nil {
			return err
		}

		// Phase 1: check everything, write nothing. A failure here rolls the
		// whole transaction back, so a request that cannot be satisfied in full
		// holds none of its lines. Holding what fits would be worse than
		// failing: the saga would compensate a reservation it believes failed,
		// and the leftover holds would sit on the shelf until their TTL expired.
		for _, line := range holds {
			item, ok := stock[line.ProductID]
			if !ok {
				return fmt.Errorf("%w: product %s", domain.ErrStockNotFound, line.ProductID)
			}
			if err := item.CheckAvailable(line.Quantity); err != nil {
				return err
			}
		}

		// Phase 2: apply. Every check above passed while these rows were locked,
		// so nothing can fail here and the request is atomic by construction.
		for _, line := range holds {
			tag, err := tx.Exec(ctx, `
				UPDATE stock_items
				   SET reserved = reserved + $2, updated_at = $3
				 WHERE product_id = $1
				   AND quantity - reserved >= $2`,
				line.ProductID, line.Quantity, now)
			if err != nil {
				return fmt.Errorf("hold stock for %s: %w", line.ProductID, err)
			}
			if tag.RowsAffected() == 0 {
				// Unreachable while the lock is held — see the package comment.
				// It is reported rather than retried because reaching it means
				// the precondition above is not the one that was checked, and
				// guessing which of the two is right is how stock gets sold
				// twice.
				return fmt.Errorf("%w: product %s: could not hold %d units",
					domain.ErrInsufficientStock, line.ProductID, line.Quantity)
			}
		}

		result = domain.ReserveResult{Reservation: pending.Clone()}
		return nil
	})
	if err != nil {
		// A concurrent replay of the same id: the other transaction committed
		// the claim, which is exactly why this insert was rejected. Its row is
		// visible now, so the answer is the stored reservation — the same answer
		// the ordinary replay above returns.
		if errors.Is(err, errReservationClaimed) {
			existing, readErr := s.GetReservation(ctx, pending.ID)
			if readErr != nil {
				return domain.ReserveResult{}, readErr
			}
			return domain.ReserveResult{Reservation: existing, AlreadyExisted: true}, nil
		}
		return domain.ReserveResult{}, err
	}
	return result, nil
}

// Commit deducts held stock permanently.
//
// The reservation row is locked for the length of the decision, so two
// concurrent commits of the same hold cannot both read "active": the second
// waits, then reads "committed" and returns the idempotent answer. The deduction
// happens exactly once, which is what stops the saga's retried commit step from
// selling the same units twice.
//
// An overdue hold is expired here, before the decision, rather than left to the
// worker. The worker is a sweep on a timer; if it is down, or the process
// restarted between ticks, a hold can be past its deadline while still marked
// active, and its units must not be sellable twice.
func (s *Store) Commit(ctx context.Context, reservationID string) (domain.Reservation, error) {
	if err := ctx.Err(); err != nil {
		return domain.Reservation{}, err
	}

	now := s.clock()

	var (
		out     domain.Reservation
		refused error
	)

	err := s.inTx(ctx, func(tx pgx.Tx) error {
		res, err := lockReservation(ctx, tx, reservationID)
		if err != nil {
			return err
		}

		if _, err := s.expireIfDue(ctx, tx, res, now); err != nil {
			return err
		}

		applied, err := res.Commit(now)
		if err != nil {
			// A refusal after expireIfDue has moved the hold on is not a reason
			// to undo that move. Expiring it returned its units to the pool and
			// recorded the transition; the caller is about to be told the
			// commit failed and must not also be handed back a hold the
			// database still believes is active — a later retry would then find
			// it active, expire it again, and return the same units twice. So
			// the transaction is allowed to commit and the error is returned
			// once it has.
			refused = err
			return nil
		}

		// applied is false only for a replay of an already-committed
		// reservation. Deducting again there would sell the same units twice.
		if applied {
			if err := deductHolds(ctx, tx, res.Lines, now); err != nil {
				return err
			}
			if err := updateReservationState(ctx, tx, res); err != nil {
				return err
			}
		}

		out = res.Clone()
		return nil
	})
	if err != nil {
		return domain.Reservation{}, err
	}
	if refused != nil {
		return domain.Reservation{}, refused
	}
	return out, nil
}

// Release gives a hold back to the pool. It is the saga's compensation.
//
// The three "nothing to do" cases — unknown id, already released, already
// expired — all succeed. A compensation runs precisely when the caller cannot
// know whether the forward step applied, so refusing to compensate an unknown
// reservation would strand the saga on the one path it most needs to work. The
// result's Released flag is the honest answer to "did I return anything".
//
// A committed reservation is the exception and fails: those units are sold, and
// returning them would make it possible to oversell retroactively.
func (s *Store) Release(ctx context.Context, reservationID, reason string) (domain.ReleaseResult, error) {
	if err := ctx.Err(); err != nil {
		return domain.ReleaseResult{}, err
	}

	now := s.clock()

	var out domain.ReleaseResult

	err := s.inTx(ctx, func(tx pgx.Tx) error {
		res, err := lockReservation(ctx, tx, reservationID)
		if errors.Is(err, domain.ErrReservationNotFound) {
			// Nothing was ever held under this id. That is a success with
			// nothing to undo, which is what a compensation wants to hear.
			out = domain.ReleaseResult{Reservation: nil, Released: false}
			return nil
		}
		if err != nil {
			return err
		}

		if _, err := s.expireIfDue(ctx, tx, res, now); err != nil {
			return err
		}

		applied, err := res.Release(reason, now)
		if err != nil {
			return err
		}

		if applied {
			if err := releaseHolds(ctx, tx, res.Lines, now); err != nil {
				return err
			}
			if err := updateReservationState(ctx, tx, res); err != nil {
				return err
			}
		}

		released := res.Clone()
		out = domain.ReleaseResult{Reservation: &released, Released: applied}
		return nil
	})
	if err != nil {
		return domain.ReleaseResult{}, err
	}
	return out, nil
}

// GetReservation returns a reservation by id.
//
// It is a read, so it does not expire anything, and the status it reports is the
// recorded one: a hold past its deadline but not yet swept reads as active with
// an ExpiresAt in the past. Callers that need the effective state can see it from
// that timestamp, and inventing a transition inside a read would make GET answer
// differently depending on which writer happened to run first.
func (s *Store) GetReservation(ctx context.Context, reservationID string) (domain.Reservation, error) {
	if err := ctx.Err(); err != nil {
		return domain.Reservation{}, err
	}

	res, err := readReservation(ctx, s.pool, reservationID)
	if err != nil {
		return domain.Reservation{}, err
	}
	return *res, nil
}

// ExpireReservations returns the units of every overdue hold to the pool.
//
// # Why this is one statement
//
// The transition and the stock it returns are a single SQL statement, and that
// is what makes them impossible to separate. One statement sees one snapshot:
// the set of holds that are due, the flip of their status, and the decrement of
// the stock they held all read the same rows at the same instant, so there is no
// window in which a hold has been marked expired but its units are still counted
// as held — or, far worse, in which its units have been returned but its status
// still says active and a second sweep could return them again.
//
// # Why it cannot double-return, and cannot race a reserve
//
// The holds to expire are selected with FOR UPDATE SKIP LOCKED. A hold another
// sweep is already working on is either locked — in which case this sweep skips
// it and leaves it alone, rather than blocking behind it — or already expired,
// in which case the status predicate excludes it. Only the rows this statement
// flipped can contribute units to the stock update, because the update joins the
// rows the flip returned: the status transition is the guard, and a row can only
// be transitioned once.
//
// A reserve running at the same time cannot be affected at all. A hold is due
// only if expires_at <= now, and a hold is created with a positive TTL, so a
// hold that a concurrent reserve has just taken is never due. A hold whose
// transaction has not committed is not visible to this statement at all — and if
// it commits after this snapshot, a later sweep sees it with its full lifetime
// ahead of it. In the other direction, the stock update takes row locks on the
// stock rows it touches, so a concurrent reserve on the same product blocks
// until this statement commits and then reads the reduced reserved count. It
// cannot hold units this sweep returned a moment earlier.
//
// The expiry is kept rather than deleted, for the same reason the in-memory
// adapter keeps it: the saga that owns a hold may still ask about it and must be
// told "expired" rather than "unknown", and the id must stay claimed so that a
// late retry cannot re-reserve it. Retention is a deployment decision; in a
// database it is a scheduled DELETE rather than the process lifetime.
func (s *Store) ExpireReservations(ctx context.Context, now time.Time) ([]domain.Reservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	rows, err := s.pool.Query(ctx, `
		WITH due AS (
			SELECT r.id
			  FROM reservations r
			 WHERE r.status = 'active'
			   AND r.expires_at <= $1
			 ORDER BY r.id
			   FOR UPDATE SKIP LOCKED
		),
		expired AS (
			UPDATE reservations r
			   SET status = 'expired', updated_at = $1
			  FROM due
			 WHERE r.id = due.id
			RETURNING r.id, r.order_id, r.status, r.release_reason,
			          r.created_at, r.expires_at, r.updated_at
		),
		-- The units are summed per product before the stock row is touched, and
		-- that aggregation is not an optimisation. A stock row joins to one line
		-- of every reservation being expired, and an UPDATE updates a target row
		-- at most once per statement however many source rows match it — so
		-- decrementing per line would return one hold's units and silently lose
		-- every other hold on the same product, leaving stock reserved that no
		-- reservation is holding.
		returned AS (
			UPDATE stock_items s
			   SET reserved = GREATEST(s.reserved - returned_units.units, 0),
			       updated_at = $1
			  FROM (
					SELECT l.product_id, sum(l.quantity) AS units
					  FROM reservation_lines l
					  JOIN expired e ON e.id = l.reservation_id
					 GROUP BY l.product_id
			  ) AS returned_units
			 WHERE s.product_id = returned_units.product_id
			RETURNING s.product_id
		)
		SELECT e.id, e.order_id, e.status, e.release_reason, e.created_at, e.expires_at, e.updated_at,
		       (SELECT json_agg(json_build_object('product_id', l.product_id, 'quantity', l.quantity)
		                        ORDER BY l.line_no)
		          FROM reservation_lines l
		         WHERE l.reservation_id = e.id) AS lines
		  FROM expired e
		 ORDER BY e.id`, now)
	if err != nil {
		return nil, fmt.Errorf("expire reservations: %w", err)
	}
	defer rows.Close()

	var expired []domain.Reservation
	for rows.Next() {
		res, err := scanExpiredReservation(rows)
		if err != nil {
			return nil, err
		}
		expired = append(expired, *res)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("expire reservations: %w", err)
	}
	return expired, nil
}

// expireIfDue transitions an overdue hold to expired and persists it.
//
// It is called from both of the mutating paths as well as from the sweep, so
// that the service's answer never depends on the worker being alive: a hold's
// deadline is a property of the hold, not of the timer that notices it.
func (s *Store) expireIfDue(ctx context.Context, tx pgx.Tx, res *domain.Reservation, now time.Time) (bool, error) {
	if !res.Expire(now) {
		return false, nil
	}

	if err := releaseHolds(ctx, tx, res.Lines, now); err != nil {
		return false, err
	}
	if err := updateReservationState(ctx, tx, res); err != nil {
		return false, err
	}
	return true, nil
}

// ----------------------------------------------------------------- helpers

// errReservationClaimed reports that another transaction owns this reservation
// id. It never leaves the package: the caller turns it back into the stored
// reservation, which is what a replay is answered with.
var errReservationClaimed = errors.New("reservation id is already claimed")

// inTx runs fn in a transaction, rolling back on error or panic.
func (s *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// lockStock takes the row locks the reserve decision rests on and returns what
// it read.
//
// The ORDER BY is load-bearing. PostgreSQL applies the row locks to the sorted
// output, so every transaction here takes the same rows in the same order; two
// requests that overlap on two products therefore cannot each hold what the
// other is waiting for, which is the classic way two-phase locking deadlocks.
func lockStock(ctx context.Context, tx pgx.Tx, lines []domain.ReservationLine) (map[string]domain.StockItem, error) {
	ids := make([]string, 0, len(lines))
	for _, line := range lines {
		ids = append(ids, line.ProductID)
	}

	rows, err := tx.Query(ctx, `
		SELECT product_id, quantity, reserved, updated_at
		  FROM stock_items
		 WHERE product_id = ANY($1)
		 ORDER BY product_id
		   FOR UPDATE`, ids)
	if err != nil {
		return nil, fmt.Errorf("lock %d stock rows: %w", len(ids), err)
	}
	defer rows.Close()

	locked := make(map[string]domain.StockItem, len(ids))
	for rows.Next() {
		item, err := scanStockItem(rows)
		if err != nil {
			return nil, fmt.Errorf("scan locked stock row: %w", err)
		}
		locked[item.ProductID] = item
	}
	return locked, rows.Err()
}

// insertReservation writes the reservation and its lines.
//
// It is the claim on the id: the primary key rejects a second writer, and that
// rejection — not a preceding SELECT — is what makes the claim atomic. Writing
// the lines in the same transaction is what makes the hold all-or-nothing: a
// reservation without its lines would be a promise with no contents, and every
// later release of it would return nothing.
func insertReservation(ctx context.Context, tx pgx.Tx, res *domain.Reservation) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO reservations (id, order_id, status, release_reason, created_at, expires_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		res.ID, res.OrderID, string(res.Status), res.ReleaseReason,
		res.CreatedAt, res.ExpiresAt, res.UpdatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return errReservationClaimed
		}
		return fmt.Errorf("insert reservation %s: %w", res.ID, err)
	}

	for i, line := range res.Lines {
		if _, err := tx.Exec(ctx, `
			INSERT INTO reservation_lines (reservation_id, line_no, product_id, quantity)
			VALUES ($1, $2, $3, $4)`,
			res.ID, i, line.ProductID, line.Quantity); err != nil {
			return fmt.Errorf("insert reservation line %d: %w", i, err)
		}
	}
	return nil
}

// lockReservation reads a reservation and locks it for the rest of the
// transaction, so that the decision the caller is about to take cannot be taken
// concurrently by anyone else.
//
// The reservation is locked before any stock row is, and every path here does
// the same, which is the ordering that keeps commit and release from deadlocking
// against each other.
func lockReservation(ctx context.Context, tx pgx.Tx, id string) (*domain.Reservation, error) {
	return readReservationRow(ctx, tx, id, true)
}

// loadReservation reads a reservation without locking it. It is used for the
// idempotency check, which only needs to know whether the id is taken.
func loadReservation(ctx context.Context, tx pgx.Tx, id string) (*domain.Reservation, error) {
	return readReservationRow(ctx, tx, id, false)
}

// readReservation reads a reservation and its lines, optionally locking the
// reservation row.
func readReservation(ctx context.Context, q querier, id string) (*domain.Reservation, error) {
	return readReservationRow(ctx, q, id, false)
}

func readReservationRow(ctx context.Context, q querier, id string, lock bool) (*domain.Reservation, error) {
	query := `
		SELECT id, order_id, status, release_reason, created_at, expires_at, updated_at
		  FROM reservations
		 WHERE id = $1`
	if lock {
		query += ` FOR UPDATE`
	}

	row := q.QueryRow(ctx, query, id)
	res, err := scanReservationRow(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", domain.ErrReservationNotFound, id)
		}
		return nil, fmt.Errorf("select reservation %s: %w", id, err)
	}

	lines, err := loadLines(ctx, q, []string{id})
	if err != nil {
		return nil, err
	}
	return rehydrate(res, lines[id])
}

// loadLines reads the lines of the given reservations, grouped by reservation.
func loadLines(ctx context.Context, q querier, ids []string) (map[string][]domain.ReservationLine, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := q.Query(ctx, `
		SELECT reservation_id, product_id, quantity
		  FROM reservation_lines
		 WHERE reservation_id = ANY($1)
		 ORDER BY reservation_id, line_no`, ids)
	if err != nil {
		return nil, fmt.Errorf("select reservation lines: %w", err)
	}
	defer rows.Close()

	lines := make(map[string][]domain.ReservationLine, len(ids))
	for rows.Next() {
		var (
			reservationID string
			line          domain.ReservationLine
		)
		if err := rows.Scan(&reservationID, &line.ProductID, &line.Quantity); err != nil {
			return nil, fmt.Errorf("scan reservation line: %w", err)
		}
		lines[reservationID] = append(lines[reservationID], line)
	}
	return lines, rows.Err()
}

// updateReservationState persists a reservation's status, its release reason and
// its timestamp. It is written once, from the row the caller is holding locked,
// so the values it writes are the ones the state machine produced.
func updateReservationState(ctx context.Context, tx pgx.Tx, res *domain.Reservation) error {
	if _, err := tx.Exec(ctx, `
		UPDATE reservations
		   SET status = $2, release_reason = $3, updated_at = $4
		 WHERE id = $1`,
		res.ID, string(res.Status), res.ReleaseReason, res.UpdatedAt); err != nil {
		return fmt.Errorf("update reservation %s: %w", res.ID, err)
	}
	return nil
}

// releaseHolds returns held units to the pool.
//
// The floor at zero mirrors the domain's ReleaseHold: a release that overshoots
// must not invent negative promises, which would make availability larger than
// the quantity on hand. The rows are visited in ascending product id order so
// that this path takes its locks in the same order reserve does.
func releaseHolds(ctx context.Context, tx pgx.Tx, lines []domain.ReservationLine, now time.Time) error {
	for _, line := range orderedLines(lines) {
		if _, err := tx.Exec(ctx, `
			UPDATE stock_items
			   SET reserved = GREATEST(reserved - $2, 0), updated_at = $3
			 WHERE product_id = $1`,
			line.ProductID, line.Quantity, now); err != nil {
			return fmt.Errorf("release %d units of %s: %w", line.Quantity, line.ProductID, err)
		}
	}
	return nil
}

// deductHolds converts held units into a permanent deduction: the goods have
// left the warehouse.
//
// Both counters move together, which is what keeps the invariant meaningful —
// after a commit the units are neither on hand nor promised, they are sold. The
// floor at zero covers the operator-error case described on SetStock, where the
// on-hand count was already corrected below what had been promised.
func deductHolds(ctx context.Context, tx pgx.Tx, lines []domain.ReservationLine, now time.Time) error {
	for _, line := range orderedLines(lines) {
		if _, err := tx.Exec(ctx, `
			UPDATE stock_items
			   SET quantity = GREATEST(quantity - $2, 0),
			       reserved = GREATEST(reserved - $2, 0),
			       updated_at = $3
			 WHERE product_id = $1`,
			line.ProductID, line.Quantity, now); err != nil {
			return fmt.Errorf("commit %d units of %s: %w", line.Quantity, line.ProductID, err)
		}
	}
	return nil
}

// queryStock runs a stock query that returns rows in the shape scanStockItem
// expects.
func (s *Store) queryStock(ctx context.Context, q querier, query string, args ...any) ([]domain.StockItem, error) {
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select stock: %w", err)
	}
	defer rows.Close()

	items := make([]domain.StockItem, 0, 8)
	for rows.Next() {
		item, err := scanStockItem(rows)
		if err != nil {
			return nil, fmt.Errorf("scan stock row: %w", err)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// rowScanner is satisfied by a pool row, a transaction row and an open result
// set, so that one scan function serves every read.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanStockItem(row rowScanner) (domain.StockItem, error) {
	var item domain.StockItem
	if err := row.Scan(&item.ProductID, &item.Quantity, &item.Reserved, &item.UpdatedAt); err != nil {
		return domain.StockItem{}, err
	}
	// The row's own invariants — a non-negative quantity, a non-negative held
	// count — are check constraints on the table, so they hold for anything this
	// query can return. What is deliberately not enforced anywhere is
	// reserved <= quantity; see the note in migrations/0001_init.sql. Available()
	// floors the difference at zero, which is why a caller can trust it.
	return item, nil
}

func scanReservationRow(row rowScanner) (*domain.Reservation, error) {
	var (
		res    domain.Reservation
		status string
	)
	if err := row.Scan(&res.ID, &res.OrderID, &status, &res.ReleaseReason,
		&res.CreatedAt, &res.ExpiresAt, &res.UpdatedAt); err != nil {
		return nil, err
	}
	res.Status = domain.ReservationStatus(status)
	return &res, nil
}

// scanExpiredReservation reads a row of the expiry statement, whose lines arrive
// as JSON from the same statement that expired them.
func scanExpiredReservation(row rowScanner) (*domain.Reservation, error) {
	var (
		res      domain.Reservation
		status   string
		linesRaw []byte
	)
	if err := row.Scan(&res.ID, &res.OrderID, &status, &res.ReleaseReason,
		&res.CreatedAt, &res.ExpiresAt, &res.UpdatedAt, &linesRaw); err != nil {
		return nil, fmt.Errorf("scan expired reservation: %w", err)
	}
	res.Status = domain.ReservationStatus(status)

	// Decoded through an explicit shape rather than straight into a domain type:
	// the JSON keys are the column names, and relying on Go's case-insensitive
	// field matching would quietly produce lines with an empty product id.
	var raw []struct {
		ProductID string `json:"product_id"`
		Quantity  int64  `json:"quantity"`
	}
	if err := json.Unmarshal(linesRaw, &raw); err != nil {
		return nil, fmt.Errorf("reservation %s: stored lines are not readable: %w", res.ID, err)
	}

	lines := make([]domain.ReservationLine, 0, len(raw))
	for _, line := range raw {
		lines = append(lines, domain.ReservationLine{ProductID: line.ProductID, Quantity: line.Quantity})
	}
	return rehydrate(&res, lines)
}

// rehydrate runs the domain's validation over a reservation read from storage.
//
// A stored reservation is expected to have moved on and to have been created
// under whatever bounds applied at the time, which is what Rehydrate allows for
// and NewReservation does not. Catching a row that violates the rules here turns
// silent corruption into an error at the point it is introduced.
func rehydrate(res *domain.Reservation, lines []domain.ReservationLine) (*domain.Reservation, error) {
	res.Lines = lines

	rebuilt, err := domain.Rehydrate(*res)
	if err != nil {
		return nil, fmt.Errorf("reservation %s: stored row is invalid: %w", res.ID, err)
	}
	return rebuilt, nil
}

// isUniqueViolation reports whether err is a PostgreSQL unique constraint
// violation (SQLSTATE 23505).
//
// It checks the code rather than matching on message text, which changes between
// server versions and locales.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// orderedLines returns a copy of lines sorted by product id, which is the order
// every path here takes its stock row locks in.
func orderedLines(lines []domain.ReservationLine) []domain.ReservationLine {
	ordered := make([]domain.ReservationLine, len(lines))
	copy(ordered, lines)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ProductID < ordered[j].ProductID })
	return ordered
}

// uniqueIDs strips blanks and repeats, preserving the order they were asked for.
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

// aggregate sums repeated products into one line, preserving first-seen order.
//
// NewReservation already guarantees distinct products, so this is a no-op on
// every request that reaches the store through the normal path. It is repeated
// here because phase 1 must check the total a request wants per product, and
// relying on an invariant enforced in another package for the check that
// prevents overselling would make the guarantee only as strong as the weakest
// caller.
func aggregate(lines []domain.ReservationLine) []domain.ReservationLine {
	sums := make([]domain.ReservationLine, 0, len(lines))
	index := make(map[string]int, len(lines))

	for _, line := range lines {
		if at, seen := index[line.ProductID]; seen {
			sums[at].Quantity += line.Quantity
			continue
		}
		index[line.ProductID] = len(sums)
		sums = append(sums, line)
	}
	return sums
}
