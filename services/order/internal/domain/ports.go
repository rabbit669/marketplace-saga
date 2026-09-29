package domain

import (
	"context"
	"time"

	"github.com/marketplace/pkg/money"
)

// The interfaces in this file are the order service's ports. The domain defines
// what it needs; adapters (gRPC clients, PostgreSQL repositories) provide it.
//
// The dependency therefore points inward: `adapters` imports `domain`, never the
// reverse. That is what makes the checkout saga testable with fakes, and it is
// not a stylistic preference — the rollback path is the part of this system most
// likely to be wrong, and being able to test it without a cluster is what makes
// testing it realistic.

// Product is the catalogue's view of a sellable item, as the order service
// needs it.
//
// This is deliberately not the catalogue service's `Product` message. A domain
// type that mirrors another service's wire type couples the two schemas
// together and turns their field renames into our compile errors.
type Product struct {
	ID       string
	SKU      string
	Name     string
	Price    money.Money
	IsActive bool
}

// CatalogClient prices a cart.
type CatalogClient interface {
	// GetProducts returns the requested products, keyed by id. Products that do
	// not exist are absent from the map rather than zero-valued, so the caller
	// can tell "missing" from "free".
	GetProducts(ctx context.Context, productIDs []string) (map[string]Product, error)
}

// ReserveLine is one product and quantity to hold.
type ReserveLine struct {
	ProductID string
	Quantity  int64
}

// ReserveRequest asks the inventory service to hold stock.
type ReserveRequest struct {
	// ReservationID is chosen by the caller and must be deterministic for a
	// given order — the order service passes the order id.
	//
	// This is what makes reserving idempotent under a saga retry: a reservation
	// that already exists is returned rather than taken twice. Without it, a
	// retry after a timeout would hold double the stock and the customer would
	// silently be unable to buy anything else.
	ReservationID string
	OrderID       string
	Lines         []ReserveLine
	// TTL bounds how long the hold survives if the saga never finishes. It is
	// the safety net for the case the recovery worker cannot fix.
	TTL time.Duration
}

// ReserveResult reports the outcome of a reservation.
type ReserveResult struct {
	ReservationID string
	// AlreadyExisted distinguishes a replay from a first execution. The saga
	// logs it, and a compensation treats both identically.
	AlreadyExisted bool
	ExpiresAt      time.Time
}

// InventoryClient manages the stock reservation lifecycle.
type InventoryClient interface {
	// Reserve holds stock. It must be idempotent on ReservationID.
	Reserve(ctx context.Context, req ReserveRequest) (ReserveResult, error)

	// Commit turns a hold into a permanent deduction. It is not compensatable:
	// once stock is committed the goods are effectively sold.
	Commit(ctx context.Context, reservationID string) error

	// Release returns a hold to the pool. It must be idempotent and must not
	// fail when the reservation was never created or was already released —
	// a compensation runs precisely when the forward step's outcome is unknown.
	Release(ctx context.Context, reservationID, reason string) error
}

// PaymentStatus mirrors the provider's states that the order service acts on.
type PaymentStatus string

const (
	PaymentAuthorized PaymentStatus = "authorized"
	PaymentCaptured   PaymentStatus = "captured"
	PaymentDeclined   PaymentStatus = "declined"
)

// AuthorizeRequest asks the provider to hold funds.
type AuthorizeRequest struct {
	OrderID string
	Amount  money.Money
	// IdempotencyKey makes the call safe to retry. Providers require this
	// because a timeout is not a failure — it is an unknown, and retrying
	// without a key charges the customer twice.
	IdempotencyKey     string
	PaymentMethodToken string
}

// AuthorizeResult reports the provider's decision.
type AuthorizeResult struct {
	PaymentID      string
	Status         PaymentStatus
	ProviderRef    string
	DeclinedReason string
	// Retryable reports whether the decline is worth another attempt. A
	// provider timeout might succeed in a second; "insufficient funds" will not,
	// and retrying it multiplies load while the customer waits.
	Retryable bool
}

// PaymentClient is the adapter for the payment provider.
type PaymentClient interface {
	// Authorize holds funds. It must be idempotent on IdempotencyKey.
	Authorize(ctx context.Context, req AuthorizeRequest) (AuthorizeResult, error)

	// Capture takes the held funds. This is the point of no return, which is
	// why it is the last step the saga can reach and why it has no compensation:
	// undoing it is a refund, a different business operation with customer-visible
	// consequences and its own rules.
	Capture(ctx context.Context, paymentID, idempotencyKey string) error

	// Void cancels a hold before capture. It must be idempotent and must
	// succeed when there is nothing to void.
	Void(ctx context.Context, paymentID, reason string) error
}

// Repository persists orders.
//
// Every method takes a context so that a caller's deadline and cancellation
// propagate to the database, which is what stops a slow query outliving the
// request that needed it.
type Repository interface {
	// Create stores a new order. It must fail if the id already exists.
	Create(ctx context.Context, order *Order) error

	// Get loads an order by id. It returns ErrOrderNotFound when absent.
	Get(ctx context.Context, id string) (*Order, error)

	// Update saves a changed order. Implementations should use optimistic
	// concurrency so that two saga workers cannot interleave writes.
	Update(ctx context.Context, order *Order) error

	// CreateIdempotent stores the order and claims the idempotency key, or does
	// neither, and reports which happened.
	//
	// They must be one atomic step. Claiming the key first and creating the
	// order afterwards leaves a window in which a concurrent retry sees the key
	// taken but finds no order — and turns a legitimate retry into a "not
	// found". Creating the order first is worse still: two callers both insert,
	// and the customer is charged twice.
	//
	// `created` false means another call already claimed the key, and
	// existingOrderID names the order it produced. The caller replays that
	// order instead of proceeding.
	CreateIdempotent(ctx context.Context, order *Order, key string) (existingOrderID string, created bool, err error)

	// ReleaseIdempotencyKey gives up a claim whose work failed permanently, so
	// that a corrected retry with the same key is not mistaken for a duplicate.
	ReleaseIdempotencyKey(ctx context.Context, key string) error
}

// Event is a domain event destined for the outbox.
type Event struct {
	// ID is unique per event and lets consumers deduplicate, because delivery
	// is at-least-once and duplicates are expected rather than exceptional.
	ID string
	// Type names the event, e.g. "order.confirmed".
	Type string
	// AggregateID is the id of the entity the event is about. It is used as the
	// Kafka partition key, which preserves per-order ordering.
	AggregateID string
	// Payload is the serialised event body.
	Payload    []byte
	OccurredAt time.Time
	// TraceParent is the W3C traceparent of the request that produced this
	// event, captured here rather than when it is published.
	//
	// The outbox poller runs seconds later, on its own goroutine, with no
	// relationship to the checkout that created the event. Injecting the trace
	// context at publish time would therefore attach the event to the poller and
	// break the causal chain a trace exists to show.
	TraceParent string
}

// EventPublisher records a domain event.
//
// Implementations must write the event in the *same transaction* as the state
// change that produced it. This is the transactional outbox: publishing to a
// broker directly would leave a window where the database committed and the
// message did not, producing an order nobody is ever notified about.
type EventPublisher interface {
	Publish(ctx context.Context, event Event) error
}
