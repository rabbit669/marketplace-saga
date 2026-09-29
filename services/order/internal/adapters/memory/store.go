// Package memory provides in-process implementations of the order service's
// storage ports.
//
// They exist so the checkout path can run end to end without PostgreSQL, which
// keeps the interesting behaviour — the saga, the compensations, the
// idempotency claim — testable and demonstrable on their own. The PostgreSQL
// implementations satisfy the same interfaces, and the same tests run against
// both; that is what stops these from becoming a parallel truth.
//
// Nothing here is durable.
package memory

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/marketplace/services/order/internal/domain"
)

// OrderStore is an in-memory order repository.
type OrderStore struct {
	mu     sync.RWMutex
	orders map[string]*domain.Order
	// keys lives here rather than in a second store so that claiming a key and
	// writing its order can happen under one lock. Two stores would need two
	// locks, and no ordering of them makes the pair atomic.
	keys map[string]string

	// updates counts writes, so tests can assert that a saga step actually
	// persisted its result rather than only returning it.
	updates int
	// hits counts replays, so tests can prove a retry was recognised rather than
	// merely tolerated.
	hits int
}

// NewOrderStore returns an empty store.
func NewOrderStore() *OrderStore {
	return &OrderStore{
		orders: make(map[string]*domain.Order),
		keys:   make(map[string]string),
	}
}

// Create implements domain.Repository.
func (s *OrderStore) Create(_ context.Context, order *domain.Order) error {
	if order == nil {
		return fmt.Errorf("%w: order is nil", domain.ErrOrderNotFound)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.orders[order.ID]; exists {
		return fmt.Errorf("%w: %s", domain.ErrDuplicateOrder, order.ID)
	}
	s.orders[order.ID] = cloneOrder(order)
	return nil
}

// Get implements domain.Repository.
//
// The value is re-validated on the way out. A real repository cannot be trusted
// to hold only well-formed rows — a migration, a manual edit or a bug in an
// older writer can leave something that violates the current rules — so the
// check belongs on the read path where every consumer benefits from it.
func (s *OrderStore) Get(_ context.Context, id string) (*domain.Order, error) {
	s.mu.RLock()
	stored, ok := s.orders[id]
	s.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %s", domain.ErrOrderNotFound, id)
	}

	rebuilt, err := domain.Rehydrate(*cloneOrder(stored))
	if err != nil {
		return nil, fmt.Errorf("order %s: stored row is invalid: %w", id, err)
	}
	return rebuilt, nil
}

// Update implements domain.Repository.
func (s *OrderStore) Update(_ context.Context, order *domain.Order) error {
	if order == nil {
		return fmt.Errorf("%w: order is nil", domain.ErrOrderNotFound)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.orders[order.ID]; !ok {
		return fmt.Errorf("%w: %s", domain.ErrOrderNotFound, order.ID)
	}
	s.orders[order.ID] = cloneOrder(order)
	s.updates++
	return nil
}

// Len returns how many orders are stored.
func (s *OrderStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.orders)
}

// Clone returns a copy of a stored order, for assertions in tests.
func (s *OrderStore) Clone(id string) *domain.Order {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if o, ok := s.orders[id]; ok {
		return cloneOrder(o)
	}
	return nil
}

// UpdateCount returns how many successful writes have happened.
func (s *OrderStore) UpdateCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.updates
}

// CreateIdempotent implements domain.Repository.
//
// The key check and both writes share one critical section, which is what makes
// two concurrent checkouts with the same key safe. A read followed by a separate
// write would let both observe the key as free.
func (s *OrderStore) CreateIdempotent(_ context.Context, order *domain.Order, key string) (string, bool, error) {
	if order == nil {
		return "", false, fmt.Errorf("%w: order is nil", domain.ErrOrderNotFound)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.keys[key]; ok {
		s.hits++
		return existing, false, nil
	}
	if _, exists := s.orders[order.ID]; exists {
		return "", false, fmt.Errorf("%w: %s", domain.ErrDuplicateOrder, order.ID)
	}

	s.orders[order.ID] = cloneOrder(order)
	s.keys[key] = order.ID
	return order.ID, true, nil
}

// ReleaseIdempotencyKey implements domain.Repository.
func (s *OrderStore) ReleaseIdempotencyKey(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.keys, key)
	return nil
}

// Hits returns how many times a claim was recognised as a replay.
func (s *OrderStore) Hits() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hits
}

// cloneOrder deep-copies an order, including the slice it owns. Sharing the
// backing array would let a caller append to a stored order's lines, which is
// the kind of aliasing bug that survives every test that only reads.
func cloneOrder(o *domain.Order) *domain.Order {
	cp := *o
	cp.Lines = slices.Clone(o.Lines)
	return &cp
}
