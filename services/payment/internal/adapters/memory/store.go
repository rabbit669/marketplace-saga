package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/marketplace/services/payment/internal/domain"
)

// Store is an in-memory payment repository.
//
// It exists so the service and its saga integration can be exercised without
// PostgreSQL, and because the two uniqueness rules that make idempotency work —
// one payment per id, one payment per idempotency key — are business rules that
// belong to the repository contract rather than to SQL. The Postgres
// implementation enforces them with unique indexes; this one enforces them
// under a mutex, and both must satisfy the same tests.
//
// It is not durable and must not be used in production.
type Store struct {
	mu sync.RWMutex

	byID map[string]*domain.Payment
	// byKey is the uniqueness constraint that makes concurrent retries safe.
	byKey map[string]string
}

// New returns an empty store.
func New() *Store {
	return &Store{
		byID:  make(map[string]*domain.Payment),
		byKey: make(map[string]string),
	}
}

// Create implements domain.Repository.
//
// The key check and both map writes happen under one lock, so two concurrent
// creates with the same key cannot both succeed. An implementation that read
// first and wrote second would race under exactly the concurrent retries this
// method exists to make safe.
func (s *Store) Create(_ context.Context, p *domain.Payment) error {
	if p == nil {
		return fmt.Errorf("%w: payment is nil", domain.ErrInvalidPayment)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.byID[p.ID]; exists {
		return fmt.Errorf("%w: payment %s already exists", domain.ErrInvalidPayment, p.ID)
	}
	if _, taken := s.byKey[p.IdempotencyKey]; taken {
		return fmt.Errorf("%w: %s", domain.ErrDuplicateIdempotencyKey, p.IdempotencyKey)
	}

	cp := *p
	s.byID[p.ID] = &cp
	s.byKey[p.IdempotencyKey] = p.ID
	return nil
}

// GetByID implements domain.Repository.
func (s *Store) GetByID(_ context.Context, id string) (*domain.Payment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	p, ok := s.byID[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", domain.ErrPaymentNotFound, id)
	}
	return clone(p), nil
}

// GetByIdempotencyKey implements domain.Repository.
func (s *Store) GetByIdempotencyKey(_ context.Context, key string) (*domain.Payment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	id, ok := s.byKey[key]
	if !ok {
		return nil, fmt.Errorf("%w: no payment for idempotency key %s", domain.ErrPaymentNotFound, key)
	}
	p, ok := s.byID[id]
	if !ok {
		// The two maps disagreed, which means a bug in this store rather than a
		// missing payment; saying so plainly is more useful than NotFound.
		return nil, fmt.Errorf("%w: idempotency key %s points at unknown payment %s",
			domain.ErrInvalidPayment, key, id)
	}
	return clone(p), nil
}

// GetByOrderID implements domain.Repository.
//
// One payment per order is a property of this service rather than a constraint
// on the map, so the newest record wins if more than one exists. In practice a
// second authorize for the same order carries a different idempotency key and
// represents a genuine retry after a failure, so the latest is the useful one.
func (s *Store) GetByOrderID(_ context.Context, orderID string) (*domain.Payment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var newest *domain.Payment
	for _, p := range s.byID {
		if p.OrderID != orderID {
			continue
		}
		if newest == nil || p.CreatedAt.After(newest.CreatedAt) {
			newest = p
		}
	}
	if newest == nil {
		return nil, fmt.Errorf("%w: no payment for order %s", domain.ErrPaymentNotFound, orderID)
	}
	return clone(newest), nil
}

// Update implements domain.Repository.
func (s *Store) Update(_ context.Context, p *domain.Payment) error {
	if p == nil {
		return fmt.Errorf("%w: payment is nil", domain.ErrInvalidPayment)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.byID[p.ID]
	if !ok {
		return fmt.Errorf("%w: %s", domain.ErrPaymentNotFound, p.ID)
	}
	// The idempotency key is immutable: allowing it to change would silently
	// release one key and claim another, breaking the guarantee the key exists
	// to provide.
	if existing.IdempotencyKey != p.IdempotencyKey {
		return fmt.Errorf("%w: payment %s cannot change its idempotency key",
			domain.ErrInvalidPayment, p.ID)
	}

	cp := *p
	s.byID[p.ID] = &cp
	return nil
}

// Delete implements domain.Repository.
func (s *Store) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	p, ok := s.byID[id]
	if !ok {
		return nil
	}
	delete(s.byKey, p.IdempotencyKey)
	delete(s.byID, id)
	return nil
}

// Len returns how many payments are stored, for assertions in tests.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.byID)
}

// clone returns a copy so that a caller mutating the value it received cannot
// change stored state behind the store's back.
func clone(p *domain.Payment) *domain.Payment {
	cp := *p
	return &cp
}
