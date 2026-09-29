package saga

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MemoryStore is an in-process Store.
//
// It exists so that saga behaviour can be tested without a database: the engine
// depends only on the Store interface, and every property worth testing —
// resume after a crash, compensation ordering, version conflicts — is about the
// engine's logic, not about SQL. It is also useful for local development.
//
// It is not durable and must not be used in production.
type MemoryStore struct {
	mu        sync.RWMutex
	instances map[string]*Instance
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{instances: make(map[string]*Instance)}
}

// Create implements Store.
func (m *MemoryStore) Create(_ context.Context, inst *Instance) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.instances[inst.ID]; exists {
		return ErrAlreadyExists
	}

	stored := inst.Clone()
	stored.Version = 1
	m.instances[inst.ID] = stored

	inst.Version = stored.Version
	return nil
}

// Get implements Store.
func (m *MemoryStore) Get(_ context.Context, id string) (*Instance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	inst, ok := m.instances[id]
	if !ok {
		return nil, ErrNotFound
	}
	return inst.Clone(), nil
}

// Save implements Store, enforcing optimistic concurrency.
func (m *MemoryStore) Save(_ context.Context, inst *Instance) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	current, ok := m.instances[inst.ID]
	if !ok {
		return ErrNotFound
	}
	if current.Version != inst.Version {
		return ErrVersionConflict
	}

	stored := inst.Clone()
	stored.Version = current.Version + 1
	m.instances[inst.ID] = stored

	inst.Version = stored.Version
	return nil
}

// ListStuck implements Store.
func (m *MemoryStore) ListStuck(_ context.Context, olderThan time.Time, limit int) ([]*Instance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stuck := make([]*Instance, 0, len(m.instances))
	for _, inst := range m.instances {
		if inst.Status.Terminal() {
			continue
		}
		if inst.UpdatedAt.Before(olderThan) {
			stuck = append(stuck, inst.Clone())
		}
	}

	// Oldest first: the sagas that have been stuck longest are the most urgent.
	sort.Slice(stuck, func(i, j int) bool {
		return stuck[i].UpdatedAt.Before(stuck[j].UpdatedAt)
	})

	if limit > 0 && len(stuck) > limit {
		stuck = stuck[:limit]
	}
	return stuck, nil
}

// Len returns how many instances are stored, for assertions in tests.
func (m *MemoryStore) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.instances)
}
