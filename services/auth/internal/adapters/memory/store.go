// Package memory provides in-process storage for the auth service.
//
// It exists so the service is runnable and testable without PostgreSQL, and so
// that the uniqueness rules that make registration and rotation safe have a
// second implementation to be tested against. Nothing here is durable, and a
// restart logs everyone out.
package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/marketplace/services/auth/internal/domain"
)

// Store is an in-memory domain.Repository.
type Store struct {
	mu sync.RWMutex

	users    map[string]*domain.User
	byEmail  map[string]string // normalised email -> user id
	tokens   map[string]*domain.RefreshToken
	families map[string]map[string]struct{} // family id -> token ids
}

// New returns an empty store.
func New() *Store {
	return &Store{
		users:    make(map[string]*domain.User),
		byEmail:  make(map[string]string),
		tokens:   make(map[string]*domain.RefreshToken),
		families: make(map[string]map[string]struct{}),
	}
}

// CreateUser implements domain.Repository.
//
// The email check and the write share one critical section, so two concurrent
// registrations of the same address cannot both succeed. A read followed by a
// separate write would race; the PostgreSQL implementation uses a unique index
// for the same reason.
func (s *Store) CreateUser(_ context.Context, u *domain.User) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.byEmail[u.Email]; exists {
		return fmt.Errorf("%w: %s", domain.ErrEmailTaken, u.Email)
	}
	if _, exists := s.users[u.ID]; exists {
		return fmt.Errorf("%w: id %s is taken", domain.ErrEmailTaken, u.ID)
	}

	cp := *u
	cp.Roles = append([]string(nil), u.Roles...)
	s.users[u.ID] = &cp
	s.byEmail[u.Email] = u.ID
	return nil
}

// GetUser implements domain.Repository.
func (s *Store) GetUser(_ context.Context, id string) (*domain.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	u, ok := s.users[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", domain.ErrUserNotFound, id)
	}
	cp := *u
	cp.Roles = append([]string(nil), u.Roles...)
	return &cp, nil
}

// GetUserByEmail implements domain.Repository.
func (s *Store) GetUserByEmail(_ context.Context, email string) (*domain.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	id, ok := s.byEmail[domain.NormaliseEmail(email)]
	if !ok {
		return nil, fmt.Errorf("%w: %s", domain.ErrUserNotFound, email)
	}
	u, ok := s.users[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", domain.ErrUserNotFound, email)
	}
	cp := *u
	cp.Roles = append([]string(nil), u.Roles...)
	return &cp, nil
}

// CreateRefreshToken implements domain.Repository.
func (s *Store) CreateRefreshToken(_ context.Context, t *domain.RefreshToken) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cp := *t
	s.tokens[t.ID] = &cp

	if s.families[t.FamilyID] == nil {
		s.families[t.FamilyID] = make(map[string]struct{})
	}
	s.families[t.FamilyID][t.ID] = struct{}{}
	return nil
}

// GetRefreshToken implements domain.Repository.
func (s *Store) GetRefreshToken(_ context.Context, id string) (*domain.RefreshToken, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.tokens[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", domain.ErrRefreshTokenInvalid, id)
	}
	cp := *t
	return &cp, nil
}

// RevokeRefreshToken implements domain.Repository.
func (s *Store) RevokeRefreshToken(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tokens[id]
	if !ok {
		return nil
	}
	if t.RevokedAt.IsZero() {
		t.RevokedAt = time.Now()
	}
	return nil
}

// RevokeFamily implements domain.Repository.
func (s *Store) RevokeFamily(_ context.Context, familyID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for id := range s.families[familyID] {
		if t, ok := s.tokens[id]; ok && t.RevokedAt.IsZero() {
			t.RevokedAt = now
		}
	}
	return nil
}

// Counts returns the number of users and refresh tokens, for tests.
func (s *Store) Counts() (users, tokens int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.users), len(s.tokens)
}
