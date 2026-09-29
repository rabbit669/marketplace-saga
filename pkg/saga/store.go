package saga

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNotFound is returned when a saga instance does not exist.
	ErrNotFound = errors.New("saga: instance not found")
	// ErrVersionConflict is returned by Save when another worker has modified
	// the instance since it was read. Callers should treat it as "someone else
	// is handling this saga" rather than as a failure.
	ErrVersionConflict = errors.New("saga: instance was modified concurrently")
	// ErrCompensationFailed is returned when one or more compensations could not
	// be applied. The instance is marked StatusFailed and the message lists the
	// steps that need a human.
	ErrCompensationFailed = errors.New("saga: compensation failed")
)

// Store persists saga instances.
//
// The engine persists after every step transition, so an implementation must be
// durable and reasonably fast. Save is expected to enforce optimistic
// concurrency on Instance.Version and to return ErrVersionConflict when the
// stored version has moved on.
type Store interface {
	// Create stores a new instance. It must fail if the id already exists, so
	// that starting the same saga twice is detected rather than silently
	// starting a second execution.
	Create(ctx context.Context, inst *Instance) error

	// Get loads an instance, returning ErrNotFound when it does not exist.
	Get(ctx context.Context, id string) (*Instance, error)

	// Save writes the instance and increments Version.
	Save(ctx context.Context, inst *Instance) error

	// ListStuck returns non-terminal instances whose UpdatedAt is older than
	// the given time, oldest first, up to limit entries.
	//
	// This is what makes the recovery worker possible: a saga whose process
	// died between steps stops being updated, and without this query nothing
	// would ever notice.
	ListStuck(ctx context.Context, olderThan time.Time, limit int) ([]*Instance, error)
}

// ErrAlreadyExists is returned by Create when the instance id is taken.
var ErrAlreadyExists = errors.New("saga: instance already exists")
