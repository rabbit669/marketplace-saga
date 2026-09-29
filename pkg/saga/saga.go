// Package saga implements an orchestrated saga: a sequence of local
// transactions across services, where every completed step has a compensating
// action that undoes it.
//
// The problem it exists to solve is a distributed transaction without a
// distributed transaction manager. A checkout writes to the order database,
// then the inventory database, then calls a payment provider. There is no
// transaction spanning those, and when the payment is declined there is nothing
// to roll back — the earlier writes are already committed. The only way to undo
// them is to run *new business operations* that reverse their effects. That is
// a compensation, and the sequence of compensate-in-reverse is the saga.
//
// The engine guarantees four properties, each covered by tests:
//
//  1. Forward-only progress. A saga never re-runs a step it has already
//     completed, so a crash and restart resumes rather than repeats.
//  2. Compensation in reverse order, and only for steps that completed.
//  3. Best-effort compensation. If one compensation fails, the remaining ones
//     are still attempted, because undoing earlier steps is still valuable.
//     The saga ends in StatusFailed so a human can intervene.
//  4. Recoverability. Every step transition is persisted before the next step
//     starts, so a saga is never lost in an unknown state.
//
// Steps are required to be idempotent. The engine retries on transient
// failures, and "transient" includes a request that timed out but may still
// have been applied by the remote side.
package saga

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/marketplace/pkg/resilience"
)

// Status is the lifecycle state of a saga instance.
type Status string

const (
	// StatusRunning means forward execution is in progress.
	StatusRunning Status = "running"
	// StatusCompleted means every step succeeded. Terminal.
	StatusCompleted Status = "completed"
	// StatusCompensating means a step failed and compensations are running.
	StatusCompensating Status = "compensating"
	// StatusCompensated means the failure was fully rolled back. Terminal.
	// This is a successful outcome for the business: nothing was left applied.
	StatusCompensated Status = "compensated"
	// StatusFailed means at least one compensation could not be applied and the
	// system is in a state a human has to resolve. Terminal, but recoverable by
	// Retry: an operator can re-run compensation once the blocker is gone.
	StatusFailed Status = "failed"
)

// Terminal reports whether the saga has stopped moving on its own.
func (s Status) Terminal() bool {
	switch s {
	case StatusCompleted, StatusCompensated, StatusFailed:
		return true
	default:
		return false
	}
}

func (s Status) String() string { return string(s) }

// StepStatus is the state of a single step within a saga.
type StepStatus string

const (
	StepPending            StepStatus = "pending"
	StepCompleted          StepStatus = "completed"
	StepFailed             StepStatus = "failed"
	StepCompensated        StepStatus = "compensated"
	StepCompensationFailed StepStatus = "compensation_failed"
)

// StepRecord is the durable record of one step. It is stored alongside the
// saga so that a restart can tell what has already happened.
type StepRecord struct {
	Name      string
	Status    StepStatus
	Attempts  int
	LastError string
	// Compensated reports whether a compensation was successfully applied to
	// this step, independently of Status.
	//
	// The two are not the same question. A step that completed and was undone
	// has Status=compensated. A step that failed and was *still* compensated —
	// because its outcome was unknown, see Step.CompensateOnFailure — keeps
	// Status=failed so the operator can see what triggered the rollback, while
	// Compensated records that the cleanup happened.
	Compensated bool
	StartedAt   time.Time
	FinishedAt  time.Time
}

// Instance is the persisted state of one saga execution.
type Instance struct {
	// ID is caller-supplied and stable, so that starting the same saga twice is
	// idempotent rather than producing two executions.
	ID string
	// Name identifies the saga definition, e.g. "checkout".
	Name string
	// Status is the current lifecycle state.
	Status Status
	// Steps is one record per step of the definition, in definition order.
	Steps []StepRecord
	// Attempts counts how many times execution was started, including resumes.
	Attempts int
	// LastError is the most recent failure, for operators.
	LastError string
	// FailedStep names the step whose failure triggered compensation.
	FailedStep string
	// Data holds small values produced by steps that compensation will need.
	//
	// This is the crux of recoverability: compensation often cannot be derived
	// from the saga id alone — releasing stock needs a reservation id, voiding a
	// payment needs a payment id — and those are produced by steps. Keeping them
	// in memory would make a saga unrecoverable after a restart, so they are
	// persisted here instead.
	Data map[string]string
	// StartedAt and UpdatedAt are used by the recovery worker to find sagas that
	// have stopped making progress.
	StartedAt time.Time
	UpdatedAt time.Time
	// Version supports optimistic concurrency. Two workers must not execute the
	// same saga at once, and this is what detects that.
	Version int64
}

// Step returns the record for the named step.
func (i *Instance) Step(name string) (StepRecord, bool) {
	for _, s := range i.Steps {
		if s.Name == name {
			return s, true
		}
	}
	return StepRecord{}, false
}

// Clone returns a deep copy, so that a failed Save cannot leave the caller
// holding a half-updated instance.
func (i *Instance) Clone() *Instance {
	cp := *i
	cp.Steps = make([]StepRecord, len(i.Steps))
	copy(cp.Steps, i.Steps)
	if i.Data != nil {
		cp.Data = make(map[string]string, len(i.Data))
		for k, v := range i.Data {
			cp.Data[k] = v
		}
	}
	return &cp
}

// Session gives a step access to the saga's shared data.
//
// It is passed to both Execute and Compensate. A compensating action must be
// able to run from persisted state alone, so anything it needs must have been
// put here by the forward step.
type Session struct {
	inst *Instance
}

// ID returns the saga instance id.
func (s *Session) ID() string { return s.inst.ID }

// Set stores a value produced by this step.
func (s *Session) Set(key, value string) {
	if s.inst.Data == nil {
		s.inst.Data = make(map[string]string, 4)
	}
	s.inst.Data[key] = value
}

// Get reads a value stored by an earlier step.
func (s *Session) Get(key string) (string, bool) {
	v, ok := s.inst.Data[key]
	return v, ok
}

// Require reads a value that must be present, returning an error that names the
// missing key. A missing key in a compensation means the forward step did not
// record what it produced — a bug that should be loud, not silently skipped.
func (s *Session) Require(key string) (string, error) {
	v, ok := s.inst.Data[key]
	if !ok || v == "" {
		return "", fmt.Errorf("saga %s: required value %q is not set", s.inst.ID, key)
	}
	return v, nil
}

// FailedStep returns the name of the step whose failure triggered compensation,
// or "" when forward execution has not failed.
//
// A compensating action uses it to record *why* it ran. "Stock released" is much
// less useful in an audit trail than "stock released because authorize_payment
// was declined", and only the engine knows the second half of that sentence.
func (s *Session) FailedStep() string { return s.inst.FailedStep }

// Action is a single step body. Returning nil means the step was applied.
//
// Actions must be idempotent: the engine retries them, and a retry after a
// timeout may execute against state the first attempt already changed.
type Action func(ctx context.Context, s *Session) error

// Step pairs a forward action with the compensation that undoes it.
type Step struct {
	// Name identifies the step in records and logs. It must be stable across
	// deployments: a rename on a saga that is mid-flight makes its history
	// unreadable.
	Name string
	// Execute performs the step.
	Execute Action
	// Compensate reverses it. It may be nil for a step with no side effect,
	// such as the final local status update.
	//
	// A compensation must tolerate being called when the forward step did not
	// actually apply — the classic case is a request that timed out but was
	// applied anyway. Releasing a reservation that was never taken must be a
	// no-op, not an error.
	Compensate Action

	// CompensateOnFailure also runs the compensation when Execute failed.
	//
	// This exists because "failed" and "did not apply" are not the same thing
	// across a network. A connection refused means the step definitely did not
	// apply; a request that timed out means it might have. The engine cannot
	// tell those apart, so a step whose failure mode includes an unknown
	// outcome declares it here and gets its compensation run regardless.
	//
	// The compensation must then be a no-op when there is nothing to undo —
	// which is required of every compensation anyway, but here it is load
	// bearing. Leave this false for steps whose failure is unambiguous and
	// whose compensation is expensive or has side effects of its own.
	CompensateOnFailure bool
}

// Definition is a saga's step list, shared by every execution of it.
type Definition struct {
	// Name identifies the saga type, e.g. "checkout".
	Name string
	// Steps run in order.
	Steps []Step
	// Retry applies to each step's execution and compensation. A zero value
	// means a single attempt with no delay.
	Retry resilience.RetryPolicy
}

// Validate reports definition problems that would otherwise surface mid-flight.
func (d *Definition) Validate() error {
	if d.Name == "" {
		return errors.New("saga: definition has no name")
	}
	if len(d.Steps) == 0 {
		return fmt.Errorf("saga %s: definition has no steps", d.Name)
	}

	seen := make(map[string]struct{}, len(d.Steps))
	for i, s := range d.Steps {
		if s.Name == "" {
			return fmt.Errorf("saga %s: step %d has no name", d.Name, i)
		}
		if _, dup := seen[s.Name]; dup {
			return fmt.Errorf("saga %s: duplicate step name %q", d.Name, s.Name)
		}
		seen[s.Name] = struct{}{}

		if s.Execute == nil {
			return fmt.Errorf("saga %s: step %q has no execute action", d.Name, s.Name)
		}
	}
	return nil
}

// IndexOf returns the position of the named step in the definition.
func (d *Definition) IndexOf(name string) (int, bool) {
	for i, s := range d.Steps {
		if s.Name == name {
			return i, true
		}
	}
	return -1, false
}
