package saga

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// Engine executes saga definitions against a Store.
//
// An Engine is safe for concurrent use. Concurrency between *workers* is
// handled by the store's optimistic locking rather than by in-process mutexes,
// because the competing workers may be in different processes.
type Engine struct {
	store Store
	log   *slog.Logger
	now   func() time.Time
}

// Option configures an Engine.
type Option func(*Engine)

// WithLogger sets the logger used for step transitions.
func WithLogger(l *slog.Logger) Option {
	return func(e *Engine) {
		if l != nil {
			e.log = l
		}
	}
}

// WithClock overrides the clock, for deterministic tests.
func WithClock(now func() time.Time) Option {
	return func(e *Engine) {
		if now != nil {
			e.now = now
		}
	}
}

// New returns an Engine backed by store.
func New(store Store, opts ...Option) *Engine {
	e := &Engine{
		store: store,
		log:   slog.Default(),
		now:   time.Now,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// NewInstance builds a fresh, unsaved instance for the given definition.
func NewInstance(id string, def *Definition, now time.Time) *Instance {
	steps := make([]StepRecord, len(def.Steps))
	for i, s := range def.Steps {
		steps[i] = StepRecord{Name: s.Name, Status: StepPending}
	}

	return &Instance{
		ID:        id,
		Name:      def.Name,
		Status:    StatusRunning,
		Steps:     steps,
		Data:      make(map[string]string, 4),
		StartedAt: now,
		UpdatedAt: now,
	}
}

// Start creates and runs a new saga.
//
// Starting an id that already exists returns ErrAlreadyExists from the store
// rather than running a second execution. Callers that need retry-safe
// behaviour should treat that as "already started" and load the existing
// instance instead — which is exactly what the order service does with its
// idempotency key.
func (e *Engine) Start(ctx context.Context, id string, def *Definition) (*Instance, error) {
	return e.StartWith(ctx, id, def, nil)
}

// StartWith creates and runs a new saga, seeding the session with initial data.
//
// The seed exists because some values are known only to the caller and are not
// derivable from the saga id: the order service passes the customer's payment
// method token this way. Data set here is persisted with the instance, so a
// recovery worker resuming the saga can still read it.
func (e *Engine) StartWith(
	ctx context.Context,
	id string,
	def *Definition,
	data map[string]string,
) (*Instance, error) {
	if id == "" {
		return nil, errors.New("saga: instance id is required")
	}
	if err := def.Validate(); err != nil {
		return nil, err
	}

	inst := NewInstance(id, def, e.now())
	for k, v := range data {
		inst.Data[k] = v
	}

	if err := e.store.Create(ctx, inst); err != nil {
		return nil, fmt.Errorf("saga %s: create: %w", id, err)
	}

	e.log.InfoContext(ctx, "saga started",
		slog.String("saga_id", id),
		slog.String("saga_name", def.Name),
		slog.Int("steps", len(def.Steps)),
	)

	return e.run(ctx, inst, def)
}

// Inspect loads a saga's state without running it.
//
// It backs the operator and debugging API: answering "what did this checkout
// actually do?" must not have the side effect of continuing the checkout.
func (e *Engine) Inspect(ctx context.Context, id string) (*Instance, error) {
	if id == "" {
		return nil, errors.New("saga: instance id is required")
	}

	inst, err := e.store.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("saga %s: load: %w", id, err)
	}
	return inst, nil
}

// Resume continues an existing saga. It is used by the recovery worker and by
// an operator retrying a saga whose compensation previously failed.
func (e *Engine) Resume(ctx context.Context, id string, def *Definition) (*Instance, error) {
	if err := def.Validate(); err != nil {
		return nil, err
	}

	inst, err := e.store.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("saga %s: load: %w", id, err)
	}

	if inst.Status.Terminal() && inst.Status != StatusFailed {
		return inst, nil
	}

	e.log.InfoContext(ctx, "saga resumed",
		slog.String("saga_id", id),
		slog.String("status", inst.Status.String()),
	)

	return e.run(ctx, inst, def)
}

// run drives an instance to a terminal state.
func (e *Engine) run(ctx context.Context, inst *Instance, def *Definition) (*Instance, error) {
	if err := def.Validate(); err != nil {
		return inst, err
	}
	e.normalizeSteps(inst, def)

	switch inst.Status {
	case StatusCompleted, StatusCompensated:
		return inst, nil
	case StatusCompensating, StatusFailed:
		// Compensation is retried from wherever it got to; already compensated
		// steps are skipped inside.
		return e.compensate(ctx, inst, def)
	default:
		return e.forward(ctx, inst, def)
	}
}

// normalizeSteps makes the instance's step records match the definition.
//
// A definition can gain steps between deployments while a saga is in flight, so
// records are added for new steps rather than assuming an exact match. Steps
// that were removed leave their records behind, which keeps the history honest.
func (e *Engine) normalizeSteps(inst *Instance, def *Definition) {
	have := make(map[string]StepRecord, len(inst.Steps))
	for _, r := range inst.Steps {
		have[r.Name] = r
	}

	steps := make([]StepRecord, len(def.Steps))
	for i, s := range def.Steps {
		if rec, ok := have[s.Name]; ok {
			steps[i] = rec
			continue
		}
		steps[i] = StepRecord{Name: s.Name, Status: StepPending}
	}
	inst.Steps = steps

	if inst.Data == nil {
		inst.Data = make(map[string]string, 4)
	}
}

// forward runs steps in order, skipping those already completed.
func (e *Engine) forward(ctx context.Context, inst *Instance, def *Definition) (*Instance, error) {
	inst.Status = StatusRunning
	inst.Attempts++
	inst.UpdatedAt = e.now()

	if err := e.store.Save(ctx, inst); err != nil {
		return inst, fmt.Errorf("saga %s: persist start: %w", inst.ID, err)
	}

	session := &Session{inst: inst}

	for i := range def.Steps {
		step := &def.Steps[i]
		rec := &inst.Steps[i]

		if rec.Status == StepCompleted {
			// Resume path: this step's effects are already applied. Running it
			// again would be a duplicate reservation or a second charge.
			continue
		}

		rec.StartedAt = e.now()

		attempts, err := e.executeAction(ctx, def, inst.ID, step.Name, step.Execute, session)
		rec.Attempts += attempts

		rec.FinishedAt = e.now()

		if err != nil {
			rec.Status = StepFailed
			rec.LastError = err.Error()
			inst.FailedStep = step.Name
			inst.LastError = err.Error()
			inst.UpdatedAt = e.now()

			e.log.WarnContext(ctx, "saga step failed, compensating",
				slog.String("saga_id", inst.ID),
				slog.String("step", step.Name),
				slog.String("error", err.Error()),
			)

			// Persist the failure before compensating. If the process dies
			// during compensation, the record still shows what went wrong and
			// which step triggered the rollback.
			if serr := e.store.Save(ctx, inst); serr != nil {
				return inst, fmt.Errorf("saga %s: persist failure: %w", inst.ID, serr)
			}

			return e.compensate(ctx, inst, def)
		}

		rec.Status = StepCompleted
		rec.LastError = ""
		inst.UpdatedAt = e.now()

		// Persisted before the next step starts. This single line is what turns
		// a crash from "unknown state" into "resume from step i+1".
		if serr := e.store.Save(ctx, inst); serr != nil {
			return inst, fmt.Errorf("saga %s: persist step %s: %w", inst.ID, step.Name, serr)
		}

		e.log.InfoContext(ctx, "saga step completed",
			slog.String("saga_id", inst.ID),
			slog.String("step", step.Name),
			slog.Int("attempts", rec.Attempts),
		)
	}

	inst.Status = StatusCompleted
	inst.LastError = ""
	inst.FailedStep = ""
	inst.UpdatedAt = e.now()

	if err := e.store.Save(ctx, inst); err != nil {
		return inst, fmt.Errorf("saga %s: persist completion: %w", inst.ID, err)
	}

	e.log.InfoContext(ctx, "saga completed", slog.String("saga_id", inst.ID))
	return inst, nil
}

// compensate runs compensations in reverse order.
//
// It covers steps that applied, steps whose previous compensation failed, and
// steps that failed in a way that leaves their outcome unknown.
func (e *Engine) compensate(ctx context.Context, inst *Instance, def *Definition) (*Instance, error) {
	inst.Status = StatusCompensating
	inst.UpdatedAt = e.now()

	if err := e.store.Save(ctx, inst); err != nil {
		return inst, fmt.Errorf("saga %s: persist compensating: %w", inst.ID, err)
	}

	session := &Session{inst: inst}

	var failed []string

	// Reverse order is the whole point: the last thing applied must be the
	// first thing undone, or a compensation can observe state its own step
	// depended on.
	for i := len(def.Steps) - 1; i >= 0; i-- {
		step := &def.Steps[i]
		rec := &inst.Steps[i]

		switch rec.Status {
		case StepCompleted:
			// Applied, so it must be undone.

		case StepCompensationFailed:
			// A previous compensation failed; this pass retries it.

		case StepFailed:
			// The step failed, but a failure across a network does not prove the
			// work was not done — a timeout may have been applied anyway. When
			// the step declares that possibility, its compensation runs so the
			// unknown outcome is resolved in the safe direction.
			if !step.CompensateOnFailure {
				continue
			}
			if rec.Compensated {
				continue
			}

		default:
			// Pending: never started, so nothing to undo.
			continue
		}

		if step.Compensate == nil {
			// No side effect to undo. Recorded rather than skipped so the
			// history shows the step was considered.
			rec.Compensated = true
			if rec.Status == StepCompleted {
				rec.Status = StepCompensated
			}
			continue
		}

		attempts, err := e.executeAction(ctx, def, inst.ID, step.Name+":compensate", step.Compensate, session)
		rec.Attempts += attempts

		if err != nil {
			// Deliberately continue with the remaining compensations: undoing
			// the earlier steps is still worth doing, and stopping here would
			// leave even more applied state behind. The saga ends up Failed so
			// that the residue is visible to an operator.
			rec.Status = StepCompensationFailed
			rec.Compensated = false
			rec.LastError = err.Error()
			failed = append(failed, step.Name)

			e.log.ErrorContext(ctx, "saga compensation failed",
				slog.String("saga_id", inst.ID),
				slog.String("step", step.Name),
				slog.String("error", err.Error()),
			)
		} else {
			rec.Compensated = true

			// A step that failed keeps Status=failed even though its
			// compensation succeeded — the operator needs to know which step
			// triggered the rollback, and Compensated records that the cleanup
			// ran.
			//
			// Its LastError is preserved too. Clearing it here was a bug: the
			// error explains *why* the checkout broke, and after compensation
			// succeeded it was the only place that explanation still existed.
			// The customer-facing message came out as "declined: unknown".
			if rec.Status == StepCompleted {
				rec.LastError = ""
				rec.Status = StepCompensated
			}

			e.log.InfoContext(ctx, "saga step compensated",
				slog.String("saga_id", inst.ID),
				slog.String("step", step.Name),
			)
		}

		inst.UpdatedAt = e.now()
		if serr := e.store.Save(ctx, inst); serr != nil {
			return inst, fmt.Errorf("saga %s: persist compensation of %s: %w", inst.ID, step.Name, serr)
		}
	}

	if len(failed) > 0 {
		inst.Status = StatusFailed
		inst.LastError = "compensation failed for steps: " + strings.Join(failed, ", ")
		inst.UpdatedAt = e.now()

		if serr := e.store.Save(ctx, inst); serr != nil {
			return inst, fmt.Errorf("saga %s: persist failed: %w", inst.ID, serr)
		}

		e.log.ErrorContext(ctx, "saga requires manual intervention",
			slog.String("saga_id", inst.ID),
			slog.String("failed_steps", strings.Join(failed, ",")),
		)

		return inst, fmt.Errorf("%w: saga %s: %s", ErrCompensationFailed, inst.ID, strings.Join(failed, ", "))
	}

	inst.Status = StatusCompensated
	inst.LastError = ""
	inst.UpdatedAt = e.now()

	if err := e.store.Save(ctx, inst); err != nil {
		return inst, fmt.Errorf("saga %s: persist compensated: %w", inst.ID, err)
	}

	e.log.InfoContext(ctx, "saga rolled back cleanly",
		slog.String("saga_id", inst.ID),
		slog.String("failed_step", inst.FailedStep),
	)

	return inst, nil
}

// executeAction runs one action under the definition's retry policy.
//
// It returns how many times the action was actually invoked, which is what lets
// a step record distinguish "failed once" from "failed after four attempts".
// Those two need different responses from an operator.
func (e *Engine) executeAction(
	ctx context.Context,
	def *Definition,
	sagaID, label string,
	action Action,
	session *Session,
) (int, error) {
	policy := def.Retry
	prevOnRetry := policy.OnRetry
	policy.OnRetry = func(attempt int, err error, delay time.Duration) {
		e.log.WarnContext(ctx, "saga step attempt failed, retrying",
			slog.String("saga_id", sagaID),
			slog.String("step", label),
			slog.Int("attempt", attempt),
			slog.Duration("retry_in", delay),
			slog.String("error", err.Error()),
		)
		if prevOnRetry != nil {
			prevOnRetry(attempt, err, delay)
		}
	}

	attempts := 0
	err := policy.Do(ctx, fmt.Sprintf("saga %s step %s", sagaID, label), func(ctx context.Context) error {
		attempts++
		return action(ctx, session)
	})

	return attempts, err
}

// RecoveryReport summarises one pass of the recovery worker.
type RecoveryReport struct {
	// Scanned is how many stuck sagas were found.
	Scanned int
	// Resumed is how many were successfully driven forward or rolled back.
	Resumed int
	// Failed is how many could not be recovered.
	Failed int
	// Skipped is how many had no matching definition, or were claimed by
	// another worker.
	Skipped int
}

// Recover resumes sagas that have stopped making progress.
//
// Without this, a process that dies mid-saga leaves the saga forever in
// StatusRunning: nothing is retrying it, and the stock it reserved is held
// indefinitely. The worker closes that gap.
//
// defFor resolves a stored saga name to its definition. A saga whose definition
// is no longer deployed is skipped rather than failed, so that removing a flow
// does not fill the alert channel.
func (e *Engine) Recover(
	ctx context.Context,
	stuckFor time.Duration,
	limit int,
	defFor func(name string) (*Definition, bool),
) (RecoveryReport, error) {
	var report RecoveryReport

	cutoff := e.now().Add(-stuckFor)
	stuck, err := e.store.ListStuck(ctx, cutoff, limit)
	if err != nil {
		return report, fmt.Errorf("saga recovery: list stuck: %w", err)
	}
	report.Scanned = len(stuck)

	for _, inst := range stuck {
		if ctx.Err() != nil {
			return report, ctx.Err()
		}

		def, ok := defFor(inst.Name)
		if !ok {
			e.log.WarnContext(ctx, "saga recovery: definition not found, skipping",
				slog.String("saga_id", inst.ID),
				slog.String("saga_name", inst.Name),
			)
			report.Skipped++
			continue
		}

		recovered, rerr := e.Resume(ctx, inst.ID, def)
		switch {
		case rerr == nil:
			report.Resumed++
			e.log.InfoContext(ctx, "saga recovered",
				slog.String("saga_id", inst.ID),
				slog.String("status", recovered.Status.String()),
			)
		case errors.Is(rerr, ErrVersionConflict):
			// Another worker claimed it between ListStuck and Resume. That is
			// the locking working, not a failure.
			report.Skipped++
		default:
			report.Failed++
			e.log.ErrorContext(ctx, "saga recovery failed",
				slog.String("saga_id", inst.ID),
				slog.String("error", rerr.Error()),
			)
		}
	}

	return report, nil
}
