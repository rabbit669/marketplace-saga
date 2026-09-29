package saga_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marketplace/pkg/apperr"
	"github.com/marketplace/pkg/resilience"
	"github.com/marketplace/pkg/saga"
)

// ------------------------------------------------------------------ fixture

// fixture records which actions ran, in order, and can be told to fail
// specific actions a number of times before succeeding.
type fixture struct {
	mu       sync.Mutex
	calls    []string
	failures map[string]int
	failWith error
}

func newFixture() *fixture {
	return &fixture{
		failures: make(map[string]int),
		failWith: apperr.Unavailable("test", "dependency is down"),
	}
}

// failTimes makes the named action fail n times before it can succeed.
func (f *fixture) failTimes(label string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures[label] = n
}

// clearFailures removes the failure plan, standing in for an outage ending.
func (f *fixture) clearFailures() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures = make(map[string]int)
}

// act returns an action that records the call and honours the failure plan.
func (f *fixture) act(label string) saga.Action {
	return func(_ context.Context, s *saga.Session) error {
		f.mu.Lock()
		f.calls = append(f.calls, label)
		remaining := f.failures[label]
		if remaining > 0 {
			f.failures[label] = remaining - 1
		}
		failWith := f.failWith
		f.mu.Unlock()

		if remaining > 0 {
			return failWith
		}

		// Record a produced value so that compensations have something to read,
		// which is how the real steps behave.
		s.Set(label+"_id", "id-"+label)
		return nil
	}
}

func (f *fixture) recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// reset forgets recorded calls, to isolate a second phase of a test.
func (f *fixture) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = nil
}

// checkoutDef is the saga used throughout: reserve stock, authorize payment,
// confirm the order. Only the first two have compensations, which exercises the
// "no compensation" path for the final step.
func checkoutDef(f *fixture) *saga.Definition {
	return &saga.Definition{
		Name: "checkout",
		Retry: resilience.RetryPolicy{
			MaxAttempts: 3,
			BaseDelay:   time.Millisecond,
			MaxDelay:    time.Millisecond,
			Multiplier:  1,
			// Never actually sleep: the schedule is not what these tests assert.
			Sleep: func(context.Context, time.Duration) error { return nil },
		},
		Steps: []saga.Step{
			{Name: "reserve_stock", Execute: f.act("reserve"), Compensate: f.act("release")},
			{Name: "authorize_payment", Execute: f.act("authorize"), Compensate: f.act("void")},
			{Name: "confirm_order", Execute: f.act("confirm")},
		},
	}
}

func newEngine(t *testing.T) (*saga.Engine, *saga.MemoryStore) {
	t.Helper()
	store := saga.NewMemoryStore()
	return saga.New(store, saga.WithLogger(discardLogger())), store
}

// ------------------------------------------------------------------- tests

func TestStart_RunsEveryStepInOrder(t *testing.T) {
	f := newFixture()
	engine, store := newEngine(t)

	inst, err := engine.Start(context.Background(), "saga-1", checkoutDef(f))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if inst.Status != saga.StatusCompleted {
		t.Errorf("status = %s, want completed", inst.Status)
	}

	want := []string{"reserve", "authorize", "confirm"}
	if got := f.recorded(); !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}

	// Every step is recorded as completed, so a later resume has nothing to do.
	for _, rec := range inst.Steps {
		if rec.Status != saga.StepCompleted {
			t.Errorf("step %s status = %s, want completed", rec.Name, rec.Status)
		}
	}

	// The instance is persisted, not just returned.
	stored, err := store.Get(context.Background(), "saga-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Status != saga.StatusCompleted {
		t.Errorf("stored status = %s, want completed", stored.Status)
	}
}

func TestStart_CompensatesInReverseOrderOnFailure(t *testing.T) {
	f := newFixture()
	// The payment provider declines, permanently. The reservation must be undone.
	f.failTimes("authorize", 99)

	engine, _ := newEngine(t)

	inst, err := engine.Start(context.Background(), "saga-2", checkoutDef(f))
	if err != nil {
		t.Fatalf("Start returned an error for a clean rollback: %v", err)
	}

	if inst.Status != saga.StatusCompensated {
		t.Fatalf("status = %s, want compensated", inst.Status)
	}
	if inst.FailedStep != "authorize_payment" {
		t.Errorf("FailedStep = %q, want authorize_payment", inst.FailedStep)
	}

	// Order matters: reserve ran once, authorize was attempted three times
	// (the retry policy), then release undid the reservation. confirm never ran,
	// and neither did its (absent) compensation.
	want := []string{"reserve", "authorize", "authorize", "authorize", "release"}
	if got := f.recorded(); !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}

	states := map[string]saga.StepStatus{}
	for _, rec := range inst.Steps {
		states[rec.Name] = rec.Status
	}
	if states["reserve_stock"] != saga.StepCompensated {
		t.Errorf("reserve_stock = %s, want compensated", states["reserve_stock"])
	}
	if states["authorize_payment"] != saga.StepFailed {
		t.Errorf("authorize_payment = %s, want failed", states["authorize_payment"])
	}
	if states["confirm_order"] != saga.StepPending {
		t.Errorf("confirm_order = %s, want pending: it never ran", states["confirm_order"])
	}
}

func TestCompensation_NeverRunsForStepsThatDidNotComplete(t *testing.T) {
	f := newFixture()
	// Fail the very first step: nothing was applied, so nothing may be undone.
	f.failTimes("reserve", 99)

	engine, _ := newEngine(t)

	inst, err := engine.Start(context.Background(), "saga-3", checkoutDef(f))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if inst.Status != saga.StatusCompensated {
		t.Fatalf("status = %s, want compensated", inst.Status)
	}

	calls := f.recorded()
	for _, c := range calls {
		if c == "release" || c == "void" || c == "confirm" {
			t.Errorf("call %q must not happen: nothing before it was applied (calls = %v)", c, calls)
		}
	}
}

func TestCompensation_ContinuesAfterAFailureAndFlagsTheSaga(t *testing.T) {
	f := newFixture()
	// Two steps complete, then the third fails. Undoing the reservation also
	// fails — an infrastructure problem — but voiding the payment still works.
	f.failTimes("confirm", 99)
	f.failTimes("release", 99)

	engine, store := newEngine(t)

	inst, err := engine.Start(context.Background(), "saga-4", checkoutDef(f))
	if err == nil {
		t.Fatal("Start = nil error, want ErrCompensationFailed")
	}
	if !errors.Is(err, saga.ErrCompensationFailed) {
		t.Errorf("error = %v, want it to wrap ErrCompensationFailed", err)
	}

	if inst.Status != saga.StatusFailed {
		t.Fatalf("status = %s, want failed: a human must resolve this", inst.Status)
	}
	if !strings.Contains(inst.LastError, "reserve_stock") {
		t.Errorf("LastError = %q, want it to name the step that could not be compensated", inst.LastError)
	}

	// The critical behaviour: the later compensation still ran, so the payment
	// was not left authorized just because the stock release failed.
	calls := f.recorded()
	if !slices.Contains(calls, "void") {
		t.Errorf("calls = %v, want void to have run despite the failed release", calls)
	}

	states := map[string]saga.StepStatus{}
	for _, rec := range inst.Steps {
		states[rec.Name] = rec.Status
	}
	if states["authorize_payment"] != saga.StepCompensated {
		t.Errorf("authorize_payment = %s, want compensated", states["authorize_payment"])
	}
	if states["reserve_stock"] != saga.StepCompensationFailed {
		t.Errorf("reserve_stock = %s, want compensation_failed", states["reserve_stock"])
	}

	// The failed state is durable, so an operator can find it.
	stored, gerr := store.Get(context.Background(), "saga-4")
	if gerr != nil {
		t.Fatalf("Get: %v", gerr)
	}
	if stored.Status != saga.StatusFailed {
		t.Errorf("stored status = %s, want failed", stored.Status)
	}
}

func TestResume_SkipsStepsThatAlreadyCompleted(t *testing.T) {
	// The crash scenario: the reservation was applied and persisted, then the
	// process died before the payment step ran. Resuming must not reserve twice.
	ctx := context.Background()
	f := newFixture()
	def := checkoutDef(f)
	store := saga.NewMemoryStore()
	engine := saga.New(store, saga.WithLogger(discardLogger()))

	inst := saga.NewInstance("saga-5", def, time.Now())
	inst.Steps[0].Status = saga.StepCompleted // reserve_stock already applied
	inst.Data["reserve_id"] = "res-42"
	if err := store.Create(ctx, inst); err != nil {
		t.Fatalf("Create: %v", err)
	}

	resumed, err := engine.Resume(ctx, "saga-5", def)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.Status != saga.StatusCompleted {
		t.Fatalf("status = %s, want completed", resumed.Status)
	}

	want := []string{"authorize", "confirm"}
	if got := f.recorded(); !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v: the completed step must not run again", got, want)
	}

	// Data written before the crash survives the resume, which is what lets a
	// compensation act on state it did not create in this process.
	if got, ok := resumed.Data["reserve_id"]; !ok || got != "res-42" {
		t.Errorf("Data[reserve_id] = (%q, %v), want (\"res-42\", true)", got, ok)
	}
}

func TestResume_OnTerminalSagaDoesNothing(t *testing.T) {
	ctx := context.Background()
	f := newFixture()
	def := checkoutDef(f)

	engine, _ := newEngine(t)

	if _, err := engine.Start(ctx, "saga-6", def); err != nil {
		t.Fatalf("Start: %v", err)
	}

	f.reset()

	inst, err := engine.Resume(ctx, "saga-6", def)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if inst.Status != saga.StatusCompleted {
		t.Errorf("status = %s, want completed", inst.Status)
	}
	if got := f.recorded(); len(got) != 0 {
		t.Errorf("calls = %v, want none: a completed saga must not re-run", got)
	}
}

func TestStart_RejectsDuplicateInstanceID(t *testing.T) {
	ctx := context.Background()
	f := newFixture()
	def := checkoutDef(f)

	engine, _ := newEngine(t)

	if _, err := engine.Start(ctx, "saga-7", def); err != nil {
		t.Fatalf("first Start: %v", err)
	}

	// A second start with the same id is how a retried checkout looks. It must
	// be detected rather than executed again.
	if _, err := engine.Start(ctx, "saga-7", def); !errors.Is(err, saga.ErrAlreadyExists) {
		t.Errorf("second Start = %v, want ErrAlreadyExists", err)
	}
}

func TestRetry_TransientFailureIsRetriedAndSucceeds(t *testing.T) {
	f := newFixture()
	// Two transient failures, then success. The retry policy allows three
	// attempts, so the saga completes without compensating.
	f.failTimes("authorize", 2)

	engine, _ := newEngine(t)

	inst, err := engine.Start(context.Background(), "saga-8", checkoutDef(f))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if inst.Status != saga.StatusCompleted {
		t.Fatalf("status = %s, want completed", inst.Status)
	}

	want := []string{"reserve", "authorize", "authorize", "authorize", "confirm"}
	if got := f.recorded(); !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}

	rec, _ := inst.Step("authorize_payment")
	if rec.Attempts != 3 {
		t.Errorf("attempts = %d, want 3", rec.Attempts)
	}
}

func TestRetry_PermanentErrorsFailFast(t *testing.T) {
	f := newFixture()
	f.mu.Lock()
	f.failWith = apperr.InvalidArgument("test", "card token is malformed")
	f.mu.Unlock()
	f.failTimes("authorize", 99)

	engine, _ := newEngine(t)

	inst, err := engine.Start(context.Background(), "saga-9", checkoutDef(f))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if inst.Status != saga.StatusCompensated {
		t.Fatalf("status = %s, want compensated", inst.Status)
	}

	// Exactly one attempt: retrying a malformed argument three times would only
	// add latency and load.
	want := []string{"reserve", "authorize", "release"}
	if got := f.recorded(); !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}
}

func TestSession_RequireFailsLoudlyOnMissingData(t *testing.T) {
	ctx := context.Background()

	def := &saga.Definition{
		Name: "session-test",
		Steps: []saga.Step{
			{Name: "produce", Execute: func(_ context.Context, s *saga.Session) error {
				s.Set("token", "abc")
				return nil
			}},
			{Name: "consume", Execute: func(_ context.Context, s *saga.Session) error {
				got, err := s.Require("token")
				if err != nil {
					return err
				}
				if got != "abc" {
					return errors.New("unexpected token")
				}
				// A key that was never set must produce a named error, not an
				// empty string silently treated as a valid value.
				if _, err := s.Require("never_set"); err == nil {
					return errors.New("Require on a missing key must fail")
				}
				return nil
			}},
		},
	}

	store := saga.NewMemoryStore()
	engine := saga.New(store, saga.WithLogger(discardLogger()))

	inst, err := engine.Start(ctx, "saga-session", def)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if inst.Status != saga.StatusCompleted {
		t.Fatalf("status = %s, want completed", inst.Status)
	}
	if inst.Data["token"] != "abc" {
		t.Errorf("Data[token] = %q, want abc", inst.Data["token"])
	}
}

func TestStore_VersionConflictIsDetected(t *testing.T) {
	ctx := context.Background()
	store := saga.NewMemoryStore()

	inst := saga.NewInstance("saga-10", &saga.Definition{
		Name:  "x",
		Steps: []saga.Step{{Name: "a", Execute: func(context.Context, *saga.Session) error { return nil }}},
	}, time.Now())

	if err := store.Create(ctx, inst); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Two workers load the same saga.
	first, err := store.Get(ctx, "saga-10")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	second, err := store.Get(ctx, "saga-10")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if err := store.Save(ctx, first); err != nil {
		t.Fatalf("first Save: %v", err)
	}

	// The second worker's view is now stale. Saving must be refused, which is
	// what stops two processes from executing the same saga.
	if err := store.Save(ctx, second); !errors.Is(err, saga.ErrVersionConflict) {
		t.Errorf("second Save = %v, want ErrVersionConflict", err)
	}
}

func TestStore_GetMissingReturnsNotFound(t *testing.T) {
	_, err := saga.NewMemoryStore().Get(context.Background(), "nope")
	if !errors.Is(err, saga.ErrNotFound) {
		t.Errorf("Get = %v, want ErrNotFound", err)
	}
}

func TestRecover_ResumesStuckSagas(t *testing.T) {
	ctx := context.Background()
	f := newFixture()
	def := checkoutDef(f)

	clock := newTestClock()
	store := saga.NewMemoryStore()
	engine := saga.New(store, saga.WithLogger(discardLogger()), saga.WithClock(clock.Now))

	// A saga that stopped after its first step, as if the process had died.
	inst := saga.NewInstance("saga-stuck", def, clock.Now())
	inst.Steps[0].Status = saga.StepCompleted
	inst.Data["reserve_id"] = "res-stuck"
	inst.UpdatedAt = clock.Now()
	if err := store.Create(ctx, inst); err != nil {
		t.Fatalf("Create: %v", err)
	}

	defFor := func(name string) (*saga.Definition, bool) {
		if name != "checkout" {
			return nil, false
		}
		return def, true
	}

	// Not yet stale: the worker must leave it alone.
	report, err := engine.Recover(ctx, time.Minute, 10, defFor)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if report.Scanned != 0 {
		t.Errorf("Scanned = %d, want 0 while the saga is still fresh", report.Scanned)
	}

	// Time passes without progress.
	clock.Advance(5 * time.Minute)

	report, err = engine.Recover(ctx, time.Minute, 10, defFor)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if report.Scanned != 1 || report.Resumed != 1 {
		t.Fatalf("report = %+v, want 1 scanned and 1 resumed", report)
	}

	recovered, err := store.Get(ctx, "saga-stuck")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if recovered.Status != saga.StatusCompleted {
		t.Errorf("status = %s, want completed", recovered.Status)
	}

	// The step that had already completed was not repeated.
	want := []string{"authorize", "confirm"}
	if got := f.recorded(); !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}
}

func TestRecover_SkipsSagasWithNoDefinition(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	store := saga.NewMemoryStore()
	engine := saga.New(store, saga.WithLogger(discardLogger()), saga.WithClock(clock.Now))

	f := newFixture()
	def := checkoutDef(f)

	inst := saga.NewInstance("saga-orphan", def, clock.Now())
	inst.Name = "retired-flow"
	inst.UpdatedAt = clock.Now().Add(-time.Hour)
	if err := store.Create(ctx, inst); err != nil {
		t.Fatalf("Create: %v", err)
	}

	report, err := engine.Recover(ctx, time.Minute, 10, func(string) (*saga.Definition, bool) {
		return nil, false
	})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	// Skipped, not failed: removing a flow from the codebase must not fill the
	// alert channel with unrecoverable sagas.
	if report.Skipped != 1 || report.Failed != 0 {
		t.Errorf("report = %+v, want 1 skipped and 0 failed", report)
	}
}

func TestRecover_IgnoresTerminalSagas(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	store := saga.NewMemoryStore()
	engine := saga.New(store, saga.WithLogger(discardLogger()), saga.WithClock(clock.Now))

	f := newFixture()
	def := checkoutDef(f)

	for _, status := range []saga.Status{saga.StatusCompleted, saga.StatusCompensated, saga.StatusFailed} {
		inst := saga.NewInstance("saga-"+string(status), def, clock.Now())
		inst.Status = status
		inst.UpdatedAt = clock.Now().Add(-time.Hour)
		if err := store.Create(ctx, inst); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}

	report, err := engine.Recover(ctx, time.Minute, 10, func(string) (*saga.Definition, bool) {
		return def, true
	})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if report.Scanned != 0 {
		t.Errorf("Scanned = %d, want 0: terminal sagas are not stuck", report.Scanned)
	}
}

func TestOperatorRetry_RecompensatesAfterAFailedCompensation(t *testing.T) {
	ctx := context.Background()
	f := newFixture()
	def := checkoutDef(f)
	store := saga.NewMemoryStore()
	engine := saga.New(store, saga.WithLogger(discardLogger()))

	// Payment is declined and the stock release fails too, leaving the saga in
	// the failed state that needs a human.
	f.failTimes("authorize", 99)
	f.failTimes("release", 99)

	if _, err := engine.Start(ctx, "saga-retry", def); err == nil {
		t.Fatal("Start = nil, want a compensation failure")
	}

	// The outage that blocked the release is over, so an operator retry must
	// finish the rollback.
	f.clearFailures()
	f.reset()

	inst, err := engine.Resume(ctx, "saga-retry", def)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if inst.Status != saga.StatusCompensated {
		t.Fatalf("status = %s, want compensated", inst.Status)
	}

	calls := f.recorded()
	if !slices.Contains(calls, "release") {
		t.Errorf("calls = %v, want the release to be retried", calls)
	}
	// The compensation that already succeeded must not run a second time.
	if slices.Contains(calls, "void") {
		t.Errorf("calls = %v, want void to be skipped: it was already compensated", calls)
	}
}

func TestDefinition_Validation(t *testing.T) {
	noop := func(context.Context, *saga.Session) error { return nil }

	tests := []struct {
		name    string
		def     *saga.Definition
		wantErr string
	}{
		{
			name:    "no name",
			def:     &saga.Definition{Steps: []saga.Step{{Name: "a", Execute: noop}}},
			wantErr: "no name",
		},
		{
			name:    "no steps",
			def:     &saga.Definition{Name: "x"},
			wantErr: "no steps",
		},
		{
			name:    "step without a name",
			def:     &saga.Definition{Name: "x", Steps: []saga.Step{{Execute: noop}}},
			wantErr: "no name",
		},
		{
			name: "duplicate step names",
			def: &saga.Definition{Name: "x", Steps: []saga.Step{
				{Name: "a", Execute: noop},
				{Name: "a", Execute: noop},
			}},
			wantErr: "duplicate",
		},
		{
			name: "step without an execute action",
			def: &saga.Definition{Name: "x", Steps: []saga.Step{
				{Name: "a"},
			}},
			wantErr: "no execute action",
		},
		{
			name: "valid",
			def:  &saga.Definition{Name: "x", Steps: []saga.Step{{Name: "a", Execute: noop}}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.def.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Errorf("Validate = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate = nil, want an error mentioning %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("Validate = %q, want it to mention %q", err, tc.wantErr)
			}
		})
	}
}

func TestStart_RejectsAnInvalidDefinition(t *testing.T) {
	engine, store := newEngine(t)

	if _, err := engine.Start(context.Background(), "saga-bad", &saga.Definition{Name: ""}); err == nil {
		t.Error("Start = nil, want a validation error")
	}
	if store.Len() != 0 {
		t.Error("an invalid definition must not create a stored instance")
	}
}

func TestStart_RejectsEmptyID(t *testing.T) {
	f := newFixture()
	engine, _ := newEngine(t)

	if _, err := engine.Start(context.Background(), "", checkoutDef(f)); err == nil {
		t.Error("Start with an empty id = nil, want an error")
	}
}

func TestStatus_Terminal(t *testing.T) {
	terminal := []saga.Status{saga.StatusCompleted, saga.StatusCompensated, saga.StatusFailed}
	for _, s := range terminal {
		if !s.Terminal() {
			t.Errorf("%s.Terminal() = false, want true", s)
		}
	}

	running := []saga.Status{saga.StatusRunning, saga.StatusCompensating}
	for _, s := range running {
		if s.Terminal() {
			t.Errorf("%s.Terminal() = true, want false", s)
		}
	}
}

func TestDefinitionChangedMidFlight_NewStepsRunOldRecordsSurvive(t *testing.T) {
	ctx := context.Background()
	f := newFixture()
	store := saga.NewMemoryStore()
	engine := saga.New(store, saga.WithLogger(discardLogger()))

	original := checkoutDef(f)
	inst := saga.NewInstance("saga-evolve", original, time.Now())
	inst.Steps[0].Status = saga.StepCompleted
	inst.Data["reserve_id"] = "res-evolve"
	if err := store.Create(ctx, inst); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// A deployment adds a step in the middle. The engine must reconcile rather
	// than fail on the mismatch.
	updated := &saga.Definition{
		Name:  original.Name,
		Retry: original.Retry,
		Steps: []saga.Step{
			original.Steps[0],
			{Name: "check_fraud", Execute: f.act("fraud")},
			original.Steps[1],
			original.Steps[2],
		},
	}

	resumed, err := engine.Resume(ctx, "saga-evolve", updated)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.Status != saga.StatusCompleted {
		t.Fatalf("status = %s, want completed", resumed.Status)
	}

	want := []string{"fraud", "authorize", "confirm"}
	if got := f.recorded(); !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v (the already-completed step must be skipped)", got, want)
	}
}

func TestCompensation_RunsForAFailedStepWhoseOutcomeIsUnknown(t *testing.T) {
	// A request that timed out may have been applied. The engine cannot tell,
	// so a step that declares this possibility gets its compensation run even
	// though it reported failure — which is how an orphaned hold on a customer's
	// card is avoided.
	f := newFixture()
	f.failTimes("authorize", 99)

	def := &saga.Definition{
		Name: "unknown-outcome",
		Retry: resilience.RetryPolicy{
			MaxAttempts: 1,
			Sleep:       func(context.Context, time.Duration) error { return nil },
		},
		Steps: []saga.Step{
			{Name: "reserve", Execute: f.act("reserve"), Compensate: f.act("release")},
			{
				Name:                "authorize",
				Execute:             f.act("authorize"),
				Compensate:          f.act("void"),
				CompensateOnFailure: true,
			},
		},
	}

	engine, _ := newEngine(t)

	inst, err := engine.Start(context.Background(), "saga-unknown", def)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if inst.Status != saga.StatusCompensated {
		t.Fatalf("status = %s, want compensated", inst.Status)
	}

	calls := f.recorded()
	if !slices.Contains(calls, "void") {
		t.Errorf("calls = %v, want void to have run although authorize failed", calls)
	}
	if !slices.Contains(calls, "release") {
		t.Errorf("calls = %v, want release to have run", calls)
	}

	rec, ok := inst.Step("authorize")
	if !ok {
		t.Fatal("no record for the authorize step")
	}
	// The record still says the step failed — the operator needs to know what
	// triggered the rollback — while Compensated says the cleanup happened.
	if rec.Status != saga.StepFailed {
		t.Errorf("status = %s, want failed", rec.Status)
	}
	if !rec.Compensated {
		t.Error("Compensated = false, want true: the compensation ran successfully")
	}
}

func TestCompensation_SkipsAFailedStepByDefault(t *testing.T) {
	// Without the flag the default is the conservative reading: a failed step did
	// not apply, so there is nothing to undo and its compensation is not called.
	f := newFixture()
	f.failTimes("authorize", 99)

	def := &saga.Definition{
		Name: "known-failure",
		Retry: resilience.RetryPolicy{
			MaxAttempts: 1,
			Sleep:       func(context.Context, time.Duration) error { return nil },
		},
		Steps: []saga.Step{
			{Name: "reserve", Execute: f.act("reserve"), Compensate: f.act("release")},
			{Name: "authorize", Execute: f.act("authorize"), Compensate: f.act("void")},
		},
	}

	engine, _ := newEngine(t)

	if _, err := engine.Start(context.Background(), "saga-known", def); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if calls := f.recorded(); slices.Contains(calls, "void") {
		t.Errorf("calls = %v, want no void: authorize never applied", calls)
	}
}

func TestCompensation_DoesNotRepeatAFailedStepsCompensation(t *testing.T) {
	// The saga ends Failed because the release could not be applied. On the next
	// pass the already-compensated authorize step must not be voided again.
	f := newFixture()
	f.failTimes("authorize", 99)
	f.failTimes("release", 1)

	def := &saga.Definition{
		Name: "repeat-guard",
		Retry: resilience.RetryPolicy{
			MaxAttempts: 1,
			Sleep:       func(context.Context, time.Duration) error { return nil },
		},
		Steps: []saga.Step{
			{Name: "reserve", Execute: f.act("reserve"), Compensate: f.act("release")},
			{
				Name:                "authorize",
				Execute:             f.act("authorize"),
				Compensate:          f.act("void"),
				CompensateOnFailure: true,
			},
		},
	}

	store := saga.NewMemoryStore()
	engine := saga.New(store, saga.WithLogger(discardLogger()))

	if _, err := engine.Start(context.Background(), "saga-repeat", def); err == nil {
		t.Fatal("Start = nil, want the failed compensation to be reported")
	}

	f.reset()

	inst, err := engine.Resume(context.Background(), "saga-repeat", def)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if inst.Status != saga.StatusCompensated {
		t.Fatalf("status = %s, want compensated after the retry", inst.Status)
	}

	calls := f.recorded()
	if !slices.Contains(calls, "release") {
		t.Errorf("calls = %v, want the failed release to be retried", calls)
	}
	if slices.Contains(calls, "void") {
		t.Errorf("calls = %v, want no second void: it already succeeded", calls)
	}
}
