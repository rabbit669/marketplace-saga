package app_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/marketplace/pkg/apperr"
	"github.com/marketplace/pkg/money"

	"github.com/marketplace/services/payment/internal/adapters/memory"
	"github.com/marketplace/services/payment/internal/app"
	"github.com/marketplace/services/payment/internal/domain"
)

// fakeProvider records how many times it was asked, which is the number these
// tests actually care about: an idempotency bug shows up as a second call, not
// as a wrong answer.
type fakeProvider struct {
	mu    sync.Mutex
	calls int

	approve bool
	reason  domain.DeclineReason
	err     error
	// delay makes concurrent callers overlap, which is what exposes a
	// check-then-write race.
	delay time.Duration
}

func (f *fakeProvider) Authorize(ctx context.Context, req domain.AuthorizeRequest) (domain.ProviderDecision, error) {
	f.mu.Lock()
	f.calls++
	approve, reason, err, delay := f.approve, f.reason, f.err, f.delay
	f.mu.Unlock()

	if delay > 0 {
		select {
		case <-ctx.Done():
			return domain.ProviderDecision{}, ctx.Err()
		case <-time.After(delay):
		}
	}

	if err != nil {
		return domain.ProviderDecision{}, err
	}
	if approve {
		return domain.ProviderDecision{Approved: true, ProviderReference: "psp_ok"}, nil
	}
	return domain.ProviderDecision{
		Approved:          false,
		DeclineReason:     reason,
		ProviderReference: "psp_declined",
	}, nil
}

func (f *fakeProvider) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type harness struct {
	service  *app.Service
	store    *memory.Store
	provider *fakeProvider
	clock    time.Time
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	h := &harness{
		store:    memory.New(),
		provider: &fakeProvider{approve: true},
		clock:    time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC),
	}

	var err error
	h.service, err = app.NewService(app.ServiceConfig{
		Payments: h.store,
		Provider: h.provider,
		Clock:    func() time.Time { return h.clock },
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return h
}

func (h *harness) request(key string) app.AuthorizeRequest {
	return app.AuthorizeRequest{
		OrderID:            "order-" + key,
		Amount:             money.MustNew(4999, money.USD),
		IdempotencyKey:     key,
		PaymentMethodToken: "pm_token",
	}
}

// --------------------------------------------------------------- authorize

func TestAuthorize_Approved(t *testing.T) {
	h := newHarness(t)

	result, err := h.service.Authorize(context.Background(), h.request("k1"))
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}

	if result.IdempotentReplay {
		t.Error("IdempotentReplay = true on a first call")
	}
	if result.Payment.Status != domain.StatusAuthorized {
		t.Errorf("status = %s, want authorized", result.Payment.Status)
	}
	if result.Payment.ProviderReference == "" {
		t.Error("ProviderReference is empty; reconciliation would be impossible")
	}
	if got := h.provider.callCount(); got != 1 {
		t.Errorf("provider calls = %d, want 1", got)
	}
}

func TestAuthorize_DeclinedIsASuccessfulResponseCarryingTheReason(t *testing.T) {
	// A decline is the provider answering, not the provider failing. The proto
	// models it this way and so does this service, which means the caller must
	// inspect the status — the checkout saga does so in exactly one place.
	h := newHarness(t)
	h.provider.approve = false
	h.provider.reason = domain.ReasonInsufficientFunds

	result, err := h.service.Authorize(context.Background(), h.request("k2"))
	if err != nil {
		t.Fatalf("Authorize returned an error for a decline: %v", err)
	}

	if result.Payment.Status != domain.StatusDeclined {
		t.Fatalf("status = %s, want declined", result.Payment.Status)
	}
	if result.Payment.DeclineReason != domain.ReasonInsufficientFunds {
		t.Errorf("reason = %q, want %q", result.Payment.DeclineReason, domain.ReasonInsufficientFunds)
	}
	if result.Payment.DeclineReason.Retryable() {
		t.Error("insufficient funds must be classified as permanent")
	}

	// A declined attempt still gets a provider reference: support may need to
	// look it up, and the saga's void step needs something to act on.
	if result.Payment.ProviderReference == "" {
		t.Error("a declined payment should still carry a provider reference")
	}
}

func TestAuthorize_TransientDeclineIsClassifiedRetryable(t *testing.T) {
	h := newHarness(t)
	h.provider.approve = false
	h.provider.reason = domain.ReasonProviderTimeout

	result, err := h.service.Authorize(context.Background(), h.request("k3"))
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}

	if !result.Payment.DeclineReason.Retryable() {
		t.Error("a provider timeout must be classified as retryable, or the saga compensates instead of retrying")
	}
}

func TestAuthorize_ReplaysForTheSameKey(t *testing.T) {
	h := newHarness(t)

	first, err := h.service.Authorize(context.Background(), h.request("k4"))
	if err != nil {
		t.Fatalf("first Authorize: %v", err)
	}

	second, err := h.service.Authorize(context.Background(), h.request("k4"))
	if err != nil {
		t.Fatalf("second Authorize: %v", err)
	}

	if !second.IdempotentReplay {
		t.Error("IdempotentReplay = false, want true")
	}
	if second.Payment.ID != first.Payment.ID {
		t.Errorf("second call produced payment %s, want the original %s", second.Payment.ID, first.Payment.ID)
	}

	// The critical assertion. A replay must not reach the provider, because
	// reaching the provider is what takes the customer's money.
	if got := h.provider.callCount(); got != 1 {
		t.Errorf("provider calls = %d, want 1: a replay must not re-authorize", got)
	}
	if got := h.store.Len(); got != 1 {
		t.Errorf("stored payments = %d, want 1", got)
	}
}

func TestAuthorize_ConcurrentRetriesReachTheProviderOnce(t *testing.T) {
	// The failure this guards against is the double charge: a client times out,
	// retries, and both calls reach the provider. The pending-record write is
	// what prevents it, and only an atomic claim survives this test.
	h := newHarness(t)
	h.provider.delay = 20 * time.Millisecond

	const attempts = 12

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		success  int
		inFlight int
		other    []error
	)

	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()

			_, err := h.service.Authorize(context.Background(), h.request("k-race"))

			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				success++
			case errors.Is(err, context.DeadlineExceeded):
				other = append(other, err)
			case apperr.Is(err, apperr.CodeConflict):
				// "A payment with this key is already in progress" is the
				// correct answer for a concurrent duplicate: the first call's
				// outcome is not known yet, so it cannot be replayed.
				inFlight++
			default:
				other = append(other, err)
			}
		}()
	}
	wg.Wait()

	if len(other) > 0 {
		t.Fatalf("unexpected errors: %v", other)
	}
	if success != 1 {
		t.Errorf("successful authorizations = %d, want exactly 1", success)
	}
	if inFlight != attempts-1 {
		t.Errorf("in-progress rejections = %d, want %d", inFlight, attempts-1)
	}

	// The number that matters: one key, one charge.
	if got := h.provider.callCount(); got != 1 {
		t.Errorf("provider calls = %d, want exactly 1", got)
	}
	if got := h.store.Len(); got != 1 {
		t.Errorf("stored payments = %d, want 1", got)
	}
}

func TestAuthorize_ReleasesTheKeyWhenTheProviderIsUnreachable(t *testing.T) {
	// A transient outage must not permanently claim the key. If the pending
	// record survived, every later retry with this key would fail as "already in
	// progress", turning a blip into a stuck checkout.
	h := newHarness(t)
	h.provider.err = errors.New("connection refused")

	_, err := h.service.Authorize(context.Background(), h.request("k5"))
	if err == nil {
		t.Fatal("Authorize = nil, want a provider error")
	}
	if !apperr.Is(err, apperr.CodeUnavailable) {
		t.Errorf("code = %q, want %q so the caller retries", apperr.CodeOf(err), apperr.CodeUnavailable)
	}
	if got := h.store.Len(); got != 0 {
		t.Errorf("stored payments = %d, want 0: the claim must be released", got)
	}

	// The provider comes back, and the same key now works.
	h.provider.err = nil

	result, err := h.service.Authorize(context.Background(), h.request("k5"))
	if err != nil {
		t.Fatalf("retry after the outage failed: %v", err)
	}
	if result.Payment.Status != domain.StatusAuthorized {
		t.Errorf("status = %s, want authorized", result.Payment.Status)
	}
}

func TestAuthorize_ValidatesInput(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*app.AuthorizeRequest)
	}{
		{"no order", func(r *app.AuthorizeRequest) { r.OrderID = "" }},
		{"no key", func(r *app.AuthorizeRequest) { r.IdempotencyKey = "" }},
		{"no token", func(r *app.AuthorizeRequest) { r.PaymentMethodToken = "" }},
		{"zero amount", func(r *app.AuthorizeRequest) { r.Amount = money.MustNew(0, money.USD) }},
		{"negative amount", func(r *app.AuthorizeRequest) { r.Amount = money.MustNew(-100, money.USD) }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			req := h.request("k-invalid")
			tc.mutate(&req)

			_, err := h.service.Authorize(context.Background(), req)
			if !apperr.Is(err, apperr.CodeInvalidArgument) {
				t.Errorf("code = %q, want %q (err = %v)", apperr.CodeOf(err), apperr.CodeInvalidArgument, err)
			}
			if got := h.provider.callCount(); got != 0 {
				t.Errorf("provider calls = %d, want 0: validation precedes the provider", got)
			}
		})
	}
}

// ----------------------------------------------------------------- capture

func TestCapture_IsIdempotentByPaymentID(t *testing.T) {
	// The saga retries this step, and a retry after a timeout must not fail.
	// Capturing twice is the same operation applied twice to the same record,
	// so it needs no separate idempotency key.
	h := newHarness(t)

	authorized, err := h.service.Authorize(context.Background(), h.request("k6"))
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	id := authorized.Payment.ID

	first, err := h.service.Capture(context.Background(), id)
	if err != nil {
		t.Fatalf("first Capture: %v", err)
	}
	if first.Status != domain.StatusCaptured {
		t.Fatalf("status = %s, want captured", first.Status)
	}

	second, err := h.service.Capture(context.Background(), id)
	if err != nil {
		t.Fatalf("second Capture: %v", err)
	}
	if second.Status != domain.StatusCaptured {
		t.Errorf("status = %s, want captured", second.Status)
	}
}

func TestCapture_RejectsADeclinedPayment(t *testing.T) {
	h := newHarness(t)
	h.provider.approve = false
	h.provider.reason = domain.ReasonInsufficientFunds

	declined, err := h.service.Authorize(context.Background(), h.request("k7"))
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}

	_, err = h.service.Capture(context.Background(), declined.Payment.ID)
	if err == nil {
		t.Fatal("Capture of a declined payment = nil, want a rejection")
	}
	if !apperr.Is(err, apperr.CodeFailedPrecondition) {
		t.Errorf("code = %q, want %q", apperr.CodeOf(err), apperr.CodeFailedPrecondition)
	}
}

func TestCapture_UnknownPayment(t *testing.T) {
	h := newHarness(t)

	_, err := h.service.Capture(context.Background(), "does-not-exist")
	if !apperr.Is(err, apperr.CodeNotFound) {
		t.Errorf("code = %q, want %q", apperr.CodeOf(err), apperr.CodeNotFound)
	}
}

// -------------------------------------------------------------------- void

func TestVoid_ReleasesAnAuthorization(t *testing.T) {
	h := newHarness(t)

	authorized, err := h.service.Authorize(context.Background(), h.request("k8"))
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}

	voided, err := h.service.Void(context.Background(), authorized.Payment.ID, "checkout_failed")
	if err != nil {
		t.Fatalf("Void: %v", err)
	}
	if voided.Status != domain.StatusVoided {
		t.Errorf("status = %s, want voided", voided.Status)
	}
}

func TestVoid_IsANoOpWhenThereIsNothingToVoid(t *testing.T) {
	// This runs as a saga compensation, and a compensation executes precisely
	// when the caller does not know whether the forward step applied. "There is
	// nothing to undo" must be indistinguishable from success.
	t.Run("unknown payment", func(t *testing.T) {
		h := newHarness(t)

		payment, err := h.service.Void(context.Background(), "never-existed", "checkout_failed")
		if err != nil {
			t.Errorf("Void of an unknown payment = %v, want nil", err)
		}
		if payment != nil {
			t.Errorf("payment = %v, want nil", payment)
		}
	})

	t.Run("empty id", func(t *testing.T) {
		h := newHarness(t)

		if _, err := h.service.Void(context.Background(), "", "checkout_failed"); err != nil {
			t.Errorf("Void with an empty id = %v, want nil", err)
		}
	})

	t.Run("declined payment", func(t *testing.T) {
		h := newHarness(t)
		h.provider.approve = false
		h.provider.reason = domain.ReasonInsufficientFunds

		declined, err := h.service.Authorize(context.Background(), h.request("k9"))
		if err != nil {
			t.Fatalf("Authorize: %v", err)
		}

		// The provider declined, so no funds were ever held. Voiding is still
		// called, because the saga cannot tell a decline from a timeout.
		if _, err := h.service.Void(context.Background(), declined.Payment.ID, "checkout_failed"); err != nil {
			t.Errorf("Void of a declined payment = %v, want nil", err)
		}
	})

	t.Run("already voided", func(t *testing.T) {
		h := newHarness(t)

		authorized, err := h.service.Authorize(context.Background(), h.request("k10"))
		if err != nil {
			t.Fatalf("Authorize: %v", err)
		}
		if _, err := h.service.Void(context.Background(), authorized.Payment.ID, "first"); err != nil {
			t.Fatalf("first Void: %v", err)
		}
		if _, err := h.service.Void(context.Background(), authorized.Payment.ID, "second"); err != nil {
			t.Errorf("second Void = %v, want nil", err)
		}
	})
}

func TestVoid_RefusesACapturedPayment(t *testing.T) {
	// This is not a compensation failure: the caller is asking for the wrong
	// operation. The money has moved, and returning success would tell the
	// checkout saga the customer had been refunded when they had not.
	h := newHarness(t)

	authorized, err := h.service.Authorize(context.Background(), h.request("k11"))
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if _, err := h.service.Capture(context.Background(), authorized.Payment.ID); err != nil {
		t.Fatalf("Capture: %v", err)
	}

	_, err = h.service.Void(context.Background(), authorized.Payment.ID, "too late")
	if err == nil {
		t.Fatal("Void of a captured payment = nil, want a rejection")
	}
	if !apperr.Is(err, apperr.CodeFailedPrecondition) {
		t.Errorf("code = %q, want %q", apperr.CodeOf(err), apperr.CodeFailedPrecondition)
	}
}

// ------------------------------------------------------------------ refund

func TestRefund_PartialThenFull(t *testing.T) {
	h := newHarness(t)

	authorized, err := h.service.Authorize(context.Background(), h.request("k12"))
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	id := authorized.Payment.ID
	if _, err := h.service.Capture(context.Background(), id); err != nil {
		t.Fatalf("Capture: %v", err)
	}

	partial, err := h.service.Refund(context.Background(), id, money.MustNew(1000, money.USD), "damaged")
	if err != nil {
		t.Fatalf("partial Refund: %v", err)
	}
	// A partial refund leaves the payment captured: it is still mostly paid.
	if partial.Status != domain.StatusCaptured {
		t.Errorf("status after a partial refund = %s, want captured", partial.Status)
	}

	rest, err := h.service.Refund(context.Background(), id, money.MustNew(3999, money.USD), "rest")
	if err != nil {
		t.Fatalf("second Refund: %v", err)
	}
	if rest.Status != domain.StatusRefunded {
		t.Errorf("status after a full refund = %s, want refunded", rest.Status)
	}
}

func TestRefund_RejectsMoreThanTheCapture(t *testing.T) {
	h := newHarness(t)

	authorized, err := h.service.Authorize(context.Background(), h.request("k13"))
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	id := authorized.Payment.ID
	if _, err := h.service.Capture(context.Background(), id); err != nil {
		t.Fatalf("Capture: %v", err)
	}

	_, err = h.service.Refund(context.Background(), id, money.MustNew(5000, money.USD), "too much")
	if err == nil {
		t.Fatal("Refund above the captured amount = nil, want a rejection")
	}
	if !apperr.Is(err, apperr.CodeInvalidArgument) {
		t.Errorf("code = %q, want %q", apperr.CodeOf(err), apperr.CodeInvalidArgument)
	}
}

func TestRefund_ZeroAmountMeansEverythingRemaining(t *testing.T) {
	h := newHarness(t)

	authorized, err := h.service.Authorize(context.Background(), h.request("k14"))
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	id := authorized.Payment.ID
	if _, err := h.service.Capture(context.Background(), id); err != nil {
		t.Fatalf("Capture: %v", err)
	}

	refunded, err := h.service.Refund(context.Background(), id, money.Money{}, "full")
	if err != nil {
		t.Fatalf("Refund: %v", err)
	}
	if refunded.Status != domain.StatusRefunded {
		t.Errorf("status = %s, want refunded", refunded.Status)
	}
}

// --------------------------------------------------------------------- get

func TestGet_ByIDAndByOrder(t *testing.T) {
	h := newHarness(t)

	authorized, err := h.service.Authorize(context.Background(), h.request("k15"))
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}

	byID, err := h.service.Get(context.Background(), authorized.Payment.ID, "")
	if err != nil {
		t.Fatalf("Get by id: %v", err)
	}
	if byID.ID != authorized.Payment.ID {
		t.Errorf("got %s, want %s", byID.ID, authorized.Payment.ID)
	}

	// The recovery worker uses the order lookup when it does not know whether an
	// earlier authorize attempt landed and therefore has no payment id.
	byOrder, err := h.service.Get(context.Background(), "", authorized.Payment.OrderID)
	if err != nil {
		t.Fatalf("Get by order: %v", err)
	}
	if byOrder.ID != authorized.Payment.ID {
		t.Errorf("got %s, want %s", byOrder.ID, authorized.Payment.ID)
	}

	if _, err := h.service.Get(context.Background(), "", ""); !apperr.Is(err, apperr.CodeInvalidArgument) {
		t.Errorf("Get with no identifiers gave %q, want %q", apperr.CodeOf(err), apperr.CodeInvalidArgument)
	}
	if _, err := h.service.Get(context.Background(), "nope", ""); !apperr.Is(err, apperr.CodeNotFound) {
		t.Errorf("Get of a missing payment gave %q, want %q", apperr.CodeOf(err), apperr.CodeNotFound)
	}
}

// ------------------------------------------------------------------ domain

func TestPayment_StateMachine(t *testing.T) {
	base := domain.NewPaymentInput{
		ID: "p-1", OrderID: "o-1",
		Amount:         money.MustNew(1000, money.USD),
		IdempotencyKey: "k",
	}

	t.Run("decline then void is a no-op success", func(t *testing.T) {
		p, err := domain.NewPayment(base)
		if err != nil {
			t.Fatalf("NewPayment: %v", err)
		}
		if err := p.MarkDeclined(domain.ReasonInsufficientFunds, "ref", time.Now()); err != nil {
			t.Fatalf("MarkDeclined: %v", err)
		}
		if err := p.MarkVoided(time.Now()); err != nil {
			t.Errorf("MarkVoided on a declined payment = %v, want nil", err)
		}
	})

	t.Run("void after capture is refused", func(t *testing.T) {
		p, err := domain.NewPayment(base)
		if err != nil {
			t.Fatalf("NewPayment: %v", err)
		}
		if err := p.MarkAuthorized("ref", time.Now()); err != nil {
			t.Fatalf("MarkAuthorized: %v", err)
		}
		if err := p.MarkCaptured(time.Now()); err != nil {
			t.Fatalf("MarkCaptured: %v", err)
		}
		if err := p.MarkVoided(time.Now()); !errors.Is(err, domain.ErrInvalidTransition) {
			t.Errorf("MarkVoided = %v, want ErrInvalidTransition", err)
		}
	})

	t.Run("capture twice is idempotent", func(t *testing.T) {
		p, err := domain.NewPayment(base)
		if err != nil {
			t.Fatalf("NewPayment: %v", err)
		}
		if err := p.MarkAuthorized("ref", time.Now()); err != nil {
			t.Fatalf("MarkAuthorized: %v", err)
		}
		if err := p.MarkCaptured(time.Now()); err != nil {
			t.Fatalf("first MarkCaptured: %v", err)
		}
		if err := p.MarkCaptured(time.Now()); err != nil {
			t.Errorf("second MarkCaptured = %v, want nil", err)
		}
	})

	t.Run("rejects a non-positive amount", func(t *testing.T) {
		bad := base
		bad.Amount = money.MustNew(0, money.USD)
		if _, err := domain.NewPayment(bad); err == nil {
			t.Error("NewPayment with a zero amount = nil, want a rejection")
		}
	})

	t.Run("rejects a refund in the wrong currency", func(t *testing.T) {
		p, err := domain.NewPayment(base)
		if err != nil {
			t.Fatalf("NewPayment: %v", err)
		}
		if err := p.MarkAuthorized("ref", time.Now()); err != nil {
			t.Fatalf("MarkAuthorized: %v", err)
		}
		if err := p.MarkCaptured(time.Now()); err != nil {
			t.Fatalf("MarkCaptured: %v", err)
		}
		if err := p.MarkRefunded(money.MustNew(100, money.EUR), time.Now()); err == nil {
			t.Error("refund in EUR against a USD payment = nil, want a rejection")
		}
	})
}

func TestRehydrate_RejectsCorruptRows(t *testing.T) {
	valid := domain.Payment{
		ID: "p-1", OrderID: "o-1", IdempotencyKey: "k",
		Amount: money.MustNew(1000, money.USD), Status: domain.StatusAuthorized,
	}

	tests := []struct {
		name   string
		mutate func(*domain.Payment)
	}{
		{"no id", func(p *domain.Payment) { p.ID = "" }},
		{"no order", func(p *domain.Payment) { p.OrderID = "" }},
		{"no key", func(p *domain.Payment) { p.IdempotencyKey = "" }},
		{"unknown status", func(p *domain.Payment) { p.Status = "banana" }},
		{"invalid amount", func(p *domain.Payment) { p.Amount = money.Money{} }},
		{"declined without a reason", func(p *domain.Payment) { p.Status = domain.StatusDeclined }},
		{"refunded more than captured", func(p *domain.Payment) { p.RefundedMinor = 9999 }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bad := valid
			tc.mutate(&bad)

			if _, err := domain.Rehydrate(bad); err == nil {
				t.Error("Rehydrate accepted a corrupt row; a bad record must fail loudly at read time")
			}
		})
	}

	if _, err := domain.Rehydrate(valid); err != nil {
		t.Errorf("Rehydrate of a valid row = %v, want nil", err)
	}
}

func TestStore_RejectsADuplicateIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	mk := func(id, key string) *domain.Payment {
		p, err := domain.NewPayment(domain.NewPaymentInput{
			ID: id, OrderID: "o-" + id,
			Amount:         money.MustNew(100, money.USD),
			IdempotencyKey: key,
		})
		if err != nil {
			t.Fatalf("NewPayment: %v", err)
		}
		return p
	}

	if err := store.Create(ctx, mk("p-1", "shared")); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	err := store.Create(ctx, mk("p-2", "shared"))
	if !errors.Is(err, domain.ErrDuplicateIdempotencyKey) {
		t.Errorf("second Create = %v, want ErrDuplicateIdempotencyKey", err)
	}
}

func TestStore_IsolatesStoredValuesFromCallers(t *testing.T) {
	// A caller that mutates the value it received must not change stored state
	// behind the store's back — otherwise a bug in one handler silently alters
	// what another reads.
	ctx := context.Background()
	store := memory.New()

	p, err := domain.NewPayment(domain.NewPaymentInput{
		ID: "p-1", OrderID: "o-1",
		Amount:         money.MustNew(100, money.USD),
		IdempotencyKey: "k",
	})
	if err != nil {
		t.Fatalf("NewPayment: %v", err)
	}
	if err := store.Create(ctx, p); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := store.GetByID(ctx, "p-1")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	got.Status = domain.StatusRefunded

	again, err := store.GetByID(ctx, "p-1")
	if err != nil {
		t.Fatalf("second GetByID: %v", err)
	}
	if again.Status == domain.StatusRefunded {
		t.Error("mutating a returned payment changed stored state")
	}
}

func TestStore_DeleteReleasesTheKey(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	p, err := domain.NewPayment(domain.NewPaymentInput{
		ID: "p-1", OrderID: "o-1",
		Amount:         money.MustNew(100, money.USD),
		IdempotencyKey: "k",
	})
	if err != nil {
		t.Fatalf("NewPayment: %v", err)
	}
	if err := store.Create(ctx, p); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := store.Delete(ctx, "p-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	replacement, err := domain.NewPayment(domain.NewPaymentInput{
		ID: "p-2", OrderID: "o-2",
		Amount:         money.MustNew(100, money.USD),
		IdempotencyKey: "k",
	})
	if err != nil {
		t.Fatalf("NewPayment: %v", err)
	}
	if err := store.Create(ctx, replacement); err != nil {
		t.Errorf("Create after Delete = %v, want the key to be free again", err)
	}
}
