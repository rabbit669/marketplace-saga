// Package provider contains the payment provider adapters.
package provider

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/marketplace/pkg/idgen"

	"github.com/marketplace/services/payment/internal/domain"
)

// Simulated is a stand-in for a real payment provider.
//
// It is not a test double. It runs in the deployed service, and its failure rate
// is configuration. That is deliberate: in development the checkout saga's
// compensation path must run on ordinary traffic, not only under test. A demo
// where every payment succeeds proves nothing about the part of the system most
// likely to be wrong.
type Simulated struct {
	failureRate float64
	latency     time.Duration
	rand        func() float64
}

// SimulatedConfig configures the simulated provider.
type SimulatedConfig struct {
	// FailureRate is the probability that an authorization is declined, in
	// [0, 1]. Zero means always approve.
	FailureRate float64
	// Latency is how long the provider pretends to think. It is not decoration:
	// it is what makes concurrent checkouts overlap, which is how the
	// reservation race and the idempotency claim get exercised.
	Latency time.Duration
	// Rand overrides the randomness source, for deterministic tests.
	Rand func() float64
}

// NewSimulated builds a simulated provider.
func NewSimulated(cfg SimulatedConfig) *Simulated {
	if cfg.FailureRate < 0 {
		cfg.FailureRate = 0
	}
	if cfg.FailureRate > 1 {
		cfg.FailureRate = 1
	}
	if cfg.Rand == nil {
		cfg.Rand = rand.Float64
	}
	return &Simulated{
		failureRate: cfg.FailureRate,
		latency:     cfg.Latency,
		rand:        cfg.Rand,
	}
}

// Magic payment method tokens, so that a demo or an end-to-end test can demand a
// specific outcome instead of hoping the random failure rate cooperates. They
// are part of the simulated provider's interface and are documented in the
// README next to the demo commands.
const (
	// TokenDecline always produces a permanent decline.
	TokenDecline = "pm_test_decline"
	// TokenTimeout always produces a transient decline, which the saga retries
	// before giving up and compensating.
	TokenTimeout = "pm_test_timeout"
	// TokenApprove always approves, whatever the configured failure rate.
	TokenApprove = "pm_test_approve"
)

// Authorize implements domain.Provider.
//
// It honours the context deadline during its simulated latency, so that a
// caller who has already given up does not keep a goroutine alive waiting for a
// decision nobody will read.
func (s *Simulated) Authorize(ctx context.Context, req domain.AuthorizeRequest) (domain.ProviderDecision, error) {
	if s.latency > 0 {
		timer := time.NewTimer(s.latency)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return domain.ProviderDecision{}, fmt.Errorf("provider call abandoned: %w", ctx.Err())
		case <-timer.C:
		}
	}

	switch req.PaymentMethodToken {
	case TokenApprove:
		return approved(req), nil

	case TokenDecline:
		return declined(req, domain.ReasonInsufficientFunds), nil

	case TokenTimeout:
		return declined(req, domain.ReasonProviderTimeout), nil
	}

	if s.rand() < s.failureRate {
		return declined(req, s.pickDeclineReason()), nil
	}
	return approved(req), nil
}

// approved builds an approval with a provider-side reference.
func approved(req domain.AuthorizeRequest) domain.ProviderDecision {
	return domain.ProviderDecision{
		Approved:          true,
		ProviderReference: "psp_" + idgen.New()[:18],
	}
}

func declined(req domain.AuthorizeRequest, reason domain.DeclineReason) domain.ProviderDecision {
	return domain.ProviderDecision{
		Approved: false,
		// Real providers issue a reference for a declined attempt as well.
		// Keeping one here matters downstream: the saga records it, and its
		// void step then has something to act on.
		ProviderReference: "psp_declined_" + req.OrderID,
		DeclineReason:     reason,
	}
}

// pickDeclineReason chooses a decline reason with realistic weighting.
//
// Most declines in the real world are permanent — the card has no money, or it
// expired. Only a minority are transient provider problems. Weighting the
// simulation the same way means the default development experience exercises the
// common path, and transient failures appear often enough to be noticed but not
// so often that they mask everything else.
func (s *Simulated) pickDeclineReason() domain.DeclineReason {
	if s.rand() < 0.7 {
		switch {
		case s.rand() < 0.75:
			return domain.ReasonInsufficientFunds
		case s.rand() < 0.6:
			return domain.ReasonCardExpired
		default:
			return domain.ReasonFraudSuspected
		}
	}

	if s.rand() < 0.5 {
		return domain.ReasonProviderTimeout
	}
	return domain.ReasonProviderUnavailable
}
