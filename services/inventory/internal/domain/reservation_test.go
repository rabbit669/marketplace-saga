package domain_test

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marketplace/services/inventory/internal/domain"
)

// newReservation is the happy-path helper: one line, default TTL.
func newReservation(t *testing.T, id string, lines ...domain.ReservationLine) *domain.Reservation {
	t.Helper()
	res, err := domain.NewReservation(domain.NewReservationInput{
		ID:      id,
		OrderID: "order-" + id,
		Lines:   lines,
		Now:     testNow,
	})
	if err != nil {
		t.Fatalf("NewReservation(%s): %v", id, err)
	}
	return res
}

func TestReservationStatusTerminal(t *testing.T) {
	cases := map[domain.ReservationStatus]bool{
		domain.ReservationActive:    false,
		domain.ReservationCommitted: true,
		domain.ReservationReleased:  true,
		domain.ReservationExpired:   true,
		// An unknown value must not read as terminal: treating it as such would
		// make the service silently ignore a state it does not understand.
		domain.ReservationStatus("something-else"): false,
	}

	for status, want := range cases {
		if got := status.Terminal(); got != want {
			t.Errorf("%q.Terminal() = %v, want %v", status, got, want)
		}
	}
}

func TestReservationStatusValid(t *testing.T) {
	for _, status := range []domain.ReservationStatus{
		domain.ReservationActive,
		domain.ReservationCommitted,
		domain.ReservationReleased,
		domain.ReservationExpired,
	} {
		if !status.Valid() {
			t.Errorf("%q should be valid", status)
		}
	}
	if domain.ReservationStatus("pending").Valid() {
		t.Error("an unknown status should not be valid")
	}
	if domain.ReservationStatus("").Valid() {
		t.Error("the empty status should not be valid")
	}
}

func TestNewReservationAppliesDefaultTTLAndTimestamps(t *testing.T) {
	res := newReservation(t, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 2})

	if res.Status != domain.ReservationActive {
		t.Errorf("status = %q, want active", res.Status)
	}
	if !res.CreatedAt.Equal(testNow) {
		t.Errorf("CreatedAt = %v, want %v", res.CreatedAt, testNow)
	}
	wantExpiry := testNow.Add(domain.DefaultReservationTTL)
	if !res.ExpiresAt.Equal(wantExpiry) {
		t.Errorf("ExpiresAt = %v, want %v", res.ExpiresAt, wantExpiry)
	}
	if res.ID != "r-1" || res.OrderID != "order-r-1" {
		t.Errorf("id/order = %q/%q, want r-1/order-r-1", res.ID, res.OrderID)
	}
}

func TestNewReservationHonoursExplicitTTL(t *testing.T) {
	res, err := domain.NewReservation(domain.NewReservationInput{
		ID:    "r-1",
		Lines: []domain.ReservationLine{{ProductID: "p-1", Quantity: 1}},
		TTL:   90 * time.Second,
		Now:   testNow,
	})
	if err != nil {
		t.Fatalf("NewReservation: %v", err)
	}
	if want := testNow.Add(90 * time.Second); !res.ExpiresAt.Equal(want) {
		t.Errorf("ExpiresAt = %v, want %v", res.ExpiresAt, want)
	}
}

func TestNewReservationRejectsInvalidInput(t *testing.T) {
	line := []domain.ReservationLine{{ProductID: "p-1", Quantity: 1}}

	tooManyLines := make([]domain.ReservationLine, 0, domain.MaxLinesPerReservation+1)
	for i := 0; i <= domain.MaxLinesPerReservation; i++ {
		tooManyLines = append(tooManyLines, domain.ReservationLine{ProductID: "p-" + strconv.Itoa(i), Quantity: 1})
	}

	cases := []struct {
		name  string
		input domain.NewReservationInput
	}{
		{"no id", domain.NewReservationInput{Lines: line}},
		{"blank id", domain.NewReservationInput{ID: "   ", Lines: line}},
		{"no lines", domain.NewReservationInput{ID: "r-1"}},
		{"too many lines", domain.NewReservationInput{ID: "r-1", Lines: tooManyLines}},
		{"line without product", domain.NewReservationInput{ID: "r-1", Lines: []domain.ReservationLine{{Quantity: 1}}}},
		{"zero quantity", domain.NewReservationInput{ID: "r-1", Lines: []domain.ReservationLine{{ProductID: "p-1"}}}},
		{"negative quantity", domain.NewReservationInput{ID: "r-1", Lines: []domain.ReservationLine{{ProductID: "p-1", Quantity: -5}}}},
		{"quantity above the per-line maximum", domain.NewReservationInput{
			ID:    "r-1",
			Lines: []domain.ReservationLine{{ProductID: "p-1", Quantity: domain.MaxQuantityPerLine + 1}},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The clock is left zero on purpose: validation must happen before
			// anything depends on "now", or a malformed request would be
			// rejected inconsistently depending on when it arrived.
			_, err := domain.NewReservation(tc.input)
			if !errors.Is(err, domain.ErrInvalidRequest) {
				t.Fatalf("error = %v, want ErrInvalidRequest", err)
			}
		})
	}
}

func TestNewReservationAcceptsTheLineLimitExactly(t *testing.T) {
	// The order service's cart limit and this limit are the same number, so a
	// full cart must be reservable. An off-by-one here breaks a legitimate
	// order, which is why the boundary is asserted rather than assumed.
	lines := make([]domain.ReservationLine, 0, domain.MaxLinesPerReservation)
	for i := 0; i < domain.MaxLinesPerReservation; i++ {
		lines = append(lines, domain.ReservationLine{ProductID: "p-" + strconv.Itoa(i), Quantity: 1})
	}

	if _, err := domain.NewReservation(domain.NewReservationInput{ID: "r-1", Lines: lines, Now: testNow}); err != nil {
		t.Fatalf("a reservation at the line limit was rejected: %v", err)
	}
}

func TestNewReservationMergesRepeatedProducts(t *testing.T) {
	res := newReservation(t, "r-1",
		domain.ReservationLine{ProductID: "p-2", Quantity: 3},
		domain.ReservationLine{ProductID: "p-1", Quantity: 1},
		domain.ReservationLine{ProductID: "p-2", Quantity: 4},
	)

	want := []domain.ReservationLine{
		{ProductID: "p-2", Quantity: 7},
		{ProductID: "p-1", Quantity: 1},
	}
	if len(res.Lines) != len(want) {
		t.Fatalf("lines = %+v, want %+v", res.Lines, want)
	}
	for i := range want {
		if res.Lines[i] != want[i] {
			t.Fatalf("lines = %+v, want %+v (first-seen order preserved)", res.Lines, want)
		}
	}
}

func TestNewReservationChecksTheMergedTotal(t *testing.T) {
	// Each line is within the per-line bound on its own, but the merged total
	// is what would actually be held, so it is the merged total that is bound.
	_, err := domain.NewReservation(domain.NewReservationInput{
		ID: "r-1",
		Lines: []domain.ReservationLine{
			{ProductID: "p-1", Quantity: 600},
			{ProductID: "p-1", Quantity: 600},
		},
		Now: testNow,
	})
	if !errors.Is(err, domain.ErrInvalidRequest) {
		t.Fatalf("error = %v, want ErrInvalidRequest", err)
	}
	if !strings.Contains(err.Error(), "several lines") {
		t.Errorf("error %q should explain that the lines were merged", err)
	}
}

func TestReservationCloneDoesNotAliasLines(t *testing.T) {
	res := newReservation(t, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 1})

	clone := res.Clone()
	clone.Lines[0].Quantity = 99
	clone.Status = domain.ReservationReleased

	if res.Lines[0].Quantity != 1 {
		t.Fatalf("mutating a clone changed the original: %+v", res.Lines)
	}
	if res.Status != domain.ReservationActive {
		t.Fatalf("mutating a clone changed the original status: %q", res.Status)
	}
}

func TestCommitIsIdempotentAndDeductsExactlyOnce(t *testing.T) {
	res := newReservation(t, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 2})

	applied, err := res.Commit(testNow.Add(time.Second))
	if err != nil || !applied {
		t.Fatalf("first commit: applied=%v err=%v, want true/nil", applied, err)
	}
	if res.Status != domain.ReservationCommitted {
		t.Fatalf("status = %q, want committed", res.Status)
	}

	// The saga's commit step is retried by the recovery worker, so the second
	// call must report "nothing to do" rather than deduct a second time.
	applied, err = res.Commit(testNow.Add(2 * time.Second))
	if err != nil {
		t.Fatalf("second commit: %v", err)
	}
	if applied {
		t.Fatal("second commit reported a transition: stock would be deducted twice")
	}
	if res.Status != domain.ReservationCommitted {
		t.Fatalf("status = %q, want committed", res.Status)
	}
}

func TestCommitRefusesAHoldThatIsNoLongerHeld(t *testing.T) {
	t.Run("released", func(t *testing.T) {
		res := newReservation(t, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 1})
		if _, err := res.Release("checkout_failed", testNow); err != nil {
			t.Fatalf("release: %v", err)
		}

		if _, err := res.Commit(testNow); !errors.Is(err, domain.ErrInvalidRequest) {
			t.Fatalf("error = %v, want ErrInvalidRequest", err)
		}
		if res.Status != domain.ReservationReleased {
			t.Fatalf("status = %q, want released: a refused commit must not change state", res.Status)
		}
	})

	t.Run("expired", func(t *testing.T) {
		res := newReservation(t, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 1})
		if !res.Expire(res.ExpiresAt) {
			t.Fatal("expire at the deadline: want true")
		}

		_, err := res.Commit(testNow.Add(time.Hour))
		if !errors.Is(err, domain.ErrInvalidRequest) {
			t.Fatalf("error = %v, want ErrInvalidRequest", err)
		}
	})

	t.Run("past its deadline while still active", func(t *testing.T) {
		// The worker has not swept yet, but the hold has lapsed. Committing it
		// would sell units that are already back in the pool.
		res := newReservation(t, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 1})

		_, err := res.Commit(res.ExpiresAt.Add(time.Nanosecond))
		if !errors.Is(err, domain.ErrInvalidRequest) {
			t.Fatalf("error = %v, want ErrInvalidRequest", err)
		}
		if res.Status != domain.ReservationActive {
			t.Fatalf("status = %q, want active: a refused commit must not change state", res.Status)
		}
	})
}

func TestCommitExactlyAtTheDeadlineIsRefused(t *testing.T) {
	// The boundary belongs to the hold: at ExpiresAt the reservation is late,
	// and the conservative reading is the safe one because a commit cannot be
	// undone by releasing.
	res := newReservation(t, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 1})

	if _, err := res.Commit(res.ExpiresAt); !errors.Is(err, domain.ErrInvalidRequest) {
		t.Fatalf("error = %v, want ErrInvalidRequest", err)
	}
}

func TestReleaseIsIdempotentAndRecordsTheReason(t *testing.T) {
	res := newReservation(t, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 2})

	applied, err := res.Release("checkout_failed:authorize_payment", testNow.Add(time.Second))
	if err != nil || !applied {
		t.Fatalf("first release: applied=%v err=%v, want true/nil", applied, err)
	}
	if res.ReleaseReason != "checkout_failed:authorize_payment" {
		t.Errorf("ReleaseReason = %q, want the reason it was given", res.ReleaseReason)
	}

	// A compensation runs when the caller does not know whether the forward
	// step applied, so "already released" is a success with nothing to do.
	applied, err = res.Release("checkout_failed", testNow.Add(2*time.Second))
	if err != nil {
		t.Fatalf("second release: %v", err)
	}
	if applied {
		t.Fatal("second release reported a transition")
	}
	if res.ReleaseReason != "checkout_failed:authorize_payment" {
		t.Errorf("ReleaseReason was overwritten by the replay: %q", res.ReleaseReason)
	}
}

func TestReleaseOfAnExpiredHoldIsANoOp(t *testing.T) {
	res := newReservation(t, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 1})
	if !res.Expire(res.ExpiresAt) {
		t.Fatal("expire: want true")
	}

	applied, err := res.Release("checkout_failed", testNow.Add(time.Hour))
	if err != nil {
		t.Fatalf("release of an expired hold: %v", err)
	}
	if applied {
		t.Fatal("reported returning stock that expiry had already returned")
	}
	// The status stays expired: "nobody came back" and "the saga rolled back"
	// are different incidents, and overwriting the first with the second loses
	// the only record of it.
	if res.Status != domain.ReservationExpired {
		t.Fatalf("status = %q, want expired", res.Status)
	}
}

func TestReleaseRefusesACommittedReservation(t *testing.T) {
	res := newReservation(t, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 1})
	if _, err := res.Commit(testNow); err != nil {
		t.Fatalf("commit: %v", err)
	}

	_, err := res.Release("checkout_failed", testNow)
	if !errors.Is(err, domain.ErrInvalidRequest) {
		t.Fatalf("error = %v, want ErrInvalidRequest", err)
	}
	if res.Status != domain.ReservationCommitted {
		t.Fatalf("status = %q, want committed", res.Status)
	}
}

func TestExpireOnlyMovesAnActiveOverdueHold(t *testing.T) {
	cases := []struct {
		name      string
		setup     func(t *testing.T) *domain.Reservation
		now       time.Time
		wantMoved bool
	}{
		{
			name: "overdue",
			setup: func(t *testing.T) *domain.Reservation {
				return newReservation(t, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 1})
			},
			now:       testNow.Add(domain.DefaultReservationTTL + time.Second),
			wantMoved: true,
		},
		{
			name: "exactly at the deadline",
			setup: func(t *testing.T) *domain.Reservation {
				return newReservation(t, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 1})
			},
			now:       testNow.Add(domain.DefaultReservationTTL),
			wantMoved: true,
		},
		{
			name: "not due yet",
			setup: func(t *testing.T) *domain.Reservation {
				return newReservation(t, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 1})
			},
			now:       testNow.Add(domain.DefaultReservationTTL - time.Nanosecond),
			wantMoved: false,
		},
		{
			name: "already committed",
			setup: func(t *testing.T) *domain.Reservation {
				res := newReservation(t, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 1})
				if _, err := res.Commit(testNow); err != nil {
					t.Fatalf("commit: %v", err)
				}
				return res
			},
			now:       testNow.Add(time.Hour),
			wantMoved: false,
		},
		{
			name: "already released",
			setup: func(t *testing.T) *domain.Reservation {
				res := newReservation(t, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 1})
				if _, err := res.Release("checkout_failed", testNow); err != nil {
					t.Fatalf("release: %v", err)
				}
				return res
			},
			now:       testNow.Add(time.Hour),
			wantMoved: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := tc.setup(t)
			before := res.Status

			if got := res.Expire(tc.now); got != tc.wantMoved {
				t.Fatalf("Expire = %v, want %v", got, tc.wantMoved)
			}
			if tc.wantMoved && res.Status != domain.ReservationExpired {
				t.Fatalf("status = %q, want expired", res.Status)
			}
			if !tc.wantMoved && res.Status != before {
				t.Fatalf("status changed from %q to %q, want it unchanged", before, res.Status)
			}
		})
	}
}

func TestIsExpiredIsFalseForTerminalReservations(t *testing.T) {
	res := newReservation(t, "r-1", domain.ReservationLine{ProductID: "p-1", Quantity: 1})
	if _, err := res.Commit(testNow); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// "Expired" describes a hold that lapsed. A committed sale is not late, and
	// reporting it as expired would invite a second release of sold stock.
	if res.IsExpired(testNow.Add(365 * 24 * time.Hour)) {
		t.Fatal("a committed reservation reported itself as expired")
	}
}

func TestRehydrateRejectsCorruptStoredReservations(t *testing.T) {
	valid := domain.Reservation{
		ID:        "r-1",
		Lines:     []domain.ReservationLine{{ProductID: "p-1", Quantity: 1}},
		Status:    domain.ReservationCommitted,
		CreatedAt: testNow,
		ExpiresAt: testNow.Add(time.Minute),
	}

	restored, err := domain.Rehydrate(valid)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}
	if restored.Status != domain.ReservationCommitted {
		t.Fatalf("status = %q, want committed: a stored reservation has moved on", restored.Status)
	}

	cases := map[string]domain.Reservation{
		"no id":          {Lines: valid.Lines, Status: domain.ReservationActive},
		"unknown status": {ID: "r-1", Lines: valid.Lines, Status: "pending"},
		"no lines":       {ID: "r-1", Status: domain.ReservationActive},
		"bad line":       {ID: "r-1", Status: domain.ReservationActive, Lines: []domain.ReservationLine{{ProductID: "", Quantity: 1}}},
	}
	for name, stored := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := domain.Rehydrate(stored); !errors.Is(err, domain.ErrInvalidRequest) {
				t.Fatalf("error = %v, want ErrInvalidRequest", err)
			}
		})
	}
}
