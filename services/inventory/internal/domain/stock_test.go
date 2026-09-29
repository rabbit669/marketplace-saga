package domain_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/marketplace/services/inventory/internal/domain"
)

// A fixed clock keeps these tests free of sleeps: expiry is a comparison, and a
// test that sleeps to cross a deadline is a test that fails on a loaded CI box.
var testNow = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func TestStockItemAvailableNeverGoesNegative(t *testing.T) {
	cases := []struct {
		name     string
		item     domain.StockItem
		expected int64
	}{
		{"nothing held", domain.StockItem{Quantity: 10}, 10},
		{"partly held", domain.StockItem{Quantity: 10, Reserved: 4}, 6},
		{"fully held", domain.StockItem{Quantity: 10, Reserved: 10}, 0},
		// Reachable through SetStock: an operator corrects the on-hand count
		// below what has already been promised, and the holds stand.
		{"over-held after a correction", domain.StockItem{Quantity: 3, Reserved: 10}, 0},
		{"empty shelf", domain.StockItem{}, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.item.Available(); got != tc.expected {
				t.Fatalf("Available() = %d, want %d", got, tc.expected)
			}
		})
	}
}

func TestCheckAvailableRejectsMoreThanFree(t *testing.T) {
	item := domain.StockItem{ProductID: "p-1", Quantity: 5, Reserved: 4}

	if err := item.CheckAvailable(1); err != nil {
		t.Fatalf("checking one unit against one free unit: %v", err)
	}

	err := item.CheckAvailable(2)
	if !errors.Is(err, domain.ErrInsufficientStock) {
		t.Fatalf("error = %v, want ErrInsufficientStock", err)
	}
	// The message has to name the product and both numbers: it is what the
	// caller sees and what an operator reads out of the order logs.
	for _, want := range []string{"p-1", "requested 2", "available 1"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestCheckAvailableRejectsNonPositive(t *testing.T) {
	item := domain.StockItem{ProductID: "p-1", Quantity: 5}

	for _, qty := range []int64{0, -1} {
		if err := item.CheckAvailable(qty); !errors.Is(err, domain.ErrInvalidRequest) {
			t.Errorf("CheckAvailable(%d) = %v, want ErrInvalidRequest", qty, err)
		}
	}
}

func TestNewStockItemValidatesInput(t *testing.T) {
	if _, err := domain.NewStockItem("p-1", 5, testNow); err != nil {
		t.Fatalf("valid item rejected: %v", err)
	}

	if _, err := domain.NewStockItem("  ", 5, testNow); !errors.Is(err, domain.ErrInvalidRequest) {
		t.Errorf("blank product id = %v, want ErrInvalidRequest", err)
	}
	if _, err := domain.NewStockItem("p-1", -1, testNow); !errors.Is(err, domain.ErrInvalidRequest) {
		t.Errorf("negative quantity = %v, want ErrInvalidRequest", err)
	}
}

func TestSetQuantityCorrectsOnHandButKeepsHolds(t *testing.T) {
	item, err := domain.NewStockItem("p-1", 100, testNow)
	if err != nil {
		t.Fatalf("build item: %v", err)
	}
	item.Hold(40)

	if err := item.SetQuantity(20, testNow.Add(time.Minute)); err != nil {
		t.Fatalf("set quantity: %v", err)
	}

	if item.Quantity != 20 {
		t.Errorf("Quantity = %d, want 20", item.Quantity)
	}
	// The hold is a promise to a customer mid-checkout. Correcting the shelf
	// count must not cancel it, so Reserved is untouched and Available floors.
	if item.Reserved != 40 {
		t.Errorf("Reserved = %d, want 40: a stock correction must not cancel holds", item.Reserved)
	}
	if got := item.Available(); got != 0 {
		t.Errorf("Available() = %d, want 0", got)
	}
	if !item.UpdatedAt.Equal(testNow.Add(time.Minute)) {
		t.Errorf("UpdatedAt = %v, want the write time", item.UpdatedAt)
	}

	if err := item.SetQuantity(-1, testNow); !errors.Is(err, domain.ErrInvalidRequest) {
		t.Errorf("negative quantity = %v, want ErrInvalidRequest", err)
	}
	if item.Quantity != 20 {
		t.Errorf("a rejected write changed the row: Quantity = %d", item.Quantity)
	}
}

func TestCommitHoldMovesUnitsOutOfBothCounters(t *testing.T) {
	item, err := domain.NewStockItem("p-1", 10, testNow)
	if err != nil {
		t.Fatalf("build item: %v", err)
	}
	item.Hold(4)

	item.CommitHold(4, testNow)

	if item.Quantity != 6 || item.Reserved != 0 {
		t.Fatalf("after commit: quantity %d, reserved %d, want 6 and 0", item.Quantity, item.Reserved)
	}
	if got := item.Available(); got != 6 {
		t.Errorf("Available() = %d, want 6", got)
	}
}

func TestCommitHoldFloorsAtZeroWhenStockWasCorrectedDown(t *testing.T) {
	item, err := domain.NewStockItem("p-1", 10, testNow)
	if err != nil {
		t.Fatalf("build item: %v", err)
	}
	item.Hold(10)
	if err := item.SetQuantity(3, testNow); err != nil {
		t.Fatalf("set quantity: %v", err)
	}

	item.CommitHold(10, testNow)

	// A negative on-hand count is arithmetic nobody can act on; the commit is
	// still honoured, and the row reports an empty shelf.
	if item.Quantity != 0 || item.Reserved != 0 {
		t.Fatalf("after commit: quantity %d, reserved %d, want 0 and 0", item.Quantity, item.Reserved)
	}
}

func TestReleaseHoldNeverInventsNegativeHolds(t *testing.T) {
	item, err := domain.NewStockItem("p-1", 10, testNow)
	if err != nil {
		t.Fatalf("build item: %v", err)
	}
	item.Hold(2)

	item.ReleaseHold(5, testNow)

	if item.Reserved != 0 {
		t.Fatalf("Reserved = %d, want 0", item.Reserved)
	}
	if item.Quantity != 10 {
		t.Fatalf("Quantity = %d, want 10: releasing does not change the shelf", item.Quantity)
	}
	if got := item.Available(); got != 10 {
		t.Errorf("Available() = %d, want 10", got)
	}
}
