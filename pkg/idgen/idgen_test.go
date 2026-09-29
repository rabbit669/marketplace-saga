package idgen

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNew_IsWellFormedUUIDv7(t *testing.T) {
	for range 100 {
		id := New()

		if len(id) != 36 {
			t.Fatalf("len(%q) = %d, want 36", id, len(id))
		}

		u, err := uuid.Parse(id)
		if err != nil {
			t.Fatalf("uuid.Parse(%q): %v", id, err)
		}
		if got := u.Version(); got != 7 {
			t.Fatalf("version = %d, want 7 (id %q)", got, id)
		}
		if got := u.Variant(); got != uuid.RFC4122 {
			t.Fatalf("variant = %v, want RFC4122 (id %q)", got, id)
		}
	}
}

func TestNew_IsStrictlyMonotonicUnderLoad(t *testing.T) {
	// The whole point of v7 here: generated order equals sorted order. This
	// deliberately generates far more ids than fit in one millisecond, so it
	// exercises the sequence-borrow path too.
	const n = 20_000

	ids := make([]string, n)
	for i := range ids {
		ids[i] = New()
	}

	for i := 1; i < n; i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("ids are not increasing at index %d:\n  prev = %s\n  curr = %s", i, ids[i-1], ids[i])
		}
	}

	sorted := make([]string, n)
	copy(sorted, ids)
	sort.Strings(sorted)

	for i := range sorted {
		if sorted[i] != ids[i] {
			t.Fatalf("lexical sort differs from generation order at index %d: %s vs %s", i, sorted[i], ids[i])
		}
	}
}

func TestNew_ConcurrentGenerationIsUnique(t *testing.T) {
	const (
		goroutines = 16
		perRoutine = 2_000
	)

	var (
		mu  sync.Mutex
		all = make(map[string]struct{}, goroutines*perRoutine)
		wg  sync.WaitGroup
	)

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()

			local := make([]string, 0, perRoutine)
			for range perRoutine {
				local = append(local, New())
			}

			mu.Lock()
			defer mu.Unlock()
			for _, id := range local {
				if _, dup := all[id]; dup {
					t.Errorf("duplicate id generated: %s", id)
					return
				}
				all[id] = struct{}{}
			}
		}()
	}
	wg.Wait()

	if got := len(all); got != goroutines*perRoutine {
		t.Errorf("unique ids = %d, want %d", got, goroutines*perRoutine)
	}
}

func TestNew_EncodesCreationTime(t *testing.T) {
	// Decoded straight from the first 48 bits rather than through a library
	// helper, so the test asserts the wire format we actually promise.
	before := time.Now().UnixMilli()
	id := New()
	after := time.Now().UnixMilli()

	raw := strings.ReplaceAll(id, "-", "")
	if len(raw) != 32 {
		t.Fatalf("stripped id = %q, want 32 hex chars", raw)
	}

	ms, err := strconv.ParseInt(raw[:12], 16, 64)
	if err != nil {
		t.Fatalf("parse embedded timestamp %q: %v", raw[:12], err)
	}

	if ms < before || ms > after {
		t.Errorf("embedded timestamp %d is outside the generation window [%d, %d]", ms, before, after)
	}
}

func TestRandomToken(t *testing.T) {
	tok, err := RandomToken(32)
	if err != nil {
		t.Fatalf("RandomToken: %v", err)
	}
	if len(tok) != 64 {
		t.Errorf("len(token) = %d, want 64 hex chars for 32 bytes", len(tok))
	}

	other, err := RandomToken(32)
	if err != nil {
		t.Fatalf("RandomToken: %v", err)
	}
	if tok == other {
		t.Error("two tokens must not collide")
	}

	if _, err := RandomToken(0); err == nil {
		t.Error("RandomToken(0) = nil error, want a rejection")
	}
	if _, err := RandomToken(-1); err == nil {
		t.Error("RandomToken(-1) = nil error, want a rejection")
	}
}

func TestRandomInt(t *testing.T) {
	const bound = 10

	seen := make(map[int]bool)
	for range 1_000 {
		n := RandomInt(bound)
		if n < 0 || n >= bound {
			t.Fatalf("RandomInt(%d) = %d, out of range", bound, n)
		}
		seen[n] = true
	}

	// Not a statistical test — just a guard against a generator stuck on zero.
	if len(seen) < 2 {
		t.Errorf("RandomInt produced only %d distinct values, expected a spread", len(seen))
	}

	if got := RandomInt(0); got != 0 {
		t.Errorf("RandomInt(0) = %d, want 0", got)
	}
	if got := RandomInt(-5); got != 0 {
		t.Errorf("RandomInt(-5) = %d, want 0", got)
	}
}

func TestMustParse(t *testing.T) {
	u := MustParse("018f2c1e-6b9a-7c3d-8e4f-1a2b3c4d5e6f")
	if u.String() != "018f2c1e-6b9a-7c3d-8e4f-1a2b3c4d5e6f" {
		t.Errorf("MustParse round-trip = %s", u)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("MustParse on invalid input should panic")
		}
	}()
	MustParse("definitely-not-a-uuid")
}

func TestOrderingKeyMatchesLexicalOrder(t *testing.T) {
	// Call sites that paginate on the id rely on this: the textual form of a
	// UUID v7 is fixed-width and big-endian, so plain string comparison is
	// already chronological.
	first, second := New(), New()

	if OrderingKey(first) >= OrderingKey(second) {
		t.Errorf("OrderingKey must preserve generation order: %s vs %s", first, second)
	}
	if !(first < second) {
		t.Errorf("string comparison must agree with generation order: %s vs %s", first, second)
	}
}
