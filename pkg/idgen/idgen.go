// Package idgen produces identifiers for domain entities.
//
// Identifiers are UUID v7: a 48-bit big-endian millisecond timestamp followed
// by random bits. Compared with UUID v4 this buys two things that matter at
// scale — ids sort by creation time, so an index on the primary key stays
// clustered instead of scattering inserts across the whole B-tree, and the
// embedded timestamp makes "when was this created?" answerable from the id
// alone during incident debugging.
package idgen

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	mu       sync.Mutex
	lastMS   int64
	sequence uint16
)

// maxSequence is the largest value the 12-bit rand_a field can hold. It bounds
// how many ids can be issued within a single millisecond before the generator
// has to borrow from the next one.
const maxSequence = 0x0FFF

// New returns a new UUID v7 encoded as a canonical string.
//
// Within a single millisecond the generator keeps an internal counter and
// bumps the version's random field so that ids produced in the same tick stay
// strictly increasing. Without that, two ids created microseconds apart could
// sort in the wrong order, which breaks cursor pagination that keys off the id.
func New() string {
	mu.Lock()
	now := time.Now().UnixMilli()

	switch {
	case now > lastMS:
		lastMS = now
		sequence = 0
	default:
		// Same millisecond (or a clock that stepped backwards): keep the
		// timestamp monotonic and advance the sequence instead.
		sequence++
		if sequence > maxSequence {
			// The 12-bit sequence field is exhausted. Borrow from the next
			// millisecond rather than wrapping, which would emit ids that sort
			// before their predecessors.
			lastMS++
			sequence = 0
		}
		now = lastMS
	}
	seq := sequence
	mu.Unlock()

	return formatV7(now, seq)
}

// formatV7 lays out the UUID v7 fields by hand so that the intra-millisecond
// sequence can occupy the rand_a field, which is what guarantees monotonicity.
func formatV7(ms int64, seq uint16) string {
	var b [16]byte

	// 48-bit big-endian timestamp.
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)

	// 12-bit rand_a carries the sequence so ordering is preserved.
	b[6] = 0x70 | byte(seq>>8)&0x0f // version 7 in the high nibble
	b[7] = byte(seq)

	// 62 bits of randomness for the remainder.
	if _, err := rand.Read(b[8:]); err != nil {
		// crypto/rand does not fail in practice; if it somehow does, a
		// non-unique tail is still better than panicking in a request path.
		fallback := time.Now().UnixNano()
		for i := 8; i < 16; i++ {
			b[i] = byte(fallback >> (8 * (i - 8)))
		}
	}

	// Variant bits (RFC 4122: 10xxxxxx).
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]),
		hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]),
		hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16]),
	)
}

// NewUUID returns a uuid.UUID rather than a string, for callers that store it
// as a native type.
func NewUUID() uuid.UUID {
	u, err := uuid.Parse(New())
	if err != nil {
		// Unreachable: New always produces a well-formed UUID.
		return uuid.New()
	}
	return u
}

// MustParse parses s and panics on failure. Use it only for compile-time
// constants in tests and fixtures.
func MustParse(s string) uuid.UUID {
	u, err := uuid.Parse(s)
	if err != nil {
		panic(fmt.Sprintf("idgen: invalid uuid %q: %v", s, err))
	}
	return u
}

// OrderingKey returns a sortable key for a UUID v7 string. Because the textual
// form is fixed width and big-endian, the string itself already sorts
// correctly; this exists to make that guarantee explicit at call sites that
// rely on it.
func OrderingKey(id string) string { return id }

// RandomToken returns n cryptographically random bytes, hex encoded. Used for
// refresh tokens and idempotency keys, where unpredictability matters more
// than ordering.
func RandomToken(n int) (string, error) {
	if n <= 0 {
		return "", fmt.Errorf("idgen: token length must be positive, got %d", n)
	}
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("idgen: read random bytes: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// RandomInt returns a uniform random integer in [0, n). It returns 0 when n is
// not positive.
func RandomInt(n int) int {
	if n <= 0 {
		return 0
	}
	v, err := rand.Int(rand.Reader, big.NewInt(int64(n)))
	if err != nil {
		return 0
	}
	return int(v.Int64())
}
