# ADR-0006: UUID v7 identifiers

- **Status:** Accepted
- **Date:** 2026-01-15

## Context

Every entity needs a primary key. The candidates, and what they cost:

| Option | Insert locality | Reveals count | Usable offline | Sortable |
|---|---|---|---|---|
| `BIGSERIAL` | excellent | yes | no | yes |
| UUID v4 (random) | poor | no | yes | no |
| UUID v7 (time-ordered) | good | no | yes | yes |

The order service assigns identifiers before writing to the database, because a
saga must be able to name its own instance before it exists and pass that name to
other services. A database-generated sequence cannot do that.

## Decision

Identifiers are **UUID v7**, generated in `pkg/idgen` as a string, with a
monotonic counter in the `rand_a` field so that ids issued within the same
millisecond still sort in creation order.

## Rationale

**Insert locality.** A UUID v4 primary key is uniformly random, so every insert
lands on a different B-tree page. On a table with a large working set this turns
sequential inserts into random I/O and keeps the index permanently larger than it
needs to be. UUID v7 puts the timestamp in the high 48 bits, so inserts cluster
at the end of the index exactly like a sequence would.

**Generated before the write.** A saga id is needed before the row exists: it is
passed to inventory as the reservation id (which is what makes reserving
idempotent — see ADR-0003) and to payment as the order reference. Generating the
id in the application is therefore not a preference but a requirement.

**Not enumerable.** A serial key leaks business volume: an attacker who creates
an order and sees `1042` knows how many orders exist, and can walk the range.
UUID v7 exposes only a millisecond timestamp, which is information an attacker
can usually get anyway.

**Sortable, so cursor pagination needs no extra column.** Listing orders by
`created_at` requires an index on that column and a tiebreaker for equal
timestamps. Because the textual form of a UUID v7 is fixed-width and big-endian,
plain string comparison *is* chronological — the id is its own sort key, and
`WHERE id < $cursor ORDER BY id DESC` uses the primary key index directly.

**Monotonic within a millisecond.** A pure timestamp-and-random v7 does not
guarantee ordering for ids created in the same tick. `pkg/idgen` keeps a 12-bit
counter and borrows from the next millisecond when it overflows, so generation
order and sort order always agree. This is tested directly, including the
overflow path.

## Consequences

### Positive

- Index behaviour matches a serial key while keeping application-side generation.
- Pagination is simpler and faster: no second index, no tiebreaker column.
- The creation time is recoverable from the id, which is occasionally useful
  during an incident when a record's timestamps are themselves suspect.

### Negative

- **16 bytes instead of 8**, in every table, every index and every foreign key.
  Real, and accepted; the working set is smaller than the row count suggests
  because the index stays compact.
- **The embedded timestamp leaks creation time to anyone holding an id.** For
  this domain that is not sensitive. It would be for, say, a whistleblowing
  system, and the choice would have to be revisited there.
- **Clock skew can misorder ids across machines.** Two hosts with clocks a few
  milliseconds apart can produce ids whose order does not match real time. The
  guarantee that matters — uniqueness — is unaffected, and within one process
  ordering is exact.

## Alternatives considered

**`BIGSERIAL`.** Rejected: cannot be generated before the write, which the saga
requires.

**UUID v4.** Rejected: the index-locality cost is paid on every insert forever,
and the pagination workaround (a separate `created_at` index plus a tiebreaker)
is real complexity to avoid a problem that v7 simply does not have.

**ULID.** Nearly equivalent — also 128-bit, also time-ordered. Rejected only
because UUID v7 is an RFC and is natively understood by PostgreSQL, Kafka tooling
and the `uuid` packages already in use. There is no meaningful technical
difference.

**Snowflake ids.** Rejected: they encode a machine identifier, which means the
generator needs coordination about which node owns which id. UUID v7 needs none.
