# Architecture Decision Records

Each record captures a decision that is expensive to reverse, the forces acting
on it, and the alternatives that were rejected. They are written at the moment
of the decision and are not edited afterwards — a superseded ADR gets a new
record that supersedes it, so the reasoning trail stays intact.

Format: [Michael Nygard's](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
lightweight ADR.

| # | Title | Status |
|---|---|---|
| [0001](0001-orchestrated-saga-over-choreography.md) | Orchestrated saga instead of choreographed events | Accepted |
| [0002](0002-transactional-outbox.md) | Transactional outbox instead of dual writes | Accepted |
| [0003](0003-idempotency-keys.md) | Idempotency keys on every retryable command | Accepted |
| [0004](0004-grpc-internally-rest-at-the-edge.md) | gRPC internally, REST at the edge | Accepted |
| [0005](0005-database-per-service.md) | One database per service, no cross-service foreign keys | Accepted |
| [0006](0006-uuid-v7-identifiers.md) | UUID v7 identifiers | Accepted |

## Reading order

If you are reviewing this project, these three carry the most weight:

1. **0001** — why the saga exists at all, and what it costs.
2. **0002** — how a state change and its event are kept consistent without a
   distributed transaction.
3. **0003** — how retries are made safe, which is what the saga depends on.

## Adding a record

```
docs/adr/NNNN-short-title.md
```

Copy an existing file as a template. The sections that matter are **Context**
(what forces are acting), **Decision** (what we will do), and **Alternatives
considered** (what we rejected and why — the part that is lost forever if it is
not written down).
