# ADR-0004: gRPC internally, REST at the edge

- **Status:** Accepted
- **Date:** 2026-01-15

## Context

Seven services call each other, and one of them is called by browsers and mobile
clients. Internal calls are high-volume, strongly typed, between code we own and
deploy together. Edge calls are the opposite: low-volume, consumed by clients we
do not control, and must stay backward compatible for as long as old app
versions exist in the wild.

Choosing one protocol for both means compromising one of them.

## Decision

- **Internal:** gRPC with protobuf, defined in `proto/`, generated into
  `pkg/proto/`. Contract changes are checked by `buf breaking` in CI.
- **Edge:** REST/JSON at the gateway only. The gateway is the sole service
  published on the host network; every other service listens on an internal
  address.

## Rationale

**Internal.** Protobuf gives a schema and generated clients, so a field renamed
in one service fails to compile in its callers rather than producing a silent
`null` in production. HTTP/2 multiplexing removes the head-of-line blocking that
makes many parallel JSON calls expensive, and binary framing is materially
smaller for the batch calls the order service makes.

**Edge.** Browsers speak REST and JSON well and gRPC-web badly. A public protobuf
contract would freeze every message shape forever, because an old mobile build
cannot be redeployed. REST tolerates additive change: adding an optional field
breaks nobody.

**The gateway is the translation seam.** `apperr` classifies a failure once,
transport-neutrally; the gRPC layer and the HTTP layer each map that
classification to their own vocabulary (`codes.NotFound` / `404`). Domain code
imports neither `grpc` nor `net/http`, which is what keeps this a one-line change
per new transport.

## Consequences

**Positive**

- Contract violations are compile errors, not runtime surprises.
- Internal format changes do not require a public API version.
- The gateway is the only place that needs rate limiting, CORS and auth
  middleware.

**Negative**

- Two serialisation paths and one translation layer to maintain.
- Debugging internal calls needs `grpcurl` rather than `curl`.
- Streaming is natural internally and awkward at the edge, which is why the
  gateway is request/response only and long-running work is polled via
  `GetSagaState`.

## Alternatives considered

**REST everywhere.** Rejected: no schema enforcement, and the batch calls the
order service makes would be either chatty or untyped.

**GraphQL at the edge.** Rejected: it solves a client-driven-fetching problem we
do not have, and adds a query-cost and depth-limiting surface that must be
secured.

**gRPC-web at the edge.** Rejected: browser support requires a proxy, and the
error-handling story for a consumer-facing API is worse than HTTP status codes.
