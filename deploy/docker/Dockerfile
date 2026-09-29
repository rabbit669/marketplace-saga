# syntax=docker/dockerfile:1.7
#
# One Dockerfile builds every service. SERVICE is the only thing that varies,
# which keeps the build cache warm across services: the dependency layer is
# identical for all seven, so only the final compile step is repeated.
#
#   docker build --build-arg SERVICE=order -f deploy/docker/Dockerfile .
#
# The result is a static binary on distroless: no shell, no package manager,
# no libc. That removes almost the entire class of "attacker gets a shell"
# follow-on activity, and shrinks the image to a few megabytes.

# ----------------------------------------------------------------- build
# Pinned to the build host's architecture so that a cross-platform build
# (--platform linux/amd64 from an arm64 laptop) runs the compiler natively and
# only the *output* is cross-compiled. Go does that without emulation, which
# turns a multi-minute QEMU build into a fast one.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build

ARG SERVICE
ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown

# Supplied by buildx per target platform.
ARG TARGETOS
ARG TARGETARCH

# The workspace root is not a module, and every service module replaces
# github.com/marketplace/pkg with a relative path, so the whole tree has to be
# present for the build to resolve.
WORKDIR /src

# Manifests first: this layer only invalidates when dependencies change.
# go.work.sum is copied too, and the glob keeps the build working on a clone
# where it has not been generated yet. Without it, `go work sync` inside the
# container has to re-derive every checksum, which fails when the module proxy
# is unreachable — a build that depends on the network for something already
# committed to the repository.
COPY go.work go.work.sum* ./
COPY pkg/go.mod pkg/go.sum ./pkg/
COPY services/auth/go.mod services/auth/go.sum ./services/auth/
COPY services/catalog/go.mod services/catalog/go.sum ./services/catalog/
COPY services/gateway/go.mod services/gateway/go.sum ./services/gateway/
COPY services/inventory/go.mod services/inventory/go.sum ./services/inventory/
COPY services/notification/go.mod services/notification/go.sum ./services/notification/
COPY services/order/go.mod services/order/go.sum ./services/order/
COPY services/payment/go.mod services/payment/go.sum ./services/payment/

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go work sync

# Sources last: editing a handler must not re-download the module graph.
COPY pkg/ ./pkg/
COPY services/ ./services/

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eux; \
    test -n "$SERVICE" || { echo "SERVICE build arg is required" >&2; exit 1; }; \
    test -d "services/$SERVICE" || { echo "unknown service: $SERVICE" >&2; exit 1; }; \
    CGO_ENABLED=0 \
    GOOS="${TARGETOS:-linux}" \
    GOARCH="${TARGETARCH:-$(go env GOARCH)}" \
    go build \
        -trimpath \
        -ldflags "-s -w \
            -X github.com/marketplace/pkg/version.Version=${VERSION} \
            -X github.com/marketplace/pkg/version.Commit=${COMMIT} \
            -X github.com/marketplace/pkg/version.Date=${DATE}" \
        -o /out/service \
        "./services/${SERVICE}/cmd/${SERVICE}"

# ------------------------------------------------------------------ test
# A separate stage so that `docker build --target test` runs the unit suite
# inside the same toolchain that produced the binary.
FROM build AS test
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    for m in pkg services/*; do (cd "$m" && go test ./... -count=1); done

# ------------------------------------------------------------------- run
FROM gcr.io/distroless/static-debian12:nonroot

# Build metadata, surfaced by `docker inspect` and by /version.
ARG SERVICE
ARG VERSION=dev
ARG COMMIT=none
LABEL org.opencontainers.image.title="marketplace-${SERVICE}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.source="https://github.com/marketplace"

COPY --from=build /out/service /service

# distroless:nonroot already runs as uid 65532; restating it documents intent
# and survives a base image change.
USER nonroot:nonroot

ENTRYPOINT ["/service"]
