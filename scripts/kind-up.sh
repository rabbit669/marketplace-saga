#!/usr/bin/env bash
#
# kind-up.sh — bring the whole marketplace stack up on a local kind cluster.
#
# What it does, in order:
#
#   1. creates a kind cluster (reusing it if it already exists, so a second run
#      is a no-op rather than an error);
#   2. builds the seven images from deploy/docker/Dockerfile, one per service;
#   3. loads them into the cluster's node (kind has no registry, and the images
#      are never pushed anywhere);
#   4. applies deploy/k8s with `kubectl apply -k` — the same command `make
#      k8s-apply` runs, so the thing being tested is the thing being shipped;
#   5. waits for the seven service Deployments to roll out, then prints where to
#      find the API and the three UIs.
#
# It is idempotent: re-running it reuses the cluster, rebuilds (Docker's layer
# cache makes that cheap — the Dockerfile is ordered so that the dependency layer
# is shared by all seven services and only the final compile repeats), re-loads the
# images and re-applies the manifests.
#
# Requires: docker, kind, kubectl. Nothing else.
#
# Environment overrides:
#   CLUSTER_NAME         kind cluster name            (default: marketplace)
#   IMAGE_TAG            image tag to build and load  (default: dev — must match
#                        the images: transformer in deploy/k8s/kustomization.yaml)
#   GATEWAY_HOST_PORT    host port for the API        (default: 8080, and it must
#                        match the extraPortMapping below)
#   ROLLOUT_TIMEOUT      how long to wait for the app  (default: 300s)
#
# NetworkPolicy note: kind's default CNI (kindnet) does not implement
# NetworkPolicy, so the policies in deploy/k8s/networkpolicy-*.yaml are accepted by
# the API server and enforced by nothing on this cluster. They are real on Calico,
# Cilium or any managed cluster. Nothing in the stack depends on them to work, so
# this is a fidelity gap rather than a hazard — but do not read "it works on kind"
# as "the policies are correct".

set -euo pipefail

# --------------------------------------------------------------------- settings
CLUSTER_NAME="${CLUSTER_NAME:-marketplace}"
NAMESPACE="marketplace"                       # deploy/k8s/namespace.yaml
K8S_DIR="deploy/k8s"                          # the kustomization `make k8s-apply` applies
IMAGE_PREFIX="${IMAGE_PREFIX:-marketplace}"   # deploy/k8s/kustomization.yaml (images:)
IMAGE_TAG="${IMAGE_TAG:-dev}"
SERVICES=(auth catalog gateway inventory notification order payment)
GATEWAY_NODE_PORT=30080                       # deploy/k8s/gateway.yaml (Service gateway)
HOST_PORT="${GATEWAY_HOST_PORT:-8080}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-300s}"

# Repository root, independent of where the script is called from.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

log()  { printf '\033[36m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[33mwarning:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }

# ------------------------------------------------------------------ preflight
for tool in docker kind kubectl; do
  command -v "$tool" >/dev/null 2>&1 || die "$tool is not installed. See https://kind.sigs.k8s.io/docs/user/quick-start/#installation"
done

docker info >/dev/null 2>&1 || die "the Docker daemon is not running (or not reachable)"

[ -f "$K8S_DIR/kustomization.yaml" ] || die "$K8S_DIR/kustomization.yaml not found — run this from a checkout of the repository"

# Port 8080 is also what the compose stack publishes, so this is the most common
# way for the first run to fail confusingly. A warning rather than an error: the
# check needs lsof, and a false positive should not stop someone who knows what
# they are doing.
if command -v lsof >/dev/null 2>&1 && lsof -iTCP:"$HOST_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  warn "something is already listening on host port $HOST_PORT — the API will not be reachable there."
  warn "Stop it (make down, if it is the compose stack) or run with GATEWAY_HOST_PORT=<free port>."
fi

# --------------------------------------------------------------------- cluster
# The extra port mapping is what makes http://localhost:8080 work: the gateway
# Service is a NodePort on 30080 (there is no ingress controller here), and kind
# publishes that node port on the host.
# The Xs go at the end of the template on purpose: BSD/macOS mktemp only randomises
# a template whose trailing characters are the Xs, while GNU mktemp accepts a
# suffix. `kind --config` does not care about the file extension.
KIND_CONFIG="$(mktemp "${TMPDIR:-/tmp}/kind-marketplace-XXXXXX")"
cleanup() { rm -f "$KIND_CONFIG"; }
trap cleanup EXIT

cat >"$KIND_CONFIG" <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: ${CLUSTER_NAME}
nodes:
  - role: control-plane
    extraPortMappings:
      # Container port 30080 is the gateway's nodePort (deploy/k8s/gateway.yaml).
      # Host port ${HOST_PORT} is what a developer types into a browser.
      - containerPort: ${GATEWAY_NODE_PORT}
        hostPort: ${HOST_PORT}
        listenAddress: "127.0.0.1"
        protocol: TCP
EOF

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  log "kind cluster '$CLUSTER_NAME' already exists — reusing it"
else
  log "creating kind cluster '$CLUSTER_NAME'"
  kind create cluster --name "$CLUSTER_NAME" --config "$KIND_CONFIG" --wait 120s
fi

KUBECTL=(kubectl --context "kind-${CLUSTER_NAME}")

# ---------------------------------------------------------------------- images
# One Dockerfile, seven images: the only thing that varies is the SERVICE build
# arg. VERSION/COMMIT/DATE are the same build metadata the compose stack injects,
# and they surface in /version on each service's health port.
VERSION="${VERSION:-$(git describe --tags --always --dirty 2>/dev/null || echo dev)}"
COMMIT="${COMMIT:-$(git rev-parse --short HEAD 2>/dev/null || echo none)}"
DATE="${DATE:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"

log "building ${#SERVICES[@]} images (${IMAGE_PREFIX}/<service>:${IMAGE_TAG})"
for service in "${SERVICES[@]}"; do
  echo "    building $service"
  # The build context is the repository root: the Dockerfile copies go.work, pkg/
  # and services/, and every service module resolves github.com/marketplace/pkg by
  # relative path, so the whole tree has to be present.
  docker build \
    --file deploy/docker/Dockerfile \
    --build-arg "SERVICE=$service" \
    --build-arg "VERSION=$VERSION" \
    --build-arg "COMMIT=$COMMIT" \
    --build-arg "DATE=$DATE" \
    --tag "${IMAGE_PREFIX}/${service}:${IMAGE_TAG}" \
    .
done

# One `kind load` for all seven: the command takes a list, and a single call is
# faster than seven (each one starts a container and streams a tarball).
log "loading images into the cluster"
image_refs=()
for service in "${SERVICES[@]}"; do
  image_refs+=("${IMAGE_PREFIX}/${service}:${IMAGE_TAG}")
done
kind load docker-image --name "$CLUSTER_NAME" "${image_refs[@]}"

# -------------------------------------------------------------------- manifests
log "applying $K8S_DIR"
"${KUBECTL[@]}" apply -k "$K8S_DIR"

# ------------------------------------------------------------------ wait for it
# The infrastructure is not waited on here; the services are, and their readiness
# probes are what actually depend on it. There is no `depends_on` in Kubernetes —
# a service that starts before PostgreSQL is ready fails its first connection and
# is restarted by the kubelet until it succeeds, which is why the first run of this
# script can take a minute or two longer than the second. Waiting on the rollouts
# is what makes the difference between "applied" and "usable".
log "waiting for the service deployments (timeout ${ROLLOUT_TIMEOUT})"
for service in "${SERVICES[@]}"; do
  if ! "${KUBECTL[@]}" -n "$NAMESPACE" rollout status "deployment/${service}" --timeout="$ROLLOUT_TIMEOUT"; then
    warn "deployment/${service} did not become available in ${ROLLOUT_TIMEOUT}"
    warn "diagnose with:  kubectl -n $NAMESPACE describe deployment/${service}"
    warn "                kubectl -n $NAMESPACE logs deployment/${service} --tail=50"
    exit 1
  fi
done

# ------------------------------------------------------------------------ done
"${KUBECTL[@]}" -n "$NAMESPACE" get pods

cat <<EOF

The stack is up.

  API gateway   http://localhost:${HOST_PORT}          (node port ${GATEWAY_NODE_PORT} on the kind node)
  Prometheus    kubectl -n ${NAMESPACE} port-forward svc/prometheus 9090:9090
  Grafana       kubectl -n ${NAMESPACE} port-forward svc/grafana 3000:3000
  Jaeger        kubectl -n ${NAMESPACE} port-forward svc/jaeger 16686:16686

Only the gateway is published on the host, which is the same rule the compose
stack follows; the three UIs are ClusterIPs and are reached through a port
forward. (On a CNI that enforces NetworkPolicy, a port forward may need the node
CIDR allowed explicitly — see deploy/k8s/README.md.)

A development token, for poking at the API by hand:

  make token

Tear it all down with:

  ./scripts/kind-down.sh

EOF
