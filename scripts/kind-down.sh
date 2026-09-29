#!/usr/bin/env bash
#
# kind-down.sh — tear down the cluster scripts/kind-up.sh created.
#
# Idempotent by design: running it twice is not an error, and neither is running it
# on a machine where the cluster was never created. Cluster creation is expensive
# (tens of seconds) and deletion is the moment people run things twice out of
# impatience, so "already gone" reports success.
#
# Environment overrides:
#   CLUSTER_NAME    the kind cluster to delete         (default: marketplace)
#   IMAGE_PREFIX    image name prefix to remove        (default: marketplace)
#   IMAGE_TAG       image tag to remove                (default: dev)
#   PURGE_IMAGES=1  also remove the seven local images  (default: keep them)
#
# The images are kept by default: they are harmless, they are what docker's layer
# cache will reuse on the next `kind-up.sh` run, and removing them silently would
# turn a fast re-create into a slow one.

set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-marketplace}"
IMAGE_PREFIX="${IMAGE_PREFIX:-marketplace}"
IMAGE_TAG="${IMAGE_TAG:-dev}"
SERVICES=(auth catalog gateway inventory notification order payment)

log()  { printf '\033[36m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[33mwarning:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }

command -v kind >/dev/null 2>&1 || die "kind is not installed, so there is nothing to delete with it"

# --------------------------------------------------------------------- cluster
# `kind delete cluster` also removes the kubeconfig context (kind-<name>), which is
# why nothing here touches ~/.kube/config directly.
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  log "deleting kind cluster '$CLUSTER_NAME'"
  kind delete cluster --name "$CLUSTER_NAME"
else
  log "kind cluster '$CLUSTER_NAME' does not exist — nothing to delete"
fi

# ---------------------------------------------------------------------- images
if [ "${PURGE_IMAGES:-0}" = "1" ]; then
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    log "removing local ${IMAGE_PREFIX}/<service>:${IMAGE_TAG} images"
    for service in "${SERVICES[@]}"; do
      # -f is deliberately not used: if an image is in use by a running container,
      # that is a fact worth seeing rather than overriding.
      docker rmi "${IMAGE_PREFIX}/${service}:${IMAGE_TAG}" >/dev/null 2>&1 \
        || warn "could not remove ${IMAGE_PREFIX}/${service}:${IMAGE_TAG} (already gone, or in use)"
    done
  else
    warn "Docker is not reachable; skipping image removal"
  fi
fi

cat <<EOF

Cluster removed.

Still on the machine:
  * the Kubernetes contexts for other clusters, untouched;
  * the local images (rerun with PURGE_IMAGES=1 to remove them);
  * nothing else — deploy/k8s is declarative, and the PersistentVolumes went with
    the cluster. PostgreSQL, Kafka, Prometheus and Grafana data is gone; that is
    the point of a kind cluster, not a side effect to be surprised by.

Bring it back with:

  ./scripts/kind-up.sh

EOF
