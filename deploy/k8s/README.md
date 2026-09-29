# deploy/k8s — the marketplace stack as Kubernetes manifests

`kubectl apply -k deploy/k8s` is the whole interface (that is what `make k8s-apply`
runs; `make k8s-delete` runs `kubectl delete -k deploy/k8s`). Everything lands in the
`marketplace` namespace, and every object declares that namespace itself, so any
single file can also be applied, read or deleted on its own.

This directory describes the same system as `deploy/docker-compose.yml`: seven
services built from one `deploy/docker/Dockerfile`, plus PostgreSQL, Redis, Kafka,
Jaeger, Prometheus and Grafana. The names are deliberately identical to the compose
service names, which is why `auth:9091`, `postgres`, `redis:6379`, `kafka:9092` and
`jaeger:4317` mean the same thing in both stacks and why no `*_GRPC_TARGET` value
had to be rewritten.

## Layout

| File | What it holds |
|---|---|
| `namespace.yaml` | the namespace and its Pod Security level |
| `configmap.yaml` | the shared, non-secret half of `.env.example` |
| `secret.yaml` | **placeholder** credentials and DSNs — replace before any real use |
| `gateway.yaml` | the edge: Deployment + NodePort Service; the probe, resource and securityContext reasoning written out in full |
| `auth.yaml catalog.yaml inventory.yaml order.yaml payment.yaml notification.yaml` | the other six services, Deployment + ClusterIP Service each |
| `postgres.yaml redis.yaml kafka.yaml` | the backing stores (StatefulSet + PVC where state lives) |
| `jaeger.yaml prometheus.yaml grafana.yaml` | the observability stack (Prometheus and Grafana have PVCs; Jaeger is in-memory by design) |
| `hpa.yaml` | the gateway's HorizontalPodAutoscaler |
| `pdb.yaml` | PodDisruptionBudgets for the gateway and the order service |
| `networkpolicy-baseline.yaml` | deny-by-default, DNS, span export |
| `networkpolicy-services.yaml` | one policy per service: who may call it, what it may call |
| `networkpolicy-infra.yaml` | one policy per dependency |
| `kustomization.yaml` | the entry point: the resource list and the seven image tags |

## Ports

Every service serves `/healthz` (liveness), `/readyz` (readiness), `/version` and
`/metrics` on **one dedicated health port** — the same port Prometheus scrapes
(`deploy/prometheus/prometheus.yml`) and the same one the image's
`/service -health-check` mode dials in the compose stack.

| Service | Business port | Health port | Reached from outside? |
|---|---|---|---|
| gateway | 8080 (HTTP) | 9180 | yes — NodePort 30080, mapped to host 8080 by `scripts/kind-up.sh` |
| auth | 9091 (gRPC) | 9191 | no |
| catalog | 9092 (gRPC) | 9192 | no |
| inventory | 9093 (gRPC) | 9193 | no |
| order | 9094 (gRPC) | 9194 | no |
| payment | 9095 (gRPC) | 9195 | no |
| notification | — (Kafka consumer) | 9196 | no |
| postgres | 5432 | — | no |
| redis | 6379 | — | no |
| kafka | 9092 (clients), 9093 (KRaft controller) | — | no |
| jaeger | 16686 (UI), 4317/4318 (OTLP), 14269 (admin) | — | no |
| prometheus | 9090 | — | no |
| grafana | 3000 | — | no |

Probes use `httpGet` against those endpoints rather than compose's
`/service -health-check`, because Kubernetes can speak HTTP itself: the compose
healthcheck has to be a process (the image is distroless and has no shell to run
`curl` from), while a probe does not.

## Reaching the stack

* **API** — `http://localhost:8080` after `./scripts/kind-up.sh` (the kind cluster
  is created with an `extraPortMappings` entry that publishes the gateway's node
  port 30080 on host 8080).
* **UIs** — Grafana, Prometheus and Jaeger are ClusterIPs, exactly as the
  NetworkPolicies in this directory intend ("only the gateway is reachable from
  outside"):

  ```bash
  kubectl -n marketplace port-forward svc/grafana 3000:3000
  kubectl -n marketplace port-forward svc/prometheus 9090:9090
  kubectl -n marketplace port-forward svc/jaeger 16686:16686
  ```

  If a CNI that enforces NetworkPolicy is in use, a port forward arrives from the
  node rather than from a pod and may be refused; the fix is a temporary policy
  allowing the node CIDR to the UI port, not a NodePort.

## Deliberate differences from the compose stack

Each of these is a decision, not an omission:

| Difference | Why |
|---|---|
| No `depends_on` | Kubernetes has no equivalent, and faking it with wait-for-init containers hides which dependency is missing behind an init loop. The services converge by retrying: a pod that cannot reach PostgreSQL fails its readiness probe (and, for `order`, exits and is restarted) until the dependency is up. **Consequence: the first start after `kind-up.sh` can take a minute or two longer than the second.** |
| Published host ports → one NodePort | Compose publishes 8080/5432/6379/29092/16686/9090/3000 onto the host for convenience. Here only the gateway is published, and the `*_HOST_PORT` variables have no counterpart. |
| `KAFKA_ADVERTISED_LISTENERS` has no `EXTERNAL` listener | Compose advertises `EXTERNAL://localhost:29092` for host tools such as `kcat`. In-cluster there is no host listener, and an advertised address is what clients are *told to dial* — leaving an unresolvable one in place fails far more confusingly than removing it. |
| `PGDATA` is a subdirectory of the volume | `initdb` refuses to run in a non-empty directory, and a mount root can contain `lost+found` or be root-owned. Compose mounts the volume root directly, which works for a Docker named volume and is not portable. |
| Redis has an `emptyDir`, not a PVC | Persistence is switched off in the compose command (`--save "" --appendonly no`); a PVC would promise retention the server is configured not to provide. |
| Jaeger has no PVC | `SPAN_STORAGE_TYPE=memory`: traces are a development aid and are dropped on restart by design. |
| `RATE_LIMIT_RPS` / `RATE_LIMIT_BURST` are set under their bare names | They describe the edge as a whole, so the gateway reads them unprefixed — the same spelling `.env.example` and compose use. An earlier revision read the prefixed names, which made the documented settings inert until the code was corrected. |
| `PROMETHEUS_ADDR` is absent | No service reads it: `/metrics` is served on the health port, which is what both scrape configs use. |
| `POSTGRES_HOST`/`POSTGRES_PORT`/`*_DB_NAME` are absent | The DSNs in `marketplace-secrets` are complete values. Repeating the host or the database name in the ConfigMap would create a second place that can disagree with the DSN the services actually read. |

## What was verified, and how

The environment this was authored in has **no cluster and no container runtime** —
`kubectl`, `kind`, `kustomize` and `docker` were all absent — so nothing here has
been applied to a cluster. What was actually run:

| Check | Tool | Result |
|---|---|---|
| YAML parses, every document is a mapping with `apiVersion`/`kind` | Ruby `psych` (all 22 files) | pass |
| Every field is valid against the Kubernetes 1.31 schemas, `-strict` (unknown fields rejected) | `kubeconform` v0.8.0 (built from source), on each file **and** on the rendered output | 53/53 resources valid |
| `kustomize build deploy/k8s` — the same path `kubectl apply -k` takes | `kustomize` v5.8.1 | 53 resources, no warnings |
| Ports, env var names, secret keys, DSN values, `depends_on` ↔ NetworkPolicy flows, prometheus targets, image tags, script/manifest agreement | purpose-written cross-check script (`deploy/docker-compose.yml` + `.env.example` as the reference) | 407 of 409 assertions pass; the two failures are the `.env.example` gap described below |
| `bash -n scripts/kind-up.sh scripts/kind-down.sh` | `bash` 3.2 (macOS) | pass |

The port/env cross-check is the one that matters, and it asserts, per service:

* every `*_GRPC_ADDR` / `*_HEALTH_ADDR` / `*_HTTP_ADDR` value in compose appears in
  the Deployment with the same value, and the set of `containerPort`s equals the set
  of ports those variables name;
* all three probes target the named `health` port, and that port is the one compose
  gives as `*_HEALTH_ADDR`;
* the Service publishes every container port (Prometheus scrapes by service name);
* every environment variable compose sets for a service is reachable by its
  container — from the ConfigMap, from the container's own `env`, or from the
  Secret;
* every `secretKeyRef` / `configMapKeyRef` key exists in the object it names;
* each `*_DB_DSN` in `marketplace-secrets` equals compose's interpolated DSN, and
  the database names match the ones `deploy/postgres/init.sql` creates;
* every `depends_on` edge has a matching egress rule on the caller and a matching
  ingress rule on the callee, and every port named in a policy exists on the target
  pod;
* the Prometheus scrape targets equal the seven health ports;
* the gateway's node port equals the constant in `scripts/kind-up.sh`.

### Findings from the cross-check

1. **`deploy/docker-compose.yml` references `${NOTIFICATION_DB_NAME}`, which
   `.env.example` does not document.** The other five `*_DB_NAME` variables are
   there. Compose falls back to its own default (`notification_db`), which is the
   value used in `secret.yaml`, so the two stacks agree today — but a user setting
   `NOTIFICATION_DB_NAME` in `.env` would change compose and not this deployment.
   Reported rather than "fixed", because `.env.example` is outside the scope of this
   change (a one-line addition there closes it).
2. **`notification` reads the shared `DB_DSN` while compose provides
   `NOTIFICATION_DB_DSN`.** The manifest follows compose, because compose is the
   description of the stack; until the service reads the prefixed name it starts
   with in-memory deduplication and logs a warning about it. The NetworkPolicy
   already allows `notification → postgres:5432`, so nothing else needs to change
   when that naming is settled.
3. `order` reads its dependency targets under its own prefix
   (`ORDER_CATALOG_GRPC_TARGET`), not the bare `CATALOG_GRPC_TARGET` that compose
   sets. It falls back to defaults that are identical to the configured values
   (`catalog:9092`, `inventory:9093`, `payment:9095`), so nothing here is broken —
   it is a naming mismatch worth knowing about before someone changes a target and
   observes no effect.

## Still to verify on a real cluster

Nothing in this directory has been applied, so the following are reasoned
assumptions rather than observed facts. They are listed in the order they are worth
checking:

1. **Third-party image users and filesystem permissions.** The seven application
   images are ours and are pinned to uid 65532; the infrastructure images are not.
   `postgres:16-alpine` and `redis:7-alpine` start as root and drop to their own
   user with `gosu`, which is why they re-add `SETUID`/`SETGID`/`CHOWN`/
   `DAC_OVERRIDE` (and nothing else); Kafka, Jaeger, Prometheus and Grafana are
   assumed to run as their images' documented non-root users (1000, 10001, 65534,
   472) for the `fsGroup` values to be right. Confirm each with
   `kubectl -n marketplace exec … -- id` (or `docker inspect`) before trusting it,
   and check that the PVCs are writable.
2. **`readOnlyRootFilesystem`** is set on the seven services (they write nothing to
   disk: migrations are embedded and applied over the network) and left `false` on
   every infrastructure container, whose entrypoints write config and caches outside
   their data volumes. Tightening those needs a per-image test.
3. **Pod Security Admission** is `baseline` with `warn`/`audit` at `restricted`. The
   seven services pass `restricted`; the infrastructure pods are the reason it is not
   enforced yet. `kubectl -n marketplace get events --field-selector reason=FailedCreate`
   or the `warn` output from `kubectl apply` will say which ones would fail.
4. **NetworkPolicy enforcement.** kind's default CNI does not implement NetworkPolicy,
   so these policies are inert locally. Verify on a Calico/Cilium cluster that the
   kubelet's probes still pass (they arrive from the node's IP, which is why the
   health ports allow `0.0.0.0/0`) and that egress for DNS is genuinely needed — remove
   it and the first dial fails to resolve, which is the failure mode the policy
   comments warn about.
5. **Resource figures.** Requests and limits are a sizing *rule* (request ≈ steady
   state, limit ≈ 3× for Go's live-heap multiple; host-flag ceilings such as Redis's
   `maxmemory 256mb` or Kafka's JVM heap sit deliberately below the limit), not
   measurements. Tune from `container_memory_working_set_bytes` and
   `container_cpu_cfs_throttled`.
6. **The startup budget for `order` (300s) and the HPA's behaviour** (`kubectl -n
   marketplace describe hpa gateway` needs metrics-server; kind does not ship one, so
   the gateway stays at its 2 replicas locally — which is the intended local
   configuration anyway).

## Open items

* `secret.yaml` is a placeholder and must be replaced (see its header).
* `deploy/grafana/dashboards` is not mounted: the provisioning file points at
  `/var/lib/grafana/dashboards` and the volume is an `emptyDir`. The dashboards stay
  in the repository and are installed with
  `kubectl -n marketplace create configmap grafana-dashboards --from-file=deploy/grafana/dashboards`
  (the manifest says where); duplicating them here would let them drift.
* `deploy/prometheus/prometheus.yml`, `deploy/postgres/init.sql` and the two
  Grafana provisioning files are mirrored into ConfigMaps in this directory, because
  kustomize refuses to load files from outside its own directory. Changing one
  without the other is a real risk; a `configMapGenerator` in a parent kustomization
  would remove the duplication but needs `--load-restrictor` loosened.
