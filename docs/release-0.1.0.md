# Release plan: 0.1.0

This document defines what `watchd` 0.1.0 is, which issues block it, which are deliberately deferred, and in what order the work lands. It is a planning document: when it disagrees with [the v0 semantics contract](semantics.md), the contract wins.

## What 0.1.0 is

**A deployable release.** An outside team can install `watchd`, point it at a PostgreSQL database, run a Go application against it, and know at every moment whether their projection is fresh or stale — with the operational and security model written down and the artifacts verifiable.

Deployable means two shapes, from one container image:

- **A single daemon**: the `watchd` binary, or a Docker Compose deployment next to PostgreSQL.
- **Kubernetes**: a Helm chart, so applications inside the cluster sync projections from a `watchd` Service.

It is explicitly not a scale or high-availability release. 0.1.0 is single-process soft state, as [the architecture tour](architecture.md) already states, and makes no throughput claim. In Kubernetes this means one replica per source: one logical slot has one consumer, so the chart runs `replicas: 1` with the `Recreate` strategy until #11 lands. 0.1.0 measures performance through OpenTelemetry metrics, benchmarks, and a load harness, but publishes baselines, not capacity claims.

## Where the code is today

| Area | State |
| --- | --- |
| `internal/cdc` | Implemented: replication connection, `pgoutput` decoder, resilient reader with retry and error classification, gap-free bootstrap and chunked scoped snapshots with a visibility-based boundary, `Source` lifecycle, WAL retention bound |
| `internal/watch` | Implemented: hub with bounded replay, scope routing, progress, and resync |
| `api/watch/v1`, `internal/server` | Implemented: v1 gRPC Watch contract, `CompareCursors`, server, health service. Authentication and request bounds are #52 |
| `sdk/go/watchd` | Implemented: `Sync`, `Store`, `MemoryStore`, fresh/stale state |
| `cmd/watchd`, `internal/daemon` | Implemented: JSON and YAML configuration, startup and shutdown ordering, exit codes |
| `examples/postgres` | Runnable quickstart against a locally built binary. Compose and scripted walkthrough are #59 |
| `tests/integration` | End-to-end test of process and SDK against PostgreSQL |
| `docs/` | v0 contract, architecture tour, CDC lifecycle, roadmap |
| Metrics, container, deployment | None yet: #55–#61 |
| `cmd/watchctl`, `deploy/` | Placeholder READMEs only |
| CI | Format, vet, unit and integration tests, vulnerability scan, commit policy |

Closed on the way here: #1 resilient reader, #2 gap-free snapshot handoff, #19 scoped snapshot at an arbitrary LSN, #10 community files, #26 module path, #9 CI gates, #22 WAL bound, #29 process lifecycle, #30 source coordination, #31 chunked snapshots, #32 value encoding, #3 watch hub, #4 v1 API and Go SDK, #21 cross-scope order, #35 quickstart (first slice). Each closed issue's remaining items were filed as the follow-ups below.

## Scope

### In 0.1.0

Correctness core:

- #7 — Configuration: validated source and runtime configuration
- #20 — Watch: progress for idle scopes
- #33 — CDC: detect an incompatible relation change and force Resync
- #64 — CDC: enforce the retained-WAL budget with a staged response
- #53 — CDC: cap projections, scopes, watchers, and snapshots per source
- #54 — Tests: a multi-scope client observes only consistent cuts

Security:

- #6 — Security: source credentials, scope isolation, and threat model
- #52 — API: authenticate clients, authorize scope access, and bound requests

Observability and performance:

- #55 — Telemetry: OpenTelemetry metrics foundation and ops endpoint
- #56 — Telemetry: instrument the ingest-to-serve hot path
- #5 — Operations: health model, runbook, and alerts, built on #55 and #56
- #57 — Performance: benchmarks, load harness, and first baselines

Deployment and release:

- #58 — Build: hardened, minimal container image
- #59 — Deploy: run watchd as a standalone daemon with Docker Compose
- #60 — Deploy: Helm chart for Kubernetes
- #61 — Tests: Kubernetes end-to-end on kind
- #8 — Release: publish signed, verifiable artifacts (binaries, images, chart)
- #34 — Tests: end-to-end fault-injection and convergence suite
- #37 — watchctl: `doctor` command for source prerequisites
- #36 — Release: versioning, compatibility, and changelog policy for 0.x
- #25 — Docs: what watchd is and how it differs from adjacent projects

### Deferred past 0.1.0

| Issue | Reason |
| --- | --- |
| #11 HA and single-active ownership | 0.1.0 is single-process soft state by design |
| #12 Capacity model and regression thresholds | 0.1.0 makes no throughput claim; benchmarks and baselines were carved out as #57 |
| #13 Schema evolution | Deferred except the safety slice carved out as #33 |
| #16 Shared SDK semantics and conformance | 0.1.0 is Go-only |
| #17 TypeScript SDK | 0.1.0 is Go-only |
| #18 Python SDK | 0.1.0 is Go-only |
| #23 Source-driver interface and conformance suite | Premature with one driver |
| #24 Separate ingest from serving tier | Follows #11 |
| #62 Kubernetes operator | Helm covers a single-active deployment; an operator pays off only with CRD-managed sources, slot provisioning, and #11/#24 |

## New work

Gaps that no existing issue owned. Each is filed against the `v0.1.0` milestone.

First batch, from the original plan:

- #29 — **Runtime: assemble the watchd process lifecycle.** Done.
- #30 — **CDC: coordinate many projections and scopes on one source stream.** Done; the scope cap moved to #53.
- #31 — **CDC/API: stream snapshots in bounded chunks.** Done.
- #32 — **API: define the v1 value-encoding contract.** Done.
- #33 — **CDC: detect an incompatible relation change and force Resync.** A mid-stream DDL change is not compared against the validated projection spec, so a projection can silently disagree with its source.
- #34 — **Tests: end-to-end fault-injection and convergence suite.** The suite asserts one invariant across every fault: the projection matches the source at the last progress cursor, or the client is explicitly stale.
- #35 — **Examples: a runnable quickstart with observable freshness.** First slice done; the rest moved to #59.
- #36 — **Release: versioning, compatibility, and changelog policy for 0.x.** No `CHANGELOG.md`, and no written statement of what a 0.x version number promises.
- #37 — **watchctl: `doctor` command for source prerequisites.** Reuse the validation already in `internal/cdc` to report every prerequisite failure at once, with remediating SQL, before the service starts.

Second batch, split from closed issues or added for observability and deployment:

- #52 — **API: authenticate clients, authorize scope access, and bound requests.** The rest of #4: authN/authZ interceptors, TLS and mTLS, message and stream limits, and a `buf breaking` check.
- #53 — **CDC: cap projections, scopes, watchers, and snapshots per source.** The rest of #30.
- #54 — **Tests: a multi-scope client observes only consistent cuts.** The rest of #21.
- #55 — **Telemetry: OpenTelemetry metrics foundation and ops endpoint.** The OTel SDK with a Prometheus `/metrics` endpoint and optional OTLP push, an ops port with `/healthz`, `/readyz`, and opt-in pprof, and enforced cardinality rules.
- #56 — **Telemetry: instrument the ingest-to-serve hot path.** Replication, decode, retention, snapshot, hub, gRPC, and SDK stages; includes the metrics #3 required.
- #57 — **Performance: benchmarks, load harness, and first baselines.** Microbenchmarks, a workload generator, a profiling workflow, and recorded baselines.
- #58 — **Build: hardened, minimal container image.** Distroless, non-root, read-only root filesystem, smoke test, and scan.
- #59 — **Deploy: run watchd as a standalone daemon with Docker Compose.** The reference single-daemon deployment, the rest of #35, and bare-binary docs.
- #60 — **Deploy: Helm chart for Kubernetes.** Single replica with `Recreate`, gRPC probes, secret references, the restricted security context, NetworkPolicy, and ServiceMonitor.
- #61 — **Tests: Kubernetes end-to-end on kind.** Install the chart, sync a consumer in the cluster, and resync correctly across a pod restart.
- #64 — **CDC: enforce the retained-WAL budget with a staged response.** The rest of #22: warn, degrade, and terminal stages on top of the retained-WAL measurement from #56, plus orphan-slot cleanup.

## Order

These dependencies constrain the sequence:

- #55 precedes #56 and #5; #56 precedes #57, whose benchmarks measure instrumentation overhead too, and #64, which acts on #56's retention measurement.
- #7 (file-mounted secrets, ops listener configuration) and #58 precede #59 and #60.
- #52 precedes #60 exposing the gRPC port to other namespaces.
- #60 precedes #61, and #61 precedes #8, which publishes what #61 has proven.
- #25 and #59 land alongside #8, because they describe the shipped thing.

| Phase | Work |
| --- | --- |
| A — Foundations | Done: #9, #22, #29, #30, #31, #32, #3, #4, #21 |
| B — Correctness and safety | #7, #20, #33, #53, #54, #6 → #52, #56 → #64 |
| C — Observability | #55 → #56 → #57, #5 |
| D — Deployment | #58 → #59, #60 → #61 |
| E — Ship | #34, #37, #36, #25 → #8 → tag |

## Release gate

Every issue's own definition of done applies. Before tagging, additionally:

```bash
make postgres-up
make test
make integration
go test -race ./...
```

- The fault-injection suite is green in CI, and every scenario ends converged or explicitly stale.
- The quickstart runs from a clean clone with only Docker and Go: the projection goes fresh, survives a restart, and rebuilds correctly after a forced resync.
- A WAL-budget breach is exercised deliberately: a stalled sink crosses warn, degrade, and the terminal action, and watchers resync instead of the source filling its disk.
- The published image is non-root with a read-only root filesystem and no baked credentials; `helm lint` and `helm template` pass; checksums, SBOM, and signature verify.
- The kind end-to-end job is green: the chart installs, a consumer in the cluster goes fresh, and a watchd pod restart ends in resync and fresh again.
- The Compose deployment starts from `deploy/compose` and the quickstart script completes non-interactively.
- `/metrics` exposes the documented metric catalogue, and the cardinality test passes.
- Benchmark baselines and the first hot-path findings are recorded in `docs/performance.md`.
- A full fault-run's logs and exported metrics contain neither the source password nor any row value.
- The README "Status" section is rewritten to describe a released 0.1.0.
