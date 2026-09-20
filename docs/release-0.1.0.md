# Release plan: 0.1.0

This document defines what `watchd` 0.1.0 is, which issues block it, which are deliberately deferred, and in what order the work lands. It is a planning document: when it disagrees with [the v0 semantics contract](semantics.md), the contract wins.

## What 0.1.0 is

**A deployable release.** An outside team can install `watchd`, point it at a PostgreSQL database, run a Go application against it, and know at every moment whether their projection is fresh or stale — with the operational and security model written down and the artifacts verifiable.

It is explicitly not a scale or high-availability release. 0.1.0 is single-process soft state, as [the architecture tour](architecture.md) already states, and makes no throughput claim.

## Where the code is today

| Area | State |
| --- | --- |
| `internal/cdc` | Implemented: replication connection, `pgoutput` decoder, resilient reader with retry and error classification, gap-free bootstrap, scoped snapshot, slot validation |
| `docs/` | v0 contract, architecture tour, CDC lifecycle, roadmap |
| `internal/watch`, `internal/server`, `api/watch/v1`, `sdk/go/watchd`, `cmd/watchd`, `cmd/watchctl`, `tests/`, `examples/`, `deploy/` | Placeholder READMEs only |
| CI | Commit-policy workflow only; `make build` and `make fmt` are stubs |

Closed on the way here: #1 resilient reader, #2 gap-free snapshot handoff, #19 scoped snapshot at an arbitrary LSN, #10 community files, #26 module path.

## Scope

### In 0.1.0

Correctness core:

- #3 — Watch: bounded replay, progress, and resync runtime
- #4 — API: versioned Snapshot and Watch gRPC contract with Go SDK
- #7 — Configuration: validated source and runtime configuration
- #20 — Watch: progress for idle scopes
- #21 — Semantics: total order and consistent cut across scopes of one source
- #22 — Safety: bound retained WAL and define slot abandonment policy

Operability and release:

- #5 — Operations: observability, health model, and runbook
- #6 — Security: source credentials, scope isolation, and threat model
- #9 — Build: CI quality and correctness gates
- #8 — Release: hardened container, Helm chart, and verifiable artifacts
- #25 — Docs: what watchd is and how it differs from adjacent projects

Plus the nine issues described under [New work](#new-work) below.

### Deferred past 0.1.0

| Issue | Reason |
| --- | --- |
| #11 HA and single-active ownership | 0.1.0 is single-process soft state by design |
| #12 Benchmarks and capacity model | 0.1.0 makes no throughput claim |
| #13 Schema evolution | Deferred except the safety slice carved out as #33 |
| #16 Shared SDK semantics and conformance | 0.1.0 is Go-only |
| #17 TypeScript SDK | 0.1.0 is Go-only |
| #18 Python SDK | 0.1.0 is Go-only |
| #23 Source-driver interface and conformance suite | Premature with one driver |
| #24 Separate ingest from serving tier | Follows #11 |

## New work

Gaps that no existing issue owned. Each is filed against the `v0.1.0` milestone.

- #29 — **Runtime: assemble the watchd process lifecycle.** `cmd/watchd` has no executable, and no issue owns composition: startup ordering, readiness gating, drain-and-shutdown ordering, and the rule that a batch the sink did not accept is never acknowledged.
- #30 — **CDC: coordinate many projections and scopes on one source stream.** `Reader.Bootstrap` is per-scope and slot creation is a side effect of the first bootstrap. [The CDC lifecycle document](cdc-lifecycle.md) defers this coordination explicitly. It also settles what the acknowledged LSN means when several sinks accept at different rates.
- #31 — **CDC/API: stream snapshots in bounded chunks.** A scoped snapshot is materialized whole in memory, so #4's paginated Snapshot has nothing to build on.
- #32 — **API: define the v1 value-encoding contract.** Text encoding, SQL NULL, and unchanged TOAST are described only in a Go doc comment, and `Change.Key` cannot represent a NULL key. Must be settled before the protobuf is frozen.
- #33 — **CDC: detect an incompatible relation change and force Resync.** A mid-stream DDL change is not compared against the validated projection spec, so a projection can silently disagree with its source.
- #34 — **Tests: end-to-end fault-injection and convergence suite.** `tests/` is empty. The suite asserts one invariant across every fault: the projection matches the source at the last progress cursor, or the client is explicitly stale.
- #35 — **Examples: a runnable quickstart with observable freshness.** `examples/postgres/` is a stub; the freshness rule should be visible, not only described.
- #36 — **Release: versioning, compatibility, and changelog policy for 0.x.** No `CHANGELOG.md`, and no written statement of what a 0.x version number promises.
- #37 — **watchctl: `doctor` command for source prerequisites.** Reuse the validation already in `internal/cdc` to report every prerequisite failure at once, with remediating SQL, before the service starts.

## Order

These dependencies constrain the sequence:

- #9 comes first; retrofitting CI onto merged features is worse than gating from the start.
- #21 and #32 precede #4. Both change what the v1 protobuf must contain, and freezing it first means a breaking change or a bolted-on second cursor type.
- #30 precedes #3 serving more than one scope, and #22 depends on the acknowledgement model #30 establishes.
- #31 precedes the final shape of the Snapshot RPC in #4.
- #8 is last; #25 and #35 land alongside it, because they describe the shipped thing.

| Phase | Work |
| --- | --- |
| A — Foundations | #9, #7, #25, #36 |
| B — Correctness core | #30 → #3 → #20 → #21 → #22, with #33 alongside |
| C — Surface | #32 → #31 → #4 → #29 → #37 |
| D — Ship | #5, #6, #34, #35 → #8 → tag |

`make build` and `make fmt` become real targets as part of #9.

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
- A full fault-run's logs and exported metrics contain neither the source password nor any row value.
- The README "Status" section is rewritten to describe a released 0.1.0.
