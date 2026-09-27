# watchd

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

`watchd` will help services maintain recoverable, verifiably fresh projections of selected PostgreSQL state.

It is deliberately not a task queue or a general-purpose event log. Its intended contract is that a client either knows its projection is complete through a source cursor, or it is explicitly stale and resynchronizes from the source of truth.

## Kubernetes fit

`watchd` is intended to work well in cloud-native environments. Pod restarts, rolling deployments, horizontal scaling, node maintenance, and short network interruptions are normal in Kubernetes; each can make a `LISTEN`/`NOTIFY`-based cache miss an update while it is disconnected.

Applications running in pods can use the Go SDK to maintain a local projection of selected PostgreSQL state. When a pod starts or reconnects, it resumes from its last cursor when possible, or receives an explicit resync instruction and rebuilds from PostgreSQL. A pod should serve data as fresh only after it has received a progress statement for its watched scope.

The initial version runs as a normal service deployed alongside applications: a single daemon (the binary, or Docker Compose) or a Helm-deployed Kubernetes workload, from one container image (see [deploy](deploy/README.md)). Dashboards and an optional operator are follow-on work, not prerequisites for the core correctness model.

## Status

`watchd` is pre-alpha, and runs end to end: the `watchd` process streams one PostgreSQL source, serves the v1 gRPC Watch API, and the Go SDK keeps a client's projection in sync with explicit fresh/stale state, rebuilding after a restart or resync. Try it with [the quickstart](examples/postgres/README.md). Authentication, metrics, full configuration, and a production release are not implemented yet. APIs and configuration may change without compatibility guarantees.

## Local development

Start the local PostgreSQL source with:

```bash
make postgres-up
```

It listens on `127.0.0.1:54329` and is configured for logical replication. See [the local source guide](testing/postgres/README.md) for credentials and reset instructions.

## Intended layout

- `api/` — versioned public API contracts
- `cmd/` — service and CLI entry points
- `internal/` — implementation packages
- `sdk/go/` — public Go client
- `docs/` — product and correctness documentation
- `deploy/` — deployment assets
- `tests/` — integration and fault-test suites
- `examples/` — runnable reference setups

See [the roadmap](docs/roadmap.md), the [v0 semantics contract](docs/semantics.md), and the [0.1.0 release plan](docs/release-0.1.0.md).

## Contributing

Contributions are welcome. Start with the [architecture tour](docs/architecture.md), read [CONTRIBUTING.md](CONTRIBUTING.md), and choose a scoped [GitHub issue](https://github.com/usernamenenad/watchd/issues). Every commit must be signed off under the [DCO](DCO) and reference an issue.

The project is licensed under [Apache-2.0](LICENSE). See [SECURITY.md](SECURITY.md) for private vulnerability reporting and [SUPPORT.md](SUPPORT.md) for current support expectations.
