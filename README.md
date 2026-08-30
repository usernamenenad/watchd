# watchd

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

`watchd` will help services maintain recoverable, verifiably fresh projections of selected PostgreSQL state.

It is deliberately not a task queue or a general-purpose event log. Its intended contract is that a client either knows its projection is complete through a source cursor, or it is explicitly stale and resynchronizes from the source of truth.

## Kubernetes fit

`watchd` is intended to work well in cloud-native environments. Pod restarts, rolling deployments, horizontal scaling, node maintenance, and short network interruptions are normal in Kubernetes; each can make a `LISTEN`/`NOTIFY`-based cache miss an update while it is disconnected.

Applications running in pods can use the Go SDK to maintain a local projection of selected PostgreSQL state. When a pod starts or reconnects, it resumes from its last cursor when possible, or receives an explicit resync instruction and rebuilds from PostgreSQL. A pod should serve data as fresh only after it has received a progress statement for its watched scope.

The initial version will run as a normal service deployed alongside applications. Kubernetes-specific packaging, dashboards, and an optional operator are follow-on work, not prerequisites for the core correctness model.

## Status

`watchd` is pre-alpha. The v0 contract, local PostgreSQL logical-replication environment, resilient transaction reader, and bootstrap integration spike are in place. An explicit provisioning step creates or validates a replication slot; the reader then emits only committed transaction batches, acknowledges only locally accepted batches, and reconnects after transient connection loss without recreating a missing slot. The watch runtime, network API, SDK, snapshot coordination, and production release are not implemented. APIs and configuration may change without compatibility guarantees.

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

See [the roadmap](docs/roadmap.md) and the [v0 semantics contract](docs/semantics.md).

## Contributing

Contributions are welcome. Start with the [architecture tour](docs/architecture.md), read [CONTRIBUTING.md](CONTRIBUTING.md), and choose a scoped [GitHub issue](https://github.com/usernamenenad/watchd/issues). Every commit must be signed off under the [DCO](DCO) and reference an issue.

The project is licensed under [Apache-2.0](LICENSE). See [SECURITY.md](SECURITY.md) for private vulnerability reporting and [SUPPORT.md](SUPPORT.md) for current support expectations.
