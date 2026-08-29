# watchd

`watchd` will help services maintain recoverable, verifiably fresh projections of selected PostgreSQL state.

It is deliberately not a task queue or a general-purpose event log. Its intended contract is that a client either knows its projection is complete through a source cursor, or it is explicitly stale and resynchronizes from the source of truth.

## Kubernetes fit

`watchd` is intended to work well in cloud-native environments. Pod restarts, rolling deployments, horizontal scaling, node maintenance, and short network interruptions are normal in Kubernetes; each can make a `LISTEN`/`NOTIFY`-based cache miss an update while it is disconnected.

Applications running in pods can use the Go SDK to maintain a local projection of selected PostgreSQL state. When a pod starts or reconnects, it resumes from its last cursor when possible, or receives an explicit resync instruction and rebuilds from PostgreSQL. A pod should serve data as fresh only after it has received a progress statement for its watched scope.

The initial version will run as a normal service deployed alongside applications. Kubernetes-specific packaging, dashboards, and an optional operator are follow-on work, not prerequisites for the core correctness model.

## Status

Repository skeleton only. No CDC, network API, storage, or SDK behavior is implemented yet.

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
