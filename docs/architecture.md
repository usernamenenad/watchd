# Architecture tour

`watchd` helps an application maintain a local projection of selected PostgreSQL state and know whether that projection is safe to trust.

```text
producer writes PostgreSQL
            |
            v
internal/cdc: snapshot boundary + logical replication + transaction decoding
            |
            v
internal/watch: bounded replay + scope subscriptions + progress/resync
            |
            v
internal/server: versioned network API
            |
            v
SDK/client: local projection + cursor + fresh/stale state
```

## PostgreSQL source

PostgreSQL is authoritative. Producers write projection tables in the same transaction as their application state. A projection table has a stable primary key and one configured scope column, such as `tenant_id`.

## CDC boundary

`internal/cdc` owns the PostgreSQL logical-replication connection. It decodes `pgoutput` relation and row messages, buffers changes between `BEGIN` and `COMMIT`, and emits one atomic transaction batch only after commit.

Bootstrap uses an exported PostgreSQL snapshot and its matching logical-replication position. This is the critical no-gap invariant: every committed mutation must appear either in the snapshot or after its cursor in the change stream.

## Watch runtime

`internal/watch` will own an explicitly bounded, non-authoritative replay window and the lifecycle of watchers. A usable cursor can replay changes; an unavailable cursor produces `Resync`. The runtime must isolate slow watchers so they cannot block CDC ingestion or healthy watchers.

`Progress(scope, cursor)` is a correctness statement: all relevant committed changes through that cursor have been delivered. A connected client is not automatically fresh.

## Server and SDK

`internal/server` will expose Snapshot and Watch operations without exposing PostgreSQL credentials or unrestricted SQL. The Go SDK will apply transaction batches idempotently, replace snapshots atomically, persist cursors, and expose explicit fresh/stale state.

## Failure model

The v0 runtime is single-process soft state. On restart or an unreplayable cursor, consumers resync from PostgreSQL. High availability, schema evolution, bounded resources, and source failover are tracked as explicit issues and must preserve the [v0 semantics](semantics.md).

## Package boundaries

- `cmd/watchd`: process construction and lifecycle only.
- `internal/cdc`: PostgreSQL protocol, snapshots, and committed source transactions.
- `internal/watch`: source-independent replay and watcher state.
- `internal/server`: transport adapters, authentication hooks, and operational endpoints.
- `api/watch/v1`: versioned wire contract.
- `sdk/go/watchd`: public Go client behavior.

Types should live at the boundary that owns their meaning. Executable packages must not become shared domain libraries.
