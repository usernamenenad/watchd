# CDC adapter

This package contains watchd's PostgreSQL logical-decoding adapter.

`Reader` owns one PostgreSQL logical-replication source. `Bootstrap` is the
only creation path: it creates a new persistent `pgoutput` slot with an
exported PostgreSQL snapshot, reads one configured projection scope at that
snapshot, and returns the matching opaque cursor. `Snapshot` reads any later
scope against the existing slot, while `Run` is streaming. `Run` consumes the
already-started bootstrap stream or resumes an existing slot; it handles WAL
and keepalive messages, decodes committed transaction batches, and retries
temporary connection failures with bounded exponential backoff.

`Decoder` remembers relation metadata, buffers row mutations between `BEGIN`
and `COMMIT`, and emits a `Transaction` only after commit. It bounds the
in-flight transaction by bytes and change count.

`TransactionSink` is the local acceptance boundary. `Reader` acknowledges a
transaction's PostgreSQL `TransactionEndLSN` only after the sink returns nil.
If the process or connection fails before acknowledgement, PostgreSQL may
deliver the transaction again; sinks must therefore be idempotent.

## Current v0 restrictions

- One unquoted PostgreSQL publication per reader, using `pgoutput` protocol
  version 1 and `publish = 'insert, update, delete'`. The reader rejects a
  publication that also publishes `TRUNCATE` before it starts.
- `TRUNCATE`, logical-decoding messages, and other unsupported state-affecting
  pgoutput messages fail the reader explicitly rather than being ignored.
- Projection primary-key values must not change. A primary-key update fails
  explicitly until the public change model can represent an old and new key.
- A projection's scope column must be part of its primary key, so a `DELETE`
  (which carries only the key) can be routed to its scope.
- PostgreSQL 13 or later: snapshot boundaries use `pg_current_snapshot()`.
- A persistent slot retains PostgreSQL WAL while watchd is behind. Monitor slot
  lag and do not delete or recreate a slot to recover from an invalidation;
  recovery needs a new source snapshot.

The replay buffer, watcher subscriptions, and consumer cursor persistence
remain outside this package and are tracked by later issues.

## Reader setup

```go
reader, err := cdc.NewReader(cdc.ReaderConfig{
    DatabaseURL:     databaseURL,
    SourceID:        "primary",
    SlotName:        "watchd_source",
    PublicationName: "watchd_publication",
}, replayBuffer.Accept)
if err != nil {
    return err
}

snapshot, err := reader.Bootstrap(ctx, cdc.ProjectionSpec{
    SourceID:    "primary",
    Schema:      "public",
    Table:       "tenant_permissions_projection",
    ScopeColumn: "tenant_id",
    PrimaryKey:  []string{"tenant_id", "user_id"},
}, cdc.Scope{Value: tenantID}, installSnapshotBatch)
if err != nil {
    return err
}

return reader.Run(ctx)
```

`Bootstrap` starts replication on the exact connection and cursor paired with
the snapshot; `Run` consumes that already-started stream. A later recovery
with an existing usable slot calls `Run` directly; it never creates a missing
slot.

## Source: choosing Bootstrap, Snapshot, or Run

`Source` owns one `Reader` and makes the choice between its primitives, so a
runtime never has to:

```go
source, err := cdc.NewSource(readerConfig, hub.Accept)
if err != nil {
    return err
}
// Resumes an existing slot with Run, or waits for the first scope.
if err := source.Start(ctx); err != nil {
    return err
}

// First scope on a new source: Bootstrap creates the slot, then Run starts.
// Every later scope: Snapshot against the live slot.
snapshot, err := source.Snapshot(ctx, spec, cdc.Scope{Value: tenantID}, installRows)

<-source.Done() // the stream ended; source.Err() says why
```

| Situation | Primitive `Source` uses |
| --- | --- |
| `Start`, slot exists | `Run`, resuming the slot |
| `Start`, no slot | nothing yet: no bare slot is created |
| First `Snapshot`, no slot | `Bootstrap`, then `Run` on the stream it hands over |
| Any later `Snapshot` | `Reader.Snapshot`, while `Run` streams |
| `Run` ends | `Done` closes; later `Snapshot` calls return `ErrSourceStopped` |

Concurrent first scopes wait for the one that bootstraps instead of racing
to create the slot.

## Snapshot boundary

`Bootstrap` and `Snapshot` each return a `Snapshot` whose rows and the change
stream meet with no gap. A consumer installs the rows, then applies every
streamed transaction for which `snapshot.Covers(transaction)` is false, in
stream order. `Covers` decides by source transaction ID against the read's
MVCC snapshot, not by WAL position, so a commit that races the read can
never be lost. For `Bootstrap` the stream starts exactly at the boundary, so
`Covers` is false for every transaction `Run` delivers after it; for a later
`Snapshot`, see "Adding a second scope" in `docs/cdc-lifecycle.md`.

`Bootstrap` and `Snapshot` deliver scoped rows to a `cdc.SnapshotRowSink`
in primary-key-ordered batches (`ReaderConfig.SnapshotBatchRows`, default
1000) instead of returning them all at once, so a very large scope never
has to be held fully in memory.
