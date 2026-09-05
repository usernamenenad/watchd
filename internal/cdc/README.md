# CDC adapter

This package contains watchd's PostgreSQL logical-decoding adapter.

`Reader` owns one PostgreSQL logical-replication source. `Bootstrap` is the
only creation path: it creates a new persistent `pgoutput` slot with an
exported PostgreSQL snapshot, reads one configured projection scope at that
snapshot, and returns the matching opaque cursor. `Run` consumes the
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
- A persistent slot retains PostgreSQL WAL while watchd is behind. Monitor slot
  lag and do not delete or recreate a slot to recover from an invalidation;
  recovery needs a new source snapshot.

The replay buffer, watcher subscriptions, and consumer cursor persistence
remain outside this package and are tracked by later issues.

## Reader setup

```go
reader, err := cdc.NewReader(cdc.ReaderConfig{
    DatabaseURL:     databaseURL,
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
}, cdc.Scope{Value: tenantID})
if err != nil {
    return err
}
if err := installSnapshotAtomically(snapshot); err != nil {
    return err
}

return reader.Run(ctx)
```

`Bootstrap` starts replication on the exact connection and cursor paired with
the snapshot; `Run` consumes that already-started stream. A later recovery
with an existing usable slot calls `Run` directly; it never creates a missing
slot.
