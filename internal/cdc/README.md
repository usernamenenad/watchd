# CDC adapter

This package contains watchd's PostgreSQL logical-decoding adapter.

`Reader` owns one PostgreSQL logical-replication source. Its explicit
`InitializeSlot` provisioning step creates or validates a persistent
`pgoutput` slot. `Run` requires that slot to exist, starts protocol version 1
replication, handles WAL and keepalive messages, decodes committed transaction
batches, and retries temporary connection failures with bounded exponential
backoff.

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

Snapshot/slot handoff, the replay buffer, watcher subscriptions, and cursor
persistence remain outside this package and are tracked by later issues.

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

// Provisioning is explicit so a missing slot during recovery is never silently
// recreated. This does not replace issue #2's snapshot/slot bootstrap.
_, err = reader.InitializeSlot(ctx)
if err != nil {
    return err
}

return reader.Run(ctx)
```
