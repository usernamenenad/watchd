# Watch API v1

`watch.proto` is the versioned gRPC contract for keeping a local projection
of one PostgreSQL scope in sync. The Go code next to it is generated and
committed; regenerate it with `make proto` (requires `protoc`,
`protoc-gen-go`, and `protoc-gen-go-grpc`).

The messages carry the [v0 semantics](../../../docs/semantics.md) unchanged.

## `Watch`

One stream per scope. Without a resume cursor it starts with a snapshot:

```text
SnapshotBegin → SnapshotRows … → SnapshotEnd → Batch / Progress …
```

With a cursor from an earlier `Batch` or `Progress`, the server replays what
the client missed, if it still can, and continues live. `Resync` always ends
the stream; the client marks its projection stale and watches again without
a cursor.

A client must:

1. Replace its projection with the snapshot's rows at `SnapshotEnd`, in one
   step.
2. Apply each `Batch` whole and idempotently: a batch can arrive twice.
3. Keep a column marked `unchanged_toast` at the value it already has.
4. Call itself fresh only after applying everything before a `Progress`.
5. Persist cursors verbatim. A cursor is opaque; order two cursors with
   `CompareCursors`, never by parsing them.

## Cursors

A cursor names a position in one source's change stream and carries that
source's identity. Cursors of one source are totally ordered, so across
several scopes of one source the smallest `Progress` cursor is a consistent
cut. A cursor from a different source is refused with `INVALID_ARGUMENT`.

`Batch.commit_time` is when the transaction committed, by the source's
clock. Use it to measure latency, such as commit to apply, bearing in mind
clock skew between the source and the client. Never use it to order or to
judge freshness: that is what cursors and `Progress` are for.

## Message sizes

Snapshot rows are packed into messages of about 1 MiB. A `Batch` is one
source transaction and is never split, so a client must accept messages up
to the server's transaction limit (16 MiB by default) plus overhead; the Go
SDK configures this.

## Errors

| Code | Meaning |
| --- | --- |
| `NOT_FOUND` | Unknown projection |
| `INVALID_ARGUMENT` | Malformed cursor, or one from another source |
| `FAILED_PRECONDITION` | The source is misconfigured for the projection; an operator must fix it |
| `UNAVAILABLE` | The source cannot be read right now; retry |
| `INTERNAL` | Anything else; details are logged server-side only |

## Dependencies

watchd pins `google.golang.org/grpc` to a development build
(`v1.85.0-dev.0.20260825072537-93e31b48545e`) because v1.84.0, the latest
release, has GO-2026-6443: a request without an authority or `Host` header
crashes the server. Move to v1.85.0 once it is released.

The server also implements the standard `grpc.health.v1.Health` service,
reporting `SERVING` only while its source can serve this contract.
