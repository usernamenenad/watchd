# Go SDK

`github.com/usernamenenad/watchd/sdk/go/watchd` keeps a local copy of one
PostgreSQL scope in sync with a watchd server, and tells you whether that
copy is fresh.

```go
client, err := watchd.Dial("localhost:7070",
    grpc.WithTransportCredentials(insecure.NewCredentials()))
if err != nil {
    return err
}
defer client.Close()

// An in-memory projection keyed by the table's primary key.
store := watchd.NewMemoryStore("tenant_id", "user_id")

err = client.Sync(ctx, watchd.SyncConfig{
    Projection: "tenant_permissions",
    Scope:      tenantID,
    Store:      store,
    OnState: func(state watchd.State) {
        log.Printf("fresh=%t cursor=%s rows=%d", state.Fresh, state.Cursor, store.Len())
    },
})
```

`Sync` blocks until `ctx` is cancelled. While it runs, it:

1. installs a snapshot of the scope, or resumes from the store's cursor;
2. applies every later committed change, one source transaction at a time;
3. saves the resume cursor with each change;
4. reconnects after connection failures, with backoff;
5. rebuilds from a new snapshot whenever the server says it cannot continue
   (`Resync`), replacing the projection in one step.

It returns an error only when retrying cannot help: an unknown projection, a
misconfigured server, or a failing `Store`.

## Freshness

`State.Fresh` is true only after the server has confirmed that everything
up to `State.Cursor` was delivered and applied, and while the connection is
up. Receiving a change, or being connected, is not enough: after any
disconnect or resync the projection is stale until the server confirms it
again.

## Your own store

`MemoryStore` suits caches and tests. To keep the projection in your own
database, implement `Store`:

| Method | Must |
| --- | --- |
| `Cursor` | Return the saved resume cursor, or `""` |
| `BeginSnapshot` → `Put` … `Commit` | Replace the whole projection in one step at `Commit`, and clear the saved cursor; `Abort` discards |
| `Apply` | Apply one batch and save its cursor together; applying the same batch twice must be harmless |
| `SaveCursor` | Save a confirmed cursor |

A column value marked `Unchanged` means an update did not resend a large,
unchanged value: keep the one you have.

Cursors are opaque strings. Persist them verbatim, and order two of them only
with `Client.CompareCursors`.
