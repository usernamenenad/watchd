# PostgreSQL CDC lifecycle, from zero

This document explains the part of watchd that reads changes from PostgreSQL and turns them into a local, queryable copy of a source table. It starts with the database concepts, then follows the code path from an empty cluster through bootstrap, normal operation, and recovery.

The goal is simple: a consumer should see a complete copy of the rows it is responsible for, followed by every later committed change, without a missing interval between the initial copy and the live stream.

## The words used in this document

### Transaction and commit

A transaction is a group of database changes that PostgreSQL treats as one unit. For example, moving money might update an account row and insert an audit row in one transaction. Until it commits, the outside world should not treat it as final. `COMMIT` is the moment PostgreSQL makes that group durable and visible.

### WAL

PostgreSQL records changes in its write-ahead log (WAL). Think of WAL as a durable, ordered journal: “this row was inserted”, “this row was updated”, and so on. PostgreSQL needs it for crash recovery, but it can also expose suitable changes to other systems.

### Logical replication

Logical replication is PostgreSQL’s way to send database changes as logical row events instead of copying physical disk pages. watchd uses PostgreSQL’s `pgoutput` logical-replication protocol to receive events such as `INSERT`, `UPDATE`, and `DELETE`.

Logical replication delivers committed changes. A transaction’s changes are surrounded by protocol `BEGIN` and `COMMIT` messages, so the receiver can avoid publishing half a transaction to its downstream consumer.

### Publication

A PostgreSQL publication says which database changes are allowed to be sent through logical replication. It is the database-side filter. For example:

```sql
CREATE PUBLICATION watchd_publication FOR TABLE app.orders;
```

That publication makes changes to `app.orders` available to a subscriber that asks for `watchd_publication`. It does not copy anything by itself and it does not remember progress. It is simply the source-side declaration of what may be replicated.

The database administrator owns the publication. watchd checks that its configured publication exists and that it includes the configured table.

### Logical replication slot

A logical replication slot is PostgreSQL’s durable bookmark for one replication consumer. It records how far that consumer has safely acknowledged the WAL.

If watchd disconnects, PostgreSQL retains the required WAL instead of discarding it immediately. When watchd reconnects, it asks the slot for the changes after the saved position. That is why a slot is essential for recovering a live stream without silently losing changes.

A slot is not the copied table data. It is only the bookmark and the PostgreSQL-side promise to retain the unconsumed journal.

### LSN, cursor, and position

An LSN (log sequence number) identifies a position in the WAL. In watchd code and documentation, a **cursor** is the encoded LSN used to name that position.

“Resume from cursor `P`” means “give me every committed change after position `P`.”

### Projection

A projection is the local data set watchd maintains from a source table. It is usually a subset of a source table, stored or exposed in a form that the local application can use efficiently.

The current bootstrap code describes one source table with:

```go
type ProjectionSpec struct {
    SourceID    string
    Schema      string
    Table       string
    ScopeColumn string
    PrimaryKey  []string
}
```

`SourceID` names this configured source. `Schema` and `Table` select the PostgreSQL table. `ScopeColumn` is the column used to divide the table into tenants, accounts, workspaces, or another unit of ownership. `PrimaryKey` is the ordered list of primary-key columns that identifies a row reliably, so updates and deletes can be applied to the correct local row.

### Scope

A scope says which slice of a projection a local consumer wants. It is intentionally small:

```go
type Scope struct {
    Value string
}
```

For a projection of `app.orders` scoped by `workspace_id`, `Scope{Value: "acme"}` means “the `app.orders` rows whose `workspace_id` is `acme`.”

The current code validates that the scope column exists and queries only that scope during bootstrap. A later runtime layer must coordinate all scopes that share one source stream.

### Snapshot

A snapshot is a consistent, point-in-time read of rows. It answers: “what did this table slice look like at one particular database moment?”

watchd returns:

```go
type Snapshot struct {
    SourceID string
    Cursor   string
    Rows     []map[string]any
}
```

`Rows` are the initial rows to install locally. `Cursor` is the WAL boundary that pairs that row set with the later live stream. The values are returned in PostgreSQL text form (or `nil`) so that the snapshot and logical-decoding path use compatible representations.

### Exported snapshot

An exported snapshot is a PostgreSQL snapshot token that another transaction can import. It lets two different database connections agree on the same point in time.

That matters because creating a replication slot happens on a replication-protocol connection, while reading table rows is an ordinary SQL query. The exported token lets the SQL read see exactly the database state associated with the new slot’s initial WAL position.

### Sink

A sink is the component that receives decoded, committed change batches from watchd and applies them somewhere useful. It might write to a local SQLite database, an in-memory materialized view, a cache, or an application-owned store.

The sink is responsible for making a batch durable before saying it accepted it. Its operation should be idempotent: applying the same batch again must be safe. This is necessary because a crash can occur after the sink stored a batch but before watchd told PostgreSQL it was safe to advance the slot.

## The basic problem: avoiding the gap

A naive startup could do this:

```text
1. Read the table.
2. Start live replication.
```

There is a hole between those steps. If a transaction commits after step 1 but before step 2, it is neither in the table read nor necessarily in the stream starting point. The local copy has missed a real source change.

The safe version establishes one boundary, `P`:

```text
                source changes
----------------------|---------------------->
              snapshot P     replication starts at P

snapshot: rows visible at P
stream:   committed changes after P
```

Together those two halves cover the whole history. This is a **gap-free snapshot**. It does not mean the snapshot never becomes old; it means every change after its boundary will be delivered by the stream.

## The watchd lifecycle for a brand-new source

### 1. Build a reader

`NewReader` checks the static configuration but opens no database connection and creates no slot. At this point watchd only knows what source it intends to read.

### 2. Call `Bootstrap`

For a brand-new source, the owner calls:

```go
snapshot, err := reader.Bootstrap(ctx, projection, scope)
```

`Bootstrap` is the only normal slot-creation path. It does the following, in this order:

1. Validates the projection and scope. It checks identifiers, connects to the source, verifies the publication/table relationship, and verifies that the configured scope column and primary key exist.
2. Opens a replication connection and creates a persistent `pgoutput` logical slot with PostgreSQL’s `EXPORT_SNAPSHOT` option.
3. Receives two linked values from PostgreSQL: the slot’s consistent WAL position `P`, and an exported snapshot token.
4. Keeps that replication connection open. The exported token remains usable only while its creator connection lives.
5. Opens a normal SQL connection, begins a repeatable-read read-only transaction, imports the token, and executes the scoped `SELECT`.
6. Builds `Snapshot{Rows, Cursor: P}` from that consistent SQL result.
7. Before returning, starts logical replication at exactly `P` on the original replication connection. The returned `Reader` now owns an already-started stream.

The important ordering is that the stream starts before `Bootstrap` returns. There is no later period in which the caller has a snapshot but has not yet caused the reader to begin at its paired cursor.

`Bootstrap` requires a new slot. If the slot already exists, it returns `ErrBootstrapSlotExists` rather than quietly treating that as a new bootstrap or overwriting an existing source history.

### 3. Atomically install the returned snapshot

The caller receives the rows and cursor and installs them in its own local state. “Atomically” means a reader of the local projection must not observe a half-installed mixture of old and new data.

For example, a local database could write the rows and cursor in one local transaction, then mark that projection ready. The exact storage is outside the current CDC package, but it is the job of the future projection/runtime layer.

### 4. Call `Run`

After the snapshot is installed successfully, call:

```go
err := reader.Run(ctx)
```

On the first call, `Run` consumes the replication connection that `Bootstrap` already started. It does not create another slot and it does not restart from an unrelated position.

## What happens while `Run` is live

`Run` reads PostgreSQL replication messages, decodes changes from `pgoutput`, and buffers events until the source transaction’s `COMMIT` arrives. It then sends the complete committed batch to the sink.

The required order is:

```text
PostgreSQL sends committed batch
        -> watchd calls sink.Apply(batch)
        -> sink durably accepts the batch
        -> watchd acknowledges the LSN to PostgreSQL
```

Acknowledging only after the sink accepts the batch prevents data loss. If the sink rejects a batch, watchd reports `ErrSinkRejected` and does not move PostgreSQL’s durable bookmark beyond that batch.

After a reconnect, PostgreSQL may resend a batch that reached the sink but was not acknowledged before a crash. That is why sink application must be idempotent. The normal delivery guarantee is at-least-once delivery with no acknowledged data loss, not magical exactly-once delivery.

## Recovery paths

| Situation | What the current reader does | What must already exist |
| --- | --- | --- |
| Temporary network/database disconnect during `Run` | Reconnects, checks the existing slot, and resumes it. | The PostgreSQL slot and its retained WAL. |
| Process restarts after a successfully installed snapshot | A higher-level runtime should recreate the reader and call `Run` against the existing slot. | Durable local projection/cursor ownership information and the slot. |
| Sink rejects a batch | Stops with `ErrSinkRejected`; it does not acknowledge that batch. | A repaired/replaced sink before retrying. |
| Graceful shutdown | Stops `Run`; the persistent slot remains, so a future runtime can resume it. | The slot and durable local state. |
| Slot is missing or invalidated | Returns `ErrSlotInvalidated`; it never silently creates a replacement slot. | An explicit resync/source-replacement workflow. |
| Bootstrap fails before it returns | Cleans up its temporary SQL transaction/connection and drops the slot it created when possible. | Nothing should be installed locally. |
| Hard process crash during bootstrap | PostgreSQL can retain an orphan persistent slot. | A future control-plane cleanup/recovery policy. |

The difference between a normal reconnect and a missing slot is deliberate. A reconnect resumes the same history. A newly created slot begins a different history and cannot prove that it matches the local projection, so it must be an explicit rebuild operation.

## Why there is no `InitializeSlot`

Earlier designs had an `InitializeSlot` operation: create a slot now, then do other work later. It has been removed.

By itself, a created slot is only a bookmark. It does not provide the matching table rows, and separately reading the table later reintroduces the snapshot/stream gap. A caller could also create a slot and forget it, causing PostgreSQL to retain WAL indefinitely.

`Bootstrap` is the safer public operation because it creates the slot, obtains the exported snapshot, reads the paired rows, and starts the stream as one carefully ordered lifecycle.

## What is implemented now, and what belongs above it

The current code is the source-side CDC primitive. It provides a gap-free bootstrap boundary, transaction-aware decoding, sink-before-ack ordering, reconnect/resume behaviour, typed failures, and tests around the important races.

It is not yet the entire production watchd product. The higher-level runtime/control plane still needs to provide:

- durable local projection storage and persisted applied cursors;
- a watch API and SDK that let applications register/query projections;
- coordination when multiple scopes share one source stream;
- leader election or ownership so two replicas do not consume the same slot unintentionally;
- explicit resync, source replacement, and orphan-slot cleanup after a hard crash;
- operational monitoring for slot lag, retained WAL, failed sinks, and bootstrap progress;
- authorization and tenancy boundaries around sources, publications, and scopes.

Those are not optional details for a full production service. They are deliberately separate from the narrow job of making a source snapshot and WAL stream agree on one cursor.

## Testing the boundary

The integration tests use a real PostgreSQL instance and arrange source writes before, during, and after bootstrap. They verify that rows visible in the exported snapshot appear in the returned snapshot, while later committed writes appear in the stream, with no gap or duplication caused by the boundary itself.

They also exercise invalid scopes, failed snapshot queries, publication/permission failures, missing slots, cancellation, and cleanup. Run them with:

```sh
make postgres-up
make integration
```

The commands require Docker for the PostgreSQL test container.
