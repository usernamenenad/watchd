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

Every cursor carries the ID of the source it came from, and cursors from different sources cannot be compared. A committed transaction's cursor is the position just after its commit: the position watchd acknowledges to PostgreSQL once the sink accepts the transaction, and the one a consumer persists after applying it. `Transaction.After(cursor)` tells a consumer resuming from a cursor whether it still has to apply a transaction; it uses the same rule PostgreSQL applies to a replication start position.

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

`SourceID` names this configured source and must match the reader's `ReaderConfig.SourceID`. `Schema` and `Table` select the PostgreSQL table. `ScopeColumn` is the column used to divide the table into tenants, accounts, workspaces, or another unit of ownership. `PrimaryKey` is the ordered list of primary-key columns that identifies a row reliably, so updates and deletes can be applied to the correct local row.

`ScopeColumn` must be one of the `PrimaryKey` columns. With PostgreSQL's default replica identity, a `DELETE` carries only the primary key, so a scope column outside it would leave a delete impossible to route to the scope that holds the row.

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
snapshot, err := reader.Snapshot(ctx, projection, scope, func(ctx context.Context, rows []map[string]any) error {
    return installRows(ctx, rows) // called once per primary-key-ordered batch
})
// snapshot.Cursor:            where the live stream takes over
// snapshot.Covers(transaction): whether a streamed transaction is already in the rows
```

Rows are not returned all at once. They are delivered to a `SnapshotRowSink` in primary-key-ordered batches of at most `ReaderConfig.SnapshotBatchRows` (default 1000), so a large scope is never held in memory whole. The values are PostgreSQL text form (or `nil`) so that the snapshot and logical-decoding paths use compatible representations - see "Value encoding" in [the v0 semantics contract](semantics.md) for the full rules and size limits.

The returned `Snapshot` pairs those rows with the live stream in two ways. `Cursor` is the WAL position where the stream takes over, and the position a consumer persists until it applies a later transaction. `Covers(transaction)` answers, exactly, whether a streamed transaction's effects are already in the rows.

### MVCC snapshot and transaction visibility

A repeatable-read transaction reads the database as of one instant: its MVCC snapshot. PostgreSQL describes that snapshot with `pg_current_snapshot()` as `xmin:xmax:in-progress-list`. Every transaction ID below `xmin` had finished, every ID at or above `xmax` had not, and the list names the ones in between that were still running. A committed transaction is in the snapshot's rows exactly when it had finished by then.

watchd records this for every scoped read, and `Snapshot.Covers` checks a streamed transaction's ID against it. The decision is made by transaction ID rather than by WAL position on purpose: a transaction can commit between the moment a snapshot is fixed and the moment any WAL position is read, and then it is invisible to the rows while its commit position looks "before" the boundary. Comparing positions alone silently loses such a transaction; comparing transaction IDs cannot.

### Exported snapshot

`CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT` makes PostgreSQL create a logical slot and, at the same time, export an MVCC snapshot that matches the slot's *consistent point* exactly: every transaction that commits before the consistent point is visible in it, and the slot decodes every transaction that commits after it. Another session can adopt it with `SET TRANSACTION SNAPSHOT`, which `Bootstrap` does to read the first scope. The exported snapshot is only valid until the replication connection that created it runs another command.

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
snapshot, err := reader.Bootstrap(ctx, projection, scope, installRows)
```

`Bootstrap` is the only slot-creation path. It does the following, in this order:

1. Validates the projection and scope. It checks identifiers, connects to the source, verifies the publication/table relationship, and verifies that the configured scope column and primary key exist.
2. Opens a replication connection and creates a persistent `pgoutput` logical slot with `EXPORT_SNAPSHOT`. PostgreSQL returns the slot's consistent point `P` and the name of a snapshot that matches it exactly.
3. On a normal SQL connection, begins a repeatable-read read-only transaction, adopts that exported snapshot with `SET TRANSACTION SNAPSHOT`, records it with `pg_current_snapshot()`, and streams the scoped `SELECT` to the row sink.
4. Builds `Snapshot{Cursor: P}` with that visibility.
5. Before returning, starts logical replication at exactly `P` on the original replication connection. The returned `Reader` now owns an already-started stream.

Because the exported snapshot and `P` describe the same instant, the stream from `P` contains exactly the transactions the rows do not: no gap and no overlap. `Covers` is true for none of them.

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
| Hard process crash during bootstrap | The slot survives. On the next start, `Source.Start` finds it and resumes it with `Run`. Every consumer takes a fresh snapshot after a restart anyway, so adopting it is safe, and from then on the retention policy governs it. | watchd restarting with the same `slot_name`. A slot that no watchd will ever run against again is outside watchd's reach: PostgreSQL's `max_slot_wal_keep_size` is the only backstop. |
| Retained WAL reaches the retention budget | Stops streaming, drops the slot, and returns `ErrRetainedWALBudgetExceeded`. See [Retention policy](#retention-policy). | Every consumer rebuilds from a snapshot on the new slot the next start creates. |

The difference between a normal reconnect and a missing slot is deliberate. A reconnect resumes the same history. A newly created slot begins a different history and cannot prove that it matches the local projection, so it must be an explicit rebuild operation.

## Retention policy

PostgreSQL keeps every WAL segment the slot has not confirmed. Anything that stops acknowledgement retains WAL on a server watchd does not own: a watchd outage, a sink that never accepts, a bug. So the reader bounds it. While `Run` streams, it samples `pg_replication_slots` every `RetentionSampleInterval` (30s) and compares retained WAL (the current WAL position minus the slot's `restart_lsn`) with the effective budget: the stricter of `MaxRetainedWALBytes` (1 GiB) and the server's `max_slot_wal_keep_size`.

| State | Entered at | What happens | Operator action |
| --- | --- | --- | --- |
| `ok` | below warn | Nothing. | None. |
| `warn` | `RetentionWarnFraction` (0.5) of the budget | A warning log. | Find what holds acknowledgement back: `watchd.cdc.ack_lag`, `watchd.cdc.sink.duration`, `watchd.cdc.stream.state`. |
| `degrade` | `RetentionDegradeFraction` (0.8) | An error log: the terminal action is close. | Act now: fix the cause, or accept the rebuild. Raising the budget only buys time. |
| `terminal` | the budget | Streaming stops, the slot is dropped, and `Run` returns `ErrRetainedWALBudgetExceeded`. The daemon ends every watch with `Resync` and exits with code 3. | Fix the cause and restart. The next start builds a new slot, and every consumer rebuilds from a snapshot. |

A state is left only once retained WAL falls 5% of the budget below its threshold, so a slot hovering at a threshold does not flap. `terminal` is final. States and transitions are exported as `watchd.cdc.retention.state` and `watchd.cdc.retention.transitions`, and `ReaderStats.RetentionState` reports the current one. The slot is sampled as soon as `Run` starts, so a slot that grew past its budget while watchd was down is caught at once.

Between transactions, the reader acknowledges the position in each PostgreSQL keepalive, not only the end of the last accepted transaction. A logical walsender's keepalive carries its sent position: every transaction committing before it was already streamed, or skipped because it touched no published table. Without this, a slot whose published tables are idle while other tables are busy would retain WAL forever, and the policy would drop a healthy slot. Inside a transaction the keepalive position is never acknowledged, because that transaction is not accepted yet.

## Why there is no `InitializeSlot`

Earlier designs had an `InitializeSlot` operation: create a slot now, then do other work later. It has been removed.

By itself, a created slot is only a bookmark. It does not provide the matching table rows, and separately reading the table later reintroduces the snapshot/stream gap. A caller could also create a slot and forget it, causing PostgreSQL to retain WAL indefinitely.

`Bootstrap` is the safer public operation because it creates the slot, reads the paired rows, and starts the stream as one carefully ordered lifecycle.

## Adding a second scope: `Snapshot`

`Bootstrap` only creates a slot once per source. That leaves no defined way for a later scope to catch up, since creating a slot per scope would exhaust `max_replication_slots` and retain WAL per scope for no reason — one slot already carries every scope's changes once a consumer is subscribed to the whole publication.

`Snapshot` is the operation for that case:

```go
snapshot, err := reader.Snapshot(ctx, projection, scope, installRows)
```

It assumes the slot already exists — it returns `ErrSlotNotFound` if `Bootstrap` has not run yet — and otherwise does not touch the slot at all. It has no exported slot snapshot to adopt, so it establishes its boundary itself, on its own connection:

1. Reads the WAL insert position, `pg_current_wal_insert_lsn()`, as the cursor `L`. This happens *before* the snapshot is fixed, so every transaction the snapshot cannot see commits after `L`.
2. Begins a repeatable-read read-only transaction whose first statement, `pg_current_snapshot()`, fixes and records its MVCC snapshot.
3. Streams the scoped `SELECT` to the row sink.

The consumer then installs the rows and applies every streamed transaction for which `snapshot.Covers(transaction)` is false, in stream order, skipping the ones it covers. Transactions that committed between reading `L` and fixing the snapshot are both after `L` and in the rows; `Covers` is what keeps them from being applied twice. Resuming from `L` alone can therefore replay extra transactions, but never skip one.

There is one rare case a cursor cannot express. PostgreSQL writes a transaction's commit record before the transaction becomes visible to new snapshots, and with synchronous replication it can stay invisible for as long as it waits on the standby. Such a transaction can commit *before* `L` and still be missing from the rows. A consumer that has been streaming since before the snapshot - like one serving many scopes from one stream - must therefore check the transactions it already holds with `Covers`, not only the ones that arrive after `L`.

Because `Snapshot` never claims the slot's replication connection, any number of scopes can call it at once, and it works whether or not `Run` is currently streaming from the slot.

### The replay-window pairing rule

A cursor from `Snapshot` is only a safe resume boundary if the slot has retained every change after it. PostgreSQL will eventually reuse WAL older than a slot's `restart_lsn`, so if the retained window has already moved past the cursor by the time `Snapshot` finishes, resuming from that cursor would silently skip changes instead of failing loudly.

`Snapshot` checks this before returning: it re-reads the slot's `restart_lsn` and compares it against the cursor it just captured. If the window has already closed, it returns `ErrSnapshotWindowClosed` instead of a boundary that looks usable but is not. The caller's only correct response is to call `Snapshot` again for a fresh cursor — there is no way to repair a closed window after the fact.

## Putting it together: `Source`

`cdc.Source` owns the reader and applies the rules above, so the runtime asks only for "a snapshot of this scope" and never chooses a primitive itself:

| Situation | What `Source` does |
| --- | --- |
| `Start`, and the slot exists | Resumes it with `Run` |
| `Start`, and there is no slot | Waits: no slot exists until a scope needs one, so none is ever forgotten |
| First `Snapshot` on a source with no slot | `Bootstrap` creates the slot with that scope's rows, then `Run` consumes the stream `Bootstrap` started |
| Every later `Snapshot` | `Reader.Snapshot` against the live slot, while `Run` keeps streaming |
| `Run` ends (shutdown or a terminal error such as `ErrSlotInvalidated`) | `Done` closes, `Err` reports why, and later `Snapshot` calls fail with `ErrSourceStopped`, since rows without a live stream cannot stay fresh |

After a process restart, `Start` finds the slot and resumes it; consumers then take fresh snapshots, because the process kept no replay history.

## What is implemented now, and what belongs above it

The current code is the source-side CDC primitive. It provides a gap-free bootstrap boundary, transaction-aware decoding, sink-before-ack ordering, reconnect/resume behaviour, typed failures, and tests around the important races.

It is not yet the entire production watchd product. The higher-level runtime/control plane still needs to provide:

- durable local projection storage and persisted applied cursors;
- a watch API and SDK that let applications register/query projections;
- routing each scope's committed changes from the one shared stream to the right local projection, applying `Snapshot.Covers` to transactions already streamed, and retrying `Snapshot` when `ErrSnapshotWindowClosed` occurs;
- leader election or ownership so two replicas do not consume the same slot unintentionally;
- explicit source replacement, and cleanup of a slot no watchd will run against again (only `max_slot_wal_keep_size` bounds that one; see [Retention policy](#retention-policy));
- alerting and dashboards on the exported metrics ([docs/observability.md](observability.md)), including slot lag and retained WAL;
- authorization and tenancy boundaries around sources, publications, and scopes.

Those are not optional details for a full production service. They are deliberately separate from the narrow job of making a source snapshot and WAL stream agree on one cursor.

## Testing the boundary

The integration tests use a real PostgreSQL instance and arrange source writes before, during, and after bootstrap. They verify that rows visible in the exported snapshot appear in the returned snapshot, while later committed writes appear in the stream, with no gap or duplication caused by the boundary itself.

`snapshot_boundary_integration_test.go` pins the boundary precisely: it commits a write at an exact moment relative to the snapshot (right after the MVCC snapshot is fixed, and just before the rows are read), then rebuilds the projection from the rows plus only the transactions `Covers` rejects, and requires the result to equal PostgreSQL.

They also exercise invalid scopes, failed snapshot queries, publication/permission failures, missing slots, cancellation, and cleanup. Run them with:

```sh
make postgres-up
make integration
```

The commands require Docker for the PostgreSQL test container.
