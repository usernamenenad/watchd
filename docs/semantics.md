# watchd v0 semantics

This document is the v0 contract. Implementation work must preserve these statements or explicitly change this document in the same pull request.

## Purpose

`watchd` keeps a consumer-maintained projection of selected PostgreSQL state recoverable and measurably fresh. PostgreSQL is always the source of truth; `watchd` is not an authoritative event store or task queue.

## Supported source

v0 supports one configured PostgreSQL database and one or more explicitly registered projection tables. A producer updates its application data and its projection tables in the same PostgreSQL transaction.

A projection table must have a stable primary key. v0 scopes every table using one configured equality key (initially expected to be `tenant_id`); `tenant_id` is an example name, not a requirement of the protocol.

## Terms

- **scope**: the source-defined subset of a projection a watcher is allowed to receive, such as `tenant_id = acme`.
- **cursor**: an opaque, monotonically ordered PostgreSQL logical-replication position. Clients persist and return it; they must not derive meaning from its representation. Cursors from the same source are comparable; the API exposes that comparison, so clients never decode a cursor to perform it.
- **snapshot**: a consistent source read of one scope plus the cursor immediately after which watching must continue.
- **change**: an insert, update, or delete of a projected row, including its key, resulting state when applicable, and commit cursor.
- **progress**: a statement that every committed change relevant to a scope through a cursor has been delivered to a watcher.
- **consistent cut**: given the progress cursors `c1..cn` of scopes `s1..sn` of one source, `min(c1..cn)` is a boundary a client may treat as a single snapshot-consistent state across those scopes.
- **resync**: a statement that replay cannot be performed safely; it revokes the watcher's freshness until it replaces its projection with a new snapshot.

## Value encoding

This is the v0 contract for how one PostgreSQL column value is represented, both in `internal/cdc` today and as a constraint any future network encoding (issue #4) must preserve losslessly.

A value is exactly one of:

- **text**: the column's PostgreSQL type-output-function representation - the same string PostgreSQL's own output function for that type produces (this is what `COPY ... TO` in text format and pgoutput both use). This applies uniformly to every column type, including `bytea`, arrays, composite types, `json`/`jsonb`, enums, and domain types: watchd starts logical replication without pgoutput's `binary` option, so PostgreSQL always sends column data through its text output function, whatever the underlying type. No type category is rejected or specially cased at the value layer for this reason; a consumer that needs a typed value parses the text itself (for example, treating a `tags text[]` value's `{a,b}` text as an array literal, or a `jsonb` value's text as JSON).

  **This is not always the same as SQL's `column::text` cast.** `boolean` is the clearest counterexample: `true::text` casts to `"true"`, while the type's output function - and therefore the actual value a watcher receives - is `"t"`. `column::text` is not a safe way to predict or test the wire encoding of a new type; verify against the real replication path, not a SQL cast, when adding one.

  Measured directly against PostgreSQL (`internal/cdc/type_matrix_integration_test.go`, run across every PostgreSQL version this project supports): integers, `numeric`, `real`/`double precision`, `uuid`, arrays, `jsonb`, enums, `bytea`, domains, `inet`/`cidr`, `interval`, `date`/`time`/`timestamp`, and `char(n)` (which blank-pads to its declared width - `'abc'` in a `char(8)` column decodes as `"abc     "`, not `"abc"`) all decode exactly as their PostgreSQL output-function text, matching a `::text` cast. `boolean` is the one documented exception. `timestamptz` is intentionally not pinned to a fixed string: its output depends on the replication session's `TimeZone` setting, which is server configuration, not part of this per-type contract - a consumer of a `timestamptz` column must account for that offset itself. `money` is not covered: its formatting is locale-dependent on the server, which is a bad fit for a fixed wire contract and is better addressed as a configuration-time rejection under #7 than assumed here.

- **null**: SQL NULL. Represented as Go `nil`, never as an empty string or a sentinel value.
- **unchanged TOAST**: a TOASTed column PostgreSQL omitted from an `UPDATE` message because it did not change. Represented as the distinct `cdc.UnchangedToast` type - never `nil`, never an empty string - so an applier can tell "no change, keep the existing value" apart from "the value is now NULL." Only `Change.Values` can contain this; a `Snapshot` row is a full read and never omits a column this way.

A **key** (`Change.Key`, the replica-identity columns identifying a row) is always present and always text: PostgreSQL sends replica-identity columns in full on every `UPDATE`/`DELETE`, never as NULL (a replica-identity column that could be NULL cannot identify a row) and never as unchanged TOAST (only non-key columns are TOASTed independently of the row's identity). `Change.Key` is therefore typed `map[string]string`, distinct from `Change.Values`' `map[string]any` - the narrower type states this guarantee rather than leaving every caller to re-derive it.

### Size limits

A single value is bounded by a configured per-value limit (`ReaderConfig.MaxValueBytes`, default 1 MiB), independent of the whole-transaction bound (`MaxTransactionBytes`, `MaxTransactionChanges`). `MaxValueBytes` must not exceed `MaxTransactionBytes`. Exceeding either raises a decode error (`cdc.ErrValueTooLarge` for one value, `cdc.ErrTransactionTooLarge` or `cdc.ErrTransactionTooManyChanges` for the batch) instead of retaining an unbounded transaction in memory. These are terminal, not retryable: the same oversized value or batch would recur on any retry.

### What is not representable

A pgoutput column encoding outside text/null/unchanged-toast (for example, binary-format tuple data, which watchd's configuration never requests) is rejected explicitly with `cdc.ErrUnsupportedColumnEncoding` rather than passed through as an opaque byte slice. Silently forwarding an unrepresentable encoding would let a future protocol change mis-decode a value instead of failing loudly.

## Bootstrap and recovery

When a watcher has no usable cursor, it obtains a snapshot. The snapshot and its change stream must have no gap: every committed mutation is represented either in the snapshot or in the stream after the snapshot cursor.

When a watcher reconnects with a usable cursor, it may receive replayed changes beginning at or before that cursor. Changes are therefore **at-least-once** and consumers must apply them idempotently.

When `watchd` cannot replay safely—for example, its bounded replay window was exceeded, it restarted, the source slot was invalidated, or the supplied cursor is unknown—it emits `Resync`. The watcher must:

1. Mark the affected scope stale.
2. Stop claiming freshness for that scope.
3. Obtain a new snapshot.
4. Replace the local projection atomically.
5. Resume watching after the snapshot cursor.

## Transaction and ordering rules

- Only committed PostgreSQL changes are eligible for delivery.
- A watcher must never observe a partial source transaction. Changes from one source transaction are delivered as an atomic batch.
- Batches are ordered by committed source cursor for a single configured source.
- Every scope of one configured source is derived from that source's single replication position, so all scopes of one source share one total order: batches delivered to any of that source's scopes are totally ordered by commit cursor, and cursors from different scopes of that source are mutually comparable.
- A client watching several scopes `s1..sn` of one source may therefore compute the consistent cut `min(c1..cn)` over their progress cursors and treat it as a state that existed in the source, never a mixture that did not.
- v0 makes no ordering or consistency claim across sources, across databases, or between a scope and an unrelated source. Comparing cursors from different sources is a caller error, and the API rejects it rather than returning a meaningless result.

## Freshness rule

A watcher may call a scope fresh only after it has applied all relevant changes through a `Progress(scope, cursor)` event. A network connection, successful subscription, or recent individual change is not sufficient evidence of freshness.

After a `Resync`, process restart, or local projection corruption, the scope is stale until a replacement snapshot is installed and progress is observed.

A resync on one scope invalidates any consistent cut computed over it: the scope's progress cursor is discarded, so a client must exclude that scope from `min(...)` until it installs a new snapshot and the scope reports progress again.

## Conceptual API

The initial implementation may use in-process Go interfaces before choosing a network transport. The eventual API has these conceptual operations:

```text
Snapshot(source, scope) -> rows, cursor
Watch(source, scope, resume_cursor) -> change-batch | progress | resync
Compare(cursor, cursor) -> before | equal | after   // same source only, error otherwise
```

`Compare` is how a client establishes cross-scope order and computes a consistent cut; it never decodes a cursor's representation to do so.

## Explicit non-goals

v0 does not provide exactly-once delivery, durable message history, task scheduling, event sourcing, arbitrary SQL filters/views, client writes, multi-source transactions, cross-region high availability, dynamic repartitioning, or sources other than PostgreSQL.
