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
