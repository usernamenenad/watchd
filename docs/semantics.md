# watchd v0 semantics

This document is the v0 contract. Implementation work must preserve these statements or explicitly change this document in the same pull request.

## Purpose

`watchd` keeps a consumer-maintained projection of selected PostgreSQL state recoverable and measurably fresh. PostgreSQL is always the source of truth; `watchd` is not an authoritative event store or task queue.

## Supported source

v0 supports one configured PostgreSQL database and one or more explicitly registered projection tables. A producer updates its application data and its projection tables in the same PostgreSQL transaction.

A projection table must have a stable primary key. v0 scopes every table using one configured equality key (initially expected to be `tenant_id`); `tenant_id` is an example name, not a requirement of the protocol.

## Terms

- **scope**: the source-defined subset of a projection a watcher is allowed to receive, such as `tenant_id = acme`.
- **cursor**: an opaque, monotonically ordered PostgreSQL logical-replication position. Clients persist and return it; they must not derive meaning from its representation.
- **snapshot**: a consistent source read of one scope plus the cursor immediately after which watching must continue.
- **change**: an insert, update, or delete of a projected row, including its key, resulting state when applicable, and commit cursor.
- **progress**: a statement that every committed change relevant to a scope through a cursor has been delivered to a watcher.
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
- v0 makes no ordering or consistency claim across sources, databases, or independently watched scopes.

## Freshness rule

A watcher may call a scope fresh only after it has applied all relevant changes through a `Progress(scope, cursor)` event. A network connection, successful subscription, or recent individual change is not sufficient evidence of freshness.

After a `Resync`, process restart, or local projection corruption, the scope is stale until a replacement snapshot is installed and progress is observed.

## Conceptual API

The initial implementation may use in-process Go interfaces before choosing a network transport. The eventual API has these conceptual operations:

```text
Snapshot(source, scope) -> rows, cursor
Watch(source, scope, resume_cursor) -> change-batch | progress | resync
```

## Explicit non-goals

v0 does not provide exactly-once delivery, durable message history, task scheduling, event sourcing, arbitrary SQL filters/views, client writes, multi-source transactions, cross-region high availability, dynamic repartitioning, or sources other than PostgreSQL.
