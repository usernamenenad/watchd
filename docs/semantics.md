# Initial semantics

This is a design placeholder, not an implemented guarantee.

## Freshness

A watcher may serve data as fresh only when it has received a `Progress` statement covering the requested scope through a known source cursor.

## Recovery

When a watcher cannot safely replay from its cursor, it receives `Resync`. It must obtain a consistent snapshot, replace its projection atomically, and resume after the snapshot cursor.

## Source of truth

PostgreSQL remains the authoritative store. `watchd` distributes notifications and supports recovery; it must not become a second authoritative message store.

## v0 boundary

The initial product will support one PostgreSQL source and explicit projection tables. Cross-database replication, arbitrary SQL views, bidirectional sync, and dynamic repartitioning are out of scope.

