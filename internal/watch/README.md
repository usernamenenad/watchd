# Watch runtime

`Hub` fans one source's committed transactions out to scope watchers, with
replay, progress, and explicit resync. It is the source's `TransactionSink`,
and it takes snapshots through the source (`*cdc.Source`).

It is intentionally soft state: it keeps no history across restarts, so after
one, every watcher resyncs once, rather than risk data loss or an incorrect
freshness claim.

## A watch

`Hub.Watch(ctx, Request{Projection, Scope, ResumeCursor}, send)` streams
events until the watch ends:

| Request | What the watcher receives |
| --- | --- |
| No cursor | `SnapshotBegin`, `SnapshotRows`..., `SnapshotEnd`, then live events |
| A cursor the hub can replay | Replayed `Batch` events after the cursor, then live events |
| A cursor the hub cannot replay | `Resync(cursor-unavailable)` |

Live events are `Batch` (one committed transaction's changes to this
projection and scope, never split) and `Progress` (everything relevant
through a cursor has been delivered). A watcher is fresh only after it applies
everything before a `Progress`. Both carry cursors a watcher may persist and
resume from.

`Resync` always ends the watch. The watcher marks its projection stale and
watches again without a cursor.

| Reason | Cause |
| --- | --- |
| `cursor-unavailable` | The hub restarted, or its bounded replay window no longer reaches the cursor |
| `slow-watcher` | The watcher's queue (`WatcherQueue`) filled up; it is dropped rather than slowing the source |
| `snapshot-window-closed` | The snapshot could not be paired with the stream (`cdc.ErrSnapshotWindowClosed`) |
| `source-stopped` | The source's stream ended; the runtime called `Hub.Stop` |

## The snapshot handoff

The watcher registers with the hub *before* its snapshot is read, so every
transaction streamed from then on queues for it. It also keeps its scope's
transactions already in the replay window: a commit can be streamed before
it becomes visible to new snapshots, for example while it waits on a
synchronous standby. After the snapshot, `cdc.Snapshot.Covers` decides by
transaction ID which of all these transactions the rows already contain, and
the watcher receives exactly the others.

The first `Progress` after a snapshot is the stream's own position, not the
snapshot cursor, unless nothing has been streamed yet. If the stream still
lags behind the snapshot cursor, a commit just before that cursor may not
have arrived, and a watcher resuming from the cursor would skip it.

## Bounds

- `MaxReplayTransactions` (default 10,000) bounds the replay window. The
  window starts at the hub's first snapshot; before that, no cursor is
  replayable.
- `WatcherQueue` (default 1,024) bounds each watcher's queued transactions.
  `Accept` never blocks on a watcher.

Routing uses each change's key: a projection's scope column must be part of
its primary key (enforced by `internal/cdc`). A request's `Scope` is compared
with the key's PostgreSQL text form, so a `uuid` scope must be sent in its
canonical lowercase form.
