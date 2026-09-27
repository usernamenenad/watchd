package watch

import (
	"fmt"
	"time"

	"github.com/usernamenenad/watchd/internal/cdc"
)

// Event is one message on a watch stream. A stream is exactly one of:
//
//	SnapshotBegin, SnapshotRows..., SnapshotEnd, then live events
//	live events only (resumed from a cursor)
//	Resync (and the stream ends)
//
// where live events are Batch and Progress, in source commit order.
type Event interface {
	isEvent()
}

// SnapshotBegin starts a snapshot. The watcher's projection is stale from
// here until the Progress that follows SnapshotEnd.
type SnapshotBegin struct{}

// SnapshotRows carries one primary-key-ordered batch of the scope's rows.
// Row values are PostgreSQL text or nil for SQL NULL; see docs/semantics.md.
type SnapshotRows struct {
	Rows []map[string]any
}

// SnapshotEnd ends a snapshot: the rows received since SnapshotBegin now
// replace the watcher's projection, atomically. It deliberately carries no
// cursor; the first Progress after it does.
type SnapshotEnd struct{}

// Batch is the part of one committed source transaction that belongs to the
// watcher's projection and scope. It is never split across events.
type Batch struct {
	// Cursor is the position just after the transaction. Once the batch is
	// applied, a watcher may persist it and later resume from it.
	Cursor cdc.Cursor
	// CommitTime is when the transaction committed, by the source's clock.
	// It is informational, for measuring latency; order comes from Cursor.
	CommitTime time.Time
	Changes    []cdc.Change
}

// Progress states that every relevant committed change through Cursor has
// been delivered. After applying everything before it, a watcher is fresh
// as of Cursor, and may resume from it.
type Progress struct {
	Cursor cdc.Cursor
}

// ResyncReason says why a watch cannot continue from where it is.
type ResyncReason int

const (
	// ResyncCursorUnavailable: the resume cursor is not replayable here, for
	// example after a watchd restart or once the replay window moved past it.
	ResyncCursorUnavailable ResyncReason = iota + 1
	// ResyncSlowWatcher: the watcher fell further behind than its queue
	// allows, so it was dropped rather than slowing the source.
	ResyncSlowWatcher
	// ResyncSnapshotWindowClosed: the snapshot could not be paired with the
	// stream safely; a new snapshot will be.
	ResyncSnapshotWindowClosed
	// ResyncSourceStopped: the source stream ended, for example because its
	// replication slot was invalidated.
	ResyncSourceStopped
)

func (r ResyncReason) String() string {
	switch r {
	case ResyncCursorUnavailable:
		return "cursor-unavailable"
	case ResyncSlowWatcher:
		return "slow-watcher"
	case ResyncSnapshotWindowClosed:
		return "snapshot-window-closed"
	case ResyncSourceStopped:
		return "source-stopped"
	default:
		return fmt.Sprintf("ResyncReason(%d)", int(r))
	}
}

// Resync ends a watch: the watcher must mark its projection stale, and watch
// again without a cursor to receive a new snapshot.
type Resync struct {
	Reason ResyncReason
}

func (SnapshotBegin) isEvent() {}
func (SnapshotRows) isEvent()  {}
func (SnapshotEnd) isEvent()   {}
func (Batch) isEvent()         {}
func (Progress) isEvent()      {}
func (Resync) isEvent()        {}
