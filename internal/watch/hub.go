package watch

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/usernamenenad/watchd/internal/cdc"
)

var (
	// ErrInvalidConfig indicates a hub configuration that cannot be served.
	ErrInvalidConfig = errors.New("watch: invalid hub configuration")
	// ErrUnknownProjection indicates a watch request naming no configured
	// projection.
	ErrUnknownProjection = errors.New("watch: unknown projection")
)

const (
	defaultMaxReplayTransactions = 10_000
	defaultWatcherQueue          = 1024
)

// Snapshotter reads one scope and returns where the stream takes over from
// its rows. *cdc.Source implements it: Bootstrap for a source's first scope,
// Reader.Snapshot for every later one.
type Snapshotter interface {
	Snapshot(ctx context.Context, spec cdc.ProjectionSpec, scope cdc.Scope, sink cdc.SnapshotRowSink) (cdc.Snapshot, error)
}

// Config configures a Hub for one source.
type Config struct {
	// SourceID is the source whose transactions the hub accepts; resume
	// cursors are parsed as cursors of this source.
	SourceID string
	// Projections are the projections watchers may name, by name.
	Projections map[string]cdc.ProjectionSpec
	// MaxReplayTransactions bounds how many recent transactions the hub
	// keeps for watchers resuming from a cursor. A cursor older than the
	// oldest kept transaction receives Resync.
	MaxReplayTransactions int
	// WatcherQueue bounds how many relevant transactions may wait for one
	// watcher. A watcher that falls further behind receives Resync instead
	// of slowing the source.
	WatcherQueue int
}

// Request asks to watch one scope of one projection.
type Request struct {
	Projection string
	// Scope is the scope column's value, in PostgreSQL text form (for a uuid
	// column, the canonical lowercase form).
	Scope string
	// ResumeCursor is a cursor from an earlier Batch or Progress, or empty to
	// start from a snapshot.
	ResumeCursor string
}

// Hub fans one source's committed transactions out to scope watchers. It is
// the source's TransactionSink, and it pairs every new watcher with the
// source's single stream without a gap:
//
//   - A watcher with a usable cursor replays from the hub's bounded window of
//     recent transactions, then goes live. No snapshot is needed.
//   - A watcher without one gets a snapshot, then every streamed transaction
//     the snapshot does not cover, then goes live.
//   - A watcher whose cursor is not replayable here gets Resync.
//
// The hub is soft state: it keeps no history across restarts, so after one
// every watcher resyncs once.
type Hub struct {
	sourceID    string
	projections map[string]projection
	snapshotter Snapshotter
	maxReplay   int
	queueSize   int

	mu sync.Mutex
	// ring holds the most recent transactions, oldest first.
	ring []cdc.Transaction
	// received is the cursor of the newest accepted transaction. The stream
	// delivers transactions in commit order, so every transaction before it
	// has been accepted too.
	received    cdc.Cursor
	hasReceived bool
	// floor is the oldest resume cursor the ring can still serve. It is
	// unset until the first snapshot, because until then the hub cannot
	// vouch that its ring reaches back to any watcher's cursor.
	floor       cdc.Cursor
	hasFloor    bool
	lastEvicted cdc.Cursor
	hasEvicted  bool
	watchers    map[*watcher]struct{}
	stopped     chan struct{}
	stopOnce    sync.Once
}

// queued is a relevant transaction waiting for its watcher. The transaction
// itself is kept because only it can be checked against a snapshot.
type queued struct {
	transaction cdc.Transaction
	changes     []cdc.Change
}

type projection struct {
	spec  cdc.ProjectionSpec
	table string
}

// watcher is one Watch call's registration with the hub.
type watcher struct {
	projection projection
	scope      string
	// queue receives this watcher's relevant transactions after it
	// registers, with the changes that belong to it.
	queue chan queued
	// wake is signalled on every accepted transaction, relevant or not, so
	// the watcher can report progress.
	wake chan struct{}
	// overflow is closed when the watcher's queue was full.
	overflow chan struct{}
}

// New creates a hub serving cfg's projections, taking snapshots from
// snapshotter.
func New(cfg Config, snapshotter Snapshotter) (*Hub, error) {
	if cfg.SourceID == "" || len(cfg.Projections) == 0 || snapshotter == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.MaxReplayTransactions == 0 {
		cfg.MaxReplayTransactions = defaultMaxReplayTransactions
	}
	if cfg.WatcherQueue == 0 {
		cfg.WatcherQueue = defaultWatcherQueue
	}
	if cfg.MaxReplayTransactions < 0 || cfg.WatcherQueue < 0 {
		return nil, ErrInvalidConfig
	}

	projections := make(map[string]projection, len(cfg.Projections))
	for name, spec := range cfg.Projections {
		if name == "" || spec.SourceID != cfg.SourceID {
			return nil, fmt.Errorf("%w: projection %q must be named and belong to source %q", ErrInvalidConfig, name, cfg.SourceID)
		}
		projections[name] = projection{spec: spec, table: spec.Schema + "." + spec.Table}
	}

	return &Hub{
		sourceID:    cfg.SourceID,
		projections: projections,
		snapshotter: snapshotter,
		maxReplay:   cfg.MaxReplayTransactions,
		queueSize:   cfg.WatcherQueue,
		watchers:    make(map[*watcher]struct{}),
		stopped:     make(chan struct{}),
	}, nil
}

// Accept is the source's TransactionSink. It records transaction in the
// replay window and hands each watcher its part, and never blocks on a
// watcher: one whose queue is full is dropped with Resync instead.
func (h *Hub) Accept(_ context.Context, transaction cdc.Transaction) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.hasReceived {
		order, err := transaction.Cursor.Compare(h.received)
		if err != nil {
			return err
		}
		// After a reconnect, PostgreSQL may resend a transaction the hub
		// already accepted but the reader had not yet acknowledged.
		if order <= 0 {
			return nil
		}
	}
	h.received = transaction.Cursor
	h.hasReceived = true

	h.ring = append(h.ring, transaction)
	for len(h.ring) > h.maxReplay {
		h.evictLocked()
	}

	for w := range h.watchers {
		if changes := w.projection.changes(w.scope, transaction); len(changes) > 0 {
			select {
			case w.queue <- queued{transaction: transaction, changes: changes}:
			default:
				h.dropLocked(w)
				continue
			}
		}
		select {
		case w.wake <- struct{}{}:
		default:
		}
	}
	return nil
}

// Stop ends every watch with Resync(ResyncSourceStopped) and refuses new
// ones. The runtime calls it when the source's stream ends.
func (h *Hub) Stop() {
	h.stopOnce.Do(func() { close(h.stopped) })
}

// Watch serves one watch request, calling send for each event in order until
// ctx ends, send fails, or the watch ends with Resync (which returns nil).
func (h *Hub) Watch(ctx context.Context, req Request, send func(Event) error) error {
	projection, ok := h.projections[req.Projection]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownProjection, req.Projection)
	}
	w := &watcher{
		projection: projection,
		scope:      req.Scope,
		queue:      make(chan queued, h.queueSize),
		wake:       make(chan struct{}, 1),
		overflow:   make(chan struct{}),
	}
	select {
	case <-h.stopped:
		return send(Resync{Reason: ResyncSourceStopped})
	default:
	}

	if req.ResumeCursor != "" {
		cursor, err := cdc.ParseCursor(h.sourceID, req.ResumeCursor)
		if err != nil {
			return err
		}
		return h.resume(ctx, w, cursor, send)
	}
	return h.snapshot(ctx, w, send)
}

// resume replays the ring from cursor, then streams live. It needs no
// snapshot, but only works while the ring still reaches back to cursor.
func (h *Hub) resume(ctx context.Context, w *watcher, cursor cdc.Cursor, send func(Event) error) error {
	h.mu.Lock()
	if !h.replayableLocked(cursor) {
		h.mu.Unlock()
		return send(Resync{Reason: ResyncCursorUnavailable})
	}
	var backlog []Batch
	for _, transaction := range h.ring {
		after, err := transaction.After(cursor)
		if err != nil {
			h.mu.Unlock()
			return err
		}
		if after {
			backlog = w.appendBatch(backlog, transaction)
		}
	}
	h.watchers[w] = struct{}{}
	h.mu.Unlock()
	defer h.unregister(w)

	for _, batch := range backlog {
		if err := send(batch); err != nil {
			return err
		}
	}
	// The watcher's own cursor is a safe progress point: everything before
	// it was delivered to it earlier. Live transactions are still checked
	// against it, since a cursor can be ahead of what this hub has received.
	return h.live(ctx, w, cursor, true, send, func(transaction cdc.Transaction) (bool, error) {
		return transaction.After(cursor)
	})
}

// snapshot sends the scope's rows, then every transaction the snapshot does
// not cover, then streams live.
//
// The watcher registers before the snapshot is read, so every transaction
// streamed from then on queues for it. Transactions streamed earlier are
// normally in the snapshot's rows, but not always: a commit can be streamed
// before it becomes visible to new snapshots (for example while it waits on
// a synchronous standby). So the watcher also keeps its scope's transactions
// that were already in the ring, and the snapshot decides, by Covers, which
// of all of them it still needs.
func (h *Hub) snapshot(ctx context.Context, w *watcher, send func(Event) error) error {
	h.mu.Lock()
	var streamedTransactions []cdc.Transaction
	for _, transaction := range h.ring {
		if len(w.projection.changes(w.scope, transaction)) > 0 {
			streamedTransactions = append(streamedTransactions, transaction)
		}
	}
	h.watchers[w] = struct{}{}
	h.mu.Unlock()
	defer h.unregister(w)

	if err := send(SnapshotBegin{}); err != nil {
		return err
	}
	snapshot, err := h.snapshotter.Snapshot(ctx, w.projection.spec, cdc.Scope{Value: w.scope}, func(_ context.Context, rows []map[string]any) error {
		return send(SnapshotRows{Rows: rows})
	})
	switch {
	case errors.Is(err, cdc.ErrSnapshotWindowClosed):
		return send(Resync{Reason: ResyncSnapshotWindowClosed})
	case errors.Is(err, cdc.ErrSourceStopped):
		return send(Resync{Reason: ResyncSourceStopped})
	case err != nil:
		return err
	}
	// A watcher that fell behind while the rows were read must not install
	// them: it would have to discard them again straight away.
	select {
	case <-w.overflow:
		return send(Resync{Reason: ResyncSlowWatcher})
	default:
	}

	notCovered := func(transaction cdc.Transaction) (bool, error) {
		covered, err := snapshot.Covers(transaction)
		return !covered, err
	}
	var streamed []Batch
	for _, transaction := range streamedTransactions {
		need, err := notCovered(transaction)
		if err != nil {
			return err
		}
		if need {
			streamed = w.appendBatch(streamed, transaction)
		}
	}

	h.mu.Lock()
	h.recordSnapshotLocked(snapshot.Cursor)
	h.mu.Unlock()

	if err := send(SnapshotEnd{}); err != nil {
		return err
	}
	for _, batch := range streamed {
		if err := send(batch); err != nil {
			return err
		}
	}
	// The snapshot cursor is only a fallback progress point, used before the
	// hub has received anything: if the stream still lags behind it, a
	// commit just before it may yet arrive uncovered (see Snapshot.Covers).
	return h.live(ctx, w, snapshot.Cursor, false, send, notCovered)
}

// live streams w's queued transactions that pass need, and reports progress
// as the source advances. base is where w's projection stands before any
// live transaction; baseSafe says whether progress may be claimed at base
// even after the hub has received transactions before it.
func (h *Hub) live(ctx context.Context, w *watcher, base cdc.Cursor, baseSafe bool, send func(Event) error, need func(cdc.Transaction) (bool, error)) error {
	var progressed cdc.Cursor
	for first := true; ; first = false {
		// Every transaction accepted up to position has already been queued
		// for w, so once the queue is drained, w is complete through it.
		position, received := h.position()
		if !received || (baseSafe && later(base, position)) {
			position = base
		}

		for drained := false; !drained; {
			select {
			case item := <-w.queue:
				deliver, err := need(item.transaction)
				if err != nil {
					return err
				}
				if deliver {
					if err := send(Batch{Cursor: item.transaction.Cursor, Changes: item.changes}); err != nil {
						return err
					}
				}
				if later(item.transaction.Cursor, position) {
					position = item.transaction.Cursor
				}
			default:
				drained = true
			}
		}

		if first || later(position, progressed) {
			if err := send(Progress{Cursor: position}); err != nil {
				return err
			}
			progressed = position
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.overflow:
			return send(Resync{Reason: ResyncSlowWatcher})
		case <-h.stopped:
			return send(Resync{Reason: ResyncSourceStopped})
		case <-w.wake:
		}
	}
}

// position reports the newest accepted transaction's cursor.
func (h *Hub) position() (cdc.Cursor, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.received, h.hasReceived
}

// replayableLocked reports whether the ring holds every transaction a
// watcher resuming from cursor still needs.
func (h *Hub) replayableLocked(cursor cdc.Cursor) bool {
	if !h.hasFloor {
		return false
	}
	order, err := cursor.Compare(h.floor)
	return err == nil && order >= 0
}

// recordSnapshotLocked sets the replay floor at the first snapshot. The
// source started streaming at or before that snapshot's cursor, and the
// ring has kept everything streamed since, so from then on any cursor at or
// after the floor can be replayed.
func (h *Hub) recordSnapshotLocked(cursor cdc.Cursor) {
	if h.hasFloor {
		return
	}
	h.floor = cursor
	if h.hasEvicted && later(h.lastEvicted, h.floor) {
		h.floor = h.lastEvicted
	}
	h.hasFloor = true
}

// evictLocked drops the oldest ring transaction. A watcher resuming from a
// cursor before its end would need it, so the floor moves past it.
func (h *Hub) evictLocked() {
	evicted := h.ring[0].Cursor
	h.ring[0] = cdc.Transaction{}
	h.ring = h.ring[1:]
	h.lastEvicted = evicted
	h.hasEvicted = true
	if h.hasFloor && later(evicted, h.floor) {
		h.floor = evicted
	}
}

func (h *Hub) dropLocked(w *watcher) {
	delete(h.watchers, w)
	close(w.overflow)
}

func (h *Hub) unregister(w *watcher) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.watchers, w)
}

// appendBatch appends transaction's part for w to batches, if it has one.
func (w *watcher) appendBatch(batches []Batch, transaction cdc.Transaction) []Batch {
	if changes := w.projection.changes(w.scope, transaction); len(changes) > 0 {
		batches = append(batches, Batch{Cursor: transaction.Cursor, Changes: changes})
	}
	return batches
}

// changes returns transaction's changes to this projection within scope.
// A change is routed by its key, which always contains the scope column: a
// projection's scope column is part of its primary key.
func (p projection) changes(scope string, transaction cdc.Transaction) []cdc.Change {
	var changes []cdc.Change
	for _, change := range transaction.Changes {
		if change.Table == p.table && change.Key[p.spec.ScopeColumn] == scope {
			changes = append(changes, change)
		}
	}
	return changes
}

// later reports whether a is after b. Both come from the hub's one source.
func later(a, b cdc.Cursor) bool {
	order, err := a.Compare(b)
	return err == nil && order > 0
}
