package watch

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"

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
	// Meter is optional. The hub's instruments are created from it once, in
	// New; nil records nothing. See docs/observability.md.
	Meter metric.Meter
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
	metrics     *hubMetrics

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
	// routes indexes watchers by the table and scope their changes come
	// from, so Accept only visits the watchers a transaction concerns.
	routes map[string][]*route
	// tick is closed on the next accepted transaction, waking every watcher
	// waiting on it at once so it can report progress. tickTaken says whether
	// any watcher has it: if none does, Accept keeps it rather than replacing
	// it.
	tick      chan struct{}
	tickTaken bool
	// deliveries, deliveryKeys, and deliveryIndex are Accept's scratch
	// space, reused so a transaction that concerns no watcher allocates
	// nothing.
	deliveries    []delivery
	deliveryKeys  []deliveryKey
	deliveryIndex map[deliveryKey]int
	stopped       chan struct{}
	stopOnce      sync.Once
}

// route holds the watchers of one table whose projections share a scope
// column, by scope value. A scope's slice is replaced, never modified in
// place, so Accept can keep ranging over one while it drops a watcher.
type route struct {
	scopeColumn string
	scopes      map[string][]*watcher
}

// delivery is one scope's part of a transaction, shared read-only by every
// watcher of that scope.
type delivery struct {
	watchers []*watcher
	changes  []cdc.Change
}

type deliveryKey struct {
	route *route
	scope string
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

	h := &Hub{
		sourceID:    cfg.SourceID,
		projections: projections,
		snapshotter: snapshotter,
		maxReplay:   cfg.MaxReplayTransactions,
		queueSize:   cfg.WatcherQueue,
		watchers:    make(map[*watcher]struct{}),
		routes:      make(map[string][]*route),
		tick:        make(chan struct{}),
		stopped:     make(chan struct{}),

		deliveryIndex: make(map[deliveryKey]int),
	}
	metrics, err := newHubMetrics(cfg.Meter, h)
	if err != nil {
		return nil, fmt.Errorf("watch: create instruments: %w", err)
	}
	h.metrics = metrics
	return h, nil
}

// Accept is the source's TransactionSink. It records transaction in the
// replay window and hands each watcher its part, and never blocks on a
// watcher: one whose queue is full is dropped with Resync instead.
func (h *Hub) Accept(ctx context.Context, transaction cdc.Transaction) error {
	started := time.Now()
	h.mu.Lock()
	h.metrics.lockWait.Record(ctx, time.Since(started).Seconds())
	var tick chan struct{}
	defer func() {
		h.mu.Unlock()
		// Waking watchers is left until the lock is free, so the watchers
		// that report progress do not contend with Accept for it.
		if tick != nil {
			close(tick)
		}
		h.metrics.acceptDuration.Record(ctx, time.Since(started).Seconds())
	}()

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
	if h.tickTaken {
		tick, h.tick, h.tickTaken = h.tick, make(chan struct{}), false
	}

	h.ring = append(h.ring, transaction)
	for len(h.ring) > h.maxReplay {
		h.evictLocked()
	}

	for _, change := range transaction.Changes {
		for _, r := range h.routes[change.Table] {
			scope := change.Key[r.scopeColumn]
			watchers := r.scopes[scope]
			if len(watchers) == 0 {
				continue
			}
			d := h.deliveryLocked(deliveryKey{route: r, scope: scope}, watchers)
			d.changes = append(d.changes, change)
		}
	}
	for i := range h.deliveries {
		d := &h.deliveries[i]
		for _, w := range d.watchers {
			select {
			case w.queue <- queued{transaction: transaction, changes: d.changes}:
			default:
				h.dropLocked(w)
			}
		}
		*d = delivery{}
	}
	h.deliveries = h.deliveries[:0]
	clear(h.deliveryKeys)
	h.deliveryKeys = h.deliveryKeys[:0]
	if len(h.deliveryIndex) > 0 {
		clear(h.deliveryIndex)
	}
	return nil
}

// indexedDeliveries is how many scopes a transaction must touch before
// Accept finds their deliveries by hashing instead of scanning.
const indexedDeliveries = 8

// deliveryLocked returns key's delivery in the transaction being accepted,
// starting one for watchers if it has none yet.
func (h *Hub) deliveryLocked(key deliveryKey, watchers []*watcher) *delivery {
	// Most transactions touch few scopes, which a scan finds faster than a
	// map; the newest first, as changes to one scope tend to be adjacent.
	if len(h.deliveryKeys) <= indexedDeliveries {
		for i := len(h.deliveryKeys) - 1; i >= 0; i-- {
			if h.deliveryKeys[i] == key {
				return &h.deliveries[i]
			}
		}
	} else if i, ok := h.deliveryIndex[key]; ok {
		return &h.deliveries[i]
	}
	h.deliveries = append(h.deliveries, delivery{watchers: watchers})
	h.deliveryKeys = append(h.deliveryKeys, key)
	switch n := len(h.deliveryKeys); {
	case n == indexedDeliveries+1:
		for i, key := range h.deliveryKeys {
			h.deliveryIndex[key] = i
		}
	case n > indexedDeliveries+1:
		h.deliveryIndex[key] = n - 1
	}
	return &h.deliveries[len(h.deliveries)-1]
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
		overflow:   make(chan struct{}),
	}
	send = h.metrics.countingSend(ctx, send)
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
	h.registerLocked(w)
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
	h.registerLocked(w)
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
		position, received, tick := h.position()
		newest := position
		if !received || (baseSafe && later(base, position)) {
			position = base
		}

		for drained := false; !drained; {
			select {
			case item := <-w.queue:
				h.metrics.queueDepth.Record(ctx, int64(len(w.queue)))
				if received {
					// How far behind the hub this watcher was when it got here.
					h.metrics.watcherLag.Record(ctx, newest.BytesAfter(item.transaction.Cursor))
				}
				deliver, err := need(item.transaction)
				if err != nil {
					return err
				}
				if deliver {
					if err := send(Batch{Cursor: item.transaction.Cursor, CommitTime: item.transaction.CommitTime, Changes: item.changes}); err != nil {
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
		case <-tick:
		}
	}
}

// position reports the newest accepted transaction's cursor, and the tick
// that closes when the next one is accepted.
func (h *Hub) position() (cdc.Cursor, bool, <-chan struct{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tickTaken = true
	return h.received, h.hasReceived, h.tick
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
	h.metrics.evictions.Add(context.Background(), 1)
	if h.hasFloor && later(evicted, h.floor) {
		h.floor = evicted
	}
}

// registerLocked adds w to the watchers Accept delivers to.
func (h *Hub) registerLocked(w *watcher) {
	h.watchers[w] = struct{}{}
	r := h.routeLocked(w.projection)
	// Clip, so the append copies instead of writing into a backing array
	// Accept may be ranging over.
	r.scopes[w.scope] = append(slices.Clip(r.scopes[w.scope]), w)
}

// routeLocked returns the route of p's table and scope column, creating it
// if no watcher has needed it yet.
func (h *Hub) routeLocked(p projection) *route {
	for _, r := range h.routes[p.table] {
		if r.scopeColumn == p.spec.ScopeColumn {
			return r
		}
	}
	r := &route{scopeColumn: p.spec.ScopeColumn, scopes: make(map[string][]*watcher)}
	h.routes[p.table] = append(h.routes[p.table], r)
	return r
}

// removeLocked removes w from the watchers Accept delivers to, and reports
// whether it was still registered.
func (h *Hub) removeLocked(w *watcher) bool {
	if _, ok := h.watchers[w]; !ok {
		return false
	}
	delete(h.watchers, w)
	r := h.routeLocked(w.projection)
	remaining := slices.DeleteFunc(slices.Clone(r.scopes[w.scope]), func(other *watcher) bool { return other == w })
	if len(remaining) == 0 {
		delete(r.scopes, w.scope)
	} else {
		r.scopes[w.scope] = remaining
	}
	return true
}

func (h *Hub) dropLocked(w *watcher) {
	if h.removeLocked(w) {
		close(w.overflow)
	}
}

func (h *Hub) unregister(w *watcher) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.removeLocked(w)
}

// appendBatch appends transaction's part for w to batches, if it has one.
func (w *watcher) appendBatch(batches []Batch, transaction cdc.Transaction) []Batch {
	if changes := w.projection.changes(w.scope, transaction); len(changes) > 0 {
		batches = append(batches, Batch{Cursor: transaction.Cursor, CommitTime: transaction.CommitTime, Changes: changes})
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
