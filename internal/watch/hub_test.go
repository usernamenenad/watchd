package watch

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/usernamenenad/watchd/internal/cdc"
)

const (
	testSource  = "test-postgres"
	testTable   = "public.permissions"
	tenantA     = "tenant-a"
	tenantB     = "tenant-b"
	eventWait   = 2 * time.Second
	silenceWait = 50 * time.Millisecond
)

func testSpec() cdc.ProjectionSpec {
	return cdc.ProjectionSpec{
		SourceID:    testSource,
		Schema:      "public",
		Table:       "permissions",
		ScopeColumn: "tenant_id",
		PrimaryKey:  []string{"tenant_id", "user_id"},
	}
}

// fakeSnapshotter returns a fixed snapshot and rows. duringRead, when set,
// runs while the rows are being read - where a concurrent commit would land.
type fakeSnapshotter struct {
	snapshot   cdc.Snapshot
	rows       []map[string]any
	err        error
	duringRead func()
	calls      int
}

func (f *fakeSnapshotter) Snapshot(ctx context.Context, _ cdc.ProjectionSpec, _ cdc.Scope, sink cdc.SnapshotRowSink) (cdc.Snapshot, error) {
	f.calls++
	if f.duringRead != nil {
		f.duringRead()
	}
	if f.err != nil {
		return cdc.Snapshot{}, f.err
	}
	if err := sink(ctx, f.rows); err != nil {
		return cdc.Snapshot{}, err
	}
	return f.snapshot, nil
}

func newTestHub(t *testing.T, snapshotter Snapshotter, configure ...func(*Config)) *Hub {
	t.Helper()
	cfg := Config{
		SourceID:    testSource,
		Projections: map[string]cdc.ProjectionSpec{"permissions": testSpec()},
	}
	for _, apply := range configure {
		apply(&cfg)
	}
	hub, err := New(cfg, snapshotter)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return hub
}

// change is a row change to the test projection for tenant and user.
func change(tenant, user string) cdc.Change {
	return cdc.Change{
		Operation: cdc.OperationInsert,
		Table:     testTable,
		Key:       map[string]string{"tenant_id": tenant, "user_id": user},
		Values:    map[string]any{"tenant_id": tenant, "user_id": user},
	}
}

// transaction commits at lsn, ends at lsn+10, and has source ID xid.
func transaction(lsn uint64, xid uint32, changes ...cdc.Change) cdc.Transaction {
	return cdc.NewTransactionForTest(testSource, lsn, lsn+10, xid, changes...)
}

func cursor(lsn uint64) cdc.Cursor {
	return cdc.NewCursorForTest(testSource, lsn)
}

// watchStream runs one Watch call and exposes its events.
type watchStream struct {
	events chan Event
	done   chan error
	cancel context.CancelFunc
}

func startWatch(t *testing.T, hub *Hub, req Request) *watchStream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stream := &watchStream{events: make(chan Event, 64), done: make(chan error, 1), cancel: cancel}
	go func() {
		stream.done <- hub.Watch(ctx, req, func(event Event) error {
			select {
			case stream.events <- event:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	t.Cleanup(cancel)
	return stream
}

func (s *watchStream) next(t *testing.T) Event {
	t.Helper()
	select {
	case event := <-s.events:
		return event
	case err := <-s.done:
		// A watch can send its last event and return at once; its events
		// were sent before it returned, so they come first.
		select {
		case event := <-s.events:
			s.done <- err
			return event
		default:
		}
		t.Fatalf("watch ended with %v while an event was expected", err)
	case <-time.After(eventWait):
		t.Fatal("timed out waiting for a watch event")
	}
	return nil
}

func (s *watchStream) expect(t *testing.T, want ...Event) {
	t.Helper()
	for index, expected := range want {
		if got := s.next(t); !reflect.DeepEqual(got, expected) {
			t.Fatalf("event %d = %#v, want %#v", index, got, expected)
		}
	}
}

func (s *watchStream) expectSilence(t *testing.T) {
	t.Helper()
	select {
	case event := <-s.events:
		t.Fatalf("unexpected event %#v", event)
	case <-time.After(silenceWait):
	}
}

func (s *watchStream) expectEnd(t *testing.T) error {
	t.Helper()
	select {
	case err := <-s.done:
		return err
	case <-time.After(eventWait):
		t.Fatal("watch did not end")
	}
	return nil
}

func accept(t *testing.T, hub *Hub, transactions ...cdc.Transaction) {
	t.Helper()
	for _, transaction := range transactions {
		if err := hub.Accept(context.Background(), transaction); err != nil {
			t.Fatalf("Accept: %v", err)
		}
	}
}

func batch(transaction cdc.Transaction, changes ...cdc.Change) Batch {
	return Batch{Cursor: transaction.Cursor, Changes: changes}
}

// TestSnapshotThenLiveDeliversExactlyWhatTheSnapshotLacks walks the snapshot
// path through every case the boundary decides:
//   - streamed before the watcher registered and visible to the snapshot:
//     already in the rows, not delivered;
//   - streamed before the watcher registered but still invisible to the
//     snapshot (a commit racing it): delivered;
//   - streamed while the rows were read, visible or not: only the invisible
//     one is delivered;
//   - streamed after the snapshot: delivered live.
func TestSnapshotThenLiveDeliversExactlyWhatTheSnapshotLacks(t *testing.T) {
	// Transactions below 20 are in the snapshot, except 15, which was still
	// running when it was taken.
	snapshotter := &fakeSnapshotter{
		snapshot: cdc.NewSnapshotForTest(testSource, 300, 10, 20, 15),
		rows:     []map[string]any{{"tenant_id": tenantA, "user_id": "u1"}},
	}
	hub := newTestHub(t, snapshotter)

	visibleBefore := transaction(100, 12, change(tenantA, "visible-before"))
	racingBefore := transaction(110, 15, change(tenantA, "racing-before"))
	accept(t, hub, visibleBefore, racingBefore)

	visibleDuring := transaction(200, 18, change(tenantA, "visible-during"))
	invisibleDuring := transaction(210, 21, change(tenantA, "invisible-during"))
	snapshotter.duringRead = func() { accept(t, hub, visibleDuring, invisibleDuring) }

	stream := startWatch(t, hub, Request{Projection: "permissions", Scope: tenantA})
	stream.expect(t,
		SnapshotBegin{},
		SnapshotRows{Rows: snapshotter.rows},
		SnapshotEnd{},
		batch(racingBefore, change(tenantA, "racing-before")),
		batch(invisibleDuring, change(tenantA, "invisible-during")),
		Progress{Cursor: invisibleDuring.Cursor},
	)

	after := transaction(400, 30, change(tenantA, "after"))
	accept(t, hub, after)
	stream.expect(t,
		batch(after, change(tenantA, "after")),
		Progress{Cursor: after.Cursor},
	)
}

func TestWatcherReceivesOnlyItsScopeAndProjection(t *testing.T) {
	hub := newTestHub(t, &fakeSnapshotter{snapshot: cdc.NewSnapshotForTest(testSource, 50, 1, 1)})
	stream := startWatch(t, hub, Request{Projection: "permissions", Scope: tenantA})
	stream.expect(t, SnapshotBegin{}, SnapshotRows{}, SnapshotEnd{}, Progress{Cursor: cursor(50)})

	otherTable := change(tenantA, "u2")
	otherTable.Table = "public.other"
	mixed := transaction(100, 5, change(tenantB, "u1"), change(tenantA, "u1"), otherTable)
	unrelated := transaction(200, 6, change(tenantB, "u3"))
	accept(t, hub, mixed, unrelated)

	// Only tenant A's change to this projection, in one batch; progress
	// still moves past the unrelated transaction.
	stream.expect(t, batch(mixed, change(tenantA, "u1")))
	if progress := stream.next(t).(Progress); progress.Cursor != unrelated.Cursor {
		// The two transactions may be reported in one or two progress steps.
		stream.expect(t, Progress{Cursor: unrelated.Cursor})
	}
}

// TestProgressAfterSnapshotNeverClaimsAheadOfTheStream checks the progress
// point reported right after a snapshot: the snapshot cursor when nothing
// has been streamed yet, but the stream's own position once it has - even
// when that is behind the snapshot cursor, because a commit just before the
// cursor might not have been streamed yet.
func TestProgressAfterSnapshotNeverClaimsAheadOfTheStream(t *testing.T) {
	t.Run("nothing streamed", func(t *testing.T) {
		hub := newTestHub(t, &fakeSnapshotter{snapshot: cdc.NewSnapshotForTest(testSource, 500, 1, 1)})
		startWatch(t, hub, Request{Projection: "permissions", Scope: tenantA}).
			expect(t, SnapshotBegin{}, SnapshotRows{}, SnapshotEnd{}, Progress{Cursor: cursor(500)})
	})
	t.Run("stream behind the snapshot", func(t *testing.T) {
		hub := newTestHub(t, &fakeSnapshotter{snapshot: cdc.NewSnapshotForTest(testSource, 500, 10, 10)})
		streamed := transaction(100, 5, change(tenantB, "u1"))
		accept(t, hub, streamed)
		startWatch(t, hub, Request{Projection: "permissions", Scope: tenantA}).
			expect(t, SnapshotBegin{}, SnapshotRows{}, SnapshotEnd{}, Progress{Cursor: streamed.Cursor})
	})
}

func TestResumeReplaysFromCursorWithoutSnapshot(t *testing.T) {
	snapshotter := &fakeSnapshotter{snapshot: cdc.NewSnapshotForTest(testSource, 50, 1, 1)}
	hub := newTestHub(t, snapshotter)
	first := startWatch(t, hub, Request{Projection: "permissions", Scope: tenantA})
	first.expect(t, SnapshotBegin{}, SnapshotRows{}, SnapshotEnd{}, Progress{Cursor: cursor(50)})

	t1 := transaction(100, 5, change(tenantA, "u1"))
	t2 := transaction(200, 6, change(tenantA, "u2"))
	t3 := transaction(300, 7, change(tenantA, "u3"))
	accept(t, hub, t1, t2, t3)

	// A watcher that applied t1 resumes after it: t2 and t3 are replayed
	// from the hub's window, and no second snapshot is taken.
	resumed := startWatch(t, hub, Request{Projection: "permissions", Scope: tenantA, ResumeCursor: t1.Cursor.String()})
	resumed.expect(t,
		batch(t2, change(tenantA, "u2")),
		batch(t3, change(tenantA, "u3")),
		Progress{Cursor: t3.Cursor},
	)
	if snapshotter.calls != 1 {
		t.Fatalf("snapshots = %d, want 1", snapshotter.calls)
	}

	t4 := transaction(400, 8, change(tenantA, "u4"))
	accept(t, hub, t4)
	resumed.expect(t, batch(t4, change(tenantA, "u4")), Progress{Cursor: t4.Cursor})
}

func TestResumeResyncsWhenCursorIsNotReplayable(t *testing.T) {
	t.Run("before any snapshot", func(t *testing.T) {
		// A fresh hub - for example after a restart - cannot vouch for any
		// cursor, even one it has streamed past.
		hub := newTestHub(t, &fakeSnapshotter{})
		accept(t, hub, transaction(100, 5, change(tenantA, "u1")))
		stream := startWatch(t, hub, Request{Projection: "permissions", Scope: tenantA, ResumeCursor: cursor(100).String()})
		stream.expect(t, Resync{Reason: ResyncCursorUnavailable})
		if err := stream.expectEnd(t); err != nil {
			t.Fatalf("watch error = %v, want nil after Resync", err)
		}
	})
	t.Run("evicted from the window", func(t *testing.T) {
		hub := newTestHub(t, &fakeSnapshotter{snapshot: cdc.NewSnapshotForTest(testSource, 50, 1, 1)}, func(cfg *Config) {
			cfg.MaxReplayTransactions = 2
		})
		startWatch(t, hub, Request{Projection: "permissions", Scope: tenantA}).
			expect(t, SnapshotBegin{}, SnapshotRows{}, SnapshotEnd{}, Progress{Cursor: cursor(50)})
		t1 := transaction(100, 5, change(tenantA, "u1"))
		accept(t, hub, t1, transaction(200, 6), transaction(300, 7))

		// t1 is still needed by a watcher resuming from before it, but the
		// window no longer holds it.
		startWatch(t, hub, Request{Projection: "permissions", Scope: tenantA, ResumeCursor: cursor(50).String()}).
			expect(t, Resync{Reason: ResyncCursorUnavailable})
		// Resuming after it still works.
		startWatch(t, hub, Request{Projection: "permissions", Scope: tenantA, ResumeCursor: t1.Cursor.String()}).
			expect(t, Progress{Cursor: cursor(310)})
	})
}

func TestSlowWatcherIsDroppedWithoutBlockingTheSource(t *testing.T) {
	hub := newTestHub(t, &fakeSnapshotter{snapshot: cdc.NewSnapshotForTest(testSource, 50, 1, 1)}, func(cfg *Config) {
		cfg.WatcherQueue = 2
	})

	// This watcher's client stops reading after the snapshot.
	blocked := make(chan struct{})
	events := make(chan Event, 8)
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		done <- hub.Watch(ctx, Request{Projection: "permissions", Scope: tenantA}, func(event Event) error {
			events <- event
			if _, ok := event.(Progress); ok {
				<-blocked
			}
			return nil
		})
	}()
	for {
		if _, ok := (<-events).(Progress); ok {
			break
		}
	}

	accepted := make(chan struct{})
	go func() {
		for lsn := uint64(100); lsn < 200; lsn += 20 {
			accept(t, hub, transaction(lsn, uint32(lsn), change(tenantA, fmt.Sprint(lsn))))
		}
		close(accepted)
	}()
	select {
	case <-accepted:
	case <-time.After(eventWait):
		t.Fatal("Accept blocked on a slow watcher")
	}

	close(blocked)
	for {
		event := <-events
		if resync, ok := event.(Resync); ok {
			if resync.Reason != ResyncSlowWatcher {
				t.Fatalf("resync reason = %s, want %s", resync.Reason, ResyncSlowWatcher)
			}
			break
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("watch error = %v, want nil after Resync", err)
	}
}

func TestStopResyncsLiveAndNewWatchers(t *testing.T) {
	hub := newTestHub(t, &fakeSnapshotter{snapshot: cdc.NewSnapshotForTest(testSource, 50, 1, 1)})
	live := startWatch(t, hub, Request{Projection: "permissions", Scope: tenantA})
	live.expect(t, SnapshotBegin{}, SnapshotRows{}, SnapshotEnd{}, Progress{Cursor: cursor(50)})

	hub.Stop()
	live.expect(t, Resync{Reason: ResyncSourceStopped})
	startWatch(t, hub, Request{Projection: "permissions", Scope: tenantB}).
		expect(t, Resync{Reason: ResyncSourceStopped})
}

func TestSnapshotFailuresBecomeResyncOrErrors(t *testing.T) {
	for _, test := range []struct {
		err    error
		resync ResyncReason
	}{
		{err: cdc.ErrSnapshotWindowClosed, resync: ResyncSnapshotWindowClosed},
		{err: cdc.ErrSourceStopped, resync: ResyncSourceStopped},
	} {
		hub := newTestHub(t, &fakeSnapshotter{err: test.err})
		stream := startWatch(t, hub, Request{Projection: "permissions", Scope: tenantA})
		stream.expect(t, SnapshotBegin{}, Resync{Reason: test.resync})
	}

	hub := newTestHub(t, &fakeSnapshotter{err: cdc.ErrSourceUnavailable})
	stream := startWatch(t, hub, Request{Projection: "permissions", Scope: tenantA})
	stream.expect(t, SnapshotBegin{})
	if err := stream.expectEnd(t); !errors.Is(err, cdc.ErrSourceUnavailable) {
		t.Fatalf("watch error = %v, want %v", err, cdc.ErrSourceUnavailable)
	}
}

func TestAcceptIgnoresRedeliveredTransactions(t *testing.T) {
	hub := newTestHub(t, &fakeSnapshotter{snapshot: cdc.NewSnapshotForTest(testSource, 50, 1, 1)})
	stream := startWatch(t, hub, Request{Projection: "permissions", Scope: tenantA})
	stream.expect(t, SnapshotBegin{}, SnapshotRows{}, SnapshotEnd{}, Progress{Cursor: cursor(50)})

	t1 := transaction(100, 5, change(tenantA, "u1"))
	accept(t, hub, t1)
	stream.expect(t, batch(t1, change(tenantA, "u1")), Progress{Cursor: t1.Cursor})

	// After a reconnect, PostgreSQL resends t1 before anything new.
	accept(t, hub, t1)
	stream.expectSilence(t)
}

func TestWatchRejectsInvalidRequests(t *testing.T) {
	hub := newTestHub(t, &fakeSnapshotter{})
	if err := hub.Watch(context.Background(), Request{Projection: "missing"}, func(Event) error { return nil }); !errors.Is(err, ErrUnknownProjection) {
		t.Fatalf("unknown projection error = %v, want %v", err, ErrUnknownProjection)
	}
	if err := hub.Watch(context.Background(), Request{Projection: "permissions", ResumeCursor: "not-a-cursor"}, func(Event) error { return nil }); !errors.Is(err, cdc.ErrInvalidCursor) {
		t.Fatalf("invalid cursor error = %v, want %v", err, cdc.ErrInvalidCursor)
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	foreign := testSpec()
	foreign.SourceID = "other"
	for name, cfg := range map[string]Config{
		"no source":       {Projections: map[string]cdc.ProjectionSpec{"p": testSpec()}},
		"no projections":  {SourceID: testSource},
		"foreign source":  {SourceID: testSource, Projections: map[string]cdc.ProjectionSpec{"p": foreign}},
		"unnamed":         {SourceID: testSource, Projections: map[string]cdc.ProjectionSpec{"": testSpec()}},
		"negative replay": {SourceID: testSource, Projections: map[string]cdc.ProjectionSpec{"p": testSpec()}, MaxReplayTransactions: -1},
		"negative queue":  {SourceID: testSource, Projections: map[string]cdc.ProjectionSpec{"p": testSpec()}, WatcherQueue: -1},
	} {
		if _, err := New(cfg, &fakeSnapshotter{}); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: error = %v, want %v", name, err, ErrInvalidConfig)
		}
	}
}
