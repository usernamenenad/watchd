package watchd

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"

	watchv1 "github.com/usernamenenad/watchd/api/watch/v1"
	"github.com/usernamenenad/watchd/internal/cdc"
	"github.com/usernamenenad/watchd/internal/server"
	"github.com/usernamenenad/watchd/internal/watch"
)

const e2eSource = "test-postgres"

// rowsSnapshotter answers every snapshot with fixed rows and a snapshot that
// covers transactions below xmax.
type rowsSnapshotter struct {
	rows []map[string]any
	xmax uint64
}

func (s *rowsSnapshotter) Snapshot(ctx context.Context, _ cdc.ProjectionSpec, _ cdc.Scope, sink cdc.SnapshotRowSink) (cdc.Snapshot, error) {
	if err := sink(ctx, s.rows); err != nil {
		return cdc.Snapshot{}, err
	}
	return cdc.NewSnapshotForTest(e2eSource, 100, s.xmax, s.xmax), nil
}

// TestSyncAgainstRealServerAndHub runs the SDK against the real server and
// hub, with only the database snapshot faked: the snapshot installs, live
// transactions apply, and a stopped source makes the client stale.
func TestSyncAgainstRealServerAndHub(t *testing.T) {
	snapshotter := &rowsSnapshotter{
		rows: []map[string]any{{"tenant_id": "t1", "user_id": "u1", "role": "editor"}},
		xmax: 10,
	}
	hub, err := watch.New(watch.Config{
		SourceID: e2eSource,
		Projections: map[string]cdc.ProjectionSpec{"permissions": {
			SourceID:    e2eSource,
			Schema:      "public",
			Table:       "permissions",
			ScopeColumn: "tenant_id",
			PrimaryKey:  []string{"tenant_id", "user_id"},
		}},
	}, snapshotter)
	if err != nil {
		t.Fatalf("watch.New: %v", err)
	}
	api, err := server.New(server.Config{SourceID: e2eSource}, hub)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	client := dialBufconn(t, func(s *grpc.Server) { api.Register(s) })

	store := NewMemoryStore("tenant_id", "user_id")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	states := make(chan State, 64)
	done := make(chan error, 1)
	go func() {
		done <- client.Sync(ctx, SyncConfig{
			Projection: "permissions",
			Scope:      "t1",
			Store:      store,
			MinBackoff: 10 * time.Millisecond,
			OnState:    func(state State) { states <- state },
		})
	}()
	waitFor := func(match func(State) bool) State {
		t.Helper()
		for {
			select {
			case state := <-states:
				if match(state) {
					return state
				}
			case <-time.After(waitTime):
				t.Fatal("timed out waiting for sync state")
			}
		}
	}

	waitFor(func(state State) bool { return state.Fresh })
	if store.Len() != 1 {
		t.Fatalf("rows after snapshot = %d, want 1", store.Len())
	}

	// A committed transaction touching two tenants: only t1's change
	// reaches this client.
	transaction := cdc.NewTransactionForTest(e2eSource, 200, 210, 20,
		cdc.Change{Operation: cdc.OperationInsert, Table: "public.permissions",
			Key:    map[string]string{"tenant_id": "t1", "user_id": "u2"},
			Values: map[string]any{"tenant_id": "t1", "user_id": "u2", "role": "viewer"}},
		cdc.Change{Operation: cdc.OperationInsert, Table: "public.permissions",
			Key:    map[string]string{"tenant_id": "t2", "user_id": "u9"},
			Values: map[string]any{"tenant_id": "t2", "user_id": "u9", "role": "viewer"}},
	)
	if err := hub.Accept(ctx, transaction); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	fresh := waitFor(func(state State) bool { return state.Fresh && state.Cursor == e2eSource+"@0/D2" })
	if store.Len() != 2 {
		t.Fatalf("rows = %d, want 2", store.Len())
	}
	if order, err := client.CompareCursors(ctx, e2eSource+"@0/64", fresh.Cursor); err != nil || order != -1 {
		t.Fatalf("CompareCursors(snapshot, progress) = %d, %v; want -1", order, err)
	}

	// The source stops: the client is told to resync and stays stale,
	// without spinning, until the source is back.
	hub.Stop()
	waitFor(func(state State) bool { return !state.Fresh })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

func TestSyncBacksOffWhenResyncsRepeatWithoutProgress(t *testing.T) {
	server, client := startScripted(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = client.Sync(ctx, SyncConfig{
			Projection: "permissions",
			Store:      NewMemoryStore("user_id"),
			MinBackoff: 100 * time.Millisecond,
			MaxBackoff: 100 * time.Millisecond,
		})
	}()

	var calls []time.Time
	for range 3 {
		call := nextCall(t, server)
		calls = append(calls, time.Now())
		call.respond(t, nil, resync(watchv1.ResyncReason_RESYNC_REASON_SOURCE_STOPPED))
	}
	// The first resync reconnects at once; the next one waits.
	if gap := calls[2].Sub(calls[1]); gap < 70*time.Millisecond {
		t.Fatalf("second resync reconnected after %s, want a backoff", gap)
	}
}
