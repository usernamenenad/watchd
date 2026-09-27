package watchd

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"

	watchv1 "github.com/usernamenenad/watchd/api/watch/v1"
)

const waitTime = 2 * time.Second

// scriptedServer hands each Watch call to the test: the test reads the
// request from calls and answers with a script run on that stream.
type scriptedServer struct {
	watchv1.UnimplementedWatchServiceServer
	calls chan *scriptedCall
}

type scriptedCall struct {
	request *watchv1.WatchRequest
	script  chan func(grpc.ServerStreamingServer[watchv1.WatchResponse]) error
}

func (s *scriptedServer) Watch(req *watchv1.WatchRequest, stream grpc.ServerStreamingServer[watchv1.WatchResponse]) error {
	call := &scriptedCall{request: req, script: make(chan func(grpc.ServerStreamingServer[watchv1.WatchResponse]) error)}
	select {
	case s.calls <- call:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	select {
	case script := <-call.script:
		return script(stream)
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}

func (s *scriptedServer) CompareCursors(_ context.Context, req *watchv1.CompareCursorsRequest) (*watchv1.CompareCursorsResponse, error) {
	if req.GetLeft() == req.GetRight() {
		return &watchv1.CompareCursorsResponse{Order: watchv1.CursorOrder_CURSOR_ORDER_EQUAL}, nil
	}
	return &watchv1.CompareCursorsResponse{Order: watchv1.CursorOrder_CURSOR_ORDER_BEFORE}, nil
}

// dialBufconn serves registerServices on an in-memory listener and returns a
// Client connected to it.
func dialBufconn(t *testing.T, register func(*grpc.Server)) *Client {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	client, err := Dial("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func startScripted(t *testing.T) (*scriptedServer, *Client) {
	t.Helper()
	server := &scriptedServer{calls: make(chan *scriptedCall)}
	client := dialBufconn(t, func(s *grpc.Server) { watchv1.RegisterWatchServiceServer(s, server) })
	return server, client
}

// syncRun runs Sync in the background and records every state it reports.
type syncRun struct {
	mu     sync.Mutex
	states []State
	done   chan error
	cancel context.CancelFunc
}

func startSync(t *testing.T, client *Client, store Store, configure ...func(*SyncConfig)) *syncRun {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	run := &syncRun{done: make(chan error, 1), cancel: cancel}
	cfg := SyncConfig{
		Projection: "permissions",
		Scope:      "tenant-a",
		Store:      store,
		MinBackoff: time.Millisecond,
		MaxBackoff: 5 * time.Millisecond,
		OnState: func(state State) {
			run.mu.Lock()
			defer run.mu.Unlock()
			run.states = append(run.states, state)
		},
	}
	for _, apply := range configure {
		apply(&cfg)
	}
	go func() { run.done <- client.Sync(ctx, cfg) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-run.done:
		case <-time.After(waitTime):
			t.Error("Sync did not return after cancel")
		}
	})
	return run
}

func (r *syncRun) waitForState(t *testing.T, want State) {
	t.Helper()
	deadline := time.Now().Add(waitTime)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		for _, state := range r.states {
			if state == want {
				r.mu.Unlock()
				return
			}
		}
		r.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	t.Fatalf("state %+v never reported; got %+v", want, r.states)
}

func (r *syncRun) lastState() State {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.states) == 0 {
		return State{}
	}
	return r.states[len(r.states)-1]
}

func nextCall(t *testing.T, server *scriptedServer) *scriptedCall {
	t.Helper()
	select {
	case call := <-server.calls:
		return call
	case <-time.After(waitTime):
		t.Fatal("client never called Watch")
	}
	return nil
}

// respond sends responses on the call's stream, then ends it with err (nil
// keeps the stream open until the client goes away).
func (c *scriptedCall) respond(t *testing.T, err error, responses ...*watchv1.WatchResponse) {
	t.Helper()
	c.script <- func(stream grpc.ServerStreamingServer[watchv1.WatchResponse]) error {
		for _, response := range responses {
			if sendErr := stream.Send(response); sendErr != nil {
				return sendErr
			}
		}
		if err != nil {
			return err
		}
		<-stream.Context().Done()
		return nil
	}
}

func textValue(text string) *watchv1.Value {
	return &watchv1.Value{Kind: &watchv1.Value_Text{Text: text}}
}

func row(user, role string) *watchv1.Row {
	return &watchv1.Row{Values: map[string]*watchv1.Value{"user_id": textValue(user), "role": textValue(role)}}
}

var (
	snapshotBegin = &watchv1.WatchResponse{Event: &watchv1.WatchResponse_SnapshotBegin{SnapshotBegin: &watchv1.SnapshotBegin{}}}
	snapshotEnd   = &watchv1.WatchResponse{Event: &watchv1.WatchResponse_SnapshotEnd{SnapshotEnd: &watchv1.SnapshotEnd{}}}
)

func snapshotRows(rows ...*watchv1.Row) *watchv1.WatchResponse {
	return &watchv1.WatchResponse{Event: &watchv1.WatchResponse_SnapshotRows{SnapshotRows: &watchv1.SnapshotRows{Rows: rows}}}
}

func progress(cursor string) *watchv1.WatchResponse {
	return &watchv1.WatchResponse{Event: &watchv1.WatchResponse_Progress{Progress: &watchv1.Progress{Cursor: cursor}}}
}

func batch(cursor string, changes ...*watchv1.Change) *watchv1.WatchResponse {
	return &watchv1.WatchResponse{Event: &watchv1.WatchResponse_Batch{Batch: &watchv1.Batch{Cursor: cursor, Changes: changes}}}
}

func resync(reason watchv1.ResyncReason) *watchv1.WatchResponse {
	return &watchv1.WatchResponse{Event: &watchv1.WatchResponse_Resync{Resync: &watchv1.Resync{Reason: reason}}}
}

func upsert(user string, values map[string]*watchv1.Value) *watchv1.Change {
	values["user_id"] = textValue(user)
	return &watchv1.Change{Operation: watchv1.Operation_OPERATION_UPDATE, Table: "public.permissions", Key: map[string]string{"user_id": user}, Values: values}
}

func remove(user string) *watchv1.Change {
	return &watchv1.Change{Operation: watchv1.Operation_OPERATION_DELETE, Table: "public.permissions", Key: map[string]string{"user_id": user}}
}

func roleOf(t *testing.T, store *MemoryStore, user string) (Value, bool) {
	t.Helper()
	row, ok := store.Get(map[string]string{"user_id": user})
	if !ok {
		return Value{}, false
	}
	return row["role"], true
}

func TestSyncInstallsSnapshotThenAppliesBatches(t *testing.T) {
	server, client := startScripted(t)
	store := NewMemoryStore("user_id")
	run := startSync(t, client, store)

	call := nextCall(t, server)
	if call.request.GetResumeCursor() != "" || call.request.GetProjection() != "permissions" || call.request.GetScope() != "tenant-a" {
		t.Fatalf("first request = %v, want permissions/tenant-a without a cursor", call.request)
	}
	unchanged := &watchv1.Value{Kind: &watchv1.Value_UnchangedToast{UnchangedToast: &emptypb.Empty{}}}
	null := &watchv1.Value{Kind: &watchv1.Value_Null{Null: structpb.NullValue_NULL_VALUE}}
	call.respond(t, nil,
		snapshotBegin, snapshotRows(row("u1", "editor"), row("u2", "viewer")), snapshotEnd,
		progress("src@1"),
		batch("src@2",
			upsert("u1", map[string]*watchv1.Value{"role": unchanged, "note": null}),
			remove("u2"),
			upsert("u3", map[string]*watchv1.Value{"role": textValue("owner")})),
		progress("src@2"),
	)

	run.waitForState(t, State{Fresh: true, Cursor: "src@1"})
	run.waitForState(t, State{Fresh: true, Cursor: "src@2"})

	if role, ok := roleOf(t, store, "u1"); !ok || role != (Value{Text: "editor"}) {
		t.Fatalf("u1 role = %+v, %t; want the unchanged value kept", role, ok)
	}
	if row, _ := store.Get(map[string]string{"user_id": "u1"}); row["note"] != (Value{Null: true}) {
		t.Fatalf("u1 note = %+v, want NULL", row["note"])
	}
	if _, ok := roleOf(t, store, "u2"); ok {
		t.Fatal("u2 was not deleted")
	}
	if role, _ := roleOf(t, store, "u3"); role != (Value{Text: "owner"}) {
		t.Fatalf("u3 role = %+v, want owner", role)
	}
	if cursor, _ := store.Cursor(context.Background()); cursor != "src@2" {
		t.Fatalf("stored cursor = %q, want src@2", cursor)
	}
}

func TestSyncResumesFromStoredCursorAfterDisconnect(t *testing.T) {
	server, client := startScripted(t)
	store := NewMemoryStore("user_id")
	run := startSync(t, client, store)

	nextCall(t, server).respond(t, status.Error(codes.Unavailable, "server going away"),
		snapshotBegin, snapshotRows(row("u1", "editor")), snapshotEnd, progress("src@1"))
	run.waitForState(t, State{Fresh: true, Cursor: "src@1"})

	// The connection drops: the projection is stale until the server
	// confirms it again, and the client resumes from its stored cursor.
	resumed := nextCall(t, server)
	if got := resumed.request.GetResumeCursor(); got != "src@1" {
		t.Fatalf("resume cursor = %q, want src@1", got)
	}
	run.waitForState(t, State{Fresh: false, Cursor: "src@1"})

	resumed.respond(t, nil, batch("src@2", upsert("u2", map[string]*watchv1.Value{"role": textValue("viewer")})), progress("src@2"))
	run.waitForState(t, State{Fresh: true, Cursor: "src@2"})
	if store.Len() != 2 {
		t.Fatalf("rows = %d, want 2", store.Len())
	}
}

func TestSyncRebuildsAtomicallyOnResync(t *testing.T) {
	server, client := startScripted(t)
	// A projection from an earlier run, with its resume cursor.
	store := NewMemoryStore("user_id")
	if err := store.Apply(context.Background(), Batch{Cursor: "src@1", Changes: []Change{
		{Operation: Insert, Key: map[string]string{"user_id": "old"}, Values: Row{"user_id": {Text: "old"}, "role": {Text: "editor"}}},
	}}); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	run := startSync(t, client, store)

	// The server cannot continue from that cursor (for example, it
	// restarted) and asks for a resync.
	first := nextCall(t, server)
	if first.request.GetResumeCursor() != "src@1" {
		t.Fatalf("resume cursor = %q, want src@1", first.request.GetResumeCursor())
	}
	first.respond(t, nil, resync(watchv1.ResyncReason_RESYNC_REASON_CURSOR_UNAVAILABLE))

	// The client watches again without a cursor, receiving a new snapshot.
	rebuild := nextCall(t, server)
	if rebuild.request.GetResumeCursor() != "" {
		t.Fatalf("resume cursor after resync = %q, want none", rebuild.request.GetResumeCursor())
	}
	hold := make(chan struct{})
	rebuild.script <- func(stream grpc.ServerStreamingServer[watchv1.WatchResponse]) error {
		for _, response := range []*watchv1.WatchResponse{snapshotBegin, snapshotRows(row("new", "viewer"))} {
			if err := stream.Send(response); err != nil {
				return err
			}
		}
		<-hold
		for _, response := range []*watchv1.WatchResponse{snapshotEnd, progress("src@9")} {
			if err := stream.Send(response); err != nil {
				return err
			}
		}
		<-stream.Context().Done()
		return nil
	}

	// While the new snapshot streams in, the projection is stale but
	// readers still see the old one, whole.
	run.waitForState(t, State{Fresh: false, Cursor: ""})
	if _, ok := roleOf(t, store, "old"); !ok || store.Len() != 1 {
		t.Fatalf("during the rebuild rows = %v, want the old projection whole", store.Rows())
	}

	close(hold)
	run.waitForState(t, State{Fresh: true, Cursor: "src@9"})
	if _, ok := roleOf(t, store, "new"); !ok || store.Len() != 1 {
		t.Fatalf("after the rebuild rows = %v, want only the new row", store.Rows())
	}
}

func TestSyncStartsOverWhenServerRefusesTheCursor(t *testing.T) {
	server, client := startScripted(t)
	store := NewMemoryStore("user_id")
	_ = store.SaveCursor(context.Background(), "other-source@1")
	startSync(t, client, store)

	nextCall(t, server).respond(t, status.Error(codes.InvalidArgument, "cursor belongs to another source"))
	if got := nextCall(t, server).request.GetResumeCursor(); got != "" {
		t.Fatalf("resume cursor after refusal = %q, want none", got)
	}
}

func TestSyncReturnsPermanentErrors(t *testing.T) {
	server, client := startScripted(t)
	run := startSync(t, client, NewMemoryStore("user_id"))
	nextCall(t, server).respond(t, status.Error(codes.NotFound, "unknown projection"))
	select {
	case err := <-run.done:
		if status.Code(err) != codes.NotFound {
			t.Fatalf("Sync error = %v, want NotFound", err)
		}
		run.done <- nil // let cleanup observe the return
	case <-time.After(waitTime):
		t.Fatal("Sync kept retrying a permanent error")
	}
}

// failingStore fails to apply batches, and records whether a snapshot was
// aborted.
type failingStore struct {
	*MemoryStore
	aborted chan struct{}
}

func (s *failingStore) Apply(context.Context, Batch) error { return errors.New("disk full") }

func (s *failingStore) BeginSnapshot(ctx context.Context) (SnapshotWriter, error) {
	writer, err := s.MemoryStore.BeginSnapshot(ctx)
	return &abortRecorder{SnapshotWriter: writer, aborted: s.aborted}, err
}

type abortRecorder struct {
	SnapshotWriter
	aborted chan struct{}
}

func (w *abortRecorder) Abort(ctx context.Context) error {
	close(w.aborted)
	return w.SnapshotWriter.Abort(ctx)
}

func TestSyncStopsOnStoreFailure(t *testing.T) {
	server, client := startScripted(t)
	store := &failingStore{MemoryStore: NewMemoryStore("user_id"), aborted: make(chan struct{})}
	run := startSync(t, client, store)
	nextCall(t, server).respond(t, nil, snapshotBegin, snapshotRows(row("u1", "editor")), snapshotEnd, progress("src@1"),
		batch("src@2", remove("u1")))
	select {
	case err := <-run.done:
		if !errors.Is(err, errStore) {
			t.Fatalf("Sync error = %v, want a store error", err)
		}
		run.done <- nil
	case <-time.After(waitTime):
		t.Fatal("Sync did not stop on a store failure")
	}
	if last := run.lastState(); last.Fresh {
		t.Fatalf("state after failure = %+v, want stale", last)
	}
}

func TestSyncAbortsSnapshotInterruptedByDisconnect(t *testing.T) {
	server, client := startScripted(t)
	store := &failingStore{MemoryStore: NewMemoryStore("user_id"), aborted: make(chan struct{})}
	startSync(t, client, store)
	nextCall(t, server).respond(t, status.Error(codes.Unavailable, "gone"), snapshotBegin, snapshotRows(row("u1", "editor")))
	select {
	case <-store.aborted:
	case <-time.After(waitTime):
		t.Fatal("interrupted snapshot was not aborted")
	}
	if store.Len() != 0 {
		t.Fatalf("rows = %d, want the partial snapshot discarded", store.Len())
	}
}

func TestCompareCursors(t *testing.T) {
	_, client := startScripted(t)
	if order, err := client.CompareCursors(context.Background(), "a", "a"); err != nil || order != 0 {
		t.Fatalf("CompareCursors(a, a) = %d, %v; want 0", order, err)
	}
	if order, err := client.CompareCursors(context.Background(), "a", "b"); err != nil || order != -1 {
		t.Fatalf("CompareCursors(a, b) = %d, %v; want -1", order, err)
	}
}

func TestMemoryStoreApplyIsIdempotent(t *testing.T) {
	store := NewMemoryStore("tenant_id", "user_id")
	key := map[string]string{"tenant_id": "t", "user_id": "u"}
	batch := Batch{Cursor: "c1", Changes: []Change{
		{Operation: Insert, Key: key, Values: Row{"tenant_id": {Text: "t"}, "user_id": {Text: "u"}, "role": {Text: "editor"}}},
	}}
	for range 2 {
		if err := store.Apply(context.Background(), batch); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	if row, ok := store.Get(key); !ok || store.Len() != 1 || !reflect.DeepEqual(row["role"], Value{Text: "editor"}) {
		t.Fatalf("rows = %v, want one editor row", store.Rows())
	}
	if err := store.Apply(context.Background(), Batch{Changes: []Change{{Operation: Delete, Key: map[string]string{"user_id": "u"}}}}); !errors.Is(err, ErrMissingKey) {
		t.Fatalf("partial key error = %v, want %v", err, ErrMissingKey)
	}
}
