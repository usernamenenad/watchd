package server

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"

	watchv1 "github.com/usernamenenad/watchd/api/watch/v1"
	"github.com/usernamenenad/watchd/internal/cdc"
	"github.com/usernamenenad/watchd/internal/watch"
)

const testSource = "test-postgres"

// scriptedWatcher sends a fixed list of events, then returns err.
type scriptedWatcher struct {
	events []watch.Event
	err    error
	got    watch.Request
}

func (w *scriptedWatcher) Watch(_ context.Context, req watch.Request, send func(watch.Event) error) error {
	w.got = req
	for _, event := range w.events {
		if err := send(event); err != nil {
			return err
		}
	}
	return w.err
}

func startServer(t *testing.T, watcher Watcher, configure ...func(*Config)) (*Server, watchv1.WatchServiceClient, *grpc.ClientConn) {
	t.Helper()
	cfg := Config{SourceID: testSource}
	for _, apply := range configure {
		apply(&cfg)
	}
	server, err := New(cfg, watcher)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	server.Register(grpcServer)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return server, watchv1.NewWatchServiceClient(conn), conn
}

// receiveAll runs Watch and returns every response and the final error.
func receiveAll(t *testing.T, client watchv1.WatchServiceClient, req *watchv1.WatchRequest) ([]*watchv1.WatchResponse, error) {
	t.Helper()
	stream, err := client.Watch(context.Background(), req)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	var responses []*watchv1.WatchResponse
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return responses, nil
		}
		if err != nil {
			return responses, err
		}
		responses = append(responses, response)
	}
}

func text(value string) *watchv1.Value {
	return &watchv1.Value{Kind: &watchv1.Value_Text{Text: value}}
}

func TestWatchTranslatesEveryEvent(t *testing.T) {
	cursor := cdc.NewCursorForTest(testSource, 0x2A)
	watcher := &scriptedWatcher{events: []watch.Event{
		watch.SnapshotBegin{},
		watch.SnapshotRows{Rows: []map[string]any{{"id": "1", "note": nil}}},
		watch.SnapshotEnd{},
		watch.Batch{Cursor: cursor, Changes: []cdc.Change{{
			Operation: cdc.OperationUpdate,
			Table:     "public.permissions",
			Key:       map[string]string{"id": "1"},
			Values:    map[string]any{"id": "1", "note": nil, "blob": cdc.UnchangedToast{}},
		}, {
			Operation: cdc.OperationDelete,
			Table:     "public.permissions",
			Key:       map[string]string{"id": "2"},
		}}},
		watch.Progress{Cursor: cursor},
		watch.Resync{Reason: watch.ResyncSlowWatcher},
	}}
	_, client, _ := startServer(t, watcher)

	responses, err := receiveAll(t, client, &watchv1.WatchRequest{Projection: "permissions", Scope: "tenant-a"})
	if err != nil {
		t.Fatalf("stream error: %v", err)
	}

	null := &watchv1.Value{Kind: &watchv1.Value_Null{Null: structpb.NullValue_NULL_VALUE}}
	wireCursor := testSource + "@0/2A"
	want := []*watchv1.WatchResponse{
		{Event: &watchv1.WatchResponse_SnapshotBegin{SnapshotBegin: &watchv1.SnapshotBegin{}}},
		{Event: &watchv1.WatchResponse_SnapshotRows{SnapshotRows: &watchv1.SnapshotRows{Rows: []*watchv1.Row{
			{Values: map[string]*watchv1.Value{"id": text("1"), "note": null}},
		}}}},
		{Event: &watchv1.WatchResponse_SnapshotEnd{SnapshotEnd: &watchv1.SnapshotEnd{}}},
		{Event: &watchv1.WatchResponse_Batch{Batch: &watchv1.Batch{Cursor: wireCursor, Changes: []*watchv1.Change{{
			Operation: watchv1.Operation_OPERATION_UPDATE,
			Table:     "public.permissions",
			Key:       map[string]string{"id": "1"},
			Values: map[string]*watchv1.Value{
				"id":   text("1"),
				"note": null,
				"blob": {Kind: &watchv1.Value_UnchangedToast{UnchangedToast: &emptypb.Empty{}}},
			},
		}, {
			Operation: watchv1.Operation_OPERATION_DELETE,
			Table:     "public.permissions",
			Key:       map[string]string{"id": "2"},
		}}}}},
		{Event: &watchv1.WatchResponse_Progress{Progress: &watchv1.Progress{Cursor: wireCursor}}},
		{Event: &watchv1.WatchResponse_Resync{Resync: &watchv1.Resync{Reason: watchv1.ResyncReason_RESYNC_REASON_SLOW_WATCHER}}},
	}
	if len(responses) != len(want) {
		t.Fatalf("responses = %d, want %d: %v", len(responses), len(want), responses)
	}
	for index := range want {
		if !proto.Equal(responses[index], want[index]) {
			t.Errorf("response %d = %v, want %v", index, responses[index], want[index])
		}
	}
	if watcher.got != (watch.Request{Projection: "permissions", Scope: "tenant-a"}) {
		t.Fatalf("watch request = %+v", watcher.got)
	}
}

func TestWatchPassesResumeCursorToTheWatcher(t *testing.T) {
	watcher := &scriptedWatcher{}
	_, client, _ := startServer(t, watcher)
	if _, err := receiveAll(t, client, &watchv1.WatchRequest{Projection: "p", Scope: "s", ResumeCursor: testSource + "@0/2A"}); err != nil {
		t.Fatalf("stream error: %v", err)
	}
	if watcher.got.ResumeCursor != "0/2A" {
		t.Fatalf("resume cursor passed on = %q, want %q", watcher.got.ResumeCursor, "0/2A")
	}
}

func TestWatchRejectsForeignAndMalformedCursors(t *testing.T) {
	_, client, _ := startServer(t, &scriptedWatcher{})
	for _, resume := range []string{"other-source@0/2A", "0/2A", testSource + "@not-a-position"} {
		_, err := receiveAll(t, client, &watchv1.WatchRequest{Projection: "p", ResumeCursor: resume})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("resume %q: error = %v, want InvalidArgument", resume, err)
		}
	}
}

func TestWatchMapsErrorsToStatusCodes(t *testing.T) {
	for _, test := range []struct {
		err  error
		code codes.Code
	}{
		{err: watch.ErrUnknownProjection, code: codes.NotFound},
		{err: cdc.ErrInvalidCursor, code: codes.InvalidArgument},
		{err: cdc.ErrSourceUnavailable, code: codes.Unavailable},
		{err: cdc.ErrInvalidReaderConfig, code: codes.FailedPrecondition},
		{err: errors.New("postgres://user:secret@db internal detail"), code: codes.Internal},
	} {
		_, client, _ := startServer(t, &scriptedWatcher{err: test.err})
		_, err := receiveAll(t, client, &watchv1.WatchRequest{Projection: "p"})
		if status.Code(err) != test.code {
			t.Errorf("%v: code = %v, want %v", test.err, status.Code(err), test.code)
		}
		if strings.Contains(status.Convert(err).Message(), "secret") {
			t.Errorf("%v: status message leaks internal detail: %q", test.err, status.Convert(err).Message())
		}
	}
}

func TestSnapshotRowsArePackedIntoBoundedMessages(t *testing.T) {
	rows := make([]map[string]any, 10)
	for index := range rows {
		rows[index] = map[string]any{"payload": strings.Repeat("x", 100)}
	}
	_, client, _ := startServer(t, &scriptedWatcher{events: []watch.Event{watch.SnapshotRows{Rows: rows}}}, func(cfg *Config) {
		cfg.MaxRowsMessageBytes = 350 // room for three ~110-byte rows
	})

	responses, err := receiveAll(t, client, &watchv1.WatchRequest{Projection: "p"})
	if err != nil {
		t.Fatalf("stream error: %v", err)
	}
	total := 0
	for _, response := range responses {
		message := response.GetSnapshotRows()
		if message == nil {
			t.Fatalf("unexpected response %v", response)
		}
		if len(message.GetRows()) == 0 || len(message.GetRows()) > 3 {
			t.Fatalf("message holds %d rows, want 1-3", len(message.GetRows()))
		}
		total += len(message.GetRows())
	}
	if total != len(rows) || len(responses) != 4 {
		t.Fatalf("rows = %d in %d messages, want %d in 4", total, len(responses), len(rows))
	}
}

func TestCompareCursors(t *testing.T) {
	_, client, _ := startServer(t, &scriptedWatcher{})
	ctx := context.Background()
	for _, test := range []struct {
		left, right string
		want        watchv1.CursorOrder
	}{
		{left: "0/10", right: "0/20", want: watchv1.CursorOrder_CURSOR_ORDER_BEFORE},
		{left: "0/20", right: "0/20", want: watchv1.CursorOrder_CURSOR_ORDER_EQUAL},
		{left: "1/0", right: "0/FFFF", want: watchv1.CursorOrder_CURSOR_ORDER_AFTER},
	} {
		response, err := client.CompareCursors(ctx, &watchv1.CompareCursorsRequest{Left: testSource + "@" + test.left, Right: testSource + "@" + test.right})
		if err != nil {
			t.Fatalf("CompareCursors(%s, %s): %v", test.left, test.right, err)
		}
		if response.GetOrder() != test.want {
			t.Errorf("CompareCursors(%s, %s) = %v, want %v", test.left, test.right, response.GetOrder(), test.want)
		}
	}

	_, err := client.CompareCursors(ctx, &watchv1.CompareCursorsRequest{Left: testSource + "@0/10", Right: "other@0/10"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("cross-source compare error = %v, want InvalidArgument", err)
	}
}

func TestHealthFollowsSetServing(t *testing.T) {
	server, _, conn := startServer(t, &scriptedWatcher{})
	health := healthpb.NewHealthClient(conn)
	check := func() healthpb.HealthCheckResponse_ServingStatus {
		t.Helper()
		response, err := health.Check(context.Background(), &healthpb.HealthCheckRequest{Service: watchv1.WatchService_ServiceDesc.ServiceName})
		if err != nil {
			t.Fatalf("health check: %v", err)
		}
		return response.GetStatus()
	}

	if got := check(); got != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("initial status = %v, want NOT_SERVING", got)
	}
	server.SetServing(true)
	if got := check(); got != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("status = %v, want SERVING", got)
	}
	server.SetServing(false)
	if got := check(); got != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("status = %v, want NOT_SERVING", got)
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	for name, cfg := range map[string]Config{
		"no source":         {},
		"separator":         {SourceID: "a@b"},
		"negative row size": {SourceID: testSource, MaxRowsMessageBytes: -1},
	} {
		if _, err := New(cfg, &scriptedWatcher{}); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: error = %v, want %v", name, err, ErrInvalidConfig)
		}
	}
	if _, err := New(Config{SourceID: testSource}, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("nil watcher: error = %v, want %v", err, ErrInvalidConfig)
	}
}
