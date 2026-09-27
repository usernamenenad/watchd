package watchd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	watchv1 "github.com/usernamenenad/watchd/api/watch/v1"
)

// DefaultMaxMessageBytes is the largest server message Dial accepts by
// default. A Batch is one source transaction and is never split, so this
// must exceed the server's transaction limit (16 MiB by default).
const DefaultMaxMessageBytes = 64 << 20

const (
	defaultMinBackoff = 100 * time.Millisecond
	defaultMaxBackoff = 5 * time.Second
)

// errResync ends one stream when the server asks for a resync.
var errResync = errors.New("watchd: server requested resync")

// Client talks to one watchd server.
type Client struct {
	conn *grpc.ClientConn
	api  watchv1.WatchServiceClient
}

// Dial creates a client for target. Pass the transport credentials to use,
// for example grpc.WithTransportCredentials(insecure.NewCredentials()) for a
// local server. Dial raises the maximum message size to
// DefaultMaxMessageBytes; a later grpc.WithDefaultCallOptions overrides it.
func Dial(target string, opts ...grpc.DialOption) (*Client, error) {
	opts = append([]grpc.DialOption{grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(DefaultMaxMessageBytes))}, opts...)
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, api: watchv1.NewWatchServiceClient(conn)}, nil
}

// Close closes the client's connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// CompareCursors reports whether cursor a is before (-1), equal to (0), or
// after (1) cursor b. Cursors are opaque: compare them only through this.
// Cursors from different sources are not comparable and return an error.
func (c *Client) CompareCursors(ctx context.Context, a, b string) (int, error) {
	response, err := c.api.CompareCursors(ctx, &watchv1.CompareCursorsRequest{Left: a, Right: b})
	if err != nil {
		return 0, err
	}
	switch response.GetOrder() {
	case watchv1.CursorOrder_CURSOR_ORDER_BEFORE:
		return -1, nil
	case watchv1.CursorOrder_CURSOR_ORDER_EQUAL:
		return 0, nil
	case watchv1.CursorOrder_CURSOR_ORDER_AFTER:
		return 1, nil
	default:
		return 0, fmt.Errorf("watchd: unknown cursor order %v", response.GetOrder())
	}
}

// SyncConfig configures Sync.
type SyncConfig struct {
	// Projection and Scope select what to keep in sync, for example
	// projection "permissions" and scope "<tenant id>".
	Projection string
	Scope      string
	// Store holds the projection and its cursor.
	Store Store
	// OnState, if set, is called with every state change: when the
	// projection becomes fresh, when its confirmed cursor advances, and when
	// it becomes stale. It runs on Sync's goroutine; keep it quick.
	OnState func(State)
	// MinBackoff and MaxBackoff bound the wait before reconnecting after a
	// failure. They default to 100ms and 5s.
	MinBackoff time.Duration
	MaxBackoff time.Duration
}

// Sync keeps cfg.Store in sync until ctx is cancelled, then returns nil.
//
// It resumes from the store's cursor when there is one, and otherwise
// installs a snapshot. It reconnects after connection failures, with
// backoff, and rebuilds the projection from a new snapshot whenever the
// server asks for a resync. It returns an error only for conditions a retry
// cannot fix: an unknown projection, a misconfigured server, or a failing
// Store.
func (c *Client) Sync(ctx context.Context, cfg SyncConfig) error {
	if cfg.Projection == "" || cfg.Store == nil {
		return errors.New("watchd: Sync needs a projection and a store")
	}
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = defaultMinBackoff
	}
	if cfg.MaxBackoff < cfg.MinBackoff {
		cfg.MaxBackoff = max(defaultMaxBackoff, cfg.MinBackoff)
	}

	s := &syncer{client: c, cfg: cfg}
	backoff := cfg.MinBackoff
	snapshot := false // force a snapshot, ignoring the stored cursor
	resyncs := 0      // resyncs since the last confirmed progress
	for {
		err := s.stream(ctx, snapshot)
		s.setState(false, s.state.Cursor)
		if ctx.Err() != nil {
			return nil
		}

		retryNow := false
		switch {
		case errors.Is(err, errResync):
			// The stored cursor cannot be continued from; rebuild now. A
			// server that keeps asking without ever confirming progress (for
			// example one whose source stopped) is retried with backoff.
			snapshot = true
			resyncs++
			retryNow = resyncs == 1
		case errors.Is(err, errStore):
			return err
		case status.Code(err) == codes.InvalidArgument && !snapshot:
			// The server refused the stored cursor, for example one from a
			// different source: start over from a snapshot.
			snapshot, retryNow = true, true
		case isPermanent(err):
			return err
		default:
			// Connection lost or server unavailable: reconnect, resuming
			// from the stored cursor.
			snapshot = false
		}
		if s.progressed {
			backoff = cfg.MinBackoff
			if !errors.Is(err, errResync) {
				resyncs = 0
			} else {
				resyncs = 1
			}
		}
		if retryNow {
			continue
		}

		wait := time.Duration(float64(backoff) * (0.8 + 0.4*rand.Float64()))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		backoff = min(backoff*2, cfg.MaxBackoff)
	}
}

// errStore wraps a Store failure, which ends Sync.
var errStore = errors.New("watchd: store failed")

func isPermanent(err error) bool {
	switch status.Code(err) {
	case codes.NotFound, codes.InvalidArgument, codes.FailedPrecondition, codes.PermissionDenied, codes.Unauthenticated, codes.Unimplemented:
		return true
	}
	return false
}

// syncer is the state of one Sync call.
type syncer struct {
	client     *Client
	cfg        SyncConfig
	state      State
	reported   bool
	progressed bool
}

// stream runs one Watch stream until it fails or the server asks for a
// resync, applying every event to the store.
func (s *syncer) stream(ctx context.Context, snapshot bool) error {
	s.progressed = false
	cursor := ""
	if !snapshot {
		stored, err := s.cfg.Store.Cursor(ctx)
		if err != nil {
			return fmt.Errorf("%w: read cursor: %w", errStore, err)
		}
		cursor = stored
	}

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := s.client.api.Watch(streamCtx, &watchv1.WatchRequest{
		Projection:   s.cfg.Projection,
		Scope:        s.cfg.Scope,
		ResumeCursor: cursor,
	})
	if err != nil {
		return err
	}

	var writer SnapshotWriter
	defer func() {
		if writer != nil {
			_ = writer.Abort(context.WithoutCancel(ctx))
		}
	}()

	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return errors.New("watchd: server ended the stream")
		}
		if err != nil {
			return err
		}

		switch event := response.GetEvent().(type) {
		case *watchv1.WatchResponse_SnapshotBegin:
			s.setState(false, "")
			if writer, err = s.cfg.Store.BeginSnapshot(ctx); err != nil {
				writer = nil
				return fmt.Errorf("%w: begin snapshot: %w", errStore, err)
			}

		case *watchv1.WatchResponse_SnapshotRows:
			if writer == nil {
				return errors.New("watchd: snapshot rows outside a snapshot")
			}
			rows := make([]Row, len(event.SnapshotRows.GetRows()))
			for index, row := range event.SnapshotRows.GetRows() {
				rows[index] = convertValues(row.GetValues())
			}
			if err := writer.Put(ctx, rows); err != nil {
				return fmt.Errorf("%w: put snapshot rows: %w", errStore, err)
			}

		case *watchv1.WatchResponse_SnapshotEnd:
			if writer == nil {
				return errors.New("watchd: snapshot end outside a snapshot")
			}
			err := writer.Commit(ctx)
			writer = nil
			if err != nil {
				return fmt.Errorf("%w: commit snapshot: %w", errStore, err)
			}

		case *watchv1.WatchResponse_Batch:
			if writer != nil {
				return errors.New("watchd: batch inside a snapshot")
			}
			if err := s.cfg.Store.Apply(ctx, convertBatch(event.Batch)); err != nil {
				return fmt.Errorf("%w: apply batch: %w", errStore, err)
			}

		case *watchv1.WatchResponse_Progress:
			if writer != nil {
				return errors.New("watchd: progress inside a snapshot")
			}
			cursor := event.Progress.GetCursor()
			if err := s.cfg.Store.SaveCursor(ctx, cursor); err != nil {
				return fmt.Errorf("%w: save cursor: %w", errStore, err)
			}
			s.progressed = true
			s.setState(true, cursor)

		case *watchv1.WatchResponse_Resync:
			return errResync

		default:
			return fmt.Errorf("watchd: unknown server event %T", event)
		}
	}
}

// setState records the state and reports it if it changed.
func (s *syncer) setState(fresh bool, cursor string) {
	next := State{Fresh: fresh, Cursor: cursor}
	if s.reported && next == s.state {
		return
	}
	s.state, s.reported = next, true
	if s.cfg.OnState != nil {
		s.cfg.OnState(next)
	}
}

func convertBatch(batch *watchv1.Batch) Batch {
	changes := make([]Change, len(batch.GetChanges()))
	for index, change := range batch.GetChanges() {
		changes[index] = Change{
			Operation: operations[change.GetOperation()],
			Table:     change.GetTable(),
			Key:       change.GetKey(),
			Values:    convertValues(change.GetValues()),
		}
	}
	return Batch{Cursor: batch.GetCursor(), Changes: changes}
}

var operations = map[watchv1.Operation]Operation{
	watchv1.Operation_OPERATION_INSERT: Insert,
	watchv1.Operation_OPERATION_UPDATE: Update,
	watchv1.Operation_OPERATION_DELETE: Delete,
}

func convertValues(values map[string]*watchv1.Value) Row {
	row := make(Row, len(values))
	for column, value := range values {
		switch kind := value.GetKind().(type) {
		case *watchv1.Value_Text:
			row[column] = Value{Text: kind.Text}
		case *watchv1.Value_Null:
			row[column] = Value{Null: true}
		case *watchv1.Value_UnchangedToast:
			row[column] = Value{Unchanged: true}
		}
	}
	return row
}
