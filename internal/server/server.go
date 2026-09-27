// Package server adapts the watch runtime to the v1 gRPC API.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	watchv1 "github.com/usernamenenad/watchd/api/watch/v1"
	"github.com/usernamenenad/watchd/internal/cdc"
	"github.com/usernamenenad/watchd/internal/watch"
)

// ErrInvalidConfig indicates a server configuration that cannot be served.
var ErrInvalidConfig = errors.New("server: invalid configuration")

const defaultMaxRowsMessageBytes = 1 << 20

// Watcher serves watch requests; *watch.Hub implements it.
type Watcher interface {
	Watch(ctx context.Context, req watch.Request, send func(watch.Event) error) error
}

// Config configures a Server.
type Config struct {
	// SourceID is the source the server's watcher serves. Every cursor the
	// server hands out carries it, and cursors of other sources are refused.
	SourceID string
	// MaxRowsMessageBytes is the size snapshot rows are packed into per
	// message, so a snapshot never arrives as one huge message. A single row
	// larger than this is still sent, alone. Batches are never split: one
	// batch is one source transaction, bounded by the source's
	// MaxTransactionBytes, and clients must accept messages that large.
	MaxRowsMessageBytes int
	// Logger is optional. It never receives row values or scopes.
	Logger *slog.Logger
	// Meter is optional. The server's instruments are created from it once,
	// in New; nil records nothing. See docs/observability.md.
	Meter metric.Meter
}

// Server implements watch.v1.WatchService and the standard gRPC health
// service on top of a Watcher.
type Server struct {
	watchv1.UnimplementedWatchServiceServer

	sourceID            string
	watcher             Watcher
	maxRowsMessageBytes int
	logger              *slog.Logger
	health              *health.Server
	metrics             *serverMetrics
}

// New creates a server. It reports NOT_SERVING until SetServing(true).
func New(cfg Config, watcher Watcher) (*Server, error) {
	if cfg.SourceID == "" || strings.Contains(cfg.SourceID, cursorSeparator) || watcher == nil || cfg.MaxRowsMessageBytes < 0 {
		return nil, ErrInvalidConfig
	}
	if cfg.MaxRowsMessageBytes == 0 {
		cfg.MaxRowsMessageBytes = defaultMaxRowsMessageBytes
	}
	metrics, err := newServerMetrics(cfg.Meter)
	if err != nil {
		return nil, fmt.Errorf("server: create instruments: %w", err)
	}
	s := &Server{
		sourceID:            cfg.SourceID,
		watcher:             watcher,
		maxRowsMessageBytes: cfg.MaxRowsMessageBytes,
		logger:              cfg.Logger,
		health:              health.NewServer(),
		metrics:             metrics,
	}
	s.SetServing(false)
	return s, nil
}

// Register registers the watch and health services on registrar.
func (s *Server) Register(registrar grpc.ServiceRegistrar) {
	watchv1.RegisterWatchServiceServer(registrar, s)
	healthpb.RegisterHealthServer(registrar, s.health)
}

// SetServing sets the health status of the server and of the watch service:
// SERVING only when the source can serve its contract.
func (s *Server) SetServing(serving bool) {
	state := healthpb.HealthCheckResponse_NOT_SERVING
	if serving {
		state = healthpb.HealthCheckResponse_SERVING
	}
	s.health.SetServingStatus("", state)
	s.health.SetServingStatus(watchv1.WatchService_ServiceDesc.ServiceName, state)
}

// Watch implements watch.v1.WatchService.Watch.
func (s *Server) Watch(req *watchv1.WatchRequest, stream grpc.ServerStreamingServer[watchv1.WatchResponse]) error {
	resume := ""
	if req.GetResumeCursor() != "" {
		cursor, err := s.decodeCursor(req.GetResumeCursor())
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		resume = cursor.String()
	}

	ctx := stream.Context()
	s.metrics.activeStreams.Add(ctx, 1)
	defer s.metrics.activeStreams.Add(context.WithoutCancel(ctx), -1)

	err := s.watcher.Watch(stream.Context(), watch.Request{
		Projection:   req.GetProjection(),
		Scope:        req.GetScope(),
		ResumeCursor: resume,
	}, func(event watch.Event) error {
		return s.send(stream, event)
	})
	return s.status(stream.Context(), err)
}

// CompareCursors implements watch.v1.WatchService.CompareCursors.
func (s *Server) CompareCursors(_ context.Context, req *watchv1.CompareCursorsRequest) (*watchv1.CompareCursorsResponse, error) {
	left, err := s.decodeCursor(req.GetLeft())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "left: "+err.Error())
	}
	right, err := s.decodeCursor(req.GetRight())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "right: "+err.Error())
	}
	order, err := left.Compare(right)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &watchv1.CompareCursorsResponse{Order: map[int]watchv1.CursorOrder{
		-1: watchv1.CursorOrder_CURSOR_ORDER_BEFORE,
		0:  watchv1.CursorOrder_CURSOR_ORDER_EQUAL,
		1:  watchv1.CursorOrder_CURSOR_ORDER_AFTER,
	}[order]}, nil
}

// send translates one watch event into one or more stream messages.
func (s *Server) send(stream grpc.ServerStreamingServer[watchv1.WatchResponse], event watch.Event) error {
	switch event := event.(type) {
	case watch.SnapshotBegin:
		return s.sendMessage(stream, &watchv1.WatchResponse{Event: &watchv1.WatchResponse_SnapshotBegin{SnapshotBegin: &watchv1.SnapshotBegin{}}})

	case watch.SnapshotRows:
		return s.sendRows(stream, event.Rows)

	case watch.SnapshotEnd:
		return s.sendMessage(stream, &watchv1.WatchResponse{Event: &watchv1.WatchResponse_SnapshotEnd{SnapshotEnd: &watchv1.SnapshotEnd{}}})

	case watch.Batch:
		changes := make([]*watchv1.Change, 0, len(event.Changes))
		for _, change := range event.Changes {
			converted, err := convertChange(change)
			if err != nil {
				return err
			}
			changes = append(changes, converted)
		}
		batch := &watchv1.Batch{Cursor: s.encodeCursor(event.Cursor), Changes: changes}
		if !event.CommitTime.IsZero() {
			batch.CommitTime = timestamppb.New(event.CommitTime)
		}
		if err := s.sendMessage(stream, &watchv1.WatchResponse{Event: &watchv1.WatchResponse_Batch{Batch: batch}}); err != nil {
			return err
		}
		if !event.CommitTime.IsZero() {
			s.metrics.commitToSend.Record(stream.Context(), time.Since(event.CommitTime).Seconds())
		}
		return nil

	case watch.Progress:
		return s.sendMessage(stream, &watchv1.WatchResponse{Event: &watchv1.WatchResponse_Progress{Progress: &watchv1.Progress{
			Cursor: s.encodeCursor(event.Cursor),
		}}})

	case watch.Resync:
		return s.sendMessage(stream, &watchv1.WatchResponse{Event: &watchv1.WatchResponse_Resync{Resync: &watchv1.Resync{
			Reason: resyncReasons[event.Reason],
		}}})

	default:
		return fmt.Errorf("server: unknown watch event %T", event)
	}
}

// sendRows packs rows into messages of about maxRowsMessageBytes each.
func (s *Server) sendRows(stream grpc.ServerStreamingServer[watchv1.WatchResponse], rows []map[string]any) error {
	var message []*watchv1.Row
	messageBytes := 0
	flush := func() error {
		if len(message) == 0 {
			return nil
		}
		err := s.sendMessage(stream, &watchv1.WatchResponse{Event: &watchv1.WatchResponse_SnapshotRows{SnapshotRows: &watchv1.SnapshotRows{Rows: message}}})
		if err == nil {
			s.metrics.snapshotBytes.Add(stream.Context(), int64(messageBytes))
		}
		message, messageBytes = nil, 0
		return err
	}

	for _, row := range rows {
		converted, err := convertRow(row)
		if err != nil {
			return err
		}
		rowBytes := proto.Size(converted)
		if messageBytes+rowBytes > s.maxRowsMessageBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		message = append(message, converted)
		messageBytes += rowBytes
	}
	return flush()
}

// sendMessage sends one stream message, timing how long the send blocks:
// gRPC flow control makes it wait for a client that reads slowly.
func (s *Server) sendMessage(stream grpc.ServerStreamingServer[watchv1.WatchResponse], message *watchv1.WatchResponse) error {
	started := time.Now()
	err := stream.Send(message)
	s.metrics.sendDuration.Record(stream.Context(), time.Since(started).Seconds())
	return err
}

var resyncReasons = map[watch.ResyncReason]watchv1.ResyncReason{
	watch.ResyncCursorUnavailable:    watchv1.ResyncReason_RESYNC_REASON_CURSOR_UNAVAILABLE,
	watch.ResyncSlowWatcher:          watchv1.ResyncReason_RESYNC_REASON_SLOW_WATCHER,
	watch.ResyncSnapshotWindowClosed: watchv1.ResyncReason_RESYNC_REASON_SNAPSHOT_WINDOW_CLOSED,
	watch.ResyncSourceStopped:        watchv1.ResyncReason_RESYNC_REASON_SOURCE_STOPPED,
}

var operations = map[string]watchv1.Operation{
	cdc.OperationInsert: watchv1.Operation_OPERATION_INSERT,
	cdc.OperationUpdate: watchv1.Operation_OPERATION_UPDATE,
	cdc.OperationDelete: watchv1.Operation_OPERATION_DELETE,
}

func convertChange(change cdc.Change) (*watchv1.Change, error) {
	operation, ok := operations[change.Operation]
	if !ok {
		return nil, fmt.Errorf("server: unknown change operation %q", change.Operation)
	}
	values, err := convertValues(change.Values)
	if err != nil {
		return nil, err
	}
	return &watchv1.Change{Operation: operation, Table: change.Table, Key: change.Key, Values: values}, nil
}

func convertRow(row map[string]any) (*watchv1.Row, error) {
	values, err := convertValues(row)
	if err != nil {
		return nil, err
	}
	return &watchv1.Row{Values: values}, nil
}

func convertValues(values map[string]any) (map[string]*watchv1.Value, error) {
	if len(values) == 0 {
		return nil, nil
	}
	converted := make(map[string]*watchv1.Value, len(values))
	for column, value := range values {
		switch value := value.(type) {
		case string:
			converted[column] = &watchv1.Value{Kind: &watchv1.Value_Text{Text: value}}
		case nil:
			converted[column] = &watchv1.Value{Kind: &watchv1.Value_Null{Null: structpb.NullValue_NULL_VALUE}}
		case cdc.UnchangedToast:
			converted[column] = &watchv1.Value{Kind: &watchv1.Value_UnchangedToast{UnchangedToast: &emptypb.Empty{}}}
		default:
			// The v0 value contract has exactly these three kinds.
			return nil, fmt.Errorf("server: column %q has unsupported value type %T", column, value)
		}
	}
	return converted, nil
}

// Wire cursors are "<source ID>@<cdc cursor>", so a cursor carries the
// source it belongs to and one from another source is refused, never
// misread. Clients treat the whole string as opaque.
const cursorSeparator = "@"

func (s *Server) encodeCursor(cursor cdc.Cursor) string {
	return s.sourceID + cursorSeparator + cursor.String()
}

func (s *Server) decodeCursor(text string) (cdc.Cursor, error) {
	sourceID, position, found := strings.Cut(text, cursorSeparator)
	if !found {
		return cdc.Cursor{}, fmt.Errorf("malformed cursor %q", text)
	}
	if sourceID != s.sourceID {
		return cdc.Cursor{}, fmt.Errorf("cursor belongs to source %q, not %q", sourceID, s.sourceID)
	}
	cursor, err := cdc.ParseCursor(s.sourceID, position)
	if err != nil {
		return cdc.Cursor{}, fmt.Errorf("malformed cursor %q", text)
	}
	return cursor, nil
}

// status maps a watch error to a gRPC status. Unexpected errors are logged
// and reported without detail, so no internal state reaches the client.
func (s *Server) status(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	if _, ok := status.FromError(err); ok {
		return err // already a status, e.g. from stream.Send
	}
	switch {
	case errors.Is(err, watch.ErrUnknownProjection):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, cdc.ErrInvalidCursor):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, cdc.ErrSourceUnavailable), errors.Is(err, cdc.ErrSourceNotStarted), errors.Is(err, cdc.ErrSlotInUse):
		return status.Error(codes.Unavailable, "source unavailable")
	case errors.Is(err, cdc.ErrInvalidReaderConfig), errors.Is(err, cdc.ErrInsufficientPrivileges):
		// For example a projection table missing from the publication: an
		// operator must fix the source, so retrying will not help.
		if s.logger != nil {
			s.logger.Error("watch refused: source is misconfigured", "error", err)
		}
		return status.Error(codes.FailedPrecondition, "source is misconfigured for this projection")
	}
	if s.logger != nil {
		s.logger.Error("watch failed", "error", err)
	}
	return status.Error(codes.Internal, "internal error")
}
