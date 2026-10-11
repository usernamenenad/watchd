package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto"

	watchv1 "github.com/usernamenenad/watchd/api/watch/v1"
	"github.com/usernamenenad/watchd/internal/cdc"
	"github.com/usernamenenad/watchd/internal/watch"
)

// encodingStream is a watch stream that encodes every message with gRPC's
// proto codec, as a real stream does, and discards it.
type encodingStream struct {
	grpc.ServerStream // unused; nil
	codec             encoding.CodecV2
}

func (s *encodingStream) Send(message *watchv1.WatchResponse) error {
	data, err := s.codec.Marshal(message)
	data.Free()
	return err
}

func (s *encodingStream) Context() context.Context { return context.Background() }

// BenchmarkSendBatch serves one transaction - an 8-column update - to N
// watchers of one scope, each on its own stream. "shared" is how the hub
// delivers a live batch: one Memo per scope once it has several watchers,
// so the batch is encoded once. "per-stream" encodes it for every stream,
// as before #70, and as a batch replayed to one watcher still is.
func BenchmarkSendBatch(b *testing.B) {
	server, err := New(Config{SourceID: testSource}, &scriptedWatcher{})
	if err != nil {
		b.Fatal(err)
	}
	columns := []string{"tenant_id", "user_id", "role", "granted_by", "granted_at", "expires_at", "note", "revision"}
	values := make([]cdc.Value, len(columns))
	for index, column := range columns {
		values[index] = cdc.TextValue(column + "-value")
	}
	batch := watch.Batch{
		Cursor:     cdc.NewCursorForTest(testSource, 0x16B3748),
		CommitTime: time.Now(),
		Changes: []cdc.Change{{
			Operation: cdc.OperationUpdate,
			Table:     "public.permissions",
			Key:       map[string]string{"tenant_id": "tenant_id-value", "user_id": "user_id-value"},
			Columns:   columns,
			Values:    values,
		}},
	}

	for _, watchers := range []int{1, 100, 1000} {
		streams := make([]*encodingStream, watchers)
		for index := range streams {
			streams[index] = &encodingStream{codec: encoding.GetCodecV2("proto")}
		}
		for _, mode := range []string{"per-stream", "shared"} {
			b.Run(fmt.Sprintf("watchers=%d/mode=%s", watchers, mode), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					event := batch
					if mode == "shared" && watchers > 1 {
						event.Memo = new(watch.Memo)
					}
					for _, stream := range streams {
						if err := server.send(stream, event); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}
