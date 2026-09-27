package server

import (
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"

	watchv1 "github.com/usernamenenad/watchd/api/watch/v1"
	"github.com/usernamenenad/watchd/internal/cdc"
	"github.com/usernamenenad/watchd/internal/telemetry/telemetrytest"
	"github.com/usernamenenad/watchd/internal/watch"
)

func TestBatchCarriesCommitTimeAndServerMetricsMove(t *testing.T) {
	metrics := telemetrytest.New(t)
	committed := time.Date(2026, 9, 27, 12, 0, 0, 123456000, time.UTC)
	cursor := cdc.NewCursorForTest(testSource, 0x2A)
	watcher := &scriptedWatcher{events: []watch.Event{
		watch.SnapshotBegin{},
		watch.SnapshotRows{Rows: []map[string]any{{"id": "1"}, {"id": "2"}}},
		watch.SnapshotEnd{},
		watch.Batch{Cursor: cursor, CommitTime: committed, Changes: []cdc.Change{{
			Operation: cdc.OperationInsert, Table: "public.permissions", Key: map[string]string{"id": "3"},
		}}},
		watch.Batch{Cursor: cursor}, // commit time unknown
		watch.Progress{Cursor: cursor},
	}}
	_, client, _ := startServer(t, watcher, func(cfg *Config) { cfg.Meter = metrics.Meter() })

	responses, err := receiveAll(t, client, &watchv1.WatchRequest{Projection: "permissions", Scope: "tenant-a"})
	if err != nil {
		t.Fatalf("stream error: %v", err)
	}
	var batches []*watchv1.Batch
	for _, response := range responses {
		if batch := response.GetBatch(); batch != nil {
			batches = append(batches, batch)
		}
	}
	if len(batches) != 2 {
		t.Fatalf("batches = %d, want 2", len(batches))
	}
	if got := batches[0].GetCommitTime().AsTime(); !got.Equal(committed) {
		t.Errorf("commit_time = %v, want %v", got, committed)
	}
	if batches[1].CommitTime != nil {
		t.Errorf("a batch with no known commit time carries %v", batches[1].CommitTime)
	}

	none := *attribute.EmptySet()
	if got := metrics.HistogramCount(t, "watchd.server.send.duration", none); got != uint64(len(responses)) {
		t.Errorf("send.duration count = %d, want one per message (%d)", got, len(responses))
	}
	if got := metrics.HistogramCount(t, "watchd.server.batch.commit_to_send", none); got != 1 {
		t.Errorf("commit_to_send count = %d, want 1", got)
	}
	if got, _ := metrics.Value(t, "watchd.server.snapshot.bytes", none); got <= 0 {
		t.Errorf("snapshot.bytes = %v, want > 0", got)
	}
	if got, ok := metrics.Value(t, "watchd.server.streams.active", none); !ok || got != 0 {
		t.Errorf("streams.active after the stream = %v (recorded %v), want 0", got, ok)
	}
	metrics.AssertAllowedAttributes(t)
}
