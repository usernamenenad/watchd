package watchd

import (
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	watchv1 "github.com/usernamenenad/watchd/api/watch/v1"
	"github.com/usernamenenad/watchd/internal/telemetry/telemetrytest"
)

func TestSyncMetricsFollowTheProjection(t *testing.T) {
	metrics := telemetrytest.New(t)
	server, client := startScripted(t)
	store := NewMemoryStore("user_id")
	run := startSync(t, client, store, func(cfg *SyncConfig) { cfg.Meter = metrics.Meter() })
	projection := attribute.NewSet(attribute.String("projection", "permissions"))
	value := func(name string, attrs attribute.Set) float64 {
		got, _ := metrics.Value(t, name, attrs)
		return got
	}

	// A snapshot, then a batch that carries its commit time.
	committed := &watchv1.Batch{Cursor: "src@2", CommitTime: timestamppb.New(time.Now().Add(-time.Second)),
		Changes: []*watchv1.Change{upsert("u1", map[string]*watchv1.Value{"role": textValue("owner")})}}
	nextCall(t, server).respond(t, status.Error(codes.Unavailable, "restarting"),
		snapshotBegin, snapshotRows(row("u1", "editor")), snapshotEnd, progress("src@1"),
		&watchv1.WatchResponse{Event: &watchv1.WatchResponse_Batch{Batch: committed}}, progress("src@2"))
	run.waitForState(t, State{Fresh: true, Cursor: "src@2"})

	// The stream fails; the client resumes and is told to resync, then
	// rebuilds.
	nextCall(t, server).respond(t, nil, resync(watchv1.ResyncReason_RESYNC_REASON_SLOW_WATCHER))
	nextCall(t, server).respond(t, nil, snapshotBegin, snapshotRows(row("u1", "owner")), snapshotEnd, progress("src@3"))
	run.waitForState(t, State{Fresh: true, Cursor: "src@3"})

	if got := value("watchd.sdk.syncs", projection); got != 1 {
		t.Errorf("syncs = %v, want 1", got)
	}
	if got := value("watchd.sdk.fresh", projection); got != 1 {
		t.Errorf("fresh = %v, want 1", got)
	}
	for name, want := range map[string]uint64{
		"watchd.sdk.apply.duration":            1,
		"watchd.sdk.commit_to_apply":           1,
		"watchd.sdk.snapshot.install.duration": 2,
		"watchd.sdk.time_to_fresh":             2,
	} {
		if got := metrics.HistogramCount(t, name, projection); got != want {
			t.Errorf("%s count = %d, want %d", name, got, want)
		}
	}
	slow := attribute.NewSet(attribute.String("projection", "permissions"), attribute.String("reason", "slow_watcher"))
	if got := value("watchd.sdk.resyncs", slow); got != 1 {
		t.Errorf("resyncs{slow_watcher} = %v, want 1", got)
	}
	unavailable := attribute.NewSet(attribute.String("error_class", "unavailable"), attribute.String("projection", "permissions"))
	if got := value("watchd.sdk.stream.errors", unavailable); got != 1 {
		t.Errorf("stream.errors{unavailable} = %v, want 1", got)
	}
	if applied, ok := store.Get(map[string]string{"user_id": "u1"}); !ok || applied["role"].Text != "owner" {
		t.Errorf("u1 = %v", applied)
	}

	// Once Sync returns, it is neither running nor fresh.
	run.cancel()
	if err := <-run.done; err != nil {
		t.Fatalf("Sync = %v", err)
	}
	run.done <- nil // for the cleanup
	if got := value("watchd.sdk.syncs", projection); got != 0 {
		t.Errorf("syncs after return = %v, want 0", got)
	}
	if got := value("watchd.sdk.fresh", projection); got != 0 {
		t.Errorf("fresh after return = %v, want 0", got)
	}
	metrics.AssertAllowedAttributes(t)
}

func TestConvertBatchCarriesCommitTime(t *testing.T) {
	at := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	if got := convertBatch(&watchv1.Batch{Cursor: "c", CommitTime: timestamppb.New(at)}).CommitTime; !got.Equal(at) {
		t.Errorf("CommitTime = %v, want %v", got, at)
	}
	if got := convertBatch(&watchv1.Batch{Cursor: "c"}).CommitTime; !got.IsZero() {
		t.Errorf("CommitTime without one = %v, want zero", got)
	}
}
