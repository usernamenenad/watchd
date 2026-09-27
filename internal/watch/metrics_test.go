package watch

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/usernamenenad/watchd/internal/cdc"
	"github.com/usernamenenad/watchd/internal/telemetry"
	"github.com/usernamenenad/watchd/internal/telemetry/telemetrytest"
)

func none() attribute.Set { return *attribute.EmptySet() }

func reason(r string) attribute.Set {
	return telemetry.Attributes(telemetry.KeyReason.String(r))
}

func TestHubMetricsFollowWatchersAndTheReplayWindow(t *testing.T) {
	metrics := telemetrytest.New(t)
	hub := newTestHub(t, &fakeSnapshotter{snapshot: cdc.NewSnapshotForTest(testSource, 50, 1, 1)}, func(cfg *Config) {
		cfg.MaxReplayTransactions = 2
		cfg.Meter = metrics.Meter()
	})
	live := startWatch(t, hub, Request{Projection: "permissions", Scope: tenantA})
	live.expect(t, SnapshotBegin{}, SnapshotRows{}, SnapshotEnd{}, Progress{Cursor: cursor(50)})

	committed := time.Now().Add(-time.Minute)
	transactions := make([]cdc.Transaction, 3)
	for i := range transactions {
		transactions[i] = transaction(uint64(100*(i+1)), uint32(i+5), change(tenantA, fmt.Sprint(i)))
		transactions[i].CommitTime = committed
		accept(t, hub, transactions[i])
		live.expect(t, batch(transactions[i], change(tenantA, fmt.Sprint(i))), Progress{Cursor: transactions[i].Cursor})
	}
	// A cursor evicted from the window resyncs.
	startWatch(t, hub, Request{Projection: "permissions", Scope: tenantA, ResumeCursor: cursor(50).String()}).
		expect(t, Resync{Reason: ResyncCursorUnavailable})

	for name, want := range map[string]float64{
		"watchd.hub.replay.occupancy": 2,
		"watchd.hub.replay.capacity":  2,
		"watchd.hub.replay.evictions": 1,
		"watchd.hub.watchers":         1,
		"watchd.hub.progress":         4,
	} {
		if got, _ := metrics.Value(t, name, none()); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	if got, _ := metrics.Value(t, "watchd.hub.resyncs", reason("cursor_unavailable")); got != 1 {
		t.Errorf("resyncs{cursor_unavailable} = %v, want 1", got)
	}
	if age, ok := metrics.Value(t, "watchd.hub.replay.oldest_age", none()); !ok || age < 59 {
		t.Errorf("replay.oldest_age = %v (recorded %v), want about 60", age, ok)
	}
	for name, want := range map[string]uint64{
		"watchd.hub.accept.duration":     3,
		"watchd.hub.lock.wait":           3,
		"watchd.hub.watcher.queue_depth": 3,
		"watchd.hub.watcher.lag":         3,
	} {
		if got := metrics.HistogramCount(t, name, none()); got != want {
			t.Errorf("%s count = %d, want %d", name, got, want)
		}
	}
	metrics.AssertAllowedAttributes(t)
}

func TestHubMetricsCountSlowWatcherDrops(t *testing.T) {
	metrics := telemetrytest.New(t)
	hub := newTestHub(t, &fakeSnapshotter{snapshot: cdc.NewSnapshotForTest(testSource, 50, 1, 1)}, func(cfg *Config) {
		cfg.WatcherQueue = 1
		cfg.Meter = metrics.Meter()
	})
	blocked := make(chan struct{})
	done := make(chan error, 1)
	progressed := make(chan struct{}, 1)
	go func() {
		done <- hub.Watch(context.Background(), Request{Projection: "permissions", Scope: tenantA}, func(event Event) error {
			if _, ok := event.(Progress); ok {
				select {
				case progressed <- struct{}{}:
					<-blocked
				default:
				}
			}
			return nil
		})
	}()
	<-progressed
	accept(t, hub, transaction(100, 5, change(tenantA, "u1")), transaction(200, 6, change(tenantA, "u2")))
	close(blocked)
	if err := <-done; err != nil {
		t.Fatalf("watch = %v", err)
	}
	if got, _ := metrics.Value(t, "watchd.hub.resyncs", reason("slow_watcher")); got != 1 {
		t.Errorf("resyncs{slow_watcher} = %v, want 1", got)
	}
	if got, _ := metrics.Value(t, "watchd.hub.watchers", none()); got != 0 {
		t.Errorf("watchers after the drop = %v, want 0", got)
	}
}

// BenchmarkHubAccept measures fanning one transaction out to many
// watchers, with and without instruments; compare allocs/op between them.
// Each transaction's two changes concern either no watcher ("none") or the
// one watcher of tenant-0 ("one-scope"), which the benchmark drains so it is
// never dropped. Either way Accept's cost should not grow with the number
// of watchers.
func BenchmarkHubAccept(b *testing.B) {
	for _, watchers := range []int{1, 100, 1000, 10000} {
		for _, shape := range []string{"none", "one-scope"} {
			for _, mode := range []string{"noop", "sdk"} {
				b.Run(fmt.Sprintf("watchers=%d/%s/%s", watchers, shape, mode), func(b *testing.B) {
					var meter metric.Meter
					if mode == "sdk" {
						meter = telemetrytest.New(b).Meter()
					}
					hub, err := New(Config{
						SourceID:    testSource,
						Projections: map[string]cdc.ProjectionSpec{"permissions": testSpec()},
						Meter:       meter,
					}, &fakeSnapshotter{})
					if err != nil {
						b.Fatal(err)
					}
					projection := hub.projections["permissions"]
					var target *watcher
					for i := range watchers {
						w := &watcher{projection: projection, scope: fmt.Sprintf("tenant-%d", i), queue: make(chan queued, 1), overflow: make(chan struct{})}
						hub.registerLocked(w)
						if i == 0 {
							target = w
						}
					}
					scope := "elsewhere"
					if shape == "one-scope" {
						scope = target.scope
					}
					changes := []cdc.Change{change(scope, "u1"), change(scope, "u2")}
					ctx := context.Background()
					lsn := uint64(0)
					b.ReportAllocs()
					for b.Loop() {
						lsn += 100
						if err := hub.Accept(ctx, transaction(lsn, uint32(lsn), changes...)); err != nil {
							b.Fatal(err)
						}
						select {
						case <-target.queue:
						default:
						}
					}
					select {
					case <-target.overflow:
						b.Fatal("the drained watcher was dropped")
					default:
					}
				})
			}
		}
	}
}
