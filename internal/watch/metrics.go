package watch

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/metric"

	"github.com/usernamenenad/watchd/internal/telemetry"
)

// inProcessBounds cover work inside watchd, in seconds: 50µs to 5s.
var inProcessBounds = []float64{0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

// resyncReasonNames are the reason attribute's values.
var resyncReasonNames = map[ResyncReason]string{
	ResyncCursorUnavailable:    "cursor_unavailable",
	ResyncSlowWatcher:          "slow_watcher",
	ResyncSnapshotWindowClosed: "snapshot_window_closed",
	ResyncSourceStopped:        "source_stopped",
}

// hubMetrics are the hub's instruments, created once in New, with their
// attribute sets, so recording never allocates. With no meter configured,
// every instrument is a no-op.
type hubMetrics struct {
	acceptDuration metric.Float64Histogram
	lockWait       metric.Float64Histogram
	evictions      metric.Int64Counter
	queueDepth     metric.Int64Histogram
	watcherLag     metric.Int64Histogram
	progress       metric.Int64Counter
	resyncs        metric.Int64Counter
	resyncOpts     map[ResyncReason]metric.AddOption
}

func newHubMetrics(meter metric.Meter, h *Hub) (*hubMetrics, error) {
	meter = telemetry.MeterOrNoop(meter)
	m := &hubMetrics{resyncOpts: map[ResyncReason]metric.AddOption{}}
	for reason, name := range resyncReasonNames {
		m.resyncOpts[reason] = metric.WithAttributeSet(telemetry.Attributes(telemetry.KeyReason.String(name)))
	}

	var errs []error
	check := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	var err error
	m.acceptDuration, err = meter.Float64Histogram("watchd.hub.accept.duration", metric.WithUnit("s"),
		metric.WithDescription("Time from a transaction reaching the hub until every relevant watcher has it queued, including waiting for the hub's lock."),
		metric.WithExplicitBucketBoundaries(inProcessBounds...))
	check(err)
	m.lockWait, err = meter.Float64Histogram("watchd.hub.lock.wait", metric.WithUnit("s"),
		metric.WithDescription("Time Accept waited for the hub's lock, held meanwhile by watchers registering, resuming, or reporting progress."),
		metric.WithExplicitBucketBoundaries(inProcessBounds...))
	check(err)
	m.evictions, err = meter.Int64Counter("watchd.hub.replay.evictions", metric.WithUnit("{transaction}"),
		metric.WithDescription("Transactions evicted from the replay window. A watcher resuming from before an evicted one gets Resync."))
	check(err)
	m.queueDepth, err = meter.Int64Histogram("watchd.hub.watcher.queue_depth", metric.WithUnit("{transaction}"),
		metric.WithDescription("Transactions still queued for a watcher each time it takes one, sampled across all watchers."),
		metric.WithExplicitBucketBoundaries(0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 4096, 16384))
	check(err)
	m.watcherLag, err = meter.Int64Histogram("watchd.hub.watcher.lag", metric.WithUnit("By"),
		metric.WithDescription("WAL between the hub's newest transaction and each transaction a watcher takes from its queue, sampled across all watchers."),
		metric.WithExplicitBucketBoundaries(0, 1<<10, 1<<14, 1<<16, 1<<18, 1<<20, 1<<22, 1<<24, 1<<26, 1<<28, 1<<30))
	check(err)
	m.progress, err = meter.Int64Counter("watchd.hub.progress", metric.WithUnit("{event}"),
		metric.WithDescription("Progress events sent to watchers."))
	check(err)
	m.resyncs, err = meter.Int64Counter("watchd.hub.resyncs", metric.WithUnit("{event}"),
		metric.WithDescription("Watches ended with Resync, by reason. reason=slow_watcher counts slow-watcher disconnects."))
	check(err)
	occupancy, err := meter.Int64ObservableGauge("watchd.hub.replay.occupancy", metric.WithUnit("{transaction}"),
		metric.WithDescription("Transactions held in the replay window."))
	check(err)
	capacity, err := meter.Int64ObservableGauge("watchd.hub.replay.capacity", metric.WithUnit("{transaction}"),
		metric.WithDescription("The replay window's capacity, MaxReplayTransactions."))
	check(err)
	oldestAge, err := meter.Float64ObservableGauge("watchd.hub.replay.oldest_age", metric.WithUnit("s"),
		metric.WithDescription("Age of the oldest transaction in the replay window, from its commit time: how far back a watcher can resume."))
	check(err)
	watchers, err := meter.Int64ObservableGauge("watchd.hub.watchers", metric.WithUnit("{watcher}"),
		metric.WithDescription("Watchers registered with the hub."))
	check(err)
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		h.mu.Lock()
		ringLen, watcherCount := len(h.ring), len(h.watchers)
		var oldest time.Time
		if ringLen > 0 {
			oldest = h.ring[0].CommitTime
		}
		h.mu.Unlock()

		o.ObserveInt64(occupancy, int64(ringLen))
		o.ObserveInt64(capacity, int64(h.maxReplay))
		o.ObserveInt64(watchers, int64(watcherCount))
		if !oldest.IsZero() {
			o.ObserveFloat64(oldestAge, time.Since(oldest).Seconds())
		}
		return nil
	}, occupancy, capacity, oldestAge, watchers)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// countingSend wraps a watch's send to count the Progress and Resync
// events it sends, whichever path sends them. Events are counted as they
// are handed to send: a failed send ends the watch anyway.
func (m *hubMetrics) countingSend(ctx context.Context, send func(Event) error) func(Event) error {
	return func(event Event) error {
		switch event := event.(type) {
		case Progress:
			m.progress.Add(ctx, 1)
		case Resync:
			if opt, ok := m.resyncOpts[event.Reason]; ok {
				m.resyncs.Add(context.WithoutCancel(ctx), 1, opt)
			}
		}
		return send(event)
	}
}
