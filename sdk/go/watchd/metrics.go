package watchd

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc/status"

	watchv1 "github.com/usernamenenad/watchd/api/watch/v1"
)

// latencyBounds cover the SDK's latencies, in seconds: 100µs to 5min.
var latencyBounds = []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300}

var resyncReasonNames = map[watchv1.ResyncReason]string{
	watchv1.ResyncReason_RESYNC_REASON_CURSOR_UNAVAILABLE:     "cursor_unavailable",
	watchv1.ResyncReason_RESYNC_REASON_SLOW_WATCHER:           "slow_watcher",
	watchv1.ResyncReason_RESYNC_REASON_SNAPSHOT_WINDOW_CLOSED: "snapshot_window_closed",
	watchv1.ResyncReason_RESYNC_REASON_SOURCE_STOPPED:         "source_stopped",
	watchv1.ResyncReason_RESYNC_REASON_UNSPECIFIED:            "unspecified",
}

// syncMetrics are one Sync call's instruments. Every measurement carries
// only the projection name - never the scope, which is typically a tenant -
// and bounded enums, so series do not grow with scopes. Several Sync calls
// for the same projection share their series.
type syncMetrics struct {
	apply         metric.Float64Histogram
	install       metric.Float64Histogram
	commitToApply metric.Float64Histogram
	timeToFresh   metric.Float64Histogram
	syncs         metric.Int64UpDownCounter
	fresh         metric.Int64UpDownCounter
	resyncs       metric.Int64Counter
	streamErrors  metric.Int64Counter

	record     metric.RecordOption
	add        metric.AddOption
	projection attribute.KeyValue
	resyncOpts map[watchv1.ResyncReason]metric.AddOption
}

func newSyncMetrics(meter metric.Meter, projection string) (*syncMetrics, error) {
	if meter == nil {
		meter = noop.Meter{}
	}
	kv := attribute.String("projection", projection)
	set := attribute.NewSet(kv)
	m := &syncMetrics{
		record:     metric.WithAttributeSet(set),
		add:        metric.WithAttributeSet(set),
		projection: kv,
		resyncOpts: map[watchv1.ResyncReason]metric.AddOption{},
	}
	for reason, name := range resyncReasonNames {
		m.resyncOpts[reason] = metric.WithAttributeSet(attribute.NewSet(kv, attribute.String("reason", name)))
	}

	var errs []error
	check := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	var err error
	m.apply, err = meter.Float64Histogram("watchd.sdk.apply.duration", metric.WithUnit("s"),
		metric.WithDescription("Time Store.Apply took for one batch."),
		metric.WithExplicitBucketBoundaries(latencyBounds...))
	check(err)
	m.install, err = meter.Float64Histogram("watchd.sdk.snapshot.install.duration", metric.WithUnit("s"),
		metric.WithDescription("Time from SnapshotBegin until the snapshot was committed to the store."),
		metric.WithExplicitBucketBoundaries(latencyBounds...))
	check(err)
	m.commitToApply, err = meter.Float64Histogram("watchd.sdk.commit_to_apply", metric.WithUnit("s"),
		metric.WithDescription("Time from a transaction's commit in PostgreSQL until its batch was applied to the store: end-to-end latency. Subject to clock skew between PostgreSQL and the client."),
		metric.WithExplicitBucketBoundaries(latencyBounds...))
	check(err)
	m.timeToFresh, err = meter.Float64Histogram("watchd.sdk.time_to_fresh", metric.WithUnit("s"),
		metric.WithDescription("Time from opening a watch stream, after a start, a reconnect, or a resync, until the projection was confirmed fresh."),
		metric.WithExplicitBucketBoundaries(latencyBounds...))
	check(err)
	m.syncs, err = meter.Int64UpDownCounter("watchd.sdk.syncs", metric.WithUnit("{sync}"),
		metric.WithDescription("Sync calls running."))
	check(err)
	m.fresh, err = meter.Int64UpDownCounter("watchd.sdk.fresh", metric.WithUnit("{sync}"),
		metric.WithDescription("Sync calls whose projection is currently fresh. watchd.sdk.syncs − watchd.sdk.fresh are stale."))
	check(err)
	m.resyncs, err = meter.Int64Counter("watchd.sdk.resyncs", metric.WithUnit("{event}"),
		metric.WithDescription("Resyncs the server asked for, by reason."))
	check(err)
	m.streamErrors, err = meter.Int64Counter("watchd.sdk.stream.errors", metric.WithUnit("{error}"),
		metric.WithDescription("Watch streams that ended in an error, by error_class (the gRPC status code)."))
	check(err)
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return m, nil
}

func (m *syncMetrics) resync(ctx context.Context, reason watchv1.ResyncReason) {
	opt, ok := m.resyncOpts[reason]
	if !ok {
		opt = m.resyncOpts[watchv1.ResyncReason_RESYNC_REASON_UNSPECIFIED]
	}
	m.resyncs.Add(ctx, 1, opt)
}

// streamError counts a stream that ended in err, classified by its gRPC
// status code: a closed set.
func (m *syncMetrics) streamError(ctx context.Context, err error) {
	class := strings.ToLower(status.Code(err).String())
	m.streamErrors.Add(ctx, 1, metric.WithAttributes(m.projection, attribute.String("error_class", class)))
}

func secondsSince(t time.Time) float64 { return time.Since(t).Seconds() }
