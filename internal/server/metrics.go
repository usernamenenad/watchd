package server

import (
	"errors"

	"go.opentelemetry.io/otel/metric"

	"github.com/usernamenenad/watchd/internal/telemetry"
)

// serverMetrics are the server's instruments, created once in New. With no
// meter configured, every instrument is a no-op.
type serverMetrics struct {
	activeStreams metric.Int64UpDownCounter
	sendDuration  metric.Float64Histogram
	snapshotBytes metric.Int64Counter
	commitToSend  metric.Float64Histogram
}

func newServerMetrics(meter metric.Meter) (*serverMetrics, error) {
	meter = telemetry.MeterOrNoop(meter)
	m := &serverMetrics{}
	var err1, err2, err3, err4 error
	m.activeStreams, err1 = meter.Int64UpDownCounter("watchd.server.streams.active", metric.WithUnit("{stream}"),
		metric.WithDescription("Watch streams in progress."))
	m.sendDuration, err2 = meter.Float64Histogram("watchd.server.send.duration", metric.WithUnit("s"),
		metric.WithDescription("Time one stream message took to send. Long sends mean a client reading slowly: gRPC flow control holds the send until it catches up."),
		metric.WithExplicitBucketBoundaries(0.00001, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10))
	m.snapshotBytes, err3 = meter.Int64Counter("watchd.server.snapshot.bytes", metric.WithUnit("By"),
		metric.WithDescription("Encoded bytes of snapshot rows sent to clients."))
	m.commitToSend, err4 = meter.Float64Histogram("watchd.server.batch.commit_to_send", metric.WithUnit("s"),
		metric.WithDescription("Time from a transaction's commit in PostgreSQL to its batch being sent to a client. Subject to clock skew between PostgreSQL and watchd."),
		metric.WithExplicitBucketBoundaries(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300))
	if err := errors.Join(err1, err2, err3, err4); err != nil {
		return nil, err
	}
	return m, nil
}
