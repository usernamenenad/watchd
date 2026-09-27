package telemetry

import (
	"context"
	"math"
	"runtime/metrics"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// runtimeLatencyBounds are the bucket upper bounds, in seconds, for the
// runtime latency histograms: 1µs to 1s. The runtime keeps 160-odd
// buckets per histogram; re-bucketing keeps every scrape small while
// resolving what matters when optimizing - microsecond scheduling delays
// and sub-millisecond GC pauses.
var runtimeLatencyBounds = []float64{
	1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 2.5e-4, 5e-4, 1e-3, 2.5e-3, 5e-3, 1e-2, 1e-1, 1,
}

// runtimeHistograms maps runtime/metrics histograms to the instruments
// runtimeProducer emits.
var runtimeHistograms = []struct {
	runtimeName, name, description string
}{
	{"/sched/latencies:seconds", "go.schedule.duration", "The time goroutines have spent in the scheduler in a runnable state before actually running."},
	{"/sched/pauses/total/gc:seconds", "go.gc.pause.duration", "The duration of stop-the-world pauses caused by the garbage collector."},
}

// runtimeProducer emits the Go runtime's latency histograms, which the
// runtime instrumentation's instruments do not cover: scheduling latency
// and GC stop-the-world pauses.
type runtimeProducer struct {
	start time.Time

	mu      sync.Mutex
	samples []metrics.Sample
}

var _ sdkmetric.Producer = (*runtimeProducer)(nil)

func newRuntimeProducer() *runtimeProducer {
	samples := make([]metrics.Sample, len(runtimeHistograms))
	for i, h := range runtimeHistograms {
		samples[i].Name = h.runtimeName
	}
	return &runtimeProducer{start: time.Now(), samples: samples}
}

// Produce implements sdkmetric.Producer.
func (p *runtimeProducer) Produce(context.Context) ([]metricdata.ScopeMetrics, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	metrics.Read(p.samples)
	now := time.Now()

	out := make([]metricdata.Metrics, 0, len(p.samples))
	for i, sample := range p.samples {
		if sample.Value.Kind() != metrics.KindFloat64Histogram {
			continue // not supported by this Go version
		}
		out = append(out, metricdata.Metrics{
			Name:        runtimeHistograms[i].name,
			Description: runtimeHistograms[i].description,
			Unit:        "s",
			Data: metricdata.Histogram[float64]{
				Temporality: metricdata.CumulativeTemporality,
				DataPoints:  []metricdata.HistogramDataPoint[float64]{rebucket(sample.Value.Float64Histogram(), runtimeLatencyBounds, p.start, now)},
			},
		})
	}
	return []metricdata.ScopeMetrics{{
		Scope:   instrumentation.Scope{Name: "github.com/usernamenenad/watchd/internal/telemetry"},
		Metrics: out,
	}}, nil
}

// rebucket folds a runtime histogram into bounds. Each runtime bucket
// [lo, hi) counts toward the first bound at or above hi, so no observation
// is reported below its real value. The sum is estimated from each runtime
// bucket's lower edge, as the runtime records no sum.
func rebucket(h *metrics.Float64Histogram, bounds []float64, start, now time.Time) metricdata.HistogramDataPoint[float64] {
	counts := make([]uint64, len(bounds)+1)
	var count uint64
	var sum float64
	target := 0
	for i, c := range h.Counts {
		hi := h.Buckets[i+1]
		for target < len(bounds) && bounds[target] < hi {
			target++
		}
		if c == 0 {
			continue
		}
		counts[target] += c
		count += c
		if lo := h.Buckets[i]; !math.IsInf(lo, -1) && lo > 0 {
			sum += lo * float64(c)
		}
	}
	return metricdata.HistogramDataPoint[float64]{
		Attributes:   *attribute.EmptySet(),
		StartTime:    start,
		Time:         now,
		Count:        count,
		Sum:          sum,
		Bounds:       bounds,
		BucketCounts: counts,
	}
}
