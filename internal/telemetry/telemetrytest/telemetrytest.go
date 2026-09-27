// Package telemetrytest collects metrics in-process so tests can assert
// that instruments move and that they carry only allowlisted attributes.
package telemetrytest

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/usernamenenad/watchd/internal/telemetry"
)

// Reader is an in-process metrics pipeline for one test.
type Reader struct {
	reader   *sdkmetric.ManualReader
	provider *sdkmetric.MeterProvider
}

// New returns a Reader that is shut down when t ends.
func New(t testing.TB) *Reader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return &Reader{reader: reader, provider: provider}
}

// MeterProvider returns the provider instruments under test are created from.
func (r *Reader) MeterProvider() metric.MeterProvider { return r.provider }

// Meter returns a meter whose measurements r collects.
func (r *Reader) Meter() metric.Meter { return r.provider.Meter("telemetrytest") }

// Collect returns every metric recorded so far.
func (r *Reader) Collect(t testing.TB) []metricdata.Metrics {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := r.reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	var metrics []metricdata.Metrics
	for _, scope := range data.ScopeMetrics {
		metrics = append(metrics, scope.Metrics...)
	}
	return metrics
}

// Find returns the metric called name, if it was recorded.
func (r *Reader) Find(t testing.TB, name string) (metricdata.Metrics, bool) {
	t.Helper()
	for _, m := range r.Collect(t) {
		if m.Name == name {
			return m, true
		}
	}
	return metricdata.Metrics{}, false
}

// Value returns the value of a sum or gauge data point with exactly attrs.
func (r *Reader) Value(t testing.TB, name string, attrs attribute.Set) (float64, bool) {
	t.Helper()
	m, ok := r.Find(t, name)
	if !ok {
		return 0, false
	}
	switch data := m.Data.(type) {
	case metricdata.Sum[int64]:
		return pointValue(data.DataPoints, attrs)
	case metricdata.Sum[float64]:
		return pointValue(data.DataPoints, attrs)
	case metricdata.Gauge[int64]:
		return pointValue(data.DataPoints, attrs)
	case metricdata.Gauge[float64]:
		return pointValue(data.DataPoints, attrs)
	}
	t.Fatalf("metric %s is %T, not a sum or gauge", name, m.Data)
	return 0, false
}

// HistogramCount returns how many measurements a histogram with exactly
// attrs recorded.
func (r *Reader) HistogramCount(t testing.TB, name string, attrs attribute.Set) uint64 {
	t.Helper()
	m, ok := r.Find(t, name)
	if !ok {
		return 0
	}
	switch data := m.Data.(type) {
	case metricdata.Histogram[int64]:
		return histogramCount(data.DataPoints, attrs)
	case metricdata.Histogram[float64]:
		return histogramCount(data.DataPoints, attrs)
	}
	t.Fatalf("metric %s is %T, not a histogram", name, m.Data)
	return 0
}

// AssertAllowedAttributes fails t if any watchd metric carries an attribute
// key outside the allowlist in docs/observability.md.
func (r *Reader) AssertAllowedAttributes(t testing.TB) {
	t.Helper()
	for _, m := range r.Collect(t) {
		if !telemetry.IsWatchdMetric(m.Name) {
			continue
		}
		for _, set := range attributeSets(m.Data) {
			for _, kv := range set.ToSlice() {
				if !telemetry.Allowed(kv.Key) {
					t.Errorf("metric %s carries attribute %q, which is not in the allowlist", m.Name, kv.Key)
				}
			}
		}
	}
}

func pointValue[N int64 | float64](points []metricdata.DataPoint[N], attrs attribute.Set) (float64, bool) {
	for _, point := range points {
		if point.Attributes.Equals(&attrs) {
			return float64(point.Value), true
		}
	}
	return 0, false
}

func histogramCount[N int64 | float64](points []metricdata.HistogramDataPoint[N], attrs attribute.Set) uint64 {
	for _, point := range points {
		if point.Attributes.Equals(&attrs) {
			return point.Count
		}
	}
	return 0
}

func attributeSets(data metricdata.Aggregation) []attribute.Set {
	var sets []attribute.Set
	switch data := data.(type) {
	case metricdata.Sum[int64]:
		for _, p := range data.DataPoints {
			sets = append(sets, p.Attributes)
		}
	case metricdata.Sum[float64]:
		for _, p := range data.DataPoints {
			sets = append(sets, p.Attributes)
		}
	case metricdata.Gauge[int64]:
		for _, p := range data.DataPoints {
			sets = append(sets, p.Attributes)
		}
	case metricdata.Gauge[float64]:
		for _, p := range data.DataPoints {
			sets = append(sets, p.Attributes)
		}
	case metricdata.Histogram[int64]:
		for _, p := range data.DataPoints {
			sets = append(sets, p.Attributes)
		}
	case metricdata.Histogram[float64]:
		for _, p := range data.DataPoints {
			sets = append(sets, p.Attributes)
		}
	}
	return sets
}
