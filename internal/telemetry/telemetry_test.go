package telemetry

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"runtime/metrics"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"
)

func scrape(t *testing.T, handler http.Handler) string {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("scrape = %d", recorder.Code)
	}
	return recorder.Body.String()
}

func TestPrometheusServesRuntimeAndWatchdMetrics(t *testing.T) {
	ctx := context.Background()
	tel, err := Setup(ctx, Options{SourceID: "test-source", Prometheus: true})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	defer tel.Shutdown(ctx)

	gauge, err := tel.Meter("test").Int64ObservableGauge("watchd.test.sample")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tel.Meter("test").RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(gauge, 7)
		return nil
	}, gauge); err != nil {
		t.Fatal(err)
	}

	body := scrape(t, tel.Handler())
	for _, want := range []string{"watchd_test_sample 7", "go_goroutine_count", "go_memory_used_bytes", `go_schedule_duration_seconds_bucket{le="1e-06"}`, `go_gc_pause_duration_seconds_bucket{le="+Inf"}`, `service_name="watchd"`, `watchd_source_id="test-source"`} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape does not contain %q", want)
		}
	}
}

func TestPrometheusDisabledHasNoHandler(t *testing.T) {
	tel, err := Setup(context.Background(), Options{SourceID: "s"})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	defer tel.Shutdown(context.Background())
	if tel.Handler() != nil {
		t.Fatal("Handler without the Prometheus exporter must be nil")
	}
}

// collectorStub is an OTLP/HTTP metrics receiver.
type collectorStub struct {
	mu       sync.Mutex
	requests []*collectormetrics.ExportMetricsServiceRequest
}

func (c *collectorStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	request := &collectormetrics.ExportMetricsServiceRequest{}
	if r.URL.Path != "/v1/metrics" || proto.Unmarshal(body, request) != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	c.requests = append(c.requests, request)
	c.mu.Unlock()
	response, _ := proto.Marshal(&collectormetrics.ExportMetricsServiceResponse{})
	w.Header().Set("Content-Type", "application/x-protobuf")
	_, _ = w.Write(response)
}

func TestOTLPPushesOnShutdown(t *testing.T) {
	stub := &collectorStub{}
	server := httptest.NewServer(stub)
	defer server.Close()
	t.Setenv("OTEL_METRICS_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL)
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment.name=test")

	ctx := context.Background()
	tel, err := Setup(ctx, Options{SourceID: "otlp-source", Prometheus: true})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	counter, err := tel.Meter("test").Int64Counter("watchd.test.pushed")
	if err != nil {
		t.Fatal(err)
	}
	counter.Add(ctx, 3)
	// Both exporters run at once.
	if !strings.Contains(scrape(t, tel.Handler()), "watchd_test_pushed_total 3") {
		t.Error("Prometheus scrape is missing the counter while OTLP is enabled")
	}
	if err := tel.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.requests) == 0 {
		t.Fatal("the collector received nothing")
	}
	resource := map[string]string{}
	found := false
	for _, request := range stub.requests {
		for _, rm := range request.GetResourceMetrics() {
			for _, kv := range rm.GetResource().GetAttributes() {
				resource[kv.GetKey()] = kv.GetValue().GetStringValue()
			}
			for _, sm := range rm.GetScopeMetrics() {
				for _, m := range sm.GetMetrics() {
					if m.GetName() == "watchd.test.pushed" {
						found = true
					}
				}
			}
		}
	}
	if !found {
		t.Error("the collector did not receive watchd.test.pushed")
	}
	for key, want := range map[string]string{"service.name": "watchd", "watchd.source.id": "otlp-source", "deployment.environment.name": "test"} {
		if resource[key] != want {
			t.Errorf("resource %s = %q, want %q", key, resource[key], want)
		}
	}
	if resource["service.instance.id"] == "" || resource["service.version"] == "" {
		t.Errorf("resource lacks service.instance.id or service.version: %v", resource)
	}
}

func TestInvalidEnvironment(t *testing.T) {
	for name, env := range map[string][2]string{
		"exporter": {"OTEL_METRICS_EXPORTER", "zipkin"},
		"protocol": {"OTEL_EXPORTER_OTLP_PROTOCOL", "carrier-pigeon"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("OTEL_METRICS_EXPORTER", "otlp")
			t.Setenv(env[0], env[1])
			if _, err := Setup(context.Background(), Options{SourceID: "s"}); !errors.Is(err, ErrInvalidEnvironment) {
				t.Fatalf("Setup = %v, want ErrInvalidEnvironment", err)
			}
		})
	}
}

func TestAttributesRejectsUnknownKeys(t *testing.T) {
	set := Attributes(KeyReason.String("slow_watcher"), KeySource.String("s"))
	if set.Len() != 2 {
		t.Fatalf("set has %d attributes, want 2", set.Len())
	}
	defer func() {
		if recover() == nil {
			t.Fatal("Attributes accepted a key outside the allowlist")
		}
	}()
	Attributes(attribute.String("tenant", "t1"))
}

func TestErrorHandlerIsRateLimited(t *testing.T) {
	var logs bytes.Buffer
	handler := newErrorHandler(slog.New(slog.NewTextHandler(&logs, nil)))
	for range 5 {
		handler.Handle(errors.New("collector unreachable"))
	}
	if n := strings.Count(logs.String(), "telemetry error"); n != 1 {
		t.Fatalf("logged %d times, want 1:\n%s", n, logs.String())
	}
}

func TestRebucketNeverReportsBelowTheRealValue(t *testing.T) {
	h := &metrics.Float64Histogram{
		Buckets: []float64{math.Inf(-1), 0, 2e-6, 3e-4, 2, math.Inf(1)},
		Counts:  []uint64{0, 4, 5, 6, 7},
	}
	point := rebucket(h, []float64{1e-6, 1e-5, 1e-3, 1}, time.Time{}, time.Time{})
	// [0,2µs) -> le 10µs; [2µs,300µs) -> le 1ms; [300µs,2s) -> +Inf; [2s,+Inf) -> +Inf.
	want := []uint64{0, 4, 5, 0, 13}
	if !slices.Equal(point.BucketCounts, want) || point.Count != 22 {
		t.Fatalf("buckets = %v (count %d), want %v (count 22)", point.BucketCounts, point.Count, want)
	}
}
