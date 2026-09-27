// Package telemetry builds watchd's OpenTelemetry metrics pipeline: one
// MeterProvider with a Prometheus pull exporter, an optional OTLP push
// exporter, and Go runtime metrics. See docs/observability.md for the
// conventions every watchd instrument follows.
package telemetry

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
)

// ErrInvalidEnvironment indicates OTEL_* environment variables watchd
// cannot honor, such as an unknown exporter or protocol.
var ErrInvalidEnvironment = errors.New("telemetry: invalid OpenTelemetry environment")

// SourceIDAttribute is the resource attribute naming the process's source.
const SourceIDAttribute = attribute.Key("watchd.source.id")

// errorLogInterval bounds how often export errors are logged, so an
// unreachable collector does not flood the log.
const errorLogInterval = 30 * time.Second

// Options configures Setup.
type Options struct {
	// SourceID becomes the watchd.source.id resource attribute.
	SourceID string
	// Prometheus enables the pull exporter, served by Telemetry.Handler.
	Prometheus bool
	// Logger receives export errors, rate-limited. It is optional.
	Logger *slog.Logger
}

// Telemetry is one process's metrics pipeline.
type Telemetry struct {
	provider *sdkmetric.MeterProvider
	handler  http.Handler
}

// Setup builds the metrics pipeline. OTLP push is enabled when
// OTEL_METRICS_EXPORTER lists "otlp"; the exporter then reads the standard
// OTEL_EXPORTER_OTLP_* variables, and OTEL_METRIC_EXPORT_INTERVAL sets how
// often it pushes. OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME override
// the default resource.
func Setup(ctx context.Context, opts Options) (*Telemetry, error) {
	otlp, err := otlpEnabled(os.Getenv("OTEL_METRICS_EXPORTER"))
	if err != nil {
		return nil, err
	}

	res, err := newResource(ctx, opts.SourceID)
	if err != nil {
		return nil, err
	}
	providerOptions := []sdkmetric.Option{sdkmetric.WithResource(res)}

	// One producer serves every reader; it holds only a sample buffer.
	producer := newRuntimeProducer()
	t := &Telemetry{}
	if opts.Prometheus {
		registry := prometheus.NewRegistry()
		exporter, err := otelprometheus.New(
			otelprometheus.WithRegisterer(registry),
			// Instrument names are already namespaced by component.
			otelprometheus.WithoutScopeInfo(),
			otelprometheus.WithProducer(producer),
		)
		if err != nil {
			return nil, fmt.Errorf("telemetry: prometheus exporter: %w", err)
		}
		providerOptions = append(providerOptions, sdkmetric.WithReader(exporter))
		t.handler = promhttp.HandlerFor(registry, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError})
	}
	if otlp {
		exporter, err := newOTLPExporter(ctx)
		if err != nil {
			return nil, err
		}
		providerOptions = append(providerOptions, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithProducer(producer))))
	}

	otel.SetErrorHandler(newErrorHandler(opts.Logger))
	t.provider = sdkmetric.NewMeterProvider(providerOptions...)
	if err := runtime.Start(runtime.WithMeterProvider(t.provider)); err != nil {
		_ = t.provider.Shutdown(ctx)
		return nil, fmt.Errorf("telemetry: runtime metrics: %w", err)
	}
	return t, nil
}

// MeterProvider returns the provider every watchd instrument is created from.
func (t *Telemetry) MeterProvider() metric.MeterProvider { return t.provider }

// Meter returns the meter for one instrumentation scope, by convention the
// instrumented package's import path.
func (t *Telemetry) Meter(name string) metric.Meter { return t.provider.Meter(name) }

// Handler serves the Prometheus exposition format, or is nil when the
// Prometheus exporter is disabled.
func (t *Telemetry) Handler() http.Handler { return t.handler }

// Shutdown pushes any pending OTLP export and stops every reader. Callers
// bound it with ctx; an unreachable collector only costs that budget.
func (t *Telemetry) Shutdown(ctx context.Context) error { return t.provider.Shutdown(ctx) }

// MeterOrNoop returns meter, or a no-op meter when it is nil, so components
// given no meter record nothing and pay almost nothing.
func MeterOrNoop(meter metric.Meter) metric.Meter {
	if meter == nil {
		return noop.Meter{}
	}
	return meter
}

// otlpEnabled parses OTEL_METRICS_EXPORTER. Prometheus is served on the ops
// endpoint rather than a separate server, so "prometheus" only confirms
// the default.
func otlpEnabled(value string) (bool, error) {
	otlp := false
	for name := range strings.SplitSeq(value, ",") {
		switch strings.TrimSpace(name) {
		case "", "prometheus", "none":
		case "otlp":
			otlp = true
		default:
			return false, fmt.Errorf("%w: OTEL_METRICS_EXPORTER %q: use otlp, prometheus, or none", ErrInvalidEnvironment, name)
		}
	}
	return otlp, nil
}

func newOTLPExporter(ctx context.Context) (sdkmetric.Exporter, error) {
	protocol := os.Getenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL")
	if protocol == "" {
		protocol = os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")
	}
	var (
		exporter sdkmetric.Exporter
		err      error
	)
	switch protocol {
	case "", "http/protobuf":
		exporter, err = otlpmetrichttp.New(ctx)
	case "grpc":
		exporter, err = otlpmetricgrpc.New(ctx)
	default:
		return nil, fmt.Errorf("%w: OTLP protocol %q: use grpc or http/protobuf", ErrInvalidEnvironment, protocol)
	}
	if err != nil {
		return nil, fmt.Errorf("telemetry: OTLP exporter: %w", err)
	}
	return exporter, nil
}

// newResource describes this process. OTEL_RESOURCE_ATTRIBUTES and
// OTEL_SERVICE_NAME take precedence over the defaults.
func newResource(ctx context.Context, sourceID string) (*resource.Resource, error) {
	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithAttributes(
			semconv.ServiceName("watchd"),
			semconv.ServiceVersion(version()),
			semconv.ServiceInstanceID(instanceID()),
			SourceIDAttribute.String(sourceID),
		),
		// Later detectors win, so the environment overrides the defaults.
		resource.WithFromEnv(),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidEnvironment, err)
	}
	return res, nil
}

// version is the main module's version as stamped by the Go toolchain: a
// tag, or a pseudo-version for an untagged commit.
func version() string {
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return "dev"
}

// instanceID is a random UUIDv4, so every process start is distinct.
func instanceID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// errorHandler logs OpenTelemetry errors at most once per errorLogInterval.
// An export failure never reaches CDC or serving: it is only logged.
type errorHandler struct {
	logger *slog.Logger

	mu         sync.Mutex
	last       time.Time
	suppressed int
}

func newErrorHandler(logger *slog.Logger) *errorHandler {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &errorHandler{logger: logger}
}

func (h *errorHandler) Handle(err error) {
	h.mu.Lock()
	now := time.Now()
	if !h.last.IsZero() && now.Sub(h.last) < errorLogInterval {
		h.suppressed++
		h.mu.Unlock()
		return
	}
	suppressed := h.suppressed
	h.last, h.suppressed = now, 0
	h.mu.Unlock()
	h.logger.Warn("telemetry error", "error", err, "suppressed", suppressed)
}
