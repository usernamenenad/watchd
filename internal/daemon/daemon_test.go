package daemon

import (
	"context"
	"net"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"

	"github.com/usernamenenad/watchd/internal/telemetry/telemetrytest"
)

// TestGRPCMetricsRecordOnlyRegisteredMethods keeps rpc.method bounded: a
// client calling made-up methods must not create series, whether grpc-go or
// newGRPCServer's filter is what prevents it.
func TestGRPCMetricsRecordOnlyRegisteredMethods(t *testing.T) {
	metrics := telemetrytest.New(t)
	server := newGRPCServer(metrics.MeterProvider(), func(r grpc.ServiceRegistrar) {
		healthpb.RegisterHealthServer(r, health.NewServer())
	})
	listener := bufconn.Listen(1 << 20)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("Check: %v", err)
	}
	for _, method := range []string{"/made.up.Service/Method1", "/made.up.Service/Method2"} {
		if err := conn.Invoke(ctx, method, &healthpb.HealthCheckRequest{}, &healthpb.HealthCheckResponse{}); err == nil {
			t.Fatalf("%s succeeded", method)
		}
	}

	m, ok := metrics.Find(t, "rpc.server.call.duration")
	if !ok {
		t.Fatal("rpc.server.call.duration was not recorded")
	}
	points := m.Data.(metricdata.Histogram[float64]).DataPoints
	for _, point := range points {
		if method, _ := point.Attributes.Value("rpc.method"); method.AsString() != "grpc.health.v1.Health/Check" {
			t.Errorf("recorded rpc.method %q", method.AsString())
		}
	}
	if len(points) != 1 {
		t.Errorf("data points = %d, want 1 (the registered method)", len(points))
	}
}
