//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/usernamenenad/watchd/internal/cdc"
	"github.com/usernamenenad/watchd/internal/daemon"
	"github.com/usernamenenad/watchd/internal/telemetry/telemetrytest"
)

// TestWatchdRetentionBudgetResyncsEveryWatcher drives watchd past its
// retained-WAL budget: it drops its slot, tells every client to resync, and
// exits with a source error. The next start builds a new slot, and clients
// rebuild on it.
func TestWatchdRetentionBudgetResyncsEveryWatcher(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	app := connectApplication(t, ctx)
	defer app.Close(context.Background())
	tenant := "00000000-0000-0000-0000-0000000000f1"
	setPermission(t, ctx, app, tenant, 1, "editor")

	slotName := fmt.Sprintf("watchd_budget_e2e_%d", time.Now().UnixNano())
	config := func(budget int64) daemon.Config {
		cfg, err := daemon.ParseJSONConfig(stringsReader(fmt.Sprintf(`{
			"source_id": "e2e-budget",
			"slot_name": %q,
			"publication_name": "watchd_publication",
			"listen_address": "127.0.0.1:0",
			"shutdown_timeout": "3s",
			"retention": {"max_retained_wal_bytes": %d, "sample_interval": "20ms"},
			"projections": {
				"tenant_permissions": {
					"schema": "public",
					"table": "tenant_permissions_projection",
					"scope_column": "tenant_id",
					"primary_key": ["tenant_id", "user_id"]
				}
			}
		}`, slotName, budget)), envOrDefault("WATCHD_TEST_REPLICATION_URL", defaultReplicationURL))
		if err != nil {
			t.Fatalf("ParseJSONConfig: %v", err)
		}
		return cfg
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		dropSlot(t, cleanupCtx, slotName)
	})

	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := probe.Addr().String()
	_ = probe.Close()

	// A budget far below what one burst of WAL retains.
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- daemon.Serve(ctx, config(1<<20), daemon.Listeners{API: listener}, nil) }()

	metrics := telemetrytest.New(t)
	client := startClient(t, ctx, address, tenant, metrics.Meter())
	client.waitFresh(t, ctx, app)
	if !slotExists(t, ctx, app, slotName) {
		t.Fatal("the client's snapshot did not create the slot")
	}

	var burst string
	if err := app.QueryRow(ctx, "SELECT pg_logical_emit_message(false, 'watchd-test', repeat('x', 8 * 1024 * 1024))::text").Scan(&burst); err != nil {
		t.Fatalf("emit WAL: %v", err)
	}
	select {
	case err := <-served:
		if !errors.Is(err, daemon.ErrSource) || !errors.Is(err, cdc.ErrRetainedWALBudgetExceeded) {
			t.Fatalf("watchd stopped with %v, want a source error for the retention budget", err)
		}
	case <-ctx.Done():
		t.Fatal("watchd did not stop at its retention budget")
	}
	if slotExists(t, ctx, app, slotName) {
		t.Fatal("the slot survived the terminal action")
	}
	client.waitStale(t, ctx)
	stopped := attribute.NewSet(attribute.String("projection", "tenant_permissions"), attribute.String("reason", "source_stopped"))
	if got, _ := metrics.Value(t, "watchd.sdk.resyncs", stopped); got < 1 {
		t.Fatalf("the client was not told to resync (resyncs{source_stopped} = %v)", got)
	}

	// The operator restarts watchd: a new slot, and the client rebuilds.
	stop, _ := startWatchd(t, ctx, config(1<<30), address)
	defer stop()
	setPermission(t, ctx, app, tenant, 2, "written-after-rebuild")
	client.waitFresh(t, ctx, app)
	if !slotExists(t, ctx, app, slotName) {
		t.Fatal("the restart did not build a new slot")
	}
}
