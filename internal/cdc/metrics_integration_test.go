//go:build integration

package cdc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/usernamenenad/watchd/internal/telemetry"
	"github.com/usernamenenad/watchd/internal/telemetry/telemetrytest"
)

// TestSourceMetricsAgainstPostgres checks the instruments that need a real
// source: both snapshot modes, a streamed transaction's latency, and the
// retained-WAL sample read from pg_replication_slots.
func TestSourceMetricsAgainstPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	app := connectBootstrapTest(t, ctx, testDatabaseURL)
	defer app.Close(ctx)
	if _, err := app.Exec(ctx, "DELETE FROM tenant_permissions_projection"); err != nil {
		t.Fatalf("clear projection: %v", err)
	}
	firstTenant := "00000000-0000-0000-0000-000000000090"
	secondTenant := "00000000-0000-0000-0000-000000000091"
	for _, tenant := range []string{firstTenant, secondTenant} {
		if err := insertBoundaryUser(ctx, app, tenant, "00000000-0000-0000-0000-000000000901"); err != nil {
			t.Fatalf("seed projection: %v", err)
		}
	}

	metrics := telemetrytest.New(t)
	slotName := fmt.Sprintf("watchd_metrics_%d", time.Now().UnixNano())
	transactions := make(chan Transaction, 16)
	source, stop := startTestSource(t, ctx, slotName, func(_ context.Context, transaction Transaction) error {
		transactions <- transaction
		return nil
	}, func(config *ReaderConfig) {
		config.Meter = metrics.Meter()
		config.RetentionSampleInterval = 50 * time.Millisecond
	})
	defer stop()
	registerBootstrapCleanup(t, source.reader, slotName)

	snapshotSourceScope(t, ctx, source, firstTenant)  // Bootstrap
	snapshotSourceScope(t, ctx, source, secondTenant) // Snapshot
	waitForStreamedUser(t, ctx, app, transactions, firstTenant, "00000000-0000-0000-0000-000000000902")

	for _, mode := range []string{modeBootstrap, modeSnapshot} {
		attrs := telemetry.Attributes(telemetry.KeyMode.String(mode))
		if got, _ := metrics.Value(t, "watchd.cdc.snapshot.rows", attrs); got != 1 {
			t.Errorf("snapshot.rows{%s} = %v, want 1", mode, got)
		}
		if got := metrics.HistogramCount(t, "watchd.cdc.snapshot.duration", attrs); got != 1 {
			t.Errorf("snapshot.duration{%s} count = %d, want 1", mode, got)
		}
		if got := metrics.HistogramCount(t, "watchd.cdc.snapshot.page.duration", attrs); got < 1 {
			t.Errorf("snapshot.page.duration{%s} count = %d, want at least 1", mode, got)
		}
	}
	if got := metrics.HistogramCount(t, "watchd.cdc.commit_to_accept", noAttributes()); got < 1 {
		t.Errorf("commit_to_accept count = %d, want at least 1", got)
	}
	streaming := telemetry.Attributes(telemetry.KeyState.String(stateStreaming))
	if got, _ := metrics.Value(t, "watchd.cdc.stream.state", streaming); got != 1 {
		t.Errorf("stream.state{streaming} = %v, want 1", got)
	}

	// The retention sample runs on its own interval.
	for {
		if _, ok := metrics.Value(t, "watchd.cdc.slot.retained_wal", noAttributes()); ok {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("watchd.cdc.slot.retained_wal was never sampled")
		case <-time.After(20 * time.Millisecond):
		}
	}
	if got, _ := metrics.Value(t, "watchd.cdc.slot.retained_wal", noAttributes()); got < 0 {
		t.Errorf("retained_wal = %v, want >= 0", got)
	}
	reserved := telemetry.Attributes(telemetry.KeyState.String("reserved"))
	extended := telemetry.Attributes(telemetry.KeyState.String("extended"))
	r, _ := metrics.Value(t, "watchd.cdc.slot.wal_status", reserved)
	e, _ := metrics.Value(t, "watchd.cdc.slot.wal_status", extended)
	if r+e != 1 {
		t.Errorf("wal_status reserved=%v extended=%v, want one of them", r, e)
	}
	if got, _ := metrics.Value(t, "watchd.cdc.retention.budget", noAttributes()); got <= 0 {
		t.Errorf("retention.budget = %v, want > 0", got)
	}
	metrics.AssertAllowedAttributes(t)
}
