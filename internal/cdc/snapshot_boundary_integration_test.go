//go:build integration

package cdc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The tests in this file pin the snapshot/stream boundary itself, rather
// than eventual convergence. Each one commits a write at a precise moment
// relative to the snapshot, then rebuilds the projection from the snapshot
// rows plus only the stream transactions the boundary says the snapshot does
// not contain. A write that lands in neither half is a gap: the rebuilt
// projection disagrees with PostgreSQL.

// TestBootstrapBoundaryHasNoGapForCommitAfterSnapshotIsFixed commits a write
// after the read's MVCC snapshot is fixed, but before Bootstrap has finished
// establishing its boundary. The write is invisible to the snapshot rows, so
// the stream must deliver it.
func TestBootstrapBoundaryHasNoGapForCommitAfterSnapshotIsFixed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	app := connectBootstrapTest(t, ctx, testDatabaseURL)
	defer app.Close(ctx)
	if _, err := app.Exec(ctx, "DELETE FROM tenant_permissions_projection"); err != nil {
		t.Fatalf("clear projection: %v", err)
	}

	tenantID := "00000000-0000-0000-0000-000000000040"
	racingUser := "00000000-0000-0000-0000-000000000401"
	sentinelUser := "00000000-0000-0000-0000-000000000402"

	slotName := fmt.Sprintf("watchd_boundary_bootstrap_%d", time.Now().UnixNano())
	transactions := make(chan Transaction, 16)
	reader := newBootstrapTestReader(t, slotName, func(_ context.Context, transaction Transaction) error {
		transactions <- transaction
		return nil
	})
	registerBootstrapCleanup(t, reader, slotName)
	reader.afterSnapshotFixed = func(ctx context.Context) error {
		return insertBoundaryUser(ctx, app, tenantID, racingUser)
	}

	var rows []map[string]any
	snapshot, err := reader.Bootstrap(ctx, bootstrapTestProjection(), Scope{Value: tenantID}, collectSnapshotRows(&rows))
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	reader.afterSnapshotFixed = nil

	runDone := runBoundaryReader(t, ctx, reader)
	projection := rebuildAcrossBoundary(t, ctx, app, snapshot, rows, transactions, runDone, tenantID, sentinelUser)
	if !projectionHasUser(projection, racingUser) {
		t.Fatalf("write committed after the snapshot was fixed is in neither the snapshot nor the stream after its boundary")
	}
}

// TestSnapshotBoundaryHasNoGapForCommitAfterSnapshotIsFixed is the same race
// for a later scope, taken with Snapshot while Run is already streaming.
func TestSnapshotBoundaryHasNoGapForCommitAfterSnapshotIsFixed(t *testing.T) {
	testSnapshotBoundary(t, "after_fixed", func(reader *Reader, write func(context.Context) error) {
		reader.afterSnapshotFixed = write
	})
}

// TestSnapshotBoundaryHasNoGapForCommitBeforeRowsAreRead commits the write
// after the snapshot is fixed and its boundary recorded, just before the
// scoped rows are read.
func TestSnapshotBoundaryHasNoGapForCommitBeforeRowsAreRead(t *testing.T) {
	testSnapshotBoundary(t, "before_read", func(reader *Reader, write func(context.Context) error) {
		reader.beforeSnapshotRead = write
	})
}

func testSnapshotBoundary(t *testing.T, name string, installHook func(*Reader, func(context.Context) error)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	app := connectBootstrapTest(t, ctx, testDatabaseURL)
	defer app.Close(ctx)
	if _, err := app.Exec(ctx, "DELETE FROM tenant_permissions_projection"); err != nil {
		t.Fatalf("clear projection: %v", err)
	}

	firstTenant := "00000000-0000-0000-0000-000000000050"
	tenantID := "00000000-0000-0000-0000-000000000060"
	seedUser := "00000000-0000-0000-0000-000000000601"
	racingUser := "00000000-0000-0000-0000-000000000602"
	sentinelUser := "00000000-0000-0000-0000-000000000603"

	slotName := fmt.Sprintf("watchd_boundary_%s_%d", name, time.Now().UnixNano())
	transactions := make(chan Transaction, 16)
	reader := newBootstrapTestReader(t, slotName, func(_ context.Context, transaction Transaction) error {
		transactions <- transaction
		return nil
	})
	registerBootstrapCleanup(t, reader, slotName)

	if _, err := reader.Bootstrap(ctx, bootstrapTestProjection(), Scope{Value: firstTenant}, func(context.Context, []map[string]any) error { return nil }); err != nil {
		t.Fatalf("bootstrap first scope: %v", err)
	}
	runDone := runBoundaryReader(t, ctx, reader)

	// Committed well before the snapshot: it belongs in the snapshot rows,
	// and the stream delivers it to the shared sink as well.
	if err := insertBoundaryUser(ctx, app, tenantID, seedUser); err != nil {
		t.Fatalf("seed scope: %v", err)
	}

	installHook(reader, func(ctx context.Context) error {
		return insertBoundaryUser(ctx, app, tenantID, racingUser)
	})
	var rows []map[string]any
	snapshot, err := reader.Snapshot(ctx, bootstrapTestProjection(), Scope{Value: tenantID}, collectSnapshotRows(&rows))
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	installHook(reader, nil)

	projection := rebuildAcrossBoundary(t, ctx, app, snapshot, rows, transactions, runDone, tenantID, sentinelUser)
	if !projectionHasUser(projection, seedUser) {
		t.Fatalf("write committed before the snapshot is missing")
	}
	if !projectionHasUser(projection, racingUser) {
		t.Fatalf("write racing the snapshot is in neither the snapshot nor the stream after its boundary")
	}
}

func insertBoundaryUser(ctx context.Context, conn *pgx.Conn, tenantID, userID string) error {
	_, err := conn.Exec(ctx, `
		INSERT INTO tenant_permissions_projection (tenant_id, user_id, permissions)
		VALUES ($1, $2, '{"role":"editor"}')`, tenantID, userID)
	return err
}

func runBoundaryReader(t *testing.T, ctx context.Context, reader *Reader) <-chan error {
	t.Helper()
	runCtx, stop := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- reader.Run(runCtx) }()
	t.Cleanup(func() {
		stop()
		if err := <-runDone; err != nil {
			t.Errorf("reader run: %v", err)
		}
	})
	return runDone
}

// rebuildAcrossBoundary installs the snapshot rows, commits a sentinel write,
// then applies every streamed transaction the snapshot does not contain until
// the sentinel arrives. The result must equal PostgreSQL.
func rebuildAcrossBoundary(
	t *testing.T,
	ctx context.Context,
	app *pgx.Conn,
	snapshot Snapshot,
	rows []map[string]any,
	transactions <-chan Transaction,
	runDone <-chan error,
	tenantID string,
	sentinelUser string,
) bootstrapProjection {
	t.Helper()
	projection := projectionFromBootstrapSnapshot(t, rows, tenantID)
	if err := insertBoundaryUser(ctx, app, tenantID, sentinelUser); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}
	for !projectionHasUser(projection, sentinelUser) {
		select {
		case transaction := <-transactions:
			if !snapshotContains(t, snapshot, transaction) {
				applyBootstrapTransaction(t, projection, transaction, tenantID)
			}
		case err := <-runDone:
			t.Fatalf("reader stopped before sentinel change: %v", err)
		case <-ctx.Done():
			t.Fatal("timed out waiting for sentinel change")
		}
	}

	want := readBootstrapProjection(t, ctx, app, tenantID)
	if !projectionsEqual(projection, want) {
		t.Errorf("snapshot plus stream after its boundary = %v, PostgreSQL = %v", projection, want)
	}
	return projection
}

// snapshotContains reports whether transaction's effects are already in the
// snapshot's rows, using the boundary the cdc package documents.
func snapshotContains(t *testing.T, snapshot Snapshot, transaction Transaction) bool {
	t.Helper()
	covered, err := snapshot.Covers(transaction)
	if err != nil {
		t.Fatalf("snapshot covers transaction: %v", err)
	}
	return covered
}
