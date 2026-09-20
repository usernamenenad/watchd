//go:build integration

package cdc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestSnapshotBootstrapsSecondScopeAgainstExistingSlot exercises the core
// deliverable of issue #19: a second scope can obtain a gap-free snapshot
// from a slot that Bootstrap already created for a different scope, with
// writes interleaved before, during, and after the snapshot.
func TestSnapshotBootstrapsSecondScopeAgainstExistingSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	app := connectBootstrapTest(t, ctx, testDatabaseURL)
	defer app.Close(ctx)
	if _, err := app.Exec(ctx, "DELETE FROM tenant_permissions_projection"); err != nil {
		t.Fatalf("clear projection: %v", err)
	}

	firstTenant := "00000000-0000-0000-0000-000000000020"
	secondTenant := "00000000-0000-0000-0000-000000000030"
	beforeUser := "00000000-0000-0000-0000-000000000201"
	deletedDuringUser := "00000000-0000-0000-0000-000000000202"
	duringUser := "00000000-0000-0000-0000-000000000203"
	afterUser := "00000000-0000-0000-0000-000000000204"

	slotName := fmt.Sprintf("watchd_second_scope_%d", time.Now().UnixNano())
	transactions := make(chan Transaction, 8)
	reader := newBootstrapTestReader(t, slotName, func(_ context.Context, transaction Transaction) error {
		transactions <- transaction
		return nil
	})
	registerBootstrapCleanup(t, reader, slotName)

	if _, err := reader.Bootstrap(ctx, bootstrapTestProjection(), Scope{Value: firstTenant}, func(context.Context, []map[string]any) error { return nil }); err != nil {
		t.Fatalf("bootstrap first scope: %v", err)
	}

	runCtx, stop := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- reader.Run(runCtx) }()
	defer func() {
		stop()
		if err := <-runDone; err != nil {
			t.Errorf("reader run: %v", err)
		}
	}()

	if _, err := app.Exec(ctx, `
		INSERT INTO tenant_permissions_projection (tenant_id, user_id, permissions)
		VALUES ($1, $2, '{"role":"editor"}'), ($1, $3, '{"role":"temporary"}')`,
		secondTenant, beforeUser, deletedDuringUser); err != nil {
		t.Fatalf("seed second scope before snapshot: %v", err)
	}

	snapshotReady := make(chan struct{})
	continueSnapshot := make(chan struct{})
	reader.beforeSnapshotRead = func(ctx context.Context) error {
		close(snapshotReady)
		select {
		case <-continueSnapshot:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	type snapshotResult struct {
		snapshot Snapshot
		rows     []map[string]any
		err      error
	}
	snapshotDone := make(chan snapshotResult, 1)
	go func() {
		var rows []map[string]any
		snapshot, err := reader.Snapshot(ctx, bootstrapTestProjection(), Scope{Value: secondTenant}, collectSnapshotRows(&rows))
		snapshotDone <- snapshotResult{snapshot: snapshot, rows: rows, err: err}
	}()

	select {
	case <-snapshotReady:
	case result := <-snapshotDone:
		t.Fatalf("snapshot returned before reaching the read hook: %v", result.err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for second-scope snapshot")
	}
	if err := writeDuringBootstrapTest(ctx, app, secondTenant, beforeUser, deletedDuringUser, duringUser); err != nil {
		t.Fatalf("write during snapshot: %v", err)
	}
	close(continueSnapshot)

	result := <-snapshotDone
	if result.err != nil {
		t.Fatalf("snapshot second scope: %v", result.err)
	}
	projection := projectionFromBootstrapSnapshot(t, result.rows, secondTenant)

	if err := writeAfterBootstrapTest(ctx, app, secondTenant, beforeUser, duringUser, afterUser); err != nil {
		t.Fatalf("write after snapshot: %v", err)
	}
	for !projectionHasUser(projection, afterUser) {
		select {
		case transaction := <-transactions:
			applyBootstrapTransaction(t, projection, transaction, secondTenant)
		case err := <-runDone:
			t.Fatalf("reader stopped before sentinel change: %v", err)
		case <-ctx.Done():
			t.Fatal("timed out waiting for gap-free projection")
		}
	}

	want := readBootstrapProjection(t, ctx, app, secondTenant)
	if !projectionsEqual(projection, want) {
		t.Fatalf("snapshot plus stream = %v, PostgreSQL = %v", projection, want)
	}
}

// TestSnapshotBeforeSlotExistsHasTypedError confirms Snapshot never creates a
// slot itself; only Bootstrap does.
func TestSnapshotBeforeSlotExistsHasTypedError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	slotName := fmt.Sprintf("watchd_snapshot_no_slot_%d", time.Now().UnixNano())
	reader := newBootstrapTestReader(t, slotName, func(context.Context, Transaction) error { return nil })
	registerBootstrapCleanup(t, reader, slotName)

	_, err := reader.Snapshot(ctx, bootstrapTestProjection(), Scope{Value: "00000000-0000-0000-0000-000000000001"}, func(context.Context, []map[string]any) error { return nil })
	if !errors.Is(err, ErrSlotNotFound) {
		t.Fatalf("snapshot without a slot error = %v, want %v", err, ErrSlotNotFound)
	}
}

// TestSnapshotWindowClosedHasTypedError confirms that a cursor behind the
// slot's retained boundary produces ErrSnapshotWindowClosed rather than a
// resume boundary that would silently skip changes.
//
// restart_lsn only advances at a checkpoint, and only up to roughly the
// oldest position still needed — not synchronously with acknowledgment and
// not all the way to what was just acknowledged. So this repeatedly
// advances the slot and checkpoints until restart_lsn has genuinely passed
// a cursor captured at slot creation, rather than assuming one round trip
// is enough.
func TestSnapshotWindowClosedHasTypedError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	slotName := fmt.Sprintf("watchd_snapshot_window_%d", time.Now().UnixNano())
	reader := newBootstrapTestReader(t, slotName, func(context.Context, Transaction) error { return nil })
	registerBootstrapCleanup(t, reader, slotName)

	replication, err := pgconn.Connect(ctx, testReplicationURL)
	if err != nil {
		t.Fatalf("connect replication source: %v", err)
	}
	created, err := pglogrepl.CreateReplicationSlot(ctx, replication, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		SnapshotAction: "EXPORT_SNAPSHOT",
	})
	if err != nil {
		t.Fatalf("create slot: %v", err)
	}
	staleCursor, err := pglogrepl.ParseLSN(created.ConsistentPoint)
	if err != nil {
		t.Fatalf("parse consistent point: %v", err)
	}
	if err := replication.Close(ctx); err != nil {
		t.Fatalf("release slot: %v", err)
	}

	admin := connectBootstrapTest(t, ctx, testAdminURL)
	defer admin.Close(ctx)

	const maxAdvances = 25
	var restartLSN pglogrepl.LSN
	for i := 0; i < maxAdvances; i++ {
		if _, err := admin.Exec(ctx, `
			INSERT INTO tenant_permissions_projection (tenant_id, user_id, permissions)
			VALUES ('00000000-0000-0000-0000-000000000001', gen_random_uuid(), '{"role":"editor"}')`); err != nil {
			t.Fatalf("generate WAL: %v", err)
		}
		if _, err := admin.Exec(ctx, "SELECT pg_replication_slot_advance($1, pg_current_wal_lsn())", slotName); err != nil {
			t.Fatalf("advance slot: %v", err)
		}
		if _, err := admin.Exec(ctx, "CHECKPOINT"); err != nil {
			t.Fatalf("checkpoint: %v", err)
		}

		var restartLSNText string
		if err := admin.QueryRow(ctx, `
			SELECT COALESCE(restart_lsn::text, '') FROM pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&restartLSNText); err != nil {
			t.Fatalf("read restart_lsn: %v", err)
		}
		restartLSN, err = pglogrepl.ParseLSN(restartLSNText)
		if err != nil {
			t.Fatalf("parse restart_lsn: %v", err)
		}
		if restartLSN > staleCursor {
			break
		}
	}
	if restartLSN <= staleCursor {
		t.Fatalf("restart_lsn %s never advanced past stale cursor %s after %d attempts", restartLSN, staleCursor, maxAdvances)
	}

	management := connectBootstrapTest(t, ctx, testReplicationURL)
	defer management.Close(ctx)
	if err := reader.checkReplayWindow(ctx, management, staleCursor); !errors.Is(err, ErrSnapshotWindowClosed) {
		t.Fatalf("replay window check for a stale cursor = %v, want %v", err, ErrSnapshotWindowClosed)
	}
}

// TestConcurrentSnapshotsOfDifferentScopesDoNotSerialize confirms that
// multiple scopes can each call Snapshot at the same time against one slot,
// while Run is streaming, without one snapshot blocking another.
func TestConcurrentSnapshotsOfDifferentScopesDoNotSerialize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	app := connectBootstrapTest(t, ctx, testDatabaseURL)
	defer app.Close(ctx)
	if _, err := app.Exec(ctx, "DELETE FROM tenant_permissions_projection"); err != nil {
		t.Fatalf("clear projection: %v", err)
	}

	firstTenant := "00000000-0000-0000-0000-000000000040"
	secondTenant := "00000000-0000-0000-0000-000000000050"
	thirdTenant := "00000000-0000-0000-0000-000000000060"
	if _, err := app.Exec(ctx, `
		INSERT INTO tenant_permissions_projection (tenant_id, user_id, permissions)
		VALUES
			($1, '00000000-0000-0000-0000-000000000501', '{"role":"editor"}'),
			($2, '00000000-0000-0000-0000-000000000601', '{"role":"editor"}')`,
		secondTenant, thirdTenant); err != nil {
		t.Fatalf("seed scopes: %v", err)
	}

	slotName := fmt.Sprintf("watchd_concurrent_scopes_%d", time.Now().UnixNano())
	reader := newBootstrapTestReader(t, slotName, func(context.Context, Transaction) error { return nil })
	registerBootstrapCleanup(t, reader, slotName)

	if _, err := reader.Bootstrap(ctx, bootstrapTestProjection(), Scope{Value: firstTenant}, func(context.Context, []map[string]any) error { return nil }); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	runCtx, stop := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- reader.Run(runCtx) }()
	defer func() {
		stop()
		if err := <-runDone; err != nil {
			t.Errorf("reader run: %v", err)
		}
	}()

	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, tenant := range []string{secondTenant, thirdTenant} {
		wg.Add(1)
		go func(scope string) {
			defer wg.Done()
			_, err := reader.Snapshot(ctx, bootstrapTestProjection(), Scope{Value: scope}, func(context.Context, []map[string]any) error { return nil })
			results <- err
		}(tenant)
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent snapshot: %v", err)
		}
	}
}

// TestBootstrapSnapshotDeliversRowsInPaginatedBatches confirms that a scope
// larger than one SnapshotBatchRows page is read as several ordered,
// non-overlapping keyset-paginated batches rather than one unbounded query,
// and that every row still arrives exactly once, in ascending primary-key
// order across batch boundaries.
func TestBootstrapSnapshotDeliversRowsInPaginatedBatches(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	app := connectBootstrapTest(t, ctx, testDatabaseURL)
	defer app.Close(ctx)
	if _, err := app.Exec(ctx, "DELETE FROM tenant_permissions_projection"); err != nil {
		t.Fatalf("clear projection: %v", err)
	}

	tenantID := "00000000-0000-0000-0000-000000000040"
	const rowCount = 25
	const batchRows = 10
	if _, err := app.Exec(ctx, `
		INSERT INTO tenant_permissions_projection (tenant_id, user_id, permissions)
		SELECT $1, ('00000000-0000-0000-0000-' || lpad(i::text, 12, '0'))::uuid, '{"role":"viewer"}'
		FROM generate_series(1, $2) AS i`, tenantID, rowCount); err != nil {
		t.Fatalf("seed paginated scope: %v", err)
	}

	slotName := fmt.Sprintf("watchd_paginated_scope_%d", time.Now().UnixNano())
	reader, err := NewReader(ReaderConfig{
		DatabaseURL:       testReplicationURL,
		SlotName:          slotName,
		PublicationName:   testPublication,
		StatusInterval:    100 * time.Millisecond,
		ShutdownTimeout:   time.Second,
		SnapshotBatchRows: batchRows,
	}, func(context.Context, Transaction) error { return nil })
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	registerBootstrapCleanup(t, reader, slotName)

	var (
		rows        []map[string]any
		batchCount  int
		lastUserID  string
		sawOrdering = true
	)
	_, err = reader.Bootstrap(ctx, bootstrapTestProjection(), Scope{Value: tenantID}, func(_ context.Context, batch []map[string]any) error {
		batchCount++
		if len(batch) > batchRows {
			t.Fatalf("batch size = %d, want at most %d", len(batch), batchRows)
		}
		for _, row := range batch {
			userID := bootstrapString(t, row["user_id"])
			if lastUserID != "" && userID <= lastUserID {
				sawOrdering = false
			}
			lastUserID = userID
		}
		rows = append(rows, batch...)
		return nil
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	if got, want := len(rows), rowCount; got != want {
		t.Fatalf("snapshot rows = %d, want %d", got, want)
	}
	if wantBatches := (rowCount + batchRows - 1) / batchRows; batchCount != wantBatches {
		t.Fatalf("batches invoked = %d, want %d", batchCount, wantBatches)
	}
	if !sawOrdering {
		t.Fatalf("rows were not delivered in strictly increasing primary-key order: %v", rows)
	}

	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		userID := bootstrapString(t, row["user_id"])
		if _, duplicate := seen[userID]; duplicate {
			t.Fatalf("row for user %s delivered more than once", userID)
		}
		seen[userID] = struct{}{}
	}
}

func projectionsEqual(a, b bootstrapProjection) bool {
	if len(a) != len(b) {
		return false
	}
	for userID, row := range a {
		other, found := b[userID]
		if !found || other != row {
			return false
		}
	}
	return true
}
