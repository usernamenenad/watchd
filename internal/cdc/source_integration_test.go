//go:build integration

package cdc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestSourceUsesBootstrapSnapshotAndRunAcrossRestart walks one source
// through every path Source chooses between: the first scope creates the
// slot with Bootstrap, a later scope uses Snapshot while Run streams, and a
// restarted source resumes the same slot with Run instead of recreating it.
func TestSourceUsesBootstrapSnapshotAndRunAcrossRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	app := connectBootstrapTest(t, ctx, testDatabaseURL)
	defer app.Close(ctx)
	if _, err := app.Exec(ctx, "DELETE FROM tenant_permissions_projection"); err != nil {
		t.Fatalf("clear projection: %v", err)
	}

	firstTenant := "00000000-0000-0000-0000-000000000070"
	secondTenant := "00000000-0000-0000-0000-000000000080"
	for _, seed := range []struct{ tenant, user string }{
		{firstTenant, "00000000-0000-0000-0000-000000000701"},
		{secondTenant, "00000000-0000-0000-0000-000000000801"},
	} {
		if err := insertBoundaryUser(ctx, app, seed.tenant, seed.user); err != nil {
			t.Fatalf("seed projection: %v", err)
		}
	}

	slotName := fmt.Sprintf("watchd_source_%d", time.Now().UnixNano())
	transactions := make(chan Transaction, 16)
	sink := func(_ context.Context, transaction Transaction) error {
		transactions <- transaction
		return nil
	}

	// First life: no slot exists yet.
	source, stop := startTestSource(t, ctx, slotName, sink)
	registerBootstrapCleanup(t, source.reader, slotName)
	if bootstrapSlotExists(t, ctx, slotName) {
		t.Fatal("Start created a slot before any scope asked for one")
	}

	firstRows := snapshotSourceScope(t, ctx, source, firstTenant)
	if len(firstRows) != 1 {
		t.Fatalf("first scope rows = %d, want 1", len(firstRows))
	}
	if !bootstrapSlotExists(t, ctx, slotName) {
		t.Fatal("first scope did not create the slot")
	}
	waitForStreamedUser(t, ctx, app, transactions, firstTenant, "00000000-0000-0000-0000-000000000702")

	// A later scope snapshots against the live slot.
	secondRows := snapshotSourceScope(t, ctx, source, secondTenant)
	if len(secondRows) != 1 || bootstrapString(t, secondRows[0]["tenant_id"]) != secondTenant {
		t.Fatalf("second scope rows = %v, want its one seeded row", secondRows)
	}
	waitForStreamedUser(t, ctx, app, transactions, secondTenant, "00000000-0000-0000-0000-000000000802")

	stop()
	if !bootstrapSlotExists(t, ctx, slotName) {
		t.Fatal("stopping the source dropped its slot")
	}

	// Second life: the slot exists, so Start resumes it with Run, and a
	// scope snapshots against it rather than bootstrapping again.
	restarted, stopRestarted := startTestSource(t, ctx, slotName, sink)
	defer stopRestarted()
	waitForStreamedUser(t, ctx, app, transactions, firstTenant, "00000000-0000-0000-0000-000000000703")
	if rows := snapshotSourceScope(t, ctx, restarted, firstTenant); len(rows) != 3 {
		t.Fatalf("first scope rows after restart = %d, want 3", len(rows))
	}
}

func startTestSource(t *testing.T, ctx context.Context, slotName string, sink TransactionSink, configure ...func(*ReaderConfig)) (*Source, func()) {
	t.Helper()
	config := ReaderConfig{
		DatabaseURL:     testReplicationURL,
		SourceID:        "test-postgres",
		SlotName:        slotName,
		PublicationName: testPublication,
		StatusInterval:  100 * time.Millisecond,
		ShutdownTimeout: time.Second,
	}
	for _, apply := range configure {
		apply(&config)
	}
	source, err := NewSource(config, sink)
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	sourceCtx, cancel := context.WithCancel(ctx)
	if err := source.Start(sourceCtx); err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	stopped := false
	return source, func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case <-source.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("source did not stop")
		}
		if err := source.Err(); err != nil {
			t.Fatalf("source stopped with error: %v", err)
		}
	}
}

func snapshotSourceScope(t *testing.T, ctx context.Context, source *Source, tenantID string) []map[string]any {
	t.Helper()
	var rows []map[string]any
	if _, err := source.Snapshot(ctx, bootstrapTestProjection(), Scope{Value: tenantID}, collectSnapshotRows(&rows)); err != nil {
		t.Fatalf("snapshot scope %s: %v", tenantID, err)
	}
	return rows
}

// waitForStreamedUser inserts a row and waits until the source's stream
// delivers it, proving the source is streaming.
func waitForStreamedUser(t *testing.T, ctx context.Context, app *pgx.Conn, transactions <-chan Transaction, tenantID, userID string) {
	t.Helper()
	if err := insertBoundaryUser(ctx, app, tenantID, userID); err != nil {
		t.Fatalf("insert streamed row: %v", err)
	}
	for {
		select {
		case transaction := <-transactions:
			for _, change := range transaction.Changes {
				if change.Key["tenant_id"] == tenantID && change.Key["user_id"] == userID {
					return
				}
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for streamed row %s", userID)
		}
	}
}
