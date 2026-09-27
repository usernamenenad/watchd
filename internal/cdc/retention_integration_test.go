//go:build integration

package cdc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// emitWAL writes bytes of WAL the publication does not carry, as busy
// tables outside it would, and returns the WAL position after it.
func emitWAL(t *testing.T, ctx context.Context, app *pgx.Conn, bytes int) pglogrepl.LSN {
	t.Helper()
	var text string
	if err := app.QueryRow(ctx, "SELECT pg_logical_emit_message(false, 'watchd-test', repeat('x', $1))::text", bytes).Scan(&text); err != nil {
		t.Fatalf("emit WAL: %v", err)
	}
	lsn, err := pglogrepl.ParseLSN(text)
	if err != nil {
		t.Fatalf("parse LSN %q: %v", text, err)
	}
	return lsn
}

func confirmedFlushLSN(t *testing.T, ctx context.Context, app *pgx.Conn, slotName string) pglogrepl.LSN {
	t.Helper()
	var text *string
	if err := app.QueryRow(ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&text); err != nil {
		t.Fatalf("read confirmed_flush_lsn: %v", err)
	}
	lsn, _ := pglogrepl.ParseLSN(*text)
	return lsn
}

// TestIdlePublicationDoesNotPinWAL: WAL written outside the publication is
// acknowledged through keepalives, so a slot whose tables are idle does not
// retain it - which the retention budget would otherwise, wrongly, punish.
func TestIdlePublicationDoesNotPinWAL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	app := connectBootstrapTest(t, ctx, testDatabaseURL)
	defer app.Close(ctx)

	slotName := fmt.Sprintf("watchd_idle_%d", time.Now().UnixNano())
	source, stop := startTestSource(t, ctx, slotName, func(context.Context, Transaction) error { return nil })
	defer stop()
	registerBootstrapCleanup(t, source.reader, slotName)
	snapshotSourceScope(t, ctx, source, "00000000-0000-0000-0000-0000000000a1")

	var written pglogrepl.LSN
	for range 8 {
		written = emitWAL(t, ctx, app, 128<<10)
	}
	for confirmedFlushLSN(t, ctx, app, slotName) < written {
		select {
		case <-ctx.Done():
			t.Fatalf("confirmed_flush_lsn = %s never reached %s: idle WAL is pinned", confirmedFlushLSN(t, context.Background(), app, slotName), written)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// TestRetentionBudgetDropsTheSlotInStages stalls the sink, so nothing is
// acknowledged, and grows WAL: the policy passes warn and degrade in order,
// then drops the slot and stops the source with ErrRetainedWALBudgetExceeded.
func TestRetentionBudgetDropsTheSlotInStages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	app := connectBootstrapTest(t, ctx, testDatabaseURL)
	defer app.Close(ctx)

	const budget = 4 << 20
	slotName := fmt.Sprintf("watchd_budget_%d", time.Now().UnixNano())
	stalled := func(ctx context.Context, _ Transaction) error {
		<-ctx.Done()
		return ctx.Err()
	}
	source, err := NewSource(ReaderConfig{
		DatabaseURL:             testReplicationURL,
		SourceID:                "test-postgres",
		SlotName:                slotName,
		PublicationName:         testPublication,
		StatusInterval:          100 * time.Millisecond,
		ShutdownTimeout:         time.Second,
		MaxRetainedWALBytes:     budget,
		RetentionSampleInterval: 20 * time.Millisecond,
	}, stalled)
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	sourceCtx, stopSource := context.WithCancel(ctx)
	defer stopSource()
	if err := source.Start(sourceCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	registerBootstrapCleanup(t, source.reader, slotName)
	tenant := "00000000-0000-0000-0000-0000000000b1"
	if _, err := app.Exec(ctx, "DELETE FROM tenant_permissions_projection WHERE tenant_id = $1", tenant); err != nil {
		t.Fatalf("clear tenant: %v", err)
	}
	snapshotSourceScope(t, ctx, source, tenant)

	// The stream stalls on this transaction, so acknowledgement stops.
	if err := insertBoundaryUser(ctx, app, tenant, "00000000-0000-0000-0000-0000000000b2"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var seen []string
	record := func() {
		state := source.Stats().RetentionState
		if len(seen) == 0 || seen[len(seen)-1] != state {
			seen = append(seen, state)
		}
	}
	for done := false; !done; {
		select {
		case <-source.Done():
			done = true
		case <-ctx.Done():
			t.Fatalf("the source never stopped; retention states seen: %v", seen)
		default:
			emitWAL(t, ctx, app, 128<<10)
			// Let the policy sample each step.
			for range 3 {
				time.Sleep(25 * time.Millisecond)
				record()
			}
		}
	}
	record()

	if got := strings.Join(seen, ","); !strings.HasSuffix(got, "warn,degrade,terminal") {
		t.Errorf("retention states = %s, want warn, degrade, terminal in order", got)
	}
	if err := source.Err(); !errors.Is(err, ErrRetainedWALBudgetExceeded) || !strings.Contains(err.Error(), "dropped replication slot") {
		t.Fatalf("source error = %v, want ErrRetainedWALBudgetExceeded with the slot dropped", err)
	}
	if bootstrapSlotExists(t, ctx, slotName) {
		t.Fatal("the slot still exists after the terminal action")
	}
}
