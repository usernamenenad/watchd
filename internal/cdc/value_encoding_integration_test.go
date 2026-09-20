//go:build integration

package cdc

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestValueEncodingRoundTripsEveryTypeClassAsText proves the value-encoding
// contract in docs/semantics.md: whatever a column's PostgreSQL type, its
// decoded Value is exactly PostgreSQL's own type-output-function
// representation - covering the type classes issue #32 calls out (enum,
// array, bytea, jsonb, boolean, numeric) alongside the baseline UUID/text
// columns already exercised elsewhere. It checks this on both the Snapshot
// path (Bootstrap) and the live replication path (Run).
//
// Expected strings are hardcoded, not derived from a SQL `column::text`
// cast: casting and the type's output function disagree for at least
// boolean (`true::text` is "true"; the wire value is "t"), so a cast is not
// a trustworthy oracle for this test. See "Value encoding" in
// docs/semantics.md.
func TestValueEncodingRoundTripsEveryTypeClassAsText(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	app := connectBootstrapTest(t, ctx, testDatabaseURL)
	defer app.Close(ctx)
	if _, err := app.Exec(ctx, "DELETE FROM value_encoding_projection"); err != nil {
		t.Fatalf("clear projection: %v", err)
	}

	tenantID := "00000000-0000-0000-0000-000000000020"
	insertedID := "00000000-0000-0000-0000-000000000201"
	updatedID := "00000000-0000-0000-0000-000000000202"

	if _, err := app.Exec(ctx, `
		INSERT INTO value_encoding_projection
			(tenant_id, item_id, status, tags, payload, metadata, is_active, quantity, amount)
		VALUES ($1, $2, 'active', ARRAY['a','b'], '\xdeadbeef'::bytea, '{"k":"v","n":1}'::jsonb, true, 42, 19.99)`,
		tenantID, insertedID); err != nil {
		t.Fatalf("seed projection: %v", err)
	}

	slotName := fmt.Sprintf("watchd_value_encoding_%d", time.Now().UnixNano())
	transactions := make(chan Transaction, 8)
	reader := newBootstrapTestReader(t, slotName, func(_ context.Context, transaction Transaction) error {
		transactions <- transaction
		return nil
	})
	registerBootstrapCleanup(t, reader, slotName)

	spec := ProjectionSpec{
		SourceID:    "test-postgres",
		Schema:      "public",
		Table:       "value_encoding_projection",
		ScopeColumn: "tenant_id",
		PrimaryKey:  []string{"tenant_id", "item_id"},
	}

	var rows []map[string]any
	_, err := reader.Bootstrap(ctx, spec, Scope{Value: tenantID}, collectSnapshotRows(&rows))
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("snapshot rows = %d, want 1", len(rows))
	}
	assertRowMatchesText(t, rows[0], map[string]string{
		"tenant_id": tenantID,
		"item_id":   insertedID,
		"status":    "active",
		"tags":      "{a,b}",
		"payload":   `\xdeadbeef`,
		"metadata":  `{"k": "v", "n": 1}`,
		"is_active": "t",
		"quantity":  "42",
		"amount":    "19.99",
	})

	runCtx, stopRun := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- reader.Run(runCtx) }()
	t.Cleanup(func() {
		stopRun()
		<-runDone
	})

	if _, err := app.Exec(ctx, `
		INSERT INTO value_encoding_projection
			(tenant_id, item_id, status, tags, payload, metadata, is_active, quantity, amount)
		VALUES ($1, $2, 'inactive', ARRAY['x','y','z'], '\x00ff'::bytea, '[1,2,3]'::jsonb, false, -7, 0.10)`,
		tenantID, updatedID); err != nil {
		t.Fatalf("insert via replication: %v", err)
	}

	select {
	case transaction := <-transactions:
		if len(transaction.Changes) != 1 {
			t.Fatalf("changes = %d, want 1", len(transaction.Changes))
		}
		change := transaction.Changes[0]
		if change.Operation != OperationInsert {
			t.Fatalf("operation = %s, want %s", change.Operation, OperationInsert)
		}
		assertRowMatchesText(t, change.Values, map[string]string{
			"tenant_id": tenantID,
			"item_id":   updatedID,
			"status":    "inactive",
			"tags":      "{x,y,z}",
			"payload":   `\x00ff`,
			"metadata":  "[1, 2, 3]",
			"is_active": "f",
			"quantity":  "-7",
			"amount":    "0.10",
		})
	case <-ctx.Done():
		t.Fatal("timed out waiting for replicated transaction")
	}
}

func assertRowMatchesText(t *testing.T, row map[string]any, want map[string]string) {
	t.Helper()
	if len(row) != len(want) {
		t.Fatalf("row has %d columns, want %d (row=%v, want=%v)", len(row), len(want), row, want)
	}
	for column, wantText := range want {
		got, ok := row[column]
		if !ok {
			t.Fatalf("row is missing column %s", column)
		}
		gotText, ok := got.(string)
		if !ok {
			t.Fatalf("column %s = %#v (%T), want a string matching PostgreSQL's type-output-function text", column, got, got)
		}
		if gotText != wantText {
			t.Fatalf("column %s = %q, want %q (PostgreSQL's type-output-function text)", column, gotText, wantText)
		}
	}
}
