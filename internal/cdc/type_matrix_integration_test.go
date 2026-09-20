//go:build integration

package cdc

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestTypeMatrixDecodesEveryColumnAsMeasuredOutputText is the "round-trip
// table test per type class" issue #32 asks for, run against a real
// PostgreSQL source rather than assumed from a SQL `column::text` cast -
// docs/semantics.md already documents that a cast and the type's own output
// function can disagree (boolean is the clearest example: "true" vs "t").
//
// Every expected string below was captured by observing the actual decoded
// output of internal/cdc against PostgreSQL, not derived from a cast. If a
// PostgreSQL upgrade changes one of these, that is a real contract change
// worth catching here, not a test bug to silence.
//
// TIMESTAMPTZ is deliberately not covered: its text output depends on the
// replication session's TimeZone setting, not a fixed per-type contract, so
// pinning one exact string would test this container's configuration
// instead of watchd's decoding.
func TestTypeMatrixDecodesEveryColumnAsMeasuredOutputText(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	app := connectBootstrapTest(t, ctx, testDatabaseURL)
	defer app.Close(ctx)
	if _, err := app.Exec(ctx, "DELETE FROM type_matrix_projection"); err != nil {
		t.Fatalf("clear projection: %v", err)
	}

	tenantID := "00000000-0000-0000-0000-000000000040"
	insertedID := "00000000-0000-0000-0000-000000000401"
	updatedID := "00000000-0000-0000-0000-000000000402"

	if _, err := app.Exec(ctx, `
		INSERT INTO type_matrix_projection
			(tenant_id, item_id, smallint_val, bigint_val, numeric_val, real_val, double_val,
			 bool_val, timestamp_val, date_val, time_val, interval_val, inet_val, cidr_val,
			 int_array_val, char_val, domain_val)
		VALUES ($1, $2, -1234, 9223372036854775807, -12345.6789, 3.14, 2.718281828459045,
			true, '2024-03-15 10:30:00.123456', '2024-03-15', '10:30:00.5',
			'1 year 2 mons 3 days 04:05:06', '192.168.1.0/24', '10.0.0.0/8',
			ARRAY[1,2,3], 'abc', 'hello')`,
		tenantID, insertedID); err != nil {
		t.Fatalf("seed projection: %v", err)
	}

	slotName := fmt.Sprintf("watchd_type_matrix_%d", time.Now().UnixNano())
	transactions := make(chan Transaction, 8)
	reader := newBootstrapTestReader(t, slotName, func(_ context.Context, transaction Transaction) error {
		transactions <- transaction
		return nil
	})
	registerBootstrapCleanup(t, reader, slotName)

	spec := ProjectionSpec{
		SourceID:    "test-postgres",
		Schema:      "public",
		Table:       "type_matrix_projection",
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
		"tenant_id":     tenantID,
		"item_id":       insertedID,
		"smallint_val":  "-1234",
		"bigint_val":    "9223372036854775807", // max int64
		"numeric_val":   "-12345.6789",
		"real_val":      "3.14",
		"double_val":    "2.718281828459045",
		"bool_val":      "t", // not "true" - see the package doc comment above
		"timestamp_val": "2024-03-15 10:30:00.123456",
		"date_val":      "2024-03-15",
		"time_val":      "10:30:00.5",
		"interval_val":  "1 year 2 mons 3 days 04:05:06",
		"inet_val":      "192.168.1.0/24",
		"cidr_val":      "10.0.0.0/8",
		"int_array_val": "{1,2,3}",
		"char_val":      "abc     ", // CHAR(8) blank-pads to its declared width
		"domain_val":    "hello",    // a domain decodes exactly like its base type
	})

	runCtx, stopRun := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- reader.Run(runCtx) }()
	t.Cleanup(func() {
		stopRun()
		<-runDone
	})

	if _, err := app.Exec(ctx, `
		INSERT INTO type_matrix_projection
			(tenant_id, item_id, smallint_val, bigint_val, numeric_val, real_val, double_val,
			 bool_val, timestamp_val, date_val, time_val, interval_val, inet_val, cidr_val,
			 int_array_val, char_val, domain_val)
		VALUES ($1, $2, 32767, -9223372036854775808, 0.0001, -1.5, 0,
			false, '2000-01-01 00:00:00', '2000-01-01', '00:00:00',
			'0', '::1', '2001:db8::/32',
			ARRAY[]::integer[], '', 'x')`,
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
			"tenant_id":     tenantID,
			"item_id":       updatedID,
			"smallint_val":  "32767",                // max int16
			"bigint_val":    "-9223372036854775808", // min int64
			"numeric_val":   "0.0001",
			"real_val":      "-1.5",
			"double_val":    "0",
			"bool_val":      "f",
			"timestamp_val": "2000-01-01 00:00:00",
			"date_val":      "2000-01-01",
			"time_val":      "00:00:00",
			"interval_val":  "00:00:00", // PostgreSQL normalizes a zero interval literal to this
			"inet_val":      "::1",      // IPv6
			"cidr_val":      "2001:db8::/32",
			"int_array_val": "{}", // empty array, not NULL
			"char_val":      "        ",
			"domain_val":    "x",
		})
	case <-ctx.Done():
		t.Fatal("timed out waiting for replicated transaction")
	}
}
