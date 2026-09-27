package cdc

import (
	"errors"
	"testing"

	"github.com/jackc/pglogrepl"
)

func cursor(sourceID string, lsn uint64) Cursor {
	return Cursor{sourceID: sourceID, lsn: pglogrepl.LSN(lsn)}
}

func TestCursorCompareOrdersWithinOneSource(t *testing.T) {
	earlier := cursor("source-a", 10)
	later := cursor("source-a", 20)

	got, err := earlier.Compare(later)
	if err != nil {
		t.Fatalf("Compare: %v", err)
	}
	if got != -1 {
		t.Fatalf("earlier.Compare(later) = %d, want -1", got)
	}

	got, err = later.Compare(earlier)
	if err != nil {
		t.Fatalf("Compare: %v", err)
	}
	if got != 1 {
		t.Fatalf("later.Compare(earlier) = %d, want 1", got)
	}

	got, err = earlier.Compare(earlier)
	if err != nil {
		t.Fatalf("Compare: %v", err)
	}
	if got != 0 {
		t.Fatalf("earlier.Compare(earlier) = %d, want 0", got)
	}
}

func TestCursorCompareRejectsDifferentSources(t *testing.T) {
	a := cursor("source-a", 10)
	b := cursor("source-b", 10)

	if _, err := a.Compare(b); err == nil {
		t.Fatal("Compare across sources: got nil error, want one")
	}
}

func TestCursorIsZero(t *testing.T) {
	var zero Cursor
	if !zero.IsZero() {
		t.Fatal("zero Cursor.IsZero() = false, want true")
	}
	if cursor("source-a", 1).IsZero() {
		t.Fatal("assigned Cursor.IsZero() = true, want false")
	}
}

func TestMinCursorReturnsEarliestWithinOneSource(t *testing.T) {
	c1 := cursor("source-a", 30)
	c2 := cursor("source-a", 10)
	c3 := cursor("source-a", 20)

	min, err := MinCursor(c1, c2, c3)
	if err != nil {
		t.Fatalf("MinCursor: %v", err)
	}
	if min != c2 {
		t.Fatalf("MinCursor = %v, want %v", min, c2)
	}
}

func TestMinCursorRejectsMixedSources(t *testing.T) {
	a := cursor("source-a", 10)
	b := cursor("source-b", 20)

	if _, err := MinCursor(a, b); err == nil {
		t.Fatal("MinCursor across sources: got nil error, want one")
	}
}

func TestMinCursorRejectsEmptyInput(t *testing.T) {
	if _, err := MinCursor(); !errors.Is(err, ErrNoCursors) {
		t.Fatalf("MinCursor() error = %v, want %v", err, ErrNoCursors)
	}
}

func TestTransactionAfterMirrorsReplicationStartRule(t *testing.T) {
	transaction := Transaction{Cursor: cursor("a", 90), commitLSN: 42}
	for _, test := range []struct {
		resume uint64
		want   bool
	}{
		{resume: 41, want: true},  // resume before the commit: apply it
		{resume: 42, want: true},  // PostgreSQL starts at the commit record itself
		{resume: 43, want: false}, // resume after the commit started: already applied
		{resume: 90, want: false}, // resume from this transaction's own cursor
	} {
		got, err := transaction.After(cursor("a", test.resume))
		if err != nil {
			t.Fatalf("After(%d): %v", test.resume, err)
		}
		if got != test.want {
			t.Errorf("After(%d) = %t, want %t", test.resume, got, test.want)
		}
	}
	if _, err := transaction.After(cursor("b", 1)); err == nil {
		t.Fatal("After across sources: got nil error, want one")
	}
}

func TestSnapshotCoversByTransactionVisibility(t *testing.T) {
	visibility, err := parseVisibility("100:110:105")
	if err != nil {
		t.Fatalf("parseVisibility: %v", err)
	}
	snapshot := Snapshot{SourceID: "a", Cursor: cursor("a", 1000), visibility: visibility}

	// Covers decides by transaction ID, not position: a transaction that
	// commits before the snapshot cursor but was still running when the
	// snapshot was fixed is not covered, and vice versa.
	running := Transaction{Cursor: cursor("a", 900), commitLSN: 800, xid: 105}
	finished := Transaction{Cursor: cursor("a", 1200), commitLSN: 1100, xid: 104}
	for _, test := range []struct {
		name        string
		transaction Transaction
		want        bool
	}{
		{name: "running when fixed", transaction: running, want: false},
		{name: "finished when fixed", transaction: finished, want: true},
	} {
		got, err := snapshot.Covers(test.transaction)
		if err != nil {
			t.Fatalf("%s: Covers: %v", test.name, err)
		}
		if got != test.want {
			t.Errorf("%s: Covers = %t, want %t", test.name, got, test.want)
		}
	}

	if _, err := snapshot.Covers(Transaction{Cursor: cursor("b", 1), xid: 104}); err == nil {
		t.Fatal("Covers across sources: got nil error, want one")
	}
	if covered, err := (Snapshot{Cursor: cursor("a", 1)}).Covers(finished); err != nil || covered {
		t.Fatalf("zero-visibility Covers = %t, %v; want false, nil", covered, err)
	}
}

func TestParseCursorRoundTrips(t *testing.T) {
	original := cursor("a", 0x16B3748)
	parsed, err := ParseCursor("a", original.String())
	if err != nil {
		t.Fatalf("ParseCursor: %v", err)
	}
	if order, err := parsed.Compare(original); err != nil || order != 0 {
		t.Fatalf("parsed.Compare(original) = %d, %v; want 0, nil", order, err)
	}
	if _, err := ParseCursor("a", "not-a-cursor"); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("ParseCursor(garbage) error = %v, want %v", err, ErrInvalidCursor)
	}
	if _, err := ParseCursor("", original.String()); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("ParseCursor without source error = %v, want %v", err, ErrInvalidCursor)
	}
}

func TestCursorBytesAfter(t *testing.T) {
	a := Cursor{sourceID: "s", lsn: 100}
	b := Cursor{sourceID: "s", lsn: 40}
	for _, test := range []struct {
		c, other Cursor
		want     int64
	}{
		{a, b, 60},
		{b, a, 0},
		{a, a, 0},
		{a, Cursor{sourceID: "other", lsn: 1}, 0},
	} {
		if got := test.c.BytesAfter(test.other); got != test.want {
			t.Errorf("%v.BytesAfter(%v) = %d, want %d", test.c, test.other, got, test.want)
		}
	}
}
