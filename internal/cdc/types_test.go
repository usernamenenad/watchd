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
