package cdc

import (
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"
)

// ErrNoCursors indicates that MinCursor was called with no cursors to compare.
var ErrNoCursors = errors.New("cdc: no cursors to compare")

const (
	OperationInsert = "insert"
	OperationUpdate = "update"
	OperationDelete = "delete"
)

// Change is one row mutation from a PostgreSQL projection table.
//
// Key contains the stable replica-identity columns needed to identify a row.
// A replica-identity column is always sent in full by PostgreSQL, so every
// Key entry is a non-empty PostgreSQL text value - never NULL, never
// TOAST-omitted, and never absent. Values contains the resulting row state
// for inserts and updates. Every value is one of: a string holding the
// column's PostgreSQL text representation (the same text `column::text`
// would produce, for every column type including bytea, arrays, composite
// types, json/jsonb, enums, and domain types - watchd does not request
// pgoutput's binary option, so encoding is uniform across types), nil for
// SQL NULL, or UnchangedToast for a TOAST column omitted from an UPDATE
// because it did not change. See "Value encoding" in docs/semantics.md.
type Change struct {
	Operation string
	Table     string
	Key       map[string]string
	Values    map[string]any
}

// Transaction is an atomic batch of committed source changes. Cursor is the
// source commit position and is deliberately opaque to callers.
type Transaction struct {
	Cursor  string
	Changes []Change
}

// ProjectionSpec identifies one configured projection table. It is trusted
// source configuration, not a client-provided SQL query.
type ProjectionSpec struct {
	SourceID    string
	Schema      string
	Table       string
	ScopeColumn string
	PrimaryKey  []string
}

// Scope selects one equality-partition of a ProjectionSpec.
type Scope struct {
	Value string
}

// Cursor is a source commit position. Cursors from the same source are
// totally ordered and safe to compare; the underlying representation stays
// opaque to callers, who must go through Compare/MinCursor rather than
// parsing String's output.
type Cursor struct {
	sourceID string
	lsn      pglogrepl.LSN
}

// Compare reports whether c precedes (-1), equals (0), or follows (1) other.
// It returns an error if c and other belong to different sources, since v0
// makes no ordering claim across sources.
func (c Cursor) Compare(other Cursor) (int, error) {
	if c.sourceID != other.sourceID {
		return 0, fmt.Errorf("cdc: cannot compare cursors from different sources (%q, %q)", c.sourceID, other.sourceID)
	}

	switch {
	case c.lsn < other.lsn:
		return -1, nil
	case c.lsn > other.lsn:
		return 1, nil
	default:
		return 0, nil
	}
}

// IsZero reports whether c is the zero Cursor, i.e. it was never assigned
// from a Snapshot or Transaction.
func (c Cursor) IsZero() bool {
	return c == Cursor{}
}

// String returns Cursor's opaque persisted form. Callers must persist and
// return it verbatim; they must not derive meaning from its contents.
func (c Cursor) String() string {
	return c.lsn.String()
}

// MinCursor returns the earliest of cursors. Given the progress cursors of
// several scopes of one source, the result is the consistent-cut boundary:
// a client may treat it as a snapshot-consistent state across those scopes.
// It returns an error if cursors is empty or spans more than one source.
func MinCursor(cursors ...Cursor) (Cursor, error) {
	if len(cursors) == 0 {
		return Cursor{}, ErrNoCursors
	}

	min := cursors[0]
	for _, cursor := range cursors[1:] {
		cmp, err := min.Compare(cursor)
		if err != nil {
			return Cursor{}, err
		}
		if cmp > 0 {
			min = cursor
		}
	}

	return min, nil
}

// Snapshot is a consistent scoped read paired with the opaque cursor at
// which replication must resume. Like Change.Values, row values are their
// PostgreSQL text representation or nil for SQL NULL.
type Snapshot struct {
	SourceID string
	Cursor   Cursor
	Rows     []map[string]any
}

// UnchangedToast represents a PostgreSQL TOAST value omitted from an UPDATE
// message because that column did not change. A projection applier must retain
// its existing value for that column.
type UnchangedToast struct{}

func (UnchangedToast) String() string {
	return "unchanged-toast"
}

func qualifiedTable(namespace, relation string) string {
	if namespace == "" {
		return relation
	}

	return fmt.Sprintf("%s.%s", namespace, relation)
}
