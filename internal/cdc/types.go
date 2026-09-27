package cdc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
)

var (
	// ErrNoCursors indicates that MinCursor was called with no cursors to compare.
	ErrNoCursors = errors.New("cdc: no cursors to compare")
	// ErrInvalidCursor indicates text that is not a cursor this package produced.
	ErrInvalidCursor = errors.New("cdc: invalid cursor")
)

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

// Transaction is an atomic batch of committed source changes.
//
// Cursor is the position just after this transaction's commit - the point a
// consumer that has applied it resumes from, and the position the Reader
// acknowledges to PostgreSQL once its sink accepts the transaction.
type Transaction struct {
	Cursor  Cursor
	Changes []Change
	// CommitTime is when the transaction committed, by PostgreSQL's clock.
	// It is informational, for measuring latency: ordering always comes
	// from Cursor.
	CommitTime time.Time

	// commitLSN is the start of the transaction's commit record, which is
	// what PostgreSQL itself compares against a replication start position.
	commitLSN pglogrepl.LSN
	// xid is the source transaction ID, used to decide whether a snapshot
	// already contains this transaction's effects.
	xid uint32
}

// After reports whether t commits at or after c, i.e. whether a consumer
// resuming from c must still apply t. It mirrors PostgreSQL's own rule for a
// replication start position. It returns an error if t and c belong to
// different sources.
func (t Transaction) After(c Cursor) (bool, error) {
	if t.Cursor.sourceID != c.sourceID {
		return false, fmt.Errorf("cdc: cannot compare a transaction and a cursor from different sources (%q, %q)", t.Cursor.sourceID, c.sourceID)
	}
	return t.commitLSN >= c.lsn, nil
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

// BytesAfter reports how many bytes of source WAL lie between other and c:
// 0 when c is not after other, or when they belong to different sources. It
// is for measuring lag, never for ordering; use Compare to order cursors.
func (c Cursor) BytesAfter(other Cursor) int64 {
	if c.sourceID != other.sourceID || c.lsn <= other.lsn {
		return 0
	}
	return int64(c.lsn - other.lsn)
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

// ParseCursor restores a cursor previously produced by Cursor.String for the
// given source, such as a resume position a client persisted and sent back.
func ParseCursor(sourceID, text string) (Cursor, error) {
	if sourceID == "" {
		return Cursor{}, fmt.Errorf("%w: cursor source ID is required", ErrInvalidCursor)
	}
	lsn, err := pglogrepl.ParseLSN(text)
	if err != nil {
		return Cursor{}, fmt.Errorf("%w: %q", ErrInvalidCursor, text)
	}
	return Cursor{sourceID: sourceID, lsn: lsn}, nil
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
// which replication must resume. Rows are delivered separately through the
// SnapshotRowSink passed to Snapshot/Bootstrap, not carried on this struct.
//
// The snapshot and the change stream meet at a boundary with no gap: every
// committed transaction is either already in the rows (Covers reports true)
// or must be applied from the stream. See Covers for how a consumer uses it.
type Snapshot struct {
	SourceID string
	Cursor   Cursor

	visibility *visibility
}

// Covers reports whether transaction t's effects are already in the
// snapshot's rows. A consumer installs the rows, then applies every streamed
// transaction for which Covers is false, in stream order; transactions for
// which it is true must be skipped, or the projection moves backwards.
//
// Every transaction Covers rejects commits after Cursor, with one exception:
// a transaction whose commit record is written before it becomes visible to
// new snapshots - for example while it waits on a synchronous standby - can
// commit just before Cursor and still be missing from the rows. A consumer
// that has already streamed past Cursor must therefore check the
// transactions it already holds, not only the ones that arrive later.
//
// The decision is made by transaction ID against the read's MVCC snapshot,
// not by comparing positions, so it is exact regardless of how a commit
// raced the read. It returns an error if t is from a different source.
func (s Snapshot) Covers(t Transaction) (bool, error) {
	if t.Cursor.sourceID != s.Cursor.sourceID {
		return false, fmt.Errorf("cdc: cannot check a transaction from source %q against a snapshot of source %q", t.Cursor.sourceID, s.Cursor.sourceID)
	}
	if s.visibility == nil {
		return false, nil
	}
	return s.visibility.includes(t.xid), nil
}

// TransactionSink is watchd's local durability boundary. Returning nil means
// that the transaction has been accepted and PostgreSQL may be acknowledged.
// A sink must tolerate a transaction being delivered more than once.
type TransactionSink func(context.Context, Transaction) error

// SnapshotRowSink receives one batch of a scoped snapshot read at a time, in
// primary-key order, within the read's consistent snapshot transaction. Like
// Change.Values, row values are their PostgreSQL text representation or nil
// for SQL NULL. Returning an error aborts the read; no cursor is returned
// and the read's transaction is rolled back.
type SnapshotRowSink func(context.Context, []map[string]any) error

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
