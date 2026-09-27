// Package watchd keeps a local projection of one PostgreSQL scope in sync
// with a watchd server, and reports whether it is fresh.
//
// Call Client.Sync with a Store. Sync installs a snapshot, applies every
// later committed change, persists the resume cursor, reconnects after
// failures, and rebuilds the projection whenever the server asks for a
// resync. The projection is fresh only while the server has confirmed it is
// complete (a Progress event) and the connection is up.
package watchd

// Value is one column value. Exactly one of these holds:
//
//   - Null is true: SQL NULL.
//   - Unchanged is true: an UPDATE left this (large) column unchanged and did
//     not resend it; keep the value you already have.
//   - Otherwise Text is the value's PostgreSQL text representation, for
//     every column type.
type Value struct {
	Text      string
	Null      bool
	Unchanged bool
}

// Row is one row, by column name.
type Row map[string]Value

// Operation is the kind of a Change.
type Operation int

const (
	Insert Operation = iota + 1
	Update
	Delete
)

func (o Operation) String() string {
	switch o {
	case Insert:
		return "insert"
	case Update:
		return "update"
	case Delete:
		return "delete"
	default:
		return "unknown"
	}
}

// Change is one row change.
type Change struct {
	Operation Operation
	// Table is the schema-qualified source table.
	Table string
	// Key identifies the row by its primary-key columns. Key values are
	// never NULL.
	Key map[string]string
	// Values is the row's new state for Insert and Update, and empty for
	// Delete.
	Values Row
}

// Batch is one committed source transaction's changes to this scope. It
// must be applied whole, and applying it twice must be harmless: after a
// reconnect a batch can be delivered again.
type Batch struct {
	// Cursor is the position just after the transaction; persist it with
	// the batch.
	Cursor  string
	Changes []Change
}

// State is the synchronization state of a projection.
type State struct {
	// Fresh reports that the projection is complete as of Cursor and the
	// connection is up. Anything else is stale: disconnected, rebuilding,
	// or not yet confirmed.
	Fresh bool
	// Cursor is the last position the projection is known complete at, or
	// empty before the first confirmation.
	Cursor string
}
