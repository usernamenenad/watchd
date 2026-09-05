package cdc

import "fmt"

const (
	OperationInsert = "insert"
	OperationUpdate = "update"
	OperationDelete = "delete"
)

// Change is one row mutation from a PostgreSQL projection table.
//
// Key contains the stable replica-identity columns needed to identify a row.
// Values contains the resulting row state for inserts and updates. A value can
// be a string, nil (SQL NULL), []byte, or UnchangedToast.
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

// Snapshot is a consistent scoped read paired with the opaque cursor at
// which replication must resume. Like Change.Values, row values are their
// PostgreSQL text representation or nil for SQL NULL.
type Snapshot struct {
	SourceID string
	Cursor   string
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
