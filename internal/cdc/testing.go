package cdc

import "github.com/jackc/pglogrepl"

// NewTransactionForTest builds a Transaction as the Reader would deliver it,
// for tests in other watchd packages: commitLSN is where its commit record
// starts, endLSN is its cursor, and xid is its source transaction ID.
func NewTransactionForTest(sourceID string, commitLSN, endLSN uint64, xid uint32, changes ...Change) Transaction {
	return Transaction{
		Cursor:    Cursor{sourceID: sourceID, lsn: pglogrepl.LSN(endLSN)},
		Changes:   changes,
		commitLSN: pglogrepl.LSN(commitLSN),
		xid:       xid,
	}
}

// NewSnapshotForTest builds a Snapshot, for tests in other watchd packages.
// Its rows contain every transaction whose ID is below xmax and not listed
// in inProgress, like a PostgreSQL MVCC snapshot "xmin:xmax:inProgress".
func NewSnapshotForTest(sourceID string, cursor uint64, xmin, xmax uint64, inProgress ...uint64) Snapshot {
	running := make(map[uint64]struct{}, len(inProgress))
	for _, xid := range inProgress {
		running[xid] = struct{}{}
	}
	return newSnapshot(sourceID, pglogrepl.LSN(cursor), &visibility{xmin: xmin, xmax: xmax, inProgress: running})
}

// NewCursorForTest builds a cursor of sourceID at lsn, for tests in other
// watchd packages.
func NewCursorForTest(sourceID string, lsn uint64) Cursor {
	return Cursor{sourceID: sourceID, lsn: pglogrepl.LSN(lsn)}
}
