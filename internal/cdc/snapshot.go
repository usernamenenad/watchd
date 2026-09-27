package cdc

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// Snapshot reads one scope's rows plus a resume cursor, using the reader's
// existing slot instead of creating one. Unlike Bootstrap, it can be called
// for any number of scopes, at any time, concurrently with Run and with
// other Snapshot calls.
//
// Cursor is only safe to resume from if the slot has retained every change
// after it. Snapshot checks that and returns ErrSnapshotWindowClosed if the
// slot has already moved past Cursor - the caller should just retry with a
// fresh snapshot.
func (r *Reader) Snapshot(ctx context.Context, spec ProjectionSpec, scope Scope, sink SnapshotRowSink) (Snapshot, error) {
	if sink == nil {
		return Snapshot{}, ErrSnapshotSinkRequired
	}
	if err := validateProjectionSpecConfig(spec); err != nil {
		return Snapshot{}, err
	}

	management, err := r.connectManagementWithTimeout(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	defer r.closeManagementConnection(management)

	if err := r.validatePublication(ctx, management); err != nil {
		return Snapshot{}, err
	}
	if err := r.validateProjectionSpec(ctx, management, spec); err != nil {
		return Snapshot{}, err
	}

	slot, found, err := lookupSlot(ctx, management, r.config.SlotName)
	if err != nil {
		return Snapshot{}, err
	}
	if !found {
		return Snapshot{}, ErrSlotNotFound
	}
	// Unlike requireSlot, Snapshot does not need the slot to be inactive: it
	// never takes over the slot's replication connection, so it must work
	// while Run owns the slot and other Snapshot calls run concurrently.
	if slot.slotType != "logical" || slot.plugin != "pgoutput" {
		return Snapshot{}, ErrSlotInvalidated
	}

	cursor, err := readSnapshot(ctx, management, spec, scope, r.config.ShutdownTimeout, r.config.SnapshotBatchRows, r.beforeSnapshotRead, sink)
	if err != nil {
		return Snapshot{}, err
	}

	if err := r.checkReplayWindow(ctx, management, cursor); err != nil {
		return Snapshot{}, err
	}

	return Snapshot{
		SourceID: spec.SourceID,
		Cursor: Cursor{
			sourceID: spec.SourceID,
			lsn:      cursor,
		},
	}, nil
}

// checkReplayWindow re-reads the slot's retained boundary after a snapshot
// read commits and confirms cursor is still at or after it. restart_lsn is
// the oldest WAL position PostgreSQL still guarantees to retain for the
// slot; a cursor behind it means changes between the two may already be
// gone, so resuming from cursor could silently skip them.
func (r *Reader) checkReplayWindow(ctx context.Context, management *pgx.Conn, cursor pglogrepl.LSN) error {
	slot, found, err := lookupSlot(ctx, management, r.config.SlotName)
	if err != nil {
		return err
	}
	if !found {
		return ErrSlotNotFound
	}
	restartLSN, err := parseLSN(slot.restartLSN)
	if err != nil {
		return err
	}
	if cursor < restartLSN {
		return ErrSnapshotWindowClosed
	}
	return nil
}

// readSnapshot takes a gap-free scoped read paired with a resume cursor.
// Exporting this transaction's own snapshot before reading
// pg_current_wal_lsn() pins a definite instant: the recorded cursor is
// guaranteed at or after everything the read can see, so replaying after it
// never re-delivers a snapshotted row. Both Bootstrap and Snapshot use this
// same function - Bootstrap needs no exported slot-creation snapshot,
// because this one, taken right after the slot exists, is just as gap-free.
func readSnapshot(
	ctx context.Context,
	conn *pgx.Conn,
	spec ProjectionSpec,
	scope Scope,
	cleanupTimeout time.Duration,
	batchRows int,
	beforeRead func(context.Context) error,
	sink SnapshotRowSink,
) (pglogrepl.LSN, error) {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, classifyPostgresError(err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		_ = tx.Rollback(cleanupCtx)
	}()

	var snapshotName string
	if err := tx.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshotName); err != nil {
		return 0, classifyPostgresError(err)
	}

	var cursorText string
	if err := tx.QueryRow(ctx, "SELECT pg_current_wal_lsn()").Scan(&cursorText); err != nil {
		return 0, classifyPostgresError(err)
	}
	cursor, err := pglogrepl.ParseLSN(cursorText)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid snapshot cursor", ErrPostgresServer)
	}

	if beforeRead != nil {
		if err := beforeRead(ctx); err != nil {
			return 0, err
		}
	}

	if err := scanScopedRows(ctx, tx, spec, scope, batchRows, sink); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, classifyPostgresError(err)
	}

	return cursor, nil
}

// scanScopedRows reads a scope in primary-key-ordered batches of at most
// batchRows, invoking sink once per batch instead of holding the whole
// scoped result set in memory. Every batch runs inside tx's existing
// snapshot, so pagination sees no more or less than a single unbounded read
// of the same scope would have.
func scanScopedRows(ctx context.Context, tx pgx.Tx, spec ProjectionSpec, scope Scope, batchRows int, sink SnapshotRowSink) error {
	tableName := pgx.Identifier{spec.Schema, spec.Table}.Sanitize()
	scopeColumn := pgx.Identifier{spec.ScopeColumn}.Sanitize()
	pkColumns := make([]string, len(spec.PrimaryKey))

	for index, column := range spec.PrimaryKey {
		pkColumns[index] = pgx.Identifier{column}.Sanitize()
	}

	orderBy := strings.Join(pkColumns, ", ")
	pkTuple := "(" + orderBy + ")"

	fetchBatch := func(lastKey []string) ([]map[string]any, error) {
		queryArgs := []any{scope.Value}
		query := fmt.Sprintf("SELECT * FROM %s WHERE %s = $1", tableName, scopeColumn)
		if lastKey != nil {
			placeholders := make([]string, len(lastKey))
			for index, value := range lastKey {
				queryArgs = append(queryArgs, value)
				placeholders[index] = fmt.Sprintf("$%d", len(queryArgs))
			}
			query += fmt.Sprintf(" AND %s > (%s)", pkTuple, strings.Join(placeholders, ", "))
		}
		queryArgs = append(queryArgs, batchRows)
		query += fmt.Sprintf(" ORDER BY %s LIMIT $%d", orderBy, len(queryArgs))

		args := append([]any{pgx.QueryResultFormats{pgx.TextFormatCode}}, queryArgs...)
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return nil, classifyPostgresError(err)
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		batch := make([]map[string]any, 0, batchRows)
		for rows.Next() {
			values := rows.RawValues()
			row := make(map[string]any, len(fields))
			for index, field := range fields {
				if values[index] == nil {
					row[field.Name] = nil
					continue
				}
				row[field.Name] = string(values[index])
			}
			batch = append(batch, row)
		}

		if err := rows.Err(); err != nil {
			return nil, classifyPostgresError(err)
		}

		return batch, nil
	}

	var lastKey []string
	for {
		batch, err := fetchBatch(lastKey)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}

		if err := sink(ctx, batch); err != nil {
			return err
		}

		lastRow := batch[len(batch)-1]
		nextKey := make([]string, len(spec.PrimaryKey))
		for index, column := range spec.PrimaryKey {
			// Primary-key columns can never be SQL NULL, so a non-string
			// value here means the row didn't actually contain this column.
			value, ok := lastRow[column].(string)
			if !ok {
				return fmt.Errorf("%w: primary key column %q missing or NULL in scanned row", ErrPostgresServer, column)
			}
			nextKey[index] = value
		}
		lastKey = nextKey

		if len(batch) < batchRows {
			return nil
		}
	}
}
