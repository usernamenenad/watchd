package cdc

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// Snapshot reads one scope's rows plus a resume cursor, using the reader's
// existing slot instead of creating one. Unlike Bootstrap, it can be called
// for any number of scopes, at any time, concurrently with Run and with
// other Snapshot calls.
//
// The rows and the stream meet without a gap: every committed transaction is
// either in the rows or reported by Snapshot.Covers as missing from them.
// Cursor is read before the snapshot is fixed, so it precedes every commit
// the rows cannot see (with the rare exception documented on Covers), and
// resuming from it only ever replays extra transactions, never skips one.
//
// Cursor is only safe to resume from if the slot has retained every change
// after it. Snapshot checks that and returns ErrSnapshotWindowClosed if the
// slot has already moved past Cursor - the caller should just retry with a
// fresh snapshot.
func (r *Reader) Snapshot(ctx context.Context, spec ProjectionSpec, scope Scope, sink SnapshotRowSink) (Snapshot, error) {
	if sink == nil {
		return Snapshot{}, ErrSnapshotSinkRequired
	}
	if err := r.validateSourceProjectionSpec(spec); err != nil {
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

	// Read the WAL insert position before the snapshot is fixed: any
	// transaction the snapshot cannot see commits after this point.
	var cursorText string
	if err := management.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()").Scan(&cursorText); err != nil {
		return Snapshot{}, classifyPostgresError(err)
	}
	cursor, err := pglogrepl.ParseLSN(cursorText)
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: invalid snapshot cursor", ErrPostgresServer)
	}

	visibility, err := r.readSnapshot(ctx, management, "", spec, scope, sink)
	if err != nil {
		return Snapshot{}, err
	}

	if err := r.checkReplayWindow(ctx, management, cursor); err != nil {
		return Snapshot{}, err
	}

	return newSnapshot(spec.SourceID, cursor, visibility), nil
}

// exportedSnapshotName matches the identifiers pg_export_snapshot and
// CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT return, such as
// "00000003-0000001B-1".
var exportedSnapshotName = regexp.MustCompile(`^[0-9A-F]+(-[0-9A-F]+)+$`)

func newSnapshot(sourceID string, cursor pglogrepl.LSN, visibility *visibility) Snapshot {
	return Snapshot{
		SourceID:   sourceID,
		Cursor:     Cursor{sourceID: sourceID, lsn: cursor},
		visibility: visibility,
	}
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

// readSnapshot reads one scope inside a repeatable-read transaction and
// returns that transaction's MVCC snapshot, which decides exactly which
// source transactions the rows already contain (Snapshot.Covers).
//
// With importSnapshot empty, the transaction's first statement fixes a fresh
// snapshot. Otherwise it adopts a snapshot exported by another session -
// Bootstrap uses the one PostgreSQL exports when it creates the slot, which
// matches the slot's consistent point exactly.
func (r *Reader) readSnapshot(
	ctx context.Context,
	conn *pgx.Conn,
	importSnapshot string,
	spec ProjectionSpec,
	scope Scope,
	sink SnapshotRowSink,
) (*visibility, error) {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, classifyPostgresError(err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), r.config.ShutdownTimeout)
		defer cancel()
		_ = tx.Rollback(cleanupCtx)
	}()

	if importSnapshot != "" {
		if !exportedSnapshotName.MatchString(importSnapshot) {
			return nil, fmt.Errorf("%w: invalid exported snapshot name", ErrPostgresServer)
		}
		// SET TRANSACTION SNAPSHOT takes no bind parameters; the name is
		// validated above and comes from PostgreSQL itself.
		if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT '"+importSnapshot+"'"); err != nil {
			return nil, classifyPostgresError(err)
		}
	}

	var snapshotText string
	if err := tx.QueryRow(ctx, "SELECT pg_current_snapshot()::text").Scan(&snapshotText); err != nil {
		return nil, classifyPostgresError(err)
	}
	visibility, err := parseVisibility(snapshotText)
	if err != nil {
		return nil, err
	}

	for _, hook := range []func(context.Context) error{r.afterSnapshotFixed, r.beforeSnapshotRead} {
		if hook != nil {
			if err := hook(ctx); err != nil {
				return nil, err
			}
		}
	}

	if err := scanScopedRows(ctx, tx, spec, scope, r.config.SnapshotBatchRows, sink); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, classifyPostgresError(err)
	}

	return visibility, nil
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
