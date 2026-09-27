package cdc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Bootstrap creates a new persistent slot, reads the requested scope at the
// snapshot PostgreSQL exports with it, and returns the matching resume
// cursor: the slot's consistent point. That exported snapshot sees exactly
// the transactions that commit before the consistent point, so the rows and
// the stream Bootstrap starts there meet with no gap and no overlap. The slot
// retains all changes after Cursor for a later Run call.
//
// Bootstrap deliberately refuses an existing slot: reusing one would mean
// reading against a source history whose starting point this call never
// established, so it cannot vouch for a gap-free boundary.
//
// If anything fails after the slot is created, Bootstrap drops the slot
// again, and reports a failed cleanup alongside the original error.
func (r *Reader) Bootstrap(ctx context.Context, spec ProjectionSpec, scope Scope, sink SnapshotRowSink) (_ Snapshot, err error) {
	if sink == nil {
		return Snapshot{}, ErrSnapshotSinkRequired
	}
	if err := r.validateSourceProjectionSpec(spec); err != nil {
		return Snapshot{}, err
	}

	stream, err := r.connectReplication(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	streamTransferred := false
	defer func() {
		if !streamTransferred {
			r.closeReplicationConnection(stream)
		}
	}()

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

	if err := r.checkRetentionBudget(ctx, management); err != nil {
		r.log(ctx, slog.LevelWarn, "could not check max_slot_wal_keep_size during bootstrap", "error", err)
	}

	slot, err := r.createBootstrapSlot(ctx, stream, management)
	if err != nil {
		return Snapshot{}, err
	}
	defer func() {
		if err == nil {
			return
		}
		if cleanupErr := r.dropSlot(stream, r.config.SlotName); cleanupErr != nil {
			err = fmt.Errorf("%w; clean up bootstrap replication slot: %v", err, cleanupErr)
		}
	}()

	cursor, err := pglogrepl.ParseLSN(slot.ConsistentPoint)
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: invalid slot consistent point", ErrPostgresServer)
	}

	// The exported snapshot stays valid only until the replication
	// connection runs its next command, so it is imported and fully read
	// before replication starts on that connection.
	visibility, err := r.readSnapshot(ctx, management, slot.SnapshotName, spec, scope, sink)
	if err != nil {
		return Snapshot{}, err
	}
	if err := r.startReplication(ctx, stream.Conn(), cursor); err != nil {
		return Snapshot{}, err
	}

	r.bootstrapMu.Lock()
	r.bootstrapStream = stream
	r.bootstrapLSN = cursor
	r.bootstrapMu.Unlock()
	streamTransferred = true
	r.setConnectionState("streaming")

	return newSnapshot(spec.SourceID, cursor, visibility), nil
}

// createBootstrapSlot creates the only safe initial source boundary: a new
// persistent replication slot, together with the snapshot PostgreSQL exports
// at the slot's consistent point.
func (r *Reader) createBootstrapSlot(ctx context.Context, stream *CDC, management *pgx.Conn) (pglogrepl.CreateReplicationSlotResult, error) {
	_, found, err := lookupSlot(ctx, management, r.config.SlotName)
	if err != nil {
		return pglogrepl.CreateReplicationSlotResult{}, err
	}
	if found {
		return pglogrepl.CreateReplicationSlotResult{}, ErrBootstrapSlotExists
	}

	slot, err := pglogrepl.CreateReplicationSlot(ctx, stream.Conn(), r.config.SlotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		SnapshotAction: "EXPORT_SNAPSHOT",
	})
	if err != nil {
		// A concurrent bootstrap may win after our lookup.
		if isDuplicateObject(err) {
			return pglogrepl.CreateReplicationSlotResult{}, ErrBootstrapSlotExists
		}
		return pglogrepl.CreateReplicationSlotResult{}, classifyPostgresError(err)
	}

	return slot, nil
}

func (r *Reader) dropSlot(stream *CDC, slotName string) error {
	// Closing first releases a slot that entered COPY mode immediately before
	// a bootstrap error or cancellation was observed locally.
	r.closeReplicationConnection(stream)

	cleanupCtx, cancel := context.WithTimeout(context.Background(), r.config.ShutdownTimeout)
	defer cancel()
	cleanupStream, err := r.connect(cleanupCtx, r.config.DatabaseURL)
	if err != nil {
		return classifyConnectionError(err)
	}
	defer r.closeReplicationConnection(cleanupStream)

	err = pglogrepl.DropReplicationSlot(cleanupCtx, cleanupStream.Conn(), slotName, pglogrepl.DropReplicationSlotOptions{})
	if postgresError, ok := errors.AsType[*pgconn.PgError](err); ok && postgresError.SQLState() == "42704" {
		return nil
	}
	return classifyPostgresError(err)
}
