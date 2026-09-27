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

// Bootstrap creates a new persistent slot, reads the requested scope at a
// snapshot taken right after, and returns the matching resume cursor. The
// slot retains all changes after Cursor for a later Run call.
//
// Bootstrap deliberately refuses an existing slot: reusing one would mean
// reading against a source history whose starting point this call never
// established, so it cannot vouch for a gap-free boundary.
func (r *Reader) Bootstrap(ctx context.Context, spec ProjectionSpec, scope Scope, sink SnapshotRowSink) (Snapshot, error) {
	if sink == nil {
		return Snapshot{}, ErrSnapshotSinkRequired
	}
	if err := validateProjectionSpecConfig(spec); err != nil {
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

	if err := r.createBootstrapSlot(ctx, stream, management); err != nil {
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

	// The slot now exists, so the first scope's rows and resume cursor come
	// from the same readSnapshot every later scope uses via Snapshot - there
	// is no need for the slot's own exported snapshot.
	cursor, err := readSnapshot(ctx, management, spec, scope, r.config.ShutdownTimeout, r.config.SnapshotBatchRows, r.beforeSnapshotRead, sink)
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

	return Snapshot{
		SourceID: spec.SourceID,
		Cursor: Cursor{
			sourceID: spec.SourceID,
			lsn:      cursor,
		},
	}, nil
}

// createBootstrapSlot creates the only safe initial source boundary: a new
// persistent replication slot. It does not export a snapshot itself -
// readSnapshot takes its own once the slot exists, the same way Snapshot
// does for every later scope.
func (r *Reader) createBootstrapSlot(ctx context.Context, stream *CDC, management *pgx.Conn) error {
	_, found, err := lookupSlot(ctx, management, r.config.SlotName)
	if err != nil {
		return err
	}
	if found {
		return ErrBootstrapSlotExists
	}

	_, err = pglogrepl.CreateReplicationSlot(ctx, stream.Conn(), r.config.SlotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{})
	if err != nil {
		// A concurrent bootstrap may win after our lookup.
		if isDuplicateObject(err) {
			return ErrBootstrapSlotExists
		}
		return classifyPostgresError(err)
	}

	return nil
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
