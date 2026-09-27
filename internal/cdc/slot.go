package cdc

import (
	"context"
	"errors"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

func (r *Reader) requireSlot(ctx context.Context) (pglogrepl.LSN, error) {
	management, err := r.connectManagementWithTimeout(ctx)
	if err != nil {
		return 0, err
	}
	defer r.closeManagementConnection(management)

	if err := r.validatePublication(ctx, management); err != nil {
		return 0, err
	}

	slot, found, err := lookupSlot(ctx, management, r.config.SlotName)
	if err != nil {
		return 0, err
	}
	if found {
		if err := validateSlot(slot); err != nil {
			return 0, err
		}
		return parseLSN(slot.resumeLSN)
	}
	return 0, ErrSlotInvalidated
}

type slotState struct {
	slotType   string
	plugin     string
	active     bool
	resumeLSN  string
	restartLSN string
}

func lookupSlot(ctx context.Context, conn *pgx.Conn, slotName string) (slotState, bool, error) {
	var slot slotState
	err := conn.QueryRow(ctx, `
		SELECT slot_type, plugin, active,
		       COALESCE(confirmed_flush_lsn::text, restart_lsn::text, ''),
		       COALESCE(restart_lsn::text, '')
		FROM pg_replication_slots
		WHERE slot_name = $1`, slotName).Scan(&slot.slotType, &slot.plugin, &slot.active, &slot.resumeLSN, &slot.restartLSN)
	if errors.Is(err, pgx.ErrNoRows) {
		return slotState{}, false, nil
	}
	if err != nil {
		return slotState{}, false, classifyPostgresError(err)
	}

	return slot, true, nil
}

func validateSlot(slot slotState) error {
	if slot.slotType != "logical" || slot.plugin != "pgoutput" || slot.resumeLSN == "" {
		return ErrSlotInvalidated
	}
	if slot.active {
		return retryableError(ErrSlotInUse, "")
	}
	return nil
}

func parseLSN(value string) (pglogrepl.LSN, error) {
	lsn, err := pglogrepl.ParseLSN(value)
	if err != nil {
		return 0, ErrSlotInvalidated
	}

	return lsn, nil
}
