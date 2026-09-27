package cdc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// ErrRetainedWALBudgetExceeded indicates that the slot's retained WAL
// reached the effective retention budget, so the reader stopped and dropped
// its slot rather than let PostgreSQL run out of disk. The error says
// whether the drop succeeded. The source cannot resume: every consumer must
// rebuild from a new snapshot, on a new slot.
var ErrRetainedWALBudgetExceeded = errors.New("cdc: retained WAL reached the retention budget")

// retentionState is the retention policy's stage for the slot.
type retentionState int32

const (
	// retentionOK: retained WAL is below the warn threshold.
	retentionOK retentionState = iota
	// retentionWarn: retained WAL passed RetentionWarnFraction of the
	// budget. Something is holding acknowledgement back.
	retentionWarn
	// retentionDegrade: retained WAL passed RetentionDegradeFraction of the
	// budget. The terminal action is close; an operator should act now.
	retentionDegrade
	// retentionTerminal: retained WAL reached the budget. The slot is
	// dropped and the source stops. It is final for this reader.
	retentionTerminal
)

var retentionStateNames = []string{"ok", "warn", "degrade", "terminal"}

func (s retentionState) String() string { return retentionStateNames[s] }

const (
	defaultRetentionWarnFraction    = 0.5
	defaultRetentionDegradeFraction = 0.8
	// retentionHysteresis is how far, as a fraction of the budget, retained
	// WAL must fall below a state's threshold before the state is left, so a
	// slot hovering at a threshold does not flap.
	retentionHysteresis = 0.05
	// slotDropTimeout bounds the terminal action's slot drop. It does not
	// follow the caller's context: once taken, the action protects the
	// source even during shutdown.
	slotDropTimeout = 30 * time.Second
	// slotDropRetry is how long to wait while PostgreSQL releases the slot
	// from the replication connection the reader just closed.
	slotDropRetry = 200 * time.Millisecond
)

// nextRetentionState applies the retention policy to one sample: retained
// bytes of WAL against the effective budget. It rises to a state as soon as
// its threshold is reached, falls only with hysteresis, and never leaves
// retentionTerminal.
func nextRetentionState(current retentionState, retained, budget int64, warn, degrade float64) retentionState {
	if budget <= 0 || retained < 0 || current == retentionTerminal {
		return current
	}
	ratio := float64(retained) / float64(budget)
	target := retentionOK
	switch {
	case ratio >= 1:
		return retentionTerminal
	case ratio >= degrade:
		target = retentionDegrade
	case ratio >= warn:
		target = retentionWarn
	}
	for current > target {
		threshold := warn
		if current == retentionDegrade {
			threshold = degrade
		}
		if ratio >= threshold-retentionHysteresis {
			break
		}
		current--
	}
	return max(current, target)
}

// applyRetentionPolicy moves the policy to the state one sample calls for,
// logging and counting every transition. Reaching retentionTerminal calls
// stopStream, which ends Run's stream so Run can drop the slot.
func (r *Reader) applyRetentionPolicy(ctx context.Context, retained, budget int64, stopStream context.CancelCauseFunc) {
	current := retentionState(r.metrics.retentionState.Load())
	next := nextRetentionState(current, retained, budget, r.config.RetentionWarnFraction, r.config.RetentionDegradeFraction)
	if next == current {
		return
	}
	r.metrics.retentionState.Store(int32(next))
	r.metrics.retentionTransitions.Add(context.WithoutCancel(ctx), 1, r.metrics.retentionStateOpts[next])
	r.setRetentionState(next)

	args := []any{"slot", r.config.SlotName, "retained_wal_bytes", retained, "budget_bytes", budget, "from", current.String(), "to", next.String()}
	switch next {
	case retentionOK:
		r.log(ctx, slog.LevelInfo, "retained WAL is back within the retention budget", args...)
	case retentionWarn:
		r.log(ctx, slog.LevelWarn, "retained WAL passed the retention warn threshold: acknowledgement is being held back", args...)
	case retentionDegrade:
		r.log(ctx, slog.LevelError, "retained WAL passed the retention degrade threshold: the slot will be dropped when the budget is reached", args...)
	case retentionTerminal:
		r.log(ctx, slog.LevelError, "retained WAL reached the retention budget: dropping the replication slot to protect the source; every consumer must rebuild", args...)
		stopStream(ErrRetainedWALBudgetExceeded)
	}
}

// dropSlotForBudget is the terminal action: it drops the reader's slot
// once Run has closed its replication connection. PostgreSQL releases the
// slot from that connection asynchronously, so a slot still active is
// retried until slotDropTimeout.
func (r *Reader) dropSlotForBudget(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), slotDropTimeout)
	defer cancel()
	management, err := r.connectManagementWithTimeout(ctx)
	if err != nil {
		return err
	}
	defer r.closeManagementConnection(management)

	for {
		_, err := management.Exec(ctx, "SELECT pg_drop_replication_slot($1)", r.config.SlotName)
		postgresError, isPostgres := errors.AsType[*pgconn.PgError](err)
		switch {
		case err == nil:
			return nil
		case isPostgres && postgresError.SQLState() == "42704": // already gone
			return nil
		case isPostgres && postgresError.SQLState() == "55006": // still active
			if err := r.wait(ctx, slotDropRetry); err != nil {
				return fmt.Errorf("%w: slot %s is still active", err, r.config.SlotName)
			}
		default:
			return classifyPostgresError(err)
		}
	}
}
