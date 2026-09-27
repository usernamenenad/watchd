package cdc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// defaultMaxRetainedWALBytes is the default cap on watchd's own retention
	// policy. It exists so that leaving MaxRetainedWALBytes unset never means
	// "unbounded" - see issue #22's requirement for no unsafe production
	// default.
	defaultMaxRetainedWALBytes = 1 << 30 // 1 GiB
	// defaultRetentionCheckInterval bounds how often Run re-reads the
	// server's max_slot_wal_keep_size, so a live Postgres config reload is
	// noticed without waiting for the next Bootstrap.
	defaultRetentionCheckInterval = 5 * time.Minute
	// defaultRetentionSampleInterval bounds how stale the retained-WAL
	// metrics can be.
	defaultRetentionSampleInterval = 30 * time.Second
)

// walKeepSizeUnitMultipliers converts pg_settings.unit for
// max_slot_wal_keep_size (and similarly-unitted GUCs) to bytes.
var walKeepSizeUnitMultipliers = map[string]int64{
	"B":  1,
	"kB": 1024,
	"MB": 1024 * 1024,
	"GB": 1024 * 1024 * 1024,
	"TB": 1024 * 1024 * 1024 * 1024,
}

// monitorRetentionBudget runs for as long as Run is active. Every
// RetentionCheckInterval it re-reads the server's max_slot_wal_keep_size, so
// a live Postgres config reload (ALTER SYSTEM + pg_reload_conf) is noticed
// without waiting for the next Bootstrap. Every RetentionSampleInterval it
// samples the slot's retained WAL for the retention metrics. It runs
// independently of the replication connection's own reconnect cycle, using
// short-lived management connections, and never fails Run - errors are
// logged and skipped.
func (r *Reader) monitorRetentionBudget(ctx context.Context) {
	check := time.NewTicker(r.config.RetentionCheckInterval)
	defer check.Stop()
	sample := time.NewTicker(r.config.RetentionSampleInterval)
	defer sample.Stop()

	for {
		var (
			task func(context.Context, *pgx.Conn) error
			name string
		)
		select {
		case <-ctx.Done():
			return
		case <-check.C:
			task, name = r.checkRetentionBudget, "max_slot_wal_keep_size check"
		case <-sample.C:
			task, name = r.sampleRetention, "retained WAL sample"
		}
		management, err := r.connectManagementWithTimeout(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.log(ctx, slog.LevelWarn, "skipping periodic "+name+": could not connect", "error", err)
			continue
		}
		if err := task(ctx, management); err != nil && ctx.Err() == nil {
			r.log(ctx, slog.LevelWarn, "skipping periodic "+name, "error", err)
		}
		r.closeManagementConnection(management)
	}
}

// sampleRetention reads how much WAL PostgreSQL retains for the slot, how
// much more it may write before invalidating the slot, and the effective
// retention budget, for the retention metrics. Enforcing the budget is
// #64's; this only measures.
func (r *Reader) sampleRetention(ctx context.Context, management *pgx.Conn) error {
	var (
		retained, safe *int64
		status         string
	)
	err := management.QueryRow(ctx, `
		SELECT (CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn()
		             ELSE pg_current_wal_lsn() END - restart_lsn)::bigint,
		       COALESCE(wal_status, ''),
		       safe_wal_size
		FROM pg_replication_slots
		WHERE slot_name = $1`, r.config.SlotName).Scan(&retained, &status, &safe)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrSlotNotFound
	}
	if err != nil {
		return classifyPostgresError(err)
	}
	serverBytes, unbounded, err := readMaxSlotWALKeepSize(ctx, management)
	if err != nil {
		return err
	}

	metrics := r.metrics
	metrics.retainedWAL.Store(valueOr(retained, -1))
	metrics.safeWALSize.Store(valueOr(safe, -1))
	metrics.walStatus.Store(-1)
	for i, known := range walStatuses {
		if status == known {
			metrics.walStatus.Store(int32(i))
		}
	}
	metrics.effectiveBudget.Store(effectiveRetentionBudget(r.config.MaxRetainedWALBytes, serverBytes, unbounded))
	metrics.retentionKnown.Store(true)
	return nil
}

func valueOr(value *int64, fallback int64) int64 {
	if value == nil {
		return fallback
	}
	return *value
}

// checkRetentionBudget reads the server's max_slot_wal_keep_size and warns
// when it leaves watchd's own configured retention budget as the sole
// backstop against unbounded WAL growth (server value is -1), or when the
// server is stricter than watchd's own budget and would invalidate the slot
// before watchd's own policy would act on it.
func (r *Reader) checkRetentionBudget(ctx context.Context, management *pgx.Conn) error {
	serverBytes, unbounded, err := readMaxSlotWALKeepSize(ctx, management)
	if err != nil {
		return err
	}

	if unbounded {
		r.log(ctx, slog.LevelWarn,
			"PostgreSQL max_slot_wal_keep_size is unbounded (-1): source provides no WAL retention backstop; relying entirely on watchd's configured retention budget",
			"slot", r.config.SlotName, "configured_budget_bytes", r.config.MaxRetainedWALBytes)
		return nil
	}

	effective := effectiveRetentionBudget(r.config.MaxRetainedWALBytes, serverBytes, unbounded)
	if effective < r.config.MaxRetainedWALBytes {
		r.log(ctx, slog.LevelWarn,
			"PostgreSQL max_slot_wal_keep_size is stricter than watchd's configured retention budget; effective budget clamped to the server limit",
			"slot", r.config.SlotName, "configured_budget_bytes", r.config.MaxRetainedWALBytes, "server_max_slot_wal_keep_size_bytes", serverBytes)
	}
	return nil
}

// readMaxSlotWALKeepSize reads the server's max_slot_wal_keep_size GUC in
// bytes. It reads pg_settings' raw setting/unit columns rather than SHOW's
// pretty-printed string, since the unit varies by server and -1 is the
// sentinel for unbounded retention regardless of unit.
func readMaxSlotWALKeepSize(ctx context.Context, conn *pgx.Conn) (bytes int64, unbounded bool, err error) {
	var setting, unit string
	err = conn.QueryRow(ctx, `
		SELECT setting, unit
		FROM pg_settings
		WHERE name = 'max_slot_wal_keep_size'`).Scan(&setting, &unit)
	if err != nil {
		return 0, false, classifyPostgresError(err)
	}

	value, parseErr := strconv.ParseInt(setting, 10, 64)
	if parseErr != nil {
		return 0, false, fmt.Errorf("%w: unexpected max_slot_wal_keep_size setting %q", ErrPostgresServer, setting)
	}
	if value < 0 {
		return 0, true, nil
	}

	multiplier, ok := walKeepSizeUnitMultipliers[unit]
	if !ok {
		return 0, false, fmt.Errorf("%w: unexpected max_slot_wal_keep_size unit %q", ErrPostgresServer, unit)
	}
	return value * multiplier, false, nil
}

// effectiveRetentionBudget picks the stricter of watchd's own configured
// retention budget and the server's max_slot_wal_keep_size. When the server
// is unbounded, watchd's own budget is the only backstop.
func effectiveRetentionBudget(configuredBytes, serverBytes int64, serverUnbounded bool) int64 {
	if serverUnbounded || serverBytes >= configuredBytes {
		return configuredBytes
	}
	return serverBytes
}
