package cdc

import (
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"go.opentelemetry.io/otel/metric"
)

const (
	defaultConnectionTimeout = 10 * time.Second
	defaultStatusInterval    = 10 * time.Second
	defaultShutdownTimeout   = 5 * time.Second
	defaultInitialBackoff    = 250 * time.Millisecond
	defaultMaxBackoff        = 10 * time.Second
	defaultMaxAttempts       = 8
	defaultJitter            = 0.20
	defaultSnapshotBatchRows = 1000
)

var postgresIdentifier = regexp.MustCompile(`^[a-z_][a-z0-9_$]{0,62}$`)

// RetryPolicy bounds reconnect attempts after transient connection failures.
// MaxAttempts counts retries after the initial attempt.
type RetryPolicy struct {
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	MaxAttempts    int
	Jitter         float64
}

// ReaderConfig configures one PostgreSQL logical-replication reader.
type ReaderConfig struct {
	// DatabaseURL is a normal PostgreSQL URL. The reader adds replication mode
	// for its streaming connection and removes it for management queries.
	DatabaseURL string
	// SourceID names this source. Every cursor the reader produces carries
	// it, so cursors from different sources are never compared by mistake.
	SourceID string
	// SlotName names the persistent PostgreSQL logical replication slot.
	SlotName string
	// PublicationName is the single v0 PostgreSQL publication to stream.
	PublicationName string

	// MaxTransactionBytes and MaxTransactionChanges bound the memory retained
	// for one uncommitted source transaction. MaxValueBytes bounds a single
	// column value independent of the whole-transaction bound; it must not
	// exceed MaxTransactionBytes.
	MaxTransactionBytes   int
	MaxTransactionChanges int
	MaxValueBytes         int
	// SnapshotBatchRows bounds how many rows Snapshot and Bootstrap read from
	// PostgreSQL per round trip. Rows are delivered to the caller's
	// SnapshotRowSink one batch at a time, so this also bounds how much of a
	// scoped snapshot read is held in memory at once.
	SnapshotBatchRows int
	// ConnectionTimeout bounds each new PostgreSQL replication or management
	// connection attempt. It does not bound an already-running stream.
	ConnectionTimeout time.Duration
	// StatusInterval controls how often the reader sends a standby-status
	// update while the source is idle.
	StatusInterval time.Duration
	// ShutdownTimeout bounds best-effort connection cleanup after Run returns.
	ShutdownTimeout time.Duration
	// RetryPolicy controls bounded reconnect behavior after transient failures.
	RetryPolicy RetryPolicy

	// MaxRetainedWALBytes bounds how much WAL watchd's own policy allows to
	// accumulate for its replication slot. It exists independently of the
	// PostgreSQL server's max_slot_wal_keep_size and is the only backstop
	// when that server setting is unbounded (-1, the PostgreSQL default).
	MaxRetainedWALBytes int64
	// RetentionCheckInterval controls how often Bootstrap's max_slot_wal_keep_size
	// check is repeated during Run, so a live server-side config reload is
	// noticed without a new Bootstrap.
	RetentionCheckInterval time.Duration
	// RetentionSampleInterval controls how often Run reads the slot's retained
	// WAL from pg_replication_slots, for the retention policy and metrics.
	RetentionSampleInterval time.Duration
	// RetentionWarnFraction and RetentionDegradeFraction are the retention
	// policy's thresholds, as fractions of the effective retention budget
	// (the stricter of MaxRetainedWALBytes and max_slot_wal_keep_size). They
	// default to 0.5 and 0.8, and must satisfy 0 < warn < degrade < 1. At
	// the budget itself, Run drops the slot and returns
	// ErrRetainedWALBudgetExceeded.
	RetentionWarnFraction    float64
	RetentionDegradeFraction float64

	// Meter is optional. The reader's instruments are created from it once,
	// in NewReader; nil records nothing. See docs/observability.md.
	Meter metric.Meter

	// Logger is optional. It never receives DatabaseURL, credentials, row
	// values, tenant identifiers, or other projection data.
	Logger *slog.Logger
}

func normalizeReaderConfig(config ReaderConfig) ReaderConfig {
	if config.MaxTransactionBytes == 0 {
		config.MaxTransactionBytes = defaultMaxTransactionBytes
	}
	if config.MaxTransactionChanges == 0 {
		config.MaxTransactionChanges = defaultMaxTransactionChanges
	}
	if config.MaxValueBytes == 0 {
		config.MaxValueBytes = defaultMaxValueBytes
	}
	if config.SnapshotBatchRows == 0 {
		config.SnapshotBatchRows = defaultSnapshotBatchRows
	}
	if config.ConnectionTimeout == 0 {
		config.ConnectionTimeout = defaultConnectionTimeout
	}
	if config.StatusInterval == 0 {
		config.StatusInterval = defaultStatusInterval
	}
	if config.ShutdownTimeout == 0 {
		config.ShutdownTimeout = defaultShutdownTimeout
	}
	if config.MaxRetainedWALBytes == 0 {
		config.MaxRetainedWALBytes = defaultMaxRetainedWALBytes
	}
	if config.RetentionCheckInterval == 0 {
		config.RetentionCheckInterval = defaultRetentionCheckInterval
	}
	if config.RetentionSampleInterval == 0 {
		config.RetentionSampleInterval = defaultRetentionSampleInterval
	}
	if config.RetentionWarnFraction == 0 {
		config.RetentionWarnFraction = defaultRetentionWarnFraction
	}
	if config.RetentionDegradeFraction == 0 {
		config.RetentionDegradeFraction = defaultRetentionDegradeFraction
	}
	if config.RetryPolicy.InitialBackoff == 0 {
		config.RetryPolicy.InitialBackoff = defaultInitialBackoff
	}
	if config.RetryPolicy.MaxBackoff == 0 {
		config.RetryPolicy.MaxBackoff = defaultMaxBackoff
	}
	if config.RetryPolicy.MaxAttempts == 0 {
		config.RetryPolicy.MaxAttempts = defaultMaxAttempts
	}
	if config.RetryPolicy.Jitter == 0 {
		config.RetryPolicy.Jitter = defaultJitter
	}
	return config
}

func validateReaderConfig(config ReaderConfig, sink TransactionSink) error {
	if config.DatabaseURL == "" || config.SourceID == "" || config.SlotName == "" || config.PublicationName == "" || sink == nil {
		return ErrInvalidReaderConfig
	}
	if !postgresIdentifier.MatchString(config.SlotName) || !postgresIdentifier.MatchString(config.PublicationName) {
		return fmt.Errorf("%w: slot and publication names must be unquoted PostgreSQL identifiers", ErrInvalidReaderConfig)
	}
	if config.MaxTransactionBytes <= 0 || config.MaxTransactionChanges <= 0 || config.MaxValueBytes <= 0 || config.SnapshotBatchRows <= 0 || config.ConnectionTimeout <= 0 || config.StatusInterval <= 0 || config.ShutdownTimeout <= 0 {
		return ErrInvalidReaderConfig
	}
	if config.MaxRetainedWALBytes <= 0 || config.RetentionCheckInterval <= 0 || config.RetentionSampleInterval <= 0 {
		return ErrInvalidReaderConfig
	}
	if !(0 < config.RetentionWarnFraction && config.RetentionWarnFraction < config.RetentionDegradeFraction && config.RetentionDegradeFraction < 1) {
		return fmt.Errorf("%w: retention thresholds must satisfy 0 < warn < degrade < 1", ErrInvalidReaderConfig)
	}
	if config.MaxValueBytes > config.MaxTransactionBytes {
		return fmt.Errorf("%w: MaxValueBytes must not exceed MaxTransactionBytes", ErrInvalidReaderConfig)
	}
	if config.RetryPolicy.InitialBackoff <= 0 || config.RetryPolicy.MaxBackoff < config.RetryPolicy.InitialBackoff || config.RetryPolicy.MaxAttempts < 0 || config.RetryPolicy.Jitter < 0 || config.RetryPolicy.Jitter > 1 {
		return ErrInvalidReaderConfig
	}
	return nil
}
