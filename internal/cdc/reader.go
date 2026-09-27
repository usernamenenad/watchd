package cdc

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ReaderStats is a point-in-time, process-local snapshot. Issue #5 can expose
// these values through a metrics exporter without coupling CDC to one today.
type ReaderStats struct {
	ConnectionState      string
	LastReceivedLSN      string
	LastAcknowledgedLSN  string
	TransactionsReceived uint64
	TransactionsAccepted uint64
	DecodeErrors         uint64
	ReconnectAttempts    uint64
	InFlightBytes        int
	InFlightChanges      int
	// RetentionState is the retention policy's state for the slot: "ok",
	// "warn", "degrade", or "terminal". It is "ok" until the first sample.
	RetentionState string
}

// Reader owns the lifecycle of one PostgreSQL logical replication source.
// It opens connections, streams pgoutput, emits committed batches, and only
// acknowledges batches after its TransactionSink accepts them.
type Reader struct {
	config ReaderConfig
	sink   TransactionSink

	connect           func(context.Context, string) (*CDC, error)
	connectManagement func(context.Context, string) (*pgx.Conn, error)
	sendStandbyStatus func(context.Context, *pgconn.PgConn, pglogrepl.LSN) error
	wait              func(context.Context, time.Duration) error
	now               func() time.Time
	random            func() float64

	statsMu sync.RWMutex
	stats   ReaderStats
	metrics *readerMetrics

	bootstrapMu     sync.Mutex
	bootstrapStream *CDC
	bootstrapLSN    pglogrepl.LSN

	// Test hooks for snapshot-boundary races. afterSnapshotFixed runs once
	// the read's MVCC snapshot is fixed; beforeSnapshotRead runs just before
	// the scoped rows are read.
	afterSnapshotFixed func(context.Context) error
	beforeSnapshotRead func(context.Context) error
}

// NewReader validates configuration and creates a reader. It opens no network
// connections; call Run to begin streaming.
func NewReader(config ReaderConfig, sink TransactionSink) (*Reader, error) {
	config = normalizeReaderConfig(config)
	if err := validateReaderConfig(config, sink); err != nil {
		return nil, err
	}

	metrics, err := newReaderMetrics(config.Meter, config)
	if err != nil {
		return nil, fmt.Errorf("cdc: create instruments: %w", err)
	}

	return &Reader{
		config:            config,
		metrics:           metrics,
		sink:              sink,
		connect:           Connect,
		connectManagement: connectManagement,
		sendStandbyStatus: sendStandbyStatus,
		wait:              waitContext,
		now:               time.Now,
		random:            rand.Float64,
		stats: ReaderStats{
			ConnectionState: stateIdle,
			RetentionState:  retentionOK.String(),
		},
	}, nil
}

// Stats returns a coherent, process-local point-in-time reader snapshot.
func (r *Reader) Stats() ReaderStats {
	r.statsMu.RLock()
	defer r.statsMu.RUnlock()

	return r.stats
}

func (r *Reader) setConnectionState(state string) {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()

	r.stats.ConnectionState = state
	r.metrics.setState(state)
}

func (r *Reader) setRetentionState(state retentionState) {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()

	r.stats.RetentionState = state.String()
}

func (r *Reader) setLastReceivedLSN(lsn pglogrepl.LSN) {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()

	r.stats.LastReceivedLSN = lsn.String()
	r.metrics.receivedLSN.Store(uint64(lsn))
}

func (r *Reader) setLastAcknowledgedLSN(lsn pglogrepl.LSN) {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()

	r.stats.LastAcknowledgedLSN = lsn.String()
	r.metrics.ackedLSN.Store(uint64(lsn))
}

func (r *Reader) setInFlight(changes, bytes int) {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()

	r.stats.InFlightChanges = changes
	r.stats.InFlightBytes = bytes
}

func (r *Reader) incrementTransactionsReceived() {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()

	r.stats.TransactionsReceived++
}

func (r *Reader) incrementTransactionsAccepted() {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()

	r.stats.TransactionsAccepted++
}

func (r *Reader) incrementDecodeErrors() {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()

	r.stats.DecodeErrors++
	r.metrics.decodeErrors.Add(context.Background(), 1)
}

func (r *Reader) incrementReconnects() {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()

	r.stats.ReconnectAttempts++
	r.metrics.reconnects.Add(context.Background(), 1)
}

func (r *Reader) log(ctx context.Context, level slog.Level, message string, args ...any) {
	if r.config.Logger != nil {
		r.config.Logger.Log(ctx, level, message, args...)
	}
}
