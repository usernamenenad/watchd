package cdc

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
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

	// Stats that change on every message or transaction are atomics, so the
	// streaming path takes no lock; the two rarely-changing states share
	// statsMu. LSNs are formatted only when Stats is called.
	statsMu              sync.RWMutex
	connectionState      string
	retentionState       string
	lastReceivedLSN      lsnStat
	lastAcknowledgedLSN  lsnStat
	transactionsReceived atomic.Uint64
	transactionsAccepted atomic.Uint64
	decodeErrors         atomic.Uint64
	reconnectAttempts    atomic.Uint64
	inFlightChanges      atomic.Int64
	inFlightBytes        atomic.Int64
	metrics              *readerMetrics
	shapes               shapes

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
		connectionState:   stateIdle,
		retentionState:    retentionOK.String(),
	}, nil
}

// Stats returns a process-local snapshot of the reader. Each field is
// current when read, but fields are read independently, so a snapshot taken
// while the reader streams may combine values from adjacent messages.
func (r *Reader) Stats() ReaderStats {
	r.statsMu.RLock()
	stats := ReaderStats{
		ConnectionState: r.connectionState,
		RetentionState:  r.retentionState,
	}
	r.statsMu.RUnlock()

	stats.LastReceivedLSN = r.lastReceivedLSN.String()
	stats.LastAcknowledgedLSN = r.lastAcknowledgedLSN.String()
	stats.TransactionsReceived = r.transactionsReceived.Load()
	stats.TransactionsAccepted = r.transactionsAccepted.Load()
	stats.DecodeErrors = r.decodeErrors.Load()
	stats.ReconnectAttempts = r.reconnectAttempts.Load()
	stats.InFlightChanges = int(r.inFlightChanges.Load())
	stats.InFlightBytes = int(r.inFlightBytes.Load())
	return stats
}

// lsnStat is an LSN statistic that formats only when read. It reports empty
// until the first Store, as ReaderStats always has.
type lsnStat struct {
	lsn atomic.Uint64
	set atomic.Bool
}

func (s *lsnStat) Store(lsn pglogrepl.LSN) {
	s.lsn.Store(uint64(lsn))
	s.set.Store(true)
}

func (s *lsnStat) String() string {
	if !s.set.Load() {
		return ""
	}
	return pglogrepl.LSN(s.lsn.Load()).String()
}

func (r *Reader) setConnectionState(state string) {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()

	r.connectionState = state
	r.metrics.setState(state)
}

func (r *Reader) setRetentionState(state retentionState) {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()

	r.retentionState = state.String()
}

func (r *Reader) setLastReceivedLSN(lsn pglogrepl.LSN) {
	r.lastReceivedLSN.Store(lsn)
	r.metrics.receivedLSN.Store(uint64(lsn))
}

func (r *Reader) setLastAcknowledgedLSN(lsn pglogrepl.LSN) {
	r.lastAcknowledgedLSN.Store(lsn)
	r.metrics.ackedLSN.Store(uint64(lsn))
}

func (r *Reader) setInFlight(changes, bytes int) {
	r.inFlightChanges.Store(int64(changes))
	r.inFlightBytes.Store(int64(bytes))
}

func (r *Reader) incrementTransactionsReceived() {
	r.transactionsReceived.Add(1)
}

func (r *Reader) incrementTransactionsAccepted() {
	r.transactionsAccepted.Add(1)
}

func (r *Reader) incrementDecodeErrors() {
	r.decodeErrors.Add(1)
	r.metrics.decodeErrors.Add(context.Background(), 1)
}

func (r *Reader) incrementReconnects() {
	r.reconnectAttempts.Add(1)
	r.metrics.reconnects.Add(context.Background(), 1)
}

func (r *Reader) log(ctx context.Context, level slog.Level, message string, args ...any) {
	if r.config.Logger != nil {
		r.config.Logger.Log(ctx, level, message, args...)
	}
}
