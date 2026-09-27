package cdc

import (
	"context"
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

	return &Reader{
		config:            config,
		sink:              sink,
		connect:           Connect,
		connectManagement: connectManagement,
		sendStandbyStatus: sendStandbyStatus,
		wait:              waitContext,
		now:               time.Now,
		random:            rand.Float64,
		stats: ReaderStats{
			ConnectionState: "idle",
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
}

func (r *Reader) setLastReceivedLSN(lsn pglogrepl.LSN) {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()

	r.stats.LastReceivedLSN = lsn.String()
}

func (r *Reader) setLastAcknowledgedLSN(lsn pglogrepl.LSN) {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()

	r.stats.LastAcknowledgedLSN = lsn.String()
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
}

func (r *Reader) incrementReconnects() {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()

	r.stats.ReconnectAttempts++
}

func (r *Reader) log(ctx context.Context, level slog.Level, message string, args ...any) {
	if r.config.Logger != nil {
		r.config.Logger.Log(ctx, level, message, args...)
	}
}
