package cdc

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"

	"github.com/usernamenenad/watchd/internal/telemetry"
)

// Connection states, reported by ReaderStats.ConnectionState and by
// watchd.cdc.stream.state.
const (
	stateIdle       = "idle"
	stateConnecting = "connecting"
	stateStreaming  = "streaming"
	stateBackingOff = "backing_off"
	stateStopped    = "stopped"
	stateFailed     = "failed"
)

var connectionStates = []string{stateIdle, stateConnecting, stateStreaming, stateBackingOff, stateStopped, stateFailed}

// Snapshot modes, the mode attribute of the snapshot instruments.
const (
	modeBootstrap = "bootstrap"
	modeSnapshot  = "snapshot"
)

// walStatuses are pg_replication_slots.wal_status values.
var walStatuses = []string{"reserved", "extended", "unreserved", "lost"}

// Histogram boundaries, in seconds, by expected range.
var (
	// roundTripBounds cover a request to PostgreSQL: 1ms to 30s.
	roundTripBounds = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
	// commitLatencyBounds cover commit to acceptance, which includes
	// PostgreSQL's WAL sender and any replication backlog: 1ms to 5min.
	commitLatencyBounds = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300}
	// inProcessBounds cover work inside watchd: 50µs to 5s.
	inProcessBounds = []float64{0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}
	// statusIntervalBounds cover the time between standby status updates:
	// 1ms (a busy stream acknowledges per transaction) to 1min.
	statusIntervalBounds = []float64{0.001, 0.01, 0.1, 0.5, 1, 2.5, 5, 10, 15, 30, 60}
	// snapshotBounds cover a whole scope read: 10ms to 10min.
	snapshotBounds = []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600}
)

// powersOf4 returns 1, 4, 16, ... times base, up to and including limit, so
// a size histogram resolves small values and ends at its configured bound.
func powersOf4(base, limit float64) []float64 {
	var bounds []float64
	for bound := base; bound < limit; bound *= 4 {
		bounds = append(bounds, bound)
	}
	return append(bounds, limit)
}

// readerMetrics are the reader's instruments, created once in NewReader.
// Every attribute set is built there too, so recording never allocates.
// With no meter configured, every instrument is a no-op.
type readerMetrics struct {
	// Observed by callbacks; written by the reader.
	state        atomic.Int32 // index into connectionStates
	receivedLSN  atomic.Uint64
	ackedLSN     atomic.Uint64
	serverWALEnd atomic.Uint64

	retentionKnown  atomic.Bool
	retainedWAL     atomic.Int64 // -1 when the slot's restart_lsn is unknown
	safeWALSize     atomic.Int64 // -1 when the server's retention is unbounded
	walStatus       atomic.Int32 // index into walStatuses, -1 when unknown
	effectiveBudget atomic.Int64

	reconnects     metric.Int64Counter
	streamErrors   metric.Int64Counter
	errorClassOpts map[string]metric.AddOption

	statusDuration metric.Float64Histogram
	statusInterval metric.Float64Histogram
	// lastStatus is when the last standby status was sent. Only the
	// receiving goroutine touches it.
	lastStatus time.Time

	messages     metric.Int64Counter
	walBytes     metric.Int64Counter
	transactions metric.Int64Counter
	txDuration   metric.Float64Histogram
	txChanges    metric.Int64Histogram
	txBytes      metric.Int64Histogram
	decodeErrors metric.Int64Counter
	sinkDuration metric.Float64Histogram
	commitToSink metric.Float64Histogram
	// Messages and bytes since the last commit, and when the pending
	// transaction's BEGIN arrived. Only the receiving goroutine touches them;
	// they are added to the counters once per transaction, not per message.
	pendingMessages int64
	pendingBytes    int64
	beganAt         time.Time

	snapshotDuration metric.Float64Histogram
	snapshotPage     metric.Float64Histogram
	snapshotRows     metric.Int64Counter
	snapshotBytes    metric.Int64Counter
	snapshotActive   metric.Int64UpDownCounter
	snapshotErrors   metric.Int64Counter
	modeRecordOpts   map[string]metric.RecordOption
	modeAddOpts      map[string]metric.AddOption
	snapshotErrOpts  map[[2]string]metric.AddOption
}

// errorClasses maps the reader's error sentinels to the error_class
// attribute, most specific first.
var errorClasses = []struct {
	err   error
	class string
}{
	{ErrSlotInvalidated, "slot_invalidated"},
	{ErrSlotInUse, "slot_in_use"},
	{ErrSlotNotFound, "slot_not_found"},
	{ErrSnapshotWindowClosed, "snapshot_window_closed"},
	{ErrInsufficientPrivileges, "insufficient_privileges"},
	{ErrSourceUnavailable, "source_unavailable"},
	{ErrReplicationEnded, "replication_ended"},
	{ErrSinkRejected, "sink_rejected"},
	{ErrTransactionTooLarge, "transaction_too_large"},
	{ErrTransactionTooManyChanges, "transaction_too_large"},
	{ErrValueTooLarge, "transaction_too_large"},
	{ErrMalformedReplicationData, "malformed_data"},
	{ErrUnexpectedReplicationMessage, "malformed_data"},
	{ErrUnsupportedPGOutputMessage, "unsupported_change"},
	{ErrUnsupportedColumnEncoding, "unsupported_change"},
	{ErrPrimaryKeyChangeUnsupported, "unsupported_change"},
	{ErrInvalidReaderConfig, "invalid_config"},
	{ErrPostgresServer, "postgres_server"},
	{context.DeadlineExceeded, "timeout"},
}

// errorClass classifies err for the error_class attribute. The classes are
// a closed set; anything unrecognized is "other".
func errorClass(err error) string {
	for _, c := range errorClasses {
		if errors.Is(err, c.err) {
			return c.class
		}
	}
	return "other"
}

func errorClassNames() []string {
	names := []string{"other"}
	seen := map[string]bool{"other": true}
	for _, c := range errorClasses {
		if !seen[c.class] {
			seen[c.class] = true
			names = append(names, c.class)
		}
	}
	return names
}

func newReaderMetrics(meter metric.Meter, config ReaderConfig) (*readerMetrics, error) {
	meter = telemetry.MeterOrNoop(meter)
	m := &readerMetrics{
		errorClassOpts:  map[string]metric.AddOption{},
		modeRecordOpts:  map[string]metric.RecordOption{},
		modeAddOpts:     map[string]metric.AddOption{},
		snapshotErrOpts: map[[2]string]metric.AddOption{},
	}
	m.walStatus.Store(-1)
	for _, class := range errorClassNames() {
		m.errorClassOpts[class] = metric.WithAttributeSet(telemetry.Attributes(telemetry.KeyErrorClass.String(class)))
		for _, mode := range []string{modeBootstrap, modeSnapshot} {
			m.snapshotErrOpts[[2]string{mode, class}] = metric.WithAttributeSet(telemetry.Attributes(telemetry.KeyMode.String(mode), telemetry.KeyErrorClass.String(class)))
		}
	}
	for _, mode := range []string{modeBootstrap, modeSnapshot} {
		set := telemetry.Attributes(telemetry.KeyMode.String(mode))
		m.modeRecordOpts[mode] = metric.WithAttributeSet(set)
		m.modeAddOpts[mode] = metric.WithAttributeSet(set)
	}

	var errs []error
	check := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	var err error

	m.reconnects, err = meter.Int64Counter("watchd.cdc.reconnects", metric.WithUnit("{reconnect}"),
		metric.WithDescription("Reconnect attempts after a retryable replication failure."))
	check(err)
	m.streamErrors, err = meter.Int64Counter("watchd.cdc.stream.errors", metric.WithUnit("{error}"),
		metric.WithDescription("Replication connections that ended in an error, by error_class."))
	check(err)
	m.statusDuration, err = meter.Float64Histogram("watchd.cdc.standby_status.duration", metric.WithUnit("s"),
		metric.WithDescription("Time to send one standby status update (an acknowledgement) to PostgreSQL."),
		metric.WithExplicitBucketBoundaries(inProcessBounds...))
	check(err)
	m.statusInterval, err = meter.Float64Histogram("watchd.cdc.standby_status.interval", metric.WithUnit("s"),
		metric.WithDescription("Time between consecutive standby status updates."),
		metric.WithExplicitBucketBoundaries(statusIntervalBounds...))
	check(err)
	m.messages, err = meter.Int64Counter("watchd.cdc.messages", metric.WithUnit("{message}"),
		metric.WithDescription("pgoutput messages decoded, added once per committed transaction."))
	check(err)
	m.walBytes, err = meter.Int64Counter("watchd.cdc.wal.bytes", metric.WithUnit("By"),
		metric.WithDescription("pgoutput payload bytes decoded, added once per committed transaction."))
	check(err)
	m.transactions, err = meter.Int64Counter("watchd.cdc.transactions", metric.WithUnit("{transaction}"),
		metric.WithDescription("Committed transactions accepted by the sink and acknowledged."))
	check(err)
	m.txDuration, err = meter.Float64Histogram("watchd.cdc.transaction.duration", metric.WithUnit("s"),
		metric.WithDescription("Time from a transaction's BEGIN to its COMMIT being received and decoded."),
		metric.WithExplicitBucketBoundaries(inProcessBounds...))
	check(err)
	m.txChanges, err = meter.Int64Histogram("watchd.cdc.transaction.changes", metric.WithUnit("{change}"),
		metric.WithDescription("Row changes per committed transaction, bounded by MaxTransactionChanges."),
		metric.WithExplicitBucketBoundaries(powersOf4(1, float64(config.MaxTransactionChanges))...))
	check(err)
	m.txBytes, err = meter.Int64Histogram("watchd.cdc.transaction.size", metric.WithUnit("By"),
		metric.WithDescription("Estimated in-memory size per committed transaction, bounded by MaxTransactionBytes."),
		metric.WithExplicitBucketBoundaries(powersOf4(256, float64(config.MaxTransactionBytes))...))
	check(err)
	m.decodeErrors, err = meter.Int64Counter("watchd.cdc.decode.errors", metric.WithUnit("{error}"),
		metric.WithDescription("pgoutput messages that could not be decoded."))
	check(err)
	m.sinkDuration, err = meter.Float64Histogram("watchd.cdc.sink.duration", metric.WithUnit("s"),
		metric.WithDescription("Time the sink (the hub) takes to accept a transaction. Replication waits for it before acknowledging."),
		metric.WithExplicitBucketBoundaries(inProcessBounds...))
	check(err)
	m.commitToSink, err = meter.Float64Histogram("watchd.cdc.commit_to_accept", metric.WithUnit("s"),
		metric.WithDescription("Time from a transaction's commit in PostgreSQL to its acceptance by the sink. Subject to clock skew between PostgreSQL and watchd."),
		metric.WithExplicitBucketBoundaries(commitLatencyBounds...))
	check(err)

	m.snapshotDuration, err = meter.Float64Histogram("watchd.cdc.snapshot.duration", metric.WithUnit("s"),
		metric.WithDescription("Time to read one scope, by mode, for reads that succeed."),
		metric.WithExplicitBucketBoundaries(snapshotBounds...))
	check(err)
	m.snapshotPage, err = meter.Float64Histogram("watchd.cdc.snapshot.page.duration", metric.WithUnit("s"),
		metric.WithDescription("Time to fetch one keyset page of a scope read, by mode."),
		metric.WithExplicitBucketBoundaries(roundTripBounds...))
	check(err)
	m.snapshotRows, err = meter.Int64Counter("watchd.cdc.snapshot.rows", metric.WithUnit("{row}"),
		metric.WithDescription("Rows read by scope reads, by mode."))
	check(err)
	m.snapshotBytes, err = meter.Int64Counter("watchd.cdc.snapshot.bytes", metric.WithUnit("By"),
		metric.WithDescription("Text bytes of rows read by scope reads, by mode."))
	check(err)
	m.snapshotActive, err = meter.Int64UpDownCounter("watchd.cdc.snapshot.active", metric.WithUnit("{snapshot}"),
		metric.WithDescription("Scope reads in progress, by mode."))
	check(err)
	m.snapshotErrors, err = meter.Int64Counter("watchd.cdc.snapshot.errors", metric.WithUnit("{error}"),
		metric.WithDescription("Scope reads that failed, by mode and error_class."))
	check(err)

	check(m.registerGauges(meter))
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return m, nil
}

func (m *readerMetrics) registerGauges(meter metric.Meter) error {
	state, err1 := meter.Int64ObservableGauge("watchd.cdc.stream.state",
		metric.WithDescription("1 for the replication stream's current state, 0 for the others."))
	received, err2 := meter.Int64ObservableGauge("watchd.cdc.lsn.received", metric.WithUnit("By"),
		metric.WithDescription("WAL position of the newest message received."))
	acked, err3 := meter.Int64ObservableGauge("watchd.cdc.lsn.acknowledged", metric.WithUnit("By"),
		metric.WithDescription("WAL position last acknowledged to PostgreSQL."))
	serverEnd, err4 := meter.Int64ObservableGauge("watchd.cdc.lsn.server_wal_end", metric.WithUnit("By"),
		metric.WithDescription("PostgreSQL's WAL end, as last reported on the replication stream."))
	walLag, err5 := meter.Int64ObservableGauge("watchd.cdc.wal_lag", metric.WithUnit("By"),
		metric.WithDescription("How far PostgreSQL's WAL end is ahead of what watchd has received."))
	ackLag, err6 := meter.Int64ObservableGauge("watchd.cdc.ack_lag", metric.WithUnit("By"),
		metric.WithDescription("How far what watchd has received is ahead of what it has acknowledged."))
	retained, err7 := meter.Int64ObservableGauge("watchd.cdc.slot.retained_wal", metric.WithUnit("By"),
		metric.WithDescription("WAL PostgreSQL retains for the slot: its current WAL position minus the slot's restart_lsn."))
	safe, err8 := meter.Int64ObservableGauge("watchd.cdc.slot.safe_wal_size", metric.WithUnit("By"),
		metric.WithDescription("WAL that can still be written before the slot is invalidated (pg_replication_slots.safe_wal_size). Absent when the server's retention is unbounded."))
	walStatus, err9 := meter.Int64ObservableGauge("watchd.cdc.slot.wal_status",
		metric.WithDescription("1 for the slot's pg_replication_slots.wal_status, 0 for the others."))
	budget, err10 := meter.Int64ObservableGauge("watchd.cdc.retention.budget", metric.WithUnit("By"),
		metric.WithDescription("The effective retained-WAL budget: the stricter of MaxRetainedWALBytes and max_slot_wal_keep_size."))
	if err := errors.Join(err1, err2, err3, err4, err5, err6, err7, err8, err9, err10); err != nil {
		return err
	}

	stateOpts := make([]metric.ObserveOption, len(connectionStates))
	for i, s := range connectionStates {
		stateOpts[i] = metric.WithAttributeSet(telemetry.Attributes(telemetry.KeyState.String(s)))
	}
	walStatusOpts := make([]metric.ObserveOption, len(walStatuses))
	for i, s := range walStatuses {
		walStatusOpts[i] = metric.WithAttributeSet(telemetry.Attributes(telemetry.KeyState.String(s)))
	}

	_, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		current := m.state.Load()
		for i, opt := range stateOpts {
			o.ObserveInt64(state, boolValue(int32(i) == current), opt)
		}

		receivedLSN, ackedLSN, serverEndLSN := m.receivedLSN.Load(), m.ackedLSN.Load(), m.serverWALEnd.Load()
		if receivedLSN > 0 {
			o.ObserveInt64(received, int64(receivedLSN))
			o.ObserveInt64(ackLag, lsnDistance(receivedLSN, ackedLSN))
		}
		if ackedLSN > 0 {
			o.ObserveInt64(acked, int64(ackedLSN))
		}
		if serverEndLSN > 0 {
			o.ObserveInt64(serverEnd, int64(serverEndLSN))
			o.ObserveInt64(walLag, lsnDistance(serverEndLSN, receivedLSN))
		}

		if m.retentionKnown.Load() {
			if v := m.retainedWAL.Load(); v >= 0 {
				o.ObserveInt64(retained, v)
			}
			if v := m.safeWALSize.Load(); v >= 0 {
				o.ObserveInt64(safe, v)
			}
			o.ObserveInt64(budget, m.effectiveBudget.Load())
			status := m.walStatus.Load()
			for i, opt := range walStatusOpts {
				o.ObserveInt64(walStatus, boolValue(int32(i) == status), opt)
			}
		}
		return nil
	}, state, received, acked, serverEnd, walLag, ackLag, retained, safe, walStatus, budget)
	return err
}

func (m *readerMetrics) setState(state string) {
	for i, s := range connectionStates {
		if s == state {
			m.state.Store(int32(i))
			return
		}
	}
}

func (m *readerMetrics) streamError(ctx context.Context, err error) {
	m.streamErrors.Add(ctx, 1, m.errorClassOpts[errorClass(err)])
}

// statusSent records one standby status update that took duration.
func (m *readerMetrics) statusSent(ctx context.Context, now time.Time, duration time.Duration) {
	m.statusDuration.Record(ctx, duration.Seconds())
	if !m.lastStatus.IsZero() {
		m.statusInterval.Record(ctx, now.Sub(m.lastStatus).Seconds())
	}
	m.lastStatus = now
}

// snapshotObserver measures one scope read.
type snapshotObserver struct {
	metrics *readerMetrics
	mode    string
	started time.Time
}

func (m *readerMetrics) startSnapshot(ctx context.Context, mode string) *snapshotObserver {
	m.snapshotActive.Add(ctx, 1, m.modeAddOpts[mode])
	return &snapshotObserver{metrics: m, mode: mode, started: time.Now()}
}

// sink wraps a snapshot row sink to count what it receives.
func (s *snapshotObserver) sink(next SnapshotRowSink) SnapshotRowSink {
	return func(ctx context.Context, rows []map[string]any) error {
		bytes := 0
		for _, row := range rows {
			for column, value := range row {
				bytes += len(column)
				if text, ok := value.(string); ok {
					bytes += len(text)
				}
			}
		}
		s.metrics.snapshotRows.Add(ctx, int64(len(rows)), s.metrics.modeAddOpts[s.mode])
		s.metrics.snapshotBytes.Add(ctx, int64(bytes), s.metrics.modeAddOpts[s.mode])
		return next(ctx, rows)
	}
}

func (s *snapshotObserver) page(ctx context.Context, duration time.Duration) {
	s.metrics.snapshotPage.Record(ctx, duration.Seconds(), s.metrics.modeRecordOpts[s.mode])
}

func (s *snapshotObserver) finish(ctx context.Context, err error) {
	// The read is over even when ctx is cancelled, so the counters use a
	// context that cannot be.
	ctx = context.WithoutCancel(ctx)
	s.metrics.snapshotActive.Add(ctx, -1, s.metrics.modeAddOpts[s.mode])
	if err != nil {
		s.metrics.snapshotErrors.Add(ctx, 1, s.metrics.snapshotErrOpts[[2]string{s.mode, errorClass(err)}])
		return
	}
	s.metrics.snapshotDuration.Record(ctx, time.Since(s.started).Seconds(), s.metrics.modeRecordOpts[s.mode])
}

func boolValue(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// lsnDistance is a - b in bytes, or 0 when b is not behind a.
func lsnDistance(a, b uint64) int64 {
	if a <= b {
		return 0
	}
	return int64(a - b)
}
