package cdc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

var (
	// ErrInvalidReaderConfig indicates a reader configuration that cannot be
	// made safe before opening a PostgreSQL connection.
	ErrInvalidReaderConfig = errors.New("cdc: invalid reader configuration")
	// ErrMalformedReplicationData indicates malformed CopyData, XLogData, or
	// pgoutput bytes received from the replication connection.
	ErrMalformedReplicationData = errors.New("cdc: malformed replication data")
	// ErrUnexpectedReplicationMessage indicates an unexpected PostgreSQL wire
	// message while logical replication is active.
	ErrUnexpectedReplicationMessage = errors.New("cdc: unexpected replication message")
	// ErrReplicationEnded indicates that PostgreSQL ended the replication COPY
	// stream without watchd requesting shutdown.
	ErrReplicationEnded = errors.New("cdc: replication stream ended")
	// ErrSlotInvalidated indicates that the slot or the WAL it requires is no
	// longer available. Recovery requires a new bootstrap, not a retry loop.
	ErrSlotInvalidated = errors.New("cdc: replication slot cannot be resumed")
	// ErrSlotInUse indicates that another replication connection owns the slot.
	ErrSlotInUse = errors.New("cdc: replication slot is already active")
	// ErrSinkRejected indicates that the local replay boundary did not accept a
	// committed transaction. The reader deliberately does not acknowledge it.
	ErrSinkRejected = errors.New("cdc: transaction sink rejected committed batch")
	// ErrRetryExhausted indicates that transient replication failures exceeded
	// the configured reconnect budget.
	ErrRetryExhausted = errors.New("cdc: replication reconnect budget exhausted")
	// ErrPostgresServer indicates that PostgreSQL rejected a replication
	// operation for a permanent reason not covered by a narrower error.
	ErrPostgresServer = errors.New("cdc: PostgreSQL rejected replication operation")
	// ErrSourceUnavailable indicates that watchd could not establish a usable
	// connection to the configured PostgreSQL source.
	ErrSourceUnavailable = errors.New("cdc: PostgreSQL source is unavailable")
	// ErrSnapshotExpired indicates that PostgreSQL no longer recognizes the
	// exported snapshot paired with a bootstrap cursor.
	ErrSnapshotExpired = errors.New("cdc: exported snapshot has expired")
	// ErrInsufficientPrivileges indicates that the configured database role
	// cannot perform a required bootstrap or replication operation.
	ErrInsufficientPrivileges = errors.New("cdc: insufficient PostgreSQL privileges")
	// ErrBootstrapSlotExists indicates that a bootstrap cannot export a new
	// snapshot because the configured slot already exists. Reusing that slot
	// would not provide a snapshot paired with its original consistent point.
	ErrBootstrapSlotExists = errors.New("cdc: bootstrap requires a new replication slot")
)

const (
	defaultConnectionTimeout = 10 * time.Second
	defaultStatusInterval    = 10 * time.Second
	defaultShutdownTimeout   = 5 * time.Second
	defaultInitialBackoff    = 250 * time.Millisecond
	defaultMaxBackoff        = 10 * time.Second
	defaultMaxAttempts       = 8
	defaultJitter            = 0.20
)

var postgresIdentifier = regexp.MustCompile(`^[a-z_][a-z0-9_$]{0,62}$`)

// TransactionSink is watchd's local durability boundary. Returning nil means
// that the transaction has been accepted and PostgreSQL may be acknowledged.
// A sink must tolerate a transaction being delivered more than once.
type TransactionSink func(context.Context, Transaction) error

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
	// SlotName names the persistent PostgreSQL logical replication slot.
	SlotName string
	// PublicationName is the single v0 PostgreSQL publication to stream.
	PublicationName string

	// MaxTransactionBytes and MaxTransactionChanges bound the memory retained
	// for one uncommitted source transaction.
	MaxTransactionBytes   int
	MaxTransactionChanges int
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

	// Logger is optional. It never receives DatabaseURL, credentials, row
	// values, tenant identifiers, or other projection data.
	Logger *slog.Logger
}

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

// Bootstrap creates a new persistent slot with an exported snapshot, reads
// the requested scope at that snapshot, and returns the matching resume
// cursor. The slot retains all changes after Cursor for a later Run call.
//
// Bootstrap deliberately refuses an existing slot: PostgreSQL only exports
// this snapshot while creating the slot, so an old slot cannot provide the
// required gap-free boundary.
func (r *Reader) Bootstrap(ctx context.Context, spec ProjectionSpec, scope Scope) (snapshot Snapshot, err error) {
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

	created, err := r.createBootstrapSlot(ctx, stream, management)
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

	if created.snapshotName == "" || created.resumeLSN == "" {
		return Snapshot{}, fmt.Errorf("%w: PostgreSQL did not return an exported snapshot and consistent point", ErrPostgresServer)
	}

	rows, err := readSnapshot(ctx, management, spec, scope, created.snapshotName, r.config.ShutdownTimeout, r.beforeSnapshotRead)
	if err != nil {
		return Snapshot{}, err
	}
	startLSN, err := pglogrepl.ParseLSN(created.resumeLSN)
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: invalid bootstrap consistent point", ErrPostgresServer)
	}
	if err := r.startReplication(ctx, stream.Conn(), startLSN); err != nil {
		return Snapshot{}, err
	}

	r.bootstrapMu.Lock()
	r.bootstrapStream = stream
	r.bootstrapLSN = startLSN
	r.bootstrapMu.Unlock()
	streamTransferred = true
	r.setConnectionState("streaming")

	return Snapshot{
		SourceID: spec.SourceID,
		Cursor:   created.resumeLSN,
		Rows:     rows,
	}, nil
}

// Run streams transactions until ctx is cancelled, a non-retryable error
// occurs, or the reconnect budget is exhausted. Context cancellation is a
// clean shutdown and returns nil.
func (r *Reader) Run(ctx context.Context) error {
	for attempts := 0; ; {
		r.setConnectionState("connecting")
		err := r.read(ctx)
		if ctx.Err() != nil {
			r.setConnectionState("stopped")
			return nil
		}
		if err == nil {
			r.setConnectionState("stopped")
			return nil
		}
		if !isRetryable(err) {
			r.setConnectionState("failed")
			return err
		}
		if attempts >= r.config.RetryPolicy.MaxAttempts {
			r.setConnectionState("failed")
			return fmt.Errorf("%w: %v", ErrRetryExhausted, err)
		}

		attempts++
		r.incrementReconnects()
		delay := r.retryDelay(attempts)
		r.log(ctx, slog.LevelWarn, "PostgreSQL logical replication connection lost; retrying", "attempt", attempts, "delay", delay, "error", err)
		r.setConnectionState("backing_off")
		if err := r.wait(ctx, delay); err != nil {
			if ctx.Err() != nil {
				r.setConnectionState("stopped")
				return nil
			}
			r.setConnectionState("failed")
			return err
		}
	}
}

// read owns exactly one PostgreSQL replication connection. A retryable return
// value is handled by Run, which creates a new connection and decoder.
func (r *Reader) read(ctx context.Context) error {
	if stream, startLSN, ok := r.takeBootstrapStream(); ok {
		defer r.closeReplicationConnection(stream)
		r.setConnectionState("streaming")
		decoder := NewDecoderWithLimits(r.config.MaxTransactionBytes, r.config.MaxTransactionChanges)
		return r.receive(ctx, stream.Conn(), decoder, startLSN)
	}

	stream, err := r.connectReplication(ctx)
	if err != nil {
		return err
	}
	defer r.closeReplicationConnection(stream)

	conn := stream.Conn()
	startLSN, err := r.requireSlot(ctx)
	if err != nil {
		return err
	}

	if err := r.startReplication(ctx, conn, startLSN); err != nil {
		return err
	}

	r.log(ctx, slog.LevelInfo, "PostgreSQL logical replication started", "slot", r.config.SlotName, "start_lsn", startLSN.String())
	r.setConnectionState("streaming")
	decoder := NewDecoderWithLimits(r.config.MaxTransactionBytes, r.config.MaxTransactionChanges)
	return r.receive(ctx, conn, decoder, startLSN)
}

func (r *Reader) startReplication(ctx context.Context, conn *pgconn.PgConn, startLSN pglogrepl.LSN) error {
	err := pglogrepl.StartReplication(
		ctx,
		conn,
		r.config.SlotName,
		startLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				"publication_names '" + r.config.PublicationName + "'",
			},
		})
	if err != nil {
		return classifyPostgresError(err)
	}
	return nil
}

func (r *Reader) takeBootstrapStream() (*CDC, pglogrepl.LSN, bool) {
	r.bootstrapMu.Lock()
	defer r.bootstrapMu.Unlock()
	if r.bootstrapStream == nil {
		return nil, 0, false
	}
	stream := r.bootstrapStream
	startLSN := r.bootstrapLSN
	r.bootstrapStream = nil
	r.bootstrapLSN = 0
	return stream, startLSN, true
}

func (r *Reader) receive(ctx context.Context, conn *pgconn.PgConn, decoder *Decoder, initialLSN pglogrepl.LSN) error {
	safeLSN := initialLSN
	nextStatus := r.now().Add(r.config.StatusInterval)

	for {
		receiveCtx, cancel := context.WithDeadline(ctx, nextStatus)
		raw, err := conn.ReceiveMessage(receiveCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if pgconn.Timeout(err) {
				if err := r.acknowledge(ctx, conn, safeLSN); err != nil {
					return classifyPostgresError(err)
				}
				nextStatus = r.now().Add(r.config.StatusInterval)
				continue
			}
			return err
		}

		// PostgreSQL can send either replication payloads (CopyData), an explicit
		// server error, or a CopyDone marker. Anything else is unsafe to ignore.
		switch message := raw.(type) {
		case *pgproto3.CopyData:
			if err := r.consumeCopyData(ctx, conn, decoder, &safeLSN, message.Data); err != nil {
				return err
			}

		case *pgproto3.ErrorResponse:
			return classifyErrorResponse(message)

		case *pgproto3.CopyDone:
			return retryableError(ErrReplicationEnded, "")

		default:
			return fmt.Errorf("%w: %T", ErrUnexpectedReplicationMessage, raw)
		}

		nextStatus = r.now().Add(r.config.StatusInterval)
	}
}

func (r *Reader) consumeCopyData(
	ctx context.Context,
	conn *pgconn.PgConn,
	decoder *Decoder,
	safeLSN *pglogrepl.LSN,
	data []byte,
) error {
	if len(data) == 0 {
		return ErrMalformedReplicationData
	}

	// The first byte identifies either WAL data ('w') or a primary keepalive
	// ('k'). A new identifier must be handled deliberately, never skipped.
	switch data[0] {
	case pglogrepl.XLogDataByteID:
		xlog, err := pglogrepl.ParseXLogData(data[1:])
		if err != nil {
			return fmt.Errorf("%w: parse XLogData", ErrMalformedReplicationData)
		}
		if len(xlog.WALData) == 0 {
			return fmt.Errorf("%w: XLogData has no WAL payload", ErrMalformedReplicationData)
		}
		if len(xlog.WALData) > r.config.MaxTransactionBytes {
			return ErrTransactionTooLarge
		}
		r.setLastReceivedLSN(xlog.WALStart)

		message, err := pglogrepl.Parse(xlog.WALData)
		if err != nil {
			r.incrementDecodeErrors()
			return fmt.Errorf("%w: parse pgoutput", ErrMalformedReplicationData)
		}

		transaction, err := decoder.Consume(message)
		changes, bytes := decoder.Pending()
		r.setInFlight(changes, bytes)
		if err != nil {
			r.incrementDecodeErrors()
			return err
		}
		if transaction == nil {
			return nil
		}

		commit, ok := message.(*pglogrepl.CommitMessage)
		if !ok || commit.TransactionEndLSN == 0 {
			return fmt.Errorf("%w: decoder emitted a transaction without a commit end LSN", ErrMalformedReplicationData)
		}
		r.incrementTransactionsReceived()

		// A nil sink result is the local replay-buffer acceptance boundary. If it
		// fails, safeLSN must not move and PostgreSQL will replay this batch.
		if err := r.sink(ctx, *transaction); err != nil {
			return fmt.Errorf("%w: %w", ErrSinkRejected, err)
		}

		// TransactionEndLSN, not ServerWALEnd, is the point after the complete
		// source transaction we just accepted. ServerWALEnd may be ahead of it.
		*safeLSN = commit.TransactionEndLSN
		if err := r.acknowledge(ctx, conn, *safeLSN); err != nil {
			return classifyPostgresError(err)
		}
		r.incrementTransactionsAccepted()
		r.setLastAcknowledgedLSN(*safeLSN)
		return nil

	case pglogrepl.PrimaryKeepaliveMessageByteID:
		keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(data[1:])
		if err != nil {
			return fmt.Errorf("%w: parse primary keepalive", ErrMalformedReplicationData)
		}

		// Keepalives carry no source mutation. Reply with safeLSN only when the
		// server asks; never advance it to keepalive.ServerWALEnd.
		if keepalive.ReplyRequested {
			if err := r.acknowledge(ctx, conn, *safeLSN); err != nil {
				return classifyPostgresError(err)
			}
		}
		return nil

	default:
		return fmt.Errorf("%w: CopyData discriminator %q", ErrUnexpectedReplicationMessage, data[0])
	}
}

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

func (r *Reader) validatePublication(ctx context.Context, management *pgx.Conn) error {
	var publishesInsert, publishesUpdate, publishesDelete, publishesTruncate bool
	err := management.QueryRow(ctx, `
		SELECT pubinsert, pubupdate, pubdelete, pubtruncate
		FROM pg_publication
		WHERE pubname = $1`, r.config.PublicationName).Scan(&publishesInsert, &publishesUpdate, &publishesDelete, &publishesTruncate)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: publication %q does not exist", ErrInvalidReaderConfig, r.config.PublicationName)
	}
	if err != nil {
		return classifyPostgresError(err)
	}
	if publishesTruncate {
		return fmt.Errorf("%w: publication %q publishes TRUNCATE, which v0 does not support", ErrInvalidReaderConfig, r.config.PublicationName)
	}
	if !publishesInsert || !publishesUpdate || !publishesDelete {
		return fmt.Errorf("%w: publication %q must publish INSERT, UPDATE, and DELETE", ErrInvalidReaderConfig, r.config.PublicationName)
	}
	return nil
}

func (r *Reader) validateProjectionSpec(ctx context.Context, management *pgx.Conn, spec ProjectionSpec) error {
	qualifiedTable := pgx.Identifier{spec.Schema, spec.Table}.Sanitize()
	var tableExists, scopeColumnExists bool
	err := management.QueryRow(ctx, `
		WITH projection AS (
			SELECT oid
			FROM pg_class
			WHERE oid = to_regclass($1)
			  AND relkind IN ('r', 'p')
		)
		SELECT
			EXISTS (SELECT 1 FROM projection),
			EXISTS (
				SELECT 1
				FROM pg_attribute
				WHERE attrelid = (SELECT oid FROM projection)
				  AND attname = $2
				  AND attnum > 0
				  AND NOT attisdropped
			)`, qualifiedTable, spec.ScopeColumn).Scan(&tableExists, &scopeColumnExists)
	if err != nil {
		return classifyPostgresError(err)
	}
	if !tableExists {
		return fmt.Errorf("%w: projection table %q.%q does not exist", ErrInvalidReaderConfig, spec.Schema, spec.Table)
	}
	if !scopeColumnExists {
		return fmt.Errorf("%w: projection table %q.%q has no scope column %q", ErrInvalidReaderConfig, spec.Schema, spec.Table, spec.ScopeColumn)
	}

	primaryKey, err := projectionPrimaryKey(ctx, management, qualifiedTable)
	if err != nil {
		return err
	}
	if !sameStrings(primaryKey, spec.PrimaryKey) {
		return fmt.Errorf("%w: projection table %q.%q primary key does not match configured primary key", ErrInvalidReaderConfig, spec.Schema, spec.Table)
	}

	var published bool
	err = management.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_publication_tables
			WHERE pubname = $1
			  AND schemaname = $2
			  AND tablename = $3
		)`, r.config.PublicationName, spec.Schema, spec.Table).Scan(&published)
	if err != nil {
		return classifyPostgresError(err)
	}
	if !published {
		return fmt.Errorf("%w: publication %q does not include projection table %q.%q", ErrInvalidReaderConfig, r.config.PublicationName, spec.Schema, spec.Table)
	}

	return nil
}

func validateProjectionSpecConfig(spec ProjectionSpec) error {
	if spec.SourceID == "" || spec.Schema == "" || spec.Table == "" || spec.ScopeColumn == "" || len(spec.PrimaryKey) == 0 {
		return ErrInvalidReaderConfig
	}
	if !postgresIdentifier.MatchString(spec.Schema) || !postgresIdentifier.MatchString(spec.Table) || !postgresIdentifier.MatchString(spec.ScopeColumn) {
		return fmt.Errorf("%w: projection schema, table, and scope column must be unquoted PostgreSQL identifiers", ErrInvalidReaderConfig)
	}
	seen := make(map[string]struct{}, len(spec.PrimaryKey))
	for _, column := range spec.PrimaryKey {
		if !postgresIdentifier.MatchString(column) || column == "" {
			return fmt.Errorf("%w: projection primary-key columns must be unquoted PostgreSQL identifiers", ErrInvalidReaderConfig)
		}
		if _, duplicate := seen[column]; duplicate {
			return fmt.Errorf("%w: projection primary key contains duplicate column %q", ErrInvalidReaderConfig, column)
		}
		seen[column] = struct{}{}
	}
	return nil
}

func projectionPrimaryKey(ctx context.Context, management *pgx.Conn, qualifiedTable string) ([]string, error) {
	rows, err := management.Query(ctx, `
		SELECT attribute.attname
		FROM pg_index AS index
		JOIN LATERAL unnest(index.indkey) WITH ORDINALITY AS key(attnum, position) ON true
		JOIN pg_attribute AS attribute ON attribute.attrelid = index.indrelid AND attribute.attnum = key.attnum
		WHERE index.indrelid = to_regclass($1)
		  AND index.indisprimary
		ORDER BY key.position`, qualifiedTable)
	if err != nil {
		return nil, classifyPostgresError(err)
	}
	defer rows.Close()

	var primaryKey []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, classifyPostgresError(err)
		}
		primaryKey = append(primaryKey, column)
	}
	if err := rows.Err(); err != nil {
		return nil, classifyPostgresError(err)
	}
	if len(primaryKey) == 0 {
		return nil, fmt.Errorf("%w: projection table %s has no primary key", ErrInvalidReaderConfig, qualifiedTable)
	}
	return primaryKey, nil
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

type slotCreation struct {
	resumeLSN    string
	snapshotName string
}

// createBootstrapSlot creates the only safe initial source boundary: a new
// persistent slot paired with PostgreSQL's exported snapshot.
func (r *Reader) createBootstrapSlot(ctx context.Context, stream *CDC, management *pgx.Conn) (slotCreation, error) {
	_, found, err := lookupSlot(ctx, management, r.config.SlotName)
	if err != nil {
		return slotCreation{}, err
	}
	if found {
		return slotCreation{}, ErrBootstrapSlotExists
	}

	created, err := pglogrepl.CreateReplicationSlot(ctx, stream.Conn(), r.config.SlotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		SnapshotAction: "EXPORT_SNAPSHOT",
	})
	if err != nil {
		// A concurrent bootstrap may win after our lookup, but its exported
		// snapshot belongs to that operation and cannot be reused here.
		if isDuplicateObject(err) {
			return slotCreation{}, ErrBootstrapSlotExists
		}
		return slotCreation{}, classifyPostgresError(err)
	}

	return slotCreation{
		resumeLSN:    created.ConsistentPoint,
		snapshotName: created.SnapshotName,
	}, nil
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

func readSnapshot(
	ctx context.Context,
	conn *pgx.Conn,
	spec ProjectionSpec,
	scope Scope,
	snapshotName string,
	cleanupTimeout time.Duration,
	beforeRead func(context.Context) error,
) ([]map[string]any, error) {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, classifyPostgresError(err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		_ = tx.Rollback(cleanupCtx)
	}()

	if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT "+quoteLiteral(snapshotName)); err != nil {
		return nil, classifySnapshotError(err)
	}
	if beforeRead != nil {
		if err := beforeRead(ctx); err != nil {
			return nil, err
		}
	}

	tableName := pgx.Identifier{spec.Schema, spec.Table}.Sanitize()
	scopeColumn := pgx.Identifier{spec.ScopeColumn}.Sanitize()
	rows, err := tx.Query(
		ctx,
		fmt.Sprintf("SELECT * FROM %s WHERE %s = $1", tableName, scopeColumn),
		pgx.QueryResultFormats{pgx.TextFormatCode},
		scope.Value,
	)
	if err != nil {
		return nil, classifyPostgresError(err)
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	snapshotRows := make([]map[string]any, 0)
	for rows.Next() {
		values := rows.RawValues()
		row := make(map[string]any, len(fields))
		for index, field := range fields {
			if values[index] == nil {
				row[field.Name] = nil
				continue
			}
			row[field.Name] = string(values[index])
		}
		snapshotRows = append(snapshotRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, classifyPostgresError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, classifyPostgresError(err)
	}

	return snapshotRows, nil
}

func quoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

type slotState struct {
	slotType  string
	plugin    string
	active    bool
	resumeLSN string
}

func lookupSlot(ctx context.Context, conn *pgx.Conn, slotName string) (slotState, bool, error) {
	var slot slotState
	err := conn.QueryRow(ctx, `
		SELECT slot_type, plugin, active,
		       COALESCE(confirmed_flush_lsn::text, restart_lsn::text, '')
		FROM pg_replication_slots
		WHERE slot_name = $1`, slotName).Scan(&slot.slotType, &slot.plugin, &slot.active, &slot.resumeLSN)
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

func connectManagement(ctx context.Context, databaseURL string) (*pgx.Conn, error) {
	config, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return nil, ErrInvalidDatabaseURL
	}
	// ReaderConfig accepts the same URL used by Connect. Management queries need
	// a normal PostgreSQL session, not the replication protocol.
	delete(config.RuntimeParams, "replication")

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("cdc: connect to PostgreSQL management endpoint: %w", err)
	}
	return conn, nil
}

func (r *Reader) connectReplication(ctx context.Context) (*CDC, error) {
	connectCtx, cancel := context.WithTimeout(ctx, r.config.ConnectionTimeout)
	defer cancel()

	stream, err := r.connect(connectCtx, r.config.DatabaseURL)
	if err == nil {
		return stream, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return nil, classifyConnectionError(err)
}

func (r *Reader) connectManagementWithTimeout(ctx context.Context) (*pgx.Conn, error) {
	connectCtx, cancel := context.WithTimeout(ctx, r.config.ConnectionTimeout)
	defer cancel()

	management, err := r.connectManagement(connectCtx, r.config.DatabaseURL)
	if err == nil {
		return management, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return nil, classifyConnectionError(err)
}

func (r *Reader) acknowledge(ctx context.Context, conn *pgconn.PgConn, safeLSN pglogrepl.LSN) error {
	err := r.sendStandbyStatus(ctx, conn, safeLSN)
	if err == nil {
		r.setLastAcknowledgedLSN(safeLSN)
	}
	return err
}

func sendStandbyStatus(ctx context.Context, conn *pgconn.PgConn, safeLSN pglogrepl.LSN) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: safeLSN,
	})
}

func (r *Reader) closeReplicationConnection(stream *CDC) {
	closeCtx, cancel := context.WithTimeout(context.Background(), r.config.ShutdownTimeout)
	defer cancel()
	_ = stream.Close(closeCtx)
}

func (r *Reader) closeManagementConnection(management *pgx.Conn) {
	closeCtx, cancel := context.WithTimeout(context.Background(), r.config.ShutdownTimeout)
	defer cancel()
	_ = management.Close(closeCtx)
}

func normalizeReaderConfig(config ReaderConfig) ReaderConfig {
	if config.MaxTransactionBytes == 0 {
		config.MaxTransactionBytes = defaultMaxTransactionBytes
	}
	if config.MaxTransactionChanges == 0 {
		config.MaxTransactionChanges = defaultMaxTransactionChanges
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
	if config.DatabaseURL == "" || config.SlotName == "" || config.PublicationName == "" || sink == nil {
		return ErrInvalidReaderConfig
	}
	if !postgresIdentifier.MatchString(config.SlotName) || !postgresIdentifier.MatchString(config.PublicationName) {
		return fmt.Errorf("%w: slot and publication names must be unquoted PostgreSQL identifiers", ErrInvalidReaderConfig)
	}
	if config.MaxTransactionBytes <= 0 || config.MaxTransactionChanges <= 0 || config.ConnectionTimeout <= 0 || config.StatusInterval <= 0 || config.ShutdownTimeout <= 0 {
		return ErrInvalidReaderConfig
	}
	if config.RetryPolicy.InitialBackoff <= 0 || config.RetryPolicy.MaxBackoff < config.RetryPolicy.InitialBackoff || config.RetryPolicy.MaxAttempts < 0 || config.RetryPolicy.Jitter < 0 || config.RetryPolicy.Jitter > 1 {
		return ErrInvalidReaderConfig
	}
	return nil
}

func isRetryable(err error) bool {
	if classified, ok := errors.AsType[*classifiedError](err); ok {
		return classified.retryable
	}

	return !errors.Is(err, ErrInvalidReaderConfig) &&
		!errors.Is(err, ErrInvalidDatabaseURL) &&
		!errors.Is(err, ErrMalformedReplicationData) &&
		!errors.Is(err, ErrUnexpectedReplicationMessage) &&
		!errors.Is(err, ErrSlotInvalidated) &&
		!errors.Is(err, ErrPostgresServer) &&
		!errors.Is(err, ErrTransactionTooLarge) &&
		!errors.Is(err, ErrTransactionTooManyChanges) &&
		!errors.Is(err, ErrUnsupportedPGOutputMessage) &&
		!errors.Is(err, ErrSinkRejected)
}

type classifiedError struct {
	kind      error
	retryable bool
	sqlState  string
}

func (e *classifiedError) Error() string {
	if e.sqlState == "" {
		return e.kind.Error()
	}

	return fmt.Sprintf("%s (SQLSTATE %s)", e.kind, e.sqlState)
}

func (e *classifiedError) Unwrap() error {
	return e.kind
}

func retryableError(kind error, sqlState string) error {
	return &classifiedError{kind: kind, retryable: true, sqlState: sqlState}
}

func terminalError(kind error, sqlState string) error {
	return &classifiedError{kind: kind, retryable: false, sqlState: sqlState}
}

func classifyErrorResponse(message *pgproto3.ErrorResponse) error {
	return classifySQLState(message.Code, message.Message)
}

func classifyPostgresError(err error) error {
	if postgresError, ok := errors.AsType[*pgconn.PgError](err); ok {
		return classifySQLState(postgresError.SQLState(), postgresError.Message)
	}
	return err
}

func classifyConnectionError(err error) error {
	if errors.Is(err, ErrInvalidDatabaseURL) || errors.Is(err, ErrDatabaseURLRequired) {
		return err
	}
	classified := classifyPostgresError(err)
	if errors.Is(classified, ErrInsufficientPrivileges) {
		return classified
	}
	return fmt.Errorf("%w: %v", ErrSourceUnavailable, err)
}

func classifySnapshotError(err error) error {
	if postgresError, ok := errors.AsType[*pgconn.PgError](err); ok {
		switch postgresError.SQLState() {
		case "22023", "42704": // invalid or no-longer-existing snapshot identifier
			return terminalError(ErrSnapshotExpired, postgresError.SQLState())
		}
	}
	return classifyPostgresError(err)
}

func classifySQLState(sqlState, message string) error {
	if strings.Contains(strings.ToLower(message), "requested wal segment") && strings.Contains(strings.ToLower(message), "removed") {
		return terminalError(ErrSlotInvalidated, sqlState)
	}

	switch sqlState {
	case "42501": // insufficient_privilege
		return terminalError(ErrInsufficientPrivileges, sqlState)
	case "42704": // undefined object: slot or publication no longer exists
		return terminalError(ErrSlotInvalidated, sqlState)
	case "55006": // object_in_use: another backend owns the slot
		return retryableError(ErrSlotInUse, sqlState)
	case "57P01", "57P02", "57P03": // server shutdown / crash / startup
		return retryableError(ErrReplicationEnded, sqlState)
	default:
		return terminalError(ErrPostgresServer, sqlState)
	}
}

func isDuplicateObject(err error) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) && postgresError.SQLState() == "42710"
}

func (r *Reader) retryDelay(attempt int) time.Duration {
	base := float64(r.config.RetryPolicy.InitialBackoff) * math.Pow(2, float64(attempt-1))
	base = math.Min(base, float64(r.config.RetryPolicy.MaxBackoff))
	jitter := 1 + ((r.random()*2)-1)*r.config.RetryPolicy.Jitter
	return time.Duration(base * jitter)
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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
