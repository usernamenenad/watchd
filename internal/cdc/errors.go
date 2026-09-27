package cdc

import (
	"errors"
	"fmt"
	"strings"

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
	// ErrInsufficientPrivileges indicates that the configured database role
	// cannot perform a required bootstrap or replication operation.
	ErrInsufficientPrivileges = errors.New("cdc: insufficient PostgreSQL privileges")
	// ErrBootstrapSlotExists indicates that a bootstrap cannot run because the
	// configured slot already exists. Bootstrap is the only slot-creation
	// path, so a second bootstrap against the same slot name is a caller
	// error, not a case to silently reuse.
	ErrBootstrapSlotExists = errors.New("cdc: bootstrap requires a new replication slot")
	// ErrSlotNotFound indicates that a per-scope Snapshot was requested
	// before the reader's replication slot exists. Only Bootstrap creates it.
	ErrSlotNotFound = errors.New("cdc: replication slot does not exist")
	// ErrSnapshotWindowClosed indicates that a per-scope snapshot's cursor
	// fell outside the replication slot's retained replay window before the
	// snapshot could be paired with it, so resuming from that cursor could
	// silently skip changes. The caller must take a fresh snapshot.
	ErrSnapshotWindowClosed = errors.New("cdc: snapshot cursor is outside the replication slot's replay window")
	// ErrSnapshotSinkRequired indicates that Snapshot or Bootstrap was
	// called without a SnapshotRowSink to receive scoped rows.
	ErrSnapshotSinkRequired = errors.New("cdc: snapshot row sink is required")
)

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
		!errors.Is(err, ErrUnsupportedColumnEncoding) &&
		!errors.Is(err, ErrValueTooLarge) &&
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
