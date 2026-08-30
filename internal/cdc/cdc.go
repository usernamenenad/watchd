package cdc

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrDatabaseURLRequired = errors.New("cdc: database URL is required")
	ErrInvalidDatabaseURL  = errors.New("cdc: invalid PostgreSQL database URL")
)

// CDC owns watchd's logical-replication connection to one PostgreSQL source.
//
// A CDC instance does not yet start replication or decode messages. Callers
// use Conn to pass its PostgreSQL replication connection to the reader that
// creates slots and receives pgoutput messages.
type CDC struct {
	conn *pgconn.PgConn
}

// Connect opens a PostgreSQL connection in logical-replication mode.
//
// databaseURL is a regular PostgreSQL connection URL. Connect adds the
// required replication=database runtime parameter itself, so callers should
// not need a special second URL. The configured role needs LOGIN and
// REPLICATION privileges.
func Connect(ctx context.Context, databaseURL string) (*CDC, error) {
	if databaseURL == "" {
		return nil, ErrDatabaseURLRequired
	}

	config, err := pgconn.ParseConfig(databaseURL)
	if err != nil {
		// Do not wrap the parser error: malformed URLs may contain a password.
		return nil, ErrInvalidDatabaseURL
	}
	if config.RuntimeParams == nil {
		config.RuntimeParams = make(map[string]string)
	}
	config.RuntimeParams["replication"] = "database"

	conn, err := pgconn.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("cdc: connect to PostgreSQL replication endpoint: %w", err)
	}

	return &CDC{
		conn: conn,
	}, nil
}

// Conn returns the underlying PostgreSQL replication connection. It is
// exposed for the CDC reader, which needs it to create slots, start streaming,
// receive WAL messages, and send replication acknowledgements.
func (cdc *CDC) Conn() *pgconn.PgConn {
	if cdc == nil {
		return nil
	}
	return cdc.conn
}

// Close closes the PostgreSQL replication connection. It is idempotent.
func (cdc *CDC) Close(ctx context.Context) error {
	if cdc == nil || cdc.conn == nil {
		return nil
	}

	err := cdc.conn.Close(ctx)
	cdc.conn = nil
	return err
}
