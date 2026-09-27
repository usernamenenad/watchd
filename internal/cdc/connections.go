package cdc

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

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
