package cdc

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Run streams transactions until ctx is cancelled, a non-retryable error
// occurs, or the reconnect budget is exhausted. Context cancellation is a
// clean shutdown and returns nil.
func (r *Reader) Run(ctx context.Context) error {
	monitorCtx, cancelMonitor := context.WithCancel(ctx)
	defer cancelMonitor()
	go r.monitorRetentionBudget(monitorCtx)

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
		decoder := NewDecoderWithLimits(r.config.MaxTransactionBytes, r.config.MaxTransactionChanges, r.config.MaxValueBytes)
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
	decoder := NewDecoderWithLimits(r.config.MaxTransactionBytes, r.config.MaxTransactionChanges, r.config.MaxValueBytes)
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
