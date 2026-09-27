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
		r.setConnectionState(stateConnecting)
		err := r.read(ctx)
		if ctx.Err() != nil {
			r.setConnectionState(stateStopped)
			return nil
		}
		if err == nil {
			r.setConnectionState(stateStopped)
			return nil
		}
		r.metrics.streamError(ctx, err)
		if !isRetryable(err) {
			r.setConnectionState(stateFailed)
			return err
		}
		if attempts >= r.config.RetryPolicy.MaxAttempts {
			r.setConnectionState(stateFailed)
			return fmt.Errorf("%w: %v", ErrRetryExhausted, err)
		}

		attempts++
		r.incrementReconnects()
		delay := r.retryDelay(attempts)
		r.log(ctx, slog.LevelWarn, "PostgreSQL logical replication connection lost; retrying", "attempt", attempts, "delay", delay, "error", err)
		r.setConnectionState(stateBackingOff)
		if err := r.wait(ctx, delay); err != nil {
			if ctx.Err() != nil {
				r.setConnectionState(stateStopped)
				return nil
			}
			r.setConnectionState(stateFailed)
			return err
		}
	}
}

// read owns exactly one PostgreSQL replication connection. A retryable return
// value is handled by Run, which creates a new connection and decoder.
func (r *Reader) read(ctx context.Context) error {
	if stream, startLSN, ok := r.takeBootstrapStream(); ok {
		defer r.closeReplicationConnection(stream)
		r.setConnectionState(stateStreaming)
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
	r.setConnectionState(stateStreaming)
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
	// A new connection starts with a new decoder; drop any partial
	// transaction's tallies from the last one.
	r.metrics.pendingMessages, r.metrics.pendingBytes, r.metrics.beganAt = 0, 0, time.Time{}

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
		r.observeServerWALEnd(xlog.ServerWALEnd)

		message, err := pglogrepl.Parse(xlog.WALData)
		if err != nil {
			r.incrementDecodeErrors()
			return fmt.Errorf("%w: parse pgoutput", ErrMalformedReplicationData)
		}

		metrics := r.metrics
		metrics.pendingMessages++
		metrics.pendingBytes += int64(len(xlog.WALData))
		var committedBytes int
		switch message.(type) {
		case *pglogrepl.BeginMessage:
			metrics.beganAt = r.now()
		case *pglogrepl.CommitMessage:
			// Consume resets the pending footprint at COMMIT; read it first.
			_, committedBytes = decoder.Pending()
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
		transaction.Cursor.sourceID = r.config.SourceID

		decoded := r.now()
		if !metrics.beganAt.IsZero() {
			metrics.txDuration.Record(ctx, decoded.Sub(metrics.beganAt).Seconds())
		}
		metrics.txChanges.Record(ctx, int64(len(transaction.Changes)))
		metrics.txBytes.Record(ctx, int64(committedBytes))
		metrics.messages.Add(ctx, metrics.pendingMessages)
		metrics.walBytes.Add(ctx, metrics.pendingBytes)
		metrics.pendingMessages, metrics.pendingBytes, metrics.beganAt = 0, 0, time.Time{}

		// A nil sink result is the local replay-buffer acceptance boundary. If it
		// fails, safeLSN must not move and PostgreSQL will replay this batch.
		if err := r.sink(ctx, *transaction); err != nil {
			return fmt.Errorf("%w: %w", ErrSinkRejected, err)
		}
		accepted := r.now()
		metrics.sinkDuration.Record(ctx, accepted.Sub(decoded).Seconds())
		if !transaction.CommitTime.IsZero() {
			metrics.commitToSink.Record(ctx, accepted.Sub(transaction.CommitTime).Seconds())
		}

		// TransactionEndLSN, not ServerWALEnd, is the point after the complete
		// source transaction we just accepted. ServerWALEnd may be ahead of it.
		*safeLSN = commit.TransactionEndLSN
		if err := r.acknowledge(ctx, conn, *safeLSN); err != nil {
			return classifyPostgresError(err)
		}
		r.incrementTransactionsAccepted()
		metrics.transactions.Add(ctx, 1)
		r.setLastAcknowledgedLSN(*safeLSN)
		return nil

	case pglogrepl.PrimaryKeepaliveMessageByteID:
		keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(data[1:])
		if err != nil {
			return fmt.Errorf("%w: parse primary keepalive", ErrMalformedReplicationData)
		}

		r.observeServerWALEnd(keepalive.ServerWALEnd)

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
	started := r.now()
	err := r.sendStandbyStatus(ctx, conn, safeLSN)
	if err == nil {
		now := r.now()
		r.metrics.statusSent(ctx, now, now.Sub(started))
		r.setLastAcknowledgedLSN(safeLSN)
	}
	return err
}

// observeServerWALEnd records PostgreSQL's WAL end as reported on the
// stream. It never moves backwards: a reconnect may report an older one.
func (r *Reader) observeServerWALEnd(lsn pglogrepl.LSN) {
	for {
		current := r.metrics.serverWALEnd.Load()
		if uint64(lsn) <= current || r.metrics.serverWALEnd.CompareAndSwap(current, uint64(lsn)) {
			return
		}
	}
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
