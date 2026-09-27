package cdc

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestReaderRetryDelayIsBoundedAndJittered(t *testing.T) {
	reader, err := NewReader(ReaderConfig{
		DatabaseURL:     "postgres://example.invalid/watchd",
		SourceID:        "test-postgres",
		SlotName:        "watchd_source",
		PublicationName: "watchd_publication",
		RetryPolicy: RetryPolicy{
			InitialBackoff: time.Second,
			MaxBackoff:     4 * time.Second,
			MaxAttempts:    1,
			Jitter:         0.2,
		},
	}, func(context.Context, Transaction) error { return nil })
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	reader.random = func() float64 { return 1 }

	if got, want := reader.retryDelay(1), 1200*time.Millisecond; got != want {
		t.Fatalf("first delay = %s, want %s", got, want)
	}
	if got, want := reader.retryDelay(4), 4800*time.Millisecond; got != want {
		t.Fatalf("capped delay = %s, want %s", got, want)
	}
}

func keepaliveData(serverWALEnd uint64, replyRequested bool) []byte {
	data := make([]byte, 18)
	data[0] = pglogrepl.PrimaryKeepaliveMessageByteID
	binary.BigEndian.PutUint64(data[1:9], serverWALEnd)
	if replyRequested {
		data[17] = 1
	}
	return data
}

// TestReaderAdvancesToKeepaliveBetweenTransactions: between transactions a
// keepalive's position is safe to acknowledge, so an idle publication does
// not pin WAL; inside one it is not, because that transaction is not yet
// accepted.
func TestReaderAdvancesToKeepaliveBetweenTransactions(t *testing.T) {
	reader := newUnitReader(t)
	var acknowledged pglogrepl.LSN
	reader.sendStandbyStatus = func(_ context.Context, _ *pgconn.PgConn, lsn pglogrepl.LSN) error {
		acknowledged = lsn
		return nil
	}
	ctx := context.Background()
	decoder := NewDecoder()

	safeLSN := pglogrepl.LSN(7)
	if err := reader.consumeCopyData(ctx, nil, decoder, &safeLSN, keepaliveData(42, true)); err != nil {
		t.Fatalf("consume keepalive: %v", err)
	}
	if safeLSN != 42 || acknowledged != 42 {
		t.Fatalf("between transactions: safe LSN %s, acknowledged %s; want both 0/2A", safeLSN, acknowledged)
	}
	if reader.Stats().LastAcknowledgedLSN != pglogrepl.LSN(42).String() {
		t.Fatalf("stats = %#v, want acknowledged LSN 0/2A", reader.Stats())
	}

	// An older keepalive never moves it back.
	if err := reader.consumeCopyData(ctx, nil, decoder, &safeLSN, keepaliveData(30, true)); err != nil {
		t.Fatal(err)
	}
	if safeLSN != 42 {
		t.Fatalf("an older keepalive moved the safe LSN to %s", safeLSN)
	}

	// Inside a transaction, the keepalive's position is not acknowledged.
	if err := reader.consumeCopyData(ctx, nil, decoder, &safeLSN, xlogData(50, 50, beginPayload(90, time.Now(), 1))); err != nil {
		t.Fatal(err)
	}
	if err := reader.consumeCopyData(ctx, nil, decoder, &safeLSN, keepaliveData(80, true)); err != nil {
		t.Fatal(err)
	}
	if safeLSN != 42 || acknowledged != 42 {
		t.Fatalf("inside a transaction: safe LSN %s, acknowledged %s; want both 0/2A", safeLSN, acknowledged)
	}
}

func TestReaderRejectsMalformedAndUnknownCopyData(t *testing.T) {
	reader := newUnitReader(t)
	safeLSN := pglogrepl.LSN(0)

	err := reader.consumeCopyData(context.Background(), nil, NewDecoder(), &safeLSN, []byte{pglogrepl.XLogDataByteID})
	if !errors.Is(err, ErrMalformedReplicationData) {
		t.Fatalf("malformed XLogData error = %v, want %v", err, ErrMalformedReplicationData)
	}

	err = reader.consumeCopyData(context.Background(), nil, NewDecoder(), &safeLSN, []byte{'?'})
	if !errors.Is(err, ErrUnexpectedReplicationMessage) {
		t.Fatalf("unknown CopyData error = %v, want %v", err, ErrUnexpectedReplicationMessage)
	}
}

func TestReaderStopsAfterRetryBudgetExhausts(t *testing.T) {
	reader, err := NewReader(ReaderConfig{
		DatabaseURL:     "postgres://example.invalid/watchd",
		SourceID:        "test-postgres",
		SlotName:        "watchd_source",
		PublicationName: "watchd_publication",
		RetryPolicy: RetryPolicy{
			InitialBackoff: time.Millisecond,
			MaxBackoff:     time.Millisecond,
			MaxAttempts:    2,
			Jitter:         0.1,
		},
	}, func(context.Context, Transaction) error { return nil })
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	attempts := 0
	reader.connect = func(context.Context, string) (*CDC, error) {
		attempts++
		return nil, errors.New("network unavailable")
	}
	reader.wait = func(context.Context, time.Duration) error { return nil }

	err = reader.Run(context.Background())
	if !errors.Is(err, ErrRetryExhausted) {
		t.Fatalf("Run error = %v, want %v", err, ErrRetryExhausted)
	}
	if got, want := attempts, 3; got != want {
		t.Fatalf("connection attempts = %d, want %d", got, want)
	}
	if got, want := reader.Stats().ReconnectAttempts, uint64(2); got != want {
		t.Fatalf("reconnect attempts = %d, want %d", got, want)
	}
}
