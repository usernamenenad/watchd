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

func TestNewReaderAppliesSafeDefaults(t *testing.T) {
	reader, err := NewReader(ReaderConfig{
		DatabaseURL:     "postgres://example.invalid/watchd",
		SlotName:        "watchd_source",
		PublicationName: "watchd_publication",
	}, func(context.Context, Transaction) error { return nil })
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	if reader.config.MaxTransactionBytes != defaultMaxTransactionBytes {
		t.Fatalf("MaxTransactionBytes = %d, want %d", reader.config.MaxTransactionBytes, defaultMaxTransactionBytes)
	}
	if reader.config.MaxTransactionChanges != defaultMaxTransactionChanges {
		t.Fatalf("MaxTransactionChanges = %d, want %d", reader.config.MaxTransactionChanges, defaultMaxTransactionChanges)
	}
	if reader.config.ConnectionTimeout != defaultConnectionTimeout {
		t.Fatalf("ConnectionTimeout = %s, want %s", reader.config.ConnectionTimeout, defaultConnectionTimeout)
	}
	if reader.config.StatusInterval != defaultStatusInterval {
		t.Fatalf("StatusInterval = %s, want %s", reader.config.StatusInterval, defaultStatusInterval)
	}
}

func TestNewReaderRejectsUnsafeConfiguration(t *testing.T) {
	_, err := NewReader(ReaderConfig{
		DatabaseURL:     "postgres://example.invalid/watchd",
		SlotName:        "watchd-source; DROP TABLE users",
		PublicationName: "watchd_publication",
	}, func(context.Context, Transaction) error { return nil })
	if !errors.Is(err, ErrInvalidReaderConfig) {
		t.Fatalf("error = %v, want %v", err, ErrInvalidReaderConfig)
	}
}

func TestBootstrapClassifiesUnavailableSource(t *testing.T) {
	reader := newUnitReader(t)
	reader.connect = func(context.Context, string) (*CDC, error) {
		return nil, errors.New("network unavailable")
	}

	_, err := reader.Bootstrap(context.Background(), ProjectionSpec{
		SourceID:    "test-postgres",
		Schema:      "public",
		Table:       "projection",
		ScopeColumn: "tenant_id",
		PrimaryKey:  []string{"tenant_id", "id"},
	}, Scope{Value: "tenant-a"})
	if !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("Bootstrap error = %v, want %v", err, ErrSourceUnavailable)
	}
}

func TestPostgresBootstrapErrorsAreTyped(t *testing.T) {
	permissionError := classifyPostgresError(&pgconn.PgError{Code: "42501"})
	if !errors.Is(permissionError, ErrInsufficientPrivileges) {
		t.Fatalf("permission error = %v, want %v", permissionError, ErrInsufficientPrivileges)
	}

	snapshotError := classifySnapshotError(&pgconn.PgError{Code: "22023"})
	if !errors.Is(snapshotError, ErrSnapshotExpired) {
		t.Fatalf("snapshot error = %v, want %v", snapshotError, ErrSnapshotExpired)
	}
}

func TestReaderRetryDelayIsBoundedAndJittered(t *testing.T) {
	reader, err := NewReader(ReaderConfig{
		DatabaseURL:     "postgres://example.invalid/watchd",
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

func TestReaderRepliesToRequestedKeepaliveWithSafeLSN(t *testing.T) {
	reader := newUnitReader(t)
	var acknowledged pglogrepl.LSN
	reader.sendStandbyStatus = func(_ context.Context, _ *pgconn.PgConn, lsn pglogrepl.LSN) error {
		acknowledged = lsn
		return nil
	}

	data := make([]byte, 18)
	data[0] = pglogrepl.PrimaryKeepaliveMessageByteID
	binary.BigEndian.PutUint64(data[1:9], uint64(42))
	data[17] = 1 // ReplyRequested

	safeLSN := pglogrepl.LSN(7)
	if err := reader.consumeCopyData(context.Background(), nil, NewDecoder(), &safeLSN, data); err != nil {
		t.Fatalf("consume keepalive: %v", err)
	}
	if acknowledged != safeLSN {
		t.Fatalf("acknowledged LSN = %s, want safe LSN %s", acknowledged, safeLSN)
	}
	if reader.Stats().LastAcknowledgedLSN != safeLSN.String() {
		t.Fatalf("stats = %#v, want acknowledged LSN %s", reader.Stats(), safeLSN)
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

func newUnitReader(t *testing.T) *Reader {
	t.Helper()

	reader, err := NewReader(ReaderConfig{
		DatabaseURL:     "postgres://example.invalid/watchd",
		SlotName:        "watchd_source",
		PublicationName: "watchd_publication",
	}, func(context.Context, Transaction) error { return nil })
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return reader
}
