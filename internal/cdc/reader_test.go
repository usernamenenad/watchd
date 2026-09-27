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
	if reader.config.MaxValueBytes != defaultMaxValueBytes {
		t.Fatalf("MaxValueBytes = %d, want %d", reader.config.MaxValueBytes, defaultMaxValueBytes)
	}
	if reader.config.ConnectionTimeout != defaultConnectionTimeout {
		t.Fatalf("ConnectionTimeout = %s, want %s", reader.config.ConnectionTimeout, defaultConnectionTimeout)
	}
	if reader.config.StatusInterval != defaultStatusInterval {
		t.Fatalf("StatusInterval = %s, want %s", reader.config.StatusInterval, defaultStatusInterval)
	}
	if reader.config.MaxRetainedWALBytes != defaultMaxRetainedWALBytes {
		t.Fatalf("MaxRetainedWALBytes = %d, want %d", reader.config.MaxRetainedWALBytes, defaultMaxRetainedWALBytes)
	}
	if reader.config.RetentionCheckInterval != defaultRetentionCheckInterval {
		t.Fatalf("RetentionCheckInterval = %s, want %s", reader.config.RetentionCheckInterval, defaultRetentionCheckInterval)
	}
}

func TestNewReaderRejectsNegativeRetentionBudget(t *testing.T) {
	_, err := NewReader(ReaderConfig{
		DatabaseURL:         "postgres://example.invalid/watchd",
		SlotName:            "watchd_source",
		PublicationName:     "watchd_publication",
		MaxRetainedWALBytes: -1,
	}, func(context.Context, Transaction) error { return nil })
	if !errors.Is(err, ErrInvalidReaderConfig) {
		t.Fatalf("error = %v, want %v", err, ErrInvalidReaderConfig)
	}
}

func TestEffectiveRetentionBudget(t *testing.T) {
	tests := []struct {
		name            string
		configuredBytes int64
		serverBytes     int64
		serverUnbounded bool
		want            int64
	}{
		{"server unbounded keeps watchd budget", 1000, 0, true, 1000},
		{"server stricter clamps down", 1000, 400, false, 400},
		{"server looser keeps watchd budget", 1000, 5000, false, 1000},
		{"equal values", 1000, 1000, false, 1000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := effectiveRetentionBudget(tt.configuredBytes, tt.serverBytes, tt.serverUnbounded)
			if got != tt.want {
				t.Fatalf("effectiveRetentionBudget(%d, %d, %v) = %d, want %d", tt.configuredBytes, tt.serverBytes, tt.serverUnbounded, got, tt.want)
			}
		})
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

func TestNewReaderRejectsValueLimitLargerThanTransactionLimit(t *testing.T) {
	_, err := NewReader(ReaderConfig{
		DatabaseURL:         "postgres://example.invalid/watchd",
		SlotName:            "watchd_source",
		PublicationName:     "watchd_publication",
		MaxTransactionBytes: 1024,
		MaxValueBytes:       2048,
	}, func(context.Context, Transaction) error { return nil })
	if !errors.Is(err, ErrInvalidReaderConfig) {
		t.Fatalf("error = %v, want %v", err, ErrInvalidReaderConfig)
	}
}

func TestValueContractErrorsAreNotRetryable(t *testing.T) {
	for _, err := range []error{ErrUnsupportedColumnEncoding, ErrValueTooLarge} {
		if isRetryable(err) {
			t.Fatalf("isRetryable(%v) = true, want false: a value-contract violation will not resolve itself on reconnect", err)
		}
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
	}, Scope{Value: "tenant-a"}, func(context.Context, []map[string]any) error { return nil })
	if !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("Bootstrap error = %v, want %v", err, ErrSourceUnavailable)
	}
}

func TestPostgresBootstrapErrorsAreTyped(t *testing.T) {
	permissionError := classifyPostgresError(&pgconn.PgError{Code: "42501"})
	if !errors.Is(permissionError, ErrInsufficientPrivileges) {
		t.Fatalf("permission error = %v, want %v", permissionError, ErrInsufficientPrivileges)
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
