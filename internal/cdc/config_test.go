package cdc

import (
	"context"
	"errors"
	"testing"
)

func TestNewReaderAppliesSafeDefaults(t *testing.T) {
	reader, err := NewReader(ReaderConfig{
		DatabaseURL:     "postgres://example.invalid/watchd",
		SourceID:        "test-postgres",
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

func TestNewReaderRejectsUnsafeConfiguration(t *testing.T) {
	_, err := NewReader(ReaderConfig{
		DatabaseURL:     "postgres://example.invalid/watchd",
		SourceID:        "test-postgres",
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
		SourceID:            "test-postgres",
		SlotName:            "watchd_source",
		PublicationName:     "watchd_publication",
		MaxTransactionBytes: 1024,
		MaxValueBytes:       2048,
	}, func(context.Context, Transaction) error { return nil })
	if !errors.Is(err, ErrInvalidReaderConfig) {
		t.Fatalf("error = %v, want %v", err, ErrInvalidReaderConfig)
	}
}

func TestNewReaderRequiresSourceID(t *testing.T) {
	_, err := NewReader(ReaderConfig{
		DatabaseURL:     "postgres://example.invalid/watchd",
		SlotName:        "watchd_source",
		PublicationName: "watchd_publication",
	}, func(context.Context, Transaction) error { return nil })
	if !errors.Is(err, ErrInvalidReaderConfig) {
		t.Fatalf("error = %v, want %v", err, ErrInvalidReaderConfig)
	}
}
