package cdc

import (
	"context"
	"errors"
	"testing"
)

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
