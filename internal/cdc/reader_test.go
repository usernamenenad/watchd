package cdc

import (
	"context"
	"testing"
)

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
