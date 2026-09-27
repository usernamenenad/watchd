package cdc

import (
	"context"
	"errors"
	"testing"
)

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
