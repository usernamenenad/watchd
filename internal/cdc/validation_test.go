package cdc

import (
	"errors"
	"testing"
)

func TestProjectionSpecRequiresScopeColumnInPrimaryKey(t *testing.T) {
	spec := ProjectionSpec{
		SourceID:    "test-postgres",
		Schema:      "public",
		Table:       "projection",
		ScopeColumn: "tenant_id",
		PrimaryKey:  []string{"id"},
	}
	if err := validateProjectionSpecConfig(spec); !errors.Is(err, ErrInvalidReaderConfig) {
		t.Fatalf("scope column outside primary key error = %v, want %v", err, ErrInvalidReaderConfig)
	}

	spec.PrimaryKey = []string{"tenant_id", "id"}
	if err := validateProjectionSpecConfig(spec); err != nil {
		t.Fatalf("scope column in primary key: %v", err)
	}
}

func TestReaderRejectsProjectionFromAnotherSource(t *testing.T) {
	reader := newUnitReader(t)
	spec := ProjectionSpec{
		SourceID:    "other-source",
		Schema:      "public",
		Table:       "projection",
		ScopeColumn: "tenant_id",
		PrimaryKey:  []string{"tenant_id", "id"},
	}
	if err := reader.validateSourceProjectionSpec(spec); !errors.Is(err, ErrInvalidReaderConfig) {
		t.Fatalf("foreign projection error = %v, want %v", err, ErrInvalidReaderConfig)
	}
}
