package cdc

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestValueContractErrorsAreNotRetryable(t *testing.T) {
	for _, err := range []error{ErrUnsupportedColumnEncoding, ErrValueTooLarge} {
		if isRetryable(err) {
			t.Fatalf("isRetryable(%v) = true, want false: a value-contract violation will not resolve itself on reconnect", err)
		}
	}
}

func TestPostgresBootstrapErrorsAreTyped(t *testing.T) {
	permissionError := classifyPostgresError(&pgconn.PgError{Code: "42501"})
	if !errors.Is(permissionError, ErrInsufficientPrivileges) {
		t.Fatalf("permission error = %v, want %v", permissionError, ErrInsufficientPrivileges)
	}
}
