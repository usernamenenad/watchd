package cdc

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (r *Reader) validatePublication(ctx context.Context, management *pgx.Conn) error {
	var publishesInsert, publishesUpdate, publishesDelete, publishesTruncate bool
	err := management.QueryRow(ctx, `
		SELECT pubinsert, pubupdate, pubdelete, pubtruncate
		FROM pg_publication
		WHERE pubname = $1`, r.config.PublicationName).Scan(&publishesInsert, &publishesUpdate, &publishesDelete, &publishesTruncate)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: publication %q does not exist", ErrInvalidReaderConfig, r.config.PublicationName)
	}
	if err != nil {
		return classifyPostgresError(err)
	}
	if publishesTruncate {
		return fmt.Errorf("%w: publication %q publishes TRUNCATE, which v0 does not support", ErrInvalidReaderConfig, r.config.PublicationName)
	}
	if !publishesInsert || !publishesUpdate || !publishesDelete {
		return fmt.Errorf("%w: publication %q must publish INSERT, UPDATE, and DELETE", ErrInvalidReaderConfig, r.config.PublicationName)
	}
	return nil
}

func (r *Reader) validateProjectionSpec(ctx context.Context, management *pgx.Conn, spec ProjectionSpec) error {
	qualifiedTable := pgx.Identifier{spec.Schema, spec.Table}.Sanitize()
	var tableExists, scopeColumnExists bool
	err := management.QueryRow(ctx, `
		WITH projection AS (
			SELECT oid
			FROM pg_class
			WHERE oid = to_regclass($1)
			  AND relkind IN ('r', 'p')
		)
		SELECT
			EXISTS (SELECT 1 FROM projection),
			EXISTS (
				SELECT 1
				FROM pg_attribute
				WHERE attrelid = (SELECT oid FROM projection)
				  AND attname = $2
				  AND attnum > 0
				  AND NOT attisdropped
			)`, qualifiedTable, spec.ScopeColumn).Scan(&tableExists, &scopeColumnExists)
	if err != nil {
		return classifyPostgresError(err)
	}
	if !tableExists {
		return fmt.Errorf("%w: projection table %q.%q does not exist", ErrInvalidReaderConfig, spec.Schema, spec.Table)
	}
	if !scopeColumnExists {
		return fmt.Errorf("%w: projection table %q.%q has no scope column %q", ErrInvalidReaderConfig, spec.Schema, spec.Table, spec.ScopeColumn)
	}

	primaryKey, err := projectionPrimaryKey(ctx, management, qualifiedTable)
	if err != nil {
		return err
	}
	if !sameStrings(primaryKey, spec.PrimaryKey) {
		return fmt.Errorf("%w: projection table %q.%q primary key does not match configured primary key", ErrInvalidReaderConfig, spec.Schema, spec.Table)
	}

	var published bool
	err = management.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_publication_tables
			WHERE pubname = $1
			  AND schemaname = $2
			  AND tablename = $3
		)`, r.config.PublicationName, spec.Schema, spec.Table).Scan(&published)
	if err != nil {
		return classifyPostgresError(err)
	}
	if !published {
		return fmt.Errorf("%w: publication %q does not include projection table %q.%q", ErrInvalidReaderConfig, r.config.PublicationName, spec.Schema, spec.Table)
	}

	return nil
}

// ValidateProjectionSpec checks spec's static configuration - identifiers,
// a primary key, and a scope column that is part of it - without connecting
// to PostgreSQL. Snapshot and Bootstrap additionally check spec against the
// live source.
func ValidateProjectionSpec(spec ProjectionSpec) error {
	return validateProjectionSpecConfig(spec)
}

func validateProjectionSpecConfig(spec ProjectionSpec) error {
	if spec.SourceID == "" || spec.Schema == "" || spec.Table == "" || spec.ScopeColumn == "" || len(spec.PrimaryKey) == 0 {
		return ErrInvalidReaderConfig
	}
	if !postgresIdentifier.MatchString(spec.Schema) || !postgresIdentifier.MatchString(spec.Table) || !postgresIdentifier.MatchString(spec.ScopeColumn) {
		return fmt.Errorf("%w: projection schema, table, and scope column must be unquoted PostgreSQL identifiers", ErrInvalidReaderConfig)
	}
	seen := make(map[string]struct{}, len(spec.PrimaryKey))
	for _, column := range spec.PrimaryKey {
		if !postgresIdentifier.MatchString(column) || column == "" {
			return fmt.Errorf("%w: projection primary-key columns must be unquoted PostgreSQL identifiers", ErrInvalidReaderConfig)
		}
		if _, duplicate := seen[column]; duplicate {
			return fmt.Errorf("%w: projection primary key contains duplicate column %q", ErrInvalidReaderConfig, column)
		}
		seen[column] = struct{}{}
	}
	// With the default replica identity, a DELETE carries only the primary
	// key. The scope column must be part of it, or a delete could not be
	// routed to the scope whose projection holds the row.
	if _, found := seen[spec.ScopeColumn]; !found {
		return fmt.Errorf("%w: projection scope column %q must be part of the primary key", ErrInvalidReaderConfig, spec.ScopeColumn)
	}
	return nil
}

// validateSourceProjectionSpec validates spec's static configuration and
// that it belongs to this reader's source, so every cursor the reader hands
// out for it is comparable with the stream's.
func (r *Reader) validateSourceProjectionSpec(spec ProjectionSpec) error {
	if err := validateProjectionSpecConfig(spec); err != nil {
		return err
	}
	if spec.SourceID != r.config.SourceID {
		return fmt.Errorf("%w: projection source %q does not match reader source %q", ErrInvalidReaderConfig, spec.SourceID, r.config.SourceID)
	}
	return nil
}

func projectionPrimaryKey(ctx context.Context, management *pgx.Conn, qualifiedTable string) ([]string, error) {
	rows, err := management.Query(ctx, `
		SELECT attribute.attname
		FROM pg_index AS index
		JOIN LATERAL unnest(index.indkey) WITH ORDINALITY AS key(attnum, position) ON true
		JOIN pg_attribute AS attribute ON attribute.attrelid = index.indrelid AND attribute.attnum = key.attnum
		WHERE index.indrelid = to_regclass($1)
		  AND index.indisprimary
		ORDER BY key.position`, qualifiedTable)
	if err != nil {
		return nil, classifyPostgresError(err)
	}
	defer rows.Close()

	var primaryKey []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, classifyPostgresError(err)
		}
		primaryKey = append(primaryKey, column)
	}
	if err := rows.Err(); err != nil {
		return nil, classifyPostgresError(err)
	}
	if len(primaryKey) == 0 {
		return nil, fmt.Errorf("%w: projection table %s has no primary key", ErrInvalidReaderConfig, qualifiedTable)
	}
	return primaryKey, nil
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
