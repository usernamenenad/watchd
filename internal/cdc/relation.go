package cdc

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// ErrIncompatibleRelation is matched (errors.Is) by every
// *IncompatibleRelationError: a projected table's shape in the change stream
// no longer matches the shape its spec was validated against.
var ErrIncompatibleRelation = errors.New("cdc: projected table changed incompatibly")

// IncompatibleRelationError says a projected table changed shape mid-stream:
// a column was added, dropped, or retyped, its replica identity or key
// changed, or the table was renamed or recreated. The projection can no
// longer be kept fresh from the stream, so its watchers must resync. It
// travels on Transaction.Incompatible rather than failing the stream, which
// would stop every other projection too.
type IncompatibleRelationError struct {
	// Table is the qualified name (schema.table) of the projected table.
	Table string
	// Difference describes the first difference found, for logs.
	Difference string
}

func (e *IncompatibleRelationError) Error() string {
	return fmt.Sprintf("%s: %s: %s", ErrIncompatibleRelation.Error(), e.Table, e.Difference)
}

func (e *IncompatibleRelationError) Is(target error) bool { return target == ErrIncompatibleRelation }

// relationShape is what row decoding depends on in a table: the columns the
// stream carries, in order, with their types, and how rows are keyed. The
// baseline is read from the catalog when a spec is validated; the actual
// shape comes from each pgoutput RelationMessage.
type relationShape struct {
	table           string
	oid             uint32
	replicaIdentity byte
	columns         []shapeColumn
	// key holds the replica-identity column names, sorted.
	key []string
}

type shapeColumn struct {
	name    string
	typeOID uint32
}

func shapeOfRelation(message *pglogrepl.RelationMessage) relationShape {
	shape := relationShape{
		table:           qualifiedTable(message.Namespace, message.RelationName),
		oid:             message.RelationID,
		replicaIdentity: message.ReplicaIdentity,
		columns:         make([]shapeColumn, len(message.Columns)),
	}
	for index, column := range message.Columns {
		shape.columns[index] = shapeColumn{name: column.Name, typeOID: column.DataType}
		if column.Flags == 1 {
			shape.key = append(shape.key, column.Name)
		}
	}
	slices.Sort(shape.key)

	return shape
}

// diff describes how actual departs from s, or returns "" if it does not.
func (s *relationShape) diff(actual *relationShape) string {
	switch {
	case s.oid != actual.oid:
		return fmt.Sprintf("table was dropped and recreated (relation %d, now %d)", s.oid, actual.oid)
	case s.replicaIdentity != actual.replicaIdentity:
		return fmt.Sprintf("replica identity changed from %q to %q", s.replicaIdentity, actual.replicaIdentity)
	case !slices.Equal(s.key, actual.key):
		return fmt.Sprintf("key columns changed from (%s) to (%s)", strings.Join(s.key, ","), strings.Join(actual.key, ","))
	}
	for index := range max(len(s.columns), len(actual.columns)) {
		switch {
		case index >= len(actual.columns):
			return fmt.Sprintf("column %q was dropped", s.columns[index].name)
		case index >= len(s.columns):
			return fmt.Sprintf("column %q was added", actual.columns[index].name)
		case s.columns[index].name != actual.columns[index].name:
			if !slices.ContainsFunc(actual.columns, func(c shapeColumn) bool { return c.name == s.columns[index].name }) {
				return fmt.Sprintf("column %q was dropped or renamed", s.columns[index].name)
			}
			return fmt.Sprintf("column %q was added or moved", actual.columns[index].name)
		case s.columns[index].typeOID != actual.columns[index].typeOID:
			return fmt.Sprintf("column %q changed type (type %d, now %d)", s.columns[index].name, s.columns[index].typeOID, actual.columns[index].typeOID)
		}
	}

	return ""
}

// shapeRegistry is what a Decoder checks relations against: the baselines of
// every validated projection table. Its generation changes whenever a
// baseline does, so a decoder re-checks only then.
type shapeRegistry interface {
	generation() uint64
	baselineByTable(table string) *relationShape
	baselineByOID(oid uint32) *relationShape
}

// shapes is the Reader's shapeRegistry. A baseline is recorded each time a
// projection spec validates against the live source, so re-validating is
// what accepts a table's new shape.
type shapes struct {
	gen     atomic.Uint64
	mu      sync.RWMutex
	byTable map[string]*relationShape
	byOID   map[uint32]*relationShape
}

func (s *shapes) generation() uint64 { return s.gen.Load() }

func (s *shapes) baselineByTable(table string) *relationShape {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.byTable[table]
}

func (s *shapes) baselineByOID(oid uint32) *relationShape {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.byOID[oid]
}

// record makes shape the baseline of its table. An unchanged shape leaves
// the generation alone, so validating another scope of the same table does
// not make the decoder re-check everything.
func (s *shapes) record(shape *relationShape) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.byTable == nil {
		s.byTable = make(map[string]*relationShape)
		s.byOID = make(map[uint32]*relationShape)
	}
	if previous := s.byTable[shape.table]; previous != nil {
		if previous.diff(shape) == "" {
			return
		}
		delete(s.byOID, previous.oid)
	}
	s.byTable[shape.table] = shape
	s.byOID[shape.oid] = shape
	s.gen.Add(1)
}

// projectionShape reads spec's table shape as the stream will present it:
// the columns the publication carries (a PostgreSQL 15+ column list narrows
// them), their types, and the replica-identity key.
func (r *Reader) projectionShape(ctx context.Context, management *pgx.Conn, spec ProjectionSpec) (*relationShape, error) {
	qualified := pgx.Identifier{spec.Schema, spec.Table}.Sanitize()
	shape := &relationShape{table: spec.Schema + "." + spec.Table}

	var oid int64
	var identity string
	var published []string
	err := management.QueryRow(ctx, `
		SELECT class.oid::int8, class.relreplident::text, COALESCE(
			(SELECT array_agg(attribute.attname::text ORDER BY attribute.attnum)
			 FROM pg_attribute AS attribute
			 WHERE attribute.attrelid = class.oid
			   AND attribute.attnum > 0
			   AND NOT attribute.attisdropped
			   AND attribute.attname::text = ANY (publication.attnames)),
			'{}')
		FROM pg_class AS class
		JOIN pg_publication_tables AS publication
		  ON publication.pubname = $2 AND publication.schemaname = $3 AND publication.tablename = $4
		WHERE class.oid = to_regclass($1)`,
		qualified, r.config.PublicationName, spec.Schema, spec.Table).Scan(&oid, &identity, &published)
	if err != nil {
		return nil, classifyPostgresError(err)
	}
	if len(identity) != 1 {
		return nil, fmt.Errorf("%w: projection table %s has unexpected replica identity %q", ErrInvalidReaderConfig, qualified, identity)
	}
	shape.oid = uint32(oid)
	shape.replicaIdentity = identity[0]

	rows, err := management.Query(ctx, `
		SELECT attribute.attname::text, attribute.atttypid::int8
		FROM pg_attribute AS attribute
		WHERE attribute.attrelid = to_regclass($1)
		  AND attribute.attnum > 0
		  AND NOT attribute.attisdropped
		  AND attribute.attname::text = ANY ($2)
		ORDER BY attribute.attnum`, qualified, published)
	if err != nil {
		return nil, classifyPostgresError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var typeOID int64
		if err := rows.Scan(&name, &typeOID); err != nil {
			return nil, classifyPostgresError(err)
		}
		shape.columns = append(shape.columns, shapeColumn{name: name, typeOID: uint32(typeOID)})
	}
	if err := rows.Err(); err != nil {
		return nil, classifyPostgresError(err)
	}

	switch shape.replicaIdentity {
	case 'd':
		shape.key = slices.Clone(spec.PrimaryKey)
	case 'f':
		for _, column := range shape.columns {
			shape.key = append(shape.key, column.name)
		}
	case 'i':
		keyRows, err := management.Query(ctx, `
			SELECT attribute.attname::text
			FROM pg_index AS index
			JOIN pg_attribute AS attribute
			  ON attribute.attrelid = index.indrelid AND attribute.attnum = ANY (index.indkey)
			WHERE index.indrelid = to_regclass($1) AND index.indisreplident`, qualified)
		if err != nil {
			return nil, classifyPostgresError(err)
		}
		defer keyRows.Close()
		for keyRows.Next() {
			var name string
			if err := keyRows.Scan(&name); err != nil {
				return nil, classifyPostgresError(err)
			}
			shape.key = append(shape.key, name)
		}
		if err := keyRows.Err(); err != nil {
			return nil, classifyPostgresError(err)
		}
	}
	slices.Sort(shape.key)

	return shape, nil
}
