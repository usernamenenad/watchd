package watchd

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
)

// Store holds a local projection and its resume cursor. Sync calls it from
// one goroutine at a time. Implementations backed by a database should make
// each method one local transaction.
type Store interface {
	// Cursor returns the persisted resume cursor, or "" if there is none.
	Cursor(ctx context.Context) (string, error)
	// BeginSnapshot starts replacing the whole projection. Until Commit, the
	// current projection must stay unchanged and readable.
	BeginSnapshot(ctx context.Context) (SnapshotWriter, error)
	// Apply applies one batch and persists batch.Cursor, together. It must
	// be idempotent.
	Apply(ctx context.Context, batch Batch) error
	// SaveCursor persists cursor as the resume point, when the server
	// confirms progress without a batch.
	SaveCursor(ctx context.Context, cursor string) error
}

// SnapshotWriter receives a snapshot's rows. Exactly one of Commit or Abort
// is called.
type SnapshotWriter interface {
	// Put adds rows to the new projection.
	Put(ctx context.Context, rows []Row) error
	// Commit replaces the projection with the rows put so far, in one step,
	// and clears the persisted cursor: the old one belongs to the replaced
	// projection. Sync saves a new cursor once the server confirms progress.
	Commit(ctx context.Context) error
	// Abort discards the rows put so far.
	Abort(ctx context.Context) error
}

// ErrMissingKey indicates a row or change without one of the store's
// primary-key columns.
var ErrMissingKey = errors.New("watchd: row is missing a primary-key column")

// MemoryStore is an in-memory Store for one projection, keyed by the
// projection's primary key. It is safe for concurrent readers while Sync
// writes to it, and readers never see a half-installed snapshot.
type MemoryStore struct {
	primaryKey []string

	mu     sync.RWMutex
	rows   map[string]Row
	cursor string
}

// NewMemoryStore returns an empty store for a projection whose primary key
// is primaryKey, in order.
func NewMemoryStore(primaryKey ...string) *MemoryStore {
	return &MemoryStore{primaryKey: primaryKey, rows: map[string]Row{}}
}

// Len returns the number of rows.
func (s *MemoryStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.rows)
}

// Get returns the row with the given primary-key values.
func (s *MemoryStore) Get(key map[string]string) (Row, bool) {
	id, err := s.keyOf(key)
	if err != nil {
		return nil, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	row, ok := s.rows[id]
	return maps.Clone(row), ok
}

// Rows returns a copy of every row, in no particular order.
func (s *MemoryStore) Rows() []Row {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rows := make([]Row, 0, len(s.rows))
	for _, row := range s.rows {
		rows = append(rows, maps.Clone(row))
	}
	return rows
}

// Cursor implements Store.
func (s *MemoryStore) Cursor(context.Context) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cursor, nil
}

// SaveCursor implements Store.
func (s *MemoryStore) SaveCursor(_ context.Context, cursor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursor = cursor
	return nil
}

// Apply implements Store. An insert or update replaces the row, keeping the
// current value of any Unchanged column; a delete removes it. Re-applying a
// batch leaves the same result.
func (s *MemoryStore) Apply(_ context.Context, batch Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, change := range batch.Changes {
		id, err := s.keyOf(change.Key)
		if err != nil {
			return err
		}
		switch change.Operation {
		case Delete:
			delete(s.rows, id)
		case Insert, Update:
			current := s.rows[id]
			row := make(Row, len(change.Values))
			for column, value := range change.Values {
				if value.Unchanged {
					if kept, ok := current[column]; ok {
						row[column] = kept
					}
					continue
				}
				row[column] = value
			}
			s.rows[id] = row
		default:
			return fmt.Errorf("watchd: unknown operation %v", change.Operation)
		}
	}
	s.cursor = batch.Cursor
	return nil
}

// BeginSnapshot implements Store. Rows collect beside the current projection
// and replace it at Commit.
func (s *MemoryStore) BeginSnapshot(context.Context) (SnapshotWriter, error) {
	return &memorySnapshot{store: s, rows: map[string]Row{}}, nil
}

type memorySnapshot struct {
	store *MemoryStore
	rows  map[string]Row
}

func (w *memorySnapshot) Put(_ context.Context, rows []Row) error {
	for _, row := range rows {
		key := make(map[string]string, len(w.store.primaryKey))
		for _, column := range w.store.primaryKey {
			value, ok := row[column]
			if !ok || value.Null || value.Unchanged {
				return fmt.Errorf("%w: %q", ErrMissingKey, column)
			}
			key[column] = value.Text
		}
		id, err := w.store.keyOf(key)
		if err != nil {
			return err
		}
		w.rows[id] = maps.Clone(row)
	}
	return nil
}

func (w *memorySnapshot) Commit(context.Context) error {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	w.store.rows = w.rows
	w.store.cursor = ""
	return nil
}

func (w *memorySnapshot) Abort(context.Context) error {
	w.rows = nil
	return nil
}

// keyOf encodes a row's primary-key values as one map key.
func (s *MemoryStore) keyOf(key map[string]string) (string, error) {
	parts := make([]string, len(s.primaryKey))
	for index, column := range s.primaryKey {
		value, ok := key[column]
		if !ok {
			return "", fmt.Errorf("%w: %q", ErrMissingKey, column)
		}
		parts[index] = value
	}
	return strings.Join(parts, "\x00"), nil
}
