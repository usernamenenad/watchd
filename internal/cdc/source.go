package cdc

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrSourceNotStarted indicates a Source method that needs Start to have
	// succeeded first.
	ErrSourceNotStarted = errors.New("cdc: source has not been started")
	// ErrSourceAlreadyStarted indicates a second Start call on one Source.
	ErrSourceAlreadyStarted = errors.New("cdc: source has already been started")
	// ErrSourceStopped indicates a snapshot request after the source's
	// stream ended. A snapshot is only useful paired with a live stream, so a
	// stopped source serves none; see Source.Err for why it stopped.
	ErrSourceStopped = errors.New("cdc: source has stopped")
)

// Source owns one PostgreSQL source: its Reader, the single replication slot
// every projection and scope shares, and the one Run loop that streams it.
// It is the one place that decides which Reader primitive each moment needs:
//
//   - Start finds an existing slot and resumes it with Run, or, when there is
//     none yet, waits for the first scope rather than creating a bare slot.
//   - The first Snapshot call on a source without a slot runs Bootstrap,
//     which creates the slot together with that scope's snapshot, and then
//     starts Run on the stream Bootstrap handed over.
//   - Every later Snapshot call runs Reader.Snapshot against the existing
//     slot, while Run keeps streaming.
//
// A Source is soft state around a durable slot: after a restart, Start
// resumes the slot, and every consumer takes a fresh snapshot.
type Source struct {
	reader *Reader

	// The Reader primitives Source composes. They default to reader's methods
	// and are replaced in unit tests.
	slotExists func(context.Context) (bool, error)
	bootstrap  func(context.Context, ProjectionSpec, Scope, SnapshotRowSink) (Snapshot, error)
	snapshot   func(context.Context, ProjectionSpec, Scope, SnapshotRowSink) (Snapshot, error)
	run        func(context.Context) error

	// mu guards the lifecycle state below. It is held across Bootstrap, so
	// concurrent first scopes wait for the one that creates the slot rather
	// than racing to create it.
	mu        sync.Mutex
	runCtx    context.Context
	started   bool
	streaming bool
	stopped   bool

	stopOnce sync.Once
	done     chan struct{}
	err      error
}

// NewSource validates configuration and creates a source whose stream
// delivers committed transactions to sink. It opens no connections; call
// Start.
func NewSource(config ReaderConfig, sink TransactionSink) (*Source, error) {
	reader, err := NewReader(config, sink)
	if err != nil {
		return nil, err
	}
	return &Source{
		reader:     reader,
		slotExists: reader.slotExists,
		bootstrap:  reader.Bootstrap,
		snapshot:   reader.Snapshot,
		run:        reader.Run,
		done:       make(chan struct{}),
	}, nil
}

// Start validates the source and begins streaming if its slot already
// exists. Otherwise the source waits for its first Snapshot call to create
// the slot. ctx bounds the source's whole life: cancelling it stops the
// stream, and Done then closes.
func (s *Source) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return ErrSourceAlreadyStarted
	}

	exists, err := s.slotExists(ctx)
	if err != nil {
		return err
	}

	s.started = true
	s.runCtx = ctx
	if exists {
		s.startRunLocked()
	}

	// A source still waiting for its first scope has no Run to report its
	// end, so its lifetime ends with ctx instead.
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.streaming {
			s.stopLocked(nil)
		}
	}()
	return nil
}

// Snapshot reads one scope of spec and returns the boundary where the
// source's stream takes over from its rows; see Snapshot.Covers. On a source
// without a slot, the first call creates it with Bootstrap and starts the
// stream; every later call uses Reader.Snapshot against that slot.
//
// Rows are delivered to sink while the call runs. If the call fails, rows
// already delivered must be discarded.
func (s *Source) Snapshot(ctx context.Context, spec ProjectionSpec, scope Scope, sink SnapshotRowSink) (Snapshot, error) {
	s.mu.Lock()
	if err := s.readyLocked(); err != nil {
		s.mu.Unlock()
		return Snapshot{}, err
	}
	if !s.streaming {
		defer s.mu.Unlock()
		snapshot, err := s.bootstrap(ctx, spec, scope, sink)
		if err == nil {
			s.startRunLocked()
			return snapshot, nil
		}
		if !errors.Is(err, ErrBootstrapSlotExists) {
			return Snapshot{}, err
		}
		// The slot appeared after Start looked for it, for example created by
		// another process. Resume it like one found at Start, and snapshot
		// this scope against it.
		s.startRunLocked()
	} else {
		s.mu.Unlock()
	}
	return s.snapshot(ctx, spec, scope, sink)
}

// Done is closed when the source stops: its stream ended, or its Start
// context was cancelled.
func (s *Source) Done() <-chan struct{} {
	return s.done
}

// Err reports why the source stopped once Done is closed: nil after a clean
// shutdown, or the stream's terminal error, such as ErrSlotInvalidated.
func (s *Source) Err() error {
	select {
	case <-s.done:
		return s.err
	default:
		return nil
	}
}

// Stats returns the underlying reader's point-in-time statistics.
func (s *Source) Stats() ReaderStats {
	return s.reader.Stats()
}

func (s *Source) readyLocked() error {
	switch {
	case !s.started:
		return ErrSourceNotStarted
	case s.stopped:
		return ErrSourceStopped
	}
	return nil
}

// startRunLocked starts the source's single Run loop. Callers hold mu.
func (s *Source) startRunLocked() {
	if s.streaming {
		return
	}
	s.streaming = true
	go func() {
		err := s.run(s.runCtx)
		s.mu.Lock()
		defer s.mu.Unlock()
		s.stopLocked(err)
	}()
}

func (s *Source) stopLocked(err error) {
	s.stopOnce.Do(func() {
		s.stopped = true
		s.err = err
		close(s.done)
	})
}

// slotExists validates the publication and reports whether the reader's slot
// exists and is a slot this reader can resume.
func (r *Reader) slotExists(ctx context.Context) (bool, error) {
	management, err := r.connectManagementWithTimeout(ctx)
	if err != nil {
		return false, err
	}
	defer r.closeManagementConnection(management)

	if err := r.validatePublication(ctx, management); err != nil {
		return false, err
	}
	slot, found, err := lookupSlot(ctx, management, r.config.SlotName)
	if err != nil || !found {
		return false, err
	}
	if slot.slotType != "logical" || slot.plugin != "pgoutput" {
		return false, ErrSlotInvalidated
	}
	return true, nil
}
