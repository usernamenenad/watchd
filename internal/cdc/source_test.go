package cdc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeSourcePrimitives stands in for the Reader primitives a Source composes
// and records which of them the Source chose.
type fakeSourcePrimitives struct {
	slotExists   bool
	slotErr      error
	bootstrapErr error

	bootstraps atomic.Int32
	snapshots  atomic.Int32
	runs       atomic.Int32

	// runResult, when set, is what Run returns once started; otherwise Run
	// blocks until its context is cancelled and returns nil.
	runResult chan error
	// bootstrapGate, when set, holds Bootstrap until it is closed.
	bootstrapGate chan struct{}
}

func newFakeSource(t *testing.T, fake *fakeSourcePrimitives) *Source {
	t.Helper()
	source, err := NewSource(ReaderConfig{
		DatabaseURL:     "postgres://example.invalid/watchd",
		SourceID:        "test-postgres",
		SlotName:        "watchd_source",
		PublicationName: "watchd_publication",
	}, func(context.Context, Transaction) error { return nil })
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	source.slotExists = func(context.Context) (bool, error) {
		return fake.slotExists, fake.slotErr
	}
	source.bootstrap = func(ctx context.Context, _ ProjectionSpec, _ Scope, _ SnapshotRowSink) (Snapshot, error) {
		fake.bootstraps.Add(1)
		if fake.bootstrapGate != nil {
			<-fake.bootstrapGate
		}
		return Snapshot{SourceID: "test-postgres"}, fake.bootstrapErr
	}
	source.snapshot = func(context.Context, ProjectionSpec, Scope, SnapshotRowSink) (Snapshot, error) {
		fake.snapshots.Add(1)
		return Snapshot{SourceID: "test-postgres"}, nil
	}
	source.run = func(ctx context.Context) error {
		fake.runs.Add(1)
		if fake.runResult != nil {
			select {
			case err := <-fake.runResult:
				return err
			case <-ctx.Done():
				return nil
			}
		}
		<-ctx.Done()
		return nil
	}
	return source
}

func takeSourceSnapshot(t *testing.T, source *Source) error {
	t.Helper()
	_, err := source.Snapshot(context.Background(), bootstrapUnitProjection(), Scope{Value: "tenant-a"}, func(context.Context, []map[string]any) error { return nil })
	return err
}

func bootstrapUnitProjection() ProjectionSpec {
	return ProjectionSpec{
		SourceID:    "test-postgres",
		Schema:      "public",
		Table:       "projection",
		ScopeColumn: "tenant_id",
		PrimaryKey:  []string{"tenant_id", "id"},
	}
}

func waitForCount(t *testing.T, name string, counter *atomic.Int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for counter.Load() != want {
		if time.Now().After(deadline) {
			t.Fatalf("%s = %d, want %d", name, counter.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSourceWithExistingSlotRunsAtStartAndSnapshotsEveryScope(t *testing.T) {
	fake := &fakeSourcePrimitives{slotExists: true}
	source := newFakeSource(t, fake)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := source.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitForCount(t, "runs", &fake.runs, 1)

	for range 2 {
		if err := takeSourceSnapshot(t, source); err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
	}
	if fake.bootstraps.Load() != 0 || fake.snapshots.Load() != 2 || fake.runs.Load() != 1 {
		t.Fatalf("bootstraps, snapshots, runs = %d, %d, %d; want 0, 2, 1", fake.bootstraps.Load(), fake.snapshots.Load(), fake.runs.Load())
	}
}

func TestSourceWithoutSlotBootstrapsFirstScopeThenSnapshotsLaterOnes(t *testing.T) {
	fake := &fakeSourcePrimitives{}
	source := newFakeSource(t, fake)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := source.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// No slot and no scope yet: nothing streams, and no bare slot is made.
	time.Sleep(10 * time.Millisecond)
	if fake.runs.Load() != 0 {
		t.Fatalf("runs before first scope = %d, want 0", fake.runs.Load())
	}

	if err := takeSourceSnapshot(t, source); err != nil {
		t.Fatalf("first Snapshot: %v", err)
	}
	waitForCount(t, "runs", &fake.runs, 1)
	if err := takeSourceSnapshot(t, source); err != nil {
		t.Fatalf("second Snapshot: %v", err)
	}
	if fake.bootstraps.Load() != 1 || fake.snapshots.Load() != 1 || fake.runs.Load() != 1 {
		t.Fatalf("bootstraps, snapshots, runs = %d, %d, %d; want 1, 1, 1", fake.bootstraps.Load(), fake.snapshots.Load(), fake.runs.Load())
	}
}

func TestSourceConcurrentFirstScopesBootstrapOnce(t *testing.T) {
	fake := &fakeSourcePrimitives{bootstrapGate: make(chan struct{})}
	source := newFakeSource(t, fake)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := source.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	const scopes = 5
	var wg sync.WaitGroup
	errs := make(chan error, scopes)
	for range scopes {
		wg.Go(func() { errs <- takeSourceSnapshot(t, source) })
	}
	waitForCount(t, "bootstraps", &fake.bootstraps, 1)
	close(fake.bootstrapGate)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
	}

	waitForCount(t, "runs", &fake.runs, 1)
	if fake.bootstraps.Load() != 1 || fake.snapshots.Load() != scopes-1 {
		t.Fatalf("bootstraps, snapshots = %d, %d; want 1, %d", fake.bootstraps.Load(), fake.snapshots.Load(), scopes-1)
	}
}

func TestSourceRetriesBootstrapAfterFailure(t *testing.T) {
	fake := &fakeSourcePrimitives{bootstrapErr: ErrSourceUnavailable}
	source := newFakeSource(t, fake)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := source.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := takeSourceSnapshot(t, source); !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("failed bootstrap error = %v, want %v", err, ErrSourceUnavailable)
	}
	if fake.runs.Load() != 0 {
		t.Fatalf("runs after failed bootstrap = %d, want 0", fake.runs.Load())
	}

	fake.bootstrapErr = nil
	if err := takeSourceSnapshot(t, source); err != nil {
		t.Fatalf("retried Snapshot: %v", err)
	}
	waitForCount(t, "runs", &fake.runs, 1)
	if fake.bootstraps.Load() != 2 {
		t.Fatalf("bootstraps = %d, want 2", fake.bootstraps.Load())
	}
}

// TestSourceResumesSlotCreatedAfterStart covers a slot that appears between
// Start and the first scope, e.g. created by another process: the source
// resumes it and snapshots the scope against it instead of failing.
func TestSourceResumesSlotCreatedAfterStart(t *testing.T) {
	fake := &fakeSourcePrimitives{bootstrapErr: ErrBootstrapSlotExists}
	source := newFakeSource(t, fake)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := source.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := takeSourceSnapshot(t, source); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	waitForCount(t, "runs", &fake.runs, 1)
	if fake.snapshots.Load() != 1 {
		t.Fatalf("snapshots = %d, want 1", fake.snapshots.Load())
	}
}

func TestSourceStopsWhenRunFails(t *testing.T) {
	fake := &fakeSourcePrimitives{slotExists: true, runResult: make(chan error, 1)}
	source := newFakeSource(t, fake)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := source.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	fake.runResult <- ErrSlotInvalidated
	select {
	case <-source.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("source did not stop after Run failed")
	}
	if !errors.Is(source.Err(), ErrSlotInvalidated) {
		t.Fatalf("Err = %v, want %v", source.Err(), ErrSlotInvalidated)
	}
	if err := takeSourceSnapshot(t, source); !errors.Is(err, ErrSourceStopped) {
		t.Fatalf("Snapshot after stop error = %v, want %v", err, ErrSourceStopped)
	}
}

func TestSourceStopsCleanlyWhenCancelledBeforeFirstScope(t *testing.T) {
	fake := &fakeSourcePrimitives{}
	source := newFakeSource(t, fake)
	ctx, cancel := context.WithCancel(context.Background())
	if err := source.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if source.Err() != nil {
		t.Fatalf("Err before stop = %v, want nil", source.Err())
	}

	cancel()
	select {
	case <-source.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("source did not stop after its context was cancelled")
	}
	if source.Err() != nil {
		t.Fatalf("Err after clean stop = %v, want nil", source.Err())
	}
}

func TestSourceRequiresExactlyOneStart(t *testing.T) {
	fake := &fakeSourcePrimitives{slotErr: ErrSourceUnavailable}
	source := newFakeSource(t, fake)
	if err := takeSourceSnapshot(t, source); !errors.Is(err, ErrSourceNotStarted) {
		t.Fatalf("Snapshot before Start error = %v, want %v", err, ErrSourceNotStarted)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// A failed Start leaves the source unstarted, so it can be retried.
	if err := source.Start(ctx); !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("failing Start error = %v, want %v", err, ErrSourceUnavailable)
	}
	fake.slotErr = nil
	if err := source.Start(ctx); err != nil {
		t.Fatalf("retried Start: %v", err)
	}
	if err := source.Start(ctx); !errors.Is(err, ErrSourceAlreadyStarted) {
		t.Fatalf("second Start error = %v, want %v", err, ErrSourceAlreadyStarted)
	}
}
