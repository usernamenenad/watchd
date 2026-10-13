//go:build integration

package watch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/usernamenenad/watchd/internal/cdc"
)

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

var (
	integrationDatabaseURL    = envOrDefault("WATCHD_TEST_DATABASE_URL", "postgres://watchd_app:watchd_app@127.0.0.1:54329/watchd")
	integrationReplicationURL = envOrDefault("WATCHD_TEST_REPLICATION_URL", "postgres://watchd_replicator:watchd_replicator@127.0.0.1:54329/watchd")
	integrationAdminURL       = envOrDefault("WATCHD_TEST_ADMIN_URL", "postgres://postgres:postgres@127.0.0.1:54329/watchd")
)

// TestHubConvergesEveryScopeWithConcurrentWrites runs the hub on a real
// source. The first scope's watcher takes the Bootstrap path; a second scope
// joins while the stream is live and takes the Snapshot path. Writes to both
// scopes run throughout. Each watcher's projection, rebuilt only from its
// events, must equal PostgreSQL.
func TestHubConvergesEveryScopeWithConcurrentWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	app, err := pgx.Connect(ctx, integrationDatabaseURL)
	if err != nil {
		t.Fatalf("connect application database: %v", err)
	}
	defer app.Close(context.Background())
	if _, err := app.Exec(ctx, "DELETE FROM tenant_permissions_projection"); err != nil {
		t.Fatalf("clear projection: %v", err)
	}

	tenants := []string{"00000000-0000-0000-0000-0000000000a1", "00000000-0000-0000-0000-0000000000b2"}
	for _, tenant := range tenants {
		for user := range 5 {
			writeUser(t, ctx, app, tenant, user, "seed")
		}
	}

	spec := cdc.ProjectionSpec{
		SourceID:    "test-postgres",
		Schema:      "public",
		Table:       "tenant_permissions_projection",
		ScopeColumn: "tenant_id",
		PrimaryKey:  []string{"tenant_id", "user_id"},
	}
	slotName := fmt.Sprintf("watchd_hub_%d", time.Now().UnixNano())
	dropSlotOnCleanup(t, slotName)

	var hub *Hub
	source, err := cdc.NewSource(cdc.ReaderConfig{
		DatabaseURL:     integrationReplicationURL,
		SourceID:        "test-postgres",
		SlotName:        slotName,
		PublicationName: "watchd_publication",
		StatusInterval:  100 * time.Millisecond,
		ShutdownTimeout: time.Second,
	}, func(ctx context.Context, transaction cdc.Transaction) error { return hub.Accept(ctx, transaction) })
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	hub, err = New(Config{SourceID: "test-postgres", Projections: map[string]cdc.ProjectionSpec{"permissions": spec}}, source)
	if err != nil {
		t.Fatalf("New hub: %v", err)
	}
	sourceCtx, stopSource := context.WithCancel(ctx)
	defer func() {
		stopSource()
		<-source.Done()
	}()
	if err := source.Start(sourceCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Writers keep changing both scopes for the whole test.
	writersCtx, stopWriters := context.WithCancel(ctx)
	var writers sync.WaitGroup
	for _, tenant := range tenants {
		writers.Go(func() {
			writer, err := pgx.Connect(ctx, integrationDatabaseURL)
			if err != nil {
				t.Errorf("connect writer: %v", err)
				return
			}
			defer writer.Close(context.Background())
			for round := 0; writersCtx.Err() == nil; round++ {
				writeUser(t, ctx, writer, tenant, round%8, fmt.Sprintf("round-%d", round))
				if round%3 == 0 {
					if _, err := writer.Exec(ctx, "DELETE FROM tenant_permissions_projection WHERE tenant_id = $1 AND user_id = $2", tenant, userID(round%8)); err != nil && writersCtx.Err() == nil {
						t.Errorf("delete: %v", err)
					}
				}
			}
		})
	}

	first := newProjectionWatcher(t, ctx, hub, tenants[0])
	time.Sleep(200 * time.Millisecond)
	second := newProjectionWatcher(t, ctx, hub, tenants[1])
	time.Sleep(300 * time.Millisecond)

	stopWriters()
	writers.Wait()
	// A final marker write per scope: once a watcher has it, it has
	// everything committed before it.
	for _, tenant := range tenants {
		writeUser(t, ctx, app, tenant, 99, "final")
	}
	for index, watcher := range []*projectionWatcher{first, second} {
		got := watcher.waitForUser(t, userID(99))
		if want := readProjection(t, ctx, app, tenants[index]); !reflect.DeepEqual(got, want) {
			t.Errorf("scope %s: projection from events = %v, PostgreSQL = %v", tenants[index], got, want)
		}
	}
}

func userID(n int) string {
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", n+1)
}

func writeUser(t *testing.T, ctx context.Context, conn *pgx.Conn, tenant string, user int, role string) {
	t.Helper()
	if _, err := conn.Exec(ctx, `
		INSERT INTO tenant_permissions_projection (tenant_id, user_id, permissions)
		VALUES ($1, $2, jsonb_build_object('role', $3::text))
		ON CONFLICT (tenant_id, user_id) DO UPDATE SET permissions = EXCLUDED.permissions, version = tenant_permissions_projection.version + 1`,
		tenant, userID(user), role); err != nil && ctx.Err() == nil {
		t.Errorf("write user: %v", err)
	}
}

// projectionWatcher applies one watch's events to an in-memory projection of
// user_id -> permissions.
type projectionWatcher struct {
	mu      sync.Mutex
	rows    map[string]string
	pending map[string]string
	changed chan struct{}
}

func newProjectionWatcher(t *testing.T, ctx context.Context, hub *Hub, tenant string) *projectionWatcher {
	t.Helper()
	w := &projectionWatcher{rows: map[string]string{}, changed: make(chan struct{}, 1)}
	go func() {
		err := hub.Watch(ctx, Request{Projection: "permissions", Scope: tenant}, w.apply)
		if err != nil && ctx.Err() == nil {
			t.Errorf("watch %s: %v", tenant, err)
		}
	}()
	return w
}

func (w *projectionWatcher) apply(event Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch event := event.(type) {
	case SnapshotBegin:
		w.pending = map[string]string{}
	case SnapshotRows:
		for _, row := range event.Rows {
			w.pending[row["user_id"].(string)] = row["permissions"].(string)
		}
	case SnapshotEnd:
		w.rows, w.pending = w.pending, nil
	case Batch:
		for _, change := range event.Changes {
			if change.Operation == cdc.OperationDelete {
				delete(w.rows, change.Key["user_id"])
				continue
			}
			permissions, _ := change.Value("permissions")
			if permissions.Kind != cdc.ValueText {
				return fmt.Errorf("permissions = %s, want text", permissions)
			}
			w.rows[change.Key["user_id"]] = permissions.Text
		}
	case Resync:
		return fmt.Errorf("unexpected resync: %s", event.Reason)
	}
	select {
	case w.changed <- struct{}{}:
	default:
	}
	return nil
}

func (w *projectionWatcher) waitForUser(t *testing.T, user string) map[string]string {
	t.Helper()
	deadline := time.After(20 * time.Second)
	for {
		w.mu.Lock()
		if _, ok := w.rows[user]; ok && w.pending == nil {
			snapshot := make(map[string]string, len(w.rows))
			for key, value := range w.rows {
				snapshot[key] = value
			}
			w.mu.Unlock()
			return snapshot
		}
		w.mu.Unlock()
		select {
		case <-w.changed:
		case <-deadline:
			t.Fatalf("watcher never received user %s", user)
		}
	}
}

func readProjection(t *testing.T, ctx context.Context, conn *pgx.Conn, tenant string) map[string]string {
	t.Helper()
	rows, err := conn.Query(ctx, "SELECT user_id::text, permissions::text FROM tenant_permissions_projection WHERE tenant_id = $1", tenant)
	if err != nil {
		t.Fatalf("read projection: %v", err)
	}
	defer rows.Close()
	projection := map[string]string{}
	for rows.Next() {
		var user, permissions string
		if err := rows.Scan(&user, &permissions); err != nil {
			t.Fatalf("scan projection: %v", err)
		}
		projection[user] = permissions
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate projection: %v", err)
	}
	return projection
}

func dropSlotOnCleanup(t *testing.T, slotName string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		admin, err := pgx.Connect(ctx, integrationAdminURL)
		if err != nil {
			t.Errorf("connect admin: %v", err)
			return
		}
		defer admin.Close(ctx)
		_, _ = admin.Exec(ctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active", slotName)
	})
}

// TestDDLOnAProjectedTableResyncsAndRebuilds changes a projected table's
// shape while a watcher streams it. Every incompatible change must end the
// watch with Resync(schema-changed) rather than keep delivering rows of the
// old shape; a new snapshot must then rebuild the projection to equal
// PostgreSQL, or be refused when the table no longer matches its spec. A
// change that leaves the shape as it was must not resync.
func TestDDLOnAProjectedTableResyncsAndRebuilds(t *testing.T) {
	cases := []struct {
		name string
		ddl  string
		// resync says the change is incompatible; invalid, that re-validating
		// the spec afterwards must fail.
		resync, invalid bool
	}{
		{name: "add a column", ddl: "ALTER TABLE %s ADD COLUMN added text", resync: true},
		{name: "drop a column", ddl: "ALTER TABLE %s DROP COLUMN extra", resync: true},
		{name: "change a type", ddl: "ALTER TABLE %s ALTER COLUMN permissions TYPE text", resync: true},
		{name: "change the primary key", ddl: "ALTER TABLE %[1]s DROP CONSTRAINT %[1]s_pkey, ADD PRIMARY KEY (tenant_id, user_id, extra)", resync: true, invalid: true},
		{name: "change the replica identity", ddl: "ALTER TABLE %s REPLICA IDENTITY FULL", resync: true},
		{name: "a change that keeps the shape", ddl: "ALTER TABLE %s ALTER COLUMN extra SET DEFAULT 'x'"},
	}
	for index, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			table := fmt.Sprintf("ddl_projection_%d_%d", time.Now().UnixNano()%1_000_000, index)
			admin, err := pgx.Connect(ctx, integrationAdminURL)
			if err != nil {
				t.Fatalf("connect admin: %v", err)
			}
			defer admin.Close(context.Background())
			for _, statement := range []string{
				fmt.Sprintf(`CREATE TABLE %[1]s (
					tenant_id UUID NOT NULL, user_id UUID NOT NULL, permissions JSONB NOT NULL,
					extra TEXT NOT NULL DEFAULT 'none', CONSTRAINT %[1]s_pkey PRIMARY KEY (tenant_id, user_id))`, table),
				fmt.Sprintf("GRANT SELECT, INSERT, UPDATE, DELETE ON %s TO watchd_app", table),
				fmt.Sprintf("GRANT SELECT ON %s TO watchd_replicator", table),
				fmt.Sprintf("ALTER PUBLICATION watchd_publication ADD TABLE %s", table),
			} {
				if _, err := admin.Exec(ctx, statement); err != nil {
					t.Fatalf("%s: %v", statement, err)
				}
			}
			defer func() { _, _ = admin.Exec(context.Background(), "DROP TABLE "+table) }()

			tenant := "00000000-0000-0000-0000-0000000000c3"
			write := func(user int, role string) {
				t.Helper()
				if _, err := admin.Exec(ctx, fmt.Sprintf(
					"INSERT INTO %s (tenant_id, user_id, permissions) VALUES ($1, $2, jsonb_build_object('role', $3::text))", table),
					tenant, userID(user), role); err != nil {
					t.Fatalf("write user %d: %v", user, err)
				}
			}
			write(1, "seed")

			spec := cdc.ProjectionSpec{
				SourceID: "test-postgres", Schema: "public", Table: table,
				ScopeColumn: "tenant_id", PrimaryKey: []string{"tenant_id", "user_id"},
			}
			slotName := fmt.Sprintf("watchd_ddl_%d", time.Now().UnixNano())
			dropSlotOnCleanup(t, slotName)
			var hub *Hub
			source, err := cdc.NewSource(cdc.ReaderConfig{
				DatabaseURL: integrationReplicationURL, SourceID: "test-postgres", SlotName: slotName,
				PublicationName: "watchd_publication", StatusInterval: 100 * time.Millisecond, ShutdownTimeout: time.Second,
			}, func(ctx context.Context, transaction cdc.Transaction) error { return hub.Accept(ctx, transaction) })
			if err != nil {
				t.Fatalf("NewSource: %v", err)
			}
			hub, err = New(Config{SourceID: "test-postgres", Projections: map[string]cdc.ProjectionSpec{"permissions": spec}}, source)
			if err != nil {
				t.Fatalf("New hub: %v", err)
			}
			sourceCtx, stopSource := context.WithCancel(ctx)
			defer func() {
				stopSource()
				<-source.Done()
			}()
			if err := source.Start(sourceCtx); err != nil {
				t.Fatalf("Start: %v", err)
			}

			read := func() map[string]string {
				t.Helper()
				rows, err := admin.Query(ctx, fmt.Sprintf("SELECT user_id::text, permissions::text FROM %s WHERE tenant_id = $1", table), tenant)
				if err != nil {
					t.Fatalf("read projection: %v", err)
				}
				defer rows.Close()
				projection := map[string]string{}
				for rows.Next() {
					var user, permissions string
					if err := rows.Scan(&user, &permissions); err != nil {
						t.Fatalf("scan projection: %v", err)
					}
					projection[user] = permissions
				}
				return projection
			}

			first := newScopeEvents(ctx, hub, tenant)
			first.applyUntil(t, func(p map[string]string) bool { return len(p) == 1 && first.cursor != "" })

			if _, err := admin.Exec(ctx, fmt.Sprintf(test.ddl, table)); err != nil {
				t.Fatalf("ddl: %v", err)
			}
			write(2, "after")

			if !test.resync {
				first.applyUntil(t, func(p map[string]string) bool { return len(p) == 2 })
				if got, want := first.projection, read(); !reflect.DeepEqual(got, want) {
					t.Fatalf("projection from events = %v, PostgreSQL = %v", got, want)
				}
				return
			}
			if reason := first.waitForResync(t); reason != ResyncSchemaChanged {
				t.Fatalf("resync reason = %s, want %s", reason, ResyncSchemaChanged)
			}

			// Resuming is refused: the changes after the DDL were not delivered.
			resumed := newScopeEventsFrom(ctx, hub, tenant, first.cursor)
			if reason := resumed.waitForResync(t); reason != ResyncSchemaChanged {
				t.Fatalf("resume resync reason = %s, want %s", reason, ResyncSchemaChanged)
			}

			rebuilt := newScopeEvents(ctx, hub, tenant)
			if test.invalid {
				if err := rebuilt.waitForError(t); !errors.Is(err, cdc.ErrInvalidReaderConfig) {
					t.Fatalf("watch error = %v, want a rejected spec", err)
				}
				return
			}
			rebuilt.applyUntil(t, func(p map[string]string) bool { return len(p) == 2 })
			write(3, "later")
			rebuilt.applyUntil(t, func(p map[string]string) bool { return len(p) == 3 })
			if got, want := rebuilt.projection, read(); !reflect.DeepEqual(got, want) {
				t.Fatalf("rebuilt projection = %v, PostgreSQL = %v", got, want)
			}
		})
	}
}

// scopeEvents applies one watch's events to a projection of user_id ->
// permissions, and records how the watch ended.
type scopeEvents struct {
	events     chan Event
	done       chan error
	projection map[string]string
	pending    map[string]string
	cursor     string
}

func newScopeEvents(ctx context.Context, hub *Hub, tenant string) *scopeEvents {
	return newScopeEventsFrom(ctx, hub, tenant, "")
}

func newScopeEventsFrom(ctx context.Context, hub *Hub, tenant, resume string) *scopeEvents {
	s := &scopeEvents{events: make(chan Event, 256), done: make(chan error, 1)}
	go func() {
		s.done <- hub.Watch(ctx, Request{Projection: "permissions", Scope: tenant, ResumeCursor: resume}, func(event Event) error {
			select {
			case s.events <- event:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	return s
}

func (s *scopeEvents) next(t *testing.T) (Event, error) {
	t.Helper()
	select {
	case event := <-s.events:
		return event, nil
	case err := <-s.done:
		select {
		case event := <-s.events:
			s.done <- err
			return event, nil
		default:
		}
		return nil, err
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for a watch event")
	}
	return nil, nil
}

func (s *scopeEvents) applyUntil(t *testing.T, done func(map[string]string) bool) {
	t.Helper()
	for s.projection == nil || s.pending != nil || !done(s.projection) {
		event, err := s.next(t)
		if event == nil {
			t.Fatalf("watch ended with %v", err)
		}
		switch event := event.(type) {
		case SnapshotBegin:
			s.pending = map[string]string{}
		case SnapshotRows:
			for _, row := range event.Rows {
				s.pending[row["user_id"].(string)] = fmt.Sprint(row["permissions"])
			}
		case SnapshotEnd:
			s.projection, s.pending = s.pending, nil
		case Batch:
			for _, change := range event.Changes {
				if change.Operation == cdc.OperationDelete {
					delete(s.projection, change.Key["user_id"])
					continue
				}
				permissions, _ := change.Value("permissions")
				s.projection[change.Key["user_id"]] = permissions.Text
			}
			s.cursor = event.Cursor.String()
		case Progress:
			s.cursor = event.Cursor.String()
		case Resync:
			t.Fatalf("unexpected resync: %s", event.Reason)
		}
	}
}

func (s *scopeEvents) waitForResync(t *testing.T) ResyncReason {
	t.Helper()
	for {
		event, err := s.next(t)
		if event == nil {
			t.Fatalf("watch ended with %v before a Resync", err)
		}
		switch event := event.(type) {
		case Resync:
			return event.Reason
		case Batch:
			t.Fatalf("a batch of the changed table was delivered: %#v", event)
		}
	}
}

func (s *scopeEvents) waitForError(t *testing.T) error {
	t.Helper()
	for {
		event, err := s.next(t)
		if event == nil {
			return err
		}
		if resync, ok := event.(Resync); ok {
			t.Fatalf("resync %s, want the watch to fail", resync.Reason)
		}
	}
}
