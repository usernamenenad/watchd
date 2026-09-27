//go:build integration

package watch

import (
	"context"
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
			w.rows[change.Key["user_id"]] = change.Values["permissions"].(string)
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
