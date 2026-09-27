//go:build integration

package integration

import (
	"context"
	"fmt"
	"maps"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/usernamenenad/watchd/internal/daemon"
	"github.com/usernamenenad/watchd/sdk/go/watchd"
)

// TestWatchdEndToEnd runs the watchd process against PostgreSQL with real
// SDK clients, through every path the process chooses between:
//
//  1. The first client finds no slot: its snapshot creates the slot
//     (Bootstrap), and later writes reach it live.
//  2. A second scope joins while the stream is live (Snapshot) and gets only
//     its own rows.
//  3. The process restarts on the existing slot (Run). Clients reconnect
//     with their cursors, are told to resync because the new process has no
//     history, and rebuild - including a write made while watchd was down.
//
// Throughout, a client is fresh only when its projection equals PostgreSQL.
func TestWatchdEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	app := connectApplication(t, ctx)
	defer app.Close(context.Background())
	clearProjection(t, ctx, app)

	tenantA := "00000000-0000-0000-0000-0000000000e1"
	tenantB := "00000000-0000-0000-0000-0000000000e2"
	setPermission(t, ctx, app, tenantA, 1, "editor")
	setPermission(t, ctx, app, tenantA, 2, "viewer")
	setPermission(t, ctx, app, tenantB, 1, "owner")

	slotName := fmt.Sprintf("watchd_e2e_%d", time.Now().UnixNano())
	cfg, err := daemon.ParseConfig(stringsReader(fmt.Sprintf(`{
		"source_id": "e2e",
		"slot_name": %q,
		"publication_name": "watchd_publication",
		"listen_address": "127.0.0.1:0",
		"shutdown_timeout": "3s",
		"projections": {
			"tenant_permissions": {
				"schema": "public",
				"table": "tenant_permissions_projection",
				"scope_column": "tenant_id",
				"primary_key": ["tenant_id", "user_id"]
			}
		}
	}`, slotName)), envOrDefault("WATCHD_TEST_REPLICATION_URL", defaultReplicationURL))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		dropSlot(t, cleanupCtx, slotName)
	})

	// The address stays the same across the restart, as it would for a
	// real service.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick address: %v", err)
	}
	address := probe.Addr().String()
	_ = probe.Close()

	stopFirst := startWatchd(t, ctx, cfg, address)
	if slotExists(t, ctx, app, slotName) {
		t.Fatal("watchd created a slot before any client asked for a scope")
	}

	// 1. Bootstrap: the first client creates the slot.
	clientA := startClient(t, ctx, address, tenantA)
	clientA.waitFresh(t, ctx, app)
	if !slotExists(t, ctx, app, slotName) {
		t.Fatal("the first client's snapshot did not create the slot")
	}
	setPermission(t, ctx, app, tenantA, 3, "admin")
	setPermission(t, ctx, app, tenantA, 1, "owner")
	deletePermission(t, ctx, app, tenantA, 2)
	clientA.waitFresh(t, ctx, app)

	// 2. Snapshot: a second scope joins the live stream.
	clientB := startClient(t, ctx, address, tenantB)
	clientB.waitFresh(t, ctx, app)
	setPermission(t, ctx, app, tenantB, 2, "viewer")
	clientB.waitFresh(t, ctx, app)
	clientA.waitFresh(t, ctx, app) // B's writes never reach A

	// 3. Run: restart on the existing slot, with a write while it is down.
	stopFirst()
	clientA.waitStale(t, ctx)
	setPermission(t, ctx, app, tenantA, 4, "written-while-down")
	stopSecond := startWatchd(t, ctx, cfg, address)
	defer stopSecond()
	clientA.waitFresh(t, ctx, app)
	clientB.waitFresh(t, ctx, app)
	if !clientA.sawResync() {
		t.Fatal("after a restart the client must rebuild from a snapshot, not resume")
	}
	setPermission(t, ctx, app, tenantA, 5, "after-restart")
	clientA.waitFresh(t, ctx, app)
}

func startWatchd(t *testing.T, ctx context.Context, cfg daemon.Config, address string) func() {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- daemon.Serve(serveCtx, cfg, listener, nil) }()
	stopped := false
	return func() {
		t.Helper()
		if stopped {
			return
		}
		stopped = true
		stop()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("watchd stopped with error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("watchd did not shut down")
		}
	}
}

// syncedClient is one SDK client keeping a tenant in sync.
type syncedClient struct {
	tenant string
	store  *watchd.MemoryStore

	mu      sync.Mutex
	state   watchd.State
	resyncs int
	changed chan struct{}
}

func startClient(t *testing.T, ctx context.Context, address, tenant string) *syncedClient {
	t.Helper()
	client, err := watchd.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	c := &syncedClient{tenant: tenant, store: watchd.NewMemoryStore("tenant_id", "user_id"), changed: make(chan struct{}, 1)}
	syncCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- client.Sync(syncCtx, watchd.SyncConfig{
			Projection: "tenant_permissions",
			Scope:      tenant,
			Store:      c.store,
			MinBackoff: 20 * time.Millisecond,
			MaxBackoff: 200 * time.Millisecond,
			OnState:    c.onState,
		})
	}()
	t.Cleanup(func() {
		stop()
		if err := <-done; err != nil {
			t.Errorf("Sync %s: %v", tenant, err)
		}
		_ = client.Close()
	})
	return c
}

func (c *syncedClient) onState(state watchd.State) {
	c.mu.Lock()
	// A snapshot starts with an empty cursor: count how often the client
	// rebuilt after having been fresh.
	if !state.Fresh && state.Cursor == "" && c.state.Cursor != "" {
		c.resyncs++
	}
	c.state = state
	c.mu.Unlock()
	select {
	case c.changed <- struct{}{}:
	default:
	}
}

func (c *syncedClient) sawResync() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.resyncs > 0
}

// waitFresh waits until the client is fresh and its projection equals
// PostgreSQL - the freshness contract, checked against the source.
func (c *syncedClient) waitFresh(t *testing.T, ctx context.Context, app *pgx.Conn) {
	t.Helper()
	deadline := time.After(20 * time.Second)
	for {
		c.mu.Lock()
		fresh := c.state.Fresh
		c.mu.Unlock()
		want := readPermissions(t, ctx, app, c.tenant)
		got := c.permissions()
		if fresh && maps.Equal(got, want) {
			return
		}
		select {
		case <-c.changed:
		case <-time.After(50 * time.Millisecond):
		case <-deadline:
			t.Fatalf("tenant %s: fresh=%t projection=%v, PostgreSQL=%v", c.tenant, fresh, got, want)
		}
	}
}

func (c *syncedClient) waitStale(t *testing.T, ctx context.Context) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		c.mu.Lock()
		fresh := c.state.Fresh
		c.mu.Unlock()
		if !fresh {
			return
		}
		select {
		case <-c.changed:
		case <-deadline:
			t.Fatalf("tenant %s stayed fresh after watchd stopped", c.tenant)
		}
	}
}

func (c *syncedClient) permissions() map[string]string {
	permissions := map[string]string{}
	for _, row := range c.store.Rows() {
		permissions[row["user_id"].Text] = row["permissions"].Text
	}
	return permissions
}

func e2eUser(n int) string {
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", 700+n)
}

func setPermission(t *testing.T, ctx context.Context, app *pgx.Conn, tenant string, user int, role string) {
	t.Helper()
	if _, err := app.Exec(ctx, `
		INSERT INTO tenant_permissions_projection (tenant_id, user_id, permissions)
		VALUES ($1, $2, jsonb_build_object('role', $3::text))
		ON CONFLICT (tenant_id, user_id) DO UPDATE SET permissions = EXCLUDED.permissions, version = tenant_permissions_projection.version + 1`,
		tenant, e2eUser(user), role); err != nil {
		t.Fatalf("set permission: %v", err)
	}
}

func deletePermission(t *testing.T, ctx context.Context, app *pgx.Conn, tenant string, user int) {
	t.Helper()
	if _, err := app.Exec(ctx, "DELETE FROM tenant_permissions_projection WHERE tenant_id = $1 AND user_id = $2", tenant, e2eUser(user)); err != nil {
		t.Fatalf("delete permission: %v", err)
	}
}

func readPermissions(t *testing.T, ctx context.Context, app *pgx.Conn, tenant string) map[string]string {
	t.Helper()
	rows, err := app.Query(ctx, "SELECT user_id::text, permissions::text FROM tenant_permissions_projection WHERE tenant_id = $1", tenant)
	if err != nil {
		t.Fatalf("read permissions: %v", err)
	}
	defer rows.Close()
	permissions := map[string]string{}
	for rows.Next() {
		var user, value string
		if err := rows.Scan(&user, &value); err != nil {
			t.Fatalf("scan permissions: %v", err)
		}
		permissions[user] = value
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate permissions: %v", err)
	}
	return permissions
}

func stringsReader(text string) *strings.Reader {
	return strings.NewReader(text)
}
