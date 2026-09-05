//go:build integration

package cdc

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	testDatabaseURL    = "postgres://watchd_app:watchd_app@127.0.0.1:54329/watchd"
	testReplicationURL = "postgres://watchd_replicator:watchd_replicator@127.0.0.1:54329/watchd?replication=database"
	testAdminURL       = "postgres://postgres:postgres@127.0.0.1:54329/watchd"
	testPublication    = "watchd_publication"
)

func TestBootstrapIsGapFreeAcrossConcurrentWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	app := connectBootstrapTest(t, ctx, testDatabaseURL)
	defer app.Close(ctx)
	if _, err := app.Exec(ctx, "DELETE FROM tenant_permissions_projection"); err != nil {
		t.Fatalf("clear projection: %v", err)
	}

	tenantID := "00000000-0000-0000-0000-000000000010"
	beforeUser := "00000000-0000-0000-0000-000000000101"
	deletedDuringUser := "00000000-0000-0000-0000-000000000102"
	duringUser := "00000000-0000-0000-0000-000000000103"
	afterUser := "00000000-0000-0000-0000-000000000104"
	if _, err := app.Exec(ctx, `
		INSERT INTO tenant_permissions_projection (tenant_id, user_id, permissions)
		VALUES
			($1, $2, '{"role":"editor"}'),
			($1, $3, '{"role":"temporary"}'),
			('00000000-0000-0000-0000-000000000099', '00000000-0000-0000-0000-000000000999', '{"role":"other-tenant"}')`,
		tenantID, beforeUser, deletedDuringUser); err != nil {
		t.Fatalf("seed projection before bootstrap: %v", err)
	}

	slotName := fmt.Sprintf("watchd_gap_free_%d", time.Now().UnixNano())
	transactions := make(chan Transaction, 8)
	reader := newBootstrapTestReader(t, slotName, func(_ context.Context, transaction Transaction) error {
		transactions <- transaction
		return nil
	})
	registerBootstrapCleanup(t, reader, slotName)

	snapshotReady := make(chan struct{})
	continueSnapshot := make(chan struct{})
	reader.beforeSnapshotRead = func(ctx context.Context) error {
		close(snapshotReady)
		select {
		case <-continueSnapshot:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	type bootstrapResult struct {
		snapshot Snapshot
		err      error
	}
	bootstrapDone := make(chan bootstrapResult, 1)
	go func() {
		snapshot, err := reader.Bootstrap(ctx, bootstrapTestProjection(), Scope{Value: tenantID})
		bootstrapDone <- bootstrapResult{snapshot: snapshot, err: err}
	}()

	select {
	case <-snapshotReady:
	case <-ctx.Done():
		t.Fatal("timed out waiting for imported snapshot")
	}
	if err := writeDuringBootstrapTest(ctx, app, tenantID, beforeUser, deletedDuringUser, duringUser); err != nil {
		t.Fatalf("write during snapshot: %v", err)
	}
	close(continueSnapshot)

	bootstrap := <-bootstrapDone
	if bootstrap.err != nil {
		t.Fatalf("bootstrap: %v", bootstrap.err)
	}
	projection := projectionFromBootstrapSnapshot(t, bootstrap.snapshot, tenantID)

	runCtx, stop := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- reader.Run(runCtx) }()
	defer func() {
		stop()
		if err := <-runDone; err != nil {
			t.Errorf("reader run: %v", err)
		}
	}()

	if err := writeAfterBootstrapTest(ctx, app, tenantID, beforeUser, duringUser, afterUser); err != nil {
		t.Fatalf("write after snapshot: %v", err)
	}
	for !projectionHasUser(projection, afterUser) {
		select {
		case transaction := <-transactions:
			applyBootstrapTransaction(t, projection, transaction, tenantID)
		case err := <-runDone:
			t.Fatalf("reader stopped before sentinel change: %v", err)
		case <-ctx.Done():
			t.Fatal("timed out waiting for gap-free projection")
		}
	}

	want := readBootstrapProjection(t, ctx, app, tenantID)
	if !reflect.DeepEqual(projection, want) {
		t.Fatalf("snapshot plus stream = %v, PostgreSQL = %v", projection, want)
	}
}

func TestBootstrapCancellationCleansUpSlotAndTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	slotName := fmt.Sprintf("watchd_bootstrap_cancel_%d", time.Now().UnixNano())
	reader := newBootstrapTestReader(t, slotName, func(context.Context, Transaction) error { return nil })
	registerBootstrapCleanup(t, reader, slotName)

	snapshotReady := make(chan struct{})
	reader.beforeSnapshotRead = func(ctx context.Context) error {
		close(snapshotReady)
		<-ctx.Done()
		return ctx.Err()
	}
	bootstrapCtx, cancelBootstrap := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() {
		_, err := reader.Bootstrap(bootstrapCtx, bootstrapTestProjection(), Scope{Value: "00000000-0000-0000-0000-000000000001"})
		result <- err
	}()

	select {
	case <-snapshotReady:
	case <-ctx.Done():
		t.Fatal("timed out waiting for imported snapshot")
	}
	cancelBootstrap()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled bootstrap error = %v, want context cancellation", err)
	}
	if bootstrapSlotExists(t, ctx, slotName) {
		t.Fatalf("cancelled bootstrap leaked slot %q", slotName)
	}
	if idle := idleBootstrapTransactions(t, ctx); idle != 0 {
		t.Fatalf("cancelled bootstrap left %d idle transaction(s)", idle)
	}
}

func TestExpiredExportedSnapshotHasTypedError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	replication, err := pgconn.Connect(ctx, testReplicationURL)
	if err != nil {
		t.Fatalf("connect replication source: %v", err)
	}
	slotName := fmt.Sprintf("watchd_expired_snapshot_%d", time.Now().UnixNano())
	created, err := pglogrepl.CreateReplicationSlot(ctx, replication, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Temporary:      true,
		SnapshotAction: "EXPORT_SNAPSHOT",
	})
	if err != nil {
		t.Fatalf("create temporary exported snapshot: %v", err)
	}
	if err := replication.Close(ctx); err != nil {
		t.Fatalf("expire exported snapshot: %v", err)
	}

	management := connectBootstrapTest(t, ctx, testReplicationURL)
	defer management.Close(ctx)
	_, err = readSnapshot(
		ctx,
		management,
		bootstrapTestProjection(),
		Scope{Value: "00000000-0000-0000-0000-000000000001"},
		created.SnapshotName,
		time.Second,
		nil,
	)
	if !errors.Is(err, ErrSnapshotExpired) {
		t.Fatalf("expired snapshot error = %v, want %v", err, ErrSnapshotExpired)
	}
}

type bootstrapProjectionRow struct {
	permissions string
	version     string
}

type bootstrapProjection map[string]bootstrapProjectionRow

func newBootstrapTestReader(t *testing.T, slotName string, sink TransactionSink) *Reader {
	t.Helper()
	reader, err := NewReader(ReaderConfig{
		DatabaseURL:     testReplicationURL,
		SlotName:        slotName,
		PublicationName: testPublication,
		StatusInterval:  100 * time.Millisecond,
		ShutdownTimeout: time.Second,
	}, sink)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	return reader
}

func bootstrapTestProjection() ProjectionSpec {
	return ProjectionSpec{
		SourceID:    "test-postgres",
		Schema:      "public",
		Table:       "tenant_permissions_projection",
		ScopeColumn: "tenant_id",
		PrimaryKey:  []string{"tenant_id", "user_id"},
	}
}

func connectBootstrapTest(t *testing.T, ctx context.Context, databaseURL string) *pgx.Conn {
	t.Helper()
	config, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse PostgreSQL URL: %v", err)
	}
	delete(config.RuntimeParams, "replication")
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatalf("connect PostgreSQL: %v", err)
	}
	return conn
}

func registerBootstrapCleanup(t *testing.T, reader *Reader, slotName string) {
	t.Helper()
	t.Cleanup(func() {
		if stream, _, ok := reader.takeBootstrapStream(); ok {
			reader.closeReplicationConnection(stream)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		admin := connectBootstrapTest(t, ctx, testAdminURL)
		defer admin.Close(ctx)
		_, err := admin.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slotName)
		if err == nil {
			return
		}
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.SQLState() == "42704" {
			return
		}
		t.Fatalf("drop bootstrap slot %q: %v", slotName, err)
	})
}

func bootstrapSlotExists(t *testing.T, ctx context.Context, slotName string) bool {
	t.Helper()
	admin := connectBootstrapTest(t, ctx, testAdminURL)
	defer admin.Close(ctx)
	var exists bool
	if err := admin.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slotName).Scan(&exists); err != nil {
		t.Fatalf("inspect bootstrap slot: %v", err)
	}
	return exists
}

func idleBootstrapTransactions(t *testing.T, ctx context.Context) int {
	t.Helper()
	admin := connectBootstrapTest(t, ctx, testAdminURL)
	defer admin.Close(ctx)
	var count int
	if err := admin.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_stat_activity
		WHERE datname = current_database()
		  AND usename = 'watchd_replicator'
		  AND state = 'idle in transaction'`).Scan(&count); err != nil {
		t.Fatalf("inspect bootstrap transactions: %v", err)
	}
	return count
}

func writeDuringBootstrapTest(ctx context.Context, conn *pgx.Conn, tenantID, updatedUser, deletedUser, insertedUser string) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `UPDATE tenant_permissions_projection SET permissions = '{"role":"admin"}', version = 2 WHERE tenant_id = $1 AND user_id = $2`, tenantID, updatedUser); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM tenant_permissions_projection WHERE tenant_id = $1 AND user_id = $2`, tenantID, deletedUser); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO tenant_permissions_projection (tenant_id, user_id, permissions) VALUES ($1, $2, '{"role":"viewer"}')`, tenantID, insertedUser); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func writeAfterBootstrapTest(ctx context.Context, conn *pgx.Conn, tenantID, deletedUser, updatedUser, insertedUser string) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM tenant_permissions_projection WHERE tenant_id = $1 AND user_id = $2`, tenantID, deletedUser); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE tenant_permissions_projection SET permissions = '{"role":"owner"}', version = 2 WHERE tenant_id = $1 AND user_id = $2`, tenantID, updatedUser); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO tenant_permissions_projection (tenant_id, user_id, permissions) VALUES ($1, $2, '{"role":"sentinel"}')`, tenantID, insertedUser); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func projectionFromBootstrapSnapshot(t *testing.T, snapshot Snapshot, tenantID string) bootstrapProjection {
	t.Helper()
	state := make(bootstrapProjection, len(snapshot.Rows))
	for _, row := range snapshot.Rows {
		if bootstrapString(t, row["tenant_id"]) != tenantID {
			t.Fatalf("snapshot included row outside scope: %v", row)
		}
		state[bootstrapString(t, row["user_id"])] = bootstrapProjectionRow{
			permissions: bootstrapString(t, row["permissions"]),
			version:     bootstrapString(t, row["version"]),
		}
	}
	return state
}

func applyBootstrapTransaction(t *testing.T, state bootstrapProjection, transaction Transaction, tenantID string) {
	t.Helper()
	for _, change := range transaction.Changes {
		if change.Table != "public.tenant_permissions_projection" || change.Key["tenant_id"] != tenantID {
			continue
		}
		userID := change.Key["user_id"]
		switch change.Operation {
		case OperationDelete:
			delete(state, userID)
		case OperationInsert, OperationUpdate:
			state[userID] = bootstrapProjectionRow{
				permissions: bootstrapString(t, change.Values["permissions"]),
				version:     bootstrapString(t, change.Values["version"]),
			}
		default:
			t.Fatalf("unknown projection operation %q", change.Operation)
		}
	}
}

func projectionHasUser(state bootstrapProjection, userID string) bool {
	_, found := state[userID]
	return found
}

func readBootstrapProjection(t *testing.T, ctx context.Context, conn *pgx.Conn, tenantID string) bootstrapProjection {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT user_id::text, permissions::text, version::text FROM tenant_permissions_projection WHERE tenant_id = $1`, tenantID)
	if err != nil {
		t.Fatalf("read authoritative projection: %v", err)
	}
	defer rows.Close()
	state := make(bootstrapProjection)
	for rows.Next() {
		var userID string
		var row bootstrapProjectionRow
		if err := rows.Scan(&userID, &row.permissions, &row.version); err != nil {
			t.Fatalf("scan authoritative projection: %v", err)
		}
		state[userID] = row
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate authoritative projection: %v", err)
	}
	return state
}

func bootstrapString(t *testing.T, value any) string {
	t.Helper()
	text, ok := value.(string)
	if !ok {
		t.Fatalf("projection value %v has type %T, want string", value, value)
	}
	return text
}
