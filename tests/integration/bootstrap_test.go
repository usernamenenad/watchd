//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/nenad/watchd/internal/cdc"
)

const (
	defaultDatabaseURL    = "postgres://watchd_app:watchd_app@127.0.0.1:54329/watchd"
	defaultReplicationURL = "postgres://watchd_replicator:watchd_replicator@127.0.0.1:54329/watchd?replication=database"
	publicationName       = "watchd_publication"
)

func TestBootstrapSnapshotThenStreamsLaterChange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dbURL := envOrDefault("WATCHD_TEST_DATABASE_URL", defaultDatabaseURL)
	replicationURL := envOrDefault("WATCHD_TEST_REPLICATION_URL", defaultReplicationURL)

	app, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect application database: %v", err)
	}
	defer app.Close(ctx)

	if _, err := app.Exec(ctx, "DELETE FROM tenant_permissions_projection"); err != nil {
		t.Fatalf("clear sample projection: %v", err)
	}

	seedTenant := "00000000-0000-0000-0000-000000000001"
	seedUser := "00000000-0000-0000-0000-000000000101"
	if _, err := app.Exec(ctx, `
		INSERT INTO tenant_permissions_projection (tenant_id, user_id, permissions)
		VALUES ($1, $2, '{"role":"editor"}')`, seedTenant, seedUser); err != nil {
		t.Fatalf("insert snapshot row: %v", err)
	}

	replicationConn, err := pgconn.Connect(ctx, replicationURL)
	if err != nil {
		t.Fatalf("connect replication database: %v", err)
	}
	defer replicationConn.Close(ctx)

	slotName := fmt.Sprintf("watchd_bootstrap_%d", time.Now().UnixNano())
	slot, err := pglogrepl.CreateReplicationSlot(ctx, replicationConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Temporary:      true,
		SnapshotAction: "EXPORT_SNAPSHOT",
	})
	if err != nil {
		t.Fatalf("create replication slot: %v", err)
	}

	snapshotRows := readExportedSnapshot(t, ctx, dbURL, slot.SnapshotName, seedTenant)
	if len(snapshotRows) != 1 || snapshotRows[0] != seedUser {
		t.Fatalf("snapshot = %v, want [%s]", snapshotRows, seedUser)
	}

	startLSN, err := pglogrepl.ParseLSN(slot.ConsistentPoint)
	if err != nil {
		t.Fatalf("parse consistent point %q: %v", slot.ConsistentPoint, err)
	}
	err = pglogrepl.StartReplication(ctx, replicationConn, slotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			"publication_names '" + publicationName + "'",
		},
	})
	if err != nil {
		t.Fatalf("start replication: %v", err)
	}

	streamedTenant := "00000000-0000-0000-0000-000000000002"
	streamedUser := "00000000-0000-0000-0000-000000000202"
	if _, err := app.Exec(ctx, `
		INSERT INTO tenant_permissions_projection (tenant_id, user_id, permissions)
		VALUES ($1, $2, '{"role":"viewer"}')`, streamedTenant, streamedUser); err != nil {
		t.Fatalf("insert streamed row: %v", err)
	}

	if !receiveInsert(ctx, replicationConn, streamedTenant, streamedUser) {
		t.Fatalf("did not receive streamed row %s/%s", streamedTenant, streamedUser)
	}
}

func TestReaderBootstrapReturnsScopedSnapshotAndRetainsStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	app := connectApplication(t, ctx)
	defer app.Close(ctx)
	clearProjection(t, ctx, app)

	tenantID := "00000000-0000-0000-0000-000000000001"
	if _, err := app.Exec(ctx, `
		INSERT INTO tenant_permissions_projection (tenant_id, user_id, permissions)
		VALUES
			($1, '00000000-0000-0000-0000-000000000101', '{"role":"editor"}'),
			('00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000202', '{"role":"viewer"}')`, tenantID); err != nil {
		t.Fatalf("seed projection rows: %v", err)
	}

	slotName := fmt.Sprintf("watchd_bootstrap_reader_%d", time.Now().UnixNano())
	registerSlotCleanup(t, slotName)
	transactions := make(chan cdc.Transaction, 1)
	reader := newIntegrationReader(t, slotName, func(_ context.Context, transaction cdc.Transaction) error {
		transactions <- transaction
		return nil
	})

	snapshot, err := reader.Bootstrap(ctx, cdc.ProjectionSpec{
		SourceID:    "test-postgres",
		Schema:      "public",
		Table:       "tenant_permissions_projection",
		ScopeColumn: "tenant_id",
		PrimaryKey:  []string{"tenant_id", "user_id"},
	}, cdc.Scope{Value: tenantID})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if snapshot.SourceID != "test-postgres" || snapshot.Cursor == "" {
		t.Fatalf("snapshot metadata = %#v, want source identity and cursor", snapshot)
	}
	if got, want := len(snapshot.Rows), 1; got != want {
		t.Fatalf("snapshot rows = %d, want %d scoped row", got, want)
	}

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- reader.Run(runCtx) }()
	defer func() {
		stop()
		if err := <-done; err != nil {
			t.Errorf("reader run: %v", err)
		}
	}()

	streamedUserID := "00000000-0000-0000-0000-000000000103"
	if _, err := app.Exec(ctx, `
		INSERT INTO tenant_permissions_projection (tenant_id, user_id, permissions)
		VALUES ($1, $2, '{"role":"admin"}')`, tenantID, streamedUserID); err != nil {
		t.Fatalf("insert streamed projection row: %v", err)
	}

	select {
	case transaction := <-transactions:
		if got, want := len(transaction.Changes), 1; got != want {
			t.Fatalf("streamed changes = %d, want %d", got, want)
		}
		if transaction.Changes[0].Operation != cdc.OperationInsert {
			t.Fatalf("streamed operation = %q, want insert", transaction.Changes[0].Operation)
		}
	case err := <-done:
		t.Fatalf("reader stopped before streaming after bootstrap: %v", err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for post-snapshot transaction")
	}
}

func TestReaderBootstrapRejectsMissingScopeColumnWithoutCreatingSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	app := connectApplication(t, ctx)
	defer app.Close(ctx)

	slotName := fmt.Sprintf("watchd_bootstrap_bad_scope_%d", time.Now().UnixNano())
	reader := newIntegrationReader(t, slotName, func(context.Context, cdc.Transaction) error { return nil })
	_, err := reader.Bootstrap(ctx, cdc.ProjectionSpec{
		SourceID:    "test-postgres",
		Schema:      "public",
		Table:       "tenant_permissions_projection",
		ScopeColumn: "missing_scope_column",
		PrimaryKey:  []string{"tenant_id", "user_id"},
	}, cdc.Scope{Value: "irrelevant"})
	if !errors.Is(err, cdc.ErrInvalidReaderConfig) {
		t.Fatalf("bootstrap error = %v, want invalid projection configuration", err)
	}
	if slotExists(t, ctx, app, slotName) {
		t.Fatalf("bootstrap created slot %q for invalid projection", slotName)
	}
}

func TestReaderBootstrapCleansUpSlotAfterSnapshotQueryFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	app := connectApplication(t, ctx)
	defer app.Close(ctx)

	slotName := fmt.Sprintf("watchd_bootstrap_query_failure_%d", time.Now().UnixNano())
	reader := newIntegrationReader(t, slotName, func(context.Context, cdc.Transaction) error { return nil })
	_, err := reader.Bootstrap(ctx, cdc.ProjectionSpec{
		SourceID:    "test-postgres",
		Schema:      "public",
		Table:       "tenant_permissions_projection",
		ScopeColumn: "tenant_id",
		PrimaryKey:  []string{"tenant_id", "user_id"},
	}, cdc.Scope{Value: "not-a-uuid"})
	if err == nil {
		t.Fatal("bootstrap succeeded with an invalid UUID scope")
	}
	if slotExists(t, ctx, app, slotName) {
		t.Fatalf("bootstrap leaked slot %q after snapshot query failure", slotName)
	}
}

func TestReaderBootstrapClassifiesInsufficientReplicationPrivilege(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	slotName := fmt.Sprintf("watchd_bootstrap_permission_%d", time.Now().UnixNano())
	reader, err := cdc.NewReader(cdc.ReaderConfig{
		DatabaseURL:     envOrDefault("WATCHD_TEST_DATABASE_URL", defaultDatabaseURL),
		SlotName:        slotName,
		PublicationName: publicationName,
	}, func(context.Context, cdc.Transaction) error { return nil })
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}

	_, err = reader.Bootstrap(ctx, integrationProjectionSpec(), cdc.Scope{Value: "00000000-0000-0000-0000-000000000001"})
	if !errors.Is(err, cdc.ErrInsufficientPrivileges) {
		t.Fatalf("bootstrap error = %v, want insufficient privileges", err)
	}
}

func TestReaderBootstrapRejectsInvalidPublication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	slotName := fmt.Sprintf("watchd_bootstrap_bad_publication_%d", time.Now().UnixNano())
	reader, err := cdc.NewReader(cdc.ReaderConfig{
		DatabaseURL:     envOrDefault("WATCHD_TEST_REPLICATION_URL", defaultReplicationURL),
		SlotName:        slotName,
		PublicationName: "missing_publication",
	}, func(context.Context, cdc.Transaction) error { return nil })
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}

	_, err = reader.Bootstrap(ctx, integrationProjectionSpec(), cdc.Scope{Value: "00000000-0000-0000-0000-000000000001"})
	if !errors.Is(err, cdc.ErrInvalidReaderConfig) {
		t.Fatalf("bootstrap error = %v, want invalid configuration", err)
	}
}

func integrationProjectionSpec() cdc.ProjectionSpec {
	return cdc.ProjectionSpec{
		SourceID:    "test-postgres",
		Schema:      "public",
		Table:       "tenant_permissions_projection",
		ScopeColumn: "tenant_id",
		PrimaryKey:  []string{"tenant_id", "user_id"},
	}
}

func readExportedSnapshot(t *testing.T, ctx context.Context, databaseURL, snapshotName, tenantID string) []string {
	t.Helper()

	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect snapshot database: %v", err)
	}
	defer conn.Close(ctx)

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatalf("begin snapshot transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT '"+snapshotName+"'"); err != nil {
		t.Fatalf("use exported snapshot: %v", err)
	}

	rows, err := tx.Query(ctx, `
		SELECT user_id::text
		FROM tenant_permissions_projection
		WHERE tenant_id = $1
		ORDER BY user_id`, tenantID)
	if err != nil {
		t.Fatalf("read projection snapshot: %v", err)
	}
	defer rows.Close()

	var userIDs []string
	for rows.Next() {
		var userID string
		if err := rows.Scan(&userID); err != nil {
			t.Fatalf("scan snapshot row: %v", err)
		}
		userIDs = append(userIDs, userID)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate snapshot rows: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit snapshot transaction: %v", err)
	}
	return userIDs
}

func receiveInsert(ctx context.Context, conn *pgconn.PgConn, tenantID, userID string) bool {
	relations := make(map[uint32]*pglogrepl.RelationMessage)
	for {
		raw, err := conn.ReceiveMessage(ctx)
		if err != nil {
			return false
		}
		copyData, ok := raw.(*pgproto3.CopyData)
		if !ok || len(copyData.Data) == 0 || copyData.Data[0] != pglogrepl.XLogDataByteID {
			continue
		}

		xlog, err := pglogrepl.ParseXLogData(copyData.Data[1:])
		if err != nil {
			return false
		}
		message, err := pglogrepl.Parse(xlog.WALData)
		if err != nil {
			return false
		}

		switch message := message.(type) {
		case *pglogrepl.RelationMessage:
			relations[message.RelationID] = message
		case *pglogrepl.InsertMessage:
			relation := relations[message.RelationID]
			if relation == nil || relation.RelationName != "tenant_permissions_projection" {
				continue
			}
			if tupleHasValues(relation, message.Tuple, tenantID, userID) {
				return true
			}
		}
	}
}

func tupleHasValues(relation *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData, tenantID, userID string) bool {
	values := make(map[string]string, len(tuple.Columns))
	for index, column := range tuple.Columns {
		if column.DataType != pglogrepl.TupleDataTypeText {
			continue
		}
		values[relation.Columns[index].Name] = string(column.Data)
	}
	return strings.EqualFold(values["tenant_id"], tenantID) && strings.EqualFold(values["user_id"], userID)
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
