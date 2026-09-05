//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/nenad/watchd/internal/cdc"
)

const defaultAdminURL = "postgres://postgres:postgres@127.0.0.1:54329/watchd"

func TestReaderEmitsAtomicTransactionAndAcknowledges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	app := connectApplication(t, ctx)
	defer app.Close(ctx)
	clearProjection(t, ctx, app)

	slotName := fmt.Sprintf("watchd_reader_%d", time.Now().UnixNano())
	registerSlotCleanup(t, slotName)
	transactions := make(chan cdc.Transaction, 1)
	reader := newIntegrationReader(t, slotName, func(_ context.Context, transaction cdc.Transaction) error {
		transactions <- transaction
		return nil
	})
	bootstrapReader(t, ctx, reader)

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- reader.Run(runCtx) }()
	defer stop()

	waitForSlot(t, ctx, app, slotName)

	tx, err := app.Begin(ctx)
	if err != nil {
		t.Fatalf("begin write transaction: %v", err)
	}
	for _, row := range []struct {
		tenantID string
		userID   string
		role     string
	}{
		{"00000000-0000-0000-0000-000000000011", "00000000-0000-0000-0000-000000000111", "editor"},
		{"00000000-0000-0000-0000-000000000012", "00000000-0000-0000-0000-000000000112", "viewer"},
	} {
		if _, err := tx.Exec(ctx, `
			INSERT INTO tenant_permissions_projection (tenant_id, user_id, permissions)
			VALUES ($1, $2, jsonb_build_object('role', $3::text))`, row.tenantID, row.userID, row.role); err != nil {
			t.Fatalf("insert projection row: %v", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit write transaction: %v", err)
	}

	var transaction cdc.Transaction
	select {
	case transaction = <-transactions:
	case err := <-done:
		t.Fatalf("reader stopped before emitting a transaction: %v", err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for committed transaction")
	}

	if got, want := len(transaction.Changes), 2; got != want {
		t.Fatalf("changes = %d, want %d", got, want)
	}
	for index, change := range transaction.Changes {
		if change.Operation != cdc.OperationInsert {
			t.Fatalf("change %d operation = %q, want insert", index, change.Operation)
		}
	}
	if transaction.Cursor == "" {
		t.Fatal("transaction cursor is empty")
	}

	waitForAcknowledgement(t, ctx, app, slotName)
	stats := reader.Stats()
	if stats.TransactionsAccepted != 1 || stats.LastAcknowledgedLSN == "" {
		t.Fatalf("reader stats = %#v, want one acknowledged transaction", stats)
	}

	stop()
	if err := <-done; err != nil {
		t.Fatalf("reader shutdown: %v", err)
	}
	dropSlot(t, ctx, slotName)
}

func TestReaderReconnectsAfterReplicationBackendTerminates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	app := connectApplication(t, ctx)
	defer app.Close(ctx)
	clearProjection(t, ctx, app)

	slotName := fmt.Sprintf("watchd_reconnect_%d", time.Now().UnixNano())
	registerSlotCleanup(t, slotName)
	transactions := make(chan cdc.Transaction, 1)
	reader := newIntegrationReader(t, slotName, func(_ context.Context, transaction cdc.Transaction) error {
		transactions <- transaction
		return nil
	})
	bootstrapReader(t, ctx, reader)

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- reader.Run(runCtx) }()
	defer stop()

	waitForSlot(t, ctx, app, slotName)
	backendPID := waitForActiveSlotPID(t, ctx, app, slotName)
	terminateBackend(t, ctx, backendPID)

	// A new active PID proves that Run created a fresh replication connection.
	waitForNewActiveSlotPID(t, ctx, app, slotName, backendPID)
	if _, err := app.Exec(ctx, `
		INSERT INTO tenant_permissions_projection (tenant_id, user_id, permissions)
		VALUES ('00000000-0000-0000-0000-000000000021', '00000000-0000-0000-0000-000000000121', '{"role":"admin"}')`); err != nil {
		t.Fatalf("insert after reconnect: %v", err)
	}

	select {
	case transaction := <-transactions:
		if got, want := len(transaction.Changes), 1; got != want {
			t.Fatalf("changes = %d, want %d", got, want)
		}
	case err := <-done:
		t.Fatalf("reader stopped instead of reconnecting: %v", err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for transaction after reconnect")
	}

	if got := reader.Stats().ReconnectAttempts; got == 0 {
		t.Fatal("reader did not record a reconnect attempt")
	}

	stop()
	if err := <-done; err != nil {
		t.Fatalf("reader shutdown: %v", err)
	}
	dropSlot(t, ctx, slotName)
}

func TestReaderEmitsUpdateAndDeleteInOneTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	app := connectApplication(t, ctx)
	defer app.Close(ctx)
	clearProjection(t, ctx, app)

	if _, err := app.Exec(ctx, `
		INSERT INTO tenant_permissions_projection (tenant_id, user_id, permissions)
		VALUES
			('00000000-0000-0000-0000-000000000031', '00000000-0000-0000-0000-000000000131', '{"role":"editor"}'),
			('00000000-0000-0000-0000-000000000032', '00000000-0000-0000-0000-000000000132', '{"role":"viewer"}')`); err != nil {
		t.Fatalf("seed projection rows: %v", err)
	}

	slotName := fmt.Sprintf("watchd_mutations_%d", time.Now().UnixNano())
	registerSlotCleanup(t, slotName)
	transactions := make(chan cdc.Transaction, 1)
	reader := newIntegrationReader(t, slotName, func(_ context.Context, transaction cdc.Transaction) error {
		transactions <- transaction
		return nil
	})
	bootstrapReader(t, ctx, reader)

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- reader.Run(runCtx) }()
	defer stop()

	waitForSlot(t, ctx, app, slotName)
	tx, err := app.Begin(ctx)
	if err != nil {
		t.Fatalf("begin mutation transaction: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tenant_permissions_projection
		SET permissions = '{"role":"admin"}'
		WHERE tenant_id = '00000000-0000-0000-0000-000000000031'
		  AND user_id = '00000000-0000-0000-0000-000000000131'`); err != nil {
		t.Fatalf("update projection row: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM tenant_permissions_projection
		WHERE tenant_id = '00000000-0000-0000-0000-000000000032'
		  AND user_id = '00000000-0000-0000-0000-000000000132'`); err != nil {
		t.Fatalf("delete projection row: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit mutation transaction: %v", err)
	}

	select {
	case transaction := <-transactions:
		if got, want := len(transaction.Changes), 2; got != want {
			t.Fatalf("changes = %d, want %d", got, want)
		}
		if got, want := transaction.Changes[0].Operation, cdc.OperationUpdate; got != want {
			t.Fatalf("first operation = %q, want %q", got, want)
		}
		if got, want := transaction.Changes[1].Operation, cdc.OperationDelete; got != want {
			t.Fatalf("second operation = %q, want %q", got, want)
		}
	case err := <-done:
		t.Fatalf("reader stopped before emitting update/delete transaction: %v", err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for update/delete transaction")
	}

	stop()
	if err := <-done; err != nil {
		t.Fatalf("reader shutdown: %v", err)
	}
	dropSlot(t, ctx, slotName)
}

func TestReaderNeverEmitsAbortedTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	app := connectApplication(t, ctx)
	defer app.Close(ctx)
	clearProjection(t, ctx, app)

	slotName := fmt.Sprintf("watchd_abort_%d", time.Now().UnixNano())
	registerSlotCleanup(t, slotName)
	transactions := make(chan cdc.Transaction, 1)
	reader := newIntegrationReader(t, slotName, func(_ context.Context, transaction cdc.Transaction) error {
		transactions <- transaction
		return nil
	})
	bootstrapReader(t, ctx, reader)

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- reader.Run(runCtx) }()
	defer stop()

	waitForSlot(t, ctx, app, slotName)
	tx, err := app.Begin(ctx)
	if err != nil {
		t.Fatalf("begin aborted transaction: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO tenant_permissions_projection (tenant_id, user_id, permissions)
		VALUES ('00000000-0000-0000-0000-000000000041', '00000000-0000-0000-0000-000000000141', '{"role":"editor"}')`); err != nil {
		t.Fatalf("insert aborted row: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback transaction: %v", err)
	}

	select {
	case transaction := <-transactions:
		t.Fatalf("received aborted transaction: %#v", transaction)
	case err := <-done:
		t.Fatalf("reader stopped before cancellation: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	stop()
	if err := <-done; err != nil {
		t.Fatalf("reader shutdown: %v", err)
	}
	dropSlot(t, ctx, slotName)
}

func TestReaderDoesNotAcknowledgeRejectedTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	app := connectApplication(t, ctx)
	defer app.Close(ctx)
	clearProjection(t, ctx, app)

	slotName := fmt.Sprintf("watchd_rejected_%d", time.Now().UnixNano())
	registerSlotCleanup(t, slotName)
	sinkErr := errors.New("replay buffer is full")
	reader := newIntegrationReader(t, slotName, func(context.Context, cdc.Transaction) error {
		return sinkErr
	})
	bootstrapReader(t, ctx, reader)

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- reader.Run(runCtx) }()
	defer stop()

	waitForSlot(t, ctx, app, slotName)
	// The reader may acknowledge the slot's initial consistent point in a
	// periodic heartbeat. Establish that baseline before testing a rejected
	// transaction's position.
	waitForAcknowledgement(t, ctx, app, slotName)
	before := slotConfirmedFlushLSN(t, ctx, app, slotName)
	if _, err := app.Exec(ctx, `
		INSERT INTO tenant_permissions_projection (tenant_id, user_id, permissions)
		VALUES ('00000000-0000-0000-0000-000000000051', '00000000-0000-0000-0000-000000000151', '{"role":"viewer"}')`); err != nil {
		t.Fatalf("insert rejected row: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, cdc.ErrSinkRejected) {
			t.Fatalf("reader error = %v, want %v", err, cdc.ErrSinkRejected)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for sink rejection")
	}

	after := slotConfirmedFlushLSN(t, ctx, app, slotName)
	if after != before {
		t.Fatalf("slot confirmation advanced from %q to %q after sink rejection", before, after)
	}
	dropSlot(t, ctx, slotName)
}

func TestReaderRequiresBootstrapForNewSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	app := connectApplication(t, ctx)
	defer app.Close(ctx)

	slotName := fmt.Sprintf("watchd_missing_%d", time.Now().UnixNano())
	registerSlotCleanup(t, slotName)
	reader := newIntegrationReader(t, slotName, func(context.Context, cdc.Transaction) error { return nil })

	err := reader.Run(ctx)
	if !errors.Is(err, cdc.ErrSlotInvalidated) {
		t.Fatalf("Run error = %v, want %v", err, cdc.ErrSlotInvalidated)
	}
	if slotExists(t, ctx, app, slotName) {
		t.Fatalf("reader created missing slot %q during normal startup", slotName)
	}
}

func TestReaderDoesNotRecreateDeletedSlotDuringReconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	app := connectApplication(t, ctx)
	defer app.Close(ctx)
	clearProjection(t, ctx, app)

	slotName := fmt.Sprintf("watchd_invalidated_%d", time.Now().UnixNano())
	registerSlotCleanup(t, slotName)
	reader := newIntegrationReaderWithRetry(t, slotName, func(context.Context, cdc.Transaction) error { return nil }, cdc.RetryPolicy{
		InitialBackoff: 750 * time.Millisecond,
		MaxBackoff:     750 * time.Millisecond,
		MaxAttempts:    2,
		Jitter:         0.1,
	})
	bootstrapReader(t, ctx, reader)

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- reader.Run(runCtx) }()
	defer stop()

	backendPID := waitForActiveSlotPID(t, ctx, app, slotName)
	terminateBackend(t, ctx, backendPID)
	waitForInactiveSlot(t, ctx, app, slotName)
	dropSlot(t, ctx, slotName)

	select {
	case err := <-done:
		if !errors.Is(err, cdc.ErrSlotInvalidated) {
			t.Fatalf("Run error = %v, want %v", err, cdc.ErrSlotInvalidated)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for slot invalidation")
	}
	if slotExists(t, ctx, app, slotName) {
		t.Fatalf("reader recreated invalidated slot %q", slotName)
	}
}

func TestReaderStopsWithinConfiguredShutdownTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	app := connectApplication(t, ctx)
	defer app.Close(ctx)

	slotName := fmt.Sprintf("watchd_shutdown_%d", time.Now().UnixNano())
	registerSlotCleanup(t, slotName)
	reader := newIntegrationReader(t, slotName, func(context.Context, cdc.Transaction) error { return nil })
	bootstrapReader(t, ctx, reader)

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- reader.Run(runCtx) }()
	waitForActiveSlotPID(t, ctx, app, slotName)

	started := time.Now()
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("reader shutdown: %v", err)
		}
		if elapsed := time.Since(started); elapsed > 2*time.Second {
			t.Fatalf("reader shutdown took %s, want less than two seconds", elapsed)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for reader shutdown")
	}
}

func newIntegrationReader(t *testing.T, slotName string, sink cdc.TransactionSink) *cdc.Reader {
	t.Helper()
	return newIntegrationReaderWithRetry(t, slotName, sink, cdc.RetryPolicy{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		MaxAttempts:    20,
		Jitter:         0.1,
	})
}

func newIntegrationReaderWithRetry(t *testing.T, slotName string, sink cdc.TransactionSink, retryPolicy cdc.RetryPolicy) *cdc.Reader {
	t.Helper()

	reader, err := cdc.NewReader(cdc.ReaderConfig{
		DatabaseURL:           envOrDefault("WATCHD_TEST_REPLICATION_URL", defaultReplicationURL),
		SlotName:              slotName,
		PublicationName:       publicationName,
		StatusInterval:        100 * time.Millisecond,
		ShutdownTimeout:       time.Second,
		MaxTransactionBytes:   1 << 20,
		MaxTransactionChanges: 100,
		RetryPolicy:           retryPolicy,
	}, sink)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return reader
}

func bootstrapReader(t *testing.T, ctx context.Context, reader *cdc.Reader) {
	t.Helper()

	snapshot, err := reader.Bootstrap(ctx, integrationProjectionSpec(), cdc.Scope{Value: "00000000-0000-0000-0000-000000000001"})
	if err != nil {
		t.Fatalf("bootstrap replication source: %v", err)
	}
	if snapshot.Cursor == "" {
		t.Fatalf("bootstrap snapshot = %#v, want a consistent cursor", snapshot)
	}
}

func connectApplication(t *testing.T, ctx context.Context) *pgx.Conn {
	t.Helper()

	app, err := pgx.Connect(ctx, envOrDefault("WATCHD_TEST_DATABASE_URL", defaultDatabaseURL))
	if err != nil {
		t.Fatalf("connect application database: %v", err)
	}
	return app
}

func clearProjection(t *testing.T, ctx context.Context, app *pgx.Conn) {
	t.Helper()
	if _, err := app.Exec(ctx, "DELETE FROM tenant_permissions_projection"); err != nil {
		t.Fatalf("clear sample projection: %v", err)
	}
}

func waitForSlot(t *testing.T, ctx context.Context, conn *pgx.Conn, slotName string) {
	t.Helper()
	for {
		if slotExists(t, ctx, conn, slotName) {
			return
		}
		waitForNextPoll(t, ctx)
	}
}

func slotExists(t *testing.T, ctx context.Context, conn *pgx.Conn, slotName string) bool {
	t.Helper()

	var exists bool
	if err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slotName).Scan(&exists); err != nil {
		t.Fatalf("query replication slot: %v", err)
	}
	return exists
}

func waitForAcknowledgement(t *testing.T, ctx context.Context, conn *pgx.Conn, slotName string) {
	t.Helper()
	for {
		var acknowledged *string
		if err := conn.QueryRow(ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&acknowledged); err != nil {
			t.Fatalf("query slot acknowledgement: %v", err)
		}
		if acknowledged != nil && *acknowledged != "" {
			return
		}
		waitForNextPoll(t, ctx)
	}
}

func slotConfirmedFlushLSN(t *testing.T, ctx context.Context, conn *pgx.Conn, slotName string) string {
	t.Helper()

	var lsn *string
	if err := conn.QueryRow(ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&lsn); err != nil {
		t.Fatalf("query slot confirmation: %v", err)
	}
	if lsn == nil {
		return ""
	}
	return *lsn
}

func waitForActiveSlotPID(t *testing.T, ctx context.Context, conn *pgx.Conn, slotName string) uint32 {
	t.Helper()
	for {
		var pid *uint32
		if err := conn.QueryRow(ctx, "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&pid); err != nil {
			t.Fatalf("query slot PID: %v", err)
		}
		if pid != nil {
			return *pid
		}
		waitForNextPoll(t, ctx)
	}
}

func waitForNewActiveSlotPID(t *testing.T, ctx context.Context, conn *pgx.Conn, slotName string, previous uint32) {
	t.Helper()
	for {
		if current := waitForActiveSlotPID(t, ctx, conn, slotName); current != previous {
			return
		}
		waitForNextPoll(t, ctx)
	}
}

func waitForInactiveSlot(t *testing.T, ctx context.Context, conn *pgx.Conn, slotName string) {
	t.Helper()
	for {
		var active bool
		if err := conn.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&active); err != nil {
			t.Fatalf("query slot activity: %v", err)
		}
		if !active {
			return
		}
		waitForNextPoll(t, ctx)
	}
}

func terminateBackend(t *testing.T, ctx context.Context, pid uint32) {
	t.Helper()

	admin, err := pgx.Connect(ctx, defaultAdminURL)
	if err != nil {
		t.Fatalf("connect PostgreSQL admin: %v", err)
	}
	defer admin.Close(ctx)

	var terminated bool
	if err := admin.QueryRow(ctx, "SELECT pg_terminate_backend($1)", pid).Scan(&terminated); err != nil {
		t.Fatalf("terminate replication backend: %v", err)
	}
	if !terminated {
		t.Fatalf("PostgreSQL did not terminate replication backend %d", pid)
	}
}

func dropSlot(t *testing.T, ctx context.Context, slotName string) {
	t.Helper()

	admin, err := pgx.Connect(ctx, defaultAdminURL)
	if err != nil {
		t.Fatalf("connect PostgreSQL admin: %v", err)
	}
	defer admin.Close(ctx)

	for {
		_, err = admin.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slotName)
		if err == nil {
			return
		}
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.SQLState() == "42704" {
			// The normal test path dropped this slot before t.Cleanup ran.
			return
		}
		if errors.As(err, &postgresError) && postgresError.SQLState() == "55006" {
			// A cancelled reader may take a moment to release its replication slot.
			// Wait rather than leaking the persistent slot after a failed test.
			waitForNextPoll(t, ctx)
			continue
		}
		t.Fatalf("drop replication slot %q: %v", slotName, err)
	}
}

func registerSlotCleanup(t *testing.T, slotName string) {
	t.Helper()
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		dropSlot(t, cleanupCtx, slotName)
	})
}

func waitForNextPoll(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-ctx.Done():
		t.Fatal("timed out waiting for PostgreSQL reader state")
	case <-time.After(20 * time.Millisecond):
	}
}
