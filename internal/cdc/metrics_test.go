package cdc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/usernamenenad/watchd/internal/telemetry"
	"github.com/usernamenenad/watchd/internal/telemetry/telemetrytest"
)

// pgoutput wire encoders, for driving consumeCopyData end to end.

var postgresEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

func xlogData(walStart, serverWALEnd uint64, payload []byte) []byte {
	data := []byte{pglogrepl.XLogDataByteID}
	data = binary.BigEndian.AppendUint64(data, walStart)
	data = binary.BigEndian.AppendUint64(data, serverWALEnd)
	data = binary.BigEndian.AppendUint64(data, 0) // server time
	return append(data, payload...)
}

func pgTime(t time.Time) uint64 { return uint64(t.Sub(postgresEpoch).Microseconds()) }

func beginPayload(finalLSN uint64, commitTime time.Time, xid uint32) []byte {
	data := []byte{'B'}
	data = binary.BigEndian.AppendUint64(data, finalLSN)
	data = binary.BigEndian.AppendUint64(data, pgTime(commitTime))
	return binary.BigEndian.AppendUint32(data, xid)
}

func commitPayload(commitLSN, endLSN uint64, commitTime time.Time) []byte {
	data := []byte{'C', 0}
	data = binary.BigEndian.AppendUint64(data, commitLSN)
	data = binary.BigEndian.AppendUint64(data, endLSN)
	return binary.BigEndian.AppendUint64(data, pgTime(commitTime))
}

// relationPayload describes public.tenant_permissions_projection with a
// (tenant_id, user_id) key and a text permissions column.
func relationPayload(relationID uint32) []byte {
	data := []byte{'R'}
	data = binary.BigEndian.AppendUint32(data, relationID)
	data = append(data, "public\x00tenant_permissions_projection\x00"...)
	data = append(data, 'd')
	data = binary.BigEndian.AppendUint16(data, 3)
	for _, column := range []struct {
		name string
		key  bool
	}{{"tenant_id", true}, {"user_id", true}, {"permissions", false}} {
		flags := byte(0)
		if column.key {
			flags = 1
		}
		data = append(data, flags)
		data = append(data, column.name+"\x00"...)
		data = binary.BigEndian.AppendUint32(data, 25) // text
		data = binary.BigEndian.AppendUint32(data, 0xFFFFFFFF)
	}
	return data
}

func insertPayload(relationID uint32, values ...string) []byte {
	data := []byte{'I'}
	data = binary.BigEndian.AppendUint32(data, relationID)
	data = append(data, 'N')
	data = binary.BigEndian.AppendUint16(data, uint16(len(values)))
	for _, value := range values {
		data = append(data, 't')
		data = binary.BigEndian.AppendUint32(data, uint32(len(value)))
		data = append(data, value...)
	}
	return data
}

// transactionMessages is one transaction of n inserts as CopyData payloads.
func transactionMessages(n int, lsn uint64, commitTime time.Time) [][]byte {
	const relationID = 16384
	messages := [][]byte{
		xlogData(lsn, lsn+1000, relationPayload(relationID)),
		xlogData(lsn, lsn+1000, beginPayload(lsn+100, commitTime, 700)),
	}
	for i := range n {
		messages = append(messages, xlogData(lsn+uint64(i), lsn+1000, insertPayload(relationID, "acme", fmt.Sprintf("user-%d", i), "editor")))
	}
	return append(messages, xlogData(lsn+100, lsn+1000, commitPayload(lsn+100, lsn+120, commitTime)))
}

func newMeteredReader(t testing.TB, meter metric.Meter, sink TransactionSink) *Reader {
	t.Helper()
	reader, err := NewReader(ReaderConfig{
		DatabaseURL:     "postgres://example.invalid/watchd",
		SourceID:        "test-postgres",
		SlotName:        "watchd_source",
		PublicationName: "watchd_publication",
		Meter:           meter,
	}, sink)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	reader.sendStandbyStatus = func(context.Context, *pgconn.PgConn, pglogrepl.LSN) error { return nil }
	return reader
}

func noAttributes() attribute.Set { return *attribute.EmptySet() }

func TestReaderMetricsFollowAStreamedTransaction(t *testing.T) {
	metrics := telemetrytest.New(t)
	commitTime := time.Now().Add(-time.Second)
	var accepted Transaction
	reader := newMeteredReader(t, metrics.Meter(), func(_ context.Context, transaction Transaction) error {
		accepted = transaction
		return nil
	})

	decoder := NewDecoder()
	safeLSN := pglogrepl.LSN(0)
	messages := transactionMessages(3, 5000, commitTime)
	for _, data := range messages {
		if err := reader.consumeCopyData(context.Background(), nil, decoder, &safeLSN, data); err != nil {
			t.Fatalf("consumeCopyData: %v", err)
		}
	}
	if !accepted.CommitTime.Equal(commitTime.Truncate(time.Microsecond)) {
		t.Fatalf("CommitTime = %v, want %v", accepted.CommitTime, commitTime)
	}

	for name, want := range map[string]float64{
		"watchd.cdc.transactions":       1,
		"watchd.cdc.messages":           float64(len(messages)),
		"watchd.cdc.lsn.acknowledged":   5120,
		"watchd.cdc.lsn.server_wal_end": 6000,
		"watchd.cdc.lsn.received":       5100,
		"watchd.cdc.wal_lag":            900,
		"watchd.cdc.ack_lag":            0,
	} {
		if got, ok := metrics.Value(t, name, noAttributes()); !ok || got != want {
			t.Errorf("%s = %v (recorded %v), want %v", name, got, ok, want)
		}
	}
	for _, name := range []string{
		"watchd.cdc.transaction.duration",
		"watchd.cdc.transaction.changes",
		"watchd.cdc.transaction.size",
		"watchd.cdc.sink.duration",
		"watchd.cdc.commit_to_accept",
		"watchd.cdc.standby_status.duration",
	} {
		if got := metrics.HistogramCount(t, name, noAttributes()); got != 1 {
			t.Errorf("%s recorded %d times, want 1", name, got)
		}
	}
	if bytes, _ := metrics.Value(t, "watchd.cdc.wal.bytes", noAttributes()); bytes <= 0 {
		t.Errorf("watchd.cdc.wal.bytes = %v, want > 0", bytes)
	}
	metrics.AssertAllowedAttributes(t)
}

func TestReaderMetricsCountFailuresAndState(t *testing.T) {
	metrics := telemetrytest.New(t)
	reader := newMeteredReader(t, metrics.Meter(), func(context.Context, Transaction) error { return nil })
	reader.config.RetryPolicy = RetryPolicy{InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, MaxAttempts: 2}
	reader.connect = func(context.Context, string) (*CDC, error) { return nil, errors.New("network unavailable") }
	reader.wait = func(context.Context, time.Duration) error { return nil }

	if err := reader.Run(context.Background()); !errors.Is(err, ErrRetryExhausted) {
		t.Fatalf("Run = %v, want ErrRetryExhausted", err)
	}
	if got, _ := metrics.Value(t, "watchd.cdc.reconnects", noAttributes()); got != 2 {
		t.Errorf("reconnects = %v, want 2", got)
	}
	unavailable := telemetry.Attributes(telemetry.KeyErrorClass.String("source_unavailable"))
	if got, _ := metrics.Value(t, "watchd.cdc.stream.errors", unavailable); got != 3 {
		t.Errorf("stream errors{source_unavailable} = %v, want 3", got)
	}
	for _, state := range connectionStates {
		want := float64(0)
		if state == stateFailed {
			want = 1
		}
		if got, _ := metrics.Value(t, "watchd.cdc.stream.state", telemetry.Attributes(telemetry.KeyState.String(state))); got != want {
			t.Errorf("stream.state{%s} = %v, want %v", state, got, want)
		}
	}
	metrics.AssertAllowedAttributes(t)
}

func TestSnapshotObserverMeasuresAScopeRead(t *testing.T) {
	metrics := telemetrytest.New(t)
	reader := newMeteredReader(t, metrics.Meter(), func(context.Context, Transaction) error { return nil })
	ctx := context.Background()
	snapshot := telemetry.Attributes(telemetry.KeyMode.String(modeSnapshot))
	bootstrap := telemetry.Attributes(telemetry.KeyMode.String(modeBootstrap))

	observer := reader.metrics.startSnapshot(ctx, modeSnapshot)
	if got, _ := metrics.Value(t, "watchd.cdc.snapshot.active", snapshot); got != 1 {
		t.Fatalf("active during a read = %v, want 1", got)
	}
	sink := observer.sink(func(context.Context, []map[string]any) error { return nil })
	if err := sink(ctx, []map[string]any{{"tenant_id": "acme", "note": nil}, {"tenant_id": "acme"}}); err != nil {
		t.Fatal(err)
	}
	observer.page(ctx, time.Millisecond)
	observer.finish(ctx, nil)

	failed := reader.metrics.startSnapshot(ctx, modeBootstrap)
	failed.finish(ctx, ErrSnapshotWindowClosed)

	for name, want := range map[string]float64{
		"watchd.cdc.snapshot.active": 0,
		"watchd.cdc.snapshot.rows":   2,
		"watchd.cdc.snapshot.bytes":  float64(len("tenant_id")*2 + len("acme")*2 + len("note")),
	} {
		if got, _ := metrics.Value(t, name, snapshot); got != want {
			t.Errorf("%s{snapshot} = %v, want %v", name, got, want)
		}
	}
	if got := metrics.HistogramCount(t, "watchd.cdc.snapshot.duration", snapshot); got != 1 {
		t.Errorf("snapshot.duration{snapshot} count = %d, want 1", got)
	}
	if got := metrics.HistogramCount(t, "watchd.cdc.snapshot.duration", bootstrap); got != 0 {
		t.Errorf("a failed read recorded a duration")
	}
	windowClosed := telemetry.Attributes(telemetry.KeyMode.String(modeBootstrap), telemetry.KeyErrorClass.String("snapshot_window_closed"))
	if got, _ := metrics.Value(t, "watchd.cdc.snapshot.errors", windowClosed); got != 1 {
		t.Errorf("snapshot.errors = %v, want 1", got)
	}
	metrics.AssertAllowedAttributes(t)
}

func TestErrorClass(t *testing.T) {
	for err, want := range map[error]string{
		retryableError(ErrSlotInUse, "55006"):                            "slot_in_use",
		fmt.Errorf("%w: boom", ErrSourceUnavailable):                     "source_unavailable",
		fmt.Errorf("%w: %w", ErrSinkRejected, errors.New("hub stopped")): "sink_rejected",
		ErrValueTooLarge:                           "transaction_too_large",
		errors.New("unexpected"):                   "other",
		context.DeadlineExceeded:                   "timeout",
		terminalError(ErrSlotInvalidated, "42704"): "slot_invalidated",
	} {
		if got := errorClass(err); got != want {
			t.Errorf("errorClass(%v) = %q, want %q", err, got, want)
		}
	}
}

func TestPowersOf4EndAtTheLimit(t *testing.T) {
	got := powersOf4(1, 100)
	want := []float64{1, 4, 16, 64, 100}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("powersOf4(1, 100) = %v, want %v", got, want)
	}
}

// BenchmarkConsumeTransaction measures the reader's per-transaction path -
// decode, sink, acknowledge - with and without instruments, to show what
// instrumentation costs. Compare allocs/op between the two.
func BenchmarkConsumeTransaction(b *testing.B) {
	for _, mode := range []string{"noop", "sdk"} {
		b.Run(mode, func(b *testing.B) {
			var meter metric.Meter
			if mode == "sdk" {
				meter = telemetrytest.New(b).Meter()
			}
			reader := newMeteredReader(b, meter, func(context.Context, Transaction) error { return nil })
			decoder := NewDecoder()
			messages := transactionMessages(10, 5000, time.Now())
			safeLSN := pglogrepl.LSN(0)
			ctx := context.Background()
			b.ReportAllocs()
			for b.Loop() {
				for _, data := range messages {
					if err := reader.consumeCopyData(ctx, nil, decoder, &safeLSN, data); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
