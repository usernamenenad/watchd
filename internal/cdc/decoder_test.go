package cdc

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
)

func TestDecoderEmitsOnlyCommittedTransaction(t *testing.T) {
	decoder := NewDecoder()
	relation := sampleRelation()

	mustConsume(t, decoder, relation)
	mustConsume(t, decoder, &pglogrepl.BeginMessage{Xid: 731})
	mustConsume(t, decoder, &pglogrepl.InsertMessage{
		RelationID: relation.RelationID,
		Tuple:      fullTuple("acme", "alice", "editor"),
	})

	transaction, err := decoder.Consume(&pglogrepl.CommitMessage{CommitLSN: 42, TransactionEndLSN: 90})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	want := &Transaction{
		Cursor:    Cursor{lsn: 90},
		commitLSN: 42,
		xid:       731,
		Changes: []Change{{
			Operation: OperationInsert,
			Table:     "public.tenant_permissions_projection",
			Key:       map[string]string{"tenant_id": "acme", "user_id": "alice"},
			Columns:   []string{"tenant_id", "user_id", "permissions"},
			Values:    []Value{TextValue("acme"), TextValue("alice"), TextValue("editor")},
		}},
	}
	if !reflect.DeepEqual(transaction, want) {
		t.Fatalf("transaction = %#v, want %#v", transaction, want)
	}
}

func TestDecoderPreservesUpdateAndDeleteInOneTransaction(t *testing.T) {
	decoder := NewDecoder()
	relation := sampleRelation()
	mustConsume(t, decoder, relation)
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})
	mustConsume(t, decoder, &pglogrepl.UpdateMessage{
		RelationID: relation.RelationID,
		NewTuple:   fullTuple("acme", "alice", "admin"),
	})
	mustConsume(t, decoder, &pglogrepl.DeleteMessage{
		RelationID: relation.RelationID,
		OldTuple:   keyTuple("acme", "bob"),
	})

	transaction, err := decoder.Consume(&pglogrepl.CommitMessage{CommitLSN: 43})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got, want := len(transaction.Changes), 2; got != want {
		t.Fatalf("changes = %d, want %d", got, want)
	}
	if change := transaction.Changes[0]; change.Operation != OperationUpdate || change.Values[2] != TextValue("admin") {
		t.Fatalf("update = %#v", change)
	}
	if change := transaction.Changes[1]; change.Operation != OperationDelete || !reflect.DeepEqual(change.Key, map[string]string{"tenant_id": "acme", "user_id": "bob"}) {
		t.Fatalf("delete = %#v", change)
	}
}

func TestDecoderDerivesDeleteKeyFromFullOldTuple(t *testing.T) {
	decoder := NewDecoder()
	relation := sampleRelation()
	mustConsume(t, decoder, relation)
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})
	mustConsume(t, decoder, &pglogrepl.DeleteMessage{
		RelationID: relation.RelationID,
		OldTuple:   fullTuple("acme", "alice", "editor"),
	})

	transaction, err := decoder.Consume(&pglogrepl.CommitMessage{CommitLSN: 44})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got, want := transaction.Changes[0].Key, map[string]string{"tenant_id": "acme", "user_id": "alice"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delete key = %#v, want %#v", got, want)
	}
}

func TestDecoderRejectsPrimaryKeyUpdate(t *testing.T) {
	decoder := NewDecoder()
	relation := sampleRelation()
	mustConsume(t, decoder, relation)
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})

	_, err := decoder.Consume(&pglogrepl.UpdateMessage{
		RelationID: relation.RelationID,
		OldTuple:   keyTuple("acme", "alice"),
		NewTuple:   fullTuple("acme", "bob", "editor"),
	})
	if !errors.Is(err, ErrPrimaryKeyChangeUnsupported) {
		t.Fatalf("error = %v, want %v", err, ErrPrimaryKeyChangeUnsupported)
	}
}

func TestDecoderRejectsRowChangesOutsideTransaction(t *testing.T) {
	decoder := NewDecoder()
	relation := sampleRelation()
	mustConsume(t, decoder, relation)

	_, err := decoder.Consume(&pglogrepl.InsertMessage{RelationID: relation.RelationID, Tuple: fullTuple("acme", "alice", "editor")})
	if !errors.Is(err, ErrNoTransaction) {
		t.Fatalf("error = %v, want %v", err, ErrNoTransaction)
	}
}

func TestDecoderRejectsUnknownRelation(t *testing.T) {
	decoder := NewDecoder()
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})

	_, err := decoder.Consume(&pglogrepl.InsertMessage{RelationID: 999, Tuple: fullTuple("acme", "alice", "editor")})
	if !errors.Is(err, ErrUnknownRelation) {
		t.Fatalf("error = %v, want %v", err, ErrUnknownRelation)
	}
	if err.Error() != "cdc: row change references unknown relation ID 999" {
		t.Fatalf("error = %v", err)
	}
}

func TestDecoderRejectsUnsupportedProjectionMutation(t *testing.T) {
	decoder := NewDecoder()

	_, err := decoder.Consume(&pglogrepl.TruncateMessage{})
	if !errors.Is(err, ErrUnsupportedPGOutputMessage) {
		t.Fatalf("error = %v, want %v", err, ErrUnsupportedPGOutputMessage)
	}
}

func TestDecoderEnforcesTransactionLimits(t *testing.T) {
	decoder := NewDecoderWithLimits(1024, 1, 0)
	relation := sampleRelation()
	mustConsume(t, decoder, relation)
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})
	mustConsume(t, decoder, &pglogrepl.InsertMessage{
		RelationID: relation.RelationID,
		Tuple:      fullTuple("acme", "alice", "editor"),
	})

	_, err := decoder.Consume(&pglogrepl.InsertMessage{
		RelationID: relation.RelationID,
		Tuple:      fullTuple("acme", "bob", "viewer"),
	})
	if !errors.Is(err, ErrTransactionTooManyChanges) {
		t.Fatalf("error = %v, want %v", err, ErrTransactionTooManyChanges)
	}

	tooSmall := NewDecoderWithLimits(1, 10, 0)
	mustConsume(t, tooSmall, relation)
	mustConsume(t, tooSmall, &pglogrepl.BeginMessage{})
	_, err = tooSmall.Consume(&pglogrepl.InsertMessage{
		RelationID: relation.RelationID,
		Tuple:      fullTuple("acme", "alice", "editor"),
	})
	if !errors.Is(err, ErrTransactionTooLarge) {
		t.Fatalf("error = %v, want %v", err, ErrTransactionTooLarge)
	}
}

func TestDecoderDistinguishesNullFromUnchangedToast(t *testing.T) {
	decoder := NewDecoder()
	relation := sampleRelation()
	mustConsume(t, decoder, relation)
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})
	mustConsume(t, decoder, &pglogrepl.InsertMessage{
		RelationID: relation.RelationID,
		Tuple: &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
			textColumn("acme"),
			textColumn("alice"),
			nullColumn(),
		}},
	})
	mustConsume(t, decoder, &pglogrepl.UpdateMessage{
		RelationID: relation.RelationID,
		NewTuple: &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
			textColumn("acme"),
			textColumn("bob"),
			toastColumn(),
		}},
	})

	transaction, err := decoder.Consume(&pglogrepl.CommitMessage{CommitLSN: 45})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	if insertValue, _ := transaction.Changes[0].Value("permissions"); insertValue != (Value{Kind: ValueNull}) {
		t.Fatalf("null value = %#v, want ValueNull", insertValue)
	}
	if updateValue, _ := transaction.Changes[1].Value("permissions"); updateValue != (Value{Kind: ValueUnchangedToast}) {
		t.Fatalf("unchanged-toast value = %#v, want ValueUnchangedToast", updateValue)
	}
}

func TestDecoderRejectsUnsupportedColumnEncoding(t *testing.T) {
	decoder := NewDecoder()
	relation := sampleRelation()
	mustConsume(t, decoder, relation)
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})

	_, err := decoder.Consume(&pglogrepl.InsertMessage{
		RelationID: relation.RelationID,
		Tuple: &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
			textColumn("acme"),
			textColumn("alice"),
			binaryColumn([]byte{0x01, 0x02}),
		}},
	})
	if !errors.Is(err, ErrUnsupportedColumnEncoding) {
		t.Fatalf("error = %v, want %v", err, ErrUnsupportedColumnEncoding)
	}
}

func TestDecoderRejectsOversizedValue(t *testing.T) {
	decoder := NewDecoderWithLimits(1<<20, 100, 4)
	relation := sampleRelation()
	mustConsume(t, decoder, relation)
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})

	_, err := decoder.Consume(&pglogrepl.InsertMessage{
		RelationID: relation.RelationID,
		Tuple:      fullTuple("acme", "alice", "way-too-long-for-the-limit"),
	})
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("error = %v, want %v", err, ErrValueTooLarge)
	}
}

func TestDecoderRejectsOversizedKeyValue(t *testing.T) {
	decoder := NewDecoderWithLimits(1<<20, 100, 4)
	relation := sampleRelation()
	mustConsume(t, decoder, relation)
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})

	_, err := decoder.Consume(&pglogrepl.DeleteMessage{
		RelationID: relation.RelationID,
		OldTuple:   keyTuple("way-too-long-tenant", "bob"),
	})
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("error = %v, want %v", err, ErrValueTooLarge)
	}
}

func sampleRelation() *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID:   42,
		Namespace:    "public",
		RelationName: "tenant_permissions_projection",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "tenant_id", Flags: 1},
			{Name: "user_id", Flags: 1},
			{Name: "permissions"},
		},
	}
}

func fullTuple(tenantID, userID, permissions string) *pglogrepl.TupleData {
	return &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		textColumn(tenantID),
		textColumn(userID),
		textColumn(permissions),
	}}
}

func keyTuple(tenantID, userID string) *pglogrepl.TupleData {
	return &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		textColumn(tenantID),
		textColumn(userID),
	}}
}

func textColumn(value string) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(value)}
}

func nullColumn() *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull}
}

func toastColumn() *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeToast}
}

func binaryColumn(data []byte) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeBinary, Data: data}
}

func mustConsume(t *testing.T, decoder *Decoder, message pglogrepl.Message) {
	t.Helper()
	transaction, err := decoder.Consume(message)
	if err != nil {
		t.Fatalf("consume %T: %v", message, err)
	}
	if transaction != nil {
		t.Fatalf("consume %T returned unexpected transaction %#v", message, transaction)
	}
}

// BenchmarkDecoderConsume measures the decoder alone, on pre-parsed messages:
// one transaction with an insert, an update carrying its old key, a key-only
// delete, and a delete carrying the full old row (REPLICA IDENTITY FULL).
func BenchmarkDecoderConsume(b *testing.B) {
	relation := sampleRelation()
	messages := []pglogrepl.Message{
		&pglogrepl.BeginMessage{Xid: 731},
		&pglogrepl.InsertMessage{RelationID: relation.RelationID, Tuple: fullTuple("acme", "alice", "editor")},
		&pglogrepl.UpdateMessage{RelationID: relation.RelationID, OldTuple: keyTuple("acme", "alice"), NewTuple: fullTuple("acme", "alice", "admin")},
		&pglogrepl.DeleteMessage{RelationID: relation.RelationID, OldTuple: keyTuple("acme", "bob")},
		&pglogrepl.DeleteMessage{RelationID: relation.RelationID, OldTuple: fullTuple("acme", "carol", "viewer")},
		&pglogrepl.CommitMessage{CommitLSN: 42, TransactionEndLSN: 90},
	}
	decoder := NewDecoder()
	if _, err := decoder.Consume(relation); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		for _, message := range messages {
			if _, err := decoder.Consume(message); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func TestDecoderRejectsPrimaryKeyUpdateWithFullOldTuple(t *testing.T) {
	decoder := NewDecoder()
	relation := sampleRelation()
	mustConsume(t, decoder, relation)
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})

	_, err := decoder.Consume(&pglogrepl.UpdateMessage{
		RelationID: relation.RelationID,
		OldTuple:   fullTuple("acme", "alice", "editor"),
		NewTuple:   fullTuple("acme", "bob", "editor"),
	})
	if !errors.Is(err, ErrPrimaryKeyChangeUnsupported) {
		t.Fatalf("error = %v, want %v", err, ErrPrimaryKeyChangeUnsupported)
	}
}

func TestDecoderAcceptsUpdateWithUnchangedFullOldTuple(t *testing.T) {
	decoder := NewDecoder()
	relation := sampleRelation()
	mustConsume(t, decoder, relation)
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})
	mustConsume(t, decoder, &pglogrepl.UpdateMessage{
		RelationID: relation.RelationID,
		OldTuple:   fullTuple("acme", "alice", "editor"),
		NewTuple:   fullTuple("acme", "alice", "admin"),
	})
}

func TestDecoderRejectsNullKeyInFullOldTuple(t *testing.T) {
	decoder := NewDecoder()
	relation := sampleRelation()
	mustConsume(t, decoder, relation)
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})

	_, err := decoder.Consume(&pglogrepl.DeleteMessage{
		RelationID: relation.RelationID,
		OldTuple: &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
			textColumn("acme"),
			nullColumn(),
			textColumn("editor"),
		}},
	})
	if err == nil || err.Error() != "cdc: key user_id is not a non-empty text value" {
		t.Fatalf("error = %v, want a non-empty key error", err)
	}
}

func TestDecoderDistinguishesEmptyTextFromNull(t *testing.T) {
	decoder := NewDecoder()
	relation := sampleRelation()
	mustConsume(t, decoder, relation)
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})
	mustConsume(t, decoder, &pglogrepl.InsertMessage{
		RelationID: relation.RelationID,
		Tuple:      fullTuple("acme", "alice", ""),
	})

	transaction, err := decoder.Consume(&pglogrepl.CommitMessage{CommitLSN: 46})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got, want := transaction.Changes[0].Values, []Value{TextValue("acme"), TextValue("alice"), TextValue("")}; !reflect.DeepEqual(got, want) {
		t.Fatalf("values = %#v, want %#v", got, want)
	}
}

// Decoded values must not alias the message's buffers: pgoutput tuple data
// belongs to the replication connection, and a Change outlives the message.
func TestDecoderCopiesTupleData(t *testing.T) {
	decoder := NewDecoder()
	relation := sampleRelation()
	insert := fullTuple("acme", "alice", "editor")
	deleted := keyTuple("acme", "bob")
	mustConsume(t, decoder, relation)
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})
	mustConsume(t, decoder, &pglogrepl.InsertMessage{RelationID: relation.RelationID, Tuple: insert})
	mustConsume(t, decoder, &pglogrepl.DeleteMessage{RelationID: relation.RelationID, OldTuple: deleted})
	transaction, err := decoder.Consume(&pglogrepl.CommitMessage{CommitLSN: 47})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	for _, tuple := range []*pglogrepl.TupleData{insert, deleted} {
		for _, column := range tuple.Columns {
			for i := range column.Data {
				column.Data[i] = 'x'
			}
		}
	}

	if got, want := transaction.Changes[0].Values, []Value{TextValue("acme"), TextValue("alice"), TextValue("editor")}; !reflect.DeepEqual(got, want) {
		t.Fatalf("insert values = %#v, want %#v", got, want)
	}
	if got, want := transaction.Changes[0].Key, map[string]string{"tenant_id": "acme", "user_id": "alice"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("insert key = %#v, want %#v", got, want)
	}
	if got, want := transaction.Changes[1].Key, map[string]string{"tenant_id": "acme", "user_id": "bob"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delete key = %#v, want %#v", got, want)
	}
}

// A committed Transaction owns its Changes: the next transaction must not
// write into the same backing array.
func TestDecoderTransactionsDoNotShareChanges(t *testing.T) {
	decoder := NewDecoder()
	relation := sampleRelation()
	mustConsume(t, decoder, relation)

	commit := func(userID string) *Transaction {
		t.Helper()
		mustConsume(t, decoder, &pglogrepl.BeginMessage{})
		mustConsume(t, decoder, &pglogrepl.InsertMessage{RelationID: relation.RelationID, Tuple: fullTuple("acme", userID, "editor")})
		transaction, err := decoder.Consume(&pglogrepl.CommitMessage{CommitLSN: 48})
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
		return transaction
	}

	first := commit("alice")
	commit("bob")
	if got := first.Changes[0].Key["user_id"]; got != "alice" {
		t.Fatalf("first transaction's change was overwritten: user_id = %q", got)
	}
}

func TestDecoderEmptyTransactionHasNoChanges(t *testing.T) {
	decoder := NewDecoder()
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})
	transaction, err := decoder.Consume(&pglogrepl.CommitMessage{CommitLSN: 49})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if transaction.Changes != nil {
		t.Fatalf("changes = %#v, want nil", transaction.Changes)
	}
}

// A new RelationMessage (for example after ALTER TABLE) must not rename the
// columns of changes already decoded under the old one.
func TestDecoderRelationUpdateKeepsDecodedColumns(t *testing.T) {
	decoder := NewDecoder()
	relation := sampleRelation()
	mustConsume(t, decoder, relation)
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})
	mustConsume(t, decoder, &pglogrepl.InsertMessage{RelationID: relation.RelationID, Tuple: fullTuple("acme", "alice", "editor")})

	renamed := sampleRelation()
	renamed.Columns[2].Name = "role"
	mustConsume(t, decoder, renamed)
	mustConsume(t, decoder, &pglogrepl.InsertMessage{RelationID: relation.RelationID, Tuple: fullTuple("acme", "bob", "viewer")})

	transaction, err := decoder.Consume(&pglogrepl.CommitMessage{CommitLSN: 50})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := transaction.Changes[0].Columns[2]; got != "permissions" {
		t.Fatalf("first change column = %q, want permissions", got)
	}
	if got := transaction.Changes[1].Columns[2]; got != "role" {
		t.Fatalf("second change column = %q, want role", got)
	}
}

func shapedDecoder(t *testing.T) (*Decoder, *shapes) {
	t.Helper()
	registry := &shapes{}
	baseline := sampleRelation()
	for _, column := range baseline.Columns {
		column.DataType = 25
	}
	shape := shapeOfRelation(baseline)
	registry.record(&shape)
	decoder := NewDecoder()
	decoder.shapes = registry
	return decoder, registry
}

func TestDecoderResyncsOnIncompatibleRelationChange(t *testing.T) {
	cases := map[string]struct {
		mutate func(*pglogrepl.RelationMessage)
		want   string
	}{
		"add a column": {
			func(r *pglogrepl.RelationMessage) {
				r.Columns = append(r.Columns, &pglogrepl.RelationMessageColumn{Name: "extra", DataType: 25})
			},
			`column "extra" was added`,
		},
		"drop a column": {
			func(r *pglogrepl.RelationMessage) { r.Columns = r.Columns[:2] },
			`column "permissions" was dropped`,
		},
		"change a type": {
			func(r *pglogrepl.RelationMessage) { r.Columns[2].DataType = 23 },
			`column "permissions" changed type`,
		},
		"change the primary key": {
			func(r *pglogrepl.RelationMessage) { r.Columns[1].Flags = 0 },
			"key columns changed",
		},
		"change the replica identity": {
			func(r *pglogrepl.RelationMessage) { r.ReplicaIdentity = 'f' },
			"replica identity changed",
		},
		"recreate the table": {
			func(r *pglogrepl.RelationMessage) { r.RelationID = 43 },
			"dropped and recreated",
		},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			decoder, _ := shapedDecoder(t)
			changed := sampleRelation()
			for _, column := range changed.Columns {
				column.DataType = 25
			}
			test.mutate(changed)

			mustConsume(t, decoder, &pglogrepl.BeginMessage{})
			mustConsume(t, decoder, changed)
			mustConsume(t, decoder, &pglogrepl.InsertMessage{RelationID: changed.RelationID, Tuple: fullTuple("acme", "alice", "editor")})
			transaction, err := decoder.Consume(&pglogrepl.CommitMessage{CommitLSN: 1, TransactionEndLSN: 2})
			if err != nil {
				t.Fatalf("commit: %v", err)
			}
			if len(transaction.Changes) != 0 {
				t.Fatalf("changes = %#v, want the incompatible table's changes dropped", transaction.Changes)
			}
			if len(transaction.Incompatible) != 1 {
				t.Fatalf("incompatible = %v, want one", transaction.Incompatible)
			}
			got := transaction.Incompatible[0]
			if !errors.Is(got, ErrIncompatibleRelation) || got.Table != "public.tenant_permissions_projection" || !strings.Contains(got.Difference, test.want) {
				t.Fatalf("incompatible = %#v, want table public.tenant_permissions_projection and difference containing %q", got, test.want)
			}
		})
	}
}

func TestDecoderAcceptsAnIdenticalRelationMessage(t *testing.T) {
	decoder, _ := shapedDecoder(t)
	same := sampleRelation()
	for _, column := range same.Columns {
		column.DataType = 25
	}
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})
	mustConsume(t, decoder, same)
	mustConsume(t, decoder, &pglogrepl.InsertMessage{RelationID: same.RelationID, Tuple: fullTuple("acme", "alice", "editor")})
	transaction, err := decoder.Consume(&pglogrepl.CommitMessage{CommitLSN: 1, TransactionEndLSN: 2})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(transaction.Changes) != 1 || len(transaction.Incompatible) != 0 {
		t.Fatalf("transaction = %#v, want its change delivered and nothing incompatible", transaction)
	}
}

func TestDecoderRecoversWhenTheBaselineMoves(t *testing.T) {
	decoder, registry := shapedDecoder(t)
	changed := sampleRelation()
	for _, column := range changed.Columns {
		column.DataType = 25
	}
	changed.Columns = append(changed.Columns, &pglogrepl.RelationMessageColumn{Name: "extra", DataType: 25})
	insert := &pglogrepl.InsertMessage{RelationID: changed.RelationID, Tuple: &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		textColumn("acme"), textColumn("alice"), textColumn("editor"), textColumn("x"),
	}}}

	mustConsume(t, decoder, &pglogrepl.BeginMessage{})
	mustConsume(t, decoder, changed)
	mustConsume(t, decoder, insert)
	first, err := decoder.Consume(&pglogrepl.CommitMessage{CommitLSN: 1, TransactionEndLSN: 2})
	if err != nil || len(first.Changes) != 0 || len(first.Incompatible) != 1 {
		t.Fatalf("first = %#v, %v; want its change dropped and one incompatible table", first, err)
	}

	// Re-validating the spec against the new table shape accepts it, with no
	// new Relation message from the source.
	shape := shapeOfRelation(changed)
	registry.record(&shape)
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})
	mustConsume(t, decoder, insert)
	second, err := decoder.Consume(&pglogrepl.CommitMessage{CommitLSN: 3, TransactionEndLSN: 4})
	if err != nil || len(second.Changes) != 1 || len(second.Incompatible) != 0 {
		t.Fatalf("second = %#v, %v; want its change delivered", second, err)
	}
}

func TestDecoderReportsARenamedTable(t *testing.T) {
	decoder, _ := shapedDecoder(t)
	renamed := sampleRelation()
	for _, column := range renamed.Columns {
		column.DataType = 25
	}
	renamed.RelationName = "renamed"
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})
	mustConsume(t, decoder, renamed)
	transaction, err := decoder.Consume(&pglogrepl.CommitMessage{CommitLSN: 1, TransactionEndLSN: 2})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(transaction.Incompatible) != 1 || transaction.Incompatible[0].Table != "public.tenant_permissions_projection" || !strings.Contains(transaction.Incompatible[0].Difference, "renamed to public.renamed") {
		t.Fatalf("incompatible = %v, want the projected table reported as renamed", transaction.Incompatible)
	}
}

func TestDecoderIgnoresTablesWithoutABaseline(t *testing.T) {
	decoder := NewDecoder()
	decoder.shapes = &shapes{}
	relation := sampleRelation()
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})
	mustConsume(t, decoder, relation)
	mustConsume(t, decoder, &pglogrepl.InsertMessage{RelationID: relation.RelationID, Tuple: fullTuple("acme", "alice", "editor")})
	transaction, err := decoder.Consume(&pglogrepl.CommitMessage{CommitLSN: 1, TransactionEndLSN: 2})
	if err != nil || len(transaction.Changes) != 1 || len(transaction.Incompatible) != 0 {
		t.Fatalf("transaction = %#v, %v; want the change delivered", transaction, err)
	}
}
