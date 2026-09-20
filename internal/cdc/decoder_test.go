package cdc

import (
	"errors"
	"reflect"
	"testing"

	"github.com/jackc/pglogrepl"
)

func TestDecoderEmitsOnlyCommittedTransaction(t *testing.T) {
	decoder := NewDecoder()
	relation := sampleRelation()

	mustConsume(t, decoder, relation)
	mustConsume(t, decoder, &pglogrepl.BeginMessage{})
	mustConsume(t, decoder, &pglogrepl.InsertMessage{
		RelationID: relation.RelationID,
		Tuple:      fullTuple("acme", "alice", "editor"),
	})

	transaction, err := decoder.Consume(&pglogrepl.CommitMessage{CommitLSN: 42})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	want := &Transaction{
		Cursor: "0/2A",
		Changes: []Change{{
			Operation: OperationInsert,
			Table:     "public.tenant_permissions_projection",
			Key:       map[string]string{"tenant_id": "acme", "user_id": "alice"},
			Values: map[string]any{
				"tenant_id":   "acme",
				"user_id":     "alice",
				"permissions": "editor",
			},
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
	if change := transaction.Changes[0]; change.Operation != OperationUpdate || change.Values["permissions"] != "admin" {
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

	insertValue := transaction.Changes[0].Values["permissions"]
	if insertValue != nil {
		t.Fatalf("null value = %#v, want nil", insertValue)
	}

	updateValue := transaction.Changes[1].Values["permissions"]
	if _, ok := updateValue.(UnchangedToast); !ok {
		t.Fatalf("unchanged-toast value = %#v (%T), want UnchangedToast", updateValue, updateValue)
	}
	if updateValue == nil {
		t.Fatalf("unchanged-toast value must not equal nil")
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
