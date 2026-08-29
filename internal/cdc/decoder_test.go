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
	if err == nil || err.Error() != "cdc: row change references unknown relation ID 999" {
		t.Fatalf("error = %v", err)
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
