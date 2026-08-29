package cdc

import (
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"
)

var (
	// ErrTransactionInProgress indicates an invalid nested BEGIN message.
	ErrTransactionInProgress = errors.New("cdc: received BEGIN while a transaction is pending")
	// ErrNoTransaction indicates a row mutation or COMMIT without BEGIN.
	ErrNoTransaction = errors.New("cdc: received row change or COMMIT without a pending transaction")
)

// Decoder translates pgoutput protocol messages into committed transactions.
// It is intentionally stateful: PostgreSQL identifies tables by relation ID and
// can send many row changes before the COMMIT that makes them visible.
type Decoder struct {
	relations map[uint32]*pglogrepl.RelationMessage
	pending   []Change
}

// NewDecoder returns a Decoder with no registered relations or pending changes.
func NewDecoder() *Decoder {
	return &Decoder{
		relations: make(map[uint32]*pglogrepl.RelationMessage),
	}
}

// Consume records one decoded pgoutput message. It returns a non-nil
// Transaction only when PostgreSQL commits the pending batch.
func (d *Decoder) Consume(message pglogrepl.Message) (*Transaction, error) {
	switch message := message.(type) {
	case *pglogrepl.RelationMessage:
		d.relations[message.RelationID] = message
		return nil, nil

	case *pglogrepl.BeginMessage:
		if d.pending != nil {
			return nil, ErrTransactionInProgress
		}
		d.pending = make([]Change, 0)
		return nil, nil

	case *pglogrepl.InsertMessage:
		change, err := d.insert(message)
		if err != nil {
			return nil, err
		}
		return nil, d.append(change)

	case *pglogrepl.UpdateMessage:
		change, err := d.update(message)
		if err != nil {
			return nil, err
		}
		return nil, d.append(change)

	case *pglogrepl.DeleteMessage:
		change, err := d.delete(message)
		if err != nil {
			return nil, err
		}
		return nil, d.append(change)

	case *pglogrepl.CommitMessage:
		if d.pending == nil {
			return nil, ErrNoTransaction
		}
		transaction := &Transaction{
			Cursor:  message.CommitLSN.String(),
			Changes: append([]Change(nil), d.pending...),
		}
		d.pending = nil
		return transaction, nil

	default:
		return nil, nil
	}
}

func (d *Decoder) append(change Change) error {
	if d.pending == nil {
		return ErrNoTransaction
	}
	d.pending = append(d.pending, change)

	return nil
}

func (d *Decoder) insert(message *pglogrepl.InsertMessage) (Change, error) {
	relation, err := d.relation(message.RelationID)
	if err != nil {
		return Change{}, err
	}

	values, err := tupleValues(relation, message.Tuple)
	if err != nil {
		return Change{}, err
	}

	key, err := keyFromValues(relation, values)
	if err != nil {
		return Change{}, err
	}

	return Change{
		Operation: OperationInsert,
		Table:     relationTable(relation),
		Key:       key,
		Values:    values,
	}, nil
}

func (d *Decoder) update(message *pglogrepl.UpdateMessage) (Change, error) {
	relation, err := d.relation(message.RelationID)
	if err != nil {
		return Change{}, err
	}

	values, err := tupleValues(relation, message.NewTuple)
	if err != nil {
		return Change{}, err
	}

	key, err := keyFromValues(relation, values)
	if err != nil {
		return Change{}, err
	}

	return Change{
		Operation: OperationUpdate,
		Table:     relationTable(relation),
		Key:       key,
		Values:    values,
	}, nil
}

func (d *Decoder) delete(message *pglogrepl.DeleteMessage) (Change, error) {
	relation, err := d.relation(message.RelationID)
	if err != nil {
		return Change{}, err
	}

	key, err := keyFromTuple(relation, message.OldTuple)
	if err != nil {
		return Change{}, err
	}

	return Change{
		Operation: OperationDelete,
		Table:     relationTable(relation),
		Key:       key,
	}, nil
}

func (d *Decoder) relation(id uint32) (*pglogrepl.RelationMessage, error) {
	relation := d.relations[id]
	if relation == nil {
		return nil, fmt.Errorf("cdc: row change references unknown relation ID %d", id)
	}

	return relation, nil
}

func relationTable(relation *pglogrepl.RelationMessage) string {
	return qualifiedTable(relation.Namespace, relation.RelationName)
}

func tupleValues(relation *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) (map[string]any, error) {
	if tuple == nil {
		return nil, errors.New("cdc: row mutation is missing a tuple")
	}

	if len(tuple.Columns) != len(relation.Columns) {
		return nil, fmt.Errorf("cdc: relation %s has %d columns but row tuple has %d", relationTable(relation), len(relation.Columns), len(tuple.Columns))
	}

	values := make(map[string]any, len(tuple.Columns))
	for index, column := range tuple.Columns {
		values[relation.Columns[index].Name] = columnValue(column)
	}
	return values, nil
}

func keyFromTuple(relation *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) (map[string]string, error) {
	if tuple == nil {
		return nil, errors.New("cdc: delete mutation is missing its replica-identity tuple")
	}

	keyColumns := replicaIdentityColumns(relation)
	if len(keyColumns) == 0 {
		return nil, fmt.Errorf("cdc: relation %s has no replica-identity key columns", relationTable(relation))
	}
	if len(tuple.Columns) != len(keyColumns) {
		return nil, fmt.Errorf("cdc: relation %s key tuple has %d columns, want %d", relationTable(relation), len(tuple.Columns), len(keyColumns))
	}

	key := make(map[string]string, len(keyColumns))
	for index, column := range tuple.Columns {
		value, err := keyValue(column)
		if err != nil {
			return nil, fmt.Errorf("cdc: decode key %s: %w", keyColumns[index].Name, err)
		}
		key[keyColumns[index].Name] = value
	}

	return key, nil
}

func keyFromValues(relation *pglogrepl.RelationMessage, values map[string]any) (map[string]string, error) {
	keyColumns := replicaIdentityColumns(relation)
	if len(keyColumns) == 0 {
		return nil, fmt.Errorf("cdc: relation %s has no replica-identity key columns", relationTable(relation))
	}

	key := make(map[string]string, len(keyColumns))
	for _, column := range keyColumns {
		value, ok := values[column.Name]
		if !ok {
			return nil, fmt.Errorf("cdc: row state is missing key %s", column.Name)
		}
		stringValue, ok := value.(string)
		if !ok || stringValue == "" {
			return nil, fmt.Errorf("cdc: key %s is not a non-empty text value", column.Name)
		}
		key[column.Name] = stringValue
	}

	return key, nil
}

func replicaIdentityColumns(relation *pglogrepl.RelationMessage) []*pglogrepl.RelationMessageColumn {
	var keyColumns []*pglogrepl.RelationMessageColumn
	for _, column := range relation.Columns {
		if column.Flags == 1 {
			keyColumns = append(keyColumns, column)
		}
	}

	return keyColumns
}

func columnValue(column *pglogrepl.TupleDataColumn) any {
	switch column.DataType {

	case pglogrepl.TupleDataTypeNull:
		return nil

	case pglogrepl.TupleDataTypeText:
		return string(column.Data)

	case pglogrepl.TupleDataTypeBinary:
		return append([]byte(nil), column.Data...)

	case pglogrepl.TupleDataTypeToast:
		return UnchangedToast{}

	default:
		return append([]byte(nil), column.Data...)
	}
}

func keyValue(column *pglogrepl.TupleDataColumn) (string, error) {
	if column.DataType != pglogrepl.TupleDataTypeText {
		return "", fmt.Errorf("key is not text (type %q)", column.DataType)
	}
	if len(column.Data) == 0 {
		return "", errors.New("key is empty")
	}

	return string(column.Data), nil
}
