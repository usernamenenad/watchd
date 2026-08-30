package cdc

import (
	"errors"
	"fmt"
	"maps"

	"github.com/jackc/pglogrepl"
)

var (
	// ErrTransactionInProgress indicates an invalid nested BEGIN message.
	ErrTransactionInProgress = errors.New("cdc: received BEGIN while a transaction is pending")
	// ErrNoTransaction indicates a row mutation or COMMIT without BEGIN.
	ErrNoTransaction = errors.New("cdc: received row change or COMMIT without a pending transaction")
	// ErrUnknownRelation indicates a row mutation whose relation metadata was
	// not received on this replication connection.
	ErrUnknownRelation = errors.New("cdc: row change references unknown relation")
	// ErrUnsupportedPGOutputMessage indicates a pgoutput message that could
	// affect a projection but is not part of the v0 row-change contract.
	ErrUnsupportedPGOutputMessage = errors.New("cdc: unsupported pgoutput message")
	// ErrTransactionTooLarge indicates that retaining the in-flight source
	// transaction would exceed the configured memory bound.
	ErrTransactionTooLarge = errors.New("cdc: in-flight transaction exceeds configured limit")
	// ErrTransactionTooManyChanges indicates that an in-flight transaction has
	// more row changes than the configured bound permits.
	ErrTransactionTooManyChanges = errors.New("cdc: in-flight transaction exceeds configured change limit")
	// ErrPrimaryKeyChangeUnsupported indicates a source update that changes a
	// projection key. v0 Change has one key, so representing it as an update
	// would leave an obsolete row in a consumer projection.
	ErrPrimaryKeyChangeUnsupported = errors.New("cdc: primary-key updates are not supported")
)

const (
	defaultMaxTransactionBytes   = 16 << 20
	defaultMaxTransactionChanges = 100_000
)

// Decoder translates pgoutput protocol messages into committed transactions.
// It is intentionally stateful: PostgreSQL identifies tables by relation ID and
// can send many row changes before the COMMIT that makes them visible.
type Decoder struct {
	relations             map[uint32]*pglogrepl.RelationMessage
	pending               []Change
	pendingBytes          int
	maxTransactionBytes   int
	maxTransactionChanges int
}

// NewDecoder returns a Decoder with no registered relations or pending changes.
func NewDecoder() *Decoder {
	return NewDecoderWithLimits(defaultMaxTransactionBytes, defaultMaxTransactionChanges)
}

// NewDecoderWithLimits returns a Decoder whose in-flight transaction is
// bounded by maxTransactionBytes and maxTransactionChanges. A non-positive
// limit selects the package default; Reader configuration rejects invalid
// limits before creating a decoder.
func NewDecoderWithLimits(maxTransactionBytes, maxTransactionChanges int) *Decoder {
	if maxTransactionBytes <= 0 {
		maxTransactionBytes = defaultMaxTransactionBytes
	}
	if maxTransactionChanges <= 0 {
		maxTransactionChanges = defaultMaxTransactionChanges
	}

	return &Decoder{
		relations:             make(map[uint32]*pglogrepl.RelationMessage),
		maxTransactionBytes:   maxTransactionBytes,
		maxTransactionChanges: maxTransactionChanges,
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
		d.pendingBytes = 0
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
		d.pendingBytes = 0
		return transaction, nil

	case *pglogrepl.TypeMessage, *pglogrepl.OriginMessage:
		// These messages describe PostgreSQL metadata or replication origin. They
		// do not mutate a v0 projection and are not needed to decode row tuples.
		return nil, nil

	default:
		// In particular, do not silently ignore TRUNCATE or logical-decoding
		// messages. Either could leave a projection incorrect.
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedPGOutputMessage, message.Type())
	}
}

func (d *Decoder) append(change Change) error {
	if d.pending == nil {
		return ErrNoTransaction
	}
	if len(d.pending) >= d.maxTransactionChanges {
		return ErrTransactionTooManyChanges
	}

	changeBytes := estimatedChangeBytes(change)
	if changeBytes > d.maxTransactionBytes-d.pendingBytes {
		return ErrTransactionTooLarge
	}
	d.pending = append(d.pending, change)
	d.pendingBytes += changeBytes

	return nil
}

// Pending returns the current transaction's bounded in-memory footprint. It
// is intended for reader diagnostics and must be called from the reader's
// single goroutine.
func (d *Decoder) Pending() (changes, bytes int) {
	return len(d.pending), d.pendingBytes
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
	if message.OldTuple != nil {
		oldKey, err := keyFromTuple(relation, message.OldTuple)
		if err != nil {
			return Change{}, err
		}
		if !maps.Equal(oldKey, key) {
			return Change{}, ErrPrimaryKeyChangeUnsupported
		}
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
		return nil, fmt.Errorf("%w ID %d", ErrUnknownRelation, id)
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
	if len(tuple.Columns) == len(relation.Columns) {
		// PostgreSQL may send the complete old row (for example with REPLICA
		// IDENTITY FULL) instead of a key-only tuple. Decode it as normal row
		// state, then select the configured replica-identity columns.
		values, err := tupleValues(relation, tuple)
		if err != nil {
			return nil, err
		}
		return keyFromValues(relation, values)
	}
	if len(tuple.Columns) != len(keyColumns) {
		return nil, fmt.Errorf("cdc: relation %s key tuple has %d columns, want %d or %d", relationTable(relation), len(tuple.Columns), len(keyColumns), len(relation.Columns))
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

// estimatedChangeBytes is deliberately conservative. It accounts for all
// retained strings and byte slices plus a fixed allowance for maps, slices,
// and Go object headers. The exact allocator footprint is implementation
// dependent, but this provides a deterministic source-controlled bound.
func estimatedChangeBytes(change Change) int {
	const changeOverhead = 256

	bytes := changeOverhead + len(change.Operation) + len(change.Table)
	for key, value := range change.Key {
		bytes += len(key) + len(value)
	}
	for key, value := range change.Values {
		bytes += len(key) + estimatedValueBytes(value)
	}
	return bytes
}

func estimatedValueBytes(value any) int {
	switch value := value.(type) {
	case nil:
		return 1
	case string:
		return len(value)
	case []byte:
		return len(value)
	case UnchangedToast:
		return 1
	default:
		// columnValue only creates the types above. Reserve a small amount if a
		// future decoder adds another representation before this estimator does.
		return 64
	}
}
