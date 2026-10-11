package cdc

import (
	"errors"
	"fmt"
	"strings"
	"time"

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
	// ErrUnsupportedColumnEncoding indicates a pgoutput column whose wire
	// encoding is not v0's text/null/unchanged-toast value contract (see
	// "Value encoding" in docs/semantics.md). watchd starts replication
	// without the pgoutput "binary" option, so this should not occur in
	// practice; it exists so a protocol-level encoding this decoder cannot
	// represent is rejected explicitly instead of silently mis-decoded.
	ErrUnsupportedColumnEncoding = errors.New("cdc: column value is not in the text/null/unchanged-toast wire encoding")
	// ErrValueTooLarge indicates a single column value exceeds the configured
	// per-value size bound, independent of the whole-transaction bound.
	ErrValueTooLarge = errors.New("cdc: column value exceeds configured single-value size limit")
)

const (
	defaultMaxTransactionBytes   = 16 << 20
	defaultMaxTransactionChanges = 100_000
	defaultMaxValueBytes         = 1 << 20

	// maxPendingCapacityHint caps how many changes BEGIN preallocates from the
	// previous transaction's size, so one large transaction does not make
	// every later one reserve a large slice.
	maxPendingCapacityHint = 1024
)

// Decoder translates pgoutput protocol messages into committed transactions.
// It is intentionally stateful: PostgreSQL identifies tables by relation ID and
// can send many row changes before the COMMIT that makes them visible.
type Decoder struct {
	relations             map[uint32]*relationInfo
	pending               []Change
	pendingXid            uint32
	pendingCommitTime     time.Time
	pendingBytes          int
	pendingCapacityHint   int
	maxTransactionBytes   int
	maxTransactionChanges int
	maxValueBytes         int
}

// relationInfo is what row decoding needs from a RelationMessage, derived
// once when the message arrives rather than for every row change.
type relationInfo struct {
	table string
	// columns are the relation's column names in tuple order. Every Change
	// of the relation shares this slice, so it is never modified: a new
	// RelationMessage replaces the whole relationInfo.
	columns []string
	// keyIndexes are the positions of the replica-identity columns.
	keyIndexes []int
}

func newRelationInfo(relation *pglogrepl.RelationMessage) *relationInfo {
	info := &relationInfo{
		table:   qualifiedTable(relation.Namespace, relation.RelationName),
		columns: make([]string, len(relation.Columns)),
	}
	for index, column := range relation.Columns {
		info.columns[index] = column.Name
		if column.Flags == 1 {
			info.keyIndexes = append(info.keyIndexes, index)
		}
	}

	return info
}

// NewDecoder returns a Decoder with no registered relations or pending changes.
func NewDecoder() *Decoder {
	return NewDecoderWithLimits(defaultMaxTransactionBytes, defaultMaxTransactionChanges, defaultMaxValueBytes)
}

// NewDecoderWithLimits returns a Decoder whose in-flight transaction is
// bounded by maxTransactionBytes and maxTransactionChanges, and whose
// individual column values are bounded by maxValueBytes. A non-positive
// limit selects the package default; Reader configuration rejects invalid
// limits before creating a decoder.
func NewDecoderWithLimits(maxTransactionBytes, maxTransactionChanges, maxValueBytes int) *Decoder {
	if maxTransactionBytes <= 0 {
		maxTransactionBytes = defaultMaxTransactionBytes
	}
	if maxTransactionChanges <= 0 {
		maxTransactionChanges = defaultMaxTransactionChanges
	}
	if maxValueBytes <= 0 {
		maxValueBytes = defaultMaxValueBytes
	}

	return &Decoder{
		relations:             make(map[uint32]*relationInfo),
		maxTransactionBytes:   maxTransactionBytes,
		maxTransactionChanges: maxTransactionChanges,
		maxValueBytes:         maxValueBytes,
	}
}

// Consume records one decoded pgoutput message. It returns a non-nil
// Transaction only when PostgreSQL commits the pending batch.
func (d *Decoder) Consume(message pglogrepl.Message) (*Transaction, error) {
	switch message := message.(type) {
	case *pglogrepl.RelationMessage:
		d.relations[message.RelationID] = newRelationInfo(message)
		return nil, nil

	case *pglogrepl.BeginMessage:
		if d.pending != nil {
			return nil, ErrTransactionInProgress
		}
		// A non-nil pending slice is what marks the transaction open, even
		// with zero capacity.
		d.pending = make([]Change, 0, d.pendingCapacityHint)
		d.pendingXid = message.Xid
		d.pendingCommitTime = message.CommitTime
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
		// The transaction takes ownership of the pending slice; the next
		// BEGIN starts a fresh one, so nothing here is copied.
		changes := d.pending
		if len(changes) == 0 {
			changes = nil
		}
		d.pendingCapacityHint = min(len(changes), maxPendingCapacityHint)
		// The Decoder does not know which source it reads; the Reader stamps
		// the cursor's source ID before handing the transaction to its sink.
		transaction := &Transaction{
			Cursor:     Cursor{lsn: message.TransactionEndLSN},
			Changes:    changes,
			CommitTime: d.pendingCommitTime,
			commitLSN:  message.CommitLSN,
			xid:        d.pendingXid,
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

// InTransaction reports whether a BEGIN has arrived without its COMMIT. It
// must be called from the reader's single goroutine.
func (d *Decoder) InTransaction() bool {
	return d.pending != nil
}

// Pending returns the current transaction's bounded in-memory footprint. It
// is intended for reader diagnostics and must be called from the reader's
// single goroutine.
func (d *Decoder) Pending() (changes, bytes int) {
	return len(d.pending), d.pendingBytes
}

func (d *Decoder) insert(message *pglogrepl.InsertMessage) (Change, error) {
	info, err := d.relation(message.RelationID)
	if err != nil {
		return Change{}, err
	}

	values, err := decodeRow(info, message.Tuple, d.maxValueBytes)
	if err != nil {
		return Change{}, err
	}

	key, err := keyFromValues(info, values)
	if err != nil {
		return Change{}, err
	}

	return Change{
		Operation: OperationInsert,
		Table:     info.table,
		Key:       key,
		Columns:   info.columns,
		Values:    values,
	}, nil
}

func (d *Decoder) update(message *pglogrepl.UpdateMessage) (Change, error) {
	info, err := d.relation(message.RelationID)
	if err != nil {
		return Change{}, err
	}

	values, err := decodeRow(info, message.NewTuple, d.maxValueBytes)
	if err != nil {
		return Change{}, err
	}

	key, err := keyFromValues(info, values)
	if err != nil {
		return Change{}, err
	}
	if message.OldTuple != nil {
		// The old key is only compared, so validate it in place rather than
		// building a second key map.
		if err := checkKeyTuple(info, message.OldTuple, d.maxValueBytes); err != nil {
			return Change{}, err
		}
		for k, index := range info.keyIndexes {
			if string(keyColumn(info, message.OldTuple.Columns, k).Data) != values[index].Text {
				return Change{}, ErrPrimaryKeyChangeUnsupported
			}
		}
	}

	return Change{
		Operation: OperationUpdate,
		Table:     info.table,
		Key:       key,
		Columns:   info.columns,
		Values:    values,
	}, nil
}

func (d *Decoder) delete(message *pglogrepl.DeleteMessage) (Change, error) {
	info, err := d.relation(message.RelationID)
	if err != nil {
		return Change{}, err
	}

	if err := checkKeyTuple(info, message.OldTuple, d.maxValueBytes); err != nil {
		return Change{}, err
	}

	return Change{
		Operation: OperationDelete,
		Table:     info.table,
		Key:       keyFromTuple(info, message.OldTuple.Columns),
	}, nil
}

func (d *Decoder) relation(id uint32) (*relationInfo, error) {
	info := d.relations[id]
	if info == nil {
		return nil, fmt.Errorf("%w ID %d", ErrUnknownRelation, id)
	}

	return info, nil
}

// decodeRow decodes a complete row tuple into v0's value contract (see
// Value). All of the row's text is copied into one string and each value is
// a substring of it: one allocation per row rather than one per column. A
// retained value or key therefore keeps the whole row's text alive, which
// costs nothing extra while the Change itself is retained.
func decodeRow(info *relationInfo, tuple *pglogrepl.TupleData, maxValueBytes int) ([]Value, error) {
	if tuple == nil {
		return nil, errors.New("cdc: row mutation is missing a tuple")
	}
	textBytes, err := checkRow(info, tuple, maxValueBytes)
	if err != nil {
		return nil, err
	}

	var builder strings.Builder
	builder.Grow(textBytes)
	for _, column := range tuple.Columns {
		if column.DataType == pglogrepl.TupleDataTypeText {
			builder.Write(column.Data)
		}
	}
	text := builder.String()

	values := make([]Value, len(tuple.Columns))
	offset := 0
	for index, column := range tuple.Columns {
		switch column.DataType {
		case pglogrepl.TupleDataTypeText:
			end := offset + len(column.Data)
			values[index] = Value{Kind: ValueText, Text: text[offset:end]}
			offset = end
		case pglogrepl.TupleDataTypeToast:
			values[index].Kind = ValueUnchangedToast
		}
		// TupleDataTypeNull is the zero Value.
	}

	return values, nil
}

// checkRow validates every column of a complete row tuple and returns the
// total length of its text values.
func checkRow(info *relationInfo, tuple *pglogrepl.TupleData, maxValueBytes int) (int, error) {
	if len(tuple.Columns) != len(info.columns) {
		return 0, fmt.Errorf("cdc: relation %s has %d columns but row tuple has %d", info.table, len(info.columns), len(tuple.Columns))
	}

	textBytes := 0
	for index, column := range tuple.Columns {
		if err := checkColumn(column, maxValueBytes); err != nil {
			return 0, fmt.Errorf("cdc: decode column %s: %w", info.columns[index], err)
		}
		textBytes += len(column.Data)
	}

	return textBytes, nil
}

func keyFromValues(info *relationInfo, values []Value) (map[string]string, error) {
	if len(info.keyIndexes) == 0 {
		return nil, fmt.Errorf("cdc: relation %s has no replica-identity key columns", info.table)
	}

	key := make(map[string]string, len(info.keyIndexes))
	for _, index := range info.keyIndexes {
		value := values[index]
		if value.Kind != ValueText || value.Text == "" {
			return nil, fmt.Errorf("cdc: key %s is not a non-empty text value", info.columns[index])
		}
		key[info.columns[index]] = value.Text
	}

	return key, nil
}

// checkKeyTuple validates an old-row tuple carrying a row's replica
// identity. PostgreSQL sends either the key columns alone or, for example
// with REPLICA IDENTITY FULL, the complete old row; a complete row is held
// to the same rules as new row state.
func checkKeyTuple(info *relationInfo, tuple *pglogrepl.TupleData, maxValueBytes int) error {
	if tuple == nil {
		return errors.New("cdc: delete mutation is missing its replica-identity tuple")
	}
	if len(info.keyIndexes) == 0 {
		return fmt.Errorf("cdc: relation %s has no replica-identity key columns", info.table)
	}

	switch len(tuple.Columns) {
	case len(info.columns):
		if _, err := checkRow(info, tuple, maxValueBytes); err != nil {
			return err
		}
		for _, index := range info.keyIndexes {
			column := tuple.Columns[index]
			if column.DataType != pglogrepl.TupleDataTypeText || len(column.Data) == 0 {
				return fmt.Errorf("cdc: key %s is not a non-empty text value", info.columns[index])
			}
		}

	case len(info.keyIndexes):
		for k, column := range tuple.Columns {
			if err := checkKeyColumn(column, maxValueBytes); err != nil {
				return fmt.Errorf("cdc: decode key %s: %w", info.columns[info.keyIndexes[k]], err)
			}
		}

	default:
		return fmt.Errorf("cdc: relation %s key tuple has %d columns, want %d or %d", info.table, len(tuple.Columns), len(info.keyIndexes), len(info.columns))
	}

	return nil
}

// keyColumn returns the k-th replica-identity column of a tuple that
// checkKeyTuple accepted, whether it is a complete row or key columns only.
func keyColumn(info *relationInfo, columns []*pglogrepl.TupleDataColumn, k int) *pglogrepl.TupleDataColumn {
	if len(columns) == len(info.columns) {
		return columns[info.keyIndexes[k]]
	}
	return columns[k]
}

// keyFromTuple builds the Key of a tuple that checkKeyTuple accepted, with
// the key text in one string as decodeRow does for a row.
func keyFromTuple(info *relationInfo, columns []*pglogrepl.TupleDataColumn) map[string]string {
	textBytes := 0
	for k := range info.keyIndexes {
		textBytes += len(keyColumn(info, columns, k).Data)
	}
	var builder strings.Builder
	builder.Grow(textBytes)
	for k := range info.keyIndexes {
		builder.Write(keyColumn(info, columns, k).Data)
	}
	text := builder.String()

	key := make(map[string]string, len(info.keyIndexes))
	offset := 0
	for k, index := range info.keyIndexes {
		end := offset + len(keyColumn(info, columns, k).Data)
		key[info.columns[index]] = text[offset:end]
		offset = end
	}

	return key
}

// checkColumn checks one pgoutput column against v0's value contract: a
// PostgreSQL text-format value, SQL NULL, or a TOAST column omitted from an
// UPDATE. See "Value encoding" in docs/semantics.md. watchd never requests
// the pgoutput "binary" option, so TupleDataTypeBinary and any other
// discriminator are rejected rather than passed through as an opaque byte
// slice - this decoder has no way to represent them under the
// text/null/unchanged-toast contract, and silently forwarding raw bytes
// would let a protocol change mis-decode a value instead of failing loudly.
func checkColumn(column *pglogrepl.TupleDataColumn, maxValueBytes int) error {
	switch column.DataType {
	case pglogrepl.TupleDataTypeNull, pglogrepl.TupleDataTypeToast:
		return nil

	case pglogrepl.TupleDataTypeText:
		if len(column.Data) > maxValueBytes {
			return fmt.Errorf("%w: %d bytes exceeds %d-byte limit", ErrValueTooLarge, len(column.Data), maxValueBytes)
		}
		return nil

	default:
		return fmt.Errorf("%w: discriminator %q", ErrUnsupportedColumnEncoding, column.DataType)
	}
}

func checkKeyColumn(column *pglogrepl.TupleDataColumn, maxValueBytes int) error {
	if column.DataType != pglogrepl.TupleDataTypeText {
		return fmt.Errorf("%w: key is not text (type %q)", ErrUnsupportedColumnEncoding, column.DataType)
	}
	if len(column.Data) == 0 {
		return errors.New("key is empty")
	}
	if len(column.Data) > maxValueBytes {
		return fmt.Errorf("%w: %d bytes exceeds %d-byte limit", ErrValueTooLarge, len(column.Data), maxValueBytes)
	}

	return nil
}

// estimatedChangeBytes is deliberately conservative. It accounts for all
// retained strings plus a fixed allowance for the key map, slices, and Go
// object headers. The exact allocator footprint is implementation
// dependent, but this provides a deterministic source-controlled bound.
func estimatedChangeBytes(change Change) int {
	const changeOverhead = 256

	bytes := changeOverhead + len(change.Operation) + len(change.Table)
	for key, value := range change.Key {
		bytes += len(key) + len(value)
	}
	for index, value := range change.Values {
		bytes += len(change.Columns[index]) + estimatedValueBytes(value)
	}
	return bytes
}

func estimatedValueBytes(value Value) int {
	if value.Kind == ValueText {
		return len(value.Text)
	}
	// NULL and unchanged TOAST carry no text.
	return 1
}
