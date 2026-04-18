package storage

import (
	"fmt"

	"github.com/jamesdrando/tucotuco/internal/types"
)

// Storage provides the primitive row-store operations shared by the Phase 1
// in-memory engine and later paged implementations.
type Storage interface {
	// Insert appends a row to a table and returns its storage handle.
	Insert(tx Transaction, table TableID, row Row) (RowHandle, error)
	// Scan opens an iterator over rows that match the supplied options.
	Scan(tx Transaction, table TableID, options ScanOptions) (RowIterator, error)
	// Update replaces the row stored at handle with row.
	Update(tx Transaction, table TableID, handle RowHandle, row Row) error
	// Delete removes the row stored at handle from a table.
	Delete(tx Transaction, table TableID, handle RowHandle) error
	// NewTransaction starts a new storage transaction.
	NewTransaction(options TransactionOptions) (Transaction, error)
}

// Transaction is the storage-layer transaction handle.
type Transaction interface {
	// IsolationLevel reports the transaction's isolation level.
	IsolationLevel() IsolationLevel
	// ReadOnly reports whether the transaction may write.
	ReadOnly() bool
	// Commit persists the transaction's changes.
	Commit() error
	// Rollback discards the transaction's changes.
	Rollback() error
}

// Row stores SQL values in ordinal order.
//
// Rows defensively own their values so callers cannot mutate storage-visible
// state through slices or mutable payloads returned from the API.
type Row struct {
	values []types.Value
}

// NewRow constructs a row from the supplied values.
func NewRow(values ...types.Value) Row {
	return Row{values: append([]types.Value(nil), values...)}
}

// Len reports how many column values the row carries.
func (r Row) Len() int {
	return len(r.values)
}

// Value returns the value at index and whether the index was in range.
func (r Row) Value(index int) (types.Value, bool) {
	if index < 0 || index >= len(r.values) {
		return types.Value{}, false
	}

	return r.values[index], true
}

// Values returns a defensive copy of the row values.
func (r Row) Values() []types.Value {
	return append([]types.Value(nil), r.values...)
}

// Clone returns a copy of the row and its backing slice.
func (r Row) Clone() Row {
	return Row{values: append([]types.Value(nil), r.values...)}
}

// TableID identifies a table by schema and name.
type TableID struct {
	Schema string
	Name   string
}

// Valid reports whether the identifier names a table.
func (id TableID) Valid() bool {
	return id.Name != ""
}

// String formats the identifier as schema-qualified SQL text when possible.
func (id TableID) String() string {
	if id.Name == "" {
		return id.Schema
	}

	if id.Schema == "" {
		return id.Name
	}

	return id.Schema + "." + id.Name
}

// RowHandle identifies a row inside a storage implementation.
//
// Callers should treat the handle as opaque. The zero value is invalid.
type RowHandle struct {
	Page uint64
	Slot uint64
}

// Valid reports whether the handle points at a stored row.
func (h RowHandle) Valid() bool {
	return h.Page != 0 || h.Slot != 0
}

// String formats the handle for logs and debug output.
func (h RowHandle) String() string {
	return fmt.Sprintf("%d:%d", h.Page, h.Slot)
}

// Record pairs a scanned row with the handle needed for later updates or
// deletes.
type Record struct {
	Handle RowHandle
	Row    Row
}

// RowIterator yields scanned rows one at a time.
//
// Next returns io.EOF when the iterator is exhausted.
type RowIterator interface {
	Next() (Record, error)
	Close() error
}

// ComparisonOp describes the comparison a scan constraint performs.
type ComparisonOp uint8

const (
	// ComparisonEqual matches rows whose column equals the constraint value.
	ComparisonEqual ComparisonOp = iota + 1
	// ComparisonNotEqual matches rows whose column differs from the constraint value.
	ComparisonNotEqual
	// ComparisonLess matches rows whose column is less than the constraint value.
	ComparisonLess
	// ComparisonLessOrEqual matches rows whose column is less than or equal to the constraint value.
	ComparisonLessOrEqual
	// ComparisonGreater matches rows whose column is greater than the constraint value.
	ComparisonGreater
	// ComparisonGreaterOrEqual matches rows whose column is greater than or equal to the constraint value.
	ComparisonGreaterOrEqual
)

// String formats the comparison operator for diagnostics.
func (op ComparisonOp) String() string {
	switch op {
	case ComparisonEqual:
		return "="
	case ComparisonNotEqual:
		return "<>"
	case ComparisonLess:
		return "<"
	case ComparisonLessOrEqual:
		return "<="
	case ComparisonGreater:
		return ">"
	case ComparisonGreaterOrEqual:
		return ">="
	default:
		return "UNKNOWN"
	}
}

// ScanConstraint applies a simple comparison against a zero-based column
// ordinal.
type ScanConstraint struct {
	Column int
	Op     ComparisonOp
	Value  types.Value
}

// ScanOptions controls how rows are scanned.
type ScanOptions struct {
	Constraints []ScanConstraint
	Limit       int
}

// Normalized returns a copy of the scan options with defensive slices and
// defaults applied.
//
// A zero Limit means "no limit". Negative limits are normalized to zero.
func (o ScanOptions) Normalized() ScanOptions {
	normalized := o
	if normalized.Limit < 0 {
		normalized.Limit = 0
	}

	normalized.Constraints = append([]ScanConstraint(nil), normalized.Constraints...)

	return normalized
}

// IsolationLevel is the SQL transaction isolation level.
type IsolationLevel string

const (
	// IsolationReadUncommitted maps to SQL READ UNCOMMITTED.
	IsolationReadUncommitted IsolationLevel = "READ UNCOMMITTED"
	// IsolationReadCommitted maps to SQL READ COMMITTED.
	IsolationReadCommitted IsolationLevel = "READ COMMITTED"
	// IsolationRepeatableRead maps to SQL REPEATABLE READ.
	IsolationRepeatableRead IsolationLevel = "REPEATABLE READ"
	// IsolationSerializable maps to SQL SERIALIZABLE.
	IsolationSerializable IsolationLevel = "SERIALIZABLE"
)

// String returns the SQL text form of the isolation level.
func (l IsolationLevel) String() string {
	return string(l)
}

// TransactionOptions configures a new transaction.
type TransactionOptions struct {
	Isolation IsolationLevel
	ReadOnly  bool
}

// Normalized returns a copy of the options with defaults applied.
//
// The default isolation level is READ COMMITTED as defined by SPEC.md §9.
func (o TransactionOptions) Normalized() TransactionOptions {
	normalized := o
	if normalized.Isolation == "" {
		normalized.Isolation = IsolationReadCommitted
	}

	return normalized
}
