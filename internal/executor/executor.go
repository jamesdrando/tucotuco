package executor

import (
	"errors"

	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/types"
)

var (
	// ErrOperatorNotOpen reports an operator method call that requires a
	// successful Open first.
	ErrOperatorNotOpen = errors.New("executor: operator is not open")
	// ErrOperatorOpen reports a duplicate Open call on an already-open operator.
	ErrOperatorOpen = errors.New("executor: operator is already open")
	// ErrOperatorClosed reports work attempted after an operator has been closed.
	ErrOperatorClosed = errors.New("executor: operator is closed")
)

// Operator is the Phase 1 Volcano-model contract implemented by physical
// query operators.
//
// Lifecycle semantics:
//   - Open prepares the operator for iteration and may succeed at most once.
//   - Next yields one row at a time and returns io.EOF when the operator is
//     exhausted.
//   - Next must not be called before Open or after Close.
//   - Close releases resources, is terminal, and may be called repeatedly.
type Operator interface {
	Open() error
	Next() (Row, error)
	Close() error
}

// Row carries one executor-visible row.
//
// Handle is optional. Operators that originate from storage, such as the later
// sequential scan, can preserve the originating handle for update/delete paths.
// Synthetic operators may leave Handle as the zero value.
//
// Rows defensively own their value slice so callers cannot mutate operator
// state through shared backing storage.
type Row struct {
	Handle storage.RowHandle
	values []types.Value
}

// NewRow constructs an executor row without a storage handle.
func NewRow(values ...types.Value) Row {
	return Row{values: append([]types.Value(nil), values...)}
}

// NewRowWithHandle constructs an executor row tied to one storage handle.
func NewRowWithHandle(handle storage.RowHandle, values ...types.Value) Row {
	row := NewRow(values...)
	row.Handle = handle

	return row
}

// NewRowFromStorage converts one storage record into an executor row.
func NewRowFromStorage(record storage.Record) Row {
	return Row{
		Handle: record.Handle,
		values: record.Row.Values(),
	}
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

// Clone returns a copy of the row, including its handle and backing slice.
func (r Row) Clone() Row {
	return Row{
		Handle: r.Handle,
		values: append([]types.Value(nil), r.values...),
	}
}

// StorageRow converts the executor row into the storage-layer row shape.
func (r Row) StorageRow() storage.Row {
	return storage.NewRow(r.values...)
}

type lifecycleState uint8

const (
	lifecycleReady lifecycleState = iota
	lifecycleOpen
	lifecycleClosed
)

// lifecycle centralizes the Phase 1 Open/Next/Close state machine so future
// operators share one lifecycle contract.
type lifecycle struct {
	state lifecycleState
}

// Open transitions the lifecycle into the open state.
func (l *lifecycle) Open() error {
	switch l.state {
	case lifecycleReady:
		l.state = lifecycleOpen
		return nil
	case lifecycleOpen:
		return ErrOperatorOpen
	case lifecycleClosed:
		return ErrOperatorClosed
	default:
		return ErrOperatorClosed
	}
}

// Next validates that iteration is currently allowed.
func (l *lifecycle) Next() error {
	switch l.state {
	case lifecycleReady:
		return ErrOperatorNotOpen
	case lifecycleOpen:
		return nil
	case lifecycleClosed:
		return ErrOperatorClosed
	default:
		return ErrOperatorClosed
	}
}

// Close transitions the lifecycle into its terminal state.
//
// Close is idempotent so callers can defer cleanup without tracking whether
// Open succeeded.
func (l *lifecycle) Close() error {
	l.state = lifecycleClosed

	return nil
}
