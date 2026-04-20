package executor

import (
	"errors"
	"fmt"
	"io"

	"github.com/jamesdrando/tucotuco/internal/storage"
)

var (
	errWriteNilStore                 = errors.New("executor: write store is nil")
	errInsertNilChild                = errors.New("executor: insert child is nil")
	errUpdateNilChild                = errors.New("executor: update child is nil")
	errDeleteNilChild                = errors.New("executor: delete child is nil")
	errWriteMissingHandle            = errors.New("executor: write row handle is invalid")
	errUpdateNegativeTargetCount     = errors.New("executor: update target column count must be non-negative")
	errUpdateAssignmentShape         = errors.New("executor: update assignment shape mismatch")
	errUpdateTargetOrdinalOutOfRange = errors.New("executor: update target ordinal out of range")
	errUpdateRowTooShort             = errors.New("executor: update row shorter than target column count")
)

// WriteOperator is the executor-local contract shared by DML operators.
//
// Next performs one storage mutation per call and returns the affected row.
// AffectedRows reports how many mutations have completed successfully so far.
type WriteOperator interface {
	Operator

	AffectedRows() int64
}

type insertSourceKind uint8

const (
	insertSourceValues insertSourceKind = iota + 1
	insertSourceChild
)

// Insert is the executor-side INSERT operator.
//
// Values-mode inserts the supplied executor rows directly. Child-mode consumes
// rows from its child operator and inserts each child row's values. In both
// modes, Next returns the inserted row with the storage handle assigned by the
// target table.
type Insert struct {
	lifecycle   lifecycle
	store       storage.Storage
	tx          storage.Transaction
	table       storage.TableID
	source      insertSourceKind
	rows        []Row
	child       Operator
	childOpen   bool
	nextIndex   int
	affected    int64
	done        bool
	terminalErr error
}

var _ WriteOperator = (*Insert)(nil)

// NewInsertValues constructs an INSERT operator over precomputed executor
// rows.
func NewInsertValues(
	store storage.Storage,
	tx storage.Transaction,
	table storage.TableID,
	rows ...Row,
) *Insert {
	cloned := make([]Row, 0, len(rows))
	for _, row := range rows {
		cloned = append(cloned, row.Clone())
	}

	return &Insert{
		store:  store,
		tx:     tx,
		table:  table,
		source: insertSourceValues,
		rows:   cloned,
	}
}

// NewInsertFromChild constructs an INSERT operator that reads inserted rows
// from one child operator.
func NewInsertFromChild(
	store storage.Storage,
	tx storage.Transaction,
	table storage.TableID,
	child Operator,
) *Insert {
	return &Insert{
		store:  store,
		tx:     tx,
		table:  table,
		source: insertSourceChild,
		child:  child,
	}
}

// AffectedRows reports how many inserts have succeeded so far.
func (i *Insert) AffectedRows() int64 {
	if i == nil {
		return 0
	}

	return i.affected
}

// Open prepares the insert source for iteration.
func (i *Insert) Open() error {
	if err := i.lifecycle.Open(); err != nil {
		return err
	}
	if i.store == nil {
		i.lifecycle = lifecycle{}
		return errWriteNilStore
	}
	if i.source == insertSourceChild {
		if i.child == nil {
			i.lifecycle = lifecycle{}
			return errInsertNilChild
		}
		if err := i.child.Open(); err != nil {
			i.lifecycle = lifecycle{}
			return err
		}

		i.childOpen = true
	}

	return nil
}

// Next inserts one row and returns the inserted output row.
func (i *Insert) Next() (Row, error) {
	if err := i.lifecycle.Next(); err != nil {
		return Row{}, err
	}
	if i.terminalErr != nil {
		return Row{}, i.terminalErr
	}
	if i.done {
		return Row{}, io.EOF
	}

	switch i.source {
	case insertSourceValues:
		if i.nextIndex >= len(i.rows) {
			i.done = true
			return Row{}, io.EOF
		}

		row := i.rows[i.nextIndex].Clone()
		handle, err := i.store.Insert(i.tx, i.table, row.StorageRow())
		if err != nil {
			i.terminalErr = err
			return Row{}, err
		}

		i.nextIndex++
		i.affected++
		row.Handle = handle

		return row, nil
	case insertSourceChild:
		row, err := i.child.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				i.done = true
			} else {
				i.terminalErr = err
			}

			return Row{}, err
		}

		handle, err := i.store.Insert(i.tx, i.table, row.StorageRow())
		if err != nil {
			i.terminalErr = err
			return Row{}, err
		}

		out := row.Clone()
		out.Handle = handle
		i.affected++

		return out, nil
	default:
		i.done = true
		return Row{}, io.EOF
	}
}

// Close closes the child source if one was opened and terminally closes the
// insert operator.
func (i *Insert) Close() error {
	child := i.child
	childOpen := i.childOpen
	i.childOpen = false

	if err := i.lifecycle.Close(); err != nil {
		return err
	}
	if !childOpen || child == nil {
		return nil
	}

	return child.Close()
}

// UpdateAssignment describes one UPDATE SET assignment group.
//
// Ordinals identify zero-based target-table columns inside the leading
// targetColumnCount values of each child row. Values are evaluated against the
// original child row, not the partially updated output row.
type UpdateAssignment struct {
	Ordinals []int
	Values   []CompiledExpr
}

// Update is the executor-side UPDATE operator.
//
// The child row must carry the target-table storage handle and expose the
// current target row in its first targetColumnCount values. Any trailing child
// values are preserved in the emitted row so later RETURNING-style operators
// can still see source-side context, but only the leading target prefix is
// written back to storage.
type Update struct {
	lifecycle         lifecycle
	store             storage.Storage
	tx                storage.Transaction
	table             storage.TableID
	targetColumnCount int
	assignments       []UpdateAssignment
	child             Operator
	childOpen         bool
	affected          int64
	done              bool
	terminalErr       error
}

var _ WriteOperator = (*Update)(nil)

// NewUpdate constructs an UPDATE operator over one child stream of target
// rows.
func NewUpdate(
	store storage.Storage,
	tx storage.Transaction,
	table storage.TableID,
	targetColumnCount int,
	child Operator,
	assignments ...UpdateAssignment,
) *Update {
	return &Update{
		store:             store,
		tx:                tx,
		table:             table,
		targetColumnCount: targetColumnCount,
		assignments:       cloneUpdateAssignments(assignments),
		child:             child,
	}
}

// AffectedRows reports how many updates have succeeded so far.
func (u *Update) AffectedRows() int64 {
	if u == nil {
		return 0
	}

	return u.affected
}

// Open validates the update contract and prepares the child stream.
func (u *Update) Open() error {
	if err := u.lifecycle.Open(); err != nil {
		return err
	}
	if err := u.validate(); err != nil {
		u.lifecycle = lifecycle{}
		return err
	}
	if err := u.child.Open(); err != nil {
		u.lifecycle = lifecycle{}
		return err
	}

	u.childOpen = true

	return nil
}

// Next updates one row and returns the updated output row.
func (u *Update) Next() (Row, error) {
	if err := u.lifecycle.Next(); err != nil {
		return Row{}, err
	}
	if u.terminalErr != nil {
		return Row{}, u.terminalErr
	}
	if u.done {
		return Row{}, io.EOF
	}

	input, err := u.child.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			u.done = true
		} else {
			u.terminalErr = err
		}

		return Row{}, err
	}
	if !input.Handle.Valid() {
		u.terminalErr = errWriteMissingHandle
		return Row{}, errWriteMissingHandle
	}

	values := input.Values()
	if len(values) < u.targetColumnCount {
		err := fmt.Errorf(
			"%w: have %d values, need %d",
			errUpdateRowTooShort,
			len(values),
			u.targetColumnCount,
		)
		u.terminalErr = err
		return Row{}, err
	}

	for _, assignment := range u.assignments {
		for index, ordinal := range assignment.Ordinals {
			value, err := assignment.Values[index].Eval(input)
			if err != nil {
				u.terminalErr = err
				return Row{}, err
			}
			values[ordinal] = value
		}
	}

	if err := u.store.Update(
		u.tx,
		u.table,
		input.Handle,
		storage.NewRow(values[:u.targetColumnCount]...),
	); err != nil {
		u.terminalErr = err
		return Row{}, err
	}

	u.affected++

	return Row{
		Handle: input.Handle,
		values: values,
	}, nil
}

// Close closes the child stream if one was opened and terminally closes the
// update operator.
func (u *Update) Close() error {
	child := u.child
	childOpen := u.childOpen
	u.childOpen = false

	if err := u.lifecycle.Close(); err != nil {
		return err
	}
	if !childOpen || child == nil {
		return nil
	}

	return child.Close()
}

func (u *Update) validate() error {
	switch {
	case u.store == nil:
		return errWriteNilStore
	case u.child == nil:
		return errUpdateNilChild
	case u.targetColumnCount < 0:
		return errUpdateNegativeTargetCount
	}

	for _, assignment := range u.assignments {
		if len(assignment.Ordinals) != len(assignment.Values) {
			return errUpdateAssignmentShape
		}

		for _, ordinal := range assignment.Ordinals {
			if ordinal < 0 || ordinal >= u.targetColumnCount {
				return fmt.Errorf(
					"%w: ordinal %d for target size %d",
					errUpdateTargetOrdinalOutOfRange,
					ordinal,
					u.targetColumnCount,
				)
			}
		}
	}

	return nil
}

func cloneUpdateAssignments(assignments []UpdateAssignment) []UpdateAssignment {
	if len(assignments) == 0 {
		return nil
	}

	out := make([]UpdateAssignment, len(assignments))
	for index, assignment := range assignments {
		out[index] = UpdateAssignment{
			Ordinals: append([]int(nil), assignment.Ordinals...),
			Values:   append([]CompiledExpr(nil), assignment.Values...),
		}
	}

	return out
}

// Delete is the executor-side DELETE operator.
//
// Each child row must carry the target-table storage handle. Next deletes one
// row per call and returns the deleted child row unchanged.
type Delete struct {
	lifecycle   lifecycle
	store       storage.Storage
	tx          storage.Transaction
	table       storage.TableID
	child       Operator
	childOpen   bool
	affected    int64
	done        bool
	terminalErr error
}

var _ WriteOperator = (*Delete)(nil)

// NewDelete constructs a DELETE operator over one child stream of target rows.
func NewDelete(
	store storage.Storage,
	tx storage.Transaction,
	table storage.TableID,
	child Operator,
) *Delete {
	return &Delete{
		store: store,
		tx:    tx,
		table: table,
		child: child,
	}
}

// AffectedRows reports how many deletes have succeeded so far.
func (d *Delete) AffectedRows() int64 {
	if d == nil {
		return 0
	}

	return d.affected
}

// Open validates the delete contract and prepares the child stream.
func (d *Delete) Open() error {
	if err := d.lifecycle.Open(); err != nil {
		return err
	}
	switch {
	case d.store == nil:
		d.lifecycle = lifecycle{}
		return errWriteNilStore
	case d.child == nil:
		d.lifecycle = lifecycle{}
		return errDeleteNilChild
	}

	if err := d.child.Open(); err != nil {
		d.lifecycle = lifecycle{}
		return err
	}

	d.childOpen = true

	return nil
}

// Next deletes one row and returns the deleted child row.
func (d *Delete) Next() (Row, error) {
	if err := d.lifecycle.Next(); err != nil {
		return Row{}, err
	}
	if d.terminalErr != nil {
		return Row{}, d.terminalErr
	}
	if d.done {
		return Row{}, io.EOF
	}

	input, err := d.child.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			d.done = true
		} else {
			d.terminalErr = err
		}

		return Row{}, err
	}
	if !input.Handle.Valid() {
		d.terminalErr = errWriteMissingHandle
		return Row{}, errWriteMissingHandle
	}
	if err := d.store.Delete(d.tx, d.table, input.Handle); err != nil {
		d.terminalErr = err
		return Row{}, err
	}

	d.affected++

	return input.Clone(), nil
}

// Close closes the child stream if one was opened and terminally closes the
// delete operator.
func (d *Delete) Close() error {
	child := d.child
	childOpen := d.childOpen
	d.childOpen = false

	if err := d.lifecycle.Close(); err != nil {
		return err
	}
	if !childOpen || child == nil {
		return nil
	}

	return child.Close()
}
