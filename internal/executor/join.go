package executor

import (
	"errors"
	"fmt"
	"io"

	"github.com/jamesdrando/tucotuco/internal/types"
)

var (
	errJoinNilLeftChild          = errors.New("executor: join left child is nil")
	errJoinNilRightChild         = errors.New("executor: join right child is nil")
	errJoinNegativeLeftColumns   = errors.New("executor: join left column count must be non-negative")
	errJoinNegativeRightColumns  = errors.New("executor: join right column count must be non-negative")
	errJoinUnsupportedType       = errors.New("executor: unsupported join type")
	errJoinLeftRowWidthMismatch  = errors.New("executor: join left row width mismatch")
	errJoinRightRowWidthMismatch = errors.New("executor: join right row width mismatch")
)

// JoinType identifies one executor-native nested-loop join shape.
type JoinType uint8

const (
	// JoinInvalid is the zero-value sentinel for an unsupported join type.
	JoinInvalid JoinType = iota
	// JoinInner emits only rows whose join predicate evaluates to SQL TRUE.
	JoinInner
	// JoinCross emits the cartesian product unless an optional predicate is
	// supplied, in which case it behaves like a filtered cartesian product.
	JoinCross
	// JoinLeft preserves unmatched left rows with NULL-extended right columns.
	JoinLeft
	// JoinRight preserves unmatched right rows with NULL-extended left columns.
	JoinRight
	// JoinFull preserves unmatched rows from both join inputs.
	JoinFull
)

// String returns the stable display name for the join type.
func (t JoinType) String() string {
	switch t {
	case JoinInvalid:
		return "INVALID"
	case JoinInner:
		return "INNER"
	case JoinCross:
		return "CROSS"
	case JoinLeft:
		return "LEFT"
	case JoinRight:
		return "RIGHT"
	case JoinFull:
		return "FULL"
	}

	return "INVALID"
}

// NestedLoopJoin is the executor-native serial nested-loop join operator.
//
// The operator streams the left input and materializes the entire right input
// on the first Next call. Output rows are synthetic, preserve SQL column order
// as left columns followed by right columns, and therefore always carry the
// zero storage handle.
type NestedLoopJoin struct {
	lifecycle lifecycle

	left  Operator
	right Operator

	kind         JoinType
	leftColumns  int
	rightColumns int
	on           *CompiledExpr

	leftOpen  bool
	rightOpen bool

	materialized   bool
	materializeErr error
	rightRows      []Row
	rightMatched   []bool

	leftCurrent    Row
	leftHasCurrent bool
	leftMatched    bool
	leftExhausted  bool
	rightIndex     int

	unmatchedRightIndex int
}

var _ Operator = (*NestedLoopJoin)(nil)

// NewNestedLoopJoin constructs a nested-loop join over two child operators.
//
// leftColumns and rightColumns describe the expected row widths of the child
// operators so outer-join NULL extension can preserve stable output shape even
// when one side is empty.
func NewNestedLoopJoin(
	kind JoinType,
	left Operator,
	right Operator,
	leftColumns int,
	rightColumns int,
	on *CompiledExpr,
) *NestedLoopJoin {
	return &NestedLoopJoin{
		left:         left,
		right:        right,
		kind:         kind,
		leftColumns:  leftColumns,
		rightColumns: rightColumns,
		on:           on,
	}
}

// Open prepares the streaming left input. The right input is opened lazily
// when the operator first materializes it.
func (j *NestedLoopJoin) Open() error {
	if err := j.lifecycle.Open(); err != nil {
		return err
	}
	if err := j.validateConfig(); err != nil {
		j.lifecycle = lifecycle{}
		return err
	}
	if err := j.left.Open(); err != nil {
		j.lifecycle = lifecycle{}
		return err
	}

	j.leftOpen = true
	j.resetState()

	return nil
}

// Next returns the next joined row.
func (j *NestedLoopJoin) Next() (Row, error) {
	if err := j.lifecycle.Next(); err != nil {
		return Row{}, err
	}
	if !j.materialized {
		if j.materializeErr != nil {
			return Row{}, j.materializeErr
		}
		if err := j.materializeRight(); err != nil {
			j.materializeErr = err
			return Row{}, err
		}

		j.materialized = true
	}

	for {
		if j.leftHasCurrent {
			for j.rightIndex < len(j.rightRows) {
				rightIndex := j.rightIndex
				rightRow := j.rightRows[rightIndex]
				j.rightIndex++

				matched, err := j.matches(j.leftCurrent, rightRow)
				if err != nil {
					return Row{}, err
				}
				if !matched {
					continue
				}

				j.leftMatched = true
				if len(j.rightMatched) != 0 {
					j.rightMatched[rightIndex] = true
				}

				return joinedRow(j.leftCurrent, rightRow), nil
			}

			if !j.leftMatched && j.preservesUnmatchedLeft() {
				row := nullExtendedRightRow(j.leftCurrent, j.rightColumns)
				j.clearLeftCurrent()

				return row, nil
			}

			j.clearLeftCurrent()
			continue
		}

		if !j.leftExhausted {
			row, err := j.left.Next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					j.leftExhausted = true
					continue
				}

				return Row{}, err
			}
			if err := validateJoinRowWidth(row, j.leftColumns, errJoinLeftRowWidthMismatch); err != nil {
				return Row{}, err
			}

			j.leftCurrent = row.Clone()
			j.leftHasCurrent = true
			j.leftMatched = false
			j.rightIndex = 0

			continue
		}

		if j.preservesUnmatchedRight() {
			for j.unmatchedRightIndex < len(j.rightRows) {
				rightIndex := j.unmatchedRightIndex
				j.unmatchedRightIndex++
				if j.rightMatched[rightIndex] {
					continue
				}

				return nullExtendedLeftRow(j.leftColumns, j.rightRows[rightIndex]), nil
			}
		}

		return Row{}, io.EOF
	}
}

// Close releases any opened child operators and terminally closes the join.
func (j *NestedLoopJoin) Close() error {
	left := j.left
	right := j.right
	leftOpen := j.leftOpen
	rightOpen := j.rightOpen

	j.leftOpen = false
	j.rightOpen = false
	j.resetState()

	if err := j.lifecycle.Close(); err != nil {
		return err
	}

	var closeErr error
	if rightOpen && right != nil {
		closeErr = errors.Join(closeErr, right.Close())
	}
	if leftOpen && left != nil {
		closeErr = errors.Join(closeErr, left.Close())
	}

	return closeErr
}

func (j *NestedLoopJoin) validateConfig() error {
	switch {
	case j.left == nil:
		return errJoinNilLeftChild
	case j.right == nil:
		return errJoinNilRightChild
	case j.leftColumns < 0:
		return errJoinNegativeLeftColumns
	case j.rightColumns < 0:
		return errJoinNegativeRightColumns
	case !j.kind.valid():
		return fmt.Errorf("%w: %s", errJoinUnsupportedType, j.kind)
	default:
		return nil
	}
}

func (j *NestedLoopJoin) materializeRight() error {
	if !j.rightOpen {
		if err := j.right.Open(); err != nil {
			return err
		}
		j.rightOpen = true
	}

	rows := make([]Row, 0)
	for {
		row, err := j.right.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return err
		}
		if err := validateJoinRowWidth(row, j.rightColumns, errJoinRightRowWidthMismatch); err != nil {
			return err
		}

		rows = append(rows, row.Clone())
	}

	j.rightRows = rows
	if j.preservesUnmatchedRight() {
		j.rightMatched = make([]bool, len(rows))
	} else {
		j.rightMatched = nil
	}
	j.rightIndex = 0
	j.unmatchedRightIndex = 0

	return nil
}

func (j *NestedLoopJoin) matches(left Row, right Row) (bool, error) {
	if j.on == nil {
		return true, nil
	}

	value, err := j.on.Eval(predicateJoinRow(left, right))
	if err != nil {
		return false, err
	}

	truth, err := sqlBoolFromValue(value)
	if err != nil {
		return false, err
	}

	return truth == sqlTrue, nil
}

func (j *NestedLoopJoin) resetState() {
	j.materialized = false
	j.materializeErr = nil
	j.rightRows = nil
	j.rightMatched = nil
	j.clearLeftCurrent()
	j.leftExhausted = false
	j.unmatchedRightIndex = 0
}

func (j *NestedLoopJoin) clearLeftCurrent() {
	j.leftCurrent = Row{}
	j.leftHasCurrent = false
	j.leftMatched = false
	j.rightIndex = 0
}

func (j *NestedLoopJoin) preservesUnmatchedLeft() bool {
	return j.kind == JoinLeft || j.kind == JoinFull
}

func (j *NestedLoopJoin) preservesUnmatchedRight() bool {
	return j.kind == JoinRight || j.kind == JoinFull
}

func (t JoinType) valid() bool {
	switch t {
	case JoinInner, JoinCross, JoinLeft, JoinRight, JoinFull:
		return true
	default:
		return false
	}
}

func validateJoinRowWidth(row Row, want int, kind error) error {
	if row.Len() != want {
		return fmt.Errorf("%w: expected %d columns, found %d", kind, want, row.Len())
	}

	return nil
}

func predicateJoinRow(left Row, right Row) Row {
	values := make([]types.Value, 0, left.Len()+right.Len())
	values = append(values, left.values...)
	values = append(values, right.values...)

	return Row{values: values}
}

func joinedRow(left Row, right Row) Row {
	values := make([]types.Value, 0, left.Len()+right.Len())
	values = append(values, left.values...)
	values = append(values, right.values...)

	return Row{values: values}
}

func nullExtendedRightRow(left Row, rightColumns int) Row {
	values := make([]types.Value, 0, left.Len()+rightColumns)
	values = append(values, left.values...)
	values = append(values, joinNullValues(rightColumns)...)

	return Row{values: values}
}

func nullExtendedLeftRow(leftColumns int, right Row) Row {
	values := make([]types.Value, 0, leftColumns+right.Len())
	values = append(values, joinNullValues(leftColumns)...)
	values = append(values, right.values...)

	return Row{values: values}
}

func joinNullValues(count int) []types.Value {
	values := make([]types.Value, count)
	for index := range values {
		values[index] = types.NullValue()
	}

	return values
}
