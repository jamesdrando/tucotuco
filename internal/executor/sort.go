package executor

import (
	"errors"
	"io"

	"github.com/jamesdrando/tucotuco/internal/types"
)

var errSortNilInput = errors.New("executor: sort input is nil")

// SortDirection describes how one sort key orders non-NULL values.
type SortDirection uint8

const (
	// SortAscending orders lower values before higher values.
	SortAscending SortDirection = iota
	// SortDescending orders higher values before lower values.
	SortDescending
)

// SortNullsOrder describes how one sort key orders SQL NULL values relative to
// non-NULL values.
type SortNullsOrder uint8

const (
	// SortNullsDefault uses the operator's default NULL placement:
	// `NULLS LAST` for ascending keys and `NULLS FIRST` for descending keys.
	SortNullsDefault SortNullsOrder = iota
	// SortNullsFirst places NULL values before all non-NULL values.
	SortNullsFirst
	// SortNullsLast places NULL values after all non-NULL values.
	SortNullsLast
)

// SortKey configures one executor-native ordering key.
type SortKey struct {
	Expr      CompiledExpr
	Direction SortDirection
	Nulls     SortNullsOrder
}

// Sort is the Phase 1 stable blocking sort operator.
//
// The operator preserves each child row's storage handle and row values while
// ordering rows by one or more compiled sort keys.
type Sort struct {
	lifecycle lifecycle
	input     Operator
	keys      []SortKey
	rows      []materializedSortRow
	index     int

	childOpen      bool
	materialized   bool
	materializeErr error
}

type materializedSortRow struct {
	row  Row
	keys []types.Value
}

var _ Operator = (*Sort)(nil)

// NewSort constructs a stable executor-native sort over one child operator.
func NewSort(input Operator, keys ...SortKey) *Sort {
	return &Sort{
		input: input,
		keys:  append([]SortKey(nil), keys...),
	}
}

// Open prepares the child operator for later materialization and sorting.
func (s *Sort) Open() error {
	if err := s.lifecycle.Open(); err != nil {
		return err
	}
	if s.input == nil {
		s.lifecycle = lifecycle{}

		return errSortNilInput
	}
	if err := s.input.Open(); err != nil {
		// Roll back the optimistic lifecycle transition so callers can observe
		// the original Open failure without the operator becoming terminal.
		s.lifecycle = lifecycle{}

		return err
	}

	s.childOpen = true
	s.rows = nil
	s.index = 0
	s.materialized = false
	s.materializeErr = nil

	return nil
}

// Next returns rows in stable sorted order.
func (s *Sort) Next() (Row, error) {
	if err := s.lifecycle.Next(); err != nil {
		return Row{}, err
	}
	if !s.materialized {
		if s.materializeErr != nil {
			return Row{}, s.materializeErr
		}
		if err := s.materialize(); err != nil {
			s.materializeErr = err

			return Row{}, err
		}

		s.materialized = true
	}
	if s.index >= len(s.rows) {
		return Row{}, io.EOF
	}

	row := s.rows[s.index].row.Clone()
	s.index++

	return row, nil
}

// Close releases the child operator and terminally closes the sort.
func (s *Sort) Close() error {
	input := s.input
	childOpen := s.childOpen

	s.childOpen = false
	s.rows = nil
	s.index = 0
	s.materialized = false
	s.materializeErr = nil

	if err := s.lifecycle.Close(); err != nil {
		return err
	}
	if !childOpen || input == nil {
		return nil
	}

	return input.Close()
}

func (s *Sort) materialize() error {
	rows := make([]materializedSortRow, 0)

	for {
		row, err := s.input.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return err
		}

		keys := make([]types.Value, len(s.keys))
		for index, key := range s.keys {
			value, err := key.Expr.Eval(row)
			if err != nil {
				return err
			}
			keys[index] = value
		}

		rows = append(rows, materializedSortRow{
			row:  row.Clone(),
			keys: keys,
		})
	}

	if err := stableSortRows(rows, s.compareRows); err != nil {
		return err
	}

	s.rows = rows
	s.index = 0

	return nil
}

func (s *Sort) compareRows(left, right materializedSortRow) (int, error) {
	for index, key := range s.keys {
		comparison, err := compareSortValues(
			left.keys[index],
			right.keys[index],
			key.Direction,
			key.resolvedNullsOrder(),
		)
		if err != nil {
			return 0, err
		}
		if comparison != 0 {
			return comparison, nil
		}
	}

	return 0, nil
}

func (k SortKey) resolvedNullsOrder() SortNullsOrder {
	switch k.Nulls {
	case SortNullsFirst, SortNullsLast:
		return k.Nulls
	default:
		if k.Direction == SortDescending {
			return SortNullsFirst
		}

		return SortNullsLast
	}
}

func compareSortValues(
	left types.Value,
	right types.Value,
	direction SortDirection,
	nulls SortNullsOrder,
) (int, error) {
	switch {
	case left.IsNull() && right.IsNull():
		return 0, nil
	case left.IsNull():
		if nulls == SortNullsFirst {
			return -1, nil
		}

		return 1, nil
	case right.IsNull():
		if nulls == SortNullsFirst {
			return 1, nil
		}

		return -1, nil
	}

	comparison, err := left.Compare(right)
	if err != nil {
		return 0, err
	}
	if direction == SortDescending {
		comparison = -comparison
	}

	return comparison, nil
}

func stableSortRows(
	rows []materializedSortRow,
	compare func(left, right materializedSortRow) (int, error),
) error {
	if len(rows) < 2 {
		return nil
	}

	scratch := make([]materializedSortRow, len(rows))

	return stableSortRowsInto(rows, scratch, compare)
}

func stableSortRowsInto(
	rows []materializedSortRow,
	scratch []materializedSortRow,
	compare func(left, right materializedSortRow) (int, error),
) error {
	if len(rows) < 2 {
		return nil
	}

	middle := len(rows) / 2
	if err := stableSortRowsInto(rows[:middle], scratch[:middle], compare); err != nil {
		return err
	}
	if err := stableSortRowsInto(rows[middle:], scratch[middle:], compare); err != nil {
		return err
	}

	copy(scratch, rows)

	left := scratch[:middle]
	right := scratch[middle:]

	leftIndex := 0
	rightIndex := 0
	writeIndex := 0

	for leftIndex < len(left) && rightIndex < len(right) {
		comparison, err := compare(left[leftIndex], right[rightIndex])
		if err != nil {
			return err
		}

		if comparison <= 0 {
			rows[writeIndex] = left[leftIndex]
			leftIndex++
		} else {
			rows[writeIndex] = right[rightIndex]
			rightIndex++
		}

		writeIndex++
	}

	writeIndex += copy(rows[writeIndex:], left[leftIndex:])
	copy(rows[writeIndex:], right[rightIndex:])

	return nil
}
