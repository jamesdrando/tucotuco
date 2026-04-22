package executor

import (
	"errors"
	"fmt"
	"io"
	"strings"

	sqltypes "github.com/jamesdrando/tucotuco/internal/types"
)

var (
	errSetOpNilChild       = errors.New("executor: set operation child is nil")
	errUnsupportedSetOp    = errors.New("executor: unsupported set operation")
	errSetOpColumnMismatch = errors.New("executor: set operation column count mismatch")
)

// SetOp is the Phase 2 executor-native operator for UNION / INTERSECT / EXCEPT.
//
// The operator materializes both child inputs on first Next(), coercing each
// child row to the analyzer-selected output types before applying ALL or
// DISTINCT semantics. Output rows are synthetic and do not preserve child
// storage handles.
type SetOp struct {
	lifecycle lifecycle

	left          Operator
	right         Operator
	operator      string
	setQuantifier string
	leftTypes     []sqltypes.TypeDesc
	rightTypes    []sqltypes.TypeDesc
	outputTypes   []sqltypes.TypeDesc

	results        []Row
	index          int
	leftOpen       bool
	rightOpen      bool
	materialized   bool
	materializeErr error
}

var _ Operator = (*SetOp)(nil)

// NewSetOp constructs an executor-native set-operation operator.
func NewSetOp(
	left Operator,
	right Operator,
	operator string,
	setQuantifier string,
	leftTypes []sqltypes.TypeDesc,
	rightTypes []sqltypes.TypeDesc,
	outputTypes []sqltypes.TypeDesc,
) *SetOp {
	return &SetOp{
		left:          left,
		right:         right,
		operator:      strings.ToUpper(strings.TrimSpace(operator)),
		setQuantifier: strings.ToUpper(strings.TrimSpace(setQuantifier)),
		leftTypes:     append([]sqltypes.TypeDesc(nil), leftTypes...),
		rightTypes:    append([]sqltypes.TypeDesc(nil), rightTypes...),
		outputTypes:   append([]sqltypes.TypeDesc(nil), outputTypes...),
	}
}

// Open prepares both child operators for later materialization.
func (s *SetOp) Open() error {
	if err := s.lifecycle.Open(); err != nil {
		return err
	}
	if s.left == nil || s.right == nil {
		s.lifecycle = lifecycle{}
		return errSetOpNilChild
	}
	if err := validateSetOp(s.operator, s.setQuantifier); err != nil {
		s.lifecycle = lifecycle{}
		return err
	}
	if err := s.left.Open(); err != nil {
		s.lifecycle = lifecycle{}
		return err
	}
	s.leftOpen = true
	if err := s.right.Open(); err != nil {
		closeErr := s.left.Close()
		s.leftOpen = false
		s.lifecycle = lifecycle{}
		return errors.Join(err, closeErr)
	}
	s.rightOpen = true

	s.results = nil
	s.index = 0
	s.materialized = false
	s.materializeErr = nil

	return nil
}

// Next returns the next set-operation output row.
func (s *SetOp) Next() (Row, error) {
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
	if s.index >= len(s.results) {
		return Row{}, io.EOF
	}

	row := s.results[s.index].Clone()
	s.index++
	return row, nil
}

// Close releases both child operators and terminally closes the set operator.
func (s *SetOp) Close() error {
	left := s.left
	right := s.right
	leftOpen := s.leftOpen
	rightOpen := s.rightOpen

	s.leftOpen = false
	s.rightOpen = false
	s.results = nil
	s.index = 0
	s.materialized = false
	s.materializeErr = nil

	if err := s.lifecycle.Close(); err != nil {
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

type setOpBucket struct {
	values []sqltypes.Value
	count  int
}

func (s *SetOp) materialize() error {
	leftRows, err := materializeSetOpInput(s.left, s.leftTypes, s.outputTypes)
	if err != nil {
		return err
	}
	rightRows, err := materializeSetOpInput(s.right, s.rightTypes, s.outputTypes)
	if err != nil {
		return err
	}

	switch s.operator {
	case "UNION":
		if s.setQuantifier == "ALL" {
			s.results = append(append([]Row(nil), leftRows...), rightRows...)
		} else {
			s.results = distinctSetOpRows(append(append([]Row(nil), leftRows...), rightRows...))
		}
	case "INTERSECT":
		rightBuckets, rightIndex := buildSetOpBuckets(rightRows)
		s.results = intersectSetOpRows(leftRows, rightBuckets, rightIndex, s.setQuantifier == "ALL")
	case "EXCEPT":
		rightBuckets, rightIndex := buildSetOpBuckets(rightRows)
		s.results = exceptSetOpRows(leftRows, rightBuckets, rightIndex, s.setQuantifier == "ALL")
	default:
		return fmt.Errorf("%w: %s", errUnsupportedSetOp, s.operator)
	}

	s.index = 0
	return nil
}

func validateSetOp(operator string, setQuantifier string) error {
	switch operator {
	case "UNION", "INTERSECT", "EXCEPT":
	default:
		return fmt.Errorf("%w: %s", errUnsupportedSetOp, operator)
	}

	switch setQuantifier {
	case "", "ALL", "DISTINCT":
		return nil
	default:
		return fmt.Errorf("%w: quantifier %s", errUnsupportedSetOp, setQuantifier)
	}
}

func materializeSetOpInput(input Operator, sourceTypes []sqltypes.TypeDesc, outputTypes []sqltypes.TypeDesc) ([]Row, error) {
	rows := make([]Row, 0)
	for {
		row, err := input.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return rows, nil
			}
			return nil, err
		}

		coerced, err := coerceSetOpRow(row, sourceTypes, outputTypes)
		if err != nil {
			return nil, err
		}
		rows = append(rows, coerced)
	}
}

func coerceSetOpRow(row Row, sourceTypes []sqltypes.TypeDesc, outputTypes []sqltypes.TypeDesc) (Row, error) {
	if len(outputTypes) != row.Len() {
		return Row{}, fmt.Errorf("%w: row has %d columns, output has %d", errSetOpColumnMismatch, row.Len(), len(outputTypes))
	}
	if len(sourceTypes) != 0 && len(sourceTypes) != row.Len() {
		return Row{}, fmt.Errorf("%w: row has %d columns, source has %d", errSetOpColumnMismatch, row.Len(), len(sourceTypes))
	}

	values := make([]sqltypes.Value, 0, row.Len())
	for index := 0; index < row.Len(); index++ {
		value, ok := row.Value(index)
		if !ok {
			return Row{}, fmt.Errorf("%w: missing column %d", errSetOpColumnMismatch, index)
		}

		target := outputTypes[index]
		if target.Kind == sqltypes.TypeKindInvalid {
			values = append(values, value)
			continue
		}

		source := target
		if len(sourceTypes) != 0 {
			source = sourceTypes[index]
		}
		if inferred, ok := sourceTypeForValue(value, source); ok {
			source = inferred
		}
		coerced, err := castRuntimeValue(value, source, target, false)
		if err != nil {
			return Row{}, err
		}
		values = append(values, coerced)
	}

	return NewRow(values...), nil
}

func distinctSetOpRows(rows []Row) []Row {
	buckets := make([]setOpBucket, 0)
	index := make(map[string][]int)
	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		values := row.Values()
		if _, ok := findSetOpBucket(index[aggregateGroupSignature(values)], buckets, values); ok {
			continue
		}
		buckets = append(buckets, setOpBucket{
			values: append([]sqltypes.Value(nil), values...),
			count:  1,
		})
		index[aggregateGroupSignature(values)] = append(index[aggregateGroupSignature(values)], len(buckets)-1)
		out = append(out, row.Clone())
	}

	return out
}

func intersectSetOpRows(leftRows []Row, rightBuckets []setOpBucket, rightIndex map[string][]int, all bool) []Row {
	out := make([]Row, 0)
	seenDistinct := make([]setOpBucket, 0)
	seenIndex := make(map[string][]int)

	for _, row := range leftRows {
		values := row.Values()
		index, ok := findSetOpBucket(rightIndex[aggregateGroupSignature(values)], rightBuckets, values)
		if !ok || rightBuckets[index].count == 0 {
			continue
		}
		if !all {
			if _, seen := findSetOpBucket(seenIndex[aggregateGroupSignature(values)], seenDistinct, values); seen {
				continue
			}
			seenDistinct = append(seenDistinct, setOpBucket{values: append([]sqltypes.Value(nil), values...), count: 1})
			seenIndex[aggregateGroupSignature(values)] = append(seenIndex[aggregateGroupSignature(values)], len(seenDistinct)-1)
		}

		rightBuckets[index].count--
		out = append(out, row.Clone())
	}

	return out
}

func exceptSetOpRows(leftRows []Row, rightBuckets []setOpBucket, rightIndex map[string][]int, all bool) []Row {
	out := make([]Row, 0)
	seenDistinct := make([]setOpBucket, 0)
	seenIndex := make(map[string][]int)

	for _, row := range leftRows {
		values := row.Values()
		index, ok := findSetOpBucket(rightIndex[aggregateGroupSignature(values)], rightBuckets, values)
		if ok && rightBuckets[index].count > 0 {
			rightBuckets[index].count--
			continue
		}
		if !all {
			if _, seen := findSetOpBucket(seenIndex[aggregateGroupSignature(values)], seenDistinct, values); seen {
				continue
			}
			seenDistinct = append(seenDistinct, setOpBucket{values: append([]sqltypes.Value(nil), values...), count: 1})
			seenIndex[aggregateGroupSignature(values)] = append(seenIndex[aggregateGroupSignature(values)], len(seenDistinct)-1)
		}

		out = append(out, row.Clone())
	}

	return out
}

func buildSetOpBuckets(rows []Row) ([]setOpBucket, map[string][]int) {
	buckets := make([]setOpBucket, 0)
	index := make(map[string][]int)
	for _, row := range rows {
		values := row.Values()
		signature := aggregateGroupSignature(values)
		bucketIndex, ok := findSetOpBucket(index[signature], buckets, values)
		if ok {
			buckets[bucketIndex].count++
			continue
		}

		buckets = append(buckets, setOpBucket{
			values: append([]sqltypes.Value(nil), values...),
			count:  1,
		})
		index[signature] = append(index[signature], len(buckets)-1)
	}

	return buckets, index
}

func findSetOpBucket(candidates []int, buckets []setOpBucket, values []sqltypes.Value) (int, bool) {
	for _, index := range candidates {
		if index < 0 || index >= len(buckets) {
			continue
		}
		if aggregateGroupValuesEqual(buckets[index].values, values) {
			return index, true
		}
	}

	return 0, false
}
