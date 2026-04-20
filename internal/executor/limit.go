package executor

import (
	"errors"
	"io"
)

var errLimitNilInput = errors.New("executor: limit operator has nil input")

// Limit is the Phase 1 row-count truncation operator.
//
// It preserves each child row, including its storage handle, unchanged when
// the row passes through the operator.
type Limit struct {
	lifecycle lifecycle
	input     Operator
	count     uint64
	offset    uint64
	emitted   uint64
	skipped   uint64
	childOpen bool
	exhausted bool
}

var _ Operator = (*Limit)(nil)

// NewLimit constructs an executor-native row-count limit over one child
// operator.
func NewLimit(input Operator, count uint64) *Limit {
	return NewLimitWithOffset(input, count, 0)
}

// NewLimitWithOffset constructs an executor-native limit with an optional
// leading row offset. The current planner surface only uses the row-count
// limit, but the executor supports offset without depending on planner types.
func NewLimitWithOffset(input Operator, count uint64, offset uint64) *Limit {
	return &Limit{
		input:  input,
		count:  count,
		offset: offset,
	}
}

// Open prepares the child operator for iteration.
func (l *Limit) Open() error {
	if err := l.lifecycle.Open(); err != nil {
		return err
	}
	if l.input == nil {
		l.lifecycle = lifecycle{}

		return errLimitNilInput
	}
	if err := l.input.Open(); err != nil {
		// Roll back the optimistic lifecycle transition so callers can observe
		// the original Open failure without the operator becoming terminal.
		l.lifecycle = lifecycle{}

		return err
	}

	l.childOpen = true
	l.emitted = 0
	l.skipped = 0
	l.exhausted = false

	return nil
}

// Next returns the next row that survives the limit and optional offset.
func (l *Limit) Next() (Row, error) {
	if err := l.lifecycle.Next(); err != nil {
		return Row{}, err
	}
	if l.exhausted || l.emitted >= l.count {
		l.exhausted = true

		return Row{}, io.EOF
	}

	for l.skipped < l.offset {
		if _, err := l.input.Next(); err != nil {
			if errors.Is(err, io.EOF) {
				l.exhausted = true
			}

			return Row{}, err
		}

		l.skipped++
	}

	row, err := l.input.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			l.exhausted = true
		}

		return Row{}, err
	}

	l.emitted++

	return row, nil
}

// Close releases the child operator and terminally closes the limit.
func (l *Limit) Close() error {
	input := l.input
	childOpen := l.childOpen
	l.childOpen = false

	if err := l.lifecycle.Close(); err != nil {
		return err
	}
	if !childOpen || input == nil {
		return nil
	}

	return input.Close()
}
