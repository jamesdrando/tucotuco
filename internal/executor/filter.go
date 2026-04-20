package executor

// Filter is the Phase 1 row-filtering operator.
type Filter struct {
	lifecycle lifecycle
	child     Operator
	predicate CompiledExpr
	childOpen bool
}

var _ Operator = (*Filter)(nil)

// NewFilter constructs a filter over one child operator and compiled
// predicate.
func NewFilter(child Operator, predicate CompiledExpr) *Filter {
	return &Filter{
		child:     child,
		predicate: predicate,
	}
}

// Open prepares the child operator for filtered iteration.
func (f *Filter) Open() error {
	if err := f.lifecycle.Open(); err != nil {
		return err
	}

	if err := f.child.Open(); err != nil {
		// Roll back the optimistic lifecycle transition so callers can retry
		// after observing the original Open failure.
		f.lifecycle = lifecycle{}

		return err
	}

	f.childOpen = true

	return nil
}

// Next returns the next child row whose predicate evaluates to SQL TRUE.
func (f *Filter) Next() (Row, error) {
	if err := f.lifecycle.Next(); err != nil {
		return Row{}, err
	}

	for {
		row, err := f.child.Next()
		if err != nil {
			return Row{}, err
		}

		value, err := f.predicate.Eval(row)
		if err != nil {
			return Row{}, err
		}

		truth, err := sqlBoolFromValue(value)
		if err != nil {
			return Row{}, err
		}
		if truth == sqlTrue {
			return row, nil
		}
	}
}

// Close closes the child operator if it was opened and terminally closes the
// filter.
func (f *Filter) Close() error {
	child := f.child
	childOpen := f.childOpen
	f.childOpen = false

	if err := f.lifecycle.Close(); err != nil {
		return err
	}
	if !childOpen {
		return nil
	}

	return child.Close()
}
