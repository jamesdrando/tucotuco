package executor

import (
	"errors"

	"github.com/jamesdrando/tucotuco/internal/types"
)

var errProjectNilChild = errors.New("executor: project child is nil")

// Project is the Phase 1 row-preserving projection operator.
type Project struct {
	lifecycle lifecycle
	child     Operator
	exprs     []CompiledExpr
	childOpen bool
}

var _ Operator = (*Project)(nil)

// NewProject constructs a projection over one child operator.
func NewProject(child Operator, exprs ...CompiledExpr) *Project {
	return &Project{
		child: child,
		exprs: append([]CompiledExpr(nil), exprs...),
	}
}

// Open prepares the child operator used to feed projection rows.
func (p *Project) Open() error {
	if err := p.lifecycle.Open(); err != nil {
		return err
	}
	if p.child == nil {
		p.lifecycle = lifecycle{}
		return errProjectNilChild
	}
	if err := p.child.Open(); err != nil {
		// Roll back the optimistic lifecycle transition so callers can observe
		// the original Open failure without the operator becoming terminal.
		p.lifecycle = lifecycle{}
		return err
	}

	p.childOpen = true

	return nil
}

// Next evaluates the projection expressions for one input row.
func (p *Project) Next() (Row, error) {
	if err := p.lifecycle.Next(); err != nil {
		return Row{}, err
	}

	input, err := p.child.Next()
	if err != nil {
		return Row{}, err
	}

	values := make([]types.Value, len(p.exprs))
	for index, expr := range p.exprs {
		value, err := expr.Eval(input)
		if err != nil {
			return Row{}, err
		}
		values[index] = value
	}

	return Row{
		Handle: input.Handle,
		values: values,
	}, nil
}

// Close releases the child operator and terminally closes the projection.
func (p *Project) Close() error {
	child := p.child
	childOpen := p.childOpen
	p.childOpen = false

	if err := p.lifecycle.Close(); err != nil {
		return err
	}
	if !childOpen || child == nil {
		return nil
	}

	return child.Close()
}
