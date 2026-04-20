package embed

import (
	"io"
	"strconv"

	"github.com/jamesdrando/tucotuco/internal/analyzer"
	"github.com/jamesdrando/tucotuco/internal/executor"
	"github.com/jamesdrando/tucotuco/internal/parser"
	"github.com/jamesdrando/tucotuco/internal/planner"
	"github.com/jamesdrando/tucotuco/internal/storage"
	sqltypes "github.com/jamesdrando/tucotuco/internal/types"
)

type planLowerer struct {
	bindings *analyzer.Bindings
	types    *analyzer.Types
	store    storage.Storage
	tx       storage.Transaction
}

type loweredPlan struct {
	op      executor.Operator
	shape   rowShape
	columns []planner.Column
}

type rowShape struct {
	columns []shapeColumn
}

type shapeColumn struct {
	name string
	typ  sqltypes.TypeDesc
}

func buildSelectOperator(
	stmt *parser.SelectStmt,
	bindings *analyzer.Bindings,
	types *analyzer.Types,
	store storage.Storage,
	tx storage.Transaction,
) (executor.Operator, []planner.Column, error) {
	plan, diags := planner.NewBuilder(bindings, types).Build(stmt)
	if len(diags) != 0 {
		return nil, nil, diagnosticsError(diags)
	}

	lowered, err := (planLowerer{
		bindings: bindings,
		types:    types,
		store:    store,
		tx:       tx,
	}).lower(plan)
	if err != nil {
		return nil, nil, err
	}

	return lowered.op, lowered.columns, nil
}

func (l planLowerer) lower(plan planner.Plan) (loweredPlan, error) {
	switch node := plan.(type) {
	case nil:
		return loweredPlan{
			op:    &oneRowOperator{},
			shape: rowShape{},
		}, nil
	case *planner.Scan:
		return loweredPlan{
			op:      executor.NewSeqScan(l.store, l.tx, node.Table, storage.ScanOptions{}),
			shape:   shapeFromColumns(node.Columns()),
			columns: node.Columns(),
		}, nil
	case *planner.Filter:
		child, err := l.lower(node.Input)
		if err != nil {
			return loweredPlan{}, err
		}

		predicate, err := compileExpression(l.bindings, l.types, node.Predicate, child.shape)
		if err != nil {
			return loweredPlan{}, err
		}

		return loweredPlan{
			op:      executor.NewFilter(child.op, predicate),
			shape:   child.shape,
			columns: child.columns,
		}, nil
	case *planner.Project:
		child, err := l.lower(node.Input)
		if err != nil {
			return loweredPlan{}, err
		}

		exprs := make([]executor.CompiledExpr, 0, len(node.Projections))
		for _, projection := range node.Projections {
			expr, err := compileExpression(l.bindings, l.types, projection.Expr, child.shape)
			if err != nil {
				return loweredPlan{}, err
			}
			exprs = append(exprs, expr)
		}

		return loweredPlan{
			op:      executor.NewProject(child.op, exprs...),
			shape:   shapeFromColumns(node.Columns()),
			columns: node.Columns(),
		}, nil
	case *planner.Limit:
		child, err := l.lower(node.Input)
		if err != nil {
			return loweredPlan{}, err
		}

		count, err := evalLimitCount(node.Count)
		if err != nil {
			return loweredPlan{}, err
		}

		return loweredPlan{
			op:      executor.NewLimit(child.op, count),
			shape:   child.shape,
			columns: child.columns,
		}, nil
	default:
		return loweredPlan{}, featureError(nil, "unsupported logical plan %T", node)
	}
}

func shapeFromColumns(columns []planner.Column) rowShape {
	shape := rowShape{
		columns: make([]shapeColumn, 0, len(columns)),
	}
	for _, column := range columns {
		shape.columns = append(shape.columns, shapeColumn{
			name: column.Name,
			typ:  column.Type,
		})
	}

	return shape
}

func compileExpression(
	bindings *analyzer.Bindings,
	types *analyzer.Types,
	node parser.Node,
	shape rowShape,
) (executor.CompiledExpr, error) {
	return executor.CompileExpr(node, expressionMetadata{
		bindings: bindings,
		types:    types,
		shape:    shape,
	})
}

type expressionMetadata struct {
	bindings *analyzer.Bindings
	types    *analyzer.Types
	shape    rowShape
}

func (m expressionMetadata) TypeOf(node parser.Node) (sqltypes.TypeDesc, bool) {
	if m.types != nil {
		if desc, ok := m.types.Expr(node); ok {
			return desc, true
		}
	}

	if binding, ok := m.BindingOf(node); ok {
		return binding.Type, true
	}

	return sqltypes.TypeDesc{}, false
}

func (m expressionMetadata) BindingOf(node parser.Node) (executor.OrdinalBinding, bool) {
	if m.bindings != nil {
		if column, ok := m.bindings.Column(node); ok && column != nil {
			if binding, ok := m.shape.bindingForName(column.Name); ok {
				return binding, true
			}
		}
	}

	switch node := node.(type) {
	case *parser.Identifier:
		return m.shape.bindingForName(node.Name)
	case *parser.QualifiedName:
		name := qualifiedNameTail(node)
		if name == "" {
			return executor.OrdinalBinding{}, false
		}
		return m.shape.bindingForName(name)
	default:
		return executor.OrdinalBinding{}, false
	}
}

func (s rowShape) bindingForName(name string) (executor.OrdinalBinding, bool) {
	ordinal := -1
	desc := sqltypes.TypeDesc{}
	for index, column := range s.columns {
		if column.name != name {
			continue
		}
		if ordinal >= 0 {
			return executor.OrdinalBinding{}, false
		}

		ordinal = index
		desc = column.typ
	}
	if ordinal < 0 {
		return executor.OrdinalBinding{}, false
	}

	return executor.OrdinalBinding{
		Ordinal: ordinal,
		Type:    desc,
	}, true
}

func qualifiedNameTail(name *parser.QualifiedName) string {
	if name == nil || len(name.Parts) == 0 || name.Parts[len(name.Parts)-1] == nil {
		return ""
	}

	return name.Parts[len(name.Parts)-1].Name
}

func evalLimitCount(node parser.Node) (uint64, error) {
	switch node := node.(type) {
	case *parser.IntegerLiteral:
		value, err := strconv.ParseUint(node.Text, 10, 64)
		if err != nil {
			return 0, featureError(node, "invalid LIMIT value %q", node.Text)
		}
		return value, nil
	default:
		return 0, featureError(node, "LIMIT expressions are not supported in Phase 1 embed")
	}
}

type oneRowOperator struct {
	open   bool
	closed bool
	done   bool
}

func (o *oneRowOperator) Open() error {
	switch {
	case o.closed:
		return executor.ErrOperatorClosed
	case o.open:
		return executor.ErrOperatorOpen
	default:
		o.open = true
		o.done = false
		return nil
	}
}

func (o *oneRowOperator) Next() (executor.Row, error) {
	switch {
	case !o.open && !o.closed:
		return executor.Row{}, executor.ErrOperatorNotOpen
	case o.closed:
		return executor.Row{}, executor.ErrOperatorClosed
	case o.done:
		return executor.Row{}, io.EOF
	default:
		o.done = true
		return executor.NewRow(), nil
	}
}

func (o *oneRowOperator) Close() error {
	o.closed = true
	o.open = false
	return nil
}

type remapOperator struct {
	child      executor.Operator
	childOpen  bool
	open       bool
	closed     bool
	targetSize int
	ordinals   []int
}

func newRemapOperator(child executor.Operator, ordinals []int, targetSize int) *remapOperator {
	return &remapOperator{
		child:      child,
		targetSize: targetSize,
		ordinals:   append([]int(nil), ordinals...),
	}
}

func (o *remapOperator) Open() error {
	switch {
	case o.closed:
		return executor.ErrOperatorClosed
	case o.open:
		return executor.ErrOperatorOpen
	}
	if o.child == nil {
		return featureError(nil, "insert query remap child is nil")
	}
	if err := o.child.Open(); err != nil {
		return err
	}

	o.open = true
	o.childOpen = true
	return nil
}

func (o *remapOperator) Next() (executor.Row, error) {
	switch {
	case !o.open && !o.closed:
		return executor.Row{}, executor.ErrOperatorNotOpen
	case o.closed:
		return executor.Row{}, executor.ErrOperatorClosed
	}

	input, err := o.child.Next()
	if err != nil {
		return executor.Row{}, err
	}

	values := make([]sqltypes.Value, o.targetSize)
	for index := range values {
		if index >= len(o.ordinals) || o.ordinals[index] < 0 {
			values[index] = sqltypes.NullValue()
			continue
		}

		value, ok := input.Value(o.ordinals[index])
		if !ok {
			return executor.Row{}, internalError(nil, "remap input ordinal %d out of range", o.ordinals[index])
		}
		values[index] = value
	}

	return executor.NewRow(values...), nil
}

func (o *remapOperator) Close() error {
	child := o.child
	childOpen := o.childOpen

	o.closed = true
	o.open = false
	o.childOpen = false

	if !childOpen || child == nil {
		return nil
	}

	return child.Close()
}
