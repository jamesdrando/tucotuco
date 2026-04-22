package embed

import (
	"io"
	"strconv"
	"strings"

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
	relation string
	name     string
	typ      sqltypes.TypeDesc
	binding  *analyzer.ColumnBinding
}

func buildSelectOperator(
	stmt *parser.SelectStmt,
	bindings *analyzer.Bindings,
	types *analyzer.Types,
	store storage.Storage,
	tx storage.Transaction,
) (executor.Operator, []planner.Column, error) {
	lowerer := planLowerer{
		bindings: bindings,
		types:    types,
		store:    store,
		tx:       tx,
	}
	if selectContainsJoin(stmt) {
		lowered, err := lowerer.lowerSelect(stmt)
		if err != nil {
			return nil, nil, err
		}

		return lowered.op, lowered.columns, nil
	}

	plan, diags := planner.NewBuilder(bindings, types).Build(stmt)
	if len(diags) != 0 {
		return nil, nil, diagnosticsError(diags)
	}

	lowered, err := lowerer.lower(plan)
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

func (l planLowerer) lowerSelect(stmt *parser.SelectStmt) (loweredPlan, error) {
	if stmt == nil {
		return loweredPlan{
			op:    &oneRowOperator{},
			shape: rowShape{},
		}, nil
	}
	if err := validateSelectForEmbed(stmt); err != nil {
		return loweredPlan{}, err
	}

	input, err := l.lowerSelectInput(stmt)
	if err != nil {
		return loweredPlan{}, err
	}
	if stmt.Where != nil {
		predicate, err := compileExpression(l.bindings, l.types, stmt.Where, input.shape)
		if err != nil {
			return loweredPlan{}, err
		}

		input = loweredPlan{
			op:      executor.NewFilter(input.op, predicate),
			shape:   input.shape,
			columns: input.columns,
		}
	}

	return l.lowerSelectProjection(stmt, input)
}

func (l planLowerer) lowerSelectInput(stmt *parser.SelectStmt) (loweredPlan, error) {
	if stmt == nil || len(stmt.From) == 0 {
		return loweredPlan{
			op:    &oneRowOperator{},
			shape: rowShape{},
		}, nil
	}
	if len(stmt.From) != 1 {
		return loweredPlan{}, featureError(stmt.From[1], "multiple FROM sources are not supported in Phase 1 planner")
	}

	return l.lowerFromNode(stmt.From[0])
}

func (l planLowerer) lowerFromNode(node parser.Node) (loweredPlan, error) {
	switch node := node.(type) {
	case nil:
		return loweredPlan{
			op:    &oneRowOperator{},
			shape: rowShape{},
		}, nil
	case *parser.FromSource:
		return l.lowerFromSource(node)
	case *parser.JoinExpr:
		return l.lowerJoinExpr(node)
	default:
		return loweredPlan{}, featureError(node, "FROM source is not supported in Phase 1 embed")
	}
}

func (l planLowerer) lowerFromSource(source *parser.FromSource) (loweredPlan, error) {
	if source == nil {
		return loweredPlan{
			op:    &oneRowOperator{},
			shape: rowShape{},
		}, nil
	}

	var lowered loweredPlan
	switch inner := source.Source.(type) {
	case *parser.QualifiedName:
		relation, ok := l.boundRelation(source, inner)
		if !ok || relation == nil {
			return loweredPlan{}, internalError(source, "embed is missing relation metadata for the FROM source")
		}

		lowered = loweredPlan{
			op:      executor.NewSeqScan(l.store, l.tx, relation.TableID, storage.ScanOptions{}),
			shape:   shapeFromRelationBinding(relation, l.types),
			columns: columnsFromRelationBinding(relation, l.types),
		}
	case *parser.SelectStmt:
		child, err := l.lowerSelect(inner)
		if err != nil {
			return loweredPlan{}, err
		}
		lowered = child
	case *parser.JoinExpr:
		child, err := l.lowerJoinExpr(inner)
		if err != nil {
			return loweredPlan{}, err
		}
		lowered = child
	default:
		return loweredPlan{}, featureError(source, "FROM source is not supported in Phase 1 embed")
	}

	if relation, ok := l.boundRelation(source); ok && relation != nil {
		lowered.shape = lowered.shape.relabel(relation, l.types)
		lowered.columns = columnsFromRelationBinding(relation, l.types)
	}

	return lowered, nil
}

func (l planLowerer) lowerJoinExpr(join *parser.JoinExpr) (loweredPlan, error) {
	if join == nil {
		return loweredPlan{
			op:    &oneRowOperator{},
			shape: rowShape{},
		}, nil
	}
	if join.Natural {
		return loweredPlan{}, featureError(join, "NATURAL JOIN is not supported in this embed baton")
	}
	if len(join.Using) != 0 {
		return loweredPlan{}, featureError(join, "JOIN USING is not supported in this embed baton")
	}

	left, err := l.lowerFromNode(join.Left)
	if err != nil {
		return loweredPlan{}, err
	}
	right, err := l.lowerFromNode(join.Right)
	if err != nil {
		return loweredPlan{}, err
	}

	shape := left.shape.append(right.shape)
	columns := append(append([]planner.Column(nil), left.columns...), right.columns...)

	var predicate executor.CompiledExpr
	hasPredicate := false
	if join.Condition != nil {
		predicate, err = compileExpression(l.bindings, l.types, join.Condition, shape)
		if err != nil {
			return loweredPlan{}, err
		}
		hasPredicate = true
	}

	return loweredPlan{
		op: newJoinOperator(
			join.Type,
			left.op,
			right.op,
			predicate,
			hasPredicate,
			len(left.shape.columns),
			len(right.shape.columns),
		),
		shape:   shape,
		columns: columns,
	}, nil
}

func (l planLowerer) lowerSelectProjection(stmt *parser.SelectStmt, input loweredPlan) (loweredPlan, error) {
	outputTypes, ok := l.selectOutputs(stmt)
	if !ok {
		return loweredPlan{}, internalError(stmt, "embed is missing SELECT output metadata")
	}

	exprs := make([]executor.CompiledExpr, 0, len(outputTypes))
	columns := make([]planner.Column, 0, len(outputTypes))
	outputCursor := 0
	for _, item := range stmt.SelectList {
		if item == nil || item.Expr == nil {
			continue
		}

		switch expr := item.Expr.(type) {
		case *parser.Star:
			if l.bindings == nil {
				return loweredPlan{}, internalError(expr, "embed requires analyzer bindings for star expansion")
			}

			starColumns, ok := l.bindings.Star(expr)
			if !ok {
				return loweredPlan{}, internalError(expr, "embed is missing star expansion metadata")
			}

			for _, column := range starColumns {
				desc, ok := nextOutputType(outputTypes, &outputCursor)
				if !ok {
					return loweredPlan{}, internalError(expr, "embed output metadata does not match the SELECT list")
				}

				binding, ok := input.shape.bindingForColumn(column)
				if !ok {
					return loweredPlan{}, internalError(expr, "embed could not resolve star column %q during lowering", safeBindingName(column))
				}

				compiled, err := compileOrdinalBinding(binding)
				if err != nil {
					return loweredPlan{}, err
				}

				exprs = append(exprs, compiled)
				columns = append(columns, planner.Column{
					Name: safeBindingName(column),
					Type: desc,
				})
			}
		default:
			desc, ok := nextOutputType(outputTypes, &outputCursor)
			if !ok {
				return loweredPlan{}, internalError(item, "embed output metadata does not match the SELECT list")
			}

			compiled, err := compileExpression(l.bindings, l.types, expr, input.shape)
			if err != nil {
				return loweredPlan{}, err
			}

			exprs = append(exprs, compiled)
			columns = append(columns, planner.Column{
				Name: projectedColumnName(item),
				Type: desc,
			})
		}
	}

	if outputCursor != len(outputTypes) {
		return loweredPlan{}, internalError(stmt, "embed output metadata does not match the SELECT list")
	}

	return loweredPlan{
		op:      executor.NewProject(input.op, exprs...),
		shape:   shapeFromColumns(columns),
		columns: columns,
	}, nil
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

func shapeFromRelationBinding(relation *analyzer.RelationBinding, types *analyzer.Types) rowShape {
	if relation == nil {
		return rowShape{}
	}

	shape := rowShape{
		columns: make([]shapeColumn, 0, len(relation.Columns)),
	}
	for _, column := range relation.Columns {
		shape.columns = append(shape.columns, shapeColumn{
			relation: safeRelationName(column),
			name:     safeBindingName(column),
			typ:      bindingType(column, types),
			binding:  column,
		})
	}

	return shape
}

func columnsFromRelationBinding(relation *analyzer.RelationBinding, types *analyzer.Types) []planner.Column {
	if relation == nil {
		return nil
	}

	columns := make([]planner.Column, 0, len(relation.Columns))
	for _, column := range relation.Columns {
		columns = append(columns, planner.Column{
			Name: safeBindingName(column),
			Type: bindingType(column, types),
		})
	}

	return columns
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
			if binding, ok := m.shape.bindingForColumn(column); ok {
				return binding, true
			}
		}
	}

	switch node := node.(type) {
	case *parser.Identifier:
		return m.shape.bindingForUnqualifiedName(node.Name)
	case *parser.QualifiedName:
		return m.shape.bindingForQualifiedName(node)
	default:
		return executor.OrdinalBinding{}, false
	}
}

func (s rowShape) bindingForColumn(column *analyzer.ColumnBinding) (executor.OrdinalBinding, bool) {
	if column == nil {
		return executor.OrdinalBinding{}, false
	}

	for index, candidate := range s.columns {
		if candidate.binding != nil && candidate.binding == column {
			return executor.OrdinalBinding{
				Ordinal: index,
				Type:    candidate.typ,
			}, true
		}
	}

	relationName := safeRelationName(column)
	if relationName != "" {
		if binding, ok := s.bindingForQualifiedParts(relationName, column.Name); ok {
			return binding, true
		}
	}

	return s.bindingForUnqualifiedName(column.Name)
}

func (s rowShape) bindingForUnqualifiedName(name string) (executor.OrdinalBinding, bool) {
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

func (s rowShape) bindingForQualifiedName(name *parser.QualifiedName) (executor.OrdinalBinding, bool) {
	if name == nil || len(name.Parts) == 0 {
		return executor.OrdinalBinding{}, false
	}
	if len(name.Parts) == 1 {
		return s.bindingForUnqualifiedName(name.Parts[0].Name)
	}
	if len(name.Parts) != 2 || name.Parts[0] == nil || name.Parts[1] == nil {
		return executor.OrdinalBinding{}, false
	}

	return s.bindingForQualifiedParts(name.Parts[0].Name, name.Parts[1].Name)
}

func (s rowShape) bindingForQualifiedParts(relationName string, columnName string) (executor.OrdinalBinding, bool) {
	ordinal := -1
	desc := sqltypes.TypeDesc{}
	for index, column := range s.columns {
		if column.relation != relationName || column.name != columnName {
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

func (s rowShape) append(other rowShape) rowShape {
	shape := rowShape{
		columns: make([]shapeColumn, 0, len(s.columns)+len(other.columns)),
	}
	shape.columns = append(shape.columns, s.columns...)
	shape.columns = append(shape.columns, other.columns...)
	return shape
}

func (s rowShape) relabel(relation *analyzer.RelationBinding, types *analyzer.Types) rowShape {
	if relation == nil {
		return s
	}

	shape := rowShape{
		columns: make([]shapeColumn, 0, len(s.columns)),
	}
	for index, binding := range relation.Columns {
		column := shapeColumn{
			relation: safeRelationName(binding),
			name:     safeBindingName(binding),
			typ:      bindingType(binding, types),
			binding:  binding,
		}
		if column.typ.Kind == sqltypes.TypeKindInvalid && index < len(s.columns) {
			column.typ = s.columns[index].typ
		}
		shape.columns = append(shape.columns, column)
	}

	if len(shape.columns) == 0 && len(s.columns) != 0 {
		return s
	}

	return shape
}

func qualifiedNameTail(name *parser.QualifiedName) string {
	if name == nil || len(name.Parts) == 0 || name.Parts[len(name.Parts)-1] == nil {
		return ""
	}

	return name.Parts[len(name.Parts)-1].Name
}

func bindingType(binding *analyzer.ColumnBinding, types *analyzer.Types) sqltypes.TypeDesc {
	if binding == nil {
		return sqltypes.TypeDesc{}
	}
	if binding.Descriptor != nil {
		return binding.Descriptor.Type
	}
	if types == nil || binding.Source == nil {
		return sqltypes.TypeDesc{}
	}
	if desc, ok := types.Expr(binding.Source); ok {
		return desc
	}

	switch source := binding.Source.(type) {
	case *parser.SelectItem:
		if source.Expr != nil {
			if desc, ok := types.Expr(source.Expr); ok {
				return desc
			}
		}
	case *parser.ColumnDef:
		if desc, ok := types.Expr(source); ok {
			return desc
		}
	}

	return sqltypes.TypeDesc{}
}

func safeBindingName(binding *analyzer.ColumnBinding) string {
	if binding == nil {
		return ""
	}

	return binding.Name
}

func safeRelationName(binding *analyzer.ColumnBinding) string {
	if binding == nil || binding.Relation == nil {
		return ""
	}

	return binding.Relation.Name
}

func (l planLowerer) boundRelation(nodes ...parser.Node) (*analyzer.RelationBinding, bool) {
	for _, node := range nodes {
		if node == nil || l.bindings == nil {
			continue
		}

		relation, ok := l.bindings.Relation(node)
		if ok {
			return relation, true
		}
	}

	return nil, false
}

func (l planLowerer) selectOutputs(stmt *parser.SelectStmt) ([]sqltypes.TypeDesc, bool) {
	if l.types == nil {
		return nil, false
	}

	return l.types.SelectOutputs(stmt)
}

func nextOutputType(types []sqltypes.TypeDesc, cursor *int) (sqltypes.TypeDesc, bool) {
	if cursor == nil || *cursor >= len(types) {
		return sqltypes.TypeDesc{}, false
	}

	desc := types[*cursor]
	*cursor = *cursor + 1
	return desc, true
}

func compileOrdinalBinding(binding executor.OrdinalBinding) (executor.CompiledExpr, error) {
	node := &parser.Identifier{Name: "__embed_ordinal__"}
	return executor.CompileExpr(node, executor.Metadata{
		Types: map[parser.Node]sqltypes.TypeDesc{
			node: binding.Type,
		},
		Bindings: map[parser.Node]executor.OrdinalBinding{
			node: binding,
		},
	})
}

func validateSelectForEmbed(stmt *parser.SelectStmt) error {
	switch {
	case stmt == nil:
		return nil
	case stmt.SetQuantifier != "":
		return featureError(stmt, "DISTINCT queries are not supported in Phase 1 planner")
	case len(stmt.GroupBy) != 0:
		return featureError(stmt, "GROUP BY queries are not supported in Phase 1 planner")
	case stmt.Having != nil:
		return featureError(stmt, "HAVING queries are not supported in Phase 1 planner")
	case len(stmt.OrderBy) != 0:
		return featureError(stmt, "ORDER BY queries are not supported in Phase 1 planner")
	}

	for _, item := range stmt.SelectList {
		if item == nil || item.Expr == nil {
			continue
		}
		if containsAggregate(item.Expr) {
			return featureError(item.Expr, "aggregate queries are not supported in Phase 1 planner")
		}
	}

	return nil
}

func selectContainsJoin(stmt *parser.SelectStmt) bool {
	if stmt == nil {
		return false
	}

	for _, source := range stmt.From {
		if fromNodeContainsJoin(source) {
			return true
		}
	}

	return false
}

func fromNodeContainsJoin(node parser.Node) bool {
	switch node := node.(type) {
	case nil:
		return false
	case *parser.JoinExpr:
		return true
	case *parser.FromSource:
		return fromNodeContainsJoin(node.Source)
	case *parser.SelectStmt:
		return selectContainsJoin(node)
	default:
		return false
	}
}

func projectedColumnName(item *parser.SelectItem) string {
	if item == nil {
		return ""
	}
	if item.Alias != nil {
		return item.Alias.Name
	}

	switch expr := item.Expr.(type) {
	case *parser.Identifier:
		return expr.Name
	case *parser.QualifiedName:
		return qualifiedNameTail(expr)
	default:
		return ""
	}
}

func containsAggregate(node parser.Node) bool {
	switch node := node.(type) {
	case nil:
		return false
	case *parser.UnaryExpr:
		return containsAggregate(node.Operand)
	case *parser.BinaryExpr:
		return containsAggregate(node.Left) || containsAggregate(node.Right)
	case *parser.FunctionCall:
		if isAggregateFunction(node) {
			return true
		}
		for _, arg := range node.Args {
			if containsAggregate(arg) {
				return true
			}
		}
		return false
	case *parser.CastExpr:
		return containsAggregate(node.Expr)
	case *parser.WhenClause:
		return containsAggregate(node.Condition) || containsAggregate(node.Result)
	case *parser.CaseExpr:
		if containsAggregate(node.Operand) || containsAggregate(node.Else) {
			return true
		}
		for _, when := range node.Whens {
			if containsAggregate(when) {
				return true
			}
		}
		return false
	case *parser.BetweenExpr:
		return containsAggregate(node.Expr) || containsAggregate(node.Lower) || containsAggregate(node.Upper)
	case *parser.InExpr:
		if containsAggregate(node.Expr) {
			return true
		}
		for _, item := range node.List {
			if containsAggregate(item) {
				return true
			}
		}
		return false
	case *parser.LikeExpr:
		return containsAggregate(node.Expr) || containsAggregate(node.Pattern) || containsAggregate(node.Escape)
	case *parser.IsExpr:
		return containsAggregate(node.Expr) || containsAggregate(node.Right)
	default:
		return false
	}
}

func isAggregateFunction(call *parser.FunctionCall) bool {
	if call == nil || call.Name == nil || len(call.Name.Parts) == 0 {
		return false
	}

	switch strings.ToUpper(strings.TrimSpace(call.Name.Parts[len(call.Name.Parts)-1].Name)) {
	case "AVG", "COUNT", "MAX", "MIN", "SUM":
		return true
	default:
		return false
	}
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

type joinOperator struct {
	left         executor.Operator
	right        executor.Operator
	joinType     string
	predicate    executor.CompiledExpr
	hasPredicate bool
	leftWidth    int
	rightWidth   int

	open      bool
	closed    bool
	leftOpen  bool
	rightOpen bool

	rightRows    []executor.Row
	rightMatched []bool

	currentLeft        executor.Row
	haveCurrentLeft    bool
	currentRightIndex  int
	currentLeftMatched bool

	emittingUnmatchedRight bool
	unmatchedRightIndex    int
}

func newJoinOperator(
	joinType string,
	left executor.Operator,
	right executor.Operator,
	predicate executor.CompiledExpr,
	hasPredicate bool,
	leftWidth int,
	rightWidth int,
) *joinOperator {
	return &joinOperator{
		left:         left,
		right:        right,
		joinType:     strings.ToUpper(strings.TrimSpace(joinType)),
		predicate:    predicate,
		hasPredicate: hasPredicate,
		leftWidth:    leftWidth,
		rightWidth:   rightWidth,
	}
}

func (o *joinOperator) Open() error {
	switch {
	case o.closed:
		return executor.ErrOperatorClosed
	case o.open:
		return executor.ErrOperatorOpen
	}
	if o.left == nil || o.right == nil {
		return featureError(nil, "join child operator is nil")
	}
	if !supportedJoinType(o.joinType) {
		return featureError(nil, "unsupported join type %q", o.joinType)
	}

	if err := o.right.Open(); err != nil {
		return err
	}
	o.rightOpen = true

	rightRows, err := o.consumeRightRows()
	if err != nil {
		closeErr := o.right.Close()
		o.rightOpen = false
		return joinErrors(err, closeErr)
	}
	closeErr := o.right.Close()
	o.rightOpen = false
	if closeErr != nil {
		return closeErr
	}

	if err := o.left.Open(); err != nil {
		return err
	}
	o.leftOpen = true

	o.rightRows = rightRows
	o.rightMatched = make([]bool, len(rightRows))
	o.currentLeft = executor.Row{}
	o.haveCurrentLeft = false
	o.currentRightIndex = 0
	o.currentLeftMatched = false
	o.emittingUnmatchedRight = false
	o.unmatchedRightIndex = 0
	o.open = true

	return nil
}

func (o *joinOperator) consumeRightRows() ([]executor.Row, error) {
	rows := make([]executor.Row, 0)
	for {
		row, err := o.right.Next()
		if err != nil {
			if err == io.EOF {
				return rows, nil
			}
			return nil, err
		}

		rows = append(rows, row.Clone())
	}
}

func (o *joinOperator) Next() (executor.Row, error) {
	switch {
	case !o.open && !o.closed:
		return executor.Row{}, executor.ErrOperatorNotOpen
	case o.closed:
		return executor.Row{}, executor.ErrOperatorClosed
	}

	for {
		if o.emittingUnmatchedRight {
			for o.unmatchedRightIndex < len(o.rightRows) {
				index := o.unmatchedRightIndex
				o.unmatchedRightIndex++
				if o.rightMatched[index] {
					continue
				}

				return joinRows(zeroRow(o.leftWidth), o.rightRows[index]), nil
			}

			return executor.Row{}, io.EOF
		}

		if !o.haveCurrentLeft {
			row, err := o.left.Next()
			if err != nil {
				if err == io.EOF {
					if o.joinType == "RIGHT" || o.joinType == "FULL" {
						o.emittingUnmatchedRight = true
						continue
					}
					return executor.Row{}, io.EOF
				}
				return executor.Row{}, err
			}

			o.currentLeft = row.Clone()
			o.haveCurrentLeft = true
			o.currentRightIndex = 0
			o.currentLeftMatched = false
		}

		for o.currentRightIndex < len(o.rightRows) {
			index := o.currentRightIndex
			o.currentRightIndex++

			matched, err := o.matches(o.currentLeft, o.rightRows[index])
			if err != nil {
				return executor.Row{}, err
			}
			if !matched {
				continue
			}

			o.currentLeftMatched = true
			o.rightMatched[index] = true
			return joinRows(o.currentLeft, o.rightRows[index]), nil
		}

		if !o.currentLeftMatched && (o.joinType == "LEFT" || o.joinType == "FULL") {
			row := joinRows(o.currentLeft, zeroRow(o.rightWidth))
			o.haveCurrentLeft = false
			return row, nil
		}

		o.haveCurrentLeft = false
	}
}

func (o *joinOperator) matches(left executor.Row, right executor.Row) (bool, error) {
	if !o.hasPredicate {
		return true, nil
	}

	row := joinRows(left, right)
	value, err := o.predicate.Eval(row)
	if err != nil {
		return false, err
	}
	if value.IsNull() {
		return false, nil
	}

	raw, ok := value.Raw().(bool)
	if !ok {
		return false, internalError(nil, "JOIN condition evaluated to non-boolean %s", value.Kind())
	}

	return raw, nil
}

func (o *joinOperator) Close() error {
	left := o.left
	right := o.right
	leftOpen := o.leftOpen
	rightOpen := o.rightOpen

	o.open = false
	o.closed = true
	o.leftOpen = false
	o.rightOpen = false
	o.rightRows = nil
	o.rightMatched = nil
	o.currentLeft = executor.Row{}
	o.haveCurrentLeft = false
	o.currentRightIndex = 0
	o.currentLeftMatched = false
	o.emittingUnmatchedRight = false
	o.unmatchedRightIndex = 0

	var err error
	if leftOpen && left != nil {
		err = joinErrors(err, left.Close())
	}
	if rightOpen && right != nil {
		err = joinErrors(err, right.Close())
	}

	return err
}

func supportedJoinType(joinType string) bool {
	switch joinType {
	case "INNER", "LEFT", "RIGHT", "FULL", "CROSS":
		return true
	default:
		return false
	}
}

func joinRows(left executor.Row, right executor.Row) executor.Row {
	values := append(left.Values(), right.Values()...)
	return executor.NewRow(values...)
}

func zeroRow(width int) executor.Row {
	values := make([]sqltypes.Value, width)
	for index := range values {
		values[index] = sqltypes.NullValue()
	}

	return executor.NewRow(values...)
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
