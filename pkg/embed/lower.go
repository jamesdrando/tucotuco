package embed

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/jamesdrando/tucotuco/internal/analyzer"
	"github.com/jamesdrando/tucotuco/internal/catalog"
	internaldiag "github.com/jamesdrando/tucotuco/internal/diag"
	"github.com/jamesdrando/tucotuco/internal/executor"
	"github.com/jamesdrando/tucotuco/internal/parser"
	"github.com/jamesdrando/tucotuco/internal/planner"
	"github.com/jamesdrando/tucotuco/internal/storage"
	sqltypes "github.com/jamesdrando/tucotuco/internal/types"
)

const sqlStateCardinalityViolation = "21000"

type planLowerer struct {
	bindings *analyzer.Bindings
	types    *analyzer.Types
	catalog  catalog.Catalog
	store    storage.Storage
	tx       storage.Transaction
	outer    *correlatedContext
	viewPath []storage.TableID
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

type correlatedContext struct {
	row   executor.Row
	shape rowShape
}

func buildQueryOperator(
	query parser.QueryExpr,
	bindings *analyzer.Bindings,
	types *analyzer.Types,
	cat catalog.Catalog,
	store storage.Storage,
	tx storage.Transaction,
) (executor.Operator, []planner.Column, error) {
	lowerer := planLowerer{
		bindings: bindings,
		types:    types,
		catalog:  cat,
		store:    store,
		tx:       tx,
	}
	if queryRequiresDirectLowering(query) || queryReferencesView(query, bindings) {
		lowered, err := lowerer.lowerQuery(query)
		if err != nil {
			return nil, nil, err
		}

		return lowered.op, lowered.columns, nil
	}

	plan, diags := planner.NewBuilder(bindings, types).Build(query)
	if len(diags) != 0 {
		return nil, nil, diagnosticsError(diags)
	}

	lowered, err := lowerer.lower(plan)
	if err != nil {
		return nil, nil, err
	}

	return lowered.op, lowered.columns, nil
}

func buildSelectOperator(
	stmt *parser.SelectStmt,
	bindings *analyzer.Bindings,
	types *analyzer.Types,
	cat catalog.Catalog,
	store storage.Storage,
	tx storage.Transaction,
) (executor.Operator, []planner.Column, error) {
	return buildQueryOperator(stmt, bindings, types, cat, store, tx)
}

func (l planLowerer) lowerQuery(query parser.QueryExpr) (loweredPlan, error) {
	switch query := query.(type) {
	case nil:
		return loweredPlan{
			op:    &oneRowOperator{},
			shape: rowShape{},
		}, nil
	case *parser.SelectStmt:
		return l.lowerSelect(query)
	case *parser.SetOpExpr:
		return l.lowerSetOpQuery(query)
	default:
		return loweredPlan{}, featureError(nil, "unsupported query expression %T", query)
	}
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

		predicate, err := compileExpression(l.bindings, l.types, l.catalog, l.store, l.tx, node.Predicate, child.shape)
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
			expr, err := compileExpression(l.bindings, l.types, l.catalog, l.store, l.tx, projection.Expr, child.shape)
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
	case *planner.Join:
		left, err := l.lower(node.Left)
		if err != nil {
			return loweredPlan{}, err
		}
		right, err := l.lower(node.Right)
		if err != nil {
			return loweredPlan{}, err
		}

		var predicate executor.CompiledExpr
		hasPredicate := false
		if node.Condition != nil {
			predicate, err = compileExpression(l.bindings, l.types, l.catalog, l.store, l.tx, node.Condition, shapeFromColumns(node.Columns()))
			if err != nil {
				return loweredPlan{}, err
			}
			hasPredicate = true
		}

		return loweredPlan{
			op: newJoinOperator(
				node.Type,
				left.op,
				right.op,
				predicate,
				hasPredicate,
				len(left.shape.columns),
				len(right.shape.columns),
				executor.Row{},
				false,
			),
			shape:   shapeFromColumns(node.Columns()),
			columns: node.Columns(),
		}, nil
	case *planner.SetOp:
		left, err := l.lower(node.Left)
		if err != nil {
			return loweredPlan{}, err
		}
		right, err := l.lower(node.Right)
		if err != nil {
			return loweredPlan{}, err
		}

		return loweredPlan{
			op: executor.NewSetOp(
				left.op,
				right.op,
				node.Operator,
				node.SetQuantifier,
				columnTypes(left.columns),
				columnTypes(right.columns),
				columnTypes(node.Columns()),
			),
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
	input = l.attachOuter(input)
	if stmt.Where != nil {
		predicate, err := compileExpression(l.bindings, l.types, l.catalog, l.store, l.tx, stmt.Where, input.shape)
		if err != nil {
			return loweredPlan{}, err
		}

		input = loweredPlan{
			op:      executor.NewFilter(input.op, predicate),
			shape:   input.shape,
			columns: input.columns,
		}
	}
	if selectUsesAggregation(stmt) {
		return l.lowerAggregateSelect(stmt, input)
	}

	return l.lowerSelectProjection(stmt, input)
}

type aggregateLowering struct {
	metadata     aggregateExpressionMetadata
	groupExprs   []executor.CompiledExpr
	aggregateOps []executor.AggregateSpec
	groupRefs    map[string]parser.Node
	aggRefs      map[*parser.FunctionCall]parser.Node
	outputShape  rowShape
	outputCols   []planner.Column
}

type aggregateExpressionMetadata struct {
	base     expressionMetadata
	types    map[parser.Node]sqltypes.TypeDesc
	bindings map[parser.Node]executor.OrdinalBinding
}

func (m aggregateExpressionMetadata) TypeOf(node parser.Node) (sqltypes.TypeDesc, bool) {
	if node != nil {
		if desc, ok := m.types[node]; ok {
			return desc, true
		}
	}

	return m.base.TypeOf(node)
}

func (m aggregateExpressionMetadata) BindingOf(node parser.Node) (executor.OrdinalBinding, bool) {
	if node != nil {
		if binding, ok := m.bindings[node]; ok {
			return binding, true
		}
	}

	return m.base.BindingOf(node)
}

func (m aggregateExpressionMetadata) CompileSubquery(node parser.Node) (executor.CompiledExpr, error) {
	return m.base.CompileSubquery(node)
}

func (l planLowerer) lowerAggregateSelect(stmt *parser.SelectStmt, input loweredPlan) (loweredPlan, error) {
	lowering, err := l.buildAggregateLowering(stmt, input)
	if err != nil {
		return loweredPlan{}, err
	}

	aggregated := loweredPlan{
		op:      executor.NewHashAggregate(input.op, lowering.groupExprs, lowering.aggregateOps...),
		shape:   lowering.outputShape,
		columns: lowering.outputCols,
	}
	if stmt.Having != nil {
		predicate, err := l.compileAggregateOutputExpr(stmt.Having, lowering)
		if err != nil {
			return loweredPlan{}, err
		}
		aggregated = loweredPlan{
			op:      executor.NewFilter(aggregated.op, predicate),
			shape:   aggregated.shape,
			columns: aggregated.columns,
		}
	}

	return l.lowerAggregateProjection(stmt, aggregated, lowering)
}

func (l planLowerer) buildAggregateLowering(stmt *parser.SelectStmt, input loweredPlan) (aggregateLowering, error) {
	lowering := aggregateLowering{
		metadata: aggregateExpressionMetadata{
			base: expressionMetadata{
				bindings: l.bindings,
				types:    l.types,
				catalog:  l.catalog,
				store:    l.store,
				tx:       l.tx,
			},
			types:    make(map[parser.Node]sqltypes.TypeDesc),
			bindings: make(map[parser.Node]executor.OrdinalBinding),
		},
		groupRefs:  make(map[string]parser.Node, len(stmt.GroupBy)),
		aggRefs:    make(map[*parser.FunctionCall]parser.Node),
		outputCols: make([]planner.Column, 0, len(stmt.GroupBy)),
	}

	for ordinal, expr := range stmt.GroupBy {
		if expr == nil {
			continue
		}

		compiled, err := compileExpression(l.bindings, l.types, l.catalog, l.store, l.tx, expr, input.shape)
		if err != nil {
			return aggregateLowering{}, err
		}
		lowering.groupExprs = append(lowering.groupExprs, compiled)

		desc, ok := l.typeOfNode(expr)
		if !ok {
			desc = compiled.Type()
		}
		binding, _ := l.boundColumn(expr)
		groupBinding := executor.OrdinalBinding{Ordinal: ordinal, Type: desc}
		ref := aggregateOutputRef(fmt.Sprintf("__embed_group_%d", ordinal))
		lowering.metadata.types[ref] = desc
		lowering.metadata.bindings[ref] = groupBinding
		key := l.groupExprKey(expr)
		if key != "" {
			if _, exists := lowering.groupRefs[key]; !exists {
				lowering.groupRefs[key] = ref
			}
		}

		lowering.outputShape.columns = append(lowering.outputShape.columns, shapeColumn{
			relation: safeRelationName(binding),
			name:     safeBindingName(binding),
			typ:      desc,
			binding:  binding,
		})
		lowering.outputCols = append(lowering.outputCols, planner.Column{
			Name:         safeBindingName(binding),
			Type:         desc,
			RelationName: safeRelationName(binding),
			Binding:      binding,
		})
	}

	aggregateCalls := collectAggregateCalls(stmt)
	for index, call := range aggregateCalls {
		spec, desc, err := l.buildAggregateSpec(call, input.shape)
		if err != nil {
			return aggregateLowering{}, err
		}

		lowering.aggregateOps = append(lowering.aggregateOps, spec)
		ordinal := len(lowering.groupExprs) + index
		aggregateBinding := executor.OrdinalBinding{Ordinal: ordinal, Type: desc}
		ref := aggregateOutputRef(fmt.Sprintf("__embed_agg_%d", ordinal))
		lowering.metadata.types[ref] = desc
		lowering.metadata.bindings[ref] = aggregateBinding
		lowering.aggRefs[call] = ref
		lowering.outputShape.columns = append(lowering.outputShape.columns, shapeColumn{typ: desc})
		lowering.outputCols = append(lowering.outputCols, planner.Column{Type: desc})
	}

	lowering.metadata.base.shape = lowering.outputShape

	return lowering, nil
}

func (l planLowerer) buildAggregateSpec(call *parser.FunctionCall, input rowShape) (executor.AggregateSpec, sqltypes.TypeDesc, error) {
	if call == nil {
		return executor.AggregateSpec{}, sqltypes.TypeDesc{}, internalError(call, "aggregate call is nil")
	}
	if strings.TrimSpace(call.SetQuantifier) != "" {
		return executor.AggregateSpec{}, sqltypes.TypeDesc{}, featureError(call, "aggregate set quantifier %q is not supported in Phase 2 embed", call.SetQuantifier)
	}

	name := aggregateFunctionName(call)
	desc, _ := l.typeOfNode(call)
	spec := executor.AggregateSpec{Name: executor.AggregateName(name)}

	if isCountStarCall(call) {
		spec.CountStar = true
		return spec, desc, nil
	}
	if len(call.Args) != 1 {
		return executor.AggregateSpec{}, sqltypes.TypeDesc{}, internalError(call, "aggregate function %q is missing its argument", name)
	}

	compiled, err := compileExpression(l.bindings, l.types, l.catalog, l.store, l.tx, call.Args[0], input)
	if err != nil {
		return executor.AggregateSpec{}, sqltypes.TypeDesc{}, err
	}
	spec.Expr = compiled

	return spec, desc, nil
}

func (l planLowerer) lowerAggregateProjection(stmt *parser.SelectStmt, input loweredPlan, lowering aggregateLowering) (loweredPlan, error) {
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
		if _, ok := item.Expr.(*parser.Star); ok {
			return loweredPlan{}, featureError(item.Expr, "SELECT * is not allowed with GROUP BY or aggregate functions")
		}

		desc, ok := nextOutputType(outputTypes, &outputCursor)
		if !ok {
			return loweredPlan{}, internalError(item, "embed output metadata does not match the SELECT list")
		}
		if l.bindings != nil {
			if binding, ok := l.bindings.Column(item.Expr); ok && binding != nil {
				if matched, ok := input.shape.bindingForColumn(binding); ok {
					desc = matched.Type
				}
			}
		}

		compiled, err := l.compileAggregateOutputExpr(item.Expr, lowering)
		if err != nil {
			return loweredPlan{}, err
		}
		exprs = append(exprs, compiled)
		columns = append(columns, planner.Column{
			Name: projectedColumnName(item),
			Type: desc,
		})
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

func (l planLowerer) compileAggregateOutputExpr(node parser.Node, lowering aggregateLowering) (executor.CompiledExpr, error) {
	rewritten := l.rewriteAggregateExpr(node, lowering)
	return executor.CompileExpr(rewritten, lowering.metadata)
}

func (l planLowerer) rewriteAggregateExpr(node parser.Node, lowering aggregateLowering) parser.Node {
	if node == nil {
		return nil
	}
	if call, ok := node.(*parser.FunctionCall); ok {
		if ref, ok := lowering.aggRefs[call]; ok {
			return ref
		}
	}
	if ref, ok := lowering.groupRefs[l.groupExprKey(node)]; ok {
		return ref
	}

	switch node := node.(type) {
	case *parser.UnaryExpr:
		clone := *node
		clone.Operand = l.rewriteAggregateExpr(node.Operand, lowering)
		return &clone
	case *parser.BinaryExpr:
		clone := *node
		clone.Left = l.rewriteAggregateExpr(node.Left, lowering)
		clone.Right = l.rewriteAggregateExpr(node.Right, lowering)
		return &clone
	case *parser.FunctionCall:
		clone := *node
		clone.Args = make([]parser.Node, 0, len(node.Args))
		for _, arg := range node.Args {
			clone.Args = append(clone.Args, l.rewriteAggregateExpr(arg, lowering))
		}
		return &clone
	case *parser.CastExpr:
		clone := *node
		clone.Expr = l.rewriteAggregateExpr(node.Expr, lowering)
		return &clone
	case *parser.WhenClause:
		clone := *node
		clone.Condition = l.rewriteAggregateExpr(node.Condition, lowering)
		clone.Result = l.rewriteAggregateExpr(node.Result, lowering)
		return &clone
	case *parser.CaseExpr:
		clone := *node
		clone.Operand = l.rewriteAggregateExpr(node.Operand, lowering)
		clone.Whens = make([]*parser.WhenClause, 0, len(node.Whens))
		for _, when := range node.Whens {
			rewritten, _ := l.rewriteAggregateExpr(when, lowering).(*parser.WhenClause)
			clone.Whens = append(clone.Whens, rewritten)
		}
		clone.Else = l.rewriteAggregateExpr(node.Else, lowering)
		return &clone
	case *parser.BetweenExpr:
		clone := *node
		clone.Expr = l.rewriteAggregateExpr(node.Expr, lowering)
		clone.Lower = l.rewriteAggregateExpr(node.Lower, lowering)
		clone.Upper = l.rewriteAggregateExpr(node.Upper, lowering)
		return &clone
	case *parser.InExpr:
		clone := *node
		clone.Expr = l.rewriteAggregateExpr(node.Expr, lowering)
		if node.Query == nil {
			clone.List = make([]parser.Node, 0, len(node.List))
			for _, item := range node.List {
				clone.List = append(clone.List, l.rewriteAggregateExpr(item, lowering))
			}
		}
		return &clone
	case *parser.LikeExpr:
		clone := *node
		clone.Expr = l.rewriteAggregateExpr(node.Expr, lowering)
		clone.Pattern = l.rewriteAggregateExpr(node.Pattern, lowering)
		clone.Escape = l.rewriteAggregateExpr(node.Escape, lowering)
		return &clone
	case *parser.IsExpr:
		clone := *node
		clone.Expr = l.rewriteAggregateExpr(node.Expr, lowering)
		clone.Right = l.rewriteAggregateExpr(node.Right, lowering)
		return &clone
	default:
		return node
	}
}

func collectAggregateCalls(stmt *parser.SelectStmt) []*parser.FunctionCall {
	calls := make([]*parser.FunctionCall, 0)
	seen := make(map[*parser.FunctionCall]struct{})
	for _, item := range stmt.SelectList {
		if item == nil {
			continue
		}
		collectAggregateCallsInExpr(item.Expr, &calls, seen)
	}
	collectAggregateCallsInExpr(stmt.Having, &calls, seen)
	return calls
}

func collectAggregateCallsInExpr(node parser.Node, out *[]*parser.FunctionCall, seen map[*parser.FunctionCall]struct{}) {
	switch node := node.(type) {
	case nil:
		return
	case *parser.SelectStmt, *parser.SubqueryExpr, *parser.ExistsExpr:
		return
	case *parser.FunctionCall:
		if isAggregateFunction(node) {
			if _, ok := seen[node]; !ok {
				seen[node] = struct{}{}
				*out = append(*out, node)
			}
			return
		}
	}

	forEachAggregateExprChild(node, func(child parser.Node) {
		collectAggregateCallsInExpr(child, out, seen)
	})
}

func aggregateOutputRef(name string) *parser.Identifier {
	return &parser.Identifier{Name: name}
}

func (l planLowerer) typeOfNode(node parser.Node) (sqltypes.TypeDesc, bool) {
	if l.types == nil || node == nil {
		return sqltypes.TypeDesc{}, false
	}

	return l.types.Expr(node)
}

func (l planLowerer) boundColumn(node parser.Node) (*analyzer.ColumnBinding, bool) {
	if l.bindings == nil || node == nil {
		return nil, false
	}

	return l.bindings.Column(node)
}

func (l planLowerer) lowerSetOpQuery(query *parser.SetOpExpr) (loweredPlan, error) {
	if query == nil {
		return loweredPlan{
			op:    &oneRowOperator{},
			shape: rowShape{},
		}, nil
	}

	left, err := l.lowerQuery(query.Left)
	if err != nil {
		return loweredPlan{}, err
	}
	right, err := l.lowerQuery(query.Right)
	if err != nil {
		return loweredPlan{}, err
	}

	outputs, ok := l.queryOutputs(query)
	if !ok {
		return loweredPlan{}, internalError(query, "embed is missing query output metadata")
	}

	columns := l.queryColumns(query, outputs)
	if len(columns) != len(outputs) {
		return loweredPlan{}, internalError(query, "embed output metadata does not match the set operation")
	}

	return loweredPlan{
		op: executor.NewSetOp(
			left.op,
			right.op,
			query.Operator,
			query.SetQuantifier,
			columnTypes(left.columns),
			columnTypes(right.columns),
			columnTypes(columns),
		),
		shape:   shapeFromColumns(columns),
		columns: columns,
	}, nil
}

func (l planLowerer) lowerSelectInput(stmt *parser.SelectStmt) (loweredPlan, error) {
	if stmt == nil || len(stmt.From) == 0 {
		return loweredPlan{
			op:    &oneRowOperator{},
			shape: rowShape{},
		}, nil
	}

	input, err := l.lowerFromNode(stmt.From[0])
	if err != nil {
		return loweredPlan{}, err
	}

	for _, source := range stmt.From[1:] {
		right, err := l.lowerFromNode(source)
		if err != nil {
			return loweredPlan{}, err
		}

		columns := append(append([]planner.Column(nil), input.columns...), right.columns...)
		input = loweredPlan{
			op: newJoinOperator(
				"CROSS",
				input.op,
				right.op,
				executor.CompiledExpr{},
				false,
				len(input.shape.columns),
				len(right.shape.columns),
				executor.Row{},
				false,
			),
			shape:   input.shape.append(right.shape),
			columns: columns,
		}
	}

	return input, nil
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

		if relation.View != nil {
			child, err := l.lowerViewRelation(source, relation)
			if err != nil {
				return loweredPlan{}, err
			}
			lowered = child
		} else {
			lowered = loweredPlan{
				op:      executor.NewSeqScan(l.store, l.tx, relation.TableID, storage.ScanOptions{}),
				shape:   shapeFromRelationBinding(relation, l.types),
				columns: columnsFromRelationBinding(relation, l.types),
			}
		}
	case parser.QueryExpr:
		child, err := l.lowerQuery(inner)
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

func (l planLowerer) lowerViewRelation(node parser.Node, relation *analyzer.RelationBinding) (loweredPlan, error) {
	if relation == nil || relation.View == nil {
		return loweredPlan{}, internalError(node, "embed is missing view metadata")
	}
	if l.catalog == nil {
		return loweredPlan{}, internalError(node, "embed cannot expand view without catalog metadata")
	}
	if l.viewPathContains(relation.TableID) {
		return loweredPlan{}, featureError(node, "recursive view expansion is not supported")
	}

	script, err := parseScript(relation.View.Query.SQL)
	if err != nil {
		return loweredPlan{}, err
	}
	if len(script.Nodes) != 1 {
		return loweredPlan{}, internalError(node, "view %q does not store exactly one query", relation.TableID.String())
	}
	query, ok := script.Nodes[0].(parser.QueryExpr)
	if !ok {
		return loweredPlan{}, internalError(node, "view %q does not store a query expression", relation.TableID.String())
	}

	bindings, resolveDiags := analyzer.NewResolver(l.catalog).ResolveScript(script)
	if len(resolveDiags) != 0 {
		return loweredPlan{}, diagnosticsError(resolveDiags)
	}
	types, typeDiags := analyzer.NewTypeChecker(bindings).CheckScript(script)
	if len(typeDiags) != 0 {
		return loweredPlan{}, diagnosticsError(typeDiags)
	}

	child := planLowerer{
		bindings: bindings,
		types:    types,
		catalog:  l.catalog,
		store:    l.store,
		tx:       l.tx,
		viewPath: append(append([]storage.TableID{}, l.viewPath...), relation.TableID),
	}
	return child.lowerQuery(query)
}

func (l planLowerer) viewPathContains(id storage.TableID) bool {
	for _, existing := range l.viewPath {
		if existing == id {
			return true
		}
	}

	return false
}

func (l planLowerer) lowerJoinExpr(join *parser.JoinExpr) (loweredPlan, error) {
	if join == nil {
		return loweredPlan{
			op:    &oneRowOperator{},
			shape: rowShape{},
		}, nil
	}
	if join.Natural {
		return loweredPlan{}, featureError(join, "NATURAL JOIN planning is not supported in Phase 2 planner")
	}
	if len(join.Using) != 0 {
		return loweredPlan{}, featureError(join, "JOIN ... USING is not supported in Phase 2 planner")
	}

	left, err := l.lowerFromNode(join.Left)
	if err != nil {
		return loweredPlan{}, err
	}
	right, err := l.lowerFromNode(join.Right)
	if err != nil {
		return loweredPlan{}, err
	}

	leftNullable := join.Type == "RIGHT" || join.Type == "FULL"
	rightNullable := join.Type == "LEFT" || join.Type == "FULL"
	shape := outerJoinShape(left.shape, leftNullable).append(outerJoinShape(right.shape, rightNullable))
	columns := append(
		append([]planner.Column(nil), outerJoinColumns(left.columns, leftNullable)...),
		outerJoinColumns(right.columns, rightNullable)...,
	)

	var predicate executor.CompiledExpr
	hasPredicate := false
	if join.Condition != nil {
		predicateShape := shape
		if l.outer != nil {
			predicateShape = predicateShape.append(l.outer.shape)
		}
		predicate, err = compileExpression(l.bindings, l.types, l.catalog, l.store, l.tx, join.Condition, predicateShape)
		if err != nil {
			return loweredPlan{}, err
		}
		hasPredicate = true
	}

	outerRow := executor.Row{}
	hasOuter := false
	if l.outer != nil {
		outerRow = l.outer.row
		hasOuter = true
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
			outerRow,
			hasOuter,
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
				if binding.Type.Kind != sqltypes.TypeKindInvalid {
					desc = binding.Type
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
			if l.bindings != nil {
				if binding, ok := l.bindings.Column(expr); ok && binding != nil {
					if matched, ok := input.shape.bindingForColumn(binding); ok {
						desc = matched.Type
					}
				}
			}

			compiled, err := compileExpression(l.bindings, l.types, l.catalog, l.store, l.tx, expr, input.shape)
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

func (l planLowerer) attachOuter(input loweredPlan) loweredPlan {
	if l.outer == nil {
		return input
	}

	return loweredPlan{
		op:      newAppendOuterRowOperator(input.op, l.outer.row),
		shape:   input.shape.append(l.outer.shape),
		columns: input.columns,
	}
}

func shapeFromColumns(columns []planner.Column) rowShape {
	shape := rowShape{
		columns: make([]shapeColumn, 0, len(columns)),
	}
	for _, column := range columns {
		shape.columns = append(shape.columns, shapeColumn{
			relation: column.RelationName,
			name:     column.Name,
			typ:      column.Type,
			binding:  column.Binding,
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

func columnsFromRelationBindingWithTypes(
	relation *analyzer.RelationBinding,
	types *analyzer.Types,
	outputs []sqltypes.TypeDesc,
) []planner.Column {
	if relation == nil {
		return nil
	}

	columns := make([]planner.Column, 0, len(relation.Columns))
	for index, column := range relation.Columns {
		desc := bindingType(column, types)
		if desc.Kind == sqltypes.TypeKindInvalid && index < len(outputs) {
			desc = outputs[index]
		}
		columns = append(columns, planner.Column{
			Name:         safeBindingName(column),
			Type:         desc,
			RelationName: safeRelationName(column),
			Binding:      column,
		})
	}

	return columns
}

func columnTypes(columns []planner.Column) []sqltypes.TypeDesc {
	types := make([]sqltypes.TypeDesc, 0, len(columns))
	for _, column := range columns {
		types = append(types, column.Type)
	}

	return types
}

func compileExpression(
	bindings *analyzer.Bindings,
	types *analyzer.Types,
	cat catalog.Catalog,
	store storage.Storage,
	tx storage.Transaction,
	node parser.Node,
	shape rowShape,
) (executor.CompiledExpr, error) {
	return executor.CompileExpr(node, expressionMetadata{
		bindings: bindings,
		types:    types,
		catalog:  cat,
		store:    store,
		tx:       tx,
		shape:    shape,
	})
}

type expressionMetadata struct {
	bindings *analyzer.Bindings
	types    *analyzer.Types
	catalog  catalog.Catalog
	store    storage.Storage
	tx       storage.Transaction
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

func (m expressionMetadata) CompileSubquery(node parser.Node) (executor.CompiledExpr, error) {
	switch node := node.(type) {
	case *parser.SubqueryExpr:
		return m.compileScalarSubquery(node)
	case *parser.ExistsExpr:
		return m.compileExistsSubquery(node)
	case *parser.InExpr:
		if node != nil && node.Query != nil {
			return m.compileInSubquery(node)
		}
	}

	return executor.CompiledExpr{}, fmt.Errorf("%w: %T", executor.ErrUnsupportedExpression, node)
}

func (m expressionMetadata) compileScalarSubquery(node *parser.SubqueryExpr) (executor.CompiledExpr, error) {
	desc, _ := m.TypeOf(node)
	return executor.NewCompiledExpr(
		desc,
		func(row executor.Row) (sqltypes.Value, error) {
			rows, columns, err := m.materializeSubquery(node.Query, row)
			if err != nil {
				return sqltypes.Value{}, err
			}

			column, err := requireSingleSubqueryColumn(node, columns, "scalar subquery")
			if err != nil {
				return sqltypes.Value{}, err
			}
			if len(rows) == 0 {
				return sqltypes.NullValue(), nil
			}
			if len(rows) > 1 {
				return sqltypes.Value{}, cardinalityError(node, "scalar subquery returned more than one row")
			}

			value, ok := rows[0].Value(0)
			if !ok {
				return sqltypes.Value{}, internalError(node, "scalar subquery row is missing its projected column")
			}
			if column.Type.Kind != sqltypes.TypeKindInvalid {
				return value, nil
			}

			return value, nil
		},
	), nil
}

func (m expressionMetadata) compileExistsSubquery(node *parser.ExistsExpr) (executor.CompiledExpr, error) {
	desc, ok := m.TypeOf(node)
	if !ok {
		desc = sqltypes.TypeDesc{Kind: sqltypes.TypeKindBoolean}
	}

	return executor.NewCompiledExpr(
		desc,
		func(row executor.Row) (sqltypes.Value, error) {
			rows, _, err := m.materializeSubquery(node.Query, row)
			if err != nil {
				return sqltypes.Value{}, err
			}

			return sqltypes.BoolValue(len(rows) != 0), nil
		},
	), nil
}

func (m expressionMetadata) compileInSubquery(node *parser.InExpr) (executor.CompiledExpr, error) {
	expr, err := executor.CompileExpr(node.Expr, m)
	if err != nil {
		return executor.CompiledExpr{}, err
	}

	desc, ok := m.TypeOf(node)
	if !ok {
		desc = sqltypes.TypeDesc{Kind: sqltypes.TypeKindBoolean, Nullable: true}
	}

	return executor.NewCompiledExpr(
		desc,
		func(row executor.Row) (sqltypes.Value, error) {
			leftValue, err := expr.Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if leftValue.IsNull() {
				return sqltypes.NullValue(), nil
			}

			rows, columns, err := m.materializeSubquery(node.Query, row)
			if err != nil {
				return sqltypes.Value{}, err
			}

			column, err := requireSingleSubqueryColumn(node, columns, "IN subquery")
			if err != nil {
				return sqltypes.Value{}, err
			}

			sawNull := false
			for _, subqueryRow := range rows {
				itemValue, ok := subqueryRow.Value(0)
				if !ok {
					return sqltypes.Value{}, internalError(node, "IN subquery row is missing its projected column")
				}
				if itemValue.IsNull() {
					sawNull = true
					continue
				}

				comparison, err := executor.CompareValues(leftValue, expr.Type(), itemValue, column.Type)
				if err != nil {
					return sqltypes.Value{}, err
				}
				if comparison == 0 {
					return sqltypes.BoolValue(!node.Negated), nil
				}
			}

			if sawNull {
				return sqltypes.NullValue(), nil
			}

			return sqltypes.BoolValue(node.Negated), nil
		},
	), nil
}

func (m expressionMetadata) materializeSubquery(query parser.QueryExpr, outerRow executor.Row) ([]executor.Row, []planner.Column, error) {
	lowerer := planLowerer{
		bindings: m.bindings,
		types:    m.types,
		catalog:  m.catalog,
		store:    m.store,
		tx:       m.tx,
		outer: &correlatedContext{
			row:   outerRow.Clone(),
			shape: m.shape,
		},
	}

	lowered, err := lowerer.lowerQuery(query)
	if err != nil {
		return nil, nil, err
	}

	rows, err := materializeRows(lowered.op)
	if err != nil {
		return nil, nil, err
	}

	return rows, lowered.columns, nil
}

func requireSingleSubqueryColumn(node parser.Node, columns []planner.Column, context string) (planner.Column, error) {
	switch len(columns) {
	case 1:
		return columns[0], nil
	case 0:
		return planner.Column{}, internalError(node, "%s did not produce any columns", context)
	default:
		return planner.Column{}, internalError(node, "%s returned %d columns", context, len(columns))
	}
}

func cardinalityError(node parser.Node, format string, args ...any) error {
	return diagnosticError(
		internaldiag.NewError(
			sqlStateCardinalityViolation,
			fmt.Sprintf(format, args...),
			nodePosition(node),
		),
	)
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

func outerJoinShape(shape rowShape, nullable bool) rowShape {
	if !nullable {
		return shape
	}

	out := rowShape{
		columns: make([]shapeColumn, len(shape.columns)),
	}
	copy(out.columns, shape.columns)
	for index := range out.columns {
		out.columns[index].typ.Nullable = true
	}

	return out
}

func outerJoinColumns(columns []planner.Column, nullable bool) []planner.Column {
	out := append([]planner.Column(nil), columns...)
	if !nullable {
		return out
	}

	for index := range out {
		out[index].Type.Nullable = true
	}

	return out
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

func (l planLowerer) queryColumns(query parser.QueryExpr, outputs []sqltypes.TypeDesc) []planner.Column {
	relation, ok := l.boundRelation(query)
	if !ok || relation == nil {
		return nil
	}

	return columnsFromRelationBindingWithTypes(relation, l.types, outputs)
}

func (l planLowerer) queryOutputs(query parser.QueryExpr) ([]sqltypes.TypeDesc, bool) {
	if l.types == nil {
		return nil, false
	}

	return l.types.QueryOutputs(query)
}

func (l planLowerer) selectOutputs(stmt *parser.SelectStmt) ([]sqltypes.TypeDesc, bool) {
	return l.queryOutputs(stmt)
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
	case len(stmt.OrderBy) != 0:
		return featureError(stmt, "ORDER BY queries are not supported in Phase 1 planner")
	case stmt.Having != nil && !selectUsesAggregation(stmt):
		return featureError(stmt, "HAVING queries are not supported in Phase 1 planner")
	}

	return nil
}

func queryRequiresDirectLowering(query parser.QueryExpr) bool {
	return queryContainsJoin(query) || queryContainsAggregate(query)
}

func queryReferencesView(query parser.QueryExpr, bindings *analyzer.Bindings) bool {
	if bindings == nil {
		return false
	}

	switch query := query.(type) {
	case nil:
		return false
	case *parser.SelectStmt:
		for _, source := range query.From {
			if fromNodeReferencesView(source, bindings) {
				return true
			}
		}
		return false
	case *parser.SetOpExpr:
		return queryReferencesView(query.Left, bindings) || queryReferencesView(query.Right, bindings)
	default:
		return false
	}
}

func fromNodeReferencesView(node parser.Node, bindings *analyzer.Bindings) bool {
	switch node := node.(type) {
	case nil:
		return false
	case *parser.FromSource:
		if relation, ok := bindings.Relation(node); ok && relation != nil && relation.View != nil {
			return true
		}
		switch source := node.Source.(type) {
		case parser.QueryExpr:
			return queryReferencesView(source, bindings)
		case *parser.JoinExpr:
			return fromNodeReferencesView(source, bindings)
		default:
			return false
		}
	case *parser.JoinExpr:
		return fromNodeReferencesView(node.Left, bindings) || fromNodeReferencesView(node.Right, bindings)
	default:
		return false
	}
}

func queryContainsJoin(query parser.QueryExpr) bool {
	switch query := query.(type) {
	case nil:
		return false
	case *parser.SelectStmt:
		return selectContainsJoin(query)
	case *parser.SetOpExpr:
		return queryContainsJoin(query.Left) || queryContainsJoin(query.Right)
	default:
		return false
	}
}

func queryContainsAggregate(query parser.QueryExpr) bool {
	switch query := query.(type) {
	case nil:
		return false
	case *parser.SelectStmt:
		return selectUsesAggregation(query)
	case *parser.SetOpExpr:
		return queryContainsAggregate(query.Left) || queryContainsAggregate(query.Right)
	default:
		return false
	}
}

func selectUsesAggregation(stmt *parser.SelectStmt) bool {
	if stmt == nil {
		return false
	}
	if len(stmt.GroupBy) != 0 {
		return true
	}
	for _, item := range stmt.SelectList {
		if item != nil && containsAggregateInQueryBlock(item.Expr) {
			return true
		}
	}

	return containsAggregateInQueryBlock(stmt.Having)
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
	case parser.QueryExpr:
		return queryContainsJoin(node)
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

func containsAggregateInQueryBlock(node parser.Node) bool {
	switch node := node.(type) {
	case nil:
		return false
	case *parser.SelectStmt, *parser.SubqueryExpr, *parser.ExistsExpr:
		return false
	case *parser.FunctionCall:
		if isAggregateFunction(node) {
			return true
		}
	}

	found := false
	forEachAggregateExprChild(node, func(child parser.Node) {
		found = containsAggregateInQueryBlock(child) || found
	})
	return found
}

func isAggregateFunction(call *parser.FunctionCall) bool {
	return aggregateFunctionName(call) != ""
}

func aggregateFunctionName(call *parser.FunctionCall) string {
	if call == nil || call.Name == nil || len(call.Name.Parts) != 1 {
		return ""
	}

	switch strings.ToUpper(strings.TrimSpace(call.Name.Parts[0].Name)) {
	case "AVG", "COUNT", "EVERY", "MAX", "MIN", "SUM":
		return strings.ToUpper(strings.TrimSpace(call.Name.Parts[0].Name))
	default:
		return ""
	}
}

func isCountStarCall(call *parser.FunctionCall) bool {
	if aggregateFunctionName(call) != "COUNT" || len(call.Args) != 1 {
		return false
	}

	star, ok := call.Args[0].(*parser.Star)
	return ok && star != nil && star.Qualifier == nil
}

func forEachAggregateExprChild(node parser.Node, visit func(parser.Node)) {
	if node == nil || visit == nil {
		return
	}

	switch node := node.(type) {
	case *parser.UnaryExpr:
		visit(node.Operand)
	case *parser.BinaryExpr:
		visit(node.Left)
		visit(node.Right)
	case *parser.FunctionCall:
		for _, arg := range node.Args {
			visit(arg)
		}
	case *parser.CastExpr:
		visit(node.Expr)
		if node.Type != nil {
			for _, arg := range node.Type.Args {
				visit(arg)
			}
		}
	case *parser.WhenClause:
		visit(node.Condition)
		visit(node.Result)
	case *parser.CaseExpr:
		visit(node.Operand)
		for _, when := range node.Whens {
			visit(when)
		}
		visit(node.Else)
	case *parser.BetweenExpr:
		visit(node.Expr)
		visit(node.Lower)
		visit(node.Upper)
	case *parser.InExpr:
		visit(node.Expr)
		if node.Query != nil {
			return
		}
		for _, item := range node.List {
			visit(item)
		}
	case *parser.LikeExpr:
		visit(node.Expr)
		visit(node.Pattern)
		visit(node.Escape)
	case *parser.IsExpr:
		visit(node.Expr)
		visit(node.Right)
	case *parser.SelectItem:
		visit(node.Expr)
	case *parser.OrderByItem:
		visit(node.Expr)
	}
}

func (l planLowerer) groupExprKey(node parser.Node) string {
	switch node := node.(type) {
	case nil:
		return ""
	case *parser.Identifier, *parser.QualifiedName:
		if binding, ok := l.boundColumn(node); ok {
			return "column:" + bindingKey(binding)
		}
	case *parser.IntegerLiteral:
		return "integer:" + strings.TrimSpace(node.Text)
	case *parser.FloatLiteral:
		return "float:" + strings.TrimSpace(node.Text)
	case *parser.StringLiteral:
		return "string:" + node.Value
	case *parser.BoolLiteral:
		return fmt.Sprintf("bool:%t", node.Value)
	case *parser.NullLiteral:
		return "null"
	case *parser.ParamLiteral:
		return "param:" + node.Text
	case *parser.Star:
		if node.Qualifier == nil {
			return "star:*"
		}
		return "star:" + qualifiedNameString(node.Qualifier)
	case *parser.UnaryExpr:
		return "unary:" + node.Operator + "(" + l.groupExprKey(node.Operand) + ")"
	case *parser.BinaryExpr:
		return "binary:" + node.Operator + "(" + l.groupExprKey(node.Left) + "," + l.groupExprKey(node.Right) + ")"
	case *parser.FunctionCall:
		parts := make([]string, 0, len(node.Args)+2)
		parts = append(parts, "call:"+qualifiedNameString(node.Name), "set:"+node.SetQuantifier)
		for _, arg := range node.Args {
			parts = append(parts, l.groupExprKey(arg))
		}
		return strings.Join(parts, "|")
	case *parser.CastExpr:
		return "cast(" + l.groupExprKey(node.Expr) + " as " + typeNameKey(node.Type) + ")"
	case *parser.WhenClause:
		return "when(" + l.groupExprKey(node.Condition) + "=>" + l.groupExprKey(node.Result) + ")"
	case *parser.CaseExpr:
		parts := []string{"case", l.groupExprKey(node.Operand)}
		for _, when := range node.Whens {
			parts = append(parts, l.groupExprKey(when))
		}
		parts = append(parts, l.groupExprKey(node.Else))
		return strings.Join(parts, "|")
	case *parser.BetweenExpr:
		return "between(" + l.groupExprKey(node.Expr) + "," + l.groupExprKey(node.Lower) + "," + l.groupExprKey(node.Upper) + ")"
	case *parser.SubqueryExpr:
		return "subquery"
	case *parser.ExistsExpr:
		return "exists(subquery)"
	case *parser.InExpr:
		parts := []string{"in", l.groupExprKey(node.Expr)}
		if node.Query != nil {
			parts = append(parts, "subquery")
			return strings.Join(parts, "|")
		}
		for _, item := range node.List {
			parts = append(parts, l.groupExprKey(item))
		}
		return strings.Join(parts, "|")
	case *parser.LikeExpr:
		return "like(" + l.groupExprKey(node.Expr) + "," + l.groupExprKey(node.Pattern) + "," + l.groupExprKey(node.Escape) + ")"
	case *parser.IsExpr:
		return "is(" + l.groupExprKey(node.Expr) + "," + node.Predicate + "," + l.groupExprKey(node.Right) + ")"
	case *parser.SelectStmt:
		return "subquery"
	default:
		return fmt.Sprintf("%T", node)
	}

	return ""
}

func qualifiedNameString(name *parser.QualifiedName) string {
	if name == nil {
		return ""
	}

	parts := make([]string, 0, len(name.Parts))
	for _, part := range name.Parts {
		if part != nil {
			parts = append(parts, part.Name)
		}
	}

	return strings.Join(parts, ".")
}

func typeNameKey(node *parser.TypeName) string {
	if node == nil {
		return ""
	}

	parts := make([]string, 0, len(node.Args)+2)
	parts = append(parts, qualifiedNameString(node.Qualifier))
	for _, name := range node.Names {
		if name != nil {
			parts = append(parts, name.Name)
		}
	}
	for _, arg := range node.Args {
		parts = append(parts, fmt.Sprintf("%T", arg))
	}
	return strings.Join(parts, ".")
}

func bindingKey(binding *analyzer.ColumnBinding) string {
	if binding == nil {
		return ""
	}

	return fmt.Sprintf("%p", binding)
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
	outer        executor.Row
	hasOuter     bool

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
	outer executor.Row,
	hasOuter bool,
) *joinOperator {
	return &joinOperator{
		left:         left,
		right:        right,
		joinType:     strings.ToUpper(strings.TrimSpace(joinType)),
		predicate:    predicate,
		hasPredicate: hasPredicate,
		leftWidth:    leftWidth,
		rightWidth:   rightWidth,
		outer:        outer.Clone(),
		hasOuter:     hasOuter,
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
	if o.hasOuter {
		row = appendOuterRow(row, o.outer)
	}
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

func appendOuterRow(row executor.Row, outer executor.Row) executor.Row {
	if outer.Len() == 0 {
		return row
	}

	values := append(row.Values(), outer.Values()...)
	return executor.NewRowWithHandle(row.Handle, values...)
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

type appendOuterRowOperator struct {
	child     executor.Operator
	childOpen bool
	open      bool
	closed    bool
	outer     executor.Row
}

func newAppendOuterRowOperator(child executor.Operator, outer executor.Row) *appendOuterRowOperator {
	return &appendOuterRowOperator{
		child: child,
		outer: outer.Clone(),
	}
}

func (o *appendOuterRowOperator) Open() error {
	switch {
	case o.closed:
		return executor.ErrOperatorClosed
	case o.open:
		return executor.ErrOperatorOpen
	case o.child == nil:
		return internalError(nil, "append-outer child operator is nil")
	}

	if err := o.child.Open(); err != nil {
		return err
	}

	o.childOpen = true
	o.open = true
	return nil
}

func (o *appendOuterRowOperator) Next() (executor.Row, error) {
	switch {
	case !o.open && !o.closed:
		return executor.Row{}, executor.ErrOperatorNotOpen
	case o.closed:
		return executor.Row{}, executor.ErrOperatorClosed
	}

	row, err := o.child.Next()
	if err != nil {
		return executor.Row{}, err
	}

	return appendOuterRow(row, o.outer), nil
}

func (o *appendOuterRowOperator) Close() error {
	child := o.child
	childOpen := o.childOpen

	o.open = false
	o.closed = true
	o.childOpen = false

	if childOpen && child != nil {
		return child.Close()
	}

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
