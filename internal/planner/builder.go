package planner

import (
	"strings"

	"github.com/jamesdrando/tucotuco/internal/analyzer"
	"github.com/jamesdrando/tucotuco/internal/diag"
	"github.com/jamesdrando/tucotuco/internal/parser"
	sqltypes "github.com/jamesdrando/tucotuco/internal/types"
)

const (
	sqlStateFeatureNotSupported = "0A000"
	sqlStatePlannerInternal     = "XX000"
)

// Builder translates analyzer-validated parser CST nodes into logical plans.
type Builder struct {
	bindings *analyzer.Bindings
	types    *analyzer.Types
}

// NewBuilder constructs a logical plan builder over existing analyzer side
// tables.
func NewBuilder(bindings *analyzer.Bindings, types *analyzer.Types) *Builder {
	return &Builder{
		bindings: bindings,
		types:    types,
	}
}

// Build translates one analyzed statement into a logical plan.
func (b *Builder) Build(node parser.Node) (Plan, []diag.Diagnostic) {
	pass := buildPass{builder: b}
	plan := pass.build(node)
	if len(pass.diagnostics) != 0 {
		return nil, pass.diagnostics
	}

	return plan, nil
}

type buildPass struct {
	builder      *Builder
	diagnostics  []diag.Diagnostic
	outputCursor int
	outputTypes  []sqltypes.TypeDesc
}

func (p *buildPass) build(node parser.Node) Plan {
	switch node := node.(type) {
	case nil:
		return nil
	case parser.QueryExpr:
		return p.buildQuery(node)
	default:
		p.addFeatureError(node, "planner only supports SELECT statements in Phase 1")
		return nil
	}
}

func (p *buildPass) buildQuery(query parser.QueryExpr) Plan {
	switch query := query.(type) {
	case nil:
		return nil
	case *parser.SelectStmt:
		return p.buildSelect(query)
	case *parser.SetOpExpr:
		return p.buildSetOp(query)
	default:
		p.addFeatureError(query, "planner only supports SELECT statements in Phase 1")
		return nil
	}
}

func (p *buildPass) buildSelect(stmt *parser.SelectStmt) Plan {
	if stmt == nil {
		return nil
	}
	if !p.validateSelect(stmt) {
		return nil
	}

	input := p.buildSelectInput(stmt)
	if len(p.diagnostics) != 0 {
		return nil
	}
	if stmt.Where != nil {
		input = NewFilter(input, stmt.Where)
	}

	outputs, ok := p.selectOutputs(stmt)
	if !ok {
		return nil
	}

	project := &Project{Input: input}
	p.outputTypes = outputs
	p.outputCursor = 0
	for _, item := range stmt.SelectList {
		project.Projections = append(project.Projections, p.buildSelectItem(item, input)...)
		if len(p.diagnostics) != 0 {
			return nil
		}
	}
	if p.outputCursor != len(p.outputTypes) {
		p.addInternalError(stmt, "planner output metadata does not match the SELECT list")
		return nil
	}

	return project
}

func (p *buildPass) buildSetOp(expr *parser.SetOpExpr) Plan {
	if expr == nil {
		return nil
	}

	left := p.buildQuery(expr.Left)
	if left == nil || len(p.diagnostics) != 0 {
		return nil
	}
	right := p.buildQuery(expr.Right)
	if right == nil || len(p.diagnostics) != 0 {
		return nil
	}

	outputs, ok := p.queryOutputs(expr)
	if !ok {
		return nil
	}

	columns := p.queryColumns(expr, outputs)
	if len(columns) != len(outputs) {
		p.addInternalError(expr, "planner output metadata does not match the set operation")
		return nil
	}

	return NewSetOp(left, right, expr.Operator, expr.SetQuantifier, columns...)
}

func (p *buildPass) validateSelect(stmt *parser.SelectStmt) bool {
	switch {
	case stmt.SetQuantifier != "":
		p.addFeatureError(stmt, "DISTINCT queries are not supported in Phase 1 planner")
		return false
	case len(stmt.GroupBy) != 0:
		p.addFeatureError(stmt, "GROUP BY queries are not supported in Phase 1 planner")
		return false
	case stmt.Having != nil:
		p.addFeatureError(stmt, "HAVING queries are not supported in Phase 1 planner")
		return false
	case len(stmt.OrderBy) != 0:
		p.addFeatureError(stmt, "ORDER BY queries are not supported in Phase 1 planner")
		return false
	}

	for _, item := range stmt.SelectList {
		if item == nil || item.Expr == nil {
			continue
		}
		if containsAggregate(item.Expr) {
			p.addFeatureError(item.Expr, "aggregate queries are not supported in Phase 1 planner")
			return false
		}
	}

	return true
}

func (p *buildPass) buildSelectInput(stmt *parser.SelectStmt) Plan {
	if stmt == nil || len(stmt.From) == 0 {
		return nil
	}

	input := p.buildFromNode(stmt.From[0])
	if input == nil || len(p.diagnostics) != 0 {
		return nil
	}

	for _, source := range stmt.From[1:] {
		right := p.buildFromNode(source)
		if right == nil || len(p.diagnostics) != 0 {
			return nil
		}

		input = NewJoin(input, right, "CROSS", nil, joinColumns("CROSS", input.Columns(), right.Columns())...)
	}

	return input
}

func (p *buildPass) buildFromNode(node parser.Node) Plan {
	switch node := node.(type) {
	case nil:
		return nil
	case *parser.FromSource:
		return p.buildFromSource(node)
	case *parser.JoinExpr:
		return p.buildJoin(node)
	default:
		p.addFeatureError(node, "FROM source is not supported in Phase 1 planner")
		return nil
	}
}

func (p *buildPass) buildFromSource(source *parser.FromSource) Plan {
	if source == nil {
		return nil
	}

	switch inner := source.Source.(type) {
	case *parser.QualifiedName:
		relation, ok := p.boundRelation(source, inner)
		if !ok || relation == nil {
			p.addInternalError(source, "planner is missing relation metadata for the FROM source")
			return nil
		}

		return NewScan(relation.TableID, relationColumns(relation)...)
	case parser.QueryExpr:
		return p.attachBoundRelation(p.buildQuery(inner), source)
	case *parser.JoinExpr:
		return p.attachBoundRelation(p.buildJoin(inner), source)
	default:
		p.addFeatureError(source, "FROM source is not supported in Phase 1 planner")
		return nil
	}
}

func (p *buildPass) buildJoin(join *parser.JoinExpr) Plan {
	if join == nil {
		return nil
	}
	if join.Natural {
		p.addFeatureError(join, "NATURAL JOIN planning is not supported in Phase 2 planner")
		return nil
	}
	if len(join.Using) != 0 {
		p.addFeatureError(join, "JOIN ... USING is not supported in Phase 2 planner")
		return nil
	}

	left := p.buildFromNode(join.Left)
	if left == nil || len(p.diagnostics) != 0 {
		return nil
	}

	right := p.buildFromNode(join.Right)
	if right == nil || len(p.diagnostics) != 0 {
		return nil
	}

	return NewJoin(left, right, join.Type, join.Condition, joinColumns(join.Type, left.Columns(), right.Columns())...)
}

func (p *buildPass) buildSelectItem(item *parser.SelectItem, input Plan) []Projection {
	if item == nil || item.Expr == nil {
		return nil
	}

	switch expr := item.Expr.(type) {
	case *parser.Star:
		return p.buildStar(expr, input)
	default:
		desc, ok := p.nextOutputType(item)
		if !ok {
			return nil
		}

		output := Column{
			Name: projectedColumnName(item),
			Type: desc,
		}
		if binding, ok := p.boundColumn(expr); ok {
			output.Binding = binding
			output.RelationName = safeBindingRelation(binding)
			if matched, ok := matchPlanColumn(input, binding); ok {
				output.Type = matched.Type
			}
		}

		return []Projection{{Expr: expr, Output: output}}
	}
}

func (p *buildPass) buildStar(star *parser.Star, input Plan) []Projection {
	if star == nil {
		return nil
	}

	bindings := p.bindings()
	if bindings == nil {
		p.addInternalError(star, "planner requires analyzer bindings")
		return nil
	}

	columns, ok := bindings.Star(star)
	if !ok {
		p.addInternalError(star, "planner is missing star expansion metadata")
		return nil
	}

	projections := make([]Projection, 0, len(columns))
	for _, column := range columns {
		desc, ok := p.nextOutputType(star)
		if !ok {
			return nil
		}
		output := Column{
			Name:         safeBindingName(column),
			Type:         desc,
			RelationName: safeBindingRelation(column),
			Binding:      column,
		}
		if matched, ok := matchPlanColumn(input, column); ok {
			output = matched
		}

		projections = append(projections, Projection{
			Expr:   syntheticColumnRef(column),
			Output: output,
		})
	}

	return projections
}

func (p *buildPass) queryOutputs(query parser.QueryExpr) ([]sqltypes.TypeDesc, bool) {
	types := p.types()
	if types == nil {
		p.addInternalError(query, "planner requires analyzer type information")
		return nil, false
	}

	outputs, ok := types.QueryOutputs(query)
	if !ok {
		p.addInternalError(query, "planner is missing query output metadata")
		return nil, false
	}

	return outputs, true
}

func (p *buildPass) selectOutputs(stmt *parser.SelectStmt) ([]sqltypes.TypeDesc, bool) {
	return p.queryOutputs(stmt)
}

func (p *buildPass) nextOutputType(node parser.Node) (sqltypes.TypeDesc, bool) {
	if p.outputCursor >= len(p.outputTypes) {
		p.addInternalError(node, "planner output metadata does not match the SELECT list")
		return sqltypes.TypeDesc{}, false
	}

	desc := p.outputTypes[p.outputCursor]
	p.outputCursor++
	return desc, true
}

func (p *buildPass) boundRelation(nodes ...parser.Node) (*analyzer.RelationBinding, bool) {
	bindings := p.bindings()
	if bindings == nil {
		return nil, false
	}

	for _, node := range nodes {
		if node == nil {
			continue
		}

		relation, ok := bindings.Relation(node)
		if ok {
			return relation, true
		}
	}

	return nil, false
}

func (p *buildPass) boundColumn(node parser.Node) (*analyzer.ColumnBinding, bool) {
	bindings := p.bindings()
	if bindings == nil || node == nil {
		return nil, false
	}

	return bindings.Column(node)
}

func (p *buildPass) bindings() *analyzer.Bindings {
	if p.builder == nil {
		return nil
	}

	return p.builder.bindings
}

func (p *buildPass) types() *analyzer.Types {
	if p.builder == nil {
		return nil
	}

	return p.builder.types
}

func (p *buildPass) addFeatureError(node parser.Node, message string) {
	p.addDiagnostic(sqlStateFeatureNotSupported, node, message)
}

func (p *buildPass) addInternalError(node parser.Node, message string) {
	p.addDiagnostic(sqlStatePlannerInternal, node, message)
}

func (p *buildPass) addDiagnostic(sqlState string, node parser.Node, message string) {
	p.diagnostics = append(p.diagnostics, diag.NewError(sqlState, message, diagPosition(node)))
}

func relationColumns(relation *analyzer.RelationBinding) []Column {
	if relation == nil {
		return nil
	}

	columns := make([]Column, 0, len(relation.Columns))
	for _, binding := range relation.Columns {
		columns = append(columns, Column{
			Name:         safeBindingName(binding),
			Type:         bindingType(binding),
			RelationName: safeBindingRelation(binding),
			Binding:      binding,
		})
	}

	return columns
}

func relationColumnsWithTypes(relation *analyzer.RelationBinding, outputs []sqltypes.TypeDesc) []Column {
	if relation == nil {
		return nil
	}

	columns := make([]Column, 0, len(relation.Columns))
	for index, binding := range relation.Columns {
		column := Column{
			Name:         safeBindingName(binding),
			Type:         bindingType(binding),
			RelationName: safeBindingRelation(binding),
			Binding:      binding,
		}
		if column.Type.Kind == sqltypes.TypeKindInvalid && index < len(outputs) {
			column.Type = outputs[index]
		}
		columns = append(columns, column)
	}

	return columns
}

func bindingType(binding *analyzer.ColumnBinding) sqltypes.TypeDesc {
	if binding == nil || binding.Descriptor == nil {
		return sqltypes.TypeDesc{}
	}

	return binding.Descriptor.Type
}

func safeBindingName(binding *analyzer.ColumnBinding) string {
	if binding == nil {
		return ""
	}

	return binding.Name
}

func safeBindingRelation(binding *analyzer.ColumnBinding) string {
	if binding == nil || binding.Relation == nil {
		return ""
	}

	return binding.Relation.Name
}

func syntheticColumnRef(binding *analyzer.ColumnBinding) parser.Node {
	if binding == nil {
		return &parser.Identifier{}
	}

	relation := safeBindingRelation(binding)
	if relation == "" {
		return &parser.Identifier{Name: safeBindingName(binding)}
	}

	return &parser.QualifiedName{
		Parts: []*parser.Identifier{
			{Name: relation},
			{Name: safeBindingName(binding)},
		},
	}
}

func joinColumns(joinType string, left []Column, right []Column) []Column {
	output := make([]Column, 0, len(left)+len(right))
	output = append(output, outerJoinColumns(left, joinType == "RIGHT" || joinType == "FULL")...)
	output = append(output, outerJoinColumns(right, joinType == "LEFT" || joinType == "FULL")...)
	return output
}

func outerJoinColumns(columns []Column, nullable bool) []Column {
	out := make([]Column, len(columns))
	for index, column := range columns {
		out[index] = column
		if nullable {
			out[index].Type.Nullable = true
		}
	}

	return out
}

func matchPlanColumn(plan Plan, binding *analyzer.ColumnBinding) (Column, bool) {
	if plan == nil || binding == nil {
		return Column{}, false
	}

	return matchColumnBinding(plan.Columns(), binding)
}

func matchColumnBinding(columns []Column, binding *analyzer.ColumnBinding) (Column, bool) {
	if binding == nil {
		return Column{}, false
	}

	for _, column := range columns {
		if column.Binding == binding {
			return column, true
		}
	}

	relation := safeBindingRelation(binding)
	if relation != "" {
		var matches []Column
		for _, column := range columns {
			if column.RelationName == relation && column.Name == binding.Name {
				matches = append(matches, column)
			}
		}
		if len(matches) == 1 {
			return matches[0], true
		}
	}

	var matches []Column
	for _, column := range columns {
		if column.Name == binding.Name {
			matches = append(matches, column)
		}
	}
	if len(matches) == 1 {
		return matches[0], true
	}

	return Column{}, false
}

func (p *buildPass) attachBoundRelation(plan Plan, source *parser.FromSource) Plan {
	if plan == nil || source == nil {
		return plan
	}

	relation, ok := p.boundRelation(source)
	if !ok || relation == nil {
		return plan
	}
	if len(relation.Columns) == 0 {
		return plan
	}

	switch node := plan.(type) {
	case *Project:
		if len(node.Projections) != len(relation.Columns) {
			p.addInternalError(source, "planner relation metadata does not match derived-table outputs")
			return nil
		}
		for index, binding := range relation.Columns {
			node.Projections[index].Output = mergeBoundColumn(node.Projections[index].Output, binding)
		}
	case *Join:
		if len(node.OutputColumns) != len(relation.Columns) {
			p.addInternalError(source, "planner relation metadata does not match joined outputs")
			return nil
		}
		for index, binding := range relation.Columns {
			node.OutputColumns[index] = mergeBoundColumn(node.OutputColumns[index], binding)
		}
	case *Scan:
		if len(node.OutputColumns) != len(relation.Columns) {
			p.addInternalError(source, "planner relation metadata does not match scan outputs")
			return nil
		}
		for index, binding := range relation.Columns {
			node.OutputColumns[index] = mergeBoundColumn(node.OutputColumns[index], binding)
		}
	case *SetOp:
		if len(node.OutputColumns) != len(relation.Columns) {
			p.addInternalError(source, "planner relation metadata does not match set-operation outputs")
			return nil
		}
		for index, binding := range relation.Columns {
			node.OutputColumns[index] = mergeBoundColumn(node.OutputColumns[index], binding)
		}
	}

	return plan
}

func (p *buildPass) queryColumns(query parser.QueryExpr, outputs []sqltypes.TypeDesc) []Column {
	relation, ok := p.boundRelation(query)
	if !ok || relation == nil {
		return nil
	}

	return relationColumnsWithTypes(relation, outputs)
}

func mergeBoundColumn(column Column, binding *analyzer.ColumnBinding) Column {
	column.Binding = binding
	column.RelationName = safeBindingRelation(binding)
	if column.Name == "" {
		column.Name = safeBindingName(binding)
	}

	return column
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
		if len(expr.Parts) == 0 {
			return ""
		}
		return expr.Parts[len(expr.Parts)-1].Name
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

	name := call.Name.Parts[len(call.Name.Parts)-1].Name
	switch strings.ToUpper(name) {
	case "AVG", "COUNT", "MAX", "MIN", "SUM":
		return true
	default:
		return false
	}
}

func diagPosition(node parser.Node) diag.Position {
	if node == nil {
		return diag.Position{}
	}

	pos := node.Pos()
	return diag.Position{
		Line:   pos.Line,
		Column: pos.Column,
		Offset: pos.Offset,
	}
}
