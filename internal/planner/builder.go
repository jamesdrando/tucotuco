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
	case *parser.SelectStmt:
		return p.buildSelect(node)
	default:
		p.addFeatureError(node, "planner only supports SELECT statements in Phase 1")
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
		project.Projections = append(project.Projections, p.buildSelectItem(item)...)
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
	if len(stmt.From) != 1 {
		p.addFeatureError(stmt.From[1], "multiple FROM sources are not supported in Phase 1 planner")
		return nil
	}

	return p.buildFromNode(stmt.From[0])
}

func (p *buildPass) buildFromNode(node parser.Node) Plan {
	switch node := node.(type) {
	case nil:
		return nil
	case *parser.FromSource:
		return p.buildFromSource(node)
	case *parser.JoinExpr:
		p.addFeatureError(node, "JOIN planning is not supported in Phase 1 planner")
		return nil
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
	case *parser.SelectStmt:
		return p.buildSelect(inner)
	case *parser.JoinExpr:
		p.addFeatureError(inner, "JOIN planning is not supported in Phase 1 planner")
		return nil
	default:
		p.addFeatureError(source, "FROM source is not supported in Phase 1 planner")
		return nil
	}
}

func (p *buildPass) buildSelectItem(item *parser.SelectItem) []Projection {
	if item == nil || item.Expr == nil {
		return nil
	}

	switch expr := item.Expr.(type) {
	case *parser.Star:
		return p.buildStar(expr)
	default:
		desc, ok := p.nextOutputType(item)
		if !ok {
			return nil
		}

		return []Projection{{
			Expr: expr,
			Output: Column{
				Name: projectedColumnName(item),
				Type: desc,
			},
		}}
	}
}

func (p *buildPass) buildStar(star *parser.Star) []Projection {
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
		projections = append(projections, Projection{
			Expr: syntheticColumnRef(column),
			Output: Column{
				Name: safeBindingName(column),
				Type: desc,
			},
		})
	}

	return projections
}

func (p *buildPass) selectOutputs(stmt *parser.SelectStmt) ([]sqltypes.TypeDesc, bool) {
	types := p.types()
	if types == nil {
		p.addInternalError(stmt, "planner requires analyzer type information")
		return nil, false
	}

	outputs, ok := types.SelectOutputs(stmt)
	if !ok {
		p.addInternalError(stmt, "planner is missing SELECT output metadata")
		return nil, false
	}

	return outputs, true
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
			Name: safeBindingName(binding),
			Type: bindingType(binding),
		})
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

func syntheticColumnRef(binding *analyzer.ColumnBinding) parser.Node {
	return &parser.Identifier{Name: safeBindingName(binding)}
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
