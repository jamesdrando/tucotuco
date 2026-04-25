package analyzer

import (
	"fmt"
	"strings"

	"github.com/jamesdrando/tucotuco/internal/parser"
)

const sqlStateGroupingError = "42803"

type groupedQueryContext struct {
	required       bool
	groupedExprs   map[string]struct{}
	groupedColumns map[*ColumnBinding]struct{}
}

func (p *typeCheckPass) validateAggregatePlacement(script *parser.Script) {
	if script == nil {
		return
	}

	for _, node := range script.Nodes {
		p.validateAggregateStatement(node)
	}
}

func (p *typeCheckPass) validateAggregateStatement(node parser.Node) {
	switch node := node.(type) {
	case *parser.SelectStmt:
		p.validateAggregateSelect(node)
	case *parser.ExplainStmt:
		if node.Analyze {
			return
		}
		p.validateAggregateQuery(node.Query)
	case *parser.InsertStmt:
		p.validateAggregateInsert(node)
	case *parser.UpdateStmt:
		p.validateAggregateUpdate(node)
	case *parser.DeleteStmt:
		p.validateAggregateDelete(node)
	case *parser.CreateTableStmt:
		p.validateAggregateCreateTable(node)
	case *parser.CreateViewStmt:
		p.validateAggregateQuery(node.Query)
	}
}

func (p *typeCheckPass) validateAggregateQuery(query parser.QueryExpr) {
	switch query := query.(type) {
	case nil:
		return
	case *parser.SelectStmt:
		p.validateAggregateSelect(query)
	case *parser.SetOpExpr:
		p.validateAggregateQuery(query.Left)
		p.validateAggregateQuery(query.Right)
	}
}

func (p *typeCheckPass) validateAggregateSelect(stmt *parser.SelectStmt) {
	if stmt == nil {
		return
	}

	for _, source := range stmt.From {
		p.validateAggregateFromNode(source)
	}

	p.validateNoAggregatesInExpr(stmt.Where, "WHERE clause")
	for _, expr := range stmt.GroupBy {
		p.validateNoAggregatesInExpr(expr, "GROUP BY clause")
	}

	for _, item := range stmt.SelectList {
		if item == nil {
			continue
		}
		p.validateAggregateExpr(item.Expr, "")
	}
	p.validateAggregateExpr(stmt.Having, "")
	for _, item := range stmt.OrderBy {
		if item == nil {
			continue
		}
		p.validateAggregateExpr(item.Expr, "")
	}

	grouping := p.newGroupedQueryContext(stmt)
	if !grouping.required {
		return
	}

	for _, item := range stmt.SelectList {
		if item == nil {
			continue
		}
		p.validateGroupedExpr(item.Expr, grouping)
	}
	p.validateGroupedExpr(stmt.Having, grouping)
	for _, item := range stmt.OrderBy {
		if item == nil {
			continue
		}
		p.validateGroupedExpr(item.Expr, grouping)
	}
}

func (p *typeCheckPass) validateAggregateFromNode(node parser.Node) {
	switch node := node.(type) {
	case *parser.FromSource:
		p.validateAggregateFromSource(node)
	case *parser.JoinExpr:
		p.validateAggregateJoin(node)
	}
}

func (p *typeCheckPass) validateAggregateFromSource(source *parser.FromSource) {
	if source == nil {
		return
	}

	switch inner := source.Source.(type) {
	case *parser.SelectStmt:
		p.validateAggregateSelect(inner)
	case *parser.JoinExpr:
		p.validateAggregateJoin(inner)
	}
}

func (p *typeCheckPass) validateAggregateJoin(join *parser.JoinExpr) {
	if join == nil {
		return
	}

	p.validateAggregateFromNode(join.Left)
	p.validateAggregateFromNode(join.Right)
	p.validateNoAggregatesInExpr(join.Condition, "JOIN condition")
}

func (p *typeCheckPass) validateAggregateInsert(stmt *parser.InsertStmt) {
	if stmt == nil {
		return
	}

	switch source := stmt.Source.(type) {
	case *parser.InsertValuesSource:
		for _, row := range source.Rows {
			for _, value := range row {
				p.validateNoAggregatesInExpr(value, "INSERT value")
			}
		}
	case *parser.InsertQuerySource:
		if query, ok := source.Query.(*parser.SelectStmt); ok {
			p.validateAggregateSelect(query)
		}
	}
}

func (p *typeCheckPass) validateAggregateUpdate(stmt *parser.UpdateStmt) {
	if stmt == nil {
		return
	}

	for _, assignment := range stmt.Assignments {
		if assignment == nil {
			continue
		}
		for _, value := range assignment.Values {
			p.validateNoAggregatesInExpr(value, "UPDATE value")
		}
	}

	p.validateNoAggregatesInExpr(stmt.Where, "UPDATE WHERE clause")
}

func (p *typeCheckPass) validateAggregateDelete(stmt *parser.DeleteStmt) {
	if stmt == nil {
		return
	}

	p.validateNoAggregatesInExpr(stmt.Where, "DELETE WHERE clause")
}

func (p *typeCheckPass) validateAggregateCreateTable(stmt *parser.CreateTableStmt) {
	if stmt == nil {
		return
	}

	for _, column := range stmt.Columns {
		if column == nil {
			continue
		}

		p.validateNoAggregatesInExpr(column.Default, "DEFAULT expression")
		for _, constraint := range column.Constraints {
			p.validateAggregateConstraint(constraint)
		}
	}

	for _, constraint := range stmt.Constraints {
		p.validateAggregateConstraint(constraint)
	}
}

func (p *typeCheckPass) validateAggregateConstraint(constraint *parser.ConstraintDef) {
	if constraint == nil {
		return
	}
	if constraint.Kind == parser.ConstraintKindCheck {
		p.validateNoAggregatesInExpr(constraint.Check, "CHECK constraint")
	}
}

func (p *typeCheckPass) validateNoAggregatesInExpr(node parser.Node, clause string) {
	switch node := node.(type) {
	case nil:
		return
	case *parser.SelectStmt:
		p.validateAggregateSelect(node)
	case *parser.FunctionCall:
		if isAggregateFunctionCall(node) {
			p.addError(sqlStateGroupingError, node.Pos(), "aggregate function %q is not allowed in %s", aggregateFunctionDisplayName(node.Name), clause)
			for _, arg := range node.Args {
				p.validateSubqueriesInExpr(arg)
			}
			return
		}

		forEachAggregateExprChild(node, func(child parser.Node) {
			p.validateNoAggregatesInExpr(child, clause)
		})
	default:
		forEachAggregateExprChild(node, func(child parser.Node) {
			p.validateNoAggregatesInExpr(child, clause)
		})
	}
}

func (p *typeCheckPass) validateSubqueriesInExpr(node parser.Node) {
	switch node := node.(type) {
	case nil:
		return
	case *parser.SelectStmt:
		p.validateAggregateSelect(node)
	default:
		forEachAggregateExprChild(node, p.validateSubqueriesInExpr)
	}
}

func (p *typeCheckPass) validateAggregateExpr(node parser.Node, enclosingAggregate string) bool {
	switch node := node.(type) {
	case nil:
		return true
	case *parser.SelectStmt:
		p.validateAggregateSelect(node)
		return true
	}

	switch node := node.(type) {
	case *parser.FunctionCall:
		if isAggregateFunctionCall(node) {
			name := aggregateFunctionDisplayName(node.Name)
			if enclosingAggregate != "" {
				p.addError(sqlStateGroupingError, node.Pos(), "aggregate function %q cannot contain aggregate function %q", enclosingAggregate, name)
				for _, arg := range node.Args {
					p.validateSubqueriesInExpr(arg)
				}
				return false
			}

			ok := true
			for _, arg := range node.Args {
				ok = p.validateAggregateExpr(arg, name) && ok
			}
			return ok
		}
	}

	ok := true
	forEachAggregateExprChild(node, func(child parser.Node) {
		ok = p.validateAggregateExpr(child, enclosingAggregate) && ok
	})
	return ok
}

func (p *typeCheckPass) newGroupedQueryContext(stmt *parser.SelectStmt) groupedQueryContext {
	context := groupedQueryContext{
		required:       len(stmt.GroupBy) > 0 || p.selectHasAggregate(stmt),
		groupedExprs:   make(map[string]struct{}, len(stmt.GroupBy)),
		groupedColumns: make(map[*ColumnBinding]struct{}),
	}
	if !context.required {
		return context
	}

	for _, expr := range stmt.GroupBy {
		key := p.groupExprKey(expr)
		if key != "" {
			context.groupedExprs[key] = struct{}{}
		}
		if binding, ok := p.lookupBoundColumn(expr); ok {
			context.groupedColumns[binding] = struct{}{}
		}
	}

	return context
}

func (p *typeCheckPass) selectHasAggregate(stmt *parser.SelectStmt) bool {
	if stmt == nil {
		return false
	}

	for _, item := range stmt.SelectList {
		if item != nil && containsAggregateInQueryBlock(item.Expr) {
			return true
		}
	}
	if containsAggregateInQueryBlock(stmt.Having) {
		return true
	}
	for _, item := range stmt.OrderBy {
		if item != nil && containsAggregateInQueryBlock(item.Expr) {
			return true
		}
	}

	return false
}

func containsAggregateInQueryBlock(node parser.Node) bool {
	switch node := node.(type) {
	case nil:
		return false
	case *parser.SelectStmt:
		return false
	case *parser.FunctionCall:
		if isAggregateFunctionCall(node) {
			return true
		}
	}

	found := false
	forEachAggregateExprChild(node, func(child parser.Node) {
		found = containsAggregateInQueryBlock(child) || found
	})
	return found
}

func (p *typeCheckPass) validateGroupedExpr(node parser.Node, context groupedQueryContext) bool {
	switch node := node.(type) {
	case nil:
		return true
	case *parser.SelectStmt:
		p.validateAggregateSelect(node)
		return true
	case *parser.Star:
		p.addError(sqlStateGroupingError, node.Pos(), "SELECT * is not allowed with GROUP BY or aggregate functions")
		return false
	case *parser.FunctionCall:
		if isAggregateFunctionCall(node) {
			return true
		}
	}

	if _, ok := context.groupedExprs[p.groupExprKey(node)]; ok {
		return true
	}

	if binding, ok := p.lookupBoundColumn(node); ok {
		if item, ok := binding.Source.(*parser.SelectItem); ok {
			return p.validateGroupedExpr(item.Expr, context)
		}
		if _, ok := context.groupedColumns[binding]; ok {
			return true
		}

		p.addError(sqlStateGroupingError, node.Pos(), "column %q must appear in GROUP BY or be used in an aggregate function", displayColumnBinding(binding))
		return false
	}

	ok := true
	forEachAggregateExprChild(node, func(child parser.Node) {
		ok = p.validateGroupedExpr(child, context) && ok
	})
	return ok
}

func (p *typeCheckPass) groupExprKey(node parser.Node) string {
	switch node := node.(type) {
	case nil:
		return ""
	case *parser.Identifier, *parser.QualifiedName:
		if binding, ok := p.lookupBoundColumn(node); ok {
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
		return "unary:" + node.Operator + "(" + p.groupExprKey(node.Operand) + ")"
	case *parser.BinaryExpr:
		return "binary:" + node.Operator + "(" + p.groupExprKey(node.Left) + "," + p.groupExprKey(node.Right) + ")"
	case *parser.FunctionCall:
		parts := make([]string, 0, len(node.Args)+2)
		parts = append(parts, "call:"+qualifiedNameString(node.Name), "set:"+node.SetQuantifier)
		for _, arg := range node.Args {
			parts = append(parts, p.groupExprKey(arg))
		}
		return strings.Join(parts, "|")
	case *parser.CastExpr:
		return "cast(" + p.groupExprKey(node.Expr) + " as " + typeNameKey(node.Type) + ")"
	case *parser.WhenClause:
		return "when(" + p.groupExprKey(node.Condition) + "=>" + p.groupExprKey(node.Result) + ")"
	case *parser.CaseExpr:
		parts := []string{"case", p.groupExprKey(node.Operand)}
		for _, when := range node.Whens {
			parts = append(parts, p.groupExprKey(when))
		}
		parts = append(parts, p.groupExprKey(node.Else))
		return strings.Join(parts, "|")
	case *parser.BetweenExpr:
		return "between(" + p.groupExprKey(node.Expr) + "," + p.groupExprKey(node.Lower) + "," + p.groupExprKey(node.Upper) + ")"
	case *parser.SubqueryExpr:
		return "subquery"
	case *parser.ExistsExpr:
		return "exists(subquery)"
	case *parser.InExpr:
		parts := []string{"in", p.groupExprKey(node.Expr)}
		if node.Query != nil {
			parts = append(parts, "subquery")
			return strings.Join(parts, "|")
		}
		for _, item := range node.List {
			parts = append(parts, p.groupExprKey(item))
		}
		return strings.Join(parts, "|")
	case *parser.LikeExpr:
		return "like(" + p.groupExprKey(node.Expr) + "," + p.groupExprKey(node.Pattern) + "," + p.groupExprKey(node.Escape) + ")"
	case *parser.IsExpr:
		return "is(" + p.groupExprKey(node.Expr) + "," + node.Predicate + "," + p.groupExprKey(node.Right) + ")"
	case *parser.SelectStmt:
		return "subquery"
	}

	return fmt.Sprintf("%T", node)
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

func bindingKey(binding *ColumnBinding) string {
	if binding == nil {
		return ""
	}

	return fmt.Sprintf("%p", binding)
}

func displayColumnBinding(binding *ColumnBinding) string {
	if binding == nil {
		return ""
	}

	return qualifiedColumnName(displayRelationName(binding.Relation), binding.Name)
}

func displayRelationName(relation *RelationBinding) string {
	if relation == nil {
		return ""
	}
	if relation.Descriptor != nil && relation.TableID.Name != "" {
		return relation.TableID.Name
	}
	return relation.Name
}

func isAggregateFunctionCall(node *parser.FunctionCall) bool {
	if node == nil || node.Name == nil || len(node.Name.Parts) != 1 {
		return false
	}

	switch functionName(node.Name) {
	case "AVG", "COUNT", "EVERY", "MAX", "MIN", "SUM":
		return true
	default:
		return false
	}
}

func aggregateFunctionDisplayName(name *parser.QualifiedName) string {
	if name == nil {
		return ""
	}

	if base := functionName(name); base != "" {
		return strings.ToLower(base)
	}

	return strings.ToLower(qualifiedNameString(name))
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
	case *parser.SubqueryExpr:
		visit(node.Query)
	case *parser.ExistsExpr:
		visit(node.Query)
	case *parser.InExpr:
		visit(node.Expr)
		if node.Query != nil {
			visit(node.Query)
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
