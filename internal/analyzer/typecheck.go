package analyzer

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jamesdrando/tucotuco/internal/diag"
	"github.com/jamesdrando/tucotuco/internal/parser"
	"github.com/jamesdrando/tucotuco/internal/token"
	sqtypes "github.com/jamesdrando/tucotuco/internal/types"
)

const (
	sqlStateCannotCoerce    = "42846"
	sqlStateUndefinedObject = "42704"
)

// CheckScript assigns semantic types to the current script and returns the
// side table together with any diagnostics that were produced.
func (c *TypeChecker) CheckScript(script *parser.Script) (*Types, []diag.Diagnostic) {
	pass := typeCheckPass{
		checker:     c,
		types:       newTypes(),
		columnTypes: make(map[*parser.ColumnDef]sqtypes.TypeDesc),
	}
	pass.checkScript(script)
	pass.validateAggregatePlacement(script)

	return pass.types, pass.diagnostics
}

func (p *typeCheckPass) checkScript(script *parser.Script) {
	if script == nil {
		return
	}

	for _, node := range script.Nodes {
		p.checkStatement(node)
	}
}

func (p *typeCheckPass) checkStatement(node parser.Node) {
	switch node := node.(type) {
	case parser.QueryExpr:
		p.checkQuery(node)
	case *parser.InsertStmt:
		p.checkInsert(node)
	case *parser.UpdateStmt:
		p.checkUpdate(node)
	case *parser.DeleteStmt:
		p.checkDelete(node)
	case *parser.CreateTableStmt:
		p.checkCreateTable(node)
	}
}

func (p *typeCheckPass) checkQuery(query parser.QueryExpr) []sqtypes.TypeDesc {
	switch query := query.(type) {
	case nil:
		return nil
	case *parser.SelectStmt:
		return p.checkSelect(query)
	case *parser.SetOpExpr:
		return p.checkSetOp(query)
	default:
		return nil
	}
}

func (p *typeCheckPass) checkSelect(stmt *parser.SelectStmt) []sqtypes.TypeDesc {
	if stmt == nil {
		return nil
	}
	if outputs, ok := p.types.QueryOutputs(stmt); ok {
		return outputs
	}

	// Install a placeholder so derived-table recursion can terminate cleanly.
	p.types.bindQuery(stmt, nil)

	for _, source := range stmt.From {
		p.checkFromNode(source)
	}

	if stmt.Where != nil {
		p.requireBoolean(stmt.Where, p.exprType(stmt.Where), "WHERE clause")
	}

	for _, expr := range stmt.GroupBy {
		p.exprType(expr)
	}

	if stmt.Having != nil {
		p.requireBoolean(stmt.Having, p.exprType(stmt.Having), "HAVING clause")
	}

	outputs := make([]sqtypes.TypeDesc, 0, len(stmt.SelectList))
	for _, item := range stmt.SelectList {
		outputs = append(outputs, p.selectItemTypes(item)...)
	}

	for _, item := range stmt.OrderBy {
		if item == nil {
			continue
		}
		p.exprType(item.Expr)
	}

	p.types.bindQuery(stmt, outputs)
	return outputs
}

func (p *typeCheckPass) checkSetOp(expr *parser.SetOpExpr) []sqtypes.TypeDesc {
	if expr == nil {
		return nil
	}
	if outputs, ok := p.types.QueryOutputs(expr); ok {
		return outputs
	}

	p.types.bindQuery(expr, nil)

	left := p.checkQuery(expr.Left)
	right := p.checkQuery(expr.Right)
	if len(left) != len(right) {
		p.addError(
			sqlStateDatatypeMismatch,
			expr.Pos(),
			"%s queries return %d and %d columns",
			expr.Operator,
			len(left),
			len(right),
		)
		return nil
	}

	outputs := make([]sqtypes.TypeDesc, len(left))
	for index := range left {
		common, ok := sqtypes.CommonSuperType(left[index], right[index])
		if !ok {
			p.addError(
				sqlStateDatatypeMismatch,
				expr.Pos(),
				"%s column %d types %s and %s are incompatible",
				expr.Operator,
				index+1,
				typeString(left[index]),
				typeString(right[index]),
			)
			continue
		}
		outputs[index] = common
	}

	p.types.bindQuery(expr, outputs)
	return outputs
}

func (p *typeCheckPass) selectItemTypes(item *parser.SelectItem) []sqtypes.TypeDesc {
	if item == nil || item.Expr == nil {
		return nil
	}

	switch expr := item.Expr.(type) {
	case *parser.Star:
		bindings := p.bindings()
		if bindings == nil {
			return nil
		}

		columns, ok := bindings.Star(expr)
		if !ok {
			return nil
		}

		out := make([]sqtypes.TypeDesc, 0, len(columns))
		for _, column := range columns {
			desc, ok := p.typeOfBinding(column)
			if !ok {
				continue
			}
			out = append(out, desc)
		}
		return out
	default:
		desc := p.exprType(item.Expr)
		p.types.bindExpr(item, desc)
		return []sqtypes.TypeDesc{desc}
	}
}

func (p *typeCheckPass) checkFromNode(node parser.Node) {
	switch node := node.(type) {
	case *parser.FromSource:
		p.checkFromSource(node)
	case *parser.JoinExpr:
		p.checkJoin(node)
	}
}

func (p *typeCheckPass) checkFromSource(source *parser.FromSource) {
	if source == nil {
		return
	}

	switch inner := source.Source.(type) {
	case parser.QueryExpr:
		p.checkQuery(inner)
	case *parser.JoinExpr:
		p.checkJoin(inner)
	}
}

func (p *typeCheckPass) checkJoin(join *parser.JoinExpr) {
	if join == nil {
		return
	}

	p.checkFromNode(join.Left)
	p.checkFromNode(join.Right)
	if join.Condition != nil {
		p.requireBoolean(join.Condition, p.exprType(join.Condition), "JOIN condition")
	}
}

func (p *typeCheckPass) checkInsert(stmt *parser.InsertStmt) {
	if stmt == nil {
		return
	}

	targets := p.insertTargetColumns(stmt)
	p.checkInsertMissingColumns(stmt)

	switch source := stmt.Source.(type) {
	case *parser.InsertValuesSource:
		for _, row := range source.Rows {
			rowTypes := make([]sqtypes.TypeDesc, 0, len(row))
			for _, value := range row {
				rowTypes = append(rowTypes, p.exprType(value))
			}
			p.checkInsertValuesShape(source.Pos(), len(targets), len(row))
			p.checkAssignmentList(targets, row, rowTypes, "INSERT value")
		}
	case *parser.InsertQuerySource:
		query, ok := source.Query.(parser.QueryExpr)
		if !ok {
			return
		}
		outputs := p.checkQuery(query)
		p.checkInsertQueryShape(source.Pos(), len(targets), len(outputs))
		p.checkAssignmentList(targets, queryOutputNodes(p.bindings(), query), outputs, "INSERT value")
	case *parser.InsertDefaultValuesSource:
		p.checkInsertDefaultValues(stmt)
	}
}

func (p *typeCheckPass) checkUpdate(stmt *parser.UpdateStmt) {
	if stmt == nil {
		return
	}

	for _, assignment := range stmt.Assignments {
		if assignment == nil {
			continue
		}

		values := make([]sqtypes.TypeDesc, 0, len(assignment.Values))
		for _, value := range assignment.Values {
			values = append(values, p.exprType(value))
		}

		targets := make([]assignmentTarget, 0, len(assignment.Columns))
		for _, column := range assignment.Columns {
			if column == nil {
				continue
			}

			binding, ok := p.lookupBoundColumn(column)
			if !ok {
				continue
			}
			desc, ok := p.typeOfBinding(binding)
			if !ok {
				continue
			}

			targets = append(targets, assignmentTarget{
				name: column.Name,
				typ:  desc,
			})
		}

		p.checkUpdateAssignmentShape(assignment.Pos(), len(targets), len(values))
		p.checkAssignmentList(targets, assignment.Values, values, "UPDATE value")
	}

	if stmt.Where != nil {
		p.requireBoolean(stmt.Where, p.exprType(stmt.Where), "UPDATE WHERE clause")
	}
}

func (p *typeCheckPass) checkDelete(stmt *parser.DeleteStmt) {
	if stmt == nil || stmt.Where == nil {
		return
	}

	p.requireBoolean(stmt.Where, p.exprType(stmt.Where), "DELETE WHERE clause")
}

func (p *typeCheckPass) checkCreateTable(stmt *parser.CreateTableStmt) {
	if stmt == nil {
		return
	}

	for _, column := range stmt.Columns {
		p.columnDefType(column)
	}

	for _, column := range stmt.Columns {
		if column == nil {
			continue
		}

		columnType, ok := p.columnDefType(column)
		if column.Default != nil {
			defaultType := p.exprType(column.Default)
			if ok {
				p.requireAssignable(column.Default, defaultType, columnType, fmt.Sprintf("DEFAULT for column %q", safeIdentifierName(column.Name)))
				p.requireNonNullDefault(column.Default, defaultType, columnType, safeIdentifierName(column.Name))
			}
		}

		for _, constraint := range column.Constraints {
			p.checkConstraint(constraint, fmt.Sprintf("column %q constraint", safeIdentifierName(column.Name)))
		}
	}

	for _, constraint := range stmt.Constraints {
		p.checkConstraint(constraint, "table constraint")
	}
}

func (p *typeCheckPass) checkConstraint(constraint *parser.ConstraintDef, context string) {
	if constraint == nil {
		return
	}

	if constraint.Kind == parser.ConstraintKindCheck {
		p.requireBoolean(constraint.Check, p.exprType(constraint.Check), context)
	}
}

func (p *typeCheckPass) exprType(node parser.Node) sqtypes.TypeDesc {
	if node == nil {
		return sqtypes.TypeDesc{}
	}
	if desc, ok := p.types.Expr(node); ok {
		return desc
	}

	var desc sqtypes.TypeDesc

	switch node := node.(type) {
	case *parser.IntegerLiteral:
		desc = integerLiteralType(node)
	case *parser.FloatLiteral:
		desc = numericLiteralType(node)
	case *parser.StringLiteral:
		desc = stringLiteralType(node)
	case *parser.BoolLiteral:
		desc = sqtypes.TypeDesc{Kind: sqtypes.TypeKindBoolean}
	case *parser.NullLiteral, *parser.ParamLiteral:
		desc = sqtypes.TypeDesc{}
	case *parser.Identifier:
		desc = p.boundNodeType(node)
	case *parser.QualifiedName:
		desc = p.boundNodeType(node)
	case *parser.UnaryExpr:
		desc = p.unaryExprType(node)
	case *parser.BinaryExpr:
		desc = p.binaryExprType(node)
	case *parser.FunctionCall:
		desc = p.functionCallType(node)
	case *parser.CastExpr:
		desc = p.castExprType(node)
	case *parser.WhenClause:
		p.exprType(node.Condition)
		desc = p.exprType(node.Result)
	case *parser.CaseExpr:
		desc = p.caseExprType(node)
	case *parser.BetweenExpr:
		desc = p.betweenExprType(node)
	case *parser.SubqueryExpr:
		desc = p.scalarSubqueryType(node, node.Query)
	case *parser.ExistsExpr:
		if node.Query != nil {
			p.checkQuery(node.Query)
		}
		desc = booleanType(false)
	case *parser.InExpr:
		desc = p.inExprType(node)
	case *parser.LikeExpr:
		desc = p.likeExprType(node)
	case *parser.IsExpr:
		desc = p.isExprType(node)
	case *parser.SelectItem:
		types := p.selectItemTypes(node)
		if len(types) == 1 {
			desc = types[0]
		}
	case parser.QueryExpr:
		desc = p.scalarSubqueryType(node, node)
	}

	p.types.bindExpr(node, desc)
	return desc
}

func (p *typeCheckPass) scalarSubqueryType(node parser.Node, query parser.QueryExpr) sqtypes.TypeDesc {
	if query == nil {
		return sqtypes.TypeDesc{}
	}

	outputs := p.checkQuery(query)
	switch len(outputs) {
	case 0:
		return sqtypes.TypeDesc{}
	case 1:
		return withNullable(outputs[0], true)
	default:
		p.addError(sqlStateDatatypeMismatch, node.Pos(), "scalar subquery returned %d columns", len(outputs))
		return sqtypes.TypeDesc{}
	}
}

func (p *typeCheckPass) boundNodeType(node parser.Node) sqtypes.TypeDesc {
	binding, ok := p.lookupBoundColumn(node)
	if !ok {
		return sqtypes.TypeDesc{}
	}

	desc, ok := p.typeOfBinding(binding)
	if !ok {
		return sqtypes.TypeDesc{}
	}

	return desc
}

func (p *typeCheckPass) typeOfBinding(binding *ColumnBinding) (sqtypes.TypeDesc, bool) {
	if binding == nil {
		return sqtypes.TypeDesc{}, false
	}
	if binding.Descriptor != nil {
		return binding.Descriptor.Type, true
	}
	if binding.Source == nil {
		return sqtypes.TypeDesc{}, false
	}

	if desc, ok := p.types.Expr(binding.Source); ok {
		return desc, true
	}

	switch source := binding.Source.(type) {
	case *parser.SelectItem:
		types := p.selectItemTypes(source)
		if len(types) != 1 {
			return sqtypes.TypeDesc{}, false
		}
		return types[0], true
	case *parser.ColumnDef:
		return p.columnDefType(source)
	default:
		return sqtypes.TypeDesc{}, false
	}
}

func (p *typeCheckPass) columnDefType(column *parser.ColumnDef) (sqtypes.TypeDesc, bool) {
	if column == nil {
		return sqtypes.TypeDesc{}, false
	}
	if desc, ok := p.types.Expr(column); ok {
		return desc, true
	}
	if desc, ok := p.columnTypes[column]; ok {
		p.types.bindExpr(column, desc)
		return desc, true
	}

	desc, err := typeDescFromTypeName(column.Type)
	if err != nil {
		p.addError(sqlStateUndefinedObject, column.Pos(), "invalid type for column %q: %v", safeIdentifierName(column.Name), err)
		return sqtypes.TypeDesc{}, false
	}

	desc = applyColumnNullability(desc, column)
	p.columnTypes[column] = desc
	p.types.bindExpr(column, desc)
	p.types.bindExpr(column.Type, desc)
	return desc, true
}

func (p *typeCheckPass) unaryExprType(node *parser.UnaryExpr) sqtypes.TypeDesc {
	if node == nil {
		return sqtypes.TypeDesc{}
	}

	operand := p.exprType(node.Operand)
	switch strings.ToUpper(node.Operator) {
	case "NOT":
		p.requireBoolean(node.Operand, operand, fmt.Sprintf("operand of %s", node.Operator))
		return booleanType(isNullableResult(operand))
	case "+", "-":
		if isUnknownType(operand) {
			return sqtypes.TypeDesc{}
		}
		if !isNumericType(operand) {
			p.addError(sqlStateDatatypeMismatch, node.Pos(), "operator %s requires a numeric operand, found %s", node.Operator, typeString(operand))
			return sqtypes.TypeDesc{}
		}
		return operand
	default:
		return sqtypes.TypeDesc{}
	}
}

func (p *typeCheckPass) binaryExprType(node *parser.BinaryExpr) sqtypes.TypeDesc {
	if node == nil {
		return sqtypes.TypeDesc{}
	}

	left := p.exprType(node.Left)
	right := p.exprType(node.Right)
	operator := strings.ToUpper(node.Operator)

	switch operator {
	case "AND", "OR":
		p.requireBoolean(node.Left, left, fmt.Sprintf("left operand of %s", operator))
		p.requireBoolean(node.Right, right, fmt.Sprintf("right operand of %s", operator))
		return booleanType(isNullableResult(left) || isNullableResult(right))
	case "+", "-", "*", "/", "%":
		return p.numericBinaryResult(node, operator, left, right)
	case "||":
		return p.concatResult(node, left, right)
	case "=", "!=", "<>", "<", "<=", ">", ">=":
		if _, ok := comparableSuperType(left, right); !ok {
			p.addError(sqlStateDatatypeMismatch, node.Pos(), "operator %s cannot compare %s and %s", operator, typeString(left), typeString(right))
		}
		return booleanType(isNullableResult(left) || isNullableResult(right))
	default:
		return sqtypes.TypeDesc{}
	}
}

func (p *typeCheckPass) numericBinaryResult(node *parser.BinaryExpr, operator string, left, right sqtypes.TypeDesc) sqtypes.TypeDesc {
	if isUnknownType(left) && isUnknownType(right) {
		return sqtypes.TypeDesc{}
	}

	result, ok := sqtypes.CommonSuperType(left, right)
	if ok && isNumericType(result) {
		return result
	}

	p.addError(sqlStateDatatypeMismatch, node.Pos(), "operator %s requires numeric operands, found %s and %s", operator, typeString(left), typeString(right))
	return sqtypes.TypeDesc{}
}

func (p *typeCheckPass) concatResult(node *parser.BinaryExpr, left, right sqtypes.TypeDesc) sqtypes.TypeDesc {
	if isUnknownType(left) && isUnknownType(right) {
		return sqtypes.TypeDesc{}
	}

	if result, ok := sqtypes.CommonSuperType(left, right); ok && isCharacterType(result) {
		return result
	}
	if result, ok := sqtypes.CommonSuperType(left, right); ok && isBinaryType(result) {
		return result
	}

	p.addError(sqlStateDatatypeMismatch, node.Pos(), "operator || requires character or binary operands, found %s and %s", typeString(left), typeString(right))
	return sqtypes.TypeDesc{}
}

func (p *typeCheckPass) caseExprType(node *parser.CaseExpr) sqtypes.TypeDesc {
	if node == nil {
		return sqtypes.TypeDesc{}
	}

	var (
		resultType sqtypes.TypeDesc
		haveResult bool
	)

	operandType := p.exprType(node.Operand)
	for _, when := range node.Whens {
		if when == nil {
			continue
		}

		conditionType := p.exprType(when.Condition)
		if node.Operand == nil {
			p.requireBoolean(when.Condition, conditionType, "CASE WHEN condition")
		} else if _, ok := comparableSuperType(operandType, conditionType); !ok {
			p.addError(sqlStateDatatypeMismatch, when.Condition.Pos(), "CASE operand of type %s cannot be matched against %s", typeString(operandType), typeString(conditionType))
		}

		whenResult := p.exprType(when.Result)
		if !haveResult {
			resultType = whenResult
			haveResult = true
			continue
		}

		common, ok := sqtypes.CommonSuperType(resultType, whenResult)
		if !ok {
			p.addError(sqlStateDatatypeMismatch, when.Result.Pos(), "CASE result types %s and %s are incompatible", typeString(resultType), typeString(whenResult))
			continue
		}
		resultType = common
	}

	if node.Else != nil {
		elseType := p.exprType(node.Else)
		if !haveResult {
			resultType = elseType
		} else if common, ok := sqtypes.CommonSuperType(resultType, elseType); ok {
			resultType = common
		} else {
			p.addError(sqlStateDatatypeMismatch, node.Else.Pos(), "CASE result types %s and %s are incompatible", typeString(resultType), typeString(elseType))
		}
	} else if haveResult && !isUnknownType(resultType) {
		resultType.Nullable = true
	}

	if !haveResult {
		return sqtypes.TypeDesc{}
	}

	return resultType
}

func (p *typeCheckPass) betweenExprType(node *parser.BetweenExpr) sqtypes.TypeDesc {
	if node == nil {
		return sqtypes.TypeDesc{}
	}

	exprType := p.exprType(node.Expr)
	lowerType := p.exprType(node.Lower)
	upperType := p.exprType(node.Upper)

	boundsType, ok := sqtypes.CommonSuperType(lowerType, upperType)
	if !ok {
		p.addError(sqlStateDatatypeMismatch, node.Pos(), "BETWEEN bounds %s and %s are incompatible", typeString(lowerType), typeString(upperType))
		return booleanType(true)
	}
	if _, ok := comparableSuperType(exprType, boundsType); !ok {
		p.addError(sqlStateDatatypeMismatch, node.Pos(), "BETWEEN expression of type %s is incompatible with bounds of type %s", typeString(exprType), typeString(boundsType))
	}

	return booleanType(isNullableResult(exprType) || isNullableResult(lowerType) || isNullableResult(upperType))
}

func (p *typeCheckPass) inExprType(node *parser.InExpr) sqtypes.TypeDesc {
	if node == nil {
		return sqtypes.TypeDesc{}
	}

	leftType := p.exprType(node.Expr)
	nullable := isNullableResult(leftType)
	if node.Query != nil {
		outputs := p.checkQuery(node.Query)
		switch len(outputs) {
		case 0:
			return booleanType(nullable)
		case 1:
			if _, ok := comparableSuperType(leftType, outputs[0]); !ok {
				p.addError(sqlStateDatatypeMismatch, node.Query.Pos(), "IN subquery result of type %s is incompatible with left-hand type %s", typeString(outputs[0]), typeString(leftType))
			}
			return booleanType(nullable || isNullableResult(outputs[0]))
		default:
			p.addError(sqlStateDatatypeMismatch, node.Query.Pos(), "IN subquery returned %d columns", len(outputs))
			return booleanType(nullable)
		}
	}

	for _, item := range node.List {
		itemType := p.exprType(item)
		if _, ok := comparableSuperType(leftType, itemType); !ok {
			p.addError(sqlStateDatatypeMismatch, item.Pos(), "IN item of type %s is incompatible with left-hand type %s", typeString(itemType), typeString(leftType))
		}
		nullable = nullable || isNullableResult(itemType)
	}

	return booleanType(nullable)
}

func (p *typeCheckPass) likeExprType(node *parser.LikeExpr) sqtypes.TypeDesc {
	if node == nil {
		return sqtypes.TypeDesc{}
	}

	exprType := p.exprType(node.Expr)
	patternType := p.exprType(node.Pattern)
	p.requireCharacter(node.Expr, exprType, "LIKE expression")
	p.requireCharacter(node.Pattern, patternType, "LIKE pattern")

	nullable := isNullableResult(exprType) || isNullableResult(patternType)
	if node.Escape != nil {
		escapeType := p.exprType(node.Escape)
		p.requireCharacter(node.Escape, escapeType, "LIKE ESCAPE expression")
		nullable = nullable || isNullableResult(escapeType)
	}

	return booleanType(nullable)
}

func (p *typeCheckPass) isExprType(node *parser.IsExpr) sqtypes.TypeDesc {
	if node == nil {
		return sqtypes.TypeDesc{}
	}

	exprType := p.exprType(node.Expr)
	switch node.Predicate {
	case "NULL":
		return booleanType(false)
	case "TRUE", "FALSE", "UNKNOWN":
		p.requireBoolean(node.Expr, exprType, fmt.Sprintf("operand of IS %s", node.Predicate))
		return booleanType(false)
	case "DISTINCT FROM":
		rightType := p.exprType(node.Right)
		if _, ok := comparableSuperType(exprType, rightType); !ok {
			p.addError(sqlStateDatatypeMismatch, node.Pos(), "IS DISTINCT FROM cannot compare %s and %s", typeString(exprType), typeString(rightType))
		}
		return booleanType(false)
	default:
		return booleanType(false)
	}
}

func (p *typeCheckPass) castExprType(node *parser.CastExpr) sqtypes.TypeDesc {
	if node == nil {
		return sqtypes.TypeDesc{}
	}

	source := p.exprType(node.Expr)
	target, err := typeDescFromTypeName(node.Type)
	if err != nil {
		p.addError(sqlStateUndefinedObject, node.Type.Pos(), "invalid CAST target type: %v", err)
		return sqtypes.TypeDesc{}
	}

	p.types.bindExpr(node.Type, target)
	if !isUnknownType(source) && !canCast(source, target) {
		p.addError(sqlStateCannotCoerce, node.Pos(), "cannot cast %s to %s", typeString(source), typeString(target))
	}

	return withNullable(target, isNullableResult(source))
}

func (p *typeCheckPass) functionCallType(node *parser.FunctionCall) sqtypes.TypeDesc {
	if node == nil {
		return sqtypes.TypeDesc{}
	}
	if node.Name != nil && len(node.Name.Parts) > 1 {
		p.undefinedFunction(node.Name, node.Pos())
		return sqtypes.TypeDesc{}
	}

	name := functionName(node.Name)
	if name == "" {
		return sqtypes.TypeDesc{}
	}

	argTypes := make([]sqtypes.TypeDesc, 0, len(node.Args))
	argNodes := make([]parser.Node, 0, len(node.Args))
	hasStar := false
	for _, arg := range node.Args {
		if _, ok := arg.(*parser.Star); ok {
			hasStar = true
			continue
		}
		argNodes = append(argNodes, arg)
		argTypes = append(argTypes, p.exprType(arg))
	}

	switch name {
	case "COUNT":
		if hasStar {
			return sqtypes.TypeDesc{Kind: sqtypes.TypeKindBigInt}
		}
		if len(argTypes) != 1 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		return sqtypes.TypeDesc{Kind: sqtypes.TypeKindBigInt}
	case "ABS":
		if len(argTypes) != 1 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		if isUnknownType(argTypes[0]) {
			return sqtypes.TypeDesc{}
		}
		if !isNumericType(argTypes[0]) {
			p.addError(sqlStateDatatypeMismatch, node.Pos(), "%s requires a numeric argument, found %s", name, typeString(argTypes[0]))
			return sqtypes.TypeDesc{}
		}
		return argTypes[0]
	case "CEIL", "FLOOR", "SIGN":
		if len(argTypes) != 1 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		p.requireNumeric(argNodes[0], argTypes[0], name+" argument")
		return withNullable(argTypes[0], strictNullable(argTypes))
	case "SQRT", "EXP", "LN", "LOG10", "SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN":
		if len(argTypes) != 1 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		p.requireNumeric(argNodes[0], argTypes[0], name+" argument")
		return sqtypes.TypeDesc{Kind: sqtypes.TypeKindDoublePrecision, Nullable: strictNullable(argTypes)}
	case "ROUND", "TRUNCATE":
		if len(argTypes) != 1 && len(argTypes) != 2 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		p.requireNumeric(argNodes[0], argTypes[0], name+" argument")
		if len(argTypes) == 2 {
			p.requireInteger(argNodes[1], argTypes[1], name+" precision")
		}
		return withNullable(argTypes[0], strictNullable(argTypes))
	case "MOD":
		if len(argTypes) != 2 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		p.requireNumeric(argNodes[0], argTypes[0], name+" argument")
		p.requireNumeric(argNodes[1], argTypes[1], name+" argument")
		result, ok := sqtypes.CommonSuperType(argTypes[0], argTypes[1])
		if !ok || (!isUnknownType(result) && !isNumericType(result)) {
			return sqtypes.TypeDesc{}
		}
		return withNullable(result, strictNullable(argTypes))
	case "POWER", "ATAN2":
		if len(argTypes) != 2 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		p.requireNumeric(argNodes[0], argTypes[0], name+" argument")
		p.requireNumeric(argNodes[1], argTypes[1], name+" argument")
		return sqtypes.TypeDesc{Kind: sqtypes.TypeKindDoublePrecision, Nullable: strictNullable(argTypes)}
	case "LOG":
		if len(argTypes) != 1 && len(argTypes) != 2 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		p.requireNumeric(argNodes[0], argTypes[0], name+" argument")
		if len(argTypes) == 2 {
			p.requireNumeric(argNodes[1], argTypes[1], name+" argument")
		}
		return sqtypes.TypeDesc{Kind: sqtypes.TypeKindDoublePrecision, Nullable: strictNullable(argTypes)}
	case "RANDOM":
		if len(argTypes) != 0 || hasStar {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		return sqtypes.TypeDesc{Kind: sqtypes.TypeKindDoublePrecision}
	case "LOWER", "UPPER", "LTRIM", "RTRIM", "TRIM", "SUBSTRING":
		if len(argTypes) == 0 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		p.requireCharacter(argNodes[0], argTypes[0], name+" argument")
		return argTypes[0]
	case "POSITION":
		if len(argTypes) != 2 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		p.requireCharacter(argNodes[0], argTypes[0], name+" argument")
		p.requireCharacter(argNodes[1], argTypes[1], name+" argument")
		return sqtypes.TypeDesc{Kind: sqtypes.TypeKindBigInt, Nullable: strictNullable(argTypes)}
	case "OVERLAY":
		if len(argTypes) != 3 && len(argTypes) != 4 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		p.requireCharacter(argNodes[0], argTypes[0], name+" argument")
		p.requireCharacter(argNodes[1], argTypes[1], name+" argument")
		p.requireInteger(argNodes[2], argTypes[2], name+" start")
		if len(argTypes) == 4 {
			p.requireInteger(argNodes[3], argTypes[3], name+" length")
		}
		if result, ok := sqtypes.CommonSuperType(argTypes[0], argTypes[1]); ok && (isUnknownType(result) || isCharacterType(result)) {
			return withNullable(result, strictNullable(argTypes))
		}
		if isCharacterType(argTypes[0]) {
			return withNullable(argTypes[0], strictNullable(argTypes))
		}
		return sqtypes.TypeDesc{}
	case "REGEXP_LIKE":
		if len(argTypes) != 2 && len(argTypes) != 3 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		for index, argType := range argTypes {
			p.requireCharacter(argNodes[index], argType, name+" argument")
		}
		return booleanType(strictNullable(argTypes))
	case "REGEXP_REPLACE":
		if len(argTypes) != 3 && len(argTypes) != 4 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		for index, argType := range argTypes {
			p.requireCharacter(argNodes[index], argType, name+" argument")
		}
		if result, ok := sqtypes.CommonSuperType(argTypes[0], argTypes[2]); ok && (isUnknownType(result) || isCharacterType(result)) {
			return withNullable(result, strictNullable(argTypes))
		}
		if isCharacterType(argTypes[0]) {
			return withNullable(argTypes[0], strictNullable(argTypes))
		}
		return sqtypes.TypeDesc{}
	case "REGEXP_SUBSTR":
		if len(argTypes) != 2 && len(argTypes) != 3 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		for index, argType := range argTypes {
			p.requireCharacter(argNodes[index], argType, name+" argument")
		}
		return withNullable(argTypes[0], strictNullable(argTypes))
	case "CONCAT":
		if len(argTypes) == 0 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}

		var (
			result   sqtypes.TypeDesc
			haveType bool
			nullable bool
		)
		for index, argType := range argTypes {
			p.requireCharacter(argNodes[index], argType, name+" argument")
			if !haveType {
				result = argType
				haveType = true
			} else if common, ok := sqtypes.CommonSuperType(result, argType); ok && isCharacterType(common) {
				result = common
			}
			nullable = nullable || isNullableResult(argType)
		}
		if !haveType {
			return sqtypes.TypeDesc{}
		}
		return withNullable(result, nullable)
	case "CHAR_LENGTH", "CHARACTER_LENGTH":
		if len(argTypes) != 1 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		p.requireCharacter(argNodes[0], argTypes[0], name+" argument")
		return sqtypes.TypeDesc{Kind: sqtypes.TypeKindBigInt, Nullable: isNullableResult(argTypes[0])}
	case "OCTET_LENGTH":
		if len(argTypes) != 1 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		if !isUnknownType(argTypes[0]) && !isCharacterType(argTypes[0]) && !isBinaryType(argTypes[0]) {
			p.addError(sqlStateDatatypeMismatch, node.Pos(), "%s requires a character or binary argument, found %s", name, typeString(argTypes[0]))
		}
		return sqtypes.TypeDesc{Kind: sqtypes.TypeKindBigInt, Nullable: isNullableResult(argTypes[0])}
	case "COALESCE":
		if len(argTypes) == 0 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		return coalesceType(p, node, argTypes)
	case "NULLIF":
		if len(argTypes) != 2 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		if _, ok := comparableSuperType(argTypes[0], argTypes[1]); !ok {
			p.addError(sqlStateDatatypeMismatch, node.Pos(), "%s arguments %s and %s are incompatible", name, typeString(argTypes[0]), typeString(argTypes[1]))
		}
		if isUnknownType(argTypes[0]) {
			if common, ok := sqtypes.CommonSuperType(argTypes[0], argTypes[1]); ok {
				return withNullable(common, true)
			}
			return sqtypes.TypeDesc{}
		}
		return withNullable(argTypes[0], true)
	case "GREATEST", "LEAST":
		if len(argTypes) == 0 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		return foldCommonSuperType(p, node, name, argTypes)
	case "SUM":
		if len(argTypes) != 1 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		if isUnknownType(argTypes[0]) {
			return sqtypes.TypeDesc{}
		}
		if !isNumericType(argTypes[0]) {
			p.addError(sqlStateDatatypeMismatch, node.Pos(), "%s requires a numeric argument, found %s", name, typeString(argTypes[0]))
			return sqtypes.TypeDesc{}
		}
		return withNullable(argTypes[0], true)
	case "AVG":
		if len(argTypes) != 1 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		if isUnknownType(argTypes[0]) {
			return sqtypes.TypeDesc{}
		}
		if !isNumericType(argTypes[0]) {
			p.addError(sqlStateDatatypeMismatch, node.Pos(), "%s requires a numeric argument, found %s", name, typeString(argTypes[0]))
			return sqtypes.TypeDesc{}
		}
		result := argTypes[0]
		if isExactIntegerType(result) {
			result = sqtypes.TypeDesc{Kind: sqtypes.TypeKindNumeric}
		}
		return withNullable(result, true)
	case "MIN", "MAX":
		if len(argTypes) != 1 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		return withNullable(argTypes[0], true)
	case "EVERY":
		if len(argTypes) != 1 {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		p.requireBoolean(argNodes[0], argTypes[0], name+" argument")
		return booleanType(true)
	case "CURRENT_DATE":
		if len(argTypes) != 0 || hasStar {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		return sqtypes.TypeDesc{Kind: sqtypes.TypeKindDate}
	case "CURRENT_TIME":
		if len(argTypes) != 0 || hasStar {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		return sqtypes.TypeDesc{Kind: sqtypes.TypeKindTimeWithTimeZone}
	case "CURRENT_TIMESTAMP":
		if len(argTypes) != 0 || hasStar {
			p.undefinedFunction(node.Name, node.Pos())
			return sqtypes.TypeDesc{}
		}
		return sqtypes.TypeDesc{Kind: sqtypes.TypeKindTimestampWithTimeZone}
	default:
		p.undefinedFunction(node.Name, node.Pos())
		return sqtypes.TypeDesc{}
	}
}

func foldCommonSuperType(p *typeCheckPass, node parser.Node, name string, args []sqtypes.TypeDesc) sqtypes.TypeDesc {
	if len(args) == 0 {
		return sqtypes.TypeDesc{}
	}

	result := args[0]
	for _, arg := range args[1:] {
		common, ok := sqtypes.CommonSuperType(result, arg)
		if !ok {
			p.addError(sqlStateDatatypeMismatch, node.Pos(), "%s arguments %s and %s are incompatible", name, typeString(result), typeString(arg))
			return sqtypes.TypeDesc{}
		}
		result = common
	}

	return result
}

func coalesceType(p *typeCheckPass, node parser.Node, args []sqtypes.TypeDesc) sqtypes.TypeDesc {
	result := foldCommonSuperType(p, node, "COALESCE", args)
	if isUnknownType(result) {
		return result
	}

	result.Nullable = true
	for _, arg := range args {
		if !isNullableResult(arg) {
			result.Nullable = false
			break
		}
	}

	return result
}

func (p *typeCheckPass) requireBoolean(node parser.Node, desc sqtypes.TypeDesc, context string) {
	if node == nil {
		return
	}
	if isUnknownType(desc) {
		p.types.bindExpr(node, booleanType(true))
		return
	}
	if sqtypes.CanImplicitlyCoerce(desc, sqtypes.TypeDesc{Kind: sqtypes.TypeKindBoolean}) {
		return
	}

	p.addError(sqlStateDatatypeMismatch, node.Pos(), "%s must be BOOLEAN, found %s", context, typeString(desc))
}

func (p *typeCheckPass) requireCharacter(node parser.Node, desc sqtypes.TypeDesc, context string) {
	if node == nil || isUnknownType(desc) {
		return
	}
	if isCharacterType(desc) {
		return
	}

	p.addError(sqlStateDatatypeMismatch, node.Pos(), "%s must be a character type, found %s", context, typeString(desc))
}

func (p *typeCheckPass) requireNumeric(node parser.Node, desc sqtypes.TypeDesc, context string) {
	if node == nil || isUnknownType(desc) {
		return
	}
	if isNumericType(desc) {
		return
	}

	p.addError(sqlStateDatatypeMismatch, node.Pos(), "%s must be numeric, found %s", context, typeString(desc))
}

func (p *typeCheckPass) requireInteger(node parser.Node, desc sqtypes.TypeDesc, context string) {
	if node == nil || isUnknownType(desc) {
		return
	}
	if isIntegerLikeType(desc) {
		return
	}

	p.addError(sqlStateDatatypeMismatch, node.Pos(), "%s must be an integer type, found %s", context, typeString(desc))
}

func (p *typeCheckPass) requireAssignable(node parser.Node, from sqtypes.TypeDesc, to sqtypes.TypeDesc, context string) {
	if node == nil || isUnknownType(to) {
		return
	}
	if isUnknownType(from) {
		p.types.bindExpr(node, to)
		return
	}
	if sqtypes.CanImplicitlyCoerce(from, to) {
		return
	}

	p.addError(sqlStateDatatypeMismatch, node.Pos(), "%s must be coercible to %s, found %s", context, typeString(to), typeString(from))
}

func (p *typeCheckPass) insertTargetColumns(stmt *parser.InsertStmt) []assignmentTarget {
	if stmt == nil || stmt.Table == nil {
		return nil
	}

	bindings := p.bindings()
	if bindings == nil {
		return nil
	}

	relation, ok := bindings.Relation(stmt.Table)
	if !ok || relation == nil {
		return nil
	}

	if len(stmt.Columns) == 0 {
		targets := make([]assignmentTarget, 0, len(relation.Columns))
		for _, column := range relation.Columns {
			desc, ok := p.typeOfBinding(column)
			if !ok {
				continue
			}
			targets = append(targets, assignmentTarget{
				name: column.Name,
				typ:  desc,
			})
		}
		return targets
	}

	targets := make([]assignmentTarget, 0, len(stmt.Columns))
	for _, column := range stmt.Columns {
		if column == nil {
			continue
		}
		binding, ok := bindings.Column(column)
		if !ok {
			continue
		}
		desc, ok := p.typeOfBinding(binding)
		if !ok {
			continue
		}
		targets = append(targets, assignmentTarget{
			name: column.Name,
			typ:  desc,
		})
	}

	return targets
}

func (p *typeCheckPass) bindings() *Bindings {
	if p.checker == nil {
		return nil
	}

	return p.checker.bindings
}

func (p *typeCheckPass) lookupBoundColumn(node parser.Node) (*ColumnBinding, bool) {
	bindings := p.bindings()
	if bindings == nil {
		return nil, false
	}

	return bindings.Column(node)
}

func (p *typeCheckPass) undefinedFunction(name *parser.QualifiedName, pos token.Pos) {
	p.addError(sqlStateUndefinedFunc, pos, "function %q does not exist", qualifiedNameString(name))
}

func (p *typeCheckPass) addError(sqlState string, pos token.Pos, format string, args ...any) {
	p.diagnostics = append(p.diagnostics, diag.NewError(sqlState, fmt.Sprintf(format, args...), toDiagPosition(pos)))
}

type assignmentTarget struct {
	name string
	typ  sqtypes.TypeDesc
}

func integerLiteralType(node *parser.IntegerLiteral) sqtypes.TypeDesc {
	if node == nil {
		return sqtypes.TypeDesc{}
	}

	text := strings.TrimSpace(node.Text)
	if text == "" {
		return sqtypes.TypeDesc{}
	}

	parsed := new(big.Int)
	if _, ok := parsed.SetString(text, 0); ok {
		switch {
		case parsed.IsInt64() && parsed.Int64() >= math.MinInt32 && parsed.Int64() <= math.MaxInt32:
			return sqtypes.TypeDesc{Kind: sqtypes.TypeKindInteger}
		case parsed.IsInt64():
			return sqtypes.TypeDesc{Kind: sqtypes.TypeKindBigInt}
		default:
			return sqtypes.TypeDesc{
				Kind:      sqtypes.TypeKindNumeric,
				Precision: digitsForBigInt(parsed),
			}
		}
	}

	return sqtypes.TypeDesc{Kind: sqtypes.TypeKindNumeric}
}

func numericLiteralType(node *parser.FloatLiteral) sqtypes.TypeDesc {
	if node == nil {
		return sqtypes.TypeDesc{}
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(node.Text)), "0x") {
		return integerLiteralType(&parser.IntegerLiteral{Text: node.Text})
	}

	decimal, err := sqtypes.ParseDecimal(node.Text)
	if err != nil {
		return sqtypes.TypeDesc{Kind: sqtypes.TypeKindNumeric}
	}

	text := decimal.String()
	scale := uint32(0)
	precision := uint32(0)
	if strings.Contains(text, ".") {
		parts := strings.SplitN(text, ".", 2)
		scale = uint32(len(parts[1]))
		precision = uint32(len(strings.TrimPrefix(parts[0], "-")) + len(parts[1]))
	} else {
		precision = uint32(len(strings.TrimPrefix(text, "-")))
	}
	if precision == 0 {
		precision = 1
	}

	return sqtypes.TypeDesc{
		Kind:      sqtypes.TypeKindNumeric,
		Precision: precision,
		Scale:     scale,
	}
}

func stringLiteralType(node *parser.StringLiteral) sqtypes.TypeDesc {
	if node == nil {
		return sqtypes.TypeDesc{}
	}

	length := utf8.RuneCountInString(node.Value)
	if length < 1 {
		length = 1
	}

	return sqtypes.TypeDesc{
		Kind:   sqtypes.TypeKindVarChar,
		Length: uint32(length),
	}
}

func comparableSuperType(left, right sqtypes.TypeDesc) (sqtypes.TypeDesc, bool) {
	return sqtypes.CommonSuperType(left, right)
}

func canCast(source, target sqtypes.TypeDesc) bool {
	value, ok := sampleValueForCast(source, target)
	if !ok {
		return false
	}

	_, err := sqtypes.Cast(value, source, target)
	if err == nil {
		return true
	}

	return !errors.Is(err, sqtypes.ErrInvalidCast)
}

func sampleValueForCast(source, target sqtypes.TypeDesc) (sqtypes.Value, bool) {
	switch source.Kind {
	case sqtypes.TypeKindSmallInt:
		return sqtypes.Int16Value(0), true
	case sqtypes.TypeKindInteger:
		return sqtypes.Int32Value(0), true
	case sqtypes.TypeKindBigInt:
		return sqtypes.Int64Value(0), true
	case sqtypes.TypeKindNumeric, sqtypes.TypeKindDecimal:
		return sqtypes.DecimalValue(sqtypes.NewDecimalFromInt64(0)), true
	case sqtypes.TypeKindBoolean:
		return sqtypes.BoolValue(true), true
	case sqtypes.TypeKindReal:
		return sqtypes.Float32Value(0), true
	case sqtypes.TypeKindDoublePrecision:
		return sqtypes.Float64Value(0), true
	case sqtypes.TypeKindChar, sqtypes.TypeKindVarChar, sqtypes.TypeKindText, sqtypes.TypeKindCLOB:
		return sqtypes.StringValue(sampleCharacterValueForTarget(target)), true
	case sqtypes.TypeKindBinary, sqtypes.TypeKindVarBinary, sqtypes.TypeKindBLOB:
		return sqtypes.BytesValue(nil), true
	case sqtypes.TypeKindDate:
		return sqtypes.DateTimeValue(time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)), true
	case sqtypes.TypeKindTime:
		return sqtypes.TimeOfDayValue(0), true
	case sqtypes.TypeKindTimeWithTimeZone:
		return sqtypes.DateTimeValue(time.Date(1, time.January, 1, 0, 0, 0, 0, time.FixedZone("UTC", 0))), true
	case sqtypes.TypeKindTimestamp:
		return sqtypes.DateTimeValue(time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)), true
	case sqtypes.TypeKindTimestampWithTimeZone:
		return sqtypes.DateTimeValue(time.Date(2000, time.January, 1, 0, 0, 0, 0, time.FixedZone("UTC", 0))), true
	case sqtypes.TypeKindInterval:
		return sqtypes.IntervalValue(sqtypes.Interval{}), true
	case sqtypes.TypeKindArray:
		return sqtypes.ArrayValue(nil), true
	case sqtypes.TypeKindRow:
		return sqtypes.RowValue(nil), true
	default:
		return sqtypes.NullValue(), false
	}
}

func sampleCharacterValueForTarget(target sqtypes.TypeDesc) string {
	switch {
	case isNumericType(target):
		return "0"
	case target.Kind == sqtypes.TypeKindBoolean:
		return "TRUE"
	case isCharacterType(target):
		return ""
	case target.Kind == sqtypes.TypeKindDate:
		return "2000-01-01"
	case target.Kind == sqtypes.TypeKindTime:
		return "00:00:00"
	case target.Kind == sqtypes.TypeKindTimeWithTimeZone:
		return "00:00:00+00:00"
	case target.Kind == sqtypes.TypeKindTimestamp:
		return "2000-01-01 00:00:00"
	case target.Kind == sqtypes.TypeKindTimestampWithTimeZone:
		return "2000-01-01 00:00:00+00:00"
	default:
		return ""
	}
}

func functionName(name *parser.QualifiedName) string {
	if name == nil || len(name.Parts) == 0 {
		return ""
	}

	return strings.ToUpper(name.Parts[len(name.Parts)-1].Name)
}

func isUnknownType(desc sqtypes.TypeDesc) bool {
	return desc == (sqtypes.TypeDesc{})
}

func isNullableResult(desc sqtypes.TypeDesc) bool {
	return isUnknownType(desc) || desc.Nullable
}

func strictNullable(args []sqtypes.TypeDesc) bool {
	for _, arg := range args {
		if isNullableResult(arg) {
			return true
		}
	}
	return false
}

func withNullable(desc sqtypes.TypeDesc, nullable bool) sqtypes.TypeDesc {
	if isUnknownType(desc) {
		return desc
	}
	desc.Nullable = nullable
	return desc
}

func booleanType(nullable bool) sqtypes.TypeDesc {
	return sqtypes.TypeDesc{
		Kind:     sqtypes.TypeKindBoolean,
		Nullable: nullable,
	}
}

func isNumericType(desc sqtypes.TypeDesc) bool {
	switch desc.Kind {
	case sqtypes.TypeKindSmallInt,
		sqtypes.TypeKindInteger,
		sqtypes.TypeKindBigInt,
		sqtypes.TypeKindNumeric,
		sqtypes.TypeKindDecimal,
		sqtypes.TypeKindReal,
		sqtypes.TypeKindDoublePrecision:
		return true
	default:
		return false
	}
}

func isExactIntegerType(desc sqtypes.TypeDesc) bool {
	switch desc.Kind {
	case sqtypes.TypeKindSmallInt, sqtypes.TypeKindInteger, sqtypes.TypeKindBigInt:
		return true
	default:
		return false
	}
}

func isIntegerLikeType(desc sqtypes.TypeDesc) bool {
	if isExactIntegerType(desc) {
		return true
	}

	switch desc.Kind {
	case sqtypes.TypeKindNumeric, sqtypes.TypeKindDecimal:
		return desc.Scale == 0
	default:
		return false
	}
}

func isCharacterType(desc sqtypes.TypeDesc) bool {
	switch desc.Kind {
	case sqtypes.TypeKindChar, sqtypes.TypeKindVarChar, sqtypes.TypeKindText, sqtypes.TypeKindCLOB:
		return true
	default:
		return false
	}
}

func isBinaryType(desc sqtypes.TypeDesc) bool {
	switch desc.Kind {
	case sqtypes.TypeKindBinary, sqtypes.TypeKindVarBinary, sqtypes.TypeKindBLOB:
		return true
	default:
		return false
	}
}

func typeString(desc sqtypes.TypeDesc) string {
	if isUnknownType(desc) {
		return "unknown"
	}

	return desc.String()
}

func digitsForBigInt(value *big.Int) uint32 {
	if value == nil {
		return 1
	}

	text := new(big.Int).Abs(value).String()
	if text == "" {
		return 1
	}

	return uint32(len(text))
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func applyColumnNullability(desc sqtypes.TypeDesc, column *parser.ColumnDef) sqtypes.TypeDesc {
	if column == nil || isUnknownType(desc) {
		return desc
	}

	for _, constraint := range column.Constraints {
		if constraint == nil {
			continue
		}

		switch constraint.Kind {
		case parser.ConstraintKindNull:
			desc.Nullable = true
		case parser.ConstraintKindNotNull, parser.ConstraintKindPrimaryKey:
			desc.Nullable = false
		}
	}

	return desc
}

func queryOutputNodes(bindings *Bindings, query parser.QueryExpr) []parser.Node {
	switch query := query.(type) {
	case nil:
		return nil
	case *parser.SelectStmt:
		nodes := make([]parser.Node, 0, len(query.SelectList))
		for _, item := range query.SelectList {
			if item == nil || item.Expr == nil {
				continue
			}

			star, ok := item.Expr.(*parser.Star)
			if !ok {
				nodes = append(nodes, item.Expr)
				continue
			}

			if bindings == nil {
				nodes = append(nodes, star)
				continue
			}

			columns, ok := bindings.Star(star)
			if !ok || len(columns) == 0 {
				nodes = append(nodes, star)
				continue
			}
			for range columns {
				nodes = append(nodes, star)
			}
		}

		return nodes
	case *parser.SetOpExpr:
		return queryOutputNodes(bindings, query.Left)
	default:
		return nil
	}
}

func safeIdentifierName(name *parser.Identifier) string {
	if name == nil {
		return ""
	}

	return name.Name
}
