package ast

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// PrettyPrint renders a stable, indented representation of an AST node.
//
// The output is structural rather than SQL-like so it is suitable for debug
// output and golden tests.
func PrettyPrint(node Node) string {
	if isNilNode(node) {
		return "<nil>"
	}

	var printer prettyPrinter
	printer.printNode(node, 0)

	return strings.TrimSuffix(printer.builder.String(), "\n")
}

var _ Visitor = (*prettyPrinter)(nil)

type prettyPrinter struct {
	builder strings.Builder
	indent  int
}

func (p *prettyPrinter) printNode(node Node, indent int) {
	if isNilNode(node) {
		p.writeLine(indent, "<nil>")
		return
	}

	previousIndent := p.indent
	p.indent = indent
	node.Accept(p)
	p.indent = previousIndent
}

func (p *prettyPrinter) writeLine(indent int, text string) {
	p.builder.WriteString(strings.Repeat("  ", indent))
	p.builder.WriteString(text)
	p.builder.WriteByte('\n')
}

func (p *prettyPrinter) writeBoolField(indent int, name string, value bool) {
	p.writeLine(indent, fmt.Sprintf("%s: %t", name, value))
}

func (p *prettyPrinter) writeStringField(indent int, name, value string) {
	p.writeLine(indent, fmt.Sprintf("%s: %s", name, strconv.Quote(value)))
}

func (p *prettyPrinter) writeNodeField(indent int, name string, node Node) {
	if isNilNode(node) {
		p.writeLine(indent, name+": <nil>")
		return
	}

	p.writeLine(indent, name+":")
	p.printNode(node, indent+1)
}

func writeNodeSlice[T Node](p *prettyPrinter, indent int, name string, nodes []T) {
	if len(nodes) == 0 {
		p.writeLine(indent, name+": []")
		return
	}

	p.writeLine(indent, name+":")
	for i, node := range nodes {
		p.writeLine(indent+1, fmt.Sprintf("[%d]:", i))
		p.printNode(node, indent+2)
	}
}

func (p *prettyPrinter) writeNodeMatrix(indent int, name string, rows [][]Node) {
	if len(rows) == 0 {
		p.writeLine(indent, name+": []")
		return
	}

	p.writeLine(indent, name+":")
	for i, row := range rows {
		p.writeLine(indent+1, fmt.Sprintf("[%d]:", i))
		if len(row) == 0 {
			p.writeLine(indent+2, "[]")
			continue
		}

		for j, node := range row {
			p.writeLine(indent+2, fmt.Sprintf("[%d]:", j))
			p.printNode(node, indent+3)
		}
	}
}

func isNilNode(node Node) bool {
	if node == nil {
		return true
	}

	value := reflect.ValueOf(node)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (p *prettyPrinter) VisitScript(node *Script) any {
	indent := p.indent

	p.writeLine(indent, "Script")
	writeNodeSlice(p, indent+1, "Nodes", node.Nodes)

	return nil
}

func (p *prettyPrinter) VisitSelectStmt(node *SelectStmt) any {
	indent := p.indent

	p.writeLine(indent, "SelectStmt")
	p.writeBoolField(indent+1, "Distinct", node.Distinct)
	writeNodeSlice(p, indent+1, "SelectList", node.SelectList)
	writeNodeSlice(p, indent+1, "From", node.From)
	p.writeNodeField(indent+1, "Where", node.Where)
	writeNodeSlice(p, indent+1, "GroupBy", node.GroupBy)
	p.writeNodeField(indent+1, "Having", node.Having)
	writeNodeSlice(p, indent+1, "OrderBy", node.OrderBy)
	p.writeNodeField(indent+1, "Limit", node.Limit)

	return nil
}

func (p *prettyPrinter) VisitSelectItem(node *SelectItem) any {
	indent := p.indent

	p.writeLine(indent, "SelectItem")
	p.writeNodeField(indent+1, "Expr", node.Expr)
	p.writeNodeField(indent+1, "Alias", node.Alias)

	return nil
}

func (p *prettyPrinter) VisitFromSource(node *FromSource) any {
	indent := p.indent

	p.writeLine(indent, "FromSource")
	p.writeNodeField(indent+1, "Source", node.Source)
	p.writeNodeField(indent+1, "Alias", node.Alias)

	return nil
}

func (p *prettyPrinter) VisitOrderByItem(node *OrderByItem) any {
	indent := p.indent

	p.writeLine(indent, "OrderByItem")
	p.writeNodeField(indent+1, "Expr", node.Expr)
	p.writeStringField(indent+1, "Direction", node.Direction)

	return nil
}

func (p *prettyPrinter) VisitInsertStmt(node *InsertStmt) any {
	indent := p.indent

	p.writeLine(indent, "InsertStmt")
	p.writeNodeField(indent+1, "Table", node.Table)
	writeNodeSlice(p, indent+1, "Columns", node.Columns)
	p.writeNodeField(indent+1, "Source", node.Source)

	return nil
}

func (p *prettyPrinter) VisitInsertValuesSource(node *InsertValuesSource) any {
	indent := p.indent

	p.writeLine(indent, "InsertValuesSource")
	p.writeNodeMatrix(indent+1, "Rows", node.Rows)

	return nil
}

func (p *prettyPrinter) VisitInsertQuerySource(node *InsertQuerySource) any {
	indent := p.indent

	p.writeLine(indent, "InsertQuerySource")
	p.writeNodeField(indent+1, "Query", node.Query)

	return nil
}

func (p *prettyPrinter) VisitInsertDefaultValuesSource(*InsertDefaultValuesSource) any {
	p.writeLine(p.indent, "InsertDefaultValuesSource")

	return nil
}

func (p *prettyPrinter) VisitUpdateAssignment(node *UpdateAssignment) any {
	indent := p.indent

	p.writeLine(indent, "UpdateAssignment")
	writeNodeSlice(p, indent+1, "Columns", node.Columns)
	writeNodeSlice(p, indent+1, "Values", node.Values)

	return nil
}

func (p *prettyPrinter) VisitUpdateStmt(node *UpdateStmt) any {
	indent := p.indent

	p.writeLine(indent, "UpdateStmt")
	p.writeNodeField(indent+1, "Table", node.Table)
	writeNodeSlice(p, indent+1, "Assignments", node.Assignments)
	p.writeNodeField(indent+1, "Where", node.Where)

	return nil
}

func (p *prettyPrinter) VisitDeleteStmt(node *DeleteStmt) any {
	indent := p.indent

	p.writeLine(indent, "DeleteStmt")
	p.writeNodeField(indent+1, "Table", node.Table)
	p.writeNodeField(indent+1, "Where", node.Where)

	return nil
}

func (p *prettyPrinter) VisitTypeName(node *TypeName) any {
	indent := p.indent

	p.writeLine(indent, "TypeName")
	p.writeNodeField(indent+1, "Qualifier", node.Qualifier)
	writeNodeSlice(p, indent+1, "Names", node.Names)
	writeNodeSlice(p, indent+1, "Args", node.Args)

	return nil
}

func (p *prettyPrinter) VisitReferenceSpec(node *ReferenceSpec) any {
	indent := p.indent

	p.writeLine(indent, "ReferenceSpec")
	p.writeNodeField(indent+1, "Table", node.Table)
	writeNodeSlice(p, indent+1, "Columns", node.Columns)

	return nil
}

func (p *prettyPrinter) VisitConstraintDef(node *ConstraintDef) any {
	indent := p.indent

	p.writeLine(indent, "ConstraintDef")
	p.writeNodeField(indent+1, "Name", node.Name)
	p.writeStringField(indent+1, "Kind", string(node.Kind))
	writeNodeSlice(p, indent+1, "Columns", node.Columns)
	p.writeNodeField(indent+1, "Check", node.Check)
	p.writeNodeField(indent+1, "Reference", node.Reference)

	return nil
}

func (p *prettyPrinter) VisitColumnDef(node *ColumnDef) any {
	indent := p.indent

	p.writeLine(indent, "ColumnDef")
	p.writeNodeField(indent+1, "Name", node.Name)
	p.writeNodeField(indent+1, "Type", node.Type)
	p.writeNodeField(indent+1, "Default", node.Default)
	writeNodeSlice(p, indent+1, "Constraints", node.Constraints)

	return nil
}

func (p *prettyPrinter) VisitCreateTableStmt(node *CreateTableStmt) any {
	indent := p.indent

	p.writeLine(indent, "CreateTableStmt")
	p.writeNodeField(indent+1, "Name", node.Name)
	writeNodeSlice(p, indent+1, "Columns", node.Columns)
	writeNodeSlice(p, indent+1, "Constraints", node.Constraints)

	return nil
}

func (p *prettyPrinter) VisitDropTableStmt(node *DropTableStmt) any {
	indent := p.indent

	p.writeLine(indent, "DropTableStmt")
	p.writeNodeField(indent+1, "Name", node.Name)

	return nil
}

func (p *prettyPrinter) VisitIdentifier(node *Identifier) any {
	p.writeLine(p.indent, fmt.Sprintf("Identifier(Name=%s)", strconv.Quote(node.Name)))

	return nil
}

func (p *prettyPrinter) VisitQualifiedName(node *QualifiedName) any {
	indent := p.indent

	p.writeLine(indent, "QualifiedName")
	writeNodeSlice(p, indent+1, "Parts", node.Parts)

	return nil
}

func (p *prettyPrinter) VisitStar(node *Star) any {
	indent := p.indent

	p.writeLine(indent, "Star")
	p.writeNodeField(indent+1, "Qualifier", node.Qualifier)

	return nil
}

func (p *prettyPrinter) VisitBinaryExpr(node *BinaryExpr) any {
	indent := p.indent

	p.writeLine(indent, "BinaryExpr")
	p.writeStringField(indent+1, "Operator", node.Operator)
	p.writeNodeField(indent+1, "Left", node.Left)
	p.writeNodeField(indent+1, "Right", node.Right)

	return nil
}

func (p *prettyPrinter) VisitUnaryExpr(node *UnaryExpr) any {
	indent := p.indent

	p.writeLine(indent, "UnaryExpr")
	p.writeStringField(indent+1, "Operator", node.Operator)
	p.writeNodeField(indent+1, "Operand", node.Operand)

	return nil
}

func (p *prettyPrinter) VisitIntegerLiteral(node *IntegerLiteral) any {
	p.writeLine(p.indent, fmt.Sprintf("IntegerLiteral(Text=%s)", strconv.Quote(node.Text)))

	return nil
}

func (p *prettyPrinter) VisitFloatLiteral(node *FloatLiteral) any {
	p.writeLine(p.indent, fmt.Sprintf("FloatLiteral(Text=%s)", strconv.Quote(node.Text)))

	return nil
}

func (p *prettyPrinter) VisitStringLiteral(node *StringLiteral) any {
	p.writeLine(p.indent, fmt.Sprintf("StringLiteral(Value=%s)", strconv.Quote(node.Value)))

	return nil
}

func (p *prettyPrinter) VisitBoolLiteral(node *BoolLiteral) any {
	p.writeLine(p.indent, fmt.Sprintf("BoolLiteral(Value=%t)", node.Value))

	return nil
}

func (p *prettyPrinter) VisitNullLiteral(*NullLiteral) any {
	p.writeLine(p.indent, "NullLiteral")

	return nil
}

func (p *prettyPrinter) VisitParamLiteral(node *ParamLiteral) any {
	p.writeLine(p.indent, fmt.Sprintf("ParamLiteral(Text=%s)", strconv.Quote(node.Text)))

	return nil
}
