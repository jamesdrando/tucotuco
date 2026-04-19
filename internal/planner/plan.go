package planner

import (
	"fmt"
	"strings"

	"github.com/jamesdrando/tucotuco/internal/parser"
	"github.com/jamesdrando/tucotuco/internal/storage"
	sqltypes "github.com/jamesdrando/tucotuco/internal/types"
)

// Kind identifies one logical plan operator.
type Kind string

const (
	// KindScan identifies a logical table scan.
	KindScan Kind = "Scan"
	// KindFilter identifies a logical selection predicate.
	KindFilter Kind = "Filter"
	// KindProject identifies a logical projection.
	KindProject Kind = "Project"
	// KindLimit identifies a logical row-count limit.
	KindLimit Kind = "Limit"
)

// String returns the display name of the plan kind.
func (k Kind) String() string {
	return string(k)
}

// Plan is the root contract implemented by every logical plan node.
//
// String returns a stable one-line summary suitable for tests and later
// EXPLAIN-style rendering.
type Plan interface {
	fmt.Stringer

	Kind() Kind
	Children() []Plan
	Columns() []Column
	plan()
}

// Column describes one output column exposed by a logical plan node.
type Column struct {
	// Name is the visible output column name.
	Name string
	// Type is the semantic SQL type of the column.
	Type sqltypes.TypeDesc
}

// String returns a compact display form of the output column.
func (c Column) String() string {
	switch {
	case c.Name == "" && c.Type.Kind == sqltypes.TypeKindInvalid:
		return "?"
	case c.Name == "":
		return c.Type.String()
	case c.Type.Kind == sqltypes.TypeKindInvalid:
		return c.Name
	default:
		return c.Name + " " + c.Type.String()
	}
}

// Projection describes one projected expression together with its output
// column metadata.
type Projection struct {
	// Expr is the analyzed CST expression that produces the output value.
	Expr parser.Node
	// Output describes the visible output column emitted by the projection.
	Output Column
}

// Scan reads rows from one catalog-backed table.
type Scan struct {
	// Table identifies the catalog table being scanned.
	Table storage.TableID
	// OutputColumns are the visible columns emitted by the scan.
	OutputColumns []Column
}

// NewScan constructs a logical scan over one table.
func NewScan(table storage.TableID, columns ...Column) *Scan {
	return &Scan{
		Table:         table,
		OutputColumns: append([]Column(nil), columns...),
	}
}

func (*Scan) plan() {}

// Kind reports the operator kind.
func (*Scan) Kind() Kind {
	return KindScan
}

// Children reports the scan inputs.
func (*Scan) Children() []Plan {
	return nil
}

// Columns reports the scan output schema.
func (s *Scan) Columns() []Column {
	if s == nil {
		return nil
	}

	return append([]Column(nil), s.OutputColumns...)
}

// String returns a stable one-line rendering of the scan.
func (s *Scan) String() string {
	if s == nil {
		return KindScan.String()
	}

	var details []string
	if s.Table.Valid() {
		details = append(details, "table="+s.Table.String())
	}
	if len(s.OutputColumns) != 0 {
		details = append(details, "columns=["+formatColumns(s.OutputColumns)+"]")
	}

	return formatPlan(KindScan, details...)
}

// Filter applies a boolean predicate to its input rows.
type Filter struct {
	// Input is the child plan whose rows are filtered.
	Input Plan
	// Predicate is the analyzed boolean expression to evaluate per row.
	Predicate parser.Node
}

// NewFilter constructs a logical filter node.
func NewFilter(input Plan, predicate parser.Node) *Filter {
	return &Filter{
		Input:     input,
		Predicate: predicate,
	}
}

func (*Filter) plan() {}

// Kind reports the operator kind.
func (*Filter) Kind() Kind {
	return KindFilter
}

// Children reports the filter input.
func (f *Filter) Children() []Plan {
	if f == nil || f.Input == nil {
		return nil
	}

	return []Plan{f.Input}
}

// Columns reports the filter output schema.
func (f *Filter) Columns() []Column {
	if f == nil || f.Input == nil {
		return nil
	}

	return f.Input.Columns()
}

// String returns a stable one-line rendering of the filter.
func (f *Filter) String() string {
	if f == nil {
		return KindFilter.String()
	}
	if f.Predicate == nil {
		return KindFilter.String()
	}

	return formatPlan(KindFilter, "predicate="+formatExpr(f.Predicate))
}

// Project evaluates expressions and reshapes the visible output columns.
type Project struct {
	// Input is the child plan whose rows feed the projection expressions.
	Input Plan
	// Projections are evaluated in order to produce the output row.
	Projections []Projection
}

// NewProject constructs a logical projection node.
func NewProject(input Plan, projections ...Projection) *Project {
	return &Project{
		Input:       input,
		Projections: append([]Projection(nil), projections...),
	}
}

func (*Project) plan() {}

// Kind reports the operator kind.
func (*Project) Kind() Kind {
	return KindProject
}

// Children reports the projection input.
func (p *Project) Children() []Plan {
	if p == nil || p.Input == nil {
		return nil
	}

	return []Plan{p.Input}
}

// Columns reports the projection output schema.
func (p *Project) Columns() []Column {
	if p == nil {
		return nil
	}

	out := make([]Column, 0, len(p.Projections))
	for _, projection := range p.Projections {
		out = append(out, projection.Output)
	}

	return out
}

// String returns a stable one-line rendering of the projection.
func (p *Project) String() string {
	if p == nil || len(p.Projections) == 0 {
		return KindProject.String()
	}

	return formatPlan(KindProject, "columns=["+formatProjectionOutputs(p.Projections)+"]")
}

// Limit truncates its input to a fixed number of rows.
type Limit struct {
	// Input is the child plan whose rows are truncated.
	Input Plan
	// Count is the analyzed row-count expression supplied by the query.
	Count parser.Node
}

// NewLimit constructs a logical limit node.
func NewLimit(input Plan, count parser.Node) *Limit {
	return &Limit{
		Input: input,
		Count: count,
	}
}

func (*Limit) plan() {}

// Kind reports the operator kind.
func (*Limit) Kind() Kind {
	return KindLimit
}

// Children reports the limit input.
func (l *Limit) Children() []Plan {
	if l == nil || l.Input == nil {
		return nil
	}

	return []Plan{l.Input}
}

// Columns reports the limit output schema.
func (l *Limit) Columns() []Column {
	if l == nil || l.Input == nil {
		return nil
	}

	return l.Input.Columns()
}

// String returns a stable one-line rendering of the limit.
func (l *Limit) String() string {
	if l == nil || l.Count == nil {
		return KindLimit.String()
	}

	return formatPlan(KindLimit, "count="+formatExpr(l.Count))
}

func formatPlan(kind Kind, details ...string) string {
	filtered := make([]string, 0, len(details))
	for _, detail := range details {
		if detail == "" {
			continue
		}
		filtered = append(filtered, detail)
	}
	if len(filtered) == 0 {
		return kind.String()
	}

	return kind.String() + "(" + strings.Join(filtered, ", ") + ")"
}

func formatColumns(columns []Column) string {
	parts := make([]string, 0, len(columns))
	for _, column := range columns {
		parts = append(parts, column.String())
	}

	return strings.Join(parts, ", ")
}

func formatProjectionOutputs(projections []Projection) string {
	parts := make([]string, 0, len(projections))
	for _, projection := range projections {
		parts = append(parts, projection.Output.String())
	}

	return strings.Join(parts, ", ")
}

func formatExpr(node parser.Node) string {
	switch expr := node.(type) {
	case nil:
		return ""
	case *parser.Identifier:
		return expr.Name
	case *parser.QualifiedName:
		return formatQualifiedName(expr)
	case *parser.Star:
		if expr.Qualifier == nil {
			return "*"
		}
		return formatQualifiedName(expr.Qualifier) + ".*"
	case *parser.IntegerLiteral:
		return expr.Text
	case *parser.FloatLiteral:
		return expr.Text
	case *parser.StringLiteral:
		return quoteStringLiteral(expr.Value)
	case *parser.BoolLiteral:
		if expr.Value {
			return "TRUE"
		}
		return "FALSE"
	case *parser.NullLiteral:
		return "NULL"
	case *parser.ParamLiteral:
		return expr.Text
	case *parser.UnaryExpr:
		if expr.Operand == nil {
			return expr.Operator
		}
		return expr.Operator + " " + formatExpr(expr.Operand)
	case *parser.BinaryExpr:
		return formatBinaryExpr(expr)
	case *parser.FunctionCall:
		return formatFunctionCall(expr)
	case *parser.CastExpr:
		return "CAST(" + formatExpr(expr.Expr) + " AS " + formatTypeName(expr.Type) + ")"
	case *parser.BetweenExpr:
		return formatBetweenExpr(expr)
	case *parser.InExpr:
		return formatInExpr(expr)
	case *parser.LikeExpr:
		return formatLikeExpr(expr)
	case *parser.IsExpr:
		return formatIsExpr(expr)
	default:
		return fmt.Sprintf("%T", node)
	}
}

func formatBinaryExpr(expr *parser.BinaryExpr) string {
	if expr == nil {
		return ""
	}

	left := formatExpr(expr.Left)
	right := formatExpr(expr.Right)
	if left == "" {
		return right
	}
	if right == "" {
		return left
	}

	return left + " " + expr.Operator + " " + right
}

func formatFunctionCall(expr *parser.FunctionCall) string {
	if expr == nil {
		return ""
	}

	var args []string
	for _, arg := range expr.Args {
		args = append(args, formatExpr(arg))
	}

	argText := strings.Join(args, ", ")
	if expr.SetQuantifier != "" {
		if argText != "" {
			argText = expr.SetQuantifier + " " + argText
		} else {
			argText = expr.SetQuantifier
		}
	}

	return formatQualifiedName(expr.Name) + "(" + argText + ")"
}

func formatBetweenExpr(expr *parser.BetweenExpr) string {
	if expr == nil {
		return ""
	}

	operator := " BETWEEN "
	if expr.Negated {
		operator = " NOT BETWEEN "
	}

	return formatExpr(expr.Expr) + operator + formatExpr(expr.Lower) + " AND " + formatExpr(expr.Upper)
}

func formatInExpr(expr *parser.InExpr) string {
	if expr == nil {
		return ""
	}

	values := make([]string, 0, len(expr.List))
	for _, item := range expr.List {
		values = append(values, formatExpr(item))
	}

	operator := " IN "
	if expr.Negated {
		operator = " NOT IN "
	}

	return formatExpr(expr.Expr) + operator + "(" + strings.Join(values, ", ") + ")"
}

func formatLikeExpr(expr *parser.LikeExpr) string {
	if expr == nil {
		return ""
	}

	operator := " LIKE "
	if expr.Negated {
		operator = " NOT LIKE "
	}

	text := formatExpr(expr.Expr) + operator + formatExpr(expr.Pattern)
	if expr.Escape != nil {
		text += " ESCAPE " + formatExpr(expr.Escape)
	}

	return text
}

func formatIsExpr(expr *parser.IsExpr) string {
	if expr == nil {
		return ""
	}

	text := formatExpr(expr.Expr) + " IS "
	if expr.Negated {
		text += "NOT "
	}
	text += expr.Predicate
	if expr.Right != nil {
		text += " " + formatExpr(expr.Right)
	}

	return text
}

func formatQualifiedName(name *parser.QualifiedName) string {
	if name == nil {
		return ""
	}

	parts := make([]string, 0, len(name.Parts))
	for _, part := range name.Parts {
		if part == nil {
			continue
		}
		parts = append(parts, part.Name)
	}

	return strings.Join(parts, ".")
}

func formatTypeName(name *parser.TypeName) string {
	if name == nil {
		return ""
	}

	parts := make([]string, 0, len(name.Names))
	for _, ident := range name.Names {
		if ident == nil {
			continue
		}
		parts = append(parts, ident.Name)
	}

	text := strings.Join(parts, " ")
	if name.Qualifier != nil {
		qualified := formatQualifiedName(name.Qualifier)
		switch {
		case qualified == "":
		case text == "":
			text = qualified
		default:
			text = qualified + "." + text
		}
	}
	if len(name.Args) == 0 {
		return text
	}

	args := make([]string, 0, len(name.Args))
	for _, arg := range name.Args {
		args = append(args, formatExpr(arg))
	}

	return text + "(" + strings.Join(args, ", ") + ")"
}

func quoteStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
