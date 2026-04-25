package parser

import "github.com/jamesdrando/tucotuco/internal/token"

// SyntaxNode is the root contract for parser concrete-syntax nodes.
//
// These types are parser-local CST scaffolding only. They intentionally do not
// participate in the semantic AST defined in internal/ast; a later lowering
// step can translate this CST into the package-owned AST shape.
type SyntaxNode interface {
	Pos() token.Pos
	End() token.Pos
}

// Node is a backwards-compatible alias for the parser-local CST contract.
type Node = SyntaxNode

// QueryExpr marks parser nodes that produce a query result set.
type QueryExpr interface {
	Node
	queryExpr()
}

// Script is the CST root for a parsed SQL input.
type Script struct {
	token.Span

	Nodes []Node
}

// Identifier captures one SQL identifier token in CST form.
type Identifier struct {
	token.Span

	Name   string
	Quoted bool
}

// QualifiedName captures a possibly dot-qualified SQL name in CST form.
type QualifiedName struct {
	token.Span

	Parts []*Identifier
}

// Star captures either `*` or `qualifier.*` in the CST.
type Star struct {
	token.Span

	Qualifier *QualifiedName
}

// IntegerLiteral captures an integer numeric literal token.
type IntegerLiteral struct {
	token.Span

	Text string
}

// FloatLiteral captures a non-integer numeric literal token.
type FloatLiteral struct {
	token.Span

	Text string
}

// StringLiteral captures a character string literal token.
type StringLiteral struct {
	token.Span

	Value string
}

// BoolLiteral captures TRUE or FALSE.
type BoolLiteral struct {
	token.Span

	Value bool
}

// NullLiteral captures NULL.
type NullLiteral struct {
	token.Span
}

// ParamLiteral captures a parameter marker token.
type ParamLiteral struct {
	token.Span

	Text string
}

// UnaryExpr captures a unary SQL expression subtree.
type UnaryExpr struct {
	token.Span

	Operator string
	Operand  Node
}

// BinaryExpr captures a binary SQL expression subtree.
type BinaryExpr struct {
	token.Span

	Operator string
	Left     Node
	Right    Node
}

// FunctionCall captures a SQL function invocation subtree.
type FunctionCall struct {
	token.Span

	Name          *QualifiedName
	Args          []Node
	SetQuantifier string
}

// CastExpr captures CAST(expr AS type).
type CastExpr struct {
	token.Span

	Expr Node
	Type *TypeName
}

// WhenClause captures one WHEN ... THEN ... arm inside CASE.
type WhenClause struct {
	token.Span

	Condition Node
	Result    Node
}

// CaseExpr captures either a searched or simple CASE expression.
type CaseExpr struct {
	token.Span

	Operand Node
	Whens   []*WhenClause
	Else    Node
}

// BetweenExpr captures a BETWEEN predicate.
type BetweenExpr struct {
	token.Span

	Expr    Node
	Lower   Node
	Upper   Node
	Negated bool
}

// SubqueryExpr captures a parenthesized scalar subquery expression.
type SubqueryExpr struct {
	token.Span

	Query QueryExpr
}

// ExistsExpr captures an EXISTS predicate.
type ExistsExpr struct {
	token.Span

	Query QueryExpr
}

// InExpr captures an IN predicate with an expression list.
type InExpr struct {
	token.Span

	Expr    Node
	List    []Node
	Query   QueryExpr
	Negated bool
}

// LikeExpr captures a LIKE predicate.
type LikeExpr struct {
	token.Span

	Expr    Node
	Pattern Node
	Escape  Node
	Negated bool
}

// IsExpr captures an IS predicate such as IS NULL.
type IsExpr struct {
	token.Span

	Expr      Node
	Predicate string
	Right     Node
	Negated   bool
}

// TypeName captures a possibly-qualified SQL type name with optional arguments.
type TypeName struct {
	token.Span

	Qualifier *QualifiedName
	Names     []*Identifier
	Args      []Node
}

// SelectStmt captures a SELECT query specification.
type SelectStmt struct {
	token.Span

	SetQuantifier string
	SelectList    []*SelectItem
	From          []Node
	Where         Node
	GroupBy       []Node
	Having        Node
	OrderBy       []*OrderByItem
}

func (*SelectStmt) queryExpr() {}

// SetOpExpr captures a query expression that combines two query inputs with a
// SQL set operator.
type SetOpExpr struct {
	token.Span

	Left          QueryExpr
	Right         QueryExpr
	Operator      string
	SetQuantifier string
}

func (*SetOpExpr) queryExpr() {}

// ExplainStmt captures an EXPLAIN wrapper around a query expression.
type ExplainStmt struct {
	token.Span

	Query   QueryExpr
	Analyze bool
}

// SelectItem captures one SELECT-list expression with an optional alias.
type SelectItem struct {
	token.Span

	Expr  Node
	Alias *Identifier
}

// FromSource captures one FROM-clause relation or derived source with an
// optional alias.
type FromSource struct {
	token.Span

	Source Node
	Alias  *Identifier
}

// JoinExpr captures one joined-table subtree.
type JoinExpr struct {
	token.Span

	Left      Node
	Right     Node
	Type      string
	Natural   bool
	Condition Node
	Using     []*Identifier
}

// OrderByItem captures one ORDER BY expression and its direction.
type OrderByItem struct {
	token.Span

	Expr      Node
	Direction string
}

// InsertSource marks the allowed source forms for INSERT.
type InsertSource interface {
	Node
	insertSource()
}

// InsertValuesSource captures an INSERT ... VALUES source.
type InsertValuesSource struct {
	token.Span

	Rows [][]Node
}

func (*InsertValuesSource) insertSource() {}

// InsertQuerySource captures an INSERT ... SELECT source.
type InsertQuerySource struct {
	token.Span

	Query Node
}

func (*InsertQuerySource) insertSource() {}

// InsertDefaultValuesSource captures an INSERT ... DEFAULT VALUES source.
type InsertDefaultValuesSource struct {
	token.Span
}

func (*InsertDefaultValuesSource) insertSource() {}

// InsertStmt captures an INSERT statement.
type InsertStmt struct {
	token.Span

	Table   *QualifiedName
	Columns []*Identifier
	Source  InsertSource
}

// UpdateAssignment captures one UPDATE SET target/value group.
type UpdateAssignment struct {
	token.Span

	Columns []*Identifier
	Values  []Node
}

// UpdateStmt captures an UPDATE statement.
type UpdateStmt struct {
	token.Span

	Table       *QualifiedName
	Assignments []*UpdateAssignment
	Where       Node
}

// DeleteStmt captures a DELETE statement.
type DeleteStmt struct {
	token.Span

	Table *QualifiedName
	Where Node
}

// ConstraintKind enumerates the CREATE TABLE constraint forms used by the CST.
type ConstraintKind string

// ConstraintKind constants enumerate the CREATE TABLE constraint forms used by
// the CST.
const (
	ConstraintKindNull       ConstraintKind = "NULL"
	ConstraintKindNotNull    ConstraintKind = "NOT NULL"
	ConstraintKindCheck      ConstraintKind = "CHECK"
	ConstraintKindUnique     ConstraintKind = "UNIQUE"
	ConstraintKindPrimaryKey ConstraintKind = "PRIMARY KEY"
	ConstraintKindReferences ConstraintKind = "REFERENCES"
	ConstraintKindForeignKey ConstraintKind = "FOREIGN KEY"
)

// ReferenceSpec captures the target relation of a REFERENCES constraint.
type ReferenceSpec struct {
	token.Span

	Table   *QualifiedName
	Columns []*Identifier
}

// ConstraintDef captures either a column or table constraint.
type ConstraintDef struct {
	token.Span

	Name      *Identifier
	Kind      ConstraintKind
	Columns   []*Identifier
	Check     Node
	Reference *ReferenceSpec
}

// ColumnDef captures one CREATE TABLE column definition.
type ColumnDef struct {
	token.Span

	Name        *Identifier
	Type        *TypeName
	Default     Node
	Constraints []*ConstraintDef
}

// CreateTableStmt captures a CREATE TABLE statement.
type CreateTableStmt struct {
	token.Span

	Name        *QualifiedName
	Columns     []*ColumnDef
	Constraints []*ConstraintDef
}

// DropTableStmt captures a DROP TABLE statement.
type DropTableStmt struct {
	token.Span

	Name *QualifiedName
}

// CreateViewStmt captures a CREATE VIEW statement.
type CreateViewStmt struct {
	token.Span

	Name        *QualifiedName
	Columns     []*Identifier
	Query       QueryExpr
	CheckOption string
}

// DropViewStmt captures a DROP VIEW statement.
type DropViewStmt struct {
	token.Span

	Name *QualifiedName
}

// CreateSchemaStmt captures a CREATE SCHEMA statement.
type CreateSchemaStmt struct {
	token.Span

	Name *Identifier
}

// DropSchemaStmt captures a DROP SCHEMA statement.
type DropSchemaStmt struct {
	token.Span

	Name     *Identifier
	Behavior string
}

// BeginStmt captures BEGIN with its optional mode keyword.
type BeginStmt struct {
	token.Span

	Mode string
}

// CommitStmt captures COMMIT with its optional WORK keyword.
type CommitStmt struct {
	token.Span

	Work bool
}

// RollbackStmt captures ROLLBACK with its optional WORK keyword.
type RollbackStmt struct {
	token.Span

	Work bool
}
