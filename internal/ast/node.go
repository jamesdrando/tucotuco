package ast

import "github.com/jamesdrando/tucotuco/internal/token"

// Node is the root contract implemented by every AST node.
//
// Pos returns the first source position that belongs to the node. End returns
// the first source position immediately after the node.
type Node interface {
	Pos() token.Pos
	End() token.Pos
	Accept(Visitor) any
}

// Visitor is the stable base visitor for the AST.
//
// The visitor grows alongside the concrete node set. Each new AST node added by
// later tasks should contribute a matching VisitXxx method here.
type Visitor interface {
	VisitScript(*Script) any
	VisitSelectStmt(*SelectStmt) any
	VisitSelectItem(*SelectItem) any
	VisitFromSource(*FromSource) any
	VisitOrderByItem(*OrderByItem) any
	VisitInsertStmt(*InsertStmt) any
	VisitInsertValuesSource(*InsertValuesSource) any
	VisitInsertQuerySource(*InsertQuerySource) any
	VisitInsertDefaultValuesSource(*InsertDefaultValuesSource) any
	VisitUpdateAssignment(*UpdateAssignment) any
	VisitUpdateStmt(*UpdateStmt) any
	VisitDeleteStmt(*DeleteStmt) any
	VisitTypeName(*TypeName) any
	VisitReferenceSpec(*ReferenceSpec) any
	VisitConstraintDef(*ConstraintDef) any
	VisitColumnDef(*ColumnDef) any
	VisitCreateTableStmt(*CreateTableStmt) any
	VisitDropTableStmt(*DropTableStmt) any
	VisitIdentifier(*Identifier) any
	VisitQualifiedName(*QualifiedName) any
	VisitStar(*Star) any
	VisitBinaryExpr(*BinaryExpr) any
	VisitUnaryExpr(*UnaryExpr) any
	VisitIntegerLiteral(*IntegerLiteral) any
	VisitFloatLiteral(*FloatLiteral) any
	VisitStringLiteral(*StringLiteral) any
	VisitBoolLiteral(*BoolLiteral) any
	VisitNullLiteral(*NullLiteral) any
	VisitParamLiteral(*ParamLiteral) any
}

// Span stores the half-open source range for a node.
type Span struct {
	start token.Pos
	stop  token.Pos
}

// NewSpan constructs a node span from its start and end positions.
func NewSpan(start, end token.Pos) Span {
	return Span{
		start: start,
		stop:  end,
	}
}

// Pos returns the first source position in the span.
func (s Span) Pos() token.Pos {
	return s.start
}

// End returns the first source position immediately after the span.
func (s Span) End() token.Pos {
	return s.stop
}
