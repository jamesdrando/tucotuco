package ast

import "testing"

func TestBinaryExprAcceptDispatch(t *testing.T) {
	t.Parallel()

	leftStart := testPos(t, 20)
	leftEnd := testPos(t, 21)
	rightStart := testPos(t, 22)
	rightEnd := testPos(t, 23)
	left := &Identifier{
		Span: NewSpan(leftStart, leftEnd),
		Name: "a",
	}
	right := &IntegerLiteral{
		Span: NewSpan(rightStart, rightEnd),
		Text: "7",
	}
	node := &BinaryExpr{
		Span:     NewSpan(left.Pos(), right.End()),
		Operator: "+",
		Left:     left,
		Right:    right,
	}

	if got := node.Pos(); got != leftStart {
		t.Fatalf("Pos() = %#v, want %#v", got, leftStart)
	}

	if got := node.End(); got != rightEnd {
		t.Fatalf("End() = %#v, want %#v", got, rightEnd)
	}

	if got := node.Operator; got != "+" {
		t.Fatalf("Operator = %q, want %q", got, "+")
	}

	if got := node.Left; got != left {
		t.Fatalf("Left = %#v, want %#v", got, left)
	}

	if got := node.Right; got != right {
		t.Fatalf("Right = %#v, want %#v", got, right)
	}

	visitor := &exprRecordingVisitor{}
	if got := node.Accept(visitor); got != "binary" {
		t.Fatalf("Accept() = %#v, want %#v", got, "binary")
	}

	if visitor.binary != node {
		t.Fatalf("VisitBinaryExpr node = %p, want %p", visitor.binary, node)
	}
}

func TestUnaryExprAcceptDispatch(t *testing.T) {
	t.Parallel()

	operandStart := testPos(t, 24)
	operandEnd := testPos(t, 25)
	operand := &StringLiteral{
		Span:  NewSpan(operandStart, operandEnd),
		Value: "x",
	}
	node := &UnaryExpr{
		Span:     NewSpan(operandStart, operandEnd),
		Operator: "-",
		Operand:  operand,
	}

	if got := node.Pos(); got != operandStart {
		t.Fatalf("Pos() = %#v, want %#v", got, operandStart)
	}

	if got := node.End(); got != operandEnd {
		t.Fatalf("End() = %#v, want %#v", got, operandEnd)
	}

	if got := node.Operator; got != "-" {
		t.Fatalf("Operator = %q, want %q", got, "-")
	}

	if got := node.Operand; got != operand {
		t.Fatalf("Operand = %#v, want %#v", got, operand)
	}

	visitor := &exprRecordingVisitor{}
	if got := node.Accept(visitor); got != "unary" {
		t.Fatalf("Accept() = %#v, want %#v", got, "unary")
	}

	if visitor.unary != node {
		t.Fatalf("VisitUnaryExpr node = %p, want %p", visitor.unary, node)
	}
}

type exprRecordingVisitor struct {
	noopVisitor

	binary *BinaryExpr
	unary  *UnaryExpr
}

func (v *exprRecordingVisitor) VisitBinaryExpr(expr *BinaryExpr) any {
	v.binary = expr
	return "binary"
}

func (v *exprRecordingVisitor) VisitUnaryExpr(expr *UnaryExpr) any {
	v.unary = expr
	return "unary"
}
