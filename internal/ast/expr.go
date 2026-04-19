package ast

// BinaryExpr represents a binary SQL expression such as `a + b`.
type BinaryExpr struct {
	Span

	// Operator stores the exact operator lexeme from the source.
	Operator string

	// Left is the left-hand operand.
	Left Node

	// Right is the right-hand operand.
	Right Node
}

// Accept dispatches the node to its concrete visitor method.
func (e *BinaryExpr) Accept(visitor Visitor) any {
	return visitor.VisitBinaryExpr(e)
}

// UnaryExpr represents a unary SQL expression such as `-x` or `NOT x`.
type UnaryExpr struct {
	Span

	// Operator stores the exact operator lexeme from the source.
	Operator string

	// Operand is the expression being negated or otherwise transformed.
	Operand Node
}

// Accept dispatches the node to its concrete visitor method.
func (e *UnaryExpr) Accept(visitor Visitor) any {
	return visitor.VisitUnaryExpr(e)
}
