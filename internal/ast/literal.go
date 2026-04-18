package ast

// IntegerLiteral represents an integer numeric literal.
type IntegerLiteral struct {
	Span

	// Text stores the literal exactly as written in the source.
	Text string
}

// Accept dispatches the node to its concrete visitor method.
func (l *IntegerLiteral) Accept(visitor Visitor) any {
	return visitor.VisitIntegerLiteral(l)
}

// FloatLiteral represents a non-integer numeric literal.
type FloatLiteral struct {
	Span

	// Text stores the literal exactly as written in the source.
	Text string
}

// Accept dispatches the node to its concrete visitor method.
func (l *FloatLiteral) Accept(visitor Visitor) any {
	return visitor.VisitFloatLiteral(l)
}

// StringLiteral represents a character string literal.
type StringLiteral struct {
	Span

	// Value stores the unescaped string contents.
	Value string
}

// Accept dispatches the node to its concrete visitor method.
func (l *StringLiteral) Accept(visitor Visitor) any {
	return visitor.VisitStringLiteral(l)
}

// BoolLiteral represents a TRUE or FALSE literal.
type BoolLiteral struct {
	Span

	// Value stores the literal boolean value.
	Value bool
}

// Accept dispatches the node to its concrete visitor method.
func (l *BoolLiteral) Accept(visitor Visitor) any {
	return visitor.VisitBoolLiteral(l)
}

// NullLiteral represents a NULL literal.
type NullLiteral struct {
	Span
}

// Accept dispatches the node to its concrete visitor method.
func (l *NullLiteral) Accept(visitor Visitor) any {
	return visitor.VisitNullLiteral(l)
}

// ParamLiteral represents a parameter marker such as ?, $1, or :name.
type ParamLiteral struct {
	Span

	// Text stores the parameter marker exactly as written in the source.
	Text string
}

// Accept dispatches the node to its concrete visitor method.
func (l *ParamLiteral) Accept(visitor Visitor) any {
	return visitor.VisitParamLiteral(l)
}
