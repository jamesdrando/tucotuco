package ast

// Identifier names a single SQL identifier token.
type Identifier struct {
	Span

	Name string
}

// Accept dispatches the node to its concrete visitor method.
func (i *Identifier) Accept(visitor Visitor) any {
	return visitor.VisitIdentifier(i)
}

// QualifiedName stores a dot-qualified SQL name.
type QualifiedName struct {
	Span

	Parts []*Identifier
}

// Accept dispatches the node to its concrete visitor method.
func (q *QualifiedName) Accept(visitor Visitor) any {
	return visitor.VisitQualifiedName(q)
}

// Star represents either an unqualified `*` or a qualified `name.*`.
type Star struct {
	Span

	Qualifier *QualifiedName
}

// Accept dispatches the node to its concrete visitor method.
func (s *Star) Accept(visitor Visitor) any {
	return visitor.VisitStar(s)
}
