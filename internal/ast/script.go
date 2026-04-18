package ast

// Script is the root node for a parsed SQL input.
type Script struct {
	Span

	Nodes []Node
}

// Accept dispatches the node to its concrete visitor method.
func (s *Script) Accept(visitor Visitor) any {
	return visitor.VisitScript(s)
}
