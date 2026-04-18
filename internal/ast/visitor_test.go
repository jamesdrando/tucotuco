package ast

type noopVisitor struct{}

func (noopVisitor) VisitScript(*Script) any {
	return nil
}

func (noopVisitor) VisitIdentifier(*Identifier) any {
	return nil
}

func (noopVisitor) VisitQualifiedName(*QualifiedName) any {
	return nil
}

func (noopVisitor) VisitStar(*Star) any {
	return nil
}
