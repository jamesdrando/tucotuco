package ast

import "testing"

func TestIdentifierAcceptDispatch(t *testing.T) {
	t.Parallel()

	start := testPos(t, 5)
	end := testPos(t, 6)
	identifier := &Identifier{
		Span: NewSpan(start, end),
		Name: "customer_id",
	}

	if got := identifier.Pos(); got != start {
		t.Fatalf("Pos() = %#v, want %#v", got, start)
	}

	if got := identifier.End(); got != end {
		t.Fatalf("End() = %#v, want %#v", got, end)
	}

	visitor := &nameRecordingVisitor{}

	if got := identifier.Accept(visitor); got != "identifier" {
		t.Fatalf("Accept() = %#v, want %#v", got, "identifier")
	}

	if visitor.identifier != identifier {
		t.Fatalf("VisitIdentifier node = %p, want %p", visitor.identifier, identifier)
	}
}

func TestQualifiedNameAcceptDispatch(t *testing.T) {
	t.Parallel()

	partOne := &Identifier{
		Span: NewSpan(testPos(t, 7), testPos(t, 8)),
		Name: "sales",
	}
	partTwo := &Identifier{
		Span: NewSpan(testPos(t, 9), testPos(t, 10)),
		Name: "orders",
	}

	start := partOne.Pos()
	end := partTwo.End()
	name := &QualifiedName{
		Span:  NewSpan(start, end),
		Parts: []*Identifier{partOne, partTwo},
	}

	if got := name.Pos(); got != start {
		t.Fatalf("Pos() = %#v, want %#v", got, start)
	}

	if got := name.End(); got != end {
		t.Fatalf("End() = %#v, want %#v", got, end)
	}

	visitor := &nameRecordingVisitor{}

	if got := name.Accept(visitor); got != "qualified_name" {
		t.Fatalf("Accept() = %#v, want %#v", got, "qualified_name")
	}

	if visitor.qualifiedName != name {
		t.Fatalf("VisitQualifiedName node = %p, want %p", visitor.qualifiedName, name)
	}
}

func TestStarAcceptDispatch(t *testing.T) {
	t.Parallel()

	qualifier := &QualifiedName{
		Span: NewSpan(testPos(t, 11), testPos(t, 12)),
		Parts: []*Identifier{
			{
				Span: NewSpan(testPos(t, 13), testPos(t, 14)),
				Name: "orders",
			},
		},
	}

	start := qualifier.Pos()
	end := testPos(t, 15)
	star := &Star{
		Span:      NewSpan(start, end),
		Qualifier: qualifier,
	}

	if got := star.Pos(); got != start {
		t.Fatalf("Pos() = %#v, want %#v", got, start)
	}

	if got := star.End(); got != end {
		t.Fatalf("End() = %#v, want %#v", got, end)
	}

	visitor := &nameRecordingVisitor{}

	if got := star.Accept(visitor); got != "star" {
		t.Fatalf("Accept() = %#v, want %#v", got, "star")
	}

	if visitor.star != star {
		t.Fatalf("VisitStar node = %p, want %p", visitor.star, star)
	}
}

type nameRecordingVisitor struct {
	noopVisitor

	identifier    *Identifier
	qualifiedName *QualifiedName
	star          *Star
}

func (v *nameRecordingVisitor) VisitIdentifier(identifier *Identifier) any {
	v.identifier = identifier
	return "identifier"
}

func (v *nameRecordingVisitor) VisitQualifiedName(name *QualifiedName) any {
	v.qualifiedName = name
	return "qualified_name"
}

func (v *nameRecordingVisitor) VisitStar(star *Star) any {
	v.star = star
	return "star"
}
