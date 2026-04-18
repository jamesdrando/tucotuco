package ast

import "testing"

func TestLiteralNodesReportSpansAndDispatch(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		build func(Span) Node
		want  string
		check func(*testing.T, *literalRecordingVisitor, Node)
	}{
		{
			name: "integer",
			build: func(span Span) Node {
				return &IntegerLiteral{
					Span: span,
					Text: "42",
				}
			},
			want: "integer",
			check: func(t *testing.T, visitor *literalRecordingVisitor, node Node) {
				t.Helper()

				literal := node.(*IntegerLiteral)
				if literal.Text != "42" {
					t.Fatalf("Text = %q, want %q", literal.Text, "42")
				}
				if visitor.integer != literal {
					t.Fatalf("VisitIntegerLiteral node = %p, want %p", visitor.integer, literal)
				}
			},
		},
		{
			name: "float",
			build: func(span Span) Node {
				return &FloatLiteral{
					Span: span,
					Text: "3.14e0",
				}
			},
			want: "float",
			check: func(t *testing.T, visitor *literalRecordingVisitor, node Node) {
				t.Helper()

				literal := node.(*FloatLiteral)
				if literal.Text != "3.14e0" {
					t.Fatalf("Text = %q, want %q", literal.Text, "3.14e0")
				}
				if visitor.float != literal {
					t.Fatalf("VisitFloatLiteral node = %p, want %p", visitor.float, literal)
				}
			},
		},
		{
			name: "string",
			build: func(span Span) Node {
				return &StringLiteral{
					Span:  span,
					Value: "hello",
				}
			},
			want: "string",
			check: func(t *testing.T, visitor *literalRecordingVisitor, node Node) {
				t.Helper()

				literal := node.(*StringLiteral)
				if literal.Value != "hello" {
					t.Fatalf("Value = %q, want %q", literal.Value, "hello")
				}
				if visitor.string != literal {
					t.Fatalf("VisitStringLiteral node = %p, want %p", visitor.string, literal)
				}
			},
		},
		{
			name: "bool",
			build: func(span Span) Node {
				return &BoolLiteral{
					Span:  span,
					Value: true,
				}
			},
			want: "bool",
			check: func(t *testing.T, visitor *literalRecordingVisitor, node Node) {
				t.Helper()

				literal := node.(*BoolLiteral)
				if !literal.Value {
					t.Fatal("Value = false, want true")
				}
				if visitor.bool != literal {
					t.Fatalf("VisitBoolLiteral node = %p, want %p", visitor.bool, literal)
				}
			},
		},
		{
			name: "null",
			build: func(span Span) Node {
				return &NullLiteral{Span: span}
			},
			want: "null",
			check: func(t *testing.T, visitor *literalRecordingVisitor, node Node) {
				t.Helper()

				literal := node.(*NullLiteral)
				if visitor.null != literal {
					t.Fatalf("VisitNullLiteral node = %p, want %p", visitor.null, literal)
				}
			},
		},
		{
			name: "param",
			build: func(span Span) Node {
				return &ParamLiteral{
					Span: span,
					Text: "$1",
				}
			},
			want: "param",
			check: func(t *testing.T, visitor *literalRecordingVisitor, node Node) {
				t.Helper()

				literal := node.(*ParamLiteral)
				if literal.Text != "$1" {
					t.Fatalf("Text = %q, want %q", literal.Text, "$1")
				}
				if visitor.param != literal {
					t.Fatalf("VisitParamLiteral node = %p, want %p", visitor.param, literal)
				}
			},
		},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			start := testPos(t, 20+i*2)
			end := testPos(t, 21+i*2)
			node := tc.build(NewSpan(start, end))

			if got := node.Pos(); got != start {
				t.Fatalf("Pos() = %#v, want %#v", got, start)
			}

			if got := node.End(); got != end {
				t.Fatalf("End() = %#v, want %#v", got, end)
			}

			visitor := &literalRecordingVisitor{}
			if got := node.Accept(visitor); got != tc.want {
				t.Fatalf("Accept() = %#v, want %#v", got, tc.want)
			}

			tc.check(t, visitor, node)
		})
	}
}

func (*recordingVisitor) VisitIdentifier(*Identifier) any {
	return nil
}

func (*recordingVisitor) VisitQualifiedName(*QualifiedName) any {
	return nil
}

func (*recordingVisitor) VisitStar(*Star) any {
	return nil
}

func (*recordingVisitor) VisitIntegerLiteral(*IntegerLiteral) any {
	return nil
}

func (*recordingVisitor) VisitFloatLiteral(*FloatLiteral) any {
	return nil
}

func (*recordingVisitor) VisitStringLiteral(*StringLiteral) any {
	return nil
}

func (*recordingVisitor) VisitBoolLiteral(*BoolLiteral) any {
	return nil
}

func (*recordingVisitor) VisitNullLiteral(*NullLiteral) any {
	return nil
}

func (*recordingVisitor) VisitParamLiteral(*ParamLiteral) any {
	return nil
}

func (noopVisitor) VisitIntegerLiteral(*IntegerLiteral) any {
	return nil
}

func (noopVisitor) VisitFloatLiteral(*FloatLiteral) any {
	return nil
}

func (noopVisitor) VisitStringLiteral(*StringLiteral) any {
	return nil
}

func (noopVisitor) VisitBoolLiteral(*BoolLiteral) any {
	return nil
}

func (noopVisitor) VisitNullLiteral(*NullLiteral) any {
	return nil
}

func (noopVisitor) VisitParamLiteral(*ParamLiteral) any {
	return nil
}

type literalRecordingVisitor struct {
	noopVisitor

	integer *IntegerLiteral
	float   *FloatLiteral
	string  *StringLiteral
	bool    *BoolLiteral
	null    *NullLiteral
	param   *ParamLiteral
}

func (v *literalRecordingVisitor) VisitIntegerLiteral(literal *IntegerLiteral) any {
	v.integer = literal
	return "integer"
}

func (v *literalRecordingVisitor) VisitFloatLiteral(literal *FloatLiteral) any {
	v.float = literal
	return "float"
}

func (v *literalRecordingVisitor) VisitStringLiteral(literal *StringLiteral) any {
	v.string = literal
	return "string"
}

func (v *literalRecordingVisitor) VisitBoolLiteral(literal *BoolLiteral) any {
	v.bool = literal
	return "bool"
}

func (v *literalRecordingVisitor) VisitNullLiteral(literal *NullLiteral) any {
	v.null = literal
	return "null"
}

func (v *literalRecordingVisitor) VisitParamLiteral(literal *ParamLiteral) any {
	v.param = literal
	return "param"
}
