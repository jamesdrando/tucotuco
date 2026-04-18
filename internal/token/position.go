package token

import "fmt"

// Pos identifies a location in SQL input.
type Pos struct {
	Line   int
	Column int
	Offset int
}

// IsZero reports whether the position is unset.
func (p Pos) IsZero() bool {
	return p.Line == 0 && p.Column == 0 && p.Offset == 0
}

// IsValid reports whether the position carries source information.
func (p Pos) IsValid() bool {
	return !p.IsZero()
}

// String formats the source position for debug output.
func (p Pos) String() string {
	if p.IsZero() {
		return "unknown"
	}

	return fmt.Sprintf("%d:%d (offset %d)", p.Line, p.Column, p.Offset)
}

// Span captures a start and end position. AST nodes can embed Span directly.
type Span struct {
	Start Pos
	Stop  Pos
}

// Pos returns the start position of the span.
func (s Span) Pos() Pos {
	return s.Start
}

// End returns the end position of the span.
func (s Span) End() Pos {
	return s.Stop
}

// IsZero reports whether both sides of the span are unset.
func (s Span) IsZero() bool {
	return s.Start.IsZero() && s.Stop.IsZero()
}

// String formats the span for debug output.
func (s Span) String() string {
	if s.IsZero() {
		return "unknown"
	}

	if s.Start == s.Stop {
		return s.Start.String()
	}

	return fmt.Sprintf("%s..%s", s.Start, s.Stop)
}
