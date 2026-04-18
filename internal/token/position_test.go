package token

import "testing"

func TestPosFormattingAndValidity(t *testing.T) {
	t.Parallel()

	zero := Pos{}
	if zero.IsValid() {
		t.Fatalf("zero position IsValid = true, want false")
	}

	if got := zero.String(); got != "unknown" {
		t.Fatalf("zero position String() = %q, want %q", got, "unknown")
	}

	pos := Pos{Line: 3, Column: 14, Offset: 57}
	if !pos.IsValid() {
		t.Fatalf("position IsValid = false, want true")
	}

	if got := pos.String(); got != "3:14 (offset 57)" {
		t.Fatalf("position String() = %q, want %q", got, "3:14 (offset 57)")
	}
}

func TestSpanMethods(t *testing.T) {
	t.Parallel()

	zero := Span{}
	if !zero.IsZero() {
		t.Fatalf("zero span IsZero = false, want true")
	}

	if got := zero.String(); got != "unknown" {
		t.Fatalf("zero span String() = %q, want %q", got, "unknown")
	}

	start := Pos{Line: 1, Column: 1, Offset: 0}
	stop := Pos{Line: 1, Column: 6, Offset: 5}
	span := Span{Start: start, Stop: stop}

	if got := span.Pos(); got != start {
		t.Fatalf("Pos() = %#v, want %#v", got, start)
	}

	if got := span.End(); got != stop {
		t.Fatalf("End() = %#v, want %#v", got, stop)
	}

	if got := span.String(); got != "1:1 (offset 0)..1:6 (offset 5)" {
		t.Fatalf("String() = %q, want %q", got, "1:1 (offset 0)..1:6 (offset 5)")
	}
}
