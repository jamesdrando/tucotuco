package ast

import (
	"reflect"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/token"
)

func TestNewSpanTracksPositions(t *testing.T) {
	t.Parallel()

	start := testPos(t, 1)
	end := testPos(t, 2)
	span := NewSpan(start, end)

	if got := span.Pos(); got != start {
		t.Fatalf("Pos() = %#v, want %#v", got, start)
	}

	if got := span.End(); got != end {
		t.Fatalf("End() = %#v, want %#v", got, end)
	}
}

func TestScriptAcceptDispatch(t *testing.T) {
	t.Parallel()

	start := testPos(t, 3)
	end := testPos(t, 4)
	script := &Script{
		Span: NewSpan(start, end),
	}

	if got := script.Pos(); got != start {
		t.Fatalf("Pos() = %#v, want %#v", got, start)
	}

	if got := script.End(); got != end {
		t.Fatalf("End() = %#v, want %#v", got, end)
	}

	visitor := &recordingVisitor{}

	if got := script.Accept(visitor); got != "script" {
		t.Fatalf("Accept() = %#v, want %#v", got, "script")
	}

	if visitor.script != script {
		t.Fatalf("VisitScript node = %p, want %p", visitor.script, script)
	}
}

func testPos(t *testing.T, seed int) token.Pos {
	t.Helper()

	var pos token.Pos
	value := reflect.ValueOf(&pos).Elem()

	if !setTestValue(value, seed) {
		t.Fatalf("token.Pos type %s cannot be populated for AST tests", value.Type())
	}

	return pos
}

func setTestValue(value reflect.Value, seed int) bool {
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value.SetInt(int64(seed))
		return true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		value.SetUint(uint64(seed))
		return true
	case reflect.Array:
		if value.Len() == 0 {
			return false
		}

		for i := range value.Len() {
			if !setTestValue(value.Index(i), seed+i+1) {
				return false
			}
		}

		return true
	case reflect.Struct:
		if value.NumField() == 0 {
			return false
		}

		settableField := false
		for i := range value.NumField() {
			field := value.Field(i)
			if !field.CanSet() {
				continue
			}

			settableField = true
			if !setTestValue(field, seed+i+1) {
				return false
			}
		}

		return settableField
	default:
		return false
	}
}

type recordingVisitor struct {
	noopVisitor

	script *Script
}

func (v *recordingVisitor) VisitScript(script *Script) any {
	v.script = script
	return "script"
}
