package planner

import (
	"reflect"
	"strings"
)

// Explain renders a logical plan as a stable indented tree suitable for
// EXPLAIN-style output and golden tests.
func Explain(plan Plan) string {
	if isNilPlan(plan) {
		return "<nil>"
	}

	var printer explainPrinter
	printer.print(plan, 0)

	return strings.TrimSuffix(printer.builder.String(), "\n")
}

type explainPrinter struct {
	builder strings.Builder
}

func (p *explainPrinter) print(plan Plan, indent int) {
	if isNilPlan(plan) {
		p.writeLine(indent, "<nil>")
		return
	}

	p.writeLine(indent, plan.String())
	for _, child := range plan.Children() {
		p.print(child, indent+1)
	}
}

func (p *explainPrinter) writeLine(indent int, text string) {
	p.builder.WriteString(strings.Repeat("  ", indent))
	p.builder.WriteString(text)
	p.builder.WriteByte('\n')
}

func isNilPlan(plan Plan) bool {
	if plan == nil {
		return true
	}

	value := reflect.ValueOf(plan)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
