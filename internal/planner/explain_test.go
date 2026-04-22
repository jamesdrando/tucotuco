package planner

import (
	"strings"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/parser"
	"github.com/jamesdrando/tucotuco/internal/storage"
)

func TestExplainNilPlan(t *testing.T) {
	t.Parallel()

	var plan Plan
	if got, want := Explain(plan), "<nil>"; got != want {
		t.Fatalf("Explain(nil) = %q, want %q", got, want)
	}

	var typedNil Plan = (*Scan)(nil)
	if got, want := Explain(typedNil), "<nil>"; got != want {
		t.Fatalf("Explain((*Scan)(nil)) = %q, want %q", got, want)
	}
}

func TestExplainBuildsStableIndentedTree(t *testing.T) {
	t.Parallel()

	stmt, bindings, types := analyzeSelect(t, plannerTestCatalog(t), "SELECT customer_id AS cid FROM orders WHERE total = 1")

	plan, diags := NewBuilder(bindings, types).Build(stmt)
	if len(diags) != 0 {
		t.Fatalf("Build() diagnostics = %#v, want none", diags)
	}

	if got, want := Explain(plan), trimExplain(`
		Project(columns=[cid INTEGER])
		  Filter(predicate=total = 1)
		    Scan(table=public.orders, columns=[id INTEGER NOT NULL, customer_id INTEGER, total INTEGER])
	`); got != want {
		t.Fatalf("Explain() = %q, want %q", got, want)
	}
}

func TestExplainBuildsStableDerivedTableTree(t *testing.T) {
	t.Parallel()

	stmt, bindings, types := analyzeSelect(t, plannerTestCatalog(t), "SELECT q.id FROM (SELECT id FROM orders) AS q")

	plan, diags := NewBuilder(bindings, types).Build(stmt)
	if len(diags) != 0 {
		t.Fatalf("Build() diagnostics = %#v, want none", diags)
	}

	if got, want := Explain(plan), trimExplain(`
		Project(columns=[id INTEGER NOT NULL])
		  Project(columns=[id INTEGER NOT NULL])
		    Scan(table=public.orders, columns=[id INTEGER NOT NULL, customer_id INTEGER, total INTEGER])
	`); got != want {
		t.Fatalf("Explain() = %q, want %q", got, want)
	}
}

func TestExplainBuildsStableLimitTree(t *testing.T) {
	t.Parallel()

	scan := NewScan(
		storage.TableID{Schema: "public", Name: "orders"},
		Column{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER")},
	)
	limit := NewLimit(scan, &parser.IntegerLiteral{Text: "10"})

	if got, want := Explain(limit), trimExplain(`
		Limit(count=10)
		  Scan(table=public.orders, columns=[customer_id INTEGER])
	`); got != want {
		t.Fatalf("Explain() = %q, want %q", got, want)
	}
}

func TestExplainBuildsStableJoinTree(t *testing.T) {
	t.Parallel()

	stmt, bindings, types := analyzeSelect(t, plannerTestCatalog(t), "SELECT o.id FROM orders AS o LEFT JOIN customers AS c ON o.customer_id = c.id")

	plan, diags := NewBuilder(bindings, types).Build(stmt)
	if len(diags) != 0 {
		t.Fatalf("Build() diagnostics = %#v, want none", diags)
	}

	if got, want := Explain(plan), trimExplain(`
		Project(columns=[id INTEGER NOT NULL])
		  Join(type=LEFT, condition=o.customer_id = c.id)
		    Scan(table=public.orders, columns=[id INTEGER NOT NULL, customer_id INTEGER, total INTEGER])
		    Scan(table=public.customers, columns=[id INTEGER NOT NULL, customer_id INTEGER])
	`); got != want {
		t.Fatalf("Explain() = %q, want %q", got, want)
	}
}

func TestExplainFormatsCorrelatedExistsPredicate(t *testing.T) {
	t.Parallel()

	stmt, bindings, types := analyzeSelect(t, plannerTestCatalog(t), "SELECT o.id FROM orders AS o WHERE EXISTS (SELECT 1 FROM customers AS c WHERE c.customer_id = o.customer_id)")

	plan, diags := NewBuilder(bindings, types).Build(stmt)
	if len(diags) != 0 {
		t.Fatalf("Build() diagnostics = %#v, want none", diags)
	}

	if got, want := Explain(plan), trimExplain(`
		Project(columns=[id INTEGER NOT NULL])
		  Filter(predicate=EXISTS (SELECT 1 FROM customers AS c WHERE c.customer_id = o.customer_id))
		    Scan(table=public.orders, columns=[id INTEGER NOT NULL, customer_id INTEGER, total INTEGER])
	`); got != want {
		t.Fatalf("Explain() = %q, want %q", got, want)
	}
}

func TestExplainDoesNotEmbedPlannerDiagnostics(t *testing.T) {
	t.Parallel()

	stmt, bindings, types := analyzeSelect(t, plannerTestCatalog(t), "SELECT customer_id FROM orders ORDER BY customer_id")

	plan, diags := NewBuilder(bindings, types).Build(stmt)
	if plan != nil {
		t.Fatalf("Build() plan = %#v, want nil", plan)
	}
	if len(diags) != 1 {
		t.Fatalf("Build() diagnostics = %#v, want 1 diagnostic", diags)
	}
	if got, want := Explain(plan), "<nil>"; got != want {
		t.Fatalf("Explain(nil plan) = %q, want %q", got, want)
	}
}

func trimExplain(text string) string {
	lines := strings.Split(text, "\n")

	for len(lines) > 0 && strings.TrimSpace(lines[0]) == "" {
		lines = lines[1:]
	}

	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}

	if len(lines) == 0 {
		return ""
	}

	indent := -1
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		width := len(line) - len(strings.TrimLeft(line, " \t"))
		if indent == -1 || width < indent {
			indent = width
		}
	}

	if indent <= 0 {
		return strings.Join(lines, "\n")
	}

	for i, line := range lines {
		if strings.TrimSpace(line) == "" {
			lines[i] = ""
			continue
		}

		lines[i] = line[indent:]
	}

	return strings.Join(lines, "\n")
}
