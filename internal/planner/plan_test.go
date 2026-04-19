package planner

import (
	"reflect"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/lexer"
	"github.com/jamesdrando/tucotuco/internal/parser"
	"github.com/jamesdrando/tucotuco/internal/storage"
	sqltypes "github.com/jamesdrando/tucotuco/internal/types"
)

func TestScanReportsShapeAndFormatting(t *testing.T) {
	t.Parallel()

	columns := []Column{
		{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER")},
		{Name: "total", Type: mustTypeDesc(t, "INTEGER")},
	}

	scan := NewScan(storage.TableID{Schema: "public", Name: "orders"}, columns...)

	if scan.Kind() != KindScan {
		t.Fatalf("Kind() = %q, want %q", scan.Kind(), KindScan)
	}
	if got := scan.Children(); got != nil {
		t.Fatalf("Children() = %#v, want nil", got)
	}
	if got := scan.Columns(); !reflect.DeepEqual(got, columns) {
		t.Fatalf("Columns() = %#v, want %#v", got, columns)
	}

	gotColumns := scan.Columns()
	gotColumns[0].Name = "mutated"
	if scan.OutputColumns[0].Name != "customer_id" {
		t.Fatalf("Columns() returned shared backing slice")
	}

	if got, want := scan.String(), "Scan(table=public.orders, columns=[customer_id INTEGER, total INTEGER])"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestFilterInheritsInputShapeAndFormatting(t *testing.T) {
	t.Parallel()

	scan := NewScan(
		storage.TableID{Schema: "public", Name: "orders"},
		Column{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER")},
		Column{Name: "total", Type: mustTypeDesc(t, "INTEGER")},
	)
	stmt := mustSelect(t, "SELECT * FROM orders WHERE customer_id = 1")
	filter := NewFilter(scan, stmt.Where)

	if filter.Kind() != KindFilter {
		t.Fatalf("Kind() = %q, want %q", filter.Kind(), KindFilter)
	}
	if got, want := filter.Children(), []Plan{scan}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Children() = %#v, want %#v", got, want)
	}
	if got, want := filter.Columns(), scan.Columns(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Columns() = %#v, want %#v", got, want)
	}

	gotColumns := filter.Columns()
	gotColumns[0].Name = "mutated"
	if scan.OutputColumns[0].Name != "customer_id" {
		t.Fatalf("Columns() returned shared backing slice")
	}

	gotChildren := filter.Children()
	gotChildren[0] = nil
	if children := filter.Children(); len(children) != 1 || children[0] != scan {
		t.Fatalf("Children() returned shared backing slice")
	}

	if got, want := filter.String(), "Filter(predicate=customer_id = 1)"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestProjectBuildsOutputColumnsAndFormatting(t *testing.T) {
	t.Parallel()

	scan := NewScan(
		storage.TableID{Schema: "public", Name: "orders"},
		Column{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER")},
		Column{Name: "total", Type: mustTypeDesc(t, "INTEGER")},
	)
	stmt := mustSelect(t, "SELECT customer_id AS cid, total FROM orders")
	project := NewProject(
		scan,
		Projection{
			Expr:   stmt.SelectList[0].Expr,
			Output: Column{Name: "cid", Type: mustTypeDesc(t, "INTEGER")},
		},
		Projection{
			Expr:   stmt.SelectList[1].Expr,
			Output: Column{Name: "total", Type: mustTypeDesc(t, "INTEGER")},
		},
	)

	wantColumns := []Column{
		{Name: "cid", Type: mustTypeDesc(t, "INTEGER")},
		{Name: "total", Type: mustTypeDesc(t, "INTEGER")},
	}
	if project.Kind() != KindProject {
		t.Fatalf("Kind() = %q, want %q", project.Kind(), KindProject)
	}
	if got, want := project.Children(), []Plan{scan}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Children() = %#v, want %#v", got, want)
	}
	if got := project.Columns(); !reflect.DeepEqual(got, wantColumns) {
		t.Fatalf("Columns() = %#v, want %#v", got, wantColumns)
	}

	gotColumns := project.Columns()
	gotColumns[0].Name = "mutated"
	if project.Projections[0].Output.Name != "cid" {
		t.Fatalf("Columns() returned shared backing slice")
	}

	if got, want := project.String(), "Project(columns=[cid INTEGER, total INTEGER])"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestLimitInheritsInputShapeAndFormatting(t *testing.T) {
	t.Parallel()

	scan := NewScan(
		storage.TableID{Schema: "public", Name: "orders"},
		Column{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER")},
	)
	limit := NewLimit(scan, &parser.IntegerLiteral{Text: "10"})

	if limit.Kind() != KindLimit {
		t.Fatalf("Kind() = %q, want %q", limit.Kind(), KindLimit)
	}
	if got, want := limit.Children(), []Plan{scan}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Children() = %#v, want %#v", got, want)
	}
	if got, want := limit.Columns(), scan.Columns(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Columns() = %#v, want %#v", got, want)
	}

	gotColumns := limit.Columns()
	gotColumns[0].Name = "mutated"
	if scan.OutputColumns[0].Name != "customer_id" {
		t.Fatalf("Columns() returned shared backing slice")
	}

	if got, want := limit.String(), "Limit(count=10)"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func mustSelect(t *testing.T, sql string) *parser.SelectStmt {
	t.Helper()

	p := parser.New(lexer.NewString(sql).All())
	script := p.ParseScript()
	if errs := p.Errors(); len(errs) != 0 {
		t.Fatalf("ParseScript(%q) errors = %#v", sql, errs)
	}
	if len(script.Nodes) != 1 {
		t.Fatalf("len(script.Nodes) = %d, want 1", len(script.Nodes))
	}

	stmt, ok := script.Nodes[0].(*parser.SelectStmt)
	if !ok {
		t.Fatalf("script.Nodes[0] = %T, want *parser.SelectStmt", script.Nodes[0])
	}

	return stmt
}

func mustTypeDesc(t *testing.T, text string) sqltypes.TypeDesc {
	t.Helper()

	desc, err := sqltypes.ParseTypeDesc(text)
	if err != nil {
		t.Fatalf("ParseTypeDesc(%q) error = %v", text, err)
	}

	return desc
}
