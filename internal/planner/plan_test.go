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

func TestAggregateReportsChildrenShapeAndFormatting(t *testing.T) {
	t.Parallel()

	scan := NewScan(
		storage.TableID{Schema: "public", Name: "orders"},
		Column{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER")},
		Column{Name: "total", Type: mustTypeDesc(t, "INTEGER")},
	)
	stmt := mustSelect(t, "SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id")
	aggregate := NewAggregate(
		scan,
		stmt.GroupBy,
		Column{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER")},
		Column{Name: "n", Type: mustTypeDesc(t, "BIGINT")},
	)

	if aggregate.Kind() != KindAggregate {
		t.Fatalf("Kind() = %q, want %q", aggregate.Kind(), KindAggregate)
	}
	if got, want := aggregate.Children(), []Plan{scan}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Children() = %#v, want %#v", got, want)
	}
	if got, want := aggregate.Columns(), []Column{
		{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER")},
		{Name: "n", Type: mustTypeDesc(t, "BIGINT")},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Columns() = %#v, want %#v", got, want)
	}
	if got, want := aggregate.String(), "Aggregate(groups=[customer_id])"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}

	gotColumns := aggregate.Columns()
	gotColumns[0].Name = "mutated"
	if aggregate.OutputColumns[0].Name != "customer_id" {
		t.Fatalf("Columns() returned shared backing slice")
	}

	gotChildren := aggregate.Children()
	gotChildren[0] = nil
	if children := aggregate.Children(); len(children) != 1 || children[0] != scan {
		t.Fatalf("Children() returned shared backing slice")
	}
}

func TestJoinReportsChildrenShapeAndFormatting(t *testing.T) {
	t.Parallel()

	left := NewScan(
		storage.TableID{Schema: "public", Name: "orders"},
		Column{Name: "id", Type: mustTypeDesc(t, "INTEGER NOT NULL")},
	)
	right := NewScan(
		storage.TableID{Schema: "public", Name: "customers"},
		Column{Name: "id", Type: mustTypeDesc(t, "INTEGER")},
	)
	stmt := mustSelect(t, "SELECT o.id FROM orders AS o INNER JOIN customers AS c ON o.id = c.id")
	joinExpr, ok := stmt.From[0].(*parser.JoinExpr)
	if !ok {
		t.Fatalf("stmt.From[0] = %T, want *parser.JoinExpr", stmt.From[0])
	}
	join := NewJoin(
		left,
		right,
		"LEFT",
		joinExpr.Condition,
		Column{Name: "id", Type: mustTypeDesc(t, "INTEGER NOT NULL")},
		Column{Name: "id", Type: mustTypeDesc(t, "INTEGER")},
	)

	if join.Kind() != KindJoin {
		t.Fatalf("Kind() = %q, want %q", join.Kind(), KindJoin)
	}
	if got, want := join.Children(), []Plan{left, right}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Children() = %#v, want %#v", got, want)
	}
	if got, want := join.Columns(), []Column{
		{Name: "id", Type: mustTypeDesc(t, "INTEGER NOT NULL")},
		{Name: "id", Type: mustTypeDesc(t, "INTEGER")},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Columns() = %#v, want %#v", got, want)
	}
	if got, want := join.String(), "Join(type=LEFT, condition=o.id = c.id)"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}

	gotColumns := join.Columns()
	gotColumns[0].Name = "mutated"
	if join.OutputColumns[0].Name != "id" {
		t.Fatalf("Columns() returned shared backing slice")
	}

	gotChildren := join.Children()
	gotChildren[0] = nil
	if children := join.Children(); len(children) != 2 || children[0] != left || children[1] != right {
		t.Fatalf("Children() returned shared backing slice")
	}
}

func TestSetOpReportsChildrenShapeAndFormatting(t *testing.T) {
	t.Parallel()

	left := NewProject(
		NewScan(storage.TableID{Schema: "public", Name: "orders"}, Column{Name: "id", Type: mustTypeDesc(t, "INTEGER NOT NULL")}),
		Projection{Expr: &parser.Identifier{Name: "id"}, Output: Column{Name: "id", Type: mustTypeDesc(t, "INTEGER NOT NULL")}},
	)
	right := NewProject(
		NewScan(storage.TableID{Schema: "public", Name: "customers"}, Column{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER")}),
		Projection{Expr: &parser.Identifier{Name: "customer_id"}, Output: Column{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER")}},
	)
	setOp := NewSetOp(left, right, "UNION", "ALL", Column{Name: "id", Type: mustTypeDesc(t, "INTEGER NOT NULL")})

	if setOp.Kind() != KindSetOp {
		t.Fatalf("Kind() = %q, want %q", setOp.Kind(), KindSetOp)
	}
	if got, want := setOp.Children(), []Plan{left, right}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Children() = %#v, want %#v", got, want)
	}
	if got, want := setOp.Columns(), []Column{{Name: "id", Type: mustTypeDesc(t, "INTEGER NOT NULL")}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Columns() = %#v, want %#v", got, want)
	}
	if got, want := setOp.String(), "SetOp(operator=UNION, quantifier=ALL)"; got != want {
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
