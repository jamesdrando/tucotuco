package planner

import (
	"reflect"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/analyzer"
	"github.com/jamesdrando/tucotuco/internal/catalog"
	"github.com/jamesdrando/tucotuco/internal/diag"
	"github.com/jamesdrando/tucotuco/internal/lexer"
	"github.com/jamesdrando/tucotuco/internal/parser"
	"github.com/jamesdrando/tucotuco/internal/storage"
	sqltypes "github.com/jamesdrando/tucotuco/internal/types"
)

func TestBuilderBuildsScanFilterProject(t *testing.T) {
	t.Parallel()

	stmt, bindings, types := analyzeSelect(t, plannerTestCatalog(t), "SELECT customer_id AS cid FROM orders WHERE total = 1")

	plan, diags := NewBuilder(bindings, types).Build(stmt)
	if len(diags) != 0 {
		t.Fatalf("Build() diagnostics = %#v, want none", diags)
	}

	project, ok := plan.(*Project)
	if !ok {
		t.Fatalf("plan = %T, want *Project", plan)
	}
	if got, want := project.String(), "Project(columns=[cid INTEGER])"; got != want {
		t.Fatalf("project.String() = %q, want %q", got, want)
	}
	if got, want := stripPlannerColumns(project.Columns()), []Column{{Name: "cid", Type: mustTypeDesc(t, "INTEGER")}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("project.Columns() = %#v, want %#v", got, want)
	}

	filter, ok := project.Input.(*Filter)
	if !ok {
		t.Fatalf("project.Input = %T, want *Filter", project.Input)
	}
	if got, want := filter.String(), "Filter(predicate=total = 1)"; got != want {
		t.Fatalf("filter.String() = %q, want %q", got, want)
	}

	scan, ok := filter.Input.(*Scan)
	if !ok {
		t.Fatalf("filter.Input = %T, want *Scan", filter.Input)
	}
	if scan.Table != (storage.TableID{Schema: "public", Name: "orders"}) {
		t.Fatalf("scan.Table = %#v, want public.orders", scan.Table)
	}
	if got, want := stripPlannerColumns(scan.Columns()), []Column{
		{Name: "id", Type: mustTypeDesc(t, "INTEGER NOT NULL")},
		{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER")},
		{Name: "total", Type: mustTypeDesc(t, "INTEGER")},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("scan.Columns() = %#v, want %#v", got, want)
	}
}

func TestBuilderExpandsStarUsingAnalyzerMetadata(t *testing.T) {
	t.Parallel()

	stmt, bindings, types := analyzeSelect(t, plannerTestCatalog(t), "SELECT * FROM orders")

	plan, diags := NewBuilder(bindings, types).Build(stmt)
	if len(diags) != 0 {
		t.Fatalf("Build() diagnostics = %#v, want none", diags)
	}

	project, ok := plan.(*Project)
	if !ok {
		t.Fatalf("plan = %T, want *Project", plan)
	}

	wantColumns := []Column{
		{Name: "id", Type: mustTypeDesc(t, "INTEGER NOT NULL")},
		{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER")},
		{Name: "total", Type: mustTypeDesc(t, "INTEGER")},
	}
	if got := stripPlannerColumns(project.Columns()); !reflect.DeepEqual(got, wantColumns) {
		t.Fatalf("project.Columns() = %#v, want %#v", got, wantColumns)
	}
	if len(project.Projections) != len(wantColumns) {
		t.Fatalf("len(project.Projections) = %d, want %d", len(project.Projections), len(wantColumns))
	}

	for index, projection := range project.Projections {
		if got, want := formatExpr(projection.Expr), "orders."+wantColumns[index].Name; got != want {
			t.Fatalf("projection[%d].Expr = %q, want %q", index, got, want)
		}
		if got := stripPlannerColumn(projection.Output); got != wantColumns[index] {
			t.Fatalf("projection[%d].Output = %#v, want %#v", index, projection.Output, wantColumns[index])
		}
	}
}

func TestBuilderBuildsDerivedTableInput(t *testing.T) {
	t.Parallel()

	stmt, bindings, types := analyzeSelect(t, plannerTestCatalog(t), "SELECT q.id FROM (SELECT id FROM orders) AS q")

	plan, diags := NewBuilder(bindings, types).Build(stmt)
	if len(diags) != 0 {
		t.Fatalf("Build() diagnostics = %#v, want none", diags)
	}

	project, ok := plan.(*Project)
	if !ok {
		t.Fatalf("plan = %T, want *Project", plan)
	}
	if got, want := project.String(), "Project(columns=[id INTEGER NOT NULL])"; got != want {
		t.Fatalf("project.String() = %q, want %q", got, want)
	}

	innerProject, ok := project.Input.(*Project)
	if !ok {
		t.Fatalf("project.Input = %T, want *Project", project.Input)
	}
	if got, want := innerProject.String(), "Project(columns=[id INTEGER NOT NULL])"; got != want {
		t.Fatalf("innerProject.String() = %q, want %q", got, want)
	}

	scan, ok := innerProject.Input.(*Scan)
	if !ok {
		t.Fatalf("innerProject.Input = %T, want *Scan", innerProject.Input)
	}
	if scan.Table != (storage.TableID{Schema: "public", Name: "orders"}) {
		t.Fatalf("scan.Table = %#v, want public.orders", scan.Table)
	}
	if got := innerProject.Columns(); len(got) != 1 {
		t.Fatalf("len(innerProject.Columns()) = %d, want 1", len(got))
	} else {
		assertPlannerColumnBinding(t, got[0], "q", "id")
	}
}

func TestBuilderRejectsUnsupportedOrderBy(t *testing.T) {
	t.Parallel()

	stmt, bindings, types := analyzeSelect(t, plannerTestCatalog(t), "SELECT customer_id FROM orders ORDER BY customer_id")

	plan, diags := NewBuilder(bindings, types).Build(stmt)
	if plan != nil {
		t.Fatalf("plan = %#v, want nil", plan)
	}

	want := []diag.Diagnostic{diag.NewError(sqlStateFeatureNotSupported, "ORDER BY queries are not supported in Phase 1 planner", diagPosition(stmt))}
	if !reflect.DeepEqual(diags, want) {
		t.Fatalf("Build() diagnostics = %#v, want %#v", diags, want)
	}
}

func TestBuilderBuildsInnerJoinPlan(t *testing.T) {
	t.Parallel()

	stmt, bindings, types := analyzeSelect(t, plannerTestCatalog(t), "SELECT o.id, c.customer_id FROM orders AS o INNER JOIN customers AS c ON o.customer_id = c.id")

	plan, diags := NewBuilder(bindings, types).Build(stmt)
	if len(diags) != 0 {
		t.Fatalf("Build() diagnostics = %#v, want none", diags)
	}

	project, ok := plan.(*Project)
	if !ok {
		t.Fatalf("plan = %T, want *Project", plan)
	}
	join, ok := project.Input.(*Join)
	if !ok {
		t.Fatalf("project.Input = %T, want *Join", project.Input)
	}
	if got, want := join.String(), "Join(type=INNER, condition=o.customer_id = c.id)"; got != want {
		t.Fatalf("join.String() = %q, want %q", got, want)
	}
	wantJoinColumns := []Column{
		{Name: "id", Type: mustTypeDesc(t, "INTEGER NOT NULL"), RelationName: "o"},
		{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER"), RelationName: "o"},
		{Name: "total", Type: mustTypeDesc(t, "INTEGER"), RelationName: "o"},
		{Name: "id", Type: mustTypeDesc(t, "INTEGER NOT NULL"), RelationName: "c"},
		{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER"), RelationName: "c"},
	}
	if got := stripPlannerColumns(join.Columns()); !reflect.DeepEqual(got, []Column{
		{Name: "id", Type: mustTypeDesc(t, "INTEGER NOT NULL")},
		{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER")},
		{Name: "total", Type: mustTypeDesc(t, "INTEGER")},
		{Name: "id", Type: mustTypeDesc(t, "INTEGER NOT NULL")},
		{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER")},
	}) {
		t.Fatalf("join.Columns() = %#v, want joined column shape", got)
	}
	for index, want := range wantJoinColumns {
		assertPlannerColumnBinding(t, join.Columns()[index], want.RelationName, want.Name)
	}
	if got, want := stripPlannerColumns(project.Columns()), []Column{
		{Name: "id", Type: mustTypeDesc(t, "INTEGER NOT NULL")},
		{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER")},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("project.Columns() = %#v, want %#v", got, want)
	}
}

func TestBuilderBuildsCrossJoinFromCommaSeparatedSources(t *testing.T) {
	t.Parallel()

	stmt, bindings, types := analyzeSelect(t, plannerTestCatalog(t), "SELECT o.id FROM orders AS o, customers AS c")

	plan, diags := NewBuilder(bindings, types).Build(stmt)
	if len(diags) != 0 {
		t.Fatalf("Build() diagnostics = %#v, want none", diags)
	}

	project, ok := plan.(*Project)
	if !ok {
		t.Fatalf("plan = %T, want *Project", plan)
	}
	join, ok := project.Input.(*Join)
	if !ok {
		t.Fatalf("project.Input = %T, want *Join", project.Input)
	}
	if got, want := join.Type, "CROSS"; got != want {
		t.Fatalf("join.Type = %q, want %q", got, want)
	}
}

func TestBuilderPropagatesOuterJoinNullabilityToProjectedColumns(t *testing.T) {
	t.Parallel()

	stmt, bindings, types := analyzeSelect(t, plannerTestCatalog(t), "SELECT c.id FROM orders AS o LEFT JOIN customers AS c ON o.customer_id = c.id")

	plan, diags := NewBuilder(bindings, types).Build(stmt)
	if len(diags) != 0 {
		t.Fatalf("Build() diagnostics = %#v, want none", diags)
	}

	project, ok := plan.(*Project)
	if !ok {
		t.Fatalf("plan = %T, want *Project", plan)
	}
	if got, want := stripPlannerColumns(project.Columns()), []Column{{Name: "id", Type: mustTypeDesc(t, "INTEGER")}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("project.Columns() = %#v, want %#v", got, want)
	}
}

func TestBuilderMarksRightAndFullJoinNullSupplyingSidesNullable(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		sql  string
		want []Column
	}{
		{
			name: "right join",
			sql:  "SELECT * FROM orders AS o RIGHT JOIN customers AS c ON o.customer_id = c.id",
			want: []Column{
				{Name: "id", Type: mustTypeDesc(t, "INTEGER"), RelationName: "o"},
				{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER"), RelationName: "o"},
				{Name: "total", Type: mustTypeDesc(t, "INTEGER"), RelationName: "o"},
				{Name: "id", Type: mustTypeDesc(t, "INTEGER NOT NULL"), RelationName: "c"},
				{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER"), RelationName: "c"},
			},
		},
		{
			name: "full join",
			sql:  "SELECT * FROM orders AS o FULL JOIN customers AS c ON o.customer_id = c.id",
			want: []Column{
				{Name: "id", Type: mustTypeDesc(t, "INTEGER"), RelationName: "o"},
				{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER"), RelationName: "o"},
				{Name: "total", Type: mustTypeDesc(t, "INTEGER"), RelationName: "o"},
				{Name: "id", Type: mustTypeDesc(t, "INTEGER"), RelationName: "c"},
				{Name: "customer_id", Type: mustTypeDesc(t, "INTEGER"), RelationName: "c"},
			},
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			stmt, bindings, types := analyzeSelect(t, plannerTestCatalog(t), testCase.sql)

			plan, diags := NewBuilder(bindings, types).Build(stmt)
			if len(diags) != 0 {
				t.Fatalf("Build() diagnostics = %#v, want none", diags)
			}

			project, ok := plan.(*Project)
			if !ok {
				t.Fatalf("plan = %T, want *Project", plan)
			}
			join, ok := project.Input.(*Join)
			if !ok {
				t.Fatalf("project.Input = %T, want *Join", project.Input)
			}

			if got, want := stripPlannerColumns(join.Columns()), []Column{
				{Name: "id", Type: testCase.want[0].Type},
				{Name: "customer_id", Type: testCase.want[1].Type},
				{Name: "total", Type: testCase.want[2].Type},
				{Name: "id", Type: testCase.want[3].Type},
				{Name: "customer_id", Type: testCase.want[4].Type},
			}; !reflect.DeepEqual(got, want) {
				t.Fatalf("join.Columns() = %#v, want %#v", got, want)
			}
			for index, want := range testCase.want {
				assertPlannerColumnBinding(t, join.Columns()[index], want.RelationName, want.Name)
			}
		})
	}
}

func TestBuilderScopesAliasedJoinedSourceMetadata(t *testing.T) {
	t.Parallel()

	stmt, bindings, types := analyzeSelect(t, plannerTestCatalog(t), "SELECT j.total FROM (orders AS o INNER JOIN customers AS c ON o.customer_id = c.id) AS j")

	plan, diags := NewBuilder(bindings, types).Build(stmt)
	if len(diags) != 0 {
		t.Fatalf("Build() diagnostics = %#v, want none", diags)
	}

	project, ok := plan.(*Project)
	if !ok {
		t.Fatalf("plan = %T, want *Project", plan)
	}
	join, ok := project.Input.(*Join)
	if !ok {
		t.Fatalf("project.Input = %T, want *Join", project.Input)
	}
	for _, column := range join.Columns() {
		assertPlannerColumnBinding(t, column, "j", column.Name)
	}
	assertPlannerColumnBinding(t, project.Columns()[0], "j", "total")
}

func TestBuilderRejectsUsingJoinPlanning(t *testing.T) {
	t.Parallel()

	stmt, bindings, types := analyzeSelect(t, plannerTestCatalog(t), "SELECT o.id FROM orders AS o JOIN customers AS c USING (customer_id)")

	plan, diags := NewBuilder(bindings, types).Build(stmt)
	if plan != nil {
		t.Fatalf("plan = %#v, want nil", plan)
	}

	want := []diag.Diagnostic{diag.NewError(sqlStateFeatureNotSupported, "JOIN ... USING is not supported in Phase 2 planner", diagPosition(stmt.From[0]))}
	if !reflect.DeepEqual(diags, want) {
		t.Fatalf("Build() diagnostics = %#v, want %#v", diags, want)
	}
}

func TestBuilderRejectsNaturalJoinPlanning(t *testing.T) {
	t.Parallel()

	stmt, bindings, types := analyzeSelect(t, plannerTestCatalog(t), "SELECT o.id FROM orders AS o NATURAL JOIN customers AS c")

	plan, diags := NewBuilder(bindings, types).Build(stmt)
	if plan != nil {
		t.Fatalf("plan = %#v, want nil", plan)
	}

	want := []diag.Diagnostic{diag.NewError(sqlStateFeatureNotSupported, "NATURAL JOIN planning is not supported in Phase 2 planner", diagPosition(stmt.From[0]))}
	if !reflect.DeepEqual(diags, want) {
		t.Fatalf("Build() diagnostics = %#v, want %#v", diags, want)
	}
}

func analyzeSelect(t *testing.T, cat catalog.Catalog, sql string) (*parser.SelectStmt, *analyzer.Bindings, *analyzer.Types) {
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

	bindings, resolveDiags := analyzer.NewResolver(cat).ResolveScript(script)
	if len(resolveDiags) != 0 {
		t.Fatalf("ResolveScript() diagnostics = %#v, want none", resolveDiags)
	}

	types, typeDiags := analyzer.NewTypeChecker(bindings).CheckScript(script)
	if len(typeDiags) != 0 {
		t.Fatalf("CheckScript() diagnostics = %#v, want none", typeDiags)
	}

	return stmt, bindings, types
}

func plannerTestCatalog(t *testing.T) catalog.Catalog {
	t.Helper()

	cat := catalog.NewMemory()
	if err := cat.CreateSchema(&catalog.SchemaDescriptor{Name: "public"}); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}
	if err := cat.CreateTable(&catalog.TableDescriptor{
		ID: storage.TableID{Schema: "public", Name: "orders"},
		Columns: []catalog.ColumnDescriptor{
			{Name: "id", Type: mustPlannerTypeDesc(t, "INTEGER NOT NULL")},
			{Name: "customer_id", Type: mustPlannerTypeDesc(t, "INTEGER")},
			{Name: "total", Type: mustPlannerTypeDesc(t, "INTEGER")},
		},
	}); err != nil {
		t.Fatalf("CreateTable(orders) error = %v", err)
	}
	if err := cat.CreateTable(&catalog.TableDescriptor{
		ID: storage.TableID{Schema: "public", Name: "customers"},
		Columns: []catalog.ColumnDescriptor{
			{Name: "id", Type: mustPlannerTypeDesc(t, "INTEGER NOT NULL")},
			{Name: "customer_id", Type: mustPlannerTypeDesc(t, "INTEGER")},
		},
	}); err != nil {
		t.Fatalf("CreateTable(customers) error = %v", err)
	}

	return cat
}

func mustPlannerTypeDesc(t *testing.T, text string) sqltypes.TypeDesc {
	t.Helper()

	desc, err := sqltypes.ParseTypeDesc(text)
	if err != nil {
		t.Fatalf("ParseTypeDesc(%q) error = %v", text, err)
	}

	return desc
}

func stripPlannerColumns(columns []Column) []Column {
	out := make([]Column, len(columns))
	for index, column := range columns {
		out[index] = stripPlannerColumn(column)
	}

	return out
}

func stripPlannerColumn(column Column) Column {
	return Column{
		Name: column.Name,
		Type: column.Type,
	}
}

func assertPlannerColumnBinding(t *testing.T, column Column, relation string, name string) {
	t.Helper()

	if column.RelationName != relation {
		t.Fatalf("column.RelationName = %q, want %q", column.RelationName, relation)
	}
	if column.Binding == nil {
		t.Fatalf("column.Binding = nil, want binding for %s.%s", relation, name)
	}
	if column.Binding.Name != name {
		t.Fatalf("column.Binding.Name = %q, want %q", column.Binding.Name, name)
	}
	if got := safeBindingRelation(column.Binding); got != relation {
		t.Fatalf("safeBindingRelation(column.Binding) = %q, want %q", got, relation)
	}
}
