package analyzer

import (
	"testing"

	"github.com/jamesdrando/tucotuco/internal/catalog"
	"github.com/jamesdrando/tucotuco/internal/lexer"
	"github.com/jamesdrando/tucotuco/internal/parser"
	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/types"
)

func TestResolverResolvesSelectColumnsAndOrderByAlias(t *testing.T) {
	t.Parallel()

	cat := testCatalog(t)
	script := parseScript(t, "SELECT customer_id AS cid, total FROM orders WHERE customer_id = 1 ORDER BY cid")

	bindings, diags := NewResolver(cat).ResolveScript(script)
	if len(diags) != 0 {
		t.Fatalf("ResolveScript() diagnostics = %#v, want none", diags)
	}

	stmt := script.Nodes[0].(*parser.SelectStmt)
	from := stmt.From[0].(*parser.FromSource)
	table := from.Source.(*parser.QualifiedName)

	relation, ok := bindings.Relation(table)
	if !ok {
		t.Fatalf("Relation(%T) missing binding", table)
	}
	if relation.TableID != (storage.TableID{Schema: "public", Name: "orders"}) {
		t.Fatalf("relation.TableID = %#v, want public.orders", relation.TableID)
	}

	selectBinding, ok := bindings.Column(stmt.SelectList[0].Expr)
	if !ok {
		t.Fatalf("Column(select expr) missing binding")
	}
	if selectBinding.Descriptor == nil || selectBinding.Descriptor.Name != "customer_id" {
		t.Fatalf("select binding = %#v, want descriptor for customer_id", selectBinding)
	}

	orderBinding, ok := bindings.Column(stmt.OrderBy[0].Expr)
	if !ok {
		t.Fatalf("Column(order by expr) missing binding")
	}
	if orderBinding.Name != "cid" {
		t.Fatalf("order binding name = %q, want %q", orderBinding.Name, "cid")
	}
	if orderBinding.Source != stmt.SelectList[0] {
		t.Fatalf("order binding source = %T, want first select item", orderBinding.Source)
	}
}

func TestResolverResolvesDerivedTableColumns(t *testing.T) {
	t.Parallel()

	cat := testCatalog(t)
	script := parseScript(t, "SELECT q.id FROM (SELECT id FROM orders) AS q")

	bindings, diags := NewResolver(cat).ResolveScript(script)
	if len(diags) != 0 {
		t.Fatalf("ResolveScript() diagnostics = %#v, want none", diags)
	}

	stmt := script.Nodes[0].(*parser.SelectStmt)
	column := stmt.SelectList[0].Expr.(*parser.QualifiedName)

	binding, ok := bindings.Column(column)
	if !ok {
		t.Fatalf("Column(%T) missing binding", column)
	}
	if binding.Relation == nil || binding.Relation.Name != "q" {
		t.Fatalf("binding.Relation = %#v, want alias q", binding.Relation)
	}
	if binding.Descriptor == nil || binding.Descriptor.Name != "id" {
		t.Fatalf("binding.Descriptor = %#v, want descriptor for id", binding.Descriptor)
	}
}

func TestResolverResolvesViewColumnsAsCatalogRelation(t *testing.T) {
	t.Parallel()

	cat := testCatalog(t)
	if err := cat.CreateSchema(&catalog.SchemaDescriptor{Name: "sales"}); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}
	if err := cat.CreateView(&catalog.ViewDescriptor{
		ID: storage.TableID{Schema: "sales", Name: "order_totals"},
		Columns: []catalog.ColumnDescriptor{
			{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindInteger, Nullable: true}},
			{Name: "total", Type: types.TypeDesc{Kind: types.TypeKindInteger, Nullable: true}},
		},
		Query: catalog.ExpressionDescriptor{SQL: "SELECT id, total FROM orders"},
	}); err != nil {
		t.Fatalf("CreateView() error = %v", err)
	}
	script := parseScript(t, "SELECT sales.order_totals.total FROM sales.order_totals")

	bindings, diags := NewResolver(cat).ResolveScript(script)
	if len(diags) != 0 {
		t.Fatalf("ResolveScript() diagnostics = %#v, want none", diags)
	}

	stmt := script.Nodes[0].(*parser.SelectStmt)
	column := stmt.SelectList[0].Expr.(*parser.QualifiedName)

	binding, ok := bindings.Column(column)
	if !ok {
		t.Fatalf("Column(%T) missing binding", column)
	}
	if binding.Relation == nil || binding.Relation.View == nil {
		t.Fatalf("binding.Relation = %#v, want view relation", binding.Relation)
	}
	if binding.Descriptor == nil || binding.Descriptor.Name != "total" {
		t.Fatalf("binding.Descriptor = %#v, want descriptor for total", binding.Descriptor)
	}
}

func TestResolverAllowsCorrelatedExpressionSubqueries(t *testing.T) {
	t.Parallel()

	cat := testCatalog(t)
	script := parseScript(t, "SELECT o.id FROM orders AS o WHERE EXISTS (SELECT 1 WHERE o.customer_id = 1)")

	bindings, diags := NewResolver(cat).ResolveScript(script)
	if len(diags) != 0 {
		t.Fatalf("ResolveScript() diagnostics = %#v, want none", diags)
	}

	stmt := script.Nodes[0].(*parser.SelectStmt)
	exists := stmt.Where.(*parser.ExistsExpr)
	innerQuery, ok := exists.Query.(*parser.SelectStmt)
	if !ok {
		t.Fatalf("exists.Query = %T, want *parser.SelectStmt", exists.Query)
	}
	comparison := innerQuery.Where.(*parser.BinaryExpr)
	column := comparison.Left.(*parser.QualifiedName)

	binding, ok := bindings.Column(column)
	if !ok {
		t.Fatalf("Column(correlated column) missing binding")
	}
	if binding.Relation == nil || binding.Relation.TableID != (storage.TableID{Schema: "public", Name: "orders"}) {
		t.Fatalf("binding.Relation = %#v, want outer orders relation", binding.Relation)
	}
}

func TestResolverPrefersLocalNamesInsideCorrelatedSubqueries(t *testing.T) {
	t.Parallel()

	cat := testCatalog(t)
	script := parseScript(t, "SELECT o.id FROM orders AS o WHERE EXISTS (SELECT 1 FROM customers AS o WHERE o.customer_id = 1)")

	bindings, diags := NewResolver(cat).ResolveScript(script)
	if len(diags) != 0 {
		t.Fatalf("ResolveScript() diagnostics = %#v, want none", diags)
	}

	stmt := script.Nodes[0].(*parser.SelectStmt)
	exists := stmt.Where.(*parser.ExistsExpr)
	innerQuery, ok := exists.Query.(*parser.SelectStmt)
	if !ok {
		t.Fatalf("exists.Query = %T, want *parser.SelectStmt", exists.Query)
	}
	comparison := innerQuery.Where.(*parser.BinaryExpr)
	column := comparison.Left.(*parser.QualifiedName)

	binding, ok := bindings.Column(column)
	if !ok {
		t.Fatalf("Column(local subquery column) missing binding")
	}
	if binding.Relation == nil || binding.Relation.TableID != (storage.TableID{Schema: "public", Name: "customers"}) {
		t.Fatalf("binding.Relation = %#v, want inner customers relation", binding.Relation)
	}
}

func TestResolverKeepsDerivedTablesIsolatedFromOuterScope(t *testing.T) {
	t.Parallel()

	cat := testCatalog(t)
	script := parseScript(t, "SELECT o.id FROM orders AS o WHERE EXISTS (SELECT 1 FROM (SELECT o.customer_id) AS q)")

	_, diags := NewResolver(cat).ResolveScript(script)
	if len(diags) != 1 {
		t.Fatalf("len(diagnostics) = %d, want 1", len(diags))
	}
	if diags[0].SQLState != sqlStateUndefinedTable {
		t.Fatalf("diagnostic SQLSTATE = %q, want %q", diags[0].SQLState, sqlStateUndefinedTable)
	}
	if diags[0].Message != `relation "o" does not exist` {
		t.Fatalf("diagnostic message = %q, want %q", diags[0].Message, `relation "o" does not exist`)
	}
}

func TestResolverResolvesSetOpDerivedTableColumns(t *testing.T) {
	t.Parallel()

	cat := testCatalog(t)
	script := parseScript(t, "SELECT q.id FROM (SELECT id FROM orders UNION SELECT id FROM orders) AS q")

	bindings, diags := NewResolver(cat).ResolveScript(script)
	if len(diags) != 0 {
		t.Fatalf("ResolveScript() diagnostics = %#v, want none", diags)
	}

	stmt := script.Nodes[0].(*parser.SelectStmt)
	column := stmt.SelectList[0].Expr.(*parser.QualifiedName)

	binding, ok := bindings.Column(column)
	if !ok {
		t.Fatalf("Column(%T) missing binding", column)
	}
	if binding.Relation == nil || binding.Relation.Name != "q" {
		t.Fatalf("binding.Relation = %#v, want alias q", binding.Relation)
	}
	if binding.Descriptor == nil || binding.Descriptor.Name != "id" {
		t.Fatalf("binding.Descriptor = %#v, want descriptor for id", binding.Descriptor)
	}
}

func TestResolverResolvesCreateTableForeignKeys(t *testing.T) {
	t.Parallel()

	cat := testCatalog(t)
	script := parseScript(t, "CREATE TABLE child (parent_id INTEGER REFERENCES parents(id), CONSTRAINT child_parent_fk FOREIGN KEY (parent_id) REFERENCES parents(id))")

	bindings, diags := NewResolver(cat).ResolveScript(script)
	if len(diags) != 0 {
		t.Fatalf("ResolveScript() diagnostics = %#v, want none", diags)
	}

	stmt := script.Nodes[0].(*parser.CreateTableStmt)
	tableConstraint := stmt.Constraints[0]

	localBinding, ok := bindings.Column(tableConstraint.Columns[0])
	if !ok {
		t.Fatalf("Column(local foreign key column) missing binding")
	}
	if localBinding.Source != stmt.Columns[0] {
		t.Fatalf("local binding source = %T, want first column definition", localBinding.Source)
	}

	targetRelation, ok := bindings.Relation(tableConstraint.Reference.Table)
	if !ok {
		t.Fatalf("Relation(reference table) missing binding")
	}
	if targetRelation.TableID != (storage.TableID{Schema: "public", Name: "parents"}) {
		t.Fatalf("targetRelation.TableID = %#v, want public.parents", targetRelation.TableID)
	}

	targetColumn, ok := bindings.Column(tableConstraint.Reference.Columns[0])
	if !ok {
		t.Fatalf("Column(reference column) missing binding")
	}
	if targetColumn.Descriptor == nil || targetColumn.Descriptor.Name != "id" {
		t.Fatalf("targetColumn.Descriptor = %#v, want descriptor for id", targetColumn.Descriptor)
	}
}

func TestResolverResolvesSchemaQualifiedTableAndColumn(t *testing.T) {
	t.Parallel()

	cat := testCatalog(t)
	if err := cat.CreateSchema(&catalog.SchemaDescriptor{Name: "archive"}); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}
	createTableInSchema(t, cat, "archive", "orders", "id", "archived_at")

	script := parseScript(t, "SELECT archive.orders.id FROM archive.orders")

	bindings, diags := NewResolver(cat).ResolveScript(script)
	if len(diags) != 0 {
		t.Fatalf("ResolveScript() diagnostics = %#v, want none", diags)
	}

	stmt := script.Nodes[0].(*parser.SelectStmt)
	from := stmt.From[0].(*parser.FromSource)
	table := from.Source.(*parser.QualifiedName)

	relation, ok := bindings.Relation(table)
	if !ok {
		t.Fatalf("Relation(%T) missing binding", table)
	}
	if relation.TableID != (storage.TableID{Schema: "archive", Name: "orders"}) {
		t.Fatalf("relation.TableID = %#v, want archive.orders", relation.TableID)
	}

	column := stmt.SelectList[0].Expr.(*parser.QualifiedName)
	binding, ok := bindings.Column(column)
	if !ok {
		t.Fatalf("Column(%T) missing binding", column)
	}
	if binding.Relation == nil || binding.Relation.TableID != (storage.TableID{Schema: "archive", Name: "orders"}) {
		t.Fatalf("binding.Relation = %#v, want archive.orders relation", binding.Relation)
	}
	if binding.Descriptor == nil || binding.Descriptor.Name != "id" {
		t.Fatalf("binding.Descriptor = %#v, want descriptor for id", binding.Descriptor)
	}
}

func TestResolverDisambiguatesSchemaQualifiedColumns(t *testing.T) {
	t.Parallel()

	cat := testCatalog(t)
	if err := cat.CreateSchema(&catalog.SchemaDescriptor{Name: "tenant"}); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}
	if err := cat.CreateSchema(&catalog.SchemaDescriptor{Name: "archive"}); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}
	createTableInSchema(t, cat, "tenant", "orders", "id", "customer_id")
	createTableInSchema(t, cat, "archive", "orders", "id", "archived_at")

	script := parseScript(t, "SELECT archive.orders.id FROM tenant.orders INNER JOIN archive.orders ON tenant.orders.id = archive.orders.id")

	bindings, diags := NewResolver(cat).ResolveScript(script)
	if len(diags) != 0 {
		t.Fatalf("ResolveScript() diagnostics = %#v, want none", diags)
	}

	stmt := script.Nodes[0].(*parser.SelectStmt)
	column := stmt.SelectList[0].Expr.(*parser.QualifiedName)

	binding, ok := bindings.Column(column)
	if !ok {
		t.Fatalf("Column(%T) missing binding", column)
	}
	if binding.Relation == nil || binding.Relation.TableID != (storage.TableID{Schema: "archive", Name: "orders"}) {
		t.Fatalf("binding.Relation = %#v, want archive.orders relation", binding.Relation)
	}
}

func TestResolverResolvesSchemaQualifiedStar(t *testing.T) {
	t.Parallel()

	cat := testCatalog(t)
	if err := cat.CreateSchema(&catalog.SchemaDescriptor{Name: "archive"}); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}
	createTableInSchema(t, cat, "archive", "orders", "id", "archived_at")

	script := parseScript(t, "SELECT archive.orders.* FROM archive.orders")

	bindings, diags := NewResolver(cat).ResolveScript(script)
	if len(diags) != 0 {
		t.Fatalf("ResolveScript() diagnostics = %#v, want none", diags)
	}

	stmt := script.Nodes[0].(*parser.SelectStmt)
	star := stmt.SelectList[0].Expr.(*parser.Star)
	columns, ok := bindings.Star(star)
	if !ok {
		t.Fatalf("Star(%T) missing binding", star)
	}
	if len(columns) != 2 {
		t.Fatalf("len(columns) = %d, want 2", len(columns))
	}
	for _, column := range columns {
		if column.Relation == nil || column.Relation.TableID != (storage.TableID{Schema: "archive", Name: "orders"}) {
			t.Fatalf("column.Relation = %#v, want archive.orders relation", column.Relation)
		}
	}
}

func TestResolverReportsUndefinedColumn(t *testing.T) {
	t.Parallel()

	cat := testCatalog(t)
	script := parseScript(t, "SELECT missing FROM orders")

	_, diags := NewResolver(cat).ResolveScript(script)
	if len(diags) != 1 {
		t.Fatalf("len(diagnostics) = %d, want 1", len(diags))
	}
	if diags[0].SQLState != sqlStateUndefinedColumn {
		t.Fatalf("diagnostic SQLSTATE = %q, want %q", diags[0].SQLState, sqlStateUndefinedColumn)
	}
	if diags[0].Message != `column "missing" does not exist` {
		t.Fatalf("diagnostic message = %q, want %q", diags[0].Message, `column "missing" does not exist`)
	}
}

func TestResolverReportsAmbiguousColumn(t *testing.T) {
	t.Parallel()

	cat := testCatalog(t)
	script := parseScript(t, "SELECT id FROM orders AS o INNER JOIN customers AS c ON o.customer_id = c.id")

	_, diags := NewResolver(cat).ResolveScript(script)
	if len(diags) != 1 {
		t.Fatalf("len(diagnostics) = %d, want 1", len(diags))
	}
	if diags[0].SQLState != sqlStateAmbiguousColumn {
		t.Fatalf("diagnostic SQLSTATE = %q, want %q", diags[0].SQLState, sqlStateAmbiguousColumn)
	}
	if diags[0].Message != `column reference "id" is ambiguous` {
		t.Fatalf("diagnostic message = %q, want %q", diags[0].Message, `column reference "id" is ambiguous`)
	}
}

func TestResolverReportsMissingTable(t *testing.T) {
	t.Parallel()

	cat := testCatalog(t)
	script := parseScript(t, "DELETE FROM missing")

	_, diags := NewResolver(cat).ResolveScript(script)
	if len(diags) != 1 {
		t.Fatalf("len(diagnostics) = %d, want 1", len(diags))
	}
	if diags[0].SQLState != sqlStateUndefinedTable {
		t.Fatalf("diagnostic SQLSTATE = %q, want %q", diags[0].SQLState, sqlStateUndefinedTable)
	}
	if diags[0].Message != `table "public.missing" does not exist` {
		t.Fatalf("diagnostic message = %q, want %q", diags[0].Message, `table "public.missing" does not exist`)
	}
}

func testCatalog(t *testing.T) catalog.Catalog {
	t.Helper()

	cat := catalog.NewMemory()
	if err := cat.CreateSchema(&catalog.SchemaDescriptor{Name: "public"}); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	createTable(t, cat, "orders", "id", "customer_id", "total")
	createTable(t, cat, "customers", "id", "customer_id")
	createTable(t, cat, "parents", "id")

	return cat
}

func createTable(t *testing.T, cat catalog.Catalog, tableName string, columnNames ...string) {
	t.Helper()

	createTableInSchema(t, cat, "public", tableName, columnNames...)
}

func createTableInSchema(t *testing.T, cat catalog.Catalog, schemaName string, tableName string, columnNames ...string) {
	t.Helper()

	columns := make([]catalog.ColumnDescriptor, 0, len(columnNames))
	for _, name := range columnNames {
		columns = append(columns, catalog.ColumnDescriptor{
			Name: name,
			Type: types.TypeDesc{
				Kind:     types.TypeKindInteger,
				Nullable: true,
			},
		})
	}

	if err := cat.CreateTable(&catalog.TableDescriptor{
		ID: storage.TableID{
			Schema: schemaName,
			Name:   tableName,
		},
		Columns: columns,
	}); err != nil {
		t.Fatalf("CreateTable(%q.%q) error = %v", schemaName, tableName, err)
	}
}

func parseScript(t *testing.T, sql string) *parser.Script {
	t.Helper()

	p := parser.New(lexer.NewString(sql).All())
	script := p.ParseScript()
	if errs := p.Errors(); len(errs) != 0 {
		t.Fatalf("ParseScript(%q) errors = %#v", sql, errs)
	}

	return script
}
