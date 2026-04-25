package analyzer

import (
	"testing"

	"github.com/jamesdrando/tucotuco/internal/catalog"
	"github.com/jamesdrando/tucotuco/internal/diag"
	"github.com/jamesdrando/tucotuco/internal/parser"
	"github.com/jamesdrando/tucotuco/internal/storage"
	sqltypes "github.com/jamesdrando/tucotuco/internal/types"
)

func TestTypeCheckerAssignsSelectOutputsAndOrderByAlias(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT customer_id AS cid, COALESCE(MAX(total), 0) AS total_or_zero, COUNT(DISTINCT total) FROM orders WHERE customer_id = 1 GROUP BY customer_id ORDER BY cid")

	typed, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 0 {
		t.Fatalf("CheckScript() diagnostics = %#v, want none", diags)
	}

	stmt := script.Nodes[0].(*parser.SelectStmt)
	if got, ok := typed.Expr(stmt.OrderBy[0].Expr); !ok || got != mustAnalyzerTypeDesc(t, "INTEGER") {
		t.Fatalf("Expr(order by alias) = (%#v, %t), want INTEGER", got, ok)
	}
	if got, ok := typed.Expr(stmt.Where); !ok || got != mustAnalyzerTypeDesc(t, "BOOLEAN") {
		t.Fatalf("Expr(where) = (%#v, %t), want BOOLEAN", got, ok)
	}

	wantOutputs := []sqltypes.TypeDesc{
		mustAnalyzerTypeDesc(t, "INTEGER"),
		mustAnalyzerTypeDesc(t, "INTEGER NOT NULL"),
		mustAnalyzerTypeDesc(t, "BIGINT NOT NULL"),
	}
	if got, ok := typed.Select(stmt); !ok || !equalTypeSlices(got, wantOutputs) {
		t.Fatalf("Select(stmt) = (%#v, %t), want %#v", got, ok, wantOutputs)
	}
}

func TestTypeCheckerAssignsDerivedTableTypes(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT q.id FROM (SELECT id FROM orders) AS q")

	typed, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 0 {
		t.Fatalf("CheckScript() diagnostics = %#v, want none", diags)
	}

	stmt := script.Nodes[0].(*parser.SelectStmt)
	if got, ok := typed.Expr(stmt.SelectList[0].Expr); !ok || got != mustAnalyzerTypeDesc(t, "INTEGER NOT NULL") {
		t.Fatalf("Expr(derived column) = (%#v, %t), want INTEGER NOT NULL", got, ok)
	}
}

func TestTypeCheckerAssignsSchemaQualifiedColumnTypes(t *testing.T) {
	t.Parallel()

	cat := emptyAnalyzerCatalog(t)
	if err := cat.CreateSchema(&catalog.SchemaDescriptor{Name: "archive"}); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}
	if err := cat.CreateTable(&catalog.TableDescriptor{
		ID: storage.TableID{Schema: "archive", Name: "orders"},
		Columns: []catalog.ColumnDescriptor{
			{Name: "id", Type: mustAnalyzerTypeDesc(t, "BIGINT NOT NULL")},
		},
	}); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	script := parseScript(t, "SELECT archive.orders.id FROM archive.orders")

	typed, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 0 {
		t.Fatalf("CheckScript() diagnostics = %#v, want none", diags)
	}

	stmt := script.Nodes[0].(*parser.SelectStmt)
	if got, ok := typed.Expr(stmt.SelectList[0].Expr); !ok || got != mustAnalyzerTypeDesc(t, "BIGINT NOT NULL") {
		t.Fatalf("Expr(schema-qualified column) = (%#v, %t), want BIGINT NOT NULL", got, ok)
	}
}

func TestTypeCheckerAssignsSetOpTypes(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT id FROM orders UNION SELECT customer_id FROM orders")

	typed, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 0 {
		t.Fatalf("CheckScript() diagnostics = %#v, want none", diags)
	}

	query, ok := script.Nodes[0].(parser.QueryExpr)
	if !ok {
		t.Fatalf("script.Nodes[0] = %T, want parser.QueryExpr", script.Nodes[0])
	}
	wantOutputs := []sqltypes.TypeDesc{mustAnalyzerTypeDesc(t, "INTEGER")}
	if got, ok := typed.QueryOutputs(query); !ok || !equalTypeSlices(got, wantOutputs) {
		t.Fatalf("QueryOutputs(query) = (%#v, %t), want %#v", got, ok, wantOutputs)
	}
}

func TestTypeCheckerAssignsSubqueryExpressionTypes(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT (SELECT o.id), EXISTS (SELECT 1 FROM orders i WHERE i.customer_id = o.customer_id), customer_id IN (SELECT id FROM orders) FROM orders AS o")

	typed, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 0 {
		t.Fatalf("CheckScript() diagnostics = %#v, want none", diags)
	}

	stmt := script.Nodes[0].(*parser.SelectStmt)
	if got, ok := typed.Expr(stmt.SelectList[0].Expr); !ok || got != mustAnalyzerTypeDesc(t, "INTEGER") {
		t.Fatalf("Expr(scalar subquery) = (%#v, %t), want INTEGER", got, ok)
	}
	if got, ok := typed.Expr(stmt.SelectList[1].Expr); !ok || got != mustAnalyzerTypeDesc(t, "BOOLEAN NOT NULL") {
		t.Fatalf("Expr(EXISTS) = (%#v, %t), want BOOLEAN NOT NULL", got, ok)
	}
	if got, ok := typed.Expr(stmt.SelectList[2].Expr); !ok || got != mustAnalyzerTypeDesc(t, "BOOLEAN") {
		t.Fatalf("Expr(IN subquery) = (%#v, %t), want BOOLEAN", got, ok)
	}
}

func TestTypeCheckerAssignsCaseAndCastTypes(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT CASE status WHEN 'a' THEN 1 WHEN 'b' THEN 2 ELSE 0 END, CAST(code AS CHARACTER VARYING(12)) FROM orders")

	typed, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 0 {
		t.Fatalf("CheckScript() diagnostics = %#v, want none", diags)
	}

	stmt := script.Nodes[0].(*parser.SelectStmt)
	if got, ok := typed.Expr(stmt.SelectList[0].Expr); !ok || got != mustAnalyzerTypeDesc(t, "INTEGER NOT NULL") {
		t.Fatalf("Expr(simple CASE) = (%#v, %t), want INTEGER NOT NULL", got, ok)
	}
	if got, ok := typed.Expr(stmt.SelectList[1].Expr); !ok || got != mustAnalyzerTypeDesc(t, "VARCHAR(12)") {
		t.Fatalf("Expr(CAST) = (%#v, %t), want VARCHAR(12)", got, ok)
	}
}

func TestTypeCheckerAssignsMissingScalarFunctionTypes(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT POSITION('x', code), OVERLAY(code, status, 1), REGEXP_LIKE(code, status), REGEXP_REPLACE(code, 'a', status), REGEXP_SUBSTR(code, status), CEIL(total), FLOOR(total), ROUND(total, 0), TRUNCATE(total, 0), MOD(total, 2), POWER(total, 2), SQRT(total), EXP(total), LN(total), LOG(10, total), LOG10(total), SIN(total), COS(total), TAN(total), ASIN(total), ACOS(total), ATAN(total), ATAN2(total, 2), SIGN(total) FROM orders")

	typed, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 0 {
		t.Fatalf("CheckScript() diagnostics = %#v, want none", diags)
	}

	stmt := script.Nodes[0].(*parser.SelectStmt)
	wantOutputs := []sqltypes.TypeDesc{
		mustAnalyzerTypeDesc(t, "BIGINT"),
		mustAnalyzerTypeDesc(t, "VARCHAR(12)"),
		mustAnalyzerTypeDesc(t, "BOOLEAN"),
		mustAnalyzerTypeDesc(t, "VARCHAR(12)"),
		mustAnalyzerTypeDesc(t, "VARCHAR(12)"),
		mustAnalyzerTypeDesc(t, "INTEGER"),
		mustAnalyzerTypeDesc(t, "INTEGER"),
		mustAnalyzerTypeDesc(t, "INTEGER"),
		mustAnalyzerTypeDesc(t, "INTEGER"),
		mustAnalyzerTypeDesc(t, "INTEGER"),
		mustAnalyzerTypeDesc(t, "DOUBLE PRECISION"),
		mustAnalyzerTypeDesc(t, "DOUBLE PRECISION"),
		mustAnalyzerTypeDesc(t, "DOUBLE PRECISION"),
		mustAnalyzerTypeDesc(t, "DOUBLE PRECISION"),
		mustAnalyzerTypeDesc(t, "DOUBLE PRECISION"),
		mustAnalyzerTypeDesc(t, "DOUBLE PRECISION"),
		mustAnalyzerTypeDesc(t, "DOUBLE PRECISION"),
		mustAnalyzerTypeDesc(t, "DOUBLE PRECISION"),
		mustAnalyzerTypeDesc(t, "DOUBLE PRECISION"),
		mustAnalyzerTypeDesc(t, "DOUBLE PRECISION"),
		mustAnalyzerTypeDesc(t, "DOUBLE PRECISION"),
		mustAnalyzerTypeDesc(t, "DOUBLE PRECISION"),
		mustAnalyzerTypeDesc(t, "DOUBLE PRECISION"),
		mustAnalyzerTypeDesc(t, "INTEGER"),
	}
	if got, ok := typed.Select(stmt); !ok || !equalTypeSlices(got, wantOutputs) {
		t.Fatalf("Select(stmt) = (%#v, %t), want %#v", got, ok, wantOutputs)
	}
}

func TestTypeCheckerAssignsNonNullScalarFunctionTypes(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT RANDOM(), POSITION('a', 'abc'), OVERLAY('abc', 'x', 1, 1), REGEXP_LIKE('abc', 'a', 'i'), REGEXP_REPLACE('abc', 'a', 'x', 'i'), REGEXP_SUBSTR('abc', 'a', 'i'), CEIL(1.2), ROUND(2.5), TRUNCATE(2.5), MOD(5, 2), POWER(2, 3), LOG(100) FROM orders")

	typed, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 0 {
		t.Fatalf("CheckScript() diagnostics = %#v, want none", diags)
	}

	stmt := script.Nodes[0].(*parser.SelectStmt)
	wantOutputs := []sqltypes.TypeDesc{
		mustAnalyzerTypeDesc(t, "DOUBLE PRECISION NOT NULL"),
		mustAnalyzerTypeDesc(t, "BIGINT NOT NULL"),
		mustAnalyzerTypeDesc(t, "VARCHAR(3) NOT NULL"),
		mustAnalyzerTypeDesc(t, "BOOLEAN NOT NULL"),
		mustAnalyzerTypeDesc(t, "VARCHAR(3) NOT NULL"),
		mustAnalyzerTypeDesc(t, "VARCHAR(3) NOT NULL"),
		mustAnalyzerTypeDesc(t, "NUMERIC(2,1) NOT NULL"),
		mustAnalyzerTypeDesc(t, "NUMERIC(2,1) NOT NULL"),
		mustAnalyzerTypeDesc(t, "NUMERIC(2,1) NOT NULL"),
		mustAnalyzerTypeDesc(t, "INTEGER NOT NULL"),
		mustAnalyzerTypeDesc(t, "DOUBLE PRECISION NOT NULL"),
		mustAnalyzerTypeDesc(t, "DOUBLE PRECISION NOT NULL"),
	}
	if got, ok := typed.Select(stmt); !ok || !equalTypeSlices(got, wantOutputs) {
		t.Fatalf("Select(stmt) = (%#v, %t), want %#v", got, ok, wantOutputs)
	}
}

func TestTypeCheckerContextualizesNullAssignments(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "INSERT INTO orders (id, total) VALUES (1, NULL)")

	typed, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 0 {
		t.Fatalf("CheckScript() diagnostics = %#v, want none", diags)
	}

	stmt := script.Nodes[0].(*parser.InsertStmt)
	value := stmt.Source.(*parser.InsertValuesSource).Rows[0][1]
	if got, ok := typed.Expr(value); !ok || got != mustAnalyzerTypeDesc(t, "INTEGER") {
		t.Fatalf("Expr(insert NULL) = (%#v, %t), want INTEGER", got, ok)
	}
}

func TestTypeCheckerUsesColumnConstraintsInCreateTableDefaults(t *testing.T) {
	t.Parallel()

	cat := emptyAnalyzerCatalog(t)
	script := parseScript(t, "CREATE TABLE widgets (id INTEGER NOT NULL, code CHARACTER VARYING(12) DEFAULT CAST(id AS CHARACTER VARYING(12)))")

	typed, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 0 {
		t.Fatalf("CheckScript() diagnostics = %#v, want none", diags)
	}

	stmt := script.Nodes[0].(*parser.CreateTableStmt)
	castExpr := stmt.Columns[1].Default.(*parser.CastExpr)
	identifier := castExpr.Expr.(*parser.Identifier)
	if got, ok := typed.Expr(identifier); !ok || got != mustAnalyzerTypeDesc(t, "INTEGER NOT NULL") {
		t.Fatalf("Expr(default identifier) = (%#v, %t), want INTEGER NOT NULL", got, ok)
	}
}

func TestTypeCheckerReportsBooleanContextMismatch(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT * FROM orders WHERE total")

	_, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 1 {
		t.Fatalf("len(diagnostics) = %d, want 1", len(diags))
	}
	if diags[0].SQLState != sqlStateDatatypeMismatch {
		t.Fatalf("diagnostic SQLSTATE = %q, want %q", diags[0].SQLState, sqlStateDatatypeMismatch)
	}
	if diags[0].Message != "WHERE clause must be BOOLEAN, found INTEGER" {
		t.Fatalf("diagnostic message = %q", diags[0].Message)
	}
}

func TestTypeCheckerReportsAssignmentMismatch(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "UPDATE orders SET total = 'x'")

	_, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 1 {
		t.Fatalf("len(diagnostics) = %d, want 1", len(diags))
	}
	if diags[0].SQLState != sqlStateDatatypeMismatch {
		t.Fatalf("diagnostic SQLSTATE = %q, want %q", diags[0].SQLState, sqlStateDatatypeMismatch)
	}
	if diags[0].Message != `UPDATE value for column "total" must be coercible to INTEGER, found VARCHAR(1) NOT NULL` {
		t.Fatalf("diagnostic message = %q", diags[0].Message)
	}
}

func TestTypeCheckerReportsScalarFunctionArityMismatch(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT RANDOM(1) FROM orders")

	_, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 1 {
		t.Fatalf("len(diagnostics) = %d, want 1", len(diags))
	}
	if diags[0].SQLState != sqlStateUndefinedFunc {
		t.Fatalf("diagnostic SQLSTATE = %q, want %q", diags[0].SQLState, sqlStateUndefinedFunc)
	}
	if diags[0].Message != `function "random" does not exist` {
		t.Fatalf("diagnostic message = %q", diags[0].Message)
	}
}

func TestTypeCheckerReportsScalarFunctionIntegerArgumentMismatch(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT ROUND(total, 1.5) FROM orders")

	_, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 1 {
		t.Fatalf("len(diagnostics) = %d, want 1", len(diags))
	}
	if diags[0].SQLState != sqlStateDatatypeMismatch {
		t.Fatalf("diagnostic SQLSTATE = %q, want %q", diags[0].SQLState, sqlStateDatatypeMismatch)
	}
	if diags[0].Message != "ROUND precision must be an integer type, found NUMERIC(2,1) NOT NULL" {
		t.Fatalf("diagnostic message = %q", diags[0].Message)
	}
}

func TestTypeCheckerReportsQualifiedFunctionAsUndefined(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT analytics.COUNT(DISTINCT total) FROM orders")

	_, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 1 {
		t.Fatalf("len(diagnostics) = %d, want 1", len(diags))
	}
	if diags[0].SQLState != sqlStateUndefinedFunc {
		t.Fatalf("diagnostic SQLSTATE = %q, want %q", diags[0].SQLState, sqlStateUndefinedFunc)
	}
	if diags[0].Message != `function "analytics.count" does not exist` {
		t.Fatalf("diagnostic message = %q", diags[0].Message)
	}
}

func TestTypeCheckerReportsScalarSubqueryShapeMismatch(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT (SELECT id, total FROM orders)")

	_, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 1 {
		t.Fatalf("len(diagnostics) = %d, want 1", len(diags))
	}
	if diags[0].SQLState != sqlStateDatatypeMismatch {
		t.Fatalf("diagnostic SQLSTATE = %q, want %q", diags[0].SQLState, sqlStateDatatypeMismatch)
	}
	if diags[0].Message != "scalar subquery returned 2 columns" {
		t.Fatalf("diagnostic message = %q", diags[0].Message)
	}
}

func TestTypeCheckerReportsInSubqueryShapeMismatch(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT customer_id IN (SELECT id, total FROM orders) FROM orders")

	_, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 1 {
		t.Fatalf("len(diagnostics) = %d, want 1", len(diags))
	}
	if diags[0].SQLState != sqlStateDatatypeMismatch {
		t.Fatalf("diagnostic SQLSTATE = %q, want %q", diags[0].SQLState, sqlStateDatatypeMismatch)
	}
	if diags[0].Message != "IN subquery returned 2 columns" {
		t.Fatalf("diagnostic message = %q", diags[0].Message)
	}
}

func TestTypeCheckerReportsInSubqueryTypeMismatch(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT active IN (SELECT total FROM orders) FROM orders")

	_, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 1 {
		t.Fatalf("len(diagnostics) = %d, want 1", len(diags))
	}
	if diags[0].SQLState != sqlStateDatatypeMismatch {
		t.Fatalf("diagnostic SQLSTATE = %q, want %q", diags[0].SQLState, sqlStateDatatypeMismatch)
	}
	if diags[0].Message != "IN subquery result of type INTEGER is incompatible with left-hand type BOOLEAN" {
		t.Fatalf("diagnostic message = %q", diags[0].Message)
	}
}

func TestTypeCheckerReportsSetOpColumnCountMismatch(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT id FROM orders UNION SELECT id, total FROM orders")

	_, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 1 {
		t.Fatalf("len(diagnostics) = %d, want 1", len(diags))
	}
	if diags[0].SQLState != sqlStateDatatypeMismatch {
		t.Fatalf("diagnostic SQLSTATE = %q, want %q", diags[0].SQLState, sqlStateDatatypeMismatch)
	}
	if diags[0].Message != "UNION queries return 1 and 2 columns" {
		t.Fatalf("diagnostic message = %q", diags[0].Message)
	}
}

func TestTypeCheckerReportsSetOpTypeMismatch(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT id FROM orders UNION SELECT active FROM orders")

	_, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 1 {
		t.Fatalf("len(diagnostics) = %d, want 1", len(diags))
	}
	if diags[0].SQLState != sqlStateDatatypeMismatch {
		t.Fatalf("diagnostic SQLSTATE = %q, want %q", diags[0].SQLState, sqlStateDatatypeMismatch)
	}
	if diags[0].Message != "UNION column 1 types INTEGER NOT NULL and BOOLEAN are incompatible" {
		t.Fatalf("diagnostic message = %q", diags[0].Message)
	}
}

func TestTypeCheckerValidatesCreateTableDefaults(t *testing.T) {
	t.Parallel()

	cat := emptyAnalyzerCatalog(t)
	script := parseScript(t, "CREATE TABLE t (code CHARACTER VARYING(12) DEFAULT CAST('x' AS CHARACTER VARYING(12)), qty INTEGER DEFAULT COALESCE(NULL, 0))")

	_, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 0 {
		t.Fatalf("CheckScript() diagnostics = %#v, want none", diags)
	}
}

func TestTypeCheckerValidatesCreateViewColumnCount(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "CREATE VIEW one_name (id_only) AS SELECT id, total FROM orders")

	_, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 1 {
		t.Fatalf("len(diagnostics) = %d, want 1", len(diags))
	}
	if diags[0].SQLState != sqlStateSyntaxError {
		t.Fatalf("diagnostic SQLSTATE = %q, want %q", diags[0].SQLState, sqlStateSyntaxError)
	}
	if diags[0].Message != "CREATE VIEW declares 1 columns for 2 query columns" {
		t.Fatalf("diagnostic message = %q", diags[0].Message)
	}
}

func typeCheckSQL(t *testing.T, cat catalog.Catalog, script *parser.Script) (*Types, []diag.Diagnostic) {
	t.Helper()

	bindings, diags := NewResolver(cat).ResolveScript(script)
	if len(diags) != 0 {
		t.Fatalf("ResolveScript() diagnostics = %#v, want none", diags)
	}

	return NewTypeChecker(bindings).CheckScript(script)
}

func mixedTypeCatalog(t *testing.T) catalog.Catalog {
	t.Helper()

	cat := emptyAnalyzerCatalog(t)
	if err := cat.CreateTable(&catalog.TableDescriptor{
		ID: storage.TableID{Schema: "public", Name: "orders"},
		Columns: []catalog.ColumnDescriptor{
			{Name: "id", Type: mustAnalyzerTypeDesc(t, "INTEGER NOT NULL")},
			{Name: "customer_id", Type: mustAnalyzerTypeDesc(t, "INTEGER")},
			{Name: "total", Type: mustAnalyzerTypeDesc(t, "INTEGER")},
			{Name: "code", Type: mustAnalyzerTypeDesc(t, "VARCHAR(12)")},
			{Name: "status", Type: mustAnalyzerTypeDesc(t, "VARCHAR(1)")},
			{Name: "active", Type: mustAnalyzerTypeDesc(t, "BOOLEAN")},
		},
	}); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	return cat
}

func emptyAnalyzerCatalog(t *testing.T) catalog.Catalog {
	t.Helper()

	cat := catalog.NewMemory()
	if err := cat.CreateSchema(&catalog.SchemaDescriptor{Name: "public"}); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	return cat
}

func mustAnalyzerTypeDesc(t *testing.T, text string) sqltypes.TypeDesc {
	t.Helper()

	desc, err := sqltypes.ParseTypeDesc(text)
	if err != nil {
		t.Fatalf("ParseTypeDesc(%q) error = %v", text, err)
	}

	return desc
}

func equalTypeSlices(left, right []sqltypes.TypeDesc) bool {
	if len(left) != len(right) {
		return false
	}

	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}

	return true
}
