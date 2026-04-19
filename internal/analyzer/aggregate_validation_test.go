package analyzer

import (
	"testing"

	"github.com/jamesdrando/tucotuco/internal/catalog"
	"github.com/jamesdrando/tucotuco/internal/diag"
	"github.com/jamesdrando/tucotuco/internal/parser"
)

func TestAggregatePlacementAllowsGroupedAggregatesAndOrderByAlias(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT customer_id AS cid, COUNT(*) AS n FROM orders GROUP BY customer_id HAVING COUNT(*) > 0 ORDER BY cid, n")

	_, diags := typeCheckAndValidateAggregatePlacementSQL(t, cat, script)
	if len(diags) != 0 {
		t.Fatalf("validateAggregatePlacement() diagnostics = %#v, want none", diags)
	}
}

func TestAggregatePlacementAllowsExpressionsBuiltFromGroupedColumns(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT customer_id + 1 FROM orders GROUP BY customer_id")

	_, diags := typeCheckAndValidateAggregatePlacementSQL(t, cat, script)
	if len(diags) != 0 {
		t.Fatalf("validateAggregatePlacement() diagnostics = %#v, want none", diags)
	}
}

func TestAggregatePlacementAllowsExpressionsMatchingGroupBy(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT customer_id + 1 FROM orders GROUP BY customer_id + 1")

	_, diags := typeCheckAndValidateAggregatePlacementSQL(t, cat, script)
	if len(diags) != 0 {
		t.Fatalf("validateAggregatePlacement() diagnostics = %#v, want none", diags)
	}
}

func TestAggregatePlacementAllowsAggregateInsideDerivedTable(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT q.n FROM (SELECT COUNT(*) AS n FROM orders) AS q")

	_, diags := typeCheckAndValidateAggregatePlacementSQL(t, cat, script)
	if len(diags) != 0 {
		t.Fatalf("validateAggregatePlacement() diagnostics = %#v, want none", diags)
	}
}

func TestAggregatePlacementReportsAggregateInWhereClause(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT customer_id FROM orders WHERE COUNT(*) > 0")

	assertSingleAggregateDiagnostic(t, cat, script, `aggregate function "count" is not allowed in WHERE clause`)
}

func TestAggregatePlacementReportsUngroupedSelectColumn(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT customer_id, COUNT(*) FROM orders")

	assertSingleAggregateDiagnostic(t, cat, script, `column "orders.customer_id" must appear in GROUP BY or be used in an aggregate function`)
}

func TestAggregatePlacementReportsUngroupedColumnAgainstCompositeGroupKey(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT customer_id FROM orders GROUP BY customer_id + 1")

	assertSingleAggregateDiagnostic(t, cat, script, `column "orders.customer_id" must appear in GROUP BY or be used in an aggregate function`)
}

func TestAggregatePlacementReportsAggregateInUpdateValue(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "UPDATE orders SET total = SUM(customer_id)")

	assertSingleAggregateDiagnostic(t, cat, script, `aggregate function "sum" is not allowed in UPDATE value`)
}

func TestAggregatePlacementReportsAggregateInCheckConstraint(t *testing.T) {
	t.Parallel()

	cat := emptyAnalyzerCatalog(t)
	script := parseScript(t, "CREATE TABLE widgets (id INTEGER, CHECK (COUNT(id) > 0))")

	assertSingleAggregateDiagnostic(t, cat, script, `aggregate function "count" is not allowed in CHECK constraint`)
}

func TestAggregatePlacementReportsAggregateInDefaultExpression(t *testing.T) {
	t.Parallel()

	cat := emptyAnalyzerCatalog(t)
	script := parseScript(t, "CREATE TABLE widgets (id BIGINT DEFAULT COUNT(1))")

	assertSingleAggregateDiagnostic(t, cat, script, `aggregate function "count" is not allowed in DEFAULT expression`)
}

func TestAggregatePlacementReportsNestedAggregate(t *testing.T) {
	t.Parallel()

	cat := mixedTypeCatalog(t)
	script := parseScript(t, "SELECT SUM(COUNT(*)) FROM orders")

	assertSingleAggregateDiagnostic(t, cat, script, `aggregate function "sum" cannot contain aggregate function "count"`)
}

func assertSingleAggregateDiagnostic(t *testing.T, cat catalog.Catalog, script *parser.Script, message string) {
	t.Helper()

	_, diags := typeCheckAndValidateAggregatePlacementSQL(t, cat, script)
	if len(diags) != 1 {
		t.Fatalf("len(diagnostics) = %d, want 1", len(diags))
	}
	if diags[0].SQLState != sqlStateGroupingError {
		t.Fatalf("diagnostic SQLSTATE = %q, want %q", diags[0].SQLState, sqlStateGroupingError)
	}
	if diags[0].Message != message {
		t.Fatalf("diagnostic message = %q, want %q", diags[0].Message, message)
	}
}

func typeCheckAndValidateAggregatePlacementSQL(t *testing.T, cat catalog.Catalog, script *parser.Script) (*Types, []diag.Diagnostic) {
	t.Helper()

	bindings, diags := NewResolver(cat).ResolveScript(script)
	if len(diags) != 0 {
		t.Fatalf("ResolveScript() diagnostics = %#v, want none", diags)
	}

	return NewTypeChecker(bindings).CheckScript(script)
}
