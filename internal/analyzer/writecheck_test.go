package analyzer

import (
	"testing"

	"github.com/jamesdrando/tucotuco/internal/catalog"
	"github.com/jamesdrando/tucotuco/internal/storage"
)

func TestTypeCheckerValidatesInsertWriteShapeAndRequiredColumns(t *testing.T) {
	t.Parallel()

	cat := writeValidationCatalog(t)

	testCases := []struct {
		name    string
		sql     string
		state   string
		message string
	}{
		{
			name:    "values count mismatch",
			sql:     "INSERT INTO widgets (id, code) VALUES (1)",
			state:   sqlStateSyntaxError,
			message: "INSERT row has 1 values for 2 target columns",
		},
		{
			name:    "query count mismatch",
			sql:     "INSERT INTO widgets (id) SELECT 1, 2",
			state:   sqlStateSyntaxError,
			message: "INSERT query returns 2 columns for 1 target columns",
		},
		{
			name:    "omits required column",
			sql:     "INSERT INTO widgets (code) VALUES ('x')",
			state:   sqlStateNotNullViolation,
			message: `INSERT omits required column "id"`,
		},
		{
			name:    "default values missing required column",
			sql:     "INSERT INTO widgets DEFAULT VALUES",
			state:   sqlStateNotNullViolation,
			message: `DEFAULT VALUES has no value for required column "id"`,
		},
		{
			name:    "null insert into not null column",
			sql:     "INSERT INTO widgets (id) VALUES (NULL)",
			state:   sqlStateNotNullViolation,
			message: `null value in column "id" violates NOT NULL constraint`,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			script := parseScript(t, testCase.sql)
			_, diags := typeCheckSQL(t, cat, script)
			if len(diags) != 1 {
				t.Fatalf("len(diagnostics) = %d, want 1", len(diags))
			}
			if diags[0].SQLState != testCase.state {
				t.Fatalf("diagnostic SQLSTATE = %q, want %q", diags[0].SQLState, testCase.state)
			}
			if diags[0].Message != testCase.message {
				t.Fatalf("diagnostic message = %q, want %q", diags[0].Message, testCase.message)
			}
		})
	}
}

func TestTypeCheckerAllowsInsertWhenCatalogProvidesDefaults(t *testing.T) {
	t.Parallel()

	cat := writeValidationCatalog(t)

	testCases := []string{
		"INSERT INTO widgets_with_default (code) VALUES ('x')",
		"INSERT INTO widgets_with_default DEFAULT VALUES",
	}

	for _, sql := range testCases {
		sql := sql
		t.Run(sql, func(t *testing.T) {
			t.Parallel()

			script := parseScript(t, sql)
			_, diags := typeCheckSQL(t, cat, script)
			if len(diags) != 0 {
				t.Fatalf("CheckScript() diagnostics = %#v, want none", diags)
			}
		})
	}
}

func TestTypeCheckerValidatesUpdateAssignmentShapeAndNullability(t *testing.T) {
	t.Parallel()

	cat := writeValidationCatalog(t)

	testCases := []struct {
		name    string
		sql     string
		state   string
		message string
	}{
		{
			name:    "tuple assignment count mismatch",
			sql:     "UPDATE widgets SET (id, code) = (1)",
			state:   sqlStateSyntaxError,
			message: "UPDATE assignment has 1 values for 2 target columns",
		},
		{
			name:    "null update into not null column",
			sql:     "UPDATE widgets SET id = NULL",
			state:   sqlStateNotNullViolation,
			message: `null value in column "id" violates NOT NULL constraint`,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			script := parseScript(t, testCase.sql)
			_, diags := typeCheckSQL(t, cat, script)
			if len(diags) != 1 {
				t.Fatalf("len(diagnostics) = %d, want 1", len(diags))
			}
			if diags[0].SQLState != testCase.state {
				t.Fatalf("diagnostic SQLSTATE = %q, want %q", diags[0].SQLState, testCase.state)
			}
			if diags[0].Message != testCase.message {
				t.Fatalf("diagnostic message = %q, want %q", diags[0].Message, testCase.message)
			}
		})
	}
}

func TestTypeCheckerRejectsNullDefaultForNotNullColumn(t *testing.T) {
	t.Parallel()

	cat := emptyAnalyzerCatalog(t)
	script := parseScript(t, "CREATE TABLE widgets (id INTEGER NOT NULL DEFAULT NULL)")

	_, diags := typeCheckSQL(t, cat, script)
	if len(diags) != 1 {
		t.Fatalf("len(diagnostics) = %d, want 1", len(diags))
	}
	if diags[0].SQLState != sqlStateNotNullViolation {
		t.Fatalf("diagnostic SQLSTATE = %q, want %q", diags[0].SQLState, sqlStateNotNullViolation)
	}
	if diags[0].Message != `DEFAULT for column "id" must not be NULL` {
		t.Fatalf("diagnostic message = %q", diags[0].Message)
	}
}

func writeValidationCatalog(t *testing.T) catalog.Catalog {
	t.Helper()

	cat := emptyAnalyzerCatalog(t)
	createWriteValidationTable(t, cat, "widgets", []catalog.ColumnDescriptor{
		{Name: "id", Type: mustAnalyzerTypeDesc(t, "INTEGER NOT NULL")},
		{Name: "code", Type: mustAnalyzerTypeDesc(t, "VARCHAR(12)")},
	})
	createWriteValidationTable(t, cat, "widgets_with_default", []catalog.ColumnDescriptor{
		{
			Name:    "id",
			Type:    mustAnalyzerTypeDesc(t, "INTEGER NOT NULL"),
			Default: &catalog.ExpressionDescriptor{SQL: "1"},
		},
		{Name: "code", Type: mustAnalyzerTypeDesc(t, "VARCHAR(12)")},
	})

	return cat
}

func createWriteValidationTable(t *testing.T, cat catalog.Catalog, table string, columns []catalog.ColumnDescriptor) {
	t.Helper()

	if err := cat.CreateTable(&catalog.TableDescriptor{
		ID: storage.TableID{
			Schema: "public",
			Name:   table,
		},
		Columns: columns,
	}); err != nil {
		t.Fatalf("CreateTable(%q) error = %v", table, err)
	}
}
