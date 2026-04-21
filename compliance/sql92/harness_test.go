package sql92

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/script"
	"github.com/jamesdrando/tucotuco/pkg/embed"
)

func TestGoldenScripts(t *testing.T) {
	t.Parallel()

	queryDir, resultDir := fixtureDirs(t)
	entries, err := os.ReadDir(queryDir)
	if err != nil {
		t.Fatalf("ReadDir(%q) error = %v", queryDir, err)
	}

	type fixture struct {
		name string
		path string
	}

	fixtures := make([]fixture, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		fixtures = append(fixtures, fixture{
			name: strings.TrimSuffix(entry.Name(), ".sql"),
			path: filepath.Join(queryDir, entry.Name()),
		})
	}
	sort.Slice(fixtures, func(i, j int) bool {
		return fixtures[i].name < fixtures[j].name
	})

	if len(fixtures) == 0 {
		t.Fatalf("no SQL fixtures found in %q", queryDir)
	}

	for _, fixture := range fixtures {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			t.Parallel()

			sqlBytes, err := os.ReadFile(fixture.path)
			if err != nil {
				t.Fatalf("ReadFile(%q) error = %v", fixture.path, err)
			}

			got, err := runFixture(t, fixture.name, string(sqlBytes))
			if err != nil {
				t.Fatalf("runFixture(%q) error = %v", fixture.name, err)
			}

			wantPath := filepath.Join(resultDir, fixture.name+".txt")
			wantBytes, err := os.ReadFile(wantPath)
			if err != nil {
				t.Fatalf("ReadFile(%q) error = %v", wantPath, err)
			}

			want := string(wantBytes)
			if got != want {
				t.Fatalf("transcript mismatch for %s\n--- got ---\n%s\n--- want ---\n%s", fixture.name, got, want)
			}
		})
	}
}

func runFixture(t *testing.T, name, sqlText string) (string, error) {
	t.Helper()

	db, err := embed.Open(filepath.Join(t.TempDir(), "catalog.json"))
	if err != nil {
		return "", err
	}

	runner := script.New(db)
	statements := script.SplitStatements(sqlText)

	var transcript strings.Builder
	fmt.Fprintf(&transcript, "fixture: %s\n", name)
	if len(statements) == 0 {
		transcript.WriteString("no statements\n")
		return transcript.String(), nil
	}
	transcript.WriteByte('\n')

	for index, stmtSQL := range statements {
		if index > 0 {
			transcript.WriteByte('\n')
		}

		fmt.Fprintf(&transcript, "statement %d:\n", index+1)
		transcript.WriteString("  sql:\n")
		writeIndentedLines(&transcript, stmtSQL, 4)
		transcript.WriteString("  result:\n")

		result, err := runner.RunStatement(stmtSQL)
		if err != nil {
			writeTranscriptError(&transcript, err)
			continue
		}

		switch result.Kind {
		case script.StatementKindQuery:
			writeQueryResult(&transcript, result.Query)
		case script.StatementKindCommand:
			writeExecResult(&transcript, result.Command)
		default:
			return "", fmt.Errorf("unexpected statement kind %q", result.Kind)
		}
	}

	return transcript.String(), nil
}

func writeQueryResult(b *strings.Builder, result *script.QueryResult) {
	b.WriteString("    query\n")
	b.WriteString("    columns:\n")
	if result == nil || len(result.Columns) == 0 {
		b.WriteString("      <empty>\n")
	} else {
		for index, column := range result.Columns {
			fmt.Fprintf(b, "      [%d] %s\n", index, formatColumn(column))
		}
	}

	b.WriteString("    rows:\n")
	if result == nil || len(result.Rows) == 0 {
		b.WriteString("      <empty>\n")
		return
	}
	for index, row := range result.Rows {
		fmt.Fprintf(b, "      [%d] %s\n", index, formatRow(row))
	}
}

func writeExecResult(b *strings.Builder, result *script.CommandResult) {
	b.WriteString("    ok\n")
	if result == nil {
		b.WriteString("    rows_affected: 0\n")
		return
	}
	fmt.Fprintf(b, "    rows_affected: %d\n", result.RowsAffected)
}

func writeTranscriptError(b *strings.Builder, err error) {
	b.WriteString("    error\n")
	b.WriteString("    diagnostics:\n")

	var sqlErr *embed.SQLError
	if !errors.As(err, &sqlErr) || len(sqlErr.Diagnostics) == 0 {
		fmt.Fprintf(b, "      [0] %s\n", err.Error())
		return
	}

	for index, diagnostic := range sqlErr.Diagnostics {
		fmt.Fprintf(b, "      [%d] %s\n", index, diagnostic.Error())
	}
}

func writeIndentedLines(b *strings.Builder, text string, indent int) {
	prefix := strings.Repeat(" ", indent)
	lines := strings.Split(text, "\n")
	for _, line := range lines {
		b.WriteString(prefix)
		b.WriteString(line)
		b.WriteByte('\n')
	}
}

func formatColumn(column script.Column) string {
	name := column.Name
	if name == "" {
		name = "<unnamed>"
	}
	if column.Type == "" {
		return name
	}

	return name + " " + column.Type
}

func formatRow(row []any) string {
	if len(row) == 0 {
		return "[]"
	}

	parts := make([]string, len(row))
	for index, value := range row {
		parts[index] = formatValue(value)
	}

	return strings.Join(parts, " | ")
}

func formatValue(value any) string {
	switch value := value.(type) {
	case nil:
		return "NULL"
	case string:
		return strconv.Quote(value)
	case []byte:
		return strconv.Quote(string(value))
	case bool:
		return strconv.FormatBool(value)
	case int:
		return strconv.Itoa(value)
	case int8:
		return strconv.FormatInt(int64(value), 10)
	case int16:
		return strconv.FormatInt(int64(value), 10)
	case int32:
		return strconv.FormatInt(int64(value), 10)
	case int64:
		return strconv.FormatInt(value, 10)
	case uint:
		return strconv.FormatUint(uint64(value), 10)
	case uint8:
		return strconv.FormatUint(uint64(value), 10)
	case uint16:
		return strconv.FormatUint(uint64(value), 10)
	case uint32:
		return strconv.FormatUint(uint64(value), 10)
	case uint64:
		return strconv.FormatUint(value, 10)
	case float32:
		return strconv.FormatFloat(float64(value), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(value, 'g', -1, 64)
	default:
		return fmt.Sprintf("%v", value)
	}
}

func fixtureDirs(t *testing.T) (string, string) {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}

	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	return filepath.Join(root, "testdata", "queries"), filepath.Join(root, "testdata", "results")
}
