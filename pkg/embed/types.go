package embed

import (
	"bytes"
	"fmt"
	"time"

	internaldiag "github.com/jamesdrando/tucotuco/internal/diag"
	sqltypes "github.com/jamesdrando/tucotuco/internal/types"
)

// CommandResult reports the outcome of one non-query SQL statement.
type CommandResult struct {
	// RowsAffected reports how many rows the command changed.
	RowsAffected int64
}

// ResultSet stores one fully materialized query result.
type ResultSet struct {
	// Columns describes the visible output schema in ordinal order.
	Columns []Column
	// Rows stores eagerly materialized row values in column order.
	Rows [][]any
}

// Column describes one visible result column.
type Column struct {
	// Name is the visible output column name.
	Name string
	// Type is the canonical SQL type text for the column.
	Type string
}

// Position identifies one location in SQL input.
type Position struct {
	Line   int
	Column int
	Offset int
}

// IsZero reports whether the position is unset.
func (p Position) IsZero() bool {
	return p.Line == 0 && p.Column == 0 && p.Offset == 0
}

// String formats the source position for human-readable diagnostics.
func (p Position) String() string {
	if p.IsZero() {
		return "unknown"
	}

	return fmt.Sprintf("%d:%d (offset %d)", p.Line, p.Column, p.Offset)
}

// Diagnostic stores one SQL diagnostic entry.
type Diagnostic struct {
	// Severity is the diagnostic severity, such as ERROR or WARNING.
	Severity string
	// SQLState is the five-character SQLSTATE code.
	SQLState string
	// Message is the human-readable diagnostic text.
	Message string
	// Position identifies the location associated with the diagnostic, when known.
	Position Position
}

// Error formats the diagnostic as an error string.
func (d Diagnostic) Error() string {
	if d.Position.IsZero() {
		return fmt.Sprintf("%s [SQLSTATE %s]: %s", d.Severity, d.SQLState, d.Message)
	}

	return fmt.Sprintf("%s [SQLSTATE %s] at %s: %s", d.Severity, d.SQLState, d.Position, d.Message)
}

func exportDiagnostic(d internaldiag.Diagnostic) Diagnostic {
	return Diagnostic{
		Severity: d.Severity.String(),
		SQLState: d.SQLState,
		Message:  d.Message,
		Position: exportPosition(d.Position),
	}
}

func exportPosition(pos internaldiag.Position) Position {
	return Position{
		Line:   pos.Line,
		Column: pos.Column,
		Offset: pos.Offset,
	}
}

func cloneDiagnostics(diagnostics []Diagnostic) []Diagnostic {
	if len(diagnostics) == 0 {
		return nil
	}

	out := make([]Diagnostic, len(diagnostics))
	copy(out, diagnostics)
	return out
}

func cloneResultSet(result *ResultSet) *ResultSet {
	out := &ResultSet{}
	if result == nil {
		return out
	}

	out.Columns = append([]Column(nil), result.Columns...)
	if len(result.Rows) == 0 {
		return out
	}

	out.Rows = make([][]any, len(result.Rows))
	for rowIndex := range result.Rows {
		out.Rows[rowIndex] = cloneRow(result.Rows[rowIndex])
	}

	return out
}

func cloneRow(row []any) []any {
	if len(row) == 0 {
		return nil
	}

	out := make([]any, len(row))
	for index := range row {
		out[index] = cloneCell(row[index])
	}

	return out
}

func cloneCell(value any) any {
	switch value := value.(type) {
	case []byte:
		return bytes.Clone(value)
	case []any:
		return cloneRow(value)
	case map[string]int64:
		out := make(map[string]int64, len(value))
		for key, item := range value {
			out[key] = item
		}
		return out
	default:
		return value
	}
}

func canonicalTypeText(desc sqltypes.TypeDesc) string {
	return desc.String()
}

func exportValue(value sqltypes.Value) any {
	if value.IsNull() {
		return nil
	}

	switch raw := value.Raw().(type) {
	case []byte:
		return bytes.Clone(raw)
	case sqltypes.Decimal:
		return raw.String()
	case sqltypes.Interval:
		return map[string]int64{
			"months": raw.Months,
			"days":   raw.Days,
			"nanos":  raw.Nanos,
		}
	case sqltypes.Array:
		out := make([]any, len(raw))
		for index := range raw {
			out[index] = exportValue(raw[index])
		}
		return out
	case sqltypes.Row:
		out := make([]any, len(raw))
		for index := range raw {
			out[index] = exportValue(raw[index])
		}
		return out
	case time.Time:
		return raw
	case time.Duration:
		return raw
	default:
		return raw
	}
}
