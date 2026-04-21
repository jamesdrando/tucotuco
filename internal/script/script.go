package script

import (
	"errors"
	"fmt"

	"github.com/jamesdrando/tucotuco/pkg/embed"
)

// Engine is the small execution seam required by Runner.
//
// *embed.DB satisfies this interface.
type Engine interface {
	Exec(sql string) (embed.CommandResult, error)
	Query(sql string) (*embed.ResultSet, error)
}

// Runner executes SQL scripts by splitting them into statements and routing
// SELECT statements to Query while sending everything else to Exec.
type Runner struct {
	engine Engine
}

// New constructs a Runner for the supplied engine.
func New(engine Engine) *Runner {
	return &Runner{engine: engine}
}

// RunResult stores the ordered statement outcomes from one SQL script.
type RunResult struct {
	Statements []StatementResult
}

// StatementKind identifies which engine path handled one statement.
type StatementKind string

const (
	// StatementKindQuery marks a SELECT routed to Query.
	StatementKindQuery StatementKind = "query"
	// StatementKindCommand marks a non-SELECT routed to Exec.
	StatementKindCommand StatementKind = "command"
)

// StatementResult stores the deterministic outcome for one statement.
type StatementResult struct {
	SQL     string
	Kind    StatementKind
	Query   *QueryResult
	Command *CommandResult
	Error   string
}

// QueryResult stores one materialized query outcome in render-friendly form.
type QueryResult struct {
	Columns []Column
	Rows    [][]any
}

// Column describes one visible query output column.
type Column struct {
	Name string
	Type string
}

// CommandResult stores one command outcome in render-friendly form.
type CommandResult struct {
	RowsAffected int64
}

// Run executes the supplied SQL text, preserving statement order. It stops at
// the first statement error and returns the partial results collected so far.
func (r *Runner) Run(text string) (RunResult, error) {
	if r == nil || r.engine == nil {
		return RunResult{}, errors.New("script: nil runner")
	}

	statements := SplitStatements(text)
	out := RunResult{Statements: make([]StatementResult, 0, len(statements))}
	for _, statement := range statements {
		result, err := r.RunStatement(statement)
		out.Statements = append(out.Statements, result)
		if err != nil {
			return out, err
		}
	}

	return out, nil
}

// RunStatement executes one already-split SQL statement.
func (r *Runner) RunStatement(statement string) (StatementResult, error) {
	if r == nil || r.engine == nil {
		return StatementResult{}, errors.New("script: nil runner")
	}

	out := StatementResult{SQL: statement}
	if IsSelectStatement(statement) {
		out.Kind = StatementKindQuery
		result, err := r.engine.Query(statement)
		if err != nil {
			out.Error = err.Error()
			return out, err
		}

		out.Query = exportQueryResult(result)
		return out, nil
	}

	out.Kind = StatementKindCommand
	result, err := r.engine.Exec(statement)
	if err != nil {
		out.Error = err.Error()
		return out, err
	}

	out.Command = &CommandResult{RowsAffected: result.RowsAffected}
	return out, nil
}

func exportQueryResult(result *embed.ResultSet) *QueryResult {
	out := &QueryResult{}
	if result == nil {
		return out
	}

	out.Columns = make([]Column, len(result.Columns))
	for index, column := range result.Columns {
		out.Columns[index] = Column{
			Name: column.Name,
			Type: column.Type,
		}
	}

	if len(result.Rows) == 0 {
		return out
	}

	out.Rows = make([][]any, len(result.Rows))
	for rowIndex, row := range result.Rows {
		out.Rows[rowIndex] = cloneRow(row)
	}

	return out
}

func cloneRow(row []any) []any {
	if len(row) == 0 {
		return nil
	}

	out := make([]any, len(row))
	for index := range row {
		out[index] = cloneValue(row[index])
	}

	return out
}

func cloneValue(value any) any {
	switch value := value.(type) {
	case []byte:
		out := make([]byte, len(value))
		copy(out, value)
		return out
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

// String reports a compact summary of the script result.
func (r RunResult) String() string {
	return fmt.Sprintf("%d statements", len(r.Statements))
}
