package main

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/jamesdrando/tucotuco/internal/script"
)

type queryColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type queryOutput struct {
	Columns []queryColumn `json:"columns"`
	Rows    [][]any       `json:"rows"`
}

type commandOutput struct {
	RowsAffected int64 `json:"rowsAffected"`
}

func renderScriptRun(result script.RunResult, err error, stdout, stderr io.Writer) int {
	for _, statement := range result.Statements {
		if statement.Error != "" {
			if printErr := writeLine(stderr, statement.Error); printErr != nil {
				return 1
			}
			return 1
		}

		if err := renderStatement(stdout, statement); err != nil {
			if printErr := writeLine(stderr, err); printErr != nil {
				return 1
			}
			return 1
		}
	}

	if err != nil {
		if printErr := writeLine(stderr, err); printErr != nil {
			return 1
		}
		return 1
	}

	return 0
}

func renderStatement(stdout io.Writer, statement script.StatementResult) error {
	switch statement.Kind {
	case script.StatementKindQuery:
		return writeJSON(stdout, toQueryOutput(statement.Query))
	case script.StatementKindCommand:
		if statement.Command == nil {
			return writeJSON(stdout, commandOutput{})
		}
		return writeJSON(stdout, commandOutput{RowsAffected: statement.Command.RowsAffected})
	default:
		return fmt.Errorf("script: unknown statement kind %q", statement.Kind)
	}
}

func toQueryOutput(result *script.QueryResult) queryOutput {
	if result == nil {
		return queryOutput{}
	}

	out := queryOutput{
		Columns: make([]queryColumn, len(result.Columns)),
		Rows:    result.Rows,
	}
	for i, column := range result.Columns {
		out.Columns[i] = queryColumn{
			Name: column.Name,
			Type: column.Type,
		}
	}

	return out
}

func writeJSON(stdout io.Writer, value any) error {
	enc := json.NewEncoder(stdout)
	enc.SetEscapeHTML(false)
	return enc.Encode(value)
}
