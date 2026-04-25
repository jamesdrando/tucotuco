package embed

import (
	"errors"
	"io"
	"strings"

	"github.com/jamesdrando/tucotuco/internal/executor"
	"github.com/jamesdrando/tucotuco/internal/parser"
	"github.com/jamesdrando/tucotuco/internal/planner"
)

func (s *session) query(sql string) (*ResultSet, error) {
	ctx, node, err := s.analyzeSingleStatement(sql)
	if err != nil {
		return nil, err
	}

	switch node := node.(type) {
	case parser.QueryExpr:
		return s.queryRows(node, ctx)
	case *parser.ExplainStmt:
		return s.explainQuery(node, ctx)
	default:
		return nil, featureError(node, "Query only supports SELECT query expressions")
	}
}

func (s *session) queryRows(query parser.QueryExpr, ctx *analysisContext) (*ResultSet, error) {
	operator, columns, err := buildQueryOperator(query, ctx.bindings, ctx.types, s.cat, s.store, s.tx)
	if err != nil {
		return nil, err
	}

	rows, err := materializeRows(operator)
	if err != nil {
		return nil, err
	}

	result := &ResultSet{
		Columns: make([]Column, len(columns)),
		Rows:    make([][]any, 0, len(rows)),
	}
	for index, column := range columns {
		result.Columns[index] = Column{
			Name: column.Name,
			Type: canonicalTypeText(column.Type),
		}
	}
	for _, row := range rows {
		result.Rows = append(result.Rows, exportRow(row))
	}

	return result, nil
}

func (s *session) explainQuery(stmt *parser.ExplainStmt, ctx *analysisContext) (*ResultSet, error) {
	if stmt == nil {
		return nil, internalError(nil, "EXPLAIN statement is nil")
	}
	if stmt.Analyze {
		return nil, featureError(stmt, "EXPLAIN ANALYZE is not supported")
	}

	plan, diags := planner.NewBuilder(ctx.bindings, ctx.types).Build(stmt.Query)
	if len(diags) != 0 {
		return nil, diagnosticsError(diags)
	}

	lines := strings.Split(planner.Explain(plan), "\n")
	result := &ResultSet{
		Columns: []Column{{Name: "query_plan", Type: "VARCHAR"}},
		Rows:    make([][]any, 0, len(lines)),
	}
	for _, line := range lines {
		result.Rows = append(result.Rows, []any{line})
	}

	return result, nil
}

func materializeRows(operator executor.Operator) ([]executor.Row, error) {
	if operator == nil {
		return nil, internalError(nil, "query operator is nil")
	}

	if err := operator.Open(); err != nil {
		return nil, err
	}

	rows := make([]executor.Row, 0)
	var runErr error
	for {
		row, err := operator.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			runErr = err
			break
		}
		rows = append(rows, row.Clone())
	}

	closeErr := operator.Close()
	if runErr != nil {
		return nil, joinErrors(runErr, closeErr)
	}
	if closeErr != nil {
		return nil, closeErr
	}

	return rows, nil
}

func runWriteOperator(operator executor.WriteOperator) (CommandResult, error) {
	if operator == nil {
		return CommandResult{}, internalError(nil, "write operator is nil")
	}

	if err := operator.Open(); err != nil {
		return CommandResult{}, err
	}

	var runErr error
	for {
		_, err := operator.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			runErr = err
			break
		}
	}

	result := CommandResult{RowsAffected: operator.AffectedRows()}
	closeErr := operator.Close()
	if runErr != nil {
		return result, joinErrors(runErr, closeErr)
	}
	if closeErr != nil {
		return result, closeErr
	}

	return result, nil
}

func runOperator(operator executor.Operator) error {
	if operator == nil {
		return internalError(nil, "operator is nil")
	}

	if err := operator.Open(); err != nil {
		return err
	}

	var runErr error
	for {
		_, err := operator.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			runErr = err
			break
		}
	}

	closeErr := operator.Close()
	return joinErrors(runErr, closeErr)
}

func exportRow(row executor.Row) []any {
	values := row.Values()
	out := make([]any, len(values))
	for index, value := range values {
		out[index] = exportValue(value)
	}

	return out
}
