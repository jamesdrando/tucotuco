package embed

import (
	"errors"
	"fmt"

	"github.com/jamesdrando/tucotuco/internal/analyzer"
	internaldiag "github.com/jamesdrando/tucotuco/internal/diag"
	"github.com/jamesdrando/tucotuco/internal/lexer"
	"github.com/jamesdrando/tucotuco/internal/parser"
)

const (
	sqlStateFeatureNotSupported = "0A000"
	sqlStateSyntaxError         = "42601"
	sqlStateInternalError       = "XX000"
)

type analysisContext struct {
	sql      string
	script   *parser.Script
	bindings *analyzer.Bindings
	types    *analyzer.Types
}

func (s *session) analyzeSingleStatement(sql string) (*analysisContext, parser.Node, error) {
	script, err := parseScript(sql)
	if err != nil {
		return nil, nil, err
	}
	if len(script.Nodes) != 1 {
		return nil, nil, diagnosticError(internaldiag.NewError(
			sqlStateSyntaxError,
			fmt.Sprintf("expected exactly one SQL statement, found %d", len(script.Nodes)),
			scriptPosition(script),
		))
	}

	bindings, resolveDiags := analyzer.NewResolver(s.cat).ResolveScript(script)
	if len(resolveDiags) != 0 {
		return nil, nil, diagnosticsError(resolveDiags)
	}

	types, typeDiags := analyzer.NewTypeChecker(bindings).CheckScript(script)
	if len(typeDiags) != 0 {
		return nil, nil, diagnosticsError(typeDiags)
	}

	return &analysisContext{
		sql:      sql,
		script:   script,
		bindings: bindings,
		types:    types,
	}, script.Nodes[0], nil
}

func parseScript(sql string) (*parser.Script, error) {
	p := parser.New(lexer.NewString(sql).All())
	script := p.ParseScript()
	if errs := p.Errors(); len(errs) != 0 {
		return nil, diagnosticsError(errs)
	}

	return script, nil
}

func diagnosticsError(diagnostics []internaldiag.Diagnostic) error {
	if len(diagnostics) == 0 {
		return nil
	}

	errs := make([]error, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		errs = append(errs, diagnostic)
	}

	return errors.Join(errs...)
}

func diagnosticError(diagnostic internaldiag.Diagnostic) error {
	return diagnosticsError([]internaldiag.Diagnostic{diagnostic})
}

func featureError(node parser.Node, format string, args ...any) error {
	return diagnosticError(
		internaldiag.NewError(
			sqlStateFeatureNotSupported,
			fmt.Sprintf(format, args...),
			nodePosition(node),
		),
	)
}

func internalError(node parser.Node, format string, args ...any) error {
	return diagnosticError(
		internaldiag.NewError(
			sqlStateInternalError,
			fmt.Sprintf(format, args...),
			nodePosition(node),
		),
	)
}

func nodePosition(node parser.Node) internaldiag.Position {
	if node == nil {
		return internaldiag.Position{}
	}

	pos := node.Pos()
	return internaldiag.Position{
		Line:   pos.Line,
		Column: pos.Column,
		Offset: pos.Offset,
	}
}

func scriptPosition(script *parser.Script) internaldiag.Position {
	if script == nil {
		return internaldiag.Position{}
	}

	pos := script.Pos()
	return internaldiag.Position{
		Line:   pos.Line,
		Column: pos.Column,
		Offset: pos.Offset,
	}
}
