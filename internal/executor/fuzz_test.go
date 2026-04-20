package executor

import (
	"testing"

	"github.com/jamesdrando/tucotuco/internal/lexer"
	"github.com/jamesdrando/tucotuco/internal/parser"
)

func FuzzCompileEvalExpr(f *testing.F) {
	for _, seed := range []string{
		"1 + 2 * 3",
		"LOWER('ABC')",
		"COALESCE(NULL, 7)",
		"CASE WHEN TRUE THEN 1 ELSE 2 END",
		"1 BETWEEN 0 AND 2",
		"'abc' LIKE 'a%'",
		"NOT FALSE",
		"CAST('42' AS INTEGER)",
		"1 IN (1, 2, 3)",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(_ *testing.T, input string) {
		p := parser.New(lexer.NewString(input).All())
		expr := p.ParseExpr()
		if len(p.Errors()) != 0 || expr == nil {
			return
		}

		compiled, err := CompileExpr(expr, nil)
		if err != nil {
			return
		}

		_, _ = compiled.Eval(NewRow())
	})
}
