package parser

import (
	"testing"

	"github.com/jamesdrando/tucotuco/internal/lexer"
)

func FuzzParseExpr(f *testing.F) {
	for _, seed := range []string{
		"1 + 2 * 3",
		"COUNT(*)",
		"CAST(total AS INTEGER)",
		"CASE WHEN a = 1 THEN 'x' ELSE 'y' END",
		"a BETWEEN 1 AND 2",
		"a IN (1, 2, 3)",
		"a IS NOT NULL",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(_ *testing.T, input string) {
		p := New(lexer.NewString(input).All())
		_ = p.ParseExpr()
		_ = p.Errors()
	})
}
