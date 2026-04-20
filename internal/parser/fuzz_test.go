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

func FuzzParseScript(f *testing.F) {
	for _, seed := range []string{
		"",
		";",
		"SELECT 1;",
		"SELECT 1; SELECT 2;",
		"CREATE TABLE widgets (id INTEGER); INSERT INTO widgets VALUES (1);",
		"BEGIN; UPDATE widgets SET id = 2 WHERE id = 1; COMMIT;",
		"SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id;",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(_ *testing.T, input string) {
		p := New(lexer.NewString(input).All())
		_ = p.ParseScript()
		_ = p.Errors()
	})
}
