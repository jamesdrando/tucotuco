package lexer_test

import (
	"testing"

	lexerpkg "github.com/jamesdrando/tucotuco/internal/lexer"
	"github.com/jamesdrando/tucotuco/internal/token"
)

func FuzzLexer(f *testing.F) {
	seeds := []string{
		"",
		"SELECT 1;",
		"'it''s'",
		"E'line\\n'",
		"/* block */",
		"-- line comment\nSELECT",
		`U&"caf\00E9"`,
		`U&"caf!00E9" UESCAPE '!'`,
		"`quoted`",
		"0x2A",
		"1.0e-9",
		"@ bad",
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	options := []lexerpkg.Options{
		{},
		{BacktickIdentifiers: true, EscapedStrings: true},
	}

	f.Fuzz(func(t *testing.T, input string) {
		for _, option := range options {
			lexer := lexerpkg.NewWithOptions([]byte(input), option)
			lastOffset := 0

			for step := 0; step <= len(input)+1; step++ {
				tok := lexer.Next()

				if tok.Pos().Offset < lastOffset {
					t.Fatalf("token start offset moved backwards: %d < %d", tok.Pos().Offset, lastOffset)
				}

				if tok.End().Offset < tok.Pos().Offset {
					t.Fatalf("token end offset %d before start offset %d", tok.End().Offset, tok.Pos().Offset)
				}

				if tok.End().Offset > len(input) {
					t.Fatalf("token end offset %d beyond input length %d", tok.End().Offset, len(input))
				}

				lastOffset = tok.End().Offset

				if tok.Kind == token.KindEOF {
					next := lexer.Next()
					if next.Kind != token.KindEOF {
						t.Fatalf("Next() after EOF returned %v, want EOF", next.Kind)
					}

					if next.Pos() != tok.Pos() || next.End() != tok.End() {
						t.Fatalf("EOF token was not stable: first=%#v second=%#v", tok, next)
					}

					return
				}
			}

			t.Fatalf("lexer failed to reach EOF within %d steps", len(input)+2)
		}
	})
}
