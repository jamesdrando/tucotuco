package lexer_test

import (
	"strings"
	"testing"
	"unicode/utf8"

	lexerpkg "github.com/jamesdrando/tucotuco/internal/lexer"
	"github.com/jamesdrando/tucotuco/internal/token"
)

type expectedToken struct {
	kind             token.Kind
	lexeme           string
	text             string
	keyword          token.Keyword
	messageSubstring string
	start            token.Pos
	end              token.Pos
}

func TestLexerSQL92Keywords(t *testing.T) {
	keywords := token.SQL92Keywords()
	if len(keywords) < 200 {
		t.Fatalf("SQL92Keywords() length = %d, want at least 200 keywords for T-021 coverage", len(keywords))
	}

	for _, keyword := range keywords {
		keyword := keyword
		input := staggerCase(strings.ToLower(keyword.Word))

		t.Run(keyword.Word, func(t *testing.T) {
			tokens := lexerpkg.NewString(input).All()
			if len(tokens) != 2 {
				t.Fatalf("token count = %d, want 2", len(tokens))
			}

			wantEnd := endOfSingleLine(input)
			assertToken(t, tokens[0], expectedToken{
				kind:    token.KindKeyword,
				lexeme:  input,
				text:    keyword.Word,
				keyword: keyword,
				start:   token.Pos{Line: 1, Column: 1, Offset: 0},
				end:     wantEnd,
			})

			if !tokens[0].IsKeyword(keyword.Word) {
				t.Fatalf("token %q did not match canonical keyword %q", tokens[0].Lexeme, keyword.Word)
			}

			assertEOF(t, tokens[1], wantEnd)
		})
	}
}

func TestLexerSQL99KeywordAdditionsRemainIdentifiers(t *testing.T) {
	keywords := token.SQL99Keywords()

	for _, keyword := range keywords {
		keyword := keyword
		t.Run(keyword.Word, func(t *testing.T) {
			input := staggerCase(strings.ToLower(keyword.Word))
			tokens := lexerpkg.NewString(input).All()
			if len(tokens) != 2 {
				t.Fatalf("token count = %d, want 2", len(tokens))
			}

			wantEnd := endOfSingleLine(input)
			assertToken(t, tokens[0], expectedToken{
				kind:   token.KindIdentifier,
				lexeme: input,
				text:   input,
				start:  token.Pos{Line: 1, Column: 1, Offset: 0},
				end:    wantEnd,
			})

			if _, found := token.LookupKeyword(keyword.Word); !found {
				t.Fatalf("LookupKeyword(%q) = not found, want combined lookup to retain SQL:1999 additions", keyword.Word)
			}

			assertEOF(t, tokens[1], wantEnd)
		})
	}
}

func TestLexerSingleTokenCases(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		options lexerpkg.Options
		want    expectedToken
	}{
		{
			name:  "identifier preserves case",
			input: "CamelCase_9",
			want: expectedToken{
				kind:   token.KindIdentifier,
				lexeme: "CamelCase_9",
				text:   "CamelCase_9",
				start:  token.Pos{Line: 1, Column: 1, Offset: 0},
				end:    endOfSingleLine("CamelCase_9"),
			},
		},
		{
			name:  "unicode identifier advances by rune columns",
			input: "π",
			want: expectedToken{
				kind:   token.KindIdentifier,
				lexeme: "π",
				text:   "π",
				start:  token.Pos{Line: 1, Column: 1, Offset: 0},
				end:    token.Pos{Line: 1, Column: 2, Offset: len("π")},
			},
		},
		{
			name:  "quoted identifier",
			input: `"Mi""Xed"`,
			want: expectedToken{
				kind:   token.KindQuotedIdentifier,
				lexeme: `"Mi""Xed"`,
				text:   `Mi"Xed`,
				start:  token.Pos{Line: 1, Column: 1, Offset: 0},
				end:    endOfSingleLine(`"Mi""Xed"`),
			},
		},
		{
			name:  "unicode quoted identifier",
			input: `U&"caf\00E9"`,
			want: expectedToken{
				kind:   token.KindQuotedIdentifier,
				lexeme: `U&"caf\00E9"`,
				text:   "café",
				start:  token.Pos{Line: 1, Column: 1, Offset: 0},
				end:    endOfSingleLine(`U&"caf\00E9"`),
			},
		},
		{
			name:  "unicode quoted identifier with UESCAPE clause",
			input: `U&"caf!00E9" UESCAPE '!'`,
			want: expectedToken{
				kind:   token.KindQuotedIdentifier,
				lexeme: `U&"caf!00E9" UESCAPE '!'`,
				text:   "café",
				start:  token.Pos{Line: 1, Column: 1, Offset: 0},
				end:    endOfSingleLine(`U&"caf!00E9" UESCAPE '!'`),
			},
		},
		{
			name:    "backtick identifier extension",
			input:   "`Mi``xed`",
			options: lexerpkg.Options{BacktickIdentifiers: true},
			want: expectedToken{
				kind:   token.KindQuotedIdentifier,
				lexeme: "`Mi``xed`",
				text:   "Mi`xed",
				start:  token.Pos{Line: 1, Column: 1, Offset: 0},
				end:    endOfSingleLine("`Mi``xed`"),
			},
		},
		{
			name:  "line comment",
			input: "-- note",
			want: expectedToken{
				kind:   token.KindLineComment,
				lexeme: "-- note",
				text:   " note",
				start:  token.Pos{Line: 1, Column: 1, Offset: 0},
				end:    endOfSingleLine("-- note"),
			},
		},
		{
			name:  "block comment",
			input: "/* note */",
			want: expectedToken{
				kind:   token.KindBlockComment,
				lexeme: "/* note */",
				text:   " note ",
				start:  token.Pos{Line: 1, Column: 1, Offset: 0},
				end:    endOfSingleLine("/* note */"),
			},
		},
		{
			name:  "string literal",
			input: `'it''s'`,
			want: expectedToken{
				kind:   token.KindString,
				lexeme: `'it''s'`,
				text:   "it's",
				start:  token.Pos{Line: 1, Column: 1, Offset: 0},
				end:    endOfSingleLine(`'it''s'`),
			},
		},
		{
			name:    "escaped string extension",
			input:   `E'line\n'`,
			options: lexerpkg.Options{EscapedStrings: true},
			want: expectedToken{
				kind:   token.KindString,
				lexeme: `E'line\n'`,
				text:   "line\n",
				start:  token.Pos{Line: 1, Column: 1, Offset: 0},
				end:    endOfSingleLine(`E'line\n'`),
			},
		},
		{
			name:  "integer literal",
			input: "42",
			want: expectedToken{
				kind:   token.KindInteger,
				lexeme: "42",
				text:   "42",
				start:  token.Pos{Line: 1, Column: 1, Offset: 0},
				end:    endOfSingleLine("42"),
			},
		},
		{
			name:  "decimal literal",
			input: "3.1415",
			want: expectedToken{
				kind:   token.KindDecimal,
				lexeme: "3.1415",
				text:   "3.1415",
				start:  token.Pos{Line: 1, Column: 1, Offset: 0},
				end:    endOfSingleLine("3.1415"),
			},
		},
		{
			name:  "leading dot decimal literal",
			input: ".5",
			want: expectedToken{
				kind:   token.KindDecimal,
				lexeme: ".5",
				text:   ".5",
				start:  token.Pos{Line: 1, Column: 1, Offset: 0},
				end:    endOfSingleLine(".5"),
			},
		},
		{
			name:  "scientific literal",
			input: "6.02e23",
			want: expectedToken{
				kind:   token.KindScientific,
				lexeme: "6.02e23",
				text:   "6.02e23",
				start:  token.Pos{Line: 1, Column: 1, Offset: 0},
				end:    endOfSingleLine("6.02e23"),
			},
		},
		{
			name:  "hex literal",
			input: "0x2A",
			want: expectedToken{
				kind:   token.KindHex,
				lexeme: "0x2A",
				text:   "0x2A",
				start:  token.Pos{Line: 1, Column: 1, Offset: 0},
				end:    endOfSingleLine("0x2A"),
			},
		},
		{
			name:  "operator",
			input: "!=",
			want: expectedToken{
				kind:   token.KindOperator,
				lexeme: "!=",
				text:   "!=",
				start:  token.Pos{Line: 1, Column: 1, Offset: 0},
				end:    endOfSingleLine("!="),
			},
		},
		{
			name:  "punctuation",
			input: "(",
			want: expectedToken{
				kind:   token.KindPunctuation,
				lexeme: "(",
				text:   "(",
				start:  token.Pos{Line: 1, Column: 1, Offset: 0},
				end:    endOfSingleLine("("),
			},
		},
		{
			name:  "backtick disabled produces error",
			input: "`",
			want: expectedToken{
				kind:             token.KindError,
				lexeme:           "`",
				messageSubstring: "backtick identifiers are disabled",
				start:            token.Pos{Line: 1, Column: 1, Offset: 0},
				end:              endOfSingleLine("`"),
			},
		},
		{
			name:  "unterminated string produces error",
			input: "'unterminated",
			want: expectedToken{
				kind:             token.KindError,
				lexeme:           "'unterminated",
				messageSubstring: "unterminated string literal",
				start:            token.Pos{Line: 1, Column: 1, Offset: 0},
				end:              endOfSingleLine("'unterminated"),
			},
		},
		{
			name:  "invalid hex literal produces error",
			input: "0x",
			want: expectedToken{
				kind:             token.KindError,
				lexeme:           "0x",
				messageSubstring: "hex literal requires at least one hexadecimal digit",
				start:            token.Pos{Line: 1, Column: 1, Offset: 0},
				end:              endOfSingleLine("0x"),
			},
		},
		{
			name:  "invalid scientific literal produces error",
			input: "1e",
			want: expectedToken{
				kind:             token.KindError,
				lexeme:           "1e",
				messageSubstring: "scientific literal has no exponent digits",
				start:            token.Pos{Line: 1, Column: 1, Offset: 0},
				end:              endOfSingleLine("1e"),
			},
		},
		{
			name:    "invalid escaped string escape produces error",
			input:   `E'\q`,
			options: lexerpkg.Options{EscapedStrings: true},
			want: expectedToken{
				kind:             token.KindError,
				lexeme:           `E'\q`,
				messageSubstring: `unknown escape sequence \q`,
				start:            token.Pos{Line: 1, Column: 1, Offset: 0},
				end:              endOfSingleLine(`E'\q`),
			},
		},
		{
			name:  "unexpected character produces error",
			input: "@",
			want: expectedToken{
				kind:             token.KindError,
				lexeme:           "@",
				messageSubstring: "unexpected character",
				start:            token.Pos{Line: 1, Column: 1, Offset: 0},
				end:              endOfSingleLine("@"),
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			tokens := lexerpkg.NewWithOptions([]byte(tc.input), tc.options).All()
			if len(tokens) != 2 {
				t.Fatalf("token count = %d, want 2", len(tokens))
			}

			assertToken(t, tokens[0], tc.want)
			assertEOF(t, tokens[1], tc.want.end)
		})
	}
}

func TestLexerKeywordAndIdentifierCaseRules(t *testing.T) {
	input := `select CamelCase camelcase "CaseSensitive"`
	tokens := lexerpkg.NewString(input).All()
	if len(tokens) != 5 {
		t.Fatalf("token count = %d, want 5", len(tokens))
	}

	if tokens[0].Kind != token.KindKeyword || tokens[0].Keyword.Word != "SELECT" {
		t.Fatalf("first token = %#v, want SELECT keyword", tokens[0])
	}

	if tokens[1].Kind != token.KindIdentifier || tokens[1].Text != "CamelCase" {
		t.Fatalf("second token = %#v, want CamelCase identifier", tokens[1])
	}

	if tokens[2].Kind != token.KindIdentifier || tokens[2].Text != "camelcase" {
		t.Fatalf("third token = %#v, want camelcase identifier", tokens[2])
	}

	if tokens[1].Text == tokens[2].Text {
		t.Fatalf("identifier case was folded: %q == %q", tokens[1].Text, tokens[2].Text)
	}

	if tokens[3].Kind != token.KindQuotedIdentifier || tokens[3].Text != "CaseSensitive" {
		t.Fatalf("fourth token = %#v, want quoted identifier preserving case", tokens[3])
	}

	if tokens[4].Kind != token.KindEOF {
		t.Fatalf("last token kind = %v, want EOF", tokens[4].Kind)
	}
}

func TestLexerRecoveryAfterError(t *testing.T) {
	input := "@ SELECT"
	lexer := lexerpkg.NewString(input)

	first := lexer.Next()
	assertToken(t, first, expectedToken{
		kind:             token.KindError,
		lexeme:           "@",
		messageSubstring: "unexpected character",
		start:            token.Pos{Line: 1, Column: 1, Offset: 0},
		end:              token.Pos{Line: 1, Column: 2, Offset: 1},
	})

	second := lexer.Next()
	assertToken(t, second, expectedToken{
		kind:    token.KindKeyword,
		lexeme:  "SELECT",
		text:    "SELECT",
		keyword: token.Keyword{Word: "SELECT", Class: token.KeywordReserved},
		start:   token.Pos{Line: 1, Column: 3, Offset: 2},
		end:     token.Pos{Line: 1, Column: 9, Offset: 8},
	})

	assertEOF(t, lexer.Next(), token.Pos{Line: 1, Column: 9, Offset: 8})
}

func TestLexerSequenceAndPositions(t *testing.T) {
	input := "SELECT\n\tπ, 0x2A -- note\r\nFROM t;"
	tokens := lexerpkg.NewString(input).All()

	want := []expectedToken{
		{
			kind:    token.KindKeyword,
			lexeme:  "SELECT",
			text:    "SELECT",
			keyword: token.Keyword{Word: "SELECT", Class: token.KeywordReserved},
			start:   token.Pos{Line: 1, Column: 1, Offset: 0},
			end:     token.Pos{Line: 1, Column: 7, Offset: 6},
		},
		{
			kind:   token.KindIdentifier,
			lexeme: "π",
			text:   "π",
			start:  token.Pos{Line: 2, Column: 2, Offset: 8},
			end:    token.Pos{Line: 2, Column: 3, Offset: 10},
		},
		{
			kind:   token.KindPunctuation,
			lexeme: ",",
			text:   ",",
			start:  token.Pos{Line: 2, Column: 3, Offset: 10},
			end:    token.Pos{Line: 2, Column: 4, Offset: 11},
		},
		{
			kind:   token.KindHex,
			lexeme: "0x2A",
			text:   "0x2A",
			start:  token.Pos{Line: 2, Column: 5, Offset: 12},
			end:    token.Pos{Line: 2, Column: 9, Offset: 16},
		},
		{
			kind:   token.KindLineComment,
			lexeme: "-- note",
			text:   " note",
			start:  token.Pos{Line: 2, Column: 10, Offset: 17},
			end:    token.Pos{Line: 2, Column: 17, Offset: 24},
		},
		{
			kind:    token.KindKeyword,
			lexeme:  "FROM",
			text:    "FROM",
			keyword: token.Keyword{Word: "FROM", Class: token.KeywordReserved},
			start:   token.Pos{Line: 3, Column: 1, Offset: 26},
			end:     token.Pos{Line: 3, Column: 5, Offset: 30},
		},
		{
			kind:   token.KindIdentifier,
			lexeme: "t",
			text:   "t",
			start:  token.Pos{Line: 3, Column: 6, Offset: 31},
			end:    token.Pos{Line: 3, Column: 7, Offset: 32},
		},
		{
			kind:   token.KindPunctuation,
			lexeme: ";",
			text:   ";",
			start:  token.Pos{Line: 3, Column: 7, Offset: 32},
			end:    token.Pos{Line: 3, Column: 8, Offset: 33},
		},
	}

	if len(tokens) != len(want)+1 {
		t.Fatalf("token count = %d, want %d including EOF", len(tokens), len(want)+1)
	}

	for index, expected := range want {
		assertToken(t, tokens[index], expected)
	}

	assertEOF(t, tokens[len(tokens)-1], token.Pos{Line: 3, Column: 8, Offset: 33})
}

func assertToken(t *testing.T, got token.Token, want expectedToken) {
	t.Helper()

	if got.Kind != want.kind {
		t.Fatalf("kind = %v, want %v", got.Kind, want.kind)
	}

	if got.Lexeme != want.lexeme {
		t.Fatalf("lexeme = %q, want %q", got.Lexeme, want.lexeme)
	}

	if want.text != "" && got.Text != want.text {
		t.Fatalf("text = %q, want %q", got.Text, want.text)
	}

	if want.text == "" && got.Text != "" && want.kind == token.KindError {
		t.Fatalf("error token text = %q, want empty", got.Text)
	}

	if want.keyword != (token.Keyword{}) && got.Keyword != want.keyword {
		t.Fatalf("keyword = %#v, want %#v", got.Keyword, want.keyword)
	}

	if want.messageSubstring != "" && !strings.Contains(got.Message, want.messageSubstring) {
		t.Fatalf("message = %q, want substring %q", got.Message, want.messageSubstring)
	}

	if got.Pos() != want.start {
		t.Fatalf("start = %#v, want %#v", got.Pos(), want.start)
	}

	if got.End() != want.end {
		t.Fatalf("end = %#v, want %#v", got.End(), want.end)
	}
}

func assertEOF(t *testing.T, got token.Token, wantPos token.Pos) {
	t.Helper()

	if got.Kind != token.KindEOF {
		t.Fatalf("kind = %v, want EOF", got.Kind)
	}

	if got.Lexeme != "" {
		t.Fatalf("EOF lexeme = %q, want empty", got.Lexeme)
	}

	if got.Pos() != wantPos {
		t.Fatalf("EOF start = %#v, want %#v", got.Pos(), wantPos)
	}

	if got.End() != wantPos {
		t.Fatalf("EOF end = %#v, want %#v", got.End(), wantPos)
	}
}

func endOfSingleLine(input string) token.Pos {
	return token.Pos{
		Line:   1,
		Column: utf8.RuneCountInString(input) + 1,
		Offset: len(input),
	}
}

func staggerCase(input string) string {
	var builder strings.Builder
	builder.Grow(len(input))

	upper := false
	for _, r := range input {
		if 'a' <= r && r <= 'z' {
			if upper {
				builder.WriteRune(r - 'a' + 'A')
			} else {
				builder.WriteRune(r)
			}

			upper = !upper
			continue
		}

		builder.WriteRune(r)
	}

	return builder.String()
}
