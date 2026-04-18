package token

import "fmt"

// Kind classifies a lexical token emitted by the SQL lexer.
type Kind uint8

const (
	// KindError marks malformed input that could not be tokenized cleanly.
	KindError Kind = iota
	// KindEOF marks the end of the input stream.
	KindEOF
	// KindKeyword marks a recognized SQL keyword.
	KindKeyword
	// KindIdentifier marks an unquoted identifier.
	KindIdentifier
	// KindQuotedIdentifier marks a quoted identifier.
	KindQuotedIdentifier
	// KindString marks a string literal.
	KindString
	// KindInteger marks an integer literal.
	KindInteger
	// KindDecimal marks a decimal literal with a fractional component.
	KindDecimal
	// KindScientific marks a numeric literal using scientific notation.
	KindScientific
	// KindHex marks a hexadecimal numeric literal.
	KindHex
	// KindOperator marks an operator token.
	KindOperator
	// KindPunctuation marks punctuation such as commas and parentheses.
	KindPunctuation
	// KindLineComment marks a -- line comment.
	KindLineComment
	// KindBlockComment marks a /* ... */ block comment.
	KindBlockComment
)

// String returns the stable debug label for the token kind.
func (k Kind) String() string {
	switch k {
	case KindError:
		return "ERROR"
	case KindEOF:
		return "EOF"
	case KindKeyword:
		return "KEYWORD"
	case KindIdentifier:
		return "IDENTIFIER"
	case KindQuotedIdentifier:
		return "QUOTED_IDENTIFIER"
	case KindString:
		return "STRING"
	case KindInteger:
		return "INTEGER"
	case KindDecimal:
		return "DECIMAL"
	case KindScientific:
		return "SCIENTIFIC"
	case KindHex:
		return "HEX"
	case KindOperator:
		return "OPERATOR"
	case KindPunctuation:
		return "PUNCTUATION"
	case KindLineComment:
		return "LINE_COMMENT"
	case KindBlockComment:
		return "BLOCK_COMMENT"
	default:
		return "UNKNOWN"
	}
}

// IsNumeric reports whether the token kind is one of the numeric literal kinds.
func (k Kind) IsNumeric() bool {
	return k == KindInteger || k == KindDecimal || k == KindScientific || k == KindHex
}

// IsComment reports whether the token kind is one of the comment kinds.
func (k Kind) IsComment() bool {
	return k == KindLineComment || k == KindBlockComment
}

// Token is the parser-facing representation of a lexical item.
type Token struct {
	Kind    Kind
	Span    Span
	Lexeme  string
	Text    string
	Keyword Keyword
	Message string
}

// Pos returns the first source position covered by the token.
func (t Token) Pos() Pos {
	return t.Span.Start
}

// End returns the first source position immediately after the token.
func (t Token) End() Pos {
	return t.Span.Stop
}

// IsKeyword reports whether the token is the requested SQL keyword.
func (t Token) IsKeyword(word string) bool {
	if t.Kind != KindKeyword {
		return false
	}

	return t.Keyword.Word == foldKeyword(word)
}

// String formats the token for debugging.
func (t Token) String() string {
	switch {
	case t.Message != "":
		return fmt.Sprintf("%s(%q, %s)", t.Kind, t.Lexeme, t.Message)
	case t.Lexeme != "":
		return fmt.Sprintf("%s(%q)", t.Kind, t.Lexeme)
	default:
		return t.Kind.String()
	}
}
