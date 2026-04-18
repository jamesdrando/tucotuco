package lexer

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/jamesdrando/tucotuco/internal/token"
)

var multiCharOperators = []string{
	"->>",
	"<=",
	">=",
	"<>",
	"!=",
	"||",
	"::",
	":=",
	"=>",
	"->",
}

// Options controls optional lexer extensions.
type Options struct {
	BacktickIdentifiers bool
	EscapedStrings      bool
}

// Lexer incrementally tokenizes a SQL input stream.
type Lexer struct {
	input      []byte
	index      int
	line       int
	column     int
	options    Options
	emittedEOF bool
}

type state struct {
	index      int
	line       int
	column     int
	emittedEOF bool
}

// New constructs a lexer over a SQL byte slice.
func New(input []byte) *Lexer {
	return NewWithOptions(input, Options{})
}

// NewString constructs a lexer over a SQL string.
func NewString(input string) *Lexer {
	return New([]byte(input))
}

// NewWithOptions constructs a lexer over a SQL byte slice with extensions.
func NewWithOptions(input []byte, options Options) *Lexer {
	return &Lexer{
		input:   input,
		line:    1,
		column:  1,
		options: options,
	}
}

// All returns every token from the input, including EOF.
func (l *Lexer) All() []token.Token {
	tokens := make([]token.Token, 0, 16)
	for {
		tok := l.Next()
		tokens = append(tokens, tok)
		if tok.Kind == token.KindEOF {
			return tokens
		}
	}
}

// Next returns the next token from the input stream.
func (l *Lexer) Next() token.Token {
	if l.emittedEOF {
		return l.makeToken(token.KindEOF, l.pos(), "", "", token.Keyword{}, "")
	}

	l.skipWhitespace()

	start := l.pos()
	if l.eof() {
		l.emittedEOF = true
		return l.makeToken(token.KindEOF, start, "", "", token.Keyword{}, "")
	}

	if l.matchFoldedKeyword("END-EXEC") && !l.hasIdentifierContinuation(len("END-EXEC")) {
		l.advanceBytes(len("END-EXEC"))

		keyword, _ := token.LookupKeyword("END-EXEC")

		return l.makeToken(token.KindKeyword, start, l.lexeme(start), keyword.Word, keyword, "")
	}

	switch {
	case l.matchString("--"):
		return l.scanLineComment(start)
	case l.matchString("/*"):
		return l.scanBlockComment(start)
	case l.matchString(`U&"`) || l.matchString(`u&"`):
		return l.scanUnicodeQuotedIdentifier(start)
	case l.peekByte(0) == '"':
		return l.scanDelimitedIdentifier(start, '"', token.KindQuotedIdentifier)
	case l.peekByte(0) == '`':
		if !l.options.BacktickIdentifiers {
			return l.scanUnexpected(start, "backtick identifiers are disabled")
		}

		return l.scanDelimitedIdentifier(start, '`', token.KindQuotedIdentifier)
	case l.peekByte(0) == '\'':
		return l.scanString(start, false)
	case (l.peekByte(0) == 'E' || l.peekByte(0) == 'e') && l.peekByte(1) == '\'':
		if !l.options.EscapedStrings {
			return l.scanIdentifierOrKeyword(start)
		}

		return l.scanString(start, true)
	case isNumberStart(l.peekByte(0), l.peekByte(1)):
		return l.scanNumber(start)
	case l.isIdentifierStart():
		return l.scanIdentifierOrKeyword(start)
	case isPunctuation(l.peekByte(0)):
		ch := l.advanceByte()

		return l.makeToken(token.KindPunctuation, start, string(ch), string(ch), token.Keyword{}, "")
	case isOperatorStart(l.peekByte(0)):
		return l.scanOperator(start)
	default:
		return l.scanUnexpected(start, "unexpected character")
	}
}

func (l *Lexer) scanIdentifierOrKeyword(start token.Pos) token.Token {
	for !l.eof() && l.isIdentifierContinue() {
		l.advanceIdentifierRune()
	}

	lexeme := l.lexeme(start)
	keyword, ok := token.LookupKeyword(lexeme)
	if ok {
		return l.makeToken(token.KindKeyword, start, lexeme, keyword.Word, keyword, "")
	}

	return l.makeToken(token.KindIdentifier, start, lexeme, lexeme, token.Keyword{}, "")
}

func (l *Lexer) scanDelimitedIdentifier(start token.Pos, quote byte, kind token.Kind) token.Token {
	var text strings.Builder
	l.advanceByte()

	for !l.eof() {
		if l.peekByte(0) == quote {
			if l.peekByte(1) == quote {
				l.advanceByte()
				l.advanceByte()
				text.WriteByte(quote)
				continue
			}

			l.advanceByte()

			return l.makeToken(kind, start, l.lexeme(start), text.String(), token.Keyword{}, "")
		}

		r, size := l.peekRune()
		if r == utf8.RuneError && size == 1 {
			l.advanceByte()

			return l.makeToken(token.KindError, start, l.lexeme(start), "", token.Keyword{}, "invalid UTF-8 in quoted identifier")
		}

		l.advanceRune()
		text.WriteRune(r)
	}

	return l.makeToken(token.KindError, start, l.lexeme(start), "", token.Keyword{}, "unterminated quoted identifier")
}

func (l *Lexer) scanUnicodeQuotedIdentifier(start token.Pos) token.Token {
	l.advanceByte()
	l.advanceByte()

	rawContent, ok := l.scanDelimitedText('"', "quoted identifier")
	if !ok {
		return l.makeToken(token.KindError, start, l.lexeme(start), "", token.Keyword{}, "unterminated quoted identifier")
	}

	escapeCharacter := '\\'
	checkpoint := l.snapshot()
	l.skipWhitespace()
	if l.matchFoldedKeyword("UESCAPE") && !l.hasIdentifierContinuation(len("UESCAPE")) {
		l.advanceBytes(len("UESCAPE"))
		l.skipWhitespace()

		escapeText, ok := l.scanDelimitedText('\'', "UESCAPE clause")
		if !ok {
			return l.makeToken(token.KindError, start, l.lexeme(start), "", token.Keyword{}, "invalid UESCAPE clause")
		}

		if utf8.RuneCountInString(escapeText) != 1 {
			return l.makeToken(token.KindError, start, l.lexeme(start), "", token.Keyword{}, "UESCAPE clause must provide exactly one character")
		}

		escapeCharacter, _ = utf8.DecodeRuneInString(escapeText)
	} else {
		l.restore(checkpoint)
	}

	decoded, err := decodeUnicodeEscapes(rawContent, escapeCharacter)
	if err != nil {
		return l.makeToken(token.KindError, start, l.lexeme(start), "", token.Keyword{}, err.Error())
	}

	return l.makeToken(token.KindQuotedIdentifier, start, l.lexeme(start), decoded, token.Keyword{}, "")
}

func (l *Lexer) scanString(start token.Pos, escaped bool) token.Token {
	if escaped {
		l.advanceByte()
	}

	l.advanceByte()

	var text strings.Builder
	for !l.eof() {
		switch l.peekByte(0) {
		case '\'':
			if l.peekByte(1) == '\'' {
				l.advanceByte()
				l.advanceByte()
				text.WriteByte('\'')
				continue
			}

			l.advanceByte()

			return l.makeToken(token.KindString, start, l.lexeme(start), text.String(), token.Keyword{}, "")
		case '\\':
			if !escaped {
				r, _ := l.advanceRune()
				text.WriteRune(r)
				continue
			}

			value, err := l.consumeEscapedCharacter()
			if err != nil {
				return l.makeToken(token.KindError, start, l.lexeme(start), "", token.Keyword{}, err.Error())
			}

			text.WriteRune(value)
		default:
			r, size := l.peekRune()
			if r == utf8.RuneError && size == 1 {
				l.advanceByte()

				return l.makeToken(token.KindError, start, l.lexeme(start), "", token.Keyword{}, "invalid UTF-8 in string literal")
			}

			l.advanceRune()
			text.WriteRune(r)
		}
	}

	return l.makeToken(token.KindError, start, l.lexeme(start), "", token.Keyword{}, "unterminated string literal")
}

func (l *Lexer) scanNumber(start token.Pos) token.Token {
	if l.peekByte(0) == '0' && (l.peekByte(1) == 'x' || l.peekByte(1) == 'X') {
		l.advanceByte()
		l.advanceByte()
		if !isHexDigit(l.peekByte(0)) {
			return l.makeToken(token.KindError, start, l.lexeme(start), "", token.Keyword{}, "hex literal requires at least one hexadecimal digit")
		}

		for isHexDigit(l.peekByte(0)) {
			l.advanceByte()
		}

		lexeme := l.lexeme(start)

		return l.makeToken(token.KindHex, start, lexeme, lexeme, token.Keyword{}, "")
	}

	kind := token.KindInteger
	if l.peekByte(0) == '.' {
		kind = token.KindDecimal
		l.advanceByte()
		for isDigit(l.peekByte(0)) {
			l.advanceByte()
		}
	} else {
		for isDigit(l.peekByte(0)) {
			l.advanceByte()
		}

		if l.peekByte(0) == '.' {
			kind = token.KindDecimal
			l.advanceByte()
			for isDigit(l.peekByte(0)) {
				l.advanceByte()
			}
		}
	}

	if l.peekByte(0) == 'e' || l.peekByte(0) == 'E' {
		exponentStart := l.pos()
		l.advanceByte()
		if l.peekByte(0) == '+' || l.peekByte(0) == '-' {
			l.advanceByte()
		}

		if !isDigit(l.peekByte(0)) {
			return l.makeToken(token.KindError, start, l.lexeme(start), "", token.Keyword{}, fmt.Sprintf("scientific literal has no exponent digits after %s", exponentStart))
		}

		for isDigit(l.peekByte(0)) {
			l.advanceByte()
		}

		kind = token.KindScientific
	}

	lexeme := l.lexeme(start)

	return l.makeToken(kind, start, lexeme, lexeme, token.Keyword{}, "")
}

func (l *Lexer) scanOperator(start token.Pos) token.Token {
	for _, operator := range multiCharOperators {
		if l.matchString(operator) {
			l.advanceBytes(len(operator))

			return l.makeToken(token.KindOperator, start, operator, operator, token.Keyword{}, "")
		}
	}

	ch := l.advanceByte()

	return l.makeToken(token.KindOperator, start, string(ch), string(ch), token.Keyword{}, "")
}

func (l *Lexer) scanLineComment(start token.Pos) token.Token {
	l.advanceByte()
	l.advanceByte()
	bodyStart := l.index
	for !l.eof() && !isNewlineStart(l.peekByte(0)) {
		l.advanceRune()
	}

	return l.makeToken(
		token.KindLineComment,
		start,
		l.lexeme(start),
		string(l.input[bodyStart:l.index]),
		token.Keyword{},
		"",
	)
}

func (l *Lexer) scanBlockComment(start token.Pos) token.Token {
	l.advanceByte()
	l.advanceByte()
	bodyStart := l.index
	for !l.eof() {
		if l.matchString("*/") {
			bodyEnd := l.index
			l.advanceByte()
			l.advanceByte()

			return l.makeToken(
				token.KindBlockComment,
				start,
				l.lexeme(start),
				string(l.input[bodyStart:bodyEnd]),
				token.Keyword{},
				"",
			)
		}

		l.advanceRune()
	}

	return l.makeToken(token.KindError, start, l.lexeme(start), "", token.Keyword{}, "unterminated block comment")
}

func (l *Lexer) scanUnexpected(start token.Pos, message string) token.Token {
	if l.eof() {
		return l.makeToken(token.KindError, start, "", "", token.Keyword{}, message)
	}

	r, size := l.peekRune()
	if r == utf8.RuneError && size == 1 {
		l.advanceByte()
	} else {
		l.advanceRune()
	}

	return l.makeToken(token.KindError, start, l.lexeme(start), "", token.Keyword{}, message)
}

func (l *Lexer) scanDelimitedText(quote byte, name string) (string, bool) {
	var text strings.Builder
	if l.peekByte(0) != quote {
		return "", false
	}

	l.advanceByte()
	for !l.eof() {
		if l.peekByte(0) == quote {
			if l.peekByte(1) == quote {
				l.advanceByte()
				l.advanceByte()
				text.WriteByte(quote)
				continue
			}

			l.advanceByte()

			return text.String(), true
		}

		r, size := l.peekRune()
		if r == utf8.RuneError && size == 1 {
			l.advanceByte()

			return "", false
		}

		l.advanceRune()
		text.WriteRune(r)
	}

	return "", false
}

func (l *Lexer) consumeEscapedCharacter() (rune, error) {
	l.advanceByte()

	switch l.peekByte(0) {
	case 'a':
		l.advanceByte()
		return '\a', nil
	case 'b':
		l.advanceByte()
		return '\b', nil
	case 'f':
		l.advanceByte()
		return '\f', nil
	case 'n':
		l.advanceByte()
		return '\n', nil
	case 'r':
		l.advanceByte()
		return '\r', nil
	case 't':
		l.advanceByte()
		return '\t', nil
	case 'v':
		l.advanceByte()
		return '\v', nil
	case '\\':
		l.advanceByte()
		return '\\', nil
	case '\'':
		l.advanceByte()
		return '\'', nil
	case '"':
		l.advanceByte()
		return '"', nil
	case '0':
		l.advanceByte()
		return 0, nil
	case 'x':
		l.advanceByte()
		return l.consumeHexEscape(2)
	case 'u':
		l.advanceByte()
		return l.consumeHexEscape(4)
	case 'U':
		l.advanceByte()
		return l.consumeHexEscape(8)
	case 0:
		return 0, fmt.Errorf("unterminated escape sequence")
	default:
		ch := l.peekByte(0)
		l.advanceByte()
		return 0, fmt.Errorf("unknown escape sequence \\%c", ch)
	}
}

func (l *Lexer) consumeHexEscape(width int) (rune, error) {
	if l.index+width > len(l.input) {
		l.index = len(l.input)
		return 0, fmt.Errorf("hex escape requires %d hexadecimal digits", width)
	}

	raw := string(l.input[l.index : l.index+width])
	for index := 0; index < len(raw); index++ {
		if !isHexDigit(raw[index]) {
			l.advanceBytes(index + 1)
			return 0, fmt.Errorf("hex escape requires %d hexadecimal digits", width)
		}
	}

	l.advanceBytes(width)

	value, err := strconv.ParseUint(raw, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid hex escape %q", raw)
	}

	return rune(value), nil
}

func (l *Lexer) skipWhitespace() {
	for !l.eof() {
		switch l.peekByte(0) {
		case ' ', '\t', '\f':
			l.advanceByte()
		case '\n', '\r':
			l.advanceRune()
		default:
			if r, size := l.peekRune(); unicode.IsSpace(r) && !(r == utf8.RuneError && size == 1) {
				l.advanceRune()
				continue
			}

			return
		}
	}
}

func (l *Lexer) pos() token.Pos {
	return token.Pos{
		Line:   l.line,
		Column: l.column,
		Offset: l.index,
	}
}

func (l *Lexer) makeToken(kind token.Kind, start token.Pos, lexeme string, text string, keyword token.Keyword, message string) token.Token {
	return token.Token{
		Kind:    kind,
		Span:    token.Span{Start: start, Stop: l.pos()},
		Lexeme:  lexeme,
		Text:    text,
		Keyword: keyword,
		Message: message,
	}
}

func (l *Lexer) lexeme(start token.Pos) string {
	if start.Offset > len(l.input) || l.index > len(l.input) || start.Offset > l.index {
		return ""
	}

	return string(l.input[start.Offset:l.index])
}

func (l *Lexer) snapshot() state {
	return state{
		index:      l.index,
		line:       l.line,
		column:     l.column,
		emittedEOF: l.emittedEOF,
	}
}

func (l *Lexer) restore(snapshot state) {
	l.index = snapshot.index
	l.line = snapshot.line
	l.column = snapshot.column
	l.emittedEOF = snapshot.emittedEOF
}

func (l *Lexer) eof() bool {
	return l.index >= len(l.input)
}

func (l *Lexer) peekByte(offset int) byte {
	position := l.index + offset
	if position >= len(l.input) {
		return 0
	}

	return l.input[position]
}

func (l *Lexer) matchString(prefix string) bool {
	if len(prefix) > len(l.input)-l.index {
		return false
	}

	for index := 0; index < len(prefix); index++ {
		if l.input[l.index+index] != prefix[index] {
			return false
		}
	}

	return true
}

func (l *Lexer) matchFoldedKeyword(word string) bool {
	if len(word) > len(l.input)-l.index {
		return false
	}

	for index := 0; index < len(word); index++ {
		if asciiUpper(l.input[l.index+index]) != word[index] {
			return false
		}
	}

	return true
}

func (l *Lexer) hasIdentifierContinuation(offset int) bool {
	if l.index+offset >= len(l.input) {
		return false
	}

	return isIdentifierContinueByte(l.input[l.index+offset])
}

func (l *Lexer) isIdentifierStart() bool {
	if l.eof() {
		return false
	}

	r, size := l.peekRune()
	if r == utf8.RuneError && size == 1 {
		return false
	}

	return isIdentifierStartRune(r)
}

func (l *Lexer) isIdentifierContinue() bool {
	if l.eof() {
		return false
	}

	r, size := l.peekRune()
	if r == utf8.RuneError && size == 1 {
		return false
	}

	return isIdentifierContinueRune(r)
}

func (l *Lexer) peekRune() (rune, int) {
	if l.eof() {
		return utf8.RuneError, 0
	}

	if l.peekByte(0) == '\r' {
		return '\n', 1
	}

	return utf8.DecodeRune(l.input[l.index:])
}

func (l *Lexer) advanceRune() (rune, int) {
	if l.eof() {
		return utf8.RuneError, 0
	}

	if l.peekByte(0) == '\r' {
		l.advanceByte()
		if l.peekByte(0) == '\n' {
			l.advanceByte()
		}

		l.line++
		l.column = 1

		return '\n', 1
	}

	if l.peekByte(0) == '\n' {
		l.advanceByte()
		l.line++
		l.column = 1

		return '\n', 1
	}

	r, size := utf8.DecodeRune(l.input[l.index:])
	l.index += size
	l.column++

	return r, size
}

func (l *Lexer) advanceByte() byte {
	if l.eof() {
		return 0
	}

	ch := l.input[l.index]
	l.index++
	l.column++

	return ch
}

func (l *Lexer) advanceBytes(count int) {
	for remaining := count; remaining > 0 && !l.eof(); remaining-- {
		l.advanceByte()
	}
}

func (l *Lexer) advanceIdentifierRune() {
	if l.peekByte(0) < utf8.RuneSelf {
		l.advanceByte()
		return
	}

	l.advanceRune()
}

func decodeUnicodeEscapes(value string, escape rune) (string, error) {
	var builder strings.Builder
	for index := 0; index < len(value); {
		r, size := utf8.DecodeRuneInString(value[index:])
		if r == utf8.RuneError && size == 1 {
			return "", fmt.Errorf("invalid UTF-8 in Unicode identifier")
		}

		if r != escape {
			builder.WriteRune(r)
			index += size
			continue
		}

		if index+size >= len(value) {
			return "", fmt.Errorf("unterminated Unicode escape")
		}

		next, nextSize := utf8.DecodeRuneInString(value[index+size:])
		if next == escape {
			builder.WriteRune(escape)
			index += size + nextSize
			continue
		}

		if next == '+' {
			start := index + size + nextSize
			if start+6 > len(value) {
				return "", fmt.Errorf("Unicode escape requires six hexadecimal digits after +")
			}

			raw := value[start : start+6]
			if !isHexString(raw) {
				return "", fmt.Errorf("Unicode escape requires six hexadecimal digits after +")
			}

			codepoint, err := strconv.ParseInt(raw, 16, 32)
			if err != nil {
				return "", fmt.Errorf("invalid Unicode escape %q", raw)
			}

			builder.WriteRune(rune(codepoint))
			index = start + 6
			continue
		}

		start := index + size
		if start+4 > len(value) {
			return "", fmt.Errorf("Unicode escape requires four hexadecimal digits")
		}

		raw := value[start : start+4]
		if !isHexString(raw) {
			return "", fmt.Errorf("Unicode escape requires four hexadecimal digits")
		}

		codepoint, err := strconv.ParseInt(raw, 16, 32)
		if err != nil {
			return "", fmt.Errorf("invalid Unicode escape %q", raw)
		}

		builder.WriteRune(rune(codepoint))
		index = start + 4
	}

	return builder.String(), nil
}

func isNumberStart(current byte, next byte) bool {
	return isDigit(current) || (current == '.' && isDigit(next))
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isHexDigit(ch byte) bool {
	return isDigit(ch) || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
}

func isHexString(value string) bool {
	for index := 0; index < len(value); index++ {
		if !isHexDigit(value[index]) {
			return false
		}
	}

	return true
}

func isOperatorStart(ch byte) bool {
	switch ch {
	case '+', '-', '*', '/', '%', '=', '<', '>', '!', '|', '&', '^', '~', ':', '?':
		return true
	default:
		return false
	}
}

func isPunctuation(ch byte) bool {
	switch ch {
	case '(', ')', ',', ';', '.', '[', ']':
		return true
	default:
		return false
	}
}

func isNewlineStart(ch byte) bool {
	return ch == '\n' || ch == '\r'
}

func isIdentifierStartRune(r rune) bool {
	return r == '_' || unicode.IsLetter(r)
}

func isIdentifierContinueRune(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

func isIdentifierContinueByte(ch byte) bool {
	if ch >= utf8.RuneSelf {
		return true
	}

	return isIdentifierContinueRune(rune(ch))
}

func asciiUpper(ch byte) byte {
	if ch >= 'a' && ch <= 'z' {
		return ch - 'a' + 'A'
	}

	return ch
}
