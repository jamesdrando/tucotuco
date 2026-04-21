package script

import "strings"

type scanState int

const (
	scanNormal scanState = iota
	scanSingleQuote
	scanDoubleQuote
	scanLineComment
	scanBlockComment
)

// SplitStatements splits SQL text on top-level semicolons while preserving the
// current Phase 1 handling for quotes and comments.
func SplitStatements(text string) []string {
	statements := make([]string, 0, 4)
	start := 0
	hasContent := false
	state := scanNormal

	for i := 0; i < len(text); i++ {
		switch state {
		case scanNormal:
			switch text[i] {
			case ';':
				if stmt := strings.TrimSpace(text[start:i]); stmt != "" && hasContent {
					statements = append(statements, stmt)
				}
				start = i + 1
				hasContent = false
			case '\'':
				hasContent = true
				state = scanSingleQuote
			case '"':
				hasContent = true
				state = scanDoubleQuote
			case '-':
				if i+1 < len(text) && text[i+1] == '-' {
					i++
					state = scanLineComment
					continue
				}
				hasContent = true
			case '/':
				if i+1 < len(text) && text[i+1] == '*' {
					i++
					state = scanBlockComment
					continue
				}
				hasContent = true
			case ' ', '\t', '\n', '\r', '\f', '\v':
			default:
				hasContent = true
			}
		case scanSingleQuote:
			if text[i] == '\'' {
				if i+1 < len(text) && text[i+1] == '\'' {
					i++
					continue
				}
				state = scanNormal
			}
		case scanDoubleQuote:
			if text[i] == '"' {
				if i+1 < len(text) && text[i+1] == '"' {
					i++
					continue
				}
				state = scanNormal
			}
		case scanLineComment:
			if text[i] == '\n' {
				state = scanNormal
			}
		case scanBlockComment:
			if text[i] == '*' && i+1 < len(text) && text[i+1] == '/' {
				i++
				state = scanNormal
			}
		}
	}

	if stmt := strings.TrimSpace(text[start:]); stmt != "" && hasContent {
		statements = append(statements, stmt)
	}

	return statements
}

// IsSelectStatement reports whether the statement's first token is SELECT.
func IsSelectStatement(statement string) bool {
	return firstKeyword(statement) == "SELECT"
}

func firstKeyword(statement string) string {
	for i := 0; i < len(statement); i++ {
		switch statement[i] {
		case ' ', '\t', '\n', '\r', '\f', '\v':
			continue
		case '-':
			if i+1 < len(statement) && statement[i+1] == '-' {
				i += 2
				for i < len(statement) && statement[i] != '\n' {
					i++
				}
				i--
				continue
			}
		case '/':
			if i+1 < len(statement) && statement[i+1] == '*' {
				i += 2
				for i+1 < len(statement) {
					if statement[i] == '*' && statement[i+1] == '/' {
						i++
						break
					}
					i++
				}
				continue
			}
		}

		start := i
		for i < len(statement) {
			ch := statement[i]
			if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' {
				i++
				continue
			}
			break
		}
		if start < i {
			return strings.ToUpper(statement[start:i])
		}
		return ""
	}

	return ""
}
