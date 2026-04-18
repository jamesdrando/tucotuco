package diag

import (
	"fmt"
	"log/slog"
)

// Severity describes the importance of a diagnostic entry.
type Severity string

const (
	// SeverityInfo marks an informational diagnostic.
	SeverityInfo Severity = "INFO"
	// SeverityWarning marks a warning diagnostic.
	SeverityWarning Severity = "WARNING"
	// SeverityError marks an error diagnostic.
	SeverityError Severity = "ERROR"
)

// String returns the string form of the severity.
func (s Severity) String() string {
	return string(s)
}

// SlogLevel maps the severity to a slog level.
func (s Severity) SlogLevel() slog.Level {
	switch s {
	case SeverityInfo:
		return slog.LevelInfo
	case SeverityWarning:
		return slog.LevelWarn
	default:
		return slog.LevelError
	}
}

// Position identifies a location in SQL input.
type Position struct {
	Line   int
	Column int
	Offset int
}

// IsZero reports whether the position is unset.
func (p Position) IsZero() bool {
	return p.Line == 0 && p.Column == 0 && p.Offset == 0
}

// String formats the source position for human-readable diagnostics.
func (p Position) String() string {
	if p.IsZero() {
		return "unknown"
	}

	return fmt.Sprintf("%d:%d (offset %d)", p.Line, p.Column, p.Offset)
}

// LogValue exposes the position as structured slog data.
func (p Position) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int("line", p.Line),
		slog.Int("column", p.Column),
		slog.Int("offset", p.Offset),
	)
}

// Diagnostic is a structured SQL diagnostic entry.
type Diagnostic struct {
	Severity Severity
	SQLState string
	Message  string
	Position Position
}

// NewInfo constructs an informational diagnostic.
func NewInfo(sqlState, message string, position Position) Diagnostic {
	return Diagnostic{
		Severity: SeverityInfo,
		SQLState: sqlState,
		Message:  message,
		Position: position,
	}
}

// NewWarning constructs a warning diagnostic.
func NewWarning(sqlState, message string, position Position) Diagnostic {
	return Diagnostic{
		Severity: SeverityWarning,
		SQLState: sqlState,
		Message:  message,
		Position: position,
	}
}

// NewError constructs an error diagnostic.
func NewError(sqlState, message string, position Position) Diagnostic {
	return Diagnostic{
		Severity: SeverityError,
		SQLState: sqlState,
		Message:  message,
		Position: position,
	}
}

// Error formats the diagnostic as an error string.
func (d Diagnostic) Error() string {
	if d.Position.IsZero() {
		return fmt.Sprintf("%s [SQLSTATE %s]: %s", d.Severity, d.SQLState, d.Message)
	}

	return fmt.Sprintf(
		"%s [SQLSTATE %s] at %s: %s",
		d.Severity,
		d.SQLState,
		d.Position,
		d.Message,
	)
}

// SlogLevel maps the diagnostic severity to a slog level.
func (d Diagnostic) SlogLevel() slog.Level {
	return d.Severity.SlogLevel()
}

// LogValue exposes the diagnostic as structured slog data.
func (d Diagnostic) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("severity", d.Severity.String()),
		slog.String("sqlstate", d.SQLState),
		slog.String("message", d.Message),
		slog.Any("position", d.Position),
	)
}
