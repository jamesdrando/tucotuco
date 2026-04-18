package diag

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestDiagnosticLogging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		diagnostic Diagnostic
		wantLevel  string
		wantError  string
	}{
		{
			name:       "info",
			diagnostic: NewInfo("00000", "statement prepared", Position{Line: 1, Column: 1, Offset: 0}),
			wantLevel:  "INFO",
			wantError:  "INFO [SQLSTATE 00000] at 1:1 (offset 0): statement prepared",
		},
		{
			name:       "warning",
			diagnostic: NewWarning("01000", "deprecated syntax", Position{Line: 2, Column: 4, Offset: 12}),
			wantLevel:  "WARN",
			wantError:  "WARNING [SQLSTATE 01000] at 2:4 (offset 12): deprecated syntax",
		},
		{
			name:       "error",
			diagnostic: NewError("42000", "syntax error", Position{Line: 4, Column: 9, Offset: 33}),
			wantLevel:  "ERROR",
			wantError:  "ERROR [SQLSTATE 42000] at 4:9 (offset 33): syntax error",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{
				ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
					if attr.Key == slog.TimeKey {
						return slog.Attr{}
					}

					return attr
				},
			}))

			logger.LogAttrs(
				context.Background(),
				tc.diagnostic.SlogLevel(),
				"diagnostic",
				slog.Any("diagnostic", tc.diagnostic),
			)

			var record map[string]any
			if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record); err != nil {
				t.Fatalf("unmarshal log record: %v", err)
			}

			if got := record[slog.LevelKey]; got != tc.wantLevel {
				t.Fatalf("level = %v, want %q", got, tc.wantLevel)
			}

			diagnosticRecord, ok := record["diagnostic"].(map[string]any)
			if !ok {
				t.Fatalf("diagnostic field has type %T, want object", record["diagnostic"])
			}

			if got := diagnosticRecord["severity"]; got != tc.diagnostic.Severity.String() {
				t.Fatalf("severity = %v, want %q", got, tc.diagnostic.Severity)
			}

			if got := diagnosticRecord["sqlstate"]; got != tc.diagnostic.SQLState {
				t.Fatalf("sqlstate = %v, want %q", got, tc.diagnostic.SQLState)
			}

			if got := diagnosticRecord["message"]; got != tc.diagnostic.Message {
				t.Fatalf("message = %v, want %q", got, tc.diagnostic.Message)
			}

			positionRecord, ok := diagnosticRecord["position"].(map[string]any)
			if !ok {
				t.Fatalf("position field has type %T, want object", diagnosticRecord["position"])
			}

			if got := positionRecord["line"]; got != float64(tc.diagnostic.Position.Line) {
				t.Fatalf("line = %v, want %d", got, tc.diagnostic.Position.Line)
			}

			if got := positionRecord["column"]; got != float64(tc.diagnostic.Position.Column) {
				t.Fatalf("column = %v, want %d", got, tc.diagnostic.Position.Column)
			}

			if got := positionRecord["offset"]; got != float64(tc.diagnostic.Position.Offset) {
				t.Fatalf("offset = %v, want %d", got, tc.diagnostic.Position.Offset)
			}

			if got := tc.diagnostic.Error(); got != tc.wantError {
				t.Fatalf("Error() = %q, want %q", got, tc.wantError)
			}
		})
	}
}

func TestDiagnosticErrorWithoutPosition(t *testing.T) {
	t.Parallel()

	diagnostic := NewError("22012", "division by zero", Position{})

	if got, want := diagnostic.Error(), "ERROR [SQLSTATE 22012]: division by zero"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}

	if got := diagnostic.Position.String(); !strings.EqualFold(got, "unknown") {
		t.Fatalf("Position.String() = %q, want %q", got, "unknown")
	}
}
