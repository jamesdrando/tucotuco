package embed

import (
	"errors"
	"fmt"

	internaldiag "github.com/jamesdrando/tucotuco/internal/diag"
)

var (
	errNilDB    = errors.New("embed: nil DB")
	errNilTx    = errors.New("embed: nil Tx")
	errTxClosed = errors.New("embed: transaction is closed")
)

// SQLError reports one or more SQL diagnostics produced while processing a
// statement.
type SQLError struct {
	// Diagnostics preserves the SQL diagnostics associated with the failure.
	Diagnostics []Diagnostic

	cause error
}

// Error returns the primary diagnostic text.
func (e *SQLError) Error() string {
	switch {
	case e == nil:
		return ""
	case len(e.Diagnostics) == 0 && e.cause != nil:
		return e.cause.Error()
	case len(e.Diagnostics) == 0:
		return "SQL error"
	case len(e.Diagnostics) == 1:
		return e.Diagnostics[0].Error()
	default:
		return fmt.Sprintf("%s (and %d more diagnostics)", e.Diagnostics[0].Error(), len(e.Diagnostics)-1)
	}
}

// Unwrap returns the underlying cause, when one exists.
func (e *SQLError) Unwrap() error {
	if e == nil {
		return nil
	}

	return e.cause
}

func wrapSQLError(err error) error {
	if err == nil {
		return nil
	}

	switch err := err.(type) {
	case *SQLError:
		return err
	}

	diagnostics := collectDiagnostics(err)
	if len(diagnostics) == 0 {
		return err
	}

	return &SQLError{
		Diagnostics: diagnostics,
		cause:       err,
	}
}

func collectDiagnostics(err error) []Diagnostic {
	var out []Diagnostic

	var visit func(error)
	visit = func(err error) {
		if err == nil {
			return
		}

		switch err := err.(type) {
		case *SQLError:
			out = append(out, cloneDiagnostics(err.Diagnostics)...)
			return
		case internaldiag.Diagnostic:
			out = append(out, exportDiagnostic(err))
			return
		}

		if unwrapper, ok := err.(interface{ Unwrap() []error }); ok {
			for _, child := range unwrapper.Unwrap() {
				visit(child)
			}
			return
		}

		if unwrapper, ok := err.(interface{ Unwrap() error }); ok {
			visit(unwrapper.Unwrap())
		}
	}

	visit(err)
	return out
}

func joinErrors(primary error, others ...error) error {
	out := primary
	for _, err := range others {
		switch {
		case err == nil:
			continue
		case out == nil:
			out = err
		default:
			out = errors.Join(out, err)
		}
	}

	return out
}
