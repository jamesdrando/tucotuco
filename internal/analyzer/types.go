package analyzer

import (
	"github.com/jamesdrando/tucotuco/internal/diag"
	"github.com/jamesdrando/tucotuco/internal/parser"
	sqltypes "github.com/jamesdrando/tucotuco/internal/types"
)

const (
	sqlStateDatatypeMismatch = "42804"
	sqlStateUndefinedFunc    = "42883"
)

// TypeChecker assigns semantic types to parser CST expressions using existing
// name-resolution bindings.
type TypeChecker struct {
	bindings *Bindings
}

// NewTypeChecker constructs a type checker over an existing binding table.
func NewTypeChecker(bindings *Bindings) *TypeChecker {
	return &TypeChecker{bindings: bindings}
}

// Types stores the semantic types assigned during one checker pass.
//
// The table is keyed by parser-node identity, mirroring the resolver side
// tables. A present lookup whose descriptor is the zero value represents an
// expression whose type remains context-dependent or indeterminate.
type Types struct {
	exprs   map[parser.Node]sqltypes.TypeDesc
	queries map[parser.QueryExpr][]sqltypes.TypeDesc
}

func newTypes() *Types {
	return &Types{
		exprs:   make(map[parser.Node]sqltypes.TypeDesc),
		queries: make(map[parser.QueryExpr][]sqltypes.TypeDesc),
	}
}

// Expr returns the assigned type for one expression node.
func (t *Types) Expr(node parser.Node) (sqltypes.TypeDesc, bool) {
	if t == nil || node == nil {
		return sqltypes.TypeDesc{}, false
	}

	desc, ok := t.exprs[node]
	return desc, ok
}

// Select returns the projected output types for one SELECT statement.
//
// This is kept as a compatibility alias for existing call sites; new code may
// prefer SelectOutputs for a more explicit name.
func (t *Types) Select(stmt *parser.SelectStmt) ([]sqltypes.TypeDesc, bool) {
	return t.SelectOutputs(stmt)
}

// SelectOutputs returns the projected output types for one SELECT statement.
func (t *Types) SelectOutputs(stmt *parser.SelectStmt) ([]sqltypes.TypeDesc, bool) {
	return t.QueryOutputs(stmt)
}

// QueryOutputs returns the projected output types for one query expression.
func (t *Types) QueryOutputs(query parser.QueryExpr) ([]sqltypes.TypeDesc, bool) {
	if t == nil || query == nil {
		return nil, false
	}

	descs, ok := t.queries[query]
	if !ok {
		return nil, false
	}

	out := make([]sqltypes.TypeDesc, len(descs))
	copy(out, descs)
	return out, true
}

func (t *Types) bindExpr(node parser.Node, desc sqltypes.TypeDesc) {
	if t == nil || node == nil {
		return
	}

	t.exprs[node] = desc
}

func (t *Types) bindQuery(query parser.QueryExpr, descs []sqltypes.TypeDesc) {
	if t == nil || query == nil {
		return
	}

	out := make([]sqltypes.TypeDesc, len(descs))
	copy(out, descs)
	t.queries[query] = out
}

type typeCheckPass struct {
	checker     *TypeChecker
	types       *Types
	diagnostics []diag.Diagnostic
	columnTypes map[*parser.ColumnDef]sqltypes.TypeDesc
}
