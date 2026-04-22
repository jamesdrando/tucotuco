package executor

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jamesdrando/tucotuco/internal/parser"
	sqltypes "github.com/jamesdrando/tucotuco/internal/types"
)

var (
	// ErrUnsupportedExpression reports a parser expression that the executor
	// does not yet know how to compile.
	ErrUnsupportedExpression = errors.New("executor: unsupported expression")
	// ErrUnboundExpression reports a column reference without an explicit row
	// ordinal binding.
	ErrUnboundExpression = errors.New("executor: unbound expression")
	// ErrRowOrdinalOutOfRange reports a bound column ordinal that does not exist
	// in the current executor row.
	ErrRowOrdinalOutOfRange = errors.New("executor: row ordinal out of range")
	// ErrInvalidExpressionType reports a runtime value that does not match the
	// type required by one executor expression.
	ErrInvalidExpressionType = errors.New("executor: invalid expression type")
	// ErrDivisionByZero reports division or modulo by zero.
	ErrDivisionByZero = errors.New("executor: division by zero")
	// ErrInvalidLikeEscape reports a malformed LIKE ESCAPE value.
	ErrInvalidLikeEscape = errors.New("executor: invalid LIKE escape")
)

// OrdinalBinding binds one parsed column reference to a row ordinal and its
// already-analyzed SQL type.
type OrdinalBinding struct {
	Ordinal int
	Type    sqltypes.TypeDesc
}

// ExpressionMetadata provides the executor-visible type and binding metadata
// needed to compile parser expressions without consulting analyzer state at
// runtime.
type ExpressionMetadata interface {
	TypeOf(node parser.Node) (sqltypes.TypeDesc, bool)
	BindingOf(node parser.Node) (OrdinalBinding, bool)
}

// SubqueryCompiler optionally extends ExpressionMetadata with runtime support
// for subquery-shaped parser nodes.
type SubqueryCompiler interface {
	CompileSubquery(node parser.Node) (CompiledExpr, error)
}

// Metadata is the default map-backed ExpressionMetadata implementation.
type Metadata struct {
	Types    map[parser.Node]sqltypes.TypeDesc
	Bindings map[parser.Node]OrdinalBinding
}

// TypeOf returns the analyzed type for one parser node.
func (m Metadata) TypeOf(node parser.Node) (sqltypes.TypeDesc, bool) {
	if node == nil {
		return sqltypes.TypeDesc{}, false
	}
	if desc, ok := m.Types[node]; ok {
		return desc, true
	}
	if binding, ok := m.Bindings[node]; ok && !isUnknownTypeDesc(binding.Type) {
		return binding.Type, true
	}

	return sqltypes.TypeDesc{}, false
}

// BindingOf returns the row ordinal binding for one parser node.
func (m Metadata) BindingOf(node parser.Node) (OrdinalBinding, bool) {
	if node == nil || m.Bindings == nil {
		return OrdinalBinding{}, false
	}

	binding, ok := m.Bindings[node]
	return binding, ok
}

// CompiledExpr evaluates one parser expression against executor rows.
type CompiledExpr struct {
	typ  sqltypes.TypeDesc
	eval func(Row) (sqltypes.Value, error)
}

// NewCompiledExpr constructs one executor-native compiled expression from an
// analyzed result type and an evaluation closure.
func NewCompiledExpr(
	desc sqltypes.TypeDesc,
	eval func(Row) (sqltypes.Value, error),
) CompiledExpr {
	return CompiledExpr{
		typ:  desc,
		eval: eval,
	}
}

// Eval evaluates the compiled expression for one row.
func (e CompiledExpr) Eval(row Row) (sqltypes.Value, error) {
	if e.eval == nil {
		return sqltypes.Value{}, fmt.Errorf("%w: missing evaluator", ErrUnsupportedExpression)
	}

	return e.eval(row)
}

// Type reports the expression's analyzed or inferred SQL type.
func (e CompiledExpr) Type() sqltypes.TypeDesc {
	return e.typ
}

// CompileExpr compiles one parser expression into an executor-native evaluator.
func CompileExpr(node parser.Node, metadata ExpressionMetadata) (CompiledExpr, error) {
	return compiler{metadata: metadata}.compile(node)
}

type compiler struct {
	metadata ExpressionMetadata
}

func (c compiler) compile(node parser.Node) (CompiledExpr, error) {
	switch node := node.(type) {
	case nil:
		return CompiledExpr{}, fmt.Errorf("%w: <nil>", ErrUnsupportedExpression)
	case *parser.Identifier:
		return c.compileBoundReference(node)
	case *parser.QualifiedName:
		return c.compileBoundReference(node)
	case *parser.IntegerLiteral:
		return c.compileIntegerLiteral(node)
	case *parser.FloatLiteral:
		return c.compileFloatLiteral(node)
	case *parser.StringLiteral:
		return c.compileStringLiteral(node)
	case *parser.BoolLiteral:
		return c.compileBoolLiteral(node)
	case *parser.NullLiteral:
		return c.compileNullLiteral(node), nil
	case *parser.FunctionCall:
		return c.compileFunctionCall(node)
	case *parser.CaseExpr:
		return c.compileCaseExpr(node)
	case *parser.UnaryExpr:
		return c.compileUnaryExpr(node)
	case *parser.BinaryExpr:
		return c.compileBinaryExpr(node)
	case *parser.CastExpr:
		return c.compileCastExpr(node)
	case *parser.BetweenExpr:
		return c.compileBetweenExpr(node)
	case *parser.SubqueryExpr:
		return c.compileSubquery(node)
	case *parser.ExistsExpr:
		return c.compileSubquery(node)
	case *parser.InExpr:
		return c.compileInExpr(node)
	case *parser.LikeExpr:
		return c.compileLikeExpr(node)
	case *parser.IsExpr:
		return c.compileIsExpr(node)
	case *parser.SelectItem:
		return c.compile(node.Expr)
	default:
		return CompiledExpr{}, fmt.Errorf("%w: %T", ErrUnsupportedExpression, node)
	}
}

func (c compiler) compileSubquery(node parser.Node) (CompiledExpr, error) {
	compiler, ok := c.metadata.(SubqueryCompiler)
	if !ok {
		return CompiledExpr{}, fmt.Errorf("%w: %T", ErrUnsupportedExpression, node)
	}

	return compiler.CompileSubquery(node)
}

func (c compiler) compileBoundReference(node parser.Node) (CompiledExpr, error) {
	binding, ok := c.bindingOf(node)
	if !ok {
		return CompiledExpr{}, fmt.Errorf("%w: %T", ErrUnboundExpression, node)
	}
	if binding.Ordinal < 0 {
		return CompiledExpr{}, fmt.Errorf("%w: ordinal %d", ErrUnboundExpression, binding.Ordinal)
	}

	desc, ok := c.typeOf(node)
	if !ok {
		desc = binding.Type
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			value, ok := row.Value(binding.Ordinal)
			if !ok {
				return sqltypes.Value{}, fmt.Errorf(
					"%w: ordinal %d for row with %d columns",
					ErrRowOrdinalOutOfRange,
					binding.Ordinal,
					row.Len(),
				)
			}

			return value, nil
		},
	}, nil
}

func (c compiler) compileIntegerLiteral(node *parser.IntegerLiteral) (CompiledExpr, error) {
	desc, ok := c.typeOf(node)
	if !ok {
		desc = inferIntegerLiteralType(node)
	}

	value, err := integerLiteralValue(node.Text, desc)
	if err != nil {
		return CompiledExpr{}, err
	}

	return constantExpr(desc, value), nil
}

func (c compiler) compileFloatLiteral(node *parser.FloatLiteral) (CompiledExpr, error) {
	desc, ok := c.typeOf(node)
	if !ok {
		desc = inferFloatLiteralType(node)
	}

	value, err := floatLiteralValue(node.Text, desc)
	if err != nil {
		return CompiledExpr{}, err
	}

	return constantExpr(desc, value), nil
}

func (c compiler) compileStringLiteral(node *parser.StringLiteral) (CompiledExpr, error) {
	desc, ok := c.typeOf(node)
	if !ok {
		desc = inferStringLiteralType(node)
	}

	return constantExpr(desc, sqltypes.StringValue(node.Value)), nil
}

func (c compiler) compileBoolLiteral(node *parser.BoolLiteral) (CompiledExpr, error) {
	desc, ok := c.typeOf(node)
	if !ok {
		desc = sqltypes.TypeDesc{Kind: sqltypes.TypeKindBoolean}
	}

	return constantExpr(desc, sqltypes.BoolValue(node.Value)), nil
}

func (c compiler) compileNullLiteral(node parser.Node) CompiledExpr {
	desc, _ := c.typeOf(node)
	return constantExpr(desc, sqltypes.NullValue())
}

func (c compiler) compileFunctionCall(node *parser.FunctionCall) (CompiledExpr, error) {
	if node == nil {
		return CompiledExpr{}, fmt.Errorf("%w: <nil> function call", ErrUnsupportedExpression)
	}
	if strings.TrimSpace(node.SetQuantifier) != "" {
		return CompiledExpr{}, fmt.Errorf("%w: function set quantifier %q", ErrUnsupportedExpression, node.SetQuantifier)
	}

	name := functionName(node.Name)
	if name == "" {
		return CompiledExpr{}, fmt.Errorf("%w: missing function name", ErrUnsupportedExpression)
	}

	args := make([]CompiledExpr, 0, len(node.Args))
	for _, arg := range node.Args {
		if _, ok := arg.(*parser.Star); ok {
			return CompiledExpr{}, fmt.Errorf("%w: star argument in %s", ErrUnsupportedExpression, name)
		}

		compiled, err := c.compile(arg)
		if err != nil {
			return CompiledExpr{}, err
		}
		args = append(args, compiled)
	}

	desc, _ := c.typeOf(node)

	switch name {
	case "ABS":
		return c.compileAbsCall(desc, args)
	case "ACOS":
		return c.compileApproxUnaryNumericCall("ACOS", desc, args, math.Acos)
	case "ASIN":
		return c.compileApproxUnaryNumericCall("ASIN", desc, args, math.Asin)
	case "ATAN":
		return c.compileApproxUnaryNumericCall("ATAN", desc, args, math.Atan)
	case "ATAN2":
		return c.compileApproxBinaryNumericCall("ATAN2", desc, args, math.Atan2)
	case "LOWER":
		return c.compileCaseFoldCall(desc, args, strings.ToLower)
	case "UPPER":
		return c.compileCaseFoldCall(desc, args, strings.ToUpper)
	case "CEIL":
		return c.compileCeilFloorCall("CEIL", desc, args, true)
	case "COS":
		return c.compileApproxUnaryNumericCall("COS", desc, args, math.Cos)
	case "LTRIM":
		return c.compileTrimCall(desc, args, trimLeft)
	case "RTRIM":
		return c.compileTrimCall(desc, args, trimRight)
	case "TRIM":
		return c.compileTrimCall(desc, args, trimBoth)
	case "EXP":
		return c.compileApproxUnaryNumericCall("EXP", desc, args, math.Exp)
	case "FLOOR":
		return c.compileCeilFloorCall("FLOOR", desc, args, false)
	case "LN":
		return c.compileApproxUnaryNumericCall("LN", desc, args, math.Log)
	case "LOG":
		return c.compileLogCall(desc, args)
	case "LOG10":
		return c.compileApproxUnaryNumericCall("LOG10", desc, args, math.Log10)
	case "MOD":
		return c.compileModCall(desc, args)
	case "SUBSTRING":
		return c.compileSubstringCall(desc, args)
	case "OVERLAY":
		return c.compileOverlayCall(desc, args)
	case "POSITION":
		return c.compilePositionCall(desc, args)
	case "POWER":
		return c.compileApproxBinaryNumericCall("POWER", desc, args, math.Pow)
	case "RANDOM":
		return compileRandomCall(desc, args)
	case "REGEXP_LIKE":
		return c.compileRegexpLikeCall(desc, args)
	case "REGEXP_REPLACE":
		return c.compileRegexpReplaceCall(desc, args)
	case "REGEXP_SUBSTR":
		return c.compileRegexpSubstrCall(desc, args)
	case "ROUND":
		return c.compileRoundCall("ROUND", desc, args, false)
	case "SIGN":
		return c.compileSignCall(desc, args)
	case "SIN":
		return c.compileApproxUnaryNumericCall("SIN", desc, args, math.Sin)
	case "SQRT":
		return c.compileApproxUnaryNumericCall("SQRT", desc, args, math.Sqrt)
	case "TAN":
		return c.compileApproxUnaryNumericCall("TAN", desc, args, math.Tan)
	case "TRUNCATE":
		return c.compileRoundCall("TRUNCATE", desc, args, true)
	case "CONCAT":
		return c.compileConcatCall(desc, args)
	case "CHAR_LENGTH", "CHARACTER_LENGTH":
		return c.compileCharLengthCall(desc, args)
	case "OCTET_LENGTH":
		return c.compileOctetLengthCall(desc, args)
	case "COALESCE":
		return c.compileCoalesceCall(desc, args)
	case "NULLIF":
		return c.compileNullIfCall(desc, args)
	case "GREATEST":
		return c.compileExtremumCall(desc, args, true)
	case "LEAST":
		return c.compileExtremumCall(desc, args, false)
	case "CURRENT_DATE":
		return compileCurrentDateCall(desc, args)
	case "CURRENT_TIME":
		return compileCurrentTimeCall(desc, args)
	case "CURRENT_TIMESTAMP":
		return compileCurrentTimestampCall(desc, args)
	default:
		return CompiledExpr{}, fmt.Errorf("%w: function %s", ErrUnsupportedExpression, name)
	}
}

func (c compiler) compileCaseExpr(node *parser.CaseExpr) (CompiledExpr, error) {
	var (
		operand    CompiledExpr
		hasOperand bool
	)
	if node.Operand != nil {
		compiled, err := c.compile(node.Operand)
		if err != nil {
			return CompiledExpr{}, err
		}
		operand = compiled
		hasOperand = true
	}

	whens := make([]compiledWhenClause, 0, len(node.Whens))
	for _, when := range node.Whens {
		if when == nil {
			continue
		}

		condition, err := c.compile(when.Condition)
		if err != nil {
			return CompiledExpr{}, err
		}
		result, err := c.compile(when.Result)
		if err != nil {
			return CompiledExpr{}, err
		}
		whens = append(whens, compiledWhenClause{
			condition: condition,
			result:    result,
		})
	}

	var (
		elseExpr CompiledExpr
		hasElse  bool
	)
	if node.Else != nil {
		compiled, err := c.compile(node.Else)
		if err != nil {
			return CompiledExpr{}, err
		}
		elseExpr = compiled
		hasElse = true
	}

	desc, ok := c.typeOf(node)
	if !ok && hasElse {
		desc = elseExpr.typ
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			var (
				operandValue sqltypes.Value
				err          error
			)
			if hasOperand {
				operandValue, err = operand.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
			}

			for _, when := range whens {
				conditionValue, err := when.condition.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}

				matched := false
				if hasOperand {
					if !operandValue.IsNull() && !conditionValue.IsNull() {
						comparison, err := compareValues(operandValue, operand.typ, conditionValue, when.condition.typ)
						if err != nil {
							return sqltypes.Value{}, err
						}
						matched = comparison == 0
					}
				} else {
					truth, err := sqlBoolFromValue(conditionValue)
					if err != nil {
						return sqltypes.Value{}, err
					}
					matched = truth == sqlTrue
				}
				if !matched {
					continue
				}

				value, err := when.result.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}

				return coerceResultValue(value, when.result.typ, desc)
			}

			if hasElse {
				value, err := elseExpr.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}

				return coerceResultValue(value, elseExpr.typ, desc)
			}

			return sqltypes.NullValue(), nil
		},
	}, nil
}

func (c compiler) compileUnaryExpr(node *parser.UnaryExpr) (CompiledExpr, error) {
	operand, err := c.compile(node.Operand)
	if err != nil {
		return CompiledExpr{}, err
	}

	operator := strings.ToUpper(strings.TrimSpace(node.Operator))
	desc, ok := c.typeOf(node)
	if !ok {
		switch operator {
		case "NOT":
			desc = booleanDesc(isNullableType(operand.typ))
		case "+", "-":
			desc = operand.typ
		}
	}

	switch operator {
	case "NOT":
		return CompiledExpr{
			typ: desc,
			eval: func(row Row) (sqltypes.Value, error) {
				value, err := operand.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}

				truth, err := sqlBoolFromValue(value)
				if err != nil {
					return sqltypes.Value{}, err
				}

				return valueFromSQLBool(notSQLBool(truth)), nil
			},
		}, nil
	case "+":
		return CompiledExpr{
			typ: desc,
			eval: func(row Row) (sqltypes.Value, error) {
				value, err := operand.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				if value.IsNull() {
					return sqltypes.NullValue(), nil
				}

				return coerceRuntimeValue(value, operand.typ, desc)
			},
		}, nil
	case "-":
		return CompiledExpr{
			typ: desc,
			eval: func(row Row) (sqltypes.Value, error) {
				value, err := operand.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				if value.IsNull() {
					return sqltypes.NullValue(), nil
				}

				return negateNumericValue(value, operand.typ, desc)
			},
		}, nil
	default:
		return CompiledExpr{}, fmt.Errorf("%w: unary operator %q", ErrUnsupportedExpression, node.Operator)
	}
}

func (c compiler) compileBinaryExpr(node *parser.BinaryExpr) (CompiledExpr, error) {
	left, err := c.compile(node.Left)
	if err != nil {
		return CompiledExpr{}, err
	}
	right, err := c.compile(node.Right)
	if err != nil {
		return CompiledExpr{}, err
	}

	operator := strings.ToUpper(strings.TrimSpace(node.Operator))
	desc, ok := c.typeOf(node)
	if !ok {
		desc = inferBinaryType(operator, left.typ, right.typ)
	}

	switch operator {
	case "AND":
		return CompiledExpr{
			typ: desc,
			eval: func(row Row) (sqltypes.Value, error) {
				leftValue, err := left.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				rightValue, err := right.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}

				leftTruth, err := sqlBoolFromValue(leftValue)
				if err != nil {
					return sqltypes.Value{}, err
				}
				rightTruth, err := sqlBoolFromValue(rightValue)
				if err != nil {
					return sqltypes.Value{}, err
				}

				return valueFromSQLBool(andSQLBool(leftTruth, rightTruth)), nil
			},
		}, nil
	case "OR":
		return CompiledExpr{
			typ: desc,
			eval: func(row Row) (sqltypes.Value, error) {
				leftValue, err := left.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				rightValue, err := right.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}

				leftTruth, err := sqlBoolFromValue(leftValue)
				if err != nil {
					return sqltypes.Value{}, err
				}
				rightTruth, err := sqlBoolFromValue(rightValue)
				if err != nil {
					return sqltypes.Value{}, err
				}

				return valueFromSQLBool(orSQLBool(leftTruth, rightTruth)), nil
			},
		}, nil
	case "+", "-", "*", "/", "%":
		return CompiledExpr{
			typ: desc,
			eval: func(row Row) (sqltypes.Value, error) {
				leftValue, err := left.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				rightValue, err := right.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}

				return numericBinaryValue(operator, leftValue, left.typ, rightValue, right.typ, desc)
			},
		}, nil
	case "||":
		return CompiledExpr{
			typ: desc,
			eval: func(row Row) (sqltypes.Value, error) {
				leftValue, err := left.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				rightValue, err := right.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}

				return concatValues(leftValue, left.typ, rightValue, right.typ)
			},
		}, nil
	case "=", "!=", "<>", "<", "<=", ">", ">=":
		return CompiledExpr{
			typ: desc,
			eval: func(row Row) (sqltypes.Value, error) {
				leftValue, err := left.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				rightValue, err := right.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				if leftValue.IsNull() || rightValue.IsNull() {
					return sqltypes.NullValue(), nil
				}

				comparison, err := compareValues(leftValue, left.typ, rightValue, right.typ)
				if err != nil {
					return sqltypes.Value{}, err
				}

				return sqltypes.BoolValue(applyComparisonOperator(operator, comparison)), nil
			},
		}, nil
	default:
		return CompiledExpr{}, fmt.Errorf("%w: binary operator %q", ErrUnsupportedExpression, node.Operator)
	}
}

func (c compiler) compileCastExpr(node *parser.CastExpr) (CompiledExpr, error) {
	expr, err := c.compile(node.Expr)
	if err != nil {
		return CompiledExpr{}, err
	}

	target, err := typeDescFromTypeName(node.Type)
	if err != nil {
		return CompiledExpr{}, err
	}

	desc, ok := c.typeOf(node)
	if !ok {
		desc = target
		if isNullableType(expr.typ) {
			desc.Nullable = true
		}
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			value, err := expr.Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}

			return castRuntimeValue(value, expr.typ, target, false)
		},
	}, nil
}

func (c compiler) compileBetweenExpr(node *parser.BetweenExpr) (CompiledExpr, error) {
	expr, err := c.compile(node.Expr)
	if err != nil {
		return CompiledExpr{}, err
	}
	lower, err := c.compile(node.Lower)
	if err != nil {
		return CompiledExpr{}, err
	}
	upper, err := c.compile(node.Upper)
	if err != nil {
		return CompiledExpr{}, err
	}

	desc, ok := c.typeOf(node)
	if !ok {
		desc = booleanDesc(isNullableType(expr.typ) || isNullableType(lower.typ) || isNullableType(upper.typ))
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			exprValue, err := expr.Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			lowerValue, err := lower.Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			upperValue, err := upper.Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if exprValue.IsNull() || lowerValue.IsNull() || upperValue.IsNull() {
				return sqltypes.NullValue(), nil
			}

			lowerCompare, err := compareValues(exprValue, expr.typ, lowerValue, lower.typ)
			if err != nil {
				return sqltypes.Value{}, err
			}
			upperCompare, err := compareValues(exprValue, expr.typ, upperValue, upper.typ)
			if err != nil {
				return sqltypes.Value{}, err
			}

			result := lowerCompare >= 0 && upperCompare <= 0
			if node.Negated {
				result = !result
			}

			return sqltypes.BoolValue(result), nil
		},
	}, nil
}

func (c compiler) compileInExpr(node *parser.InExpr) (CompiledExpr, error) {
	if node != nil && node.Query != nil {
		return c.compileSubquery(node)
	}

	expr, err := c.compile(node.Expr)
	if err != nil {
		return CompiledExpr{}, err
	}

	items := make([]CompiledExpr, 0, len(node.List))
	for _, item := range node.List {
		compiled, err := c.compile(item)
		if err != nil {
			return CompiledExpr{}, err
		}
		items = append(items, compiled)
	}

	desc, ok := c.typeOf(node)
	if !ok {
		nullable := isNullableType(expr.typ)
		for _, item := range items {
			nullable = nullable || isNullableType(item.typ)
		}
		desc = booleanDesc(nullable)
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			leftValue, err := expr.Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if leftValue.IsNull() {
				return sqltypes.NullValue(), nil
			}

			sawNull := false
			for _, item := range items {
				itemValue, err := item.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				if itemValue.IsNull() {
					sawNull = true
					continue
				}

				comparison, err := compareValues(leftValue, expr.typ, itemValue, item.typ)
				if err != nil {
					return sqltypes.Value{}, err
				}
				if comparison == 0 {
					return sqltypes.BoolValue(!node.Negated), nil
				}
			}

			if sawNull {
				return sqltypes.NullValue(), nil
			}

			return sqltypes.BoolValue(node.Negated), nil
		},
	}, nil
}

func (c compiler) compileLikeExpr(node *parser.LikeExpr) (CompiledExpr, error) {
	expr, err := c.compile(node.Expr)
	if err != nil {
		return CompiledExpr{}, err
	}
	pattern, err := c.compile(node.Pattern)
	if err != nil {
		return CompiledExpr{}, err
	}

	var escape CompiledExpr
	if node.Escape != nil {
		escape, err = c.compile(node.Escape)
		if err != nil {
			return CompiledExpr{}, err
		}
	}

	desc, ok := c.typeOf(node)
	if !ok {
		desc = booleanDesc(true)
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			exprValue, err := expr.Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			patternValue, err := pattern.Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if exprValue.IsNull() || patternValue.IsNull() {
				return sqltypes.NullValue(), nil
			}

			text, ok := exprValue.Raw().(string)
			if !ok {
				return sqltypes.Value{}, fmt.Errorf("%w: LIKE expression is %s", ErrInvalidExpressionType, exprValue.Kind())
			}
			likePattern, ok := patternValue.Raw().(string)
			if !ok {
				return sqltypes.Value{}, fmt.Errorf("%w: LIKE pattern is %s", ErrInvalidExpressionType, patternValue.Kind())
			}

			escapeText := ""
			if node.Escape != nil {
				escapeValue, err := escape.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				if escapeValue.IsNull() {
					return sqltypes.NullValue(), nil
				}

				rawEscape, ok := escapeValue.Raw().(string)
				if !ok {
					return sqltypes.Value{}, fmt.Errorf("%w: LIKE ESCAPE is %s", ErrInvalidExpressionType, escapeValue.Kind())
				}
				if rawEscape == "" {
					return sqltypes.Value{}, fmt.Errorf("%w: empty escape", ErrInvalidLikeEscape)
				}
				escapeText = rawEscape
			}

			matched, err := likeMatch(text, likePattern, escapeText)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if node.Negated {
				matched = !matched
			}

			return sqltypes.BoolValue(matched), nil
		},
	}, nil
}

func (c compiler) compileIsExpr(node *parser.IsExpr) (CompiledExpr, error) {
	expr, err := c.compile(node.Expr)
	if err != nil {
		return CompiledExpr{}, err
	}

	var right CompiledExpr
	if node.Right != nil {
		right, err = c.compile(node.Right)
		if err != nil {
			return CompiledExpr{}, err
		}
	}

	desc, ok := c.typeOf(node)
	if !ok {
		desc = booleanDesc(false)
	}

	predicate := strings.ToUpper(strings.TrimSpace(node.Predicate))
	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			leftValue, err := expr.Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}

			var result bool
			switch predicate {
			case "NULL":
				result = leftValue.IsNull()
			case "TRUE":
				result, err = isSQLBoolPredicate(leftValue, true)
			case "FALSE":
				result, err = isSQLBoolPredicate(leftValue, false)
			case "UNKNOWN":
				result = leftValue.IsNull()
			case "DISTINCT FROM":
				rightValue, err := right.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				switch {
				case leftValue.IsNull() && rightValue.IsNull():
					result = false
				case leftValue.IsNull() || rightValue.IsNull():
					result = true
				default:
					comparison, err := compareValues(leftValue, expr.typ, rightValue, right.typ)
					if err != nil {
						return sqltypes.Value{}, err
					}
					result = comparison != 0
				}
			default:
				return sqltypes.Value{}, fmt.Errorf("%w: IS %s", ErrUnsupportedExpression, node.Predicate)
			}
			if err != nil {
				return sqltypes.Value{}, err
			}
			if node.Negated {
				result = !result
			}

			return sqltypes.BoolValue(result), nil
		},
	}, nil
}

type compiledWhenClause struct {
	condition CompiledExpr
	result    CompiledExpr
}

type trimMode uint8

const (
	trimBoth trimMode = iota
	trimLeft
	trimRight
)

type roundMode uint8

const (
	roundHalfAwayFromZero roundMode = iota
	roundTowardZero
)

func (c compiler) typeOf(node parser.Node) (sqltypes.TypeDesc, bool) {
	if c.metadata == nil || node == nil {
		return sqltypes.TypeDesc{}, false
	}

	return c.metadata.TypeOf(node)
}

func (c compiler) bindingOf(node parser.Node) (OrdinalBinding, bool) {
	if c.metadata == nil || node == nil {
		return OrdinalBinding{}, false
	}

	return c.metadata.BindingOf(node)
}

func constantExpr(desc sqltypes.TypeDesc, value sqltypes.Value) CompiledExpr {
	return CompiledExpr{
		typ: desc,
		eval: func(Row) (sqltypes.Value, error) {
			return value, nil
		},
	}
}

func functionName(name *parser.QualifiedName) string {
	if name == nil || len(name.Parts) == 0 {
		return ""
	}

	return strings.ToUpper(strings.TrimSpace(name.Parts[len(name.Parts)-1].Name))
}

func parseIntegerLiteralText(text string) (*big.Int, error) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return nil, fmt.Errorf("%w: empty integer literal", ErrInvalidExpressionType)
	}

	base := 10
	unsigned := trimmed
	if strings.HasPrefix(unsigned, "+") || strings.HasPrefix(unsigned, "-") {
		unsigned = unsigned[1:]
	}
	if strings.HasPrefix(strings.ToLower(unsigned), "0x") {
		base = 0
	}

	parsed := new(big.Int)
	if _, ok := parsed.SetString(trimmed, base); !ok {
		return nil, fmt.Errorf("%w: integer literal %q", ErrInvalidExpressionType, text)
	}

	return parsed, nil
}

func (c compiler) compileAbsCall(desc sqltypes.TypeDesc, args []CompiledExpr) (CompiledExpr, error) {
	if len(args) != 1 {
		return CompiledExpr{}, fmt.Errorf("%w: ABS expects 1 argument", ErrUnsupportedExpression)
	}
	if isUnknownTypeDesc(desc) {
		desc = args[0].typ
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			value, err := args[0].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}

			return absNumericValue(value, args[0].typ, desc)
		},
	}, nil
}

func (c compiler) compileCaseFoldCall(
	desc sqltypes.TypeDesc,
	args []CompiledExpr,
	transform func(string) string,
) (CompiledExpr, error) {
	if len(args) != 1 {
		return CompiledExpr{}, fmt.Errorf("%w: case-fold expects 1 argument", ErrUnsupportedExpression)
	}
	if isUnknownTypeDesc(desc) {
		desc = args[0].typ
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			value, err := args[0].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if value.IsNull() {
				return sqltypes.NullValue(), nil
			}

			text, err := stringFromValue(value, "character function argument")
			if err != nil {
				return sqltypes.Value{}, err
			}

			return sqltypes.StringValue(transform(text)), nil
		},
	}, nil
}

func (c compiler) compileTrimCall(desc sqltypes.TypeDesc, args []CompiledExpr, mode trimMode) (CompiledExpr, error) {
	if len(args) == 0 || len(args) > 2 {
		return CompiledExpr{}, fmt.Errorf("%w: TRIM expects 1 or 2 arguments", ErrUnsupportedExpression)
	}
	if isUnknownTypeDesc(desc) {
		desc = args[0].typ
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			value, err := args[0].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if value.IsNull() {
				return sqltypes.NullValue(), nil
			}

			text, err := stringFromValue(value, "TRIM argument")
			if err != nil {
				return sqltypes.Value{}, err
			}

			cutset := " \t\r\n"
			if len(args) == 2 {
				cutValue, err := args[1].Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				if cutValue.IsNull() {
					return sqltypes.NullValue(), nil
				}

				cutset, err = stringFromValue(cutValue, "TRIM cutset")
				if err != nil {
					return sqltypes.Value{}, err
				}
			}

			switch mode {
			case trimLeft:
				text = strings.TrimLeft(text, cutset)
			case trimRight:
				text = strings.TrimRight(text, cutset)
			default:
				text = strings.Trim(text, cutset)
			}

			return sqltypes.StringValue(text), nil
		},
	}, nil
}

func (c compiler) compileSubstringCall(desc sqltypes.TypeDesc, args []CompiledExpr) (CompiledExpr, error) {
	if len(args) < 2 || len(args) > 3 {
		return CompiledExpr{}, fmt.Errorf("%w: SUBSTRING expects 2 or 3 arguments", ErrUnsupportedExpression)
	}
	if isUnknownTypeDesc(desc) {
		desc = args[0].typ
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			value, err := args[0].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if value.IsNull() {
				return sqltypes.NullValue(), nil
			}

			text, err := stringFromValue(value, "SUBSTRING text")
			if err != nil {
				return sqltypes.Value{}, err
			}

			startValue, err := args[1].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if startValue.IsNull() {
				return sqltypes.NullValue(), nil
			}

			start, err := int64FromNumericValue(startValue, args[1].typ, "SUBSTRING start")
			if err != nil {
				return sqltypes.Value{}, err
			}

			runes := []rune(text)
			startIndex := int(start - 1)
			if startIndex < 0 {
				startIndex = 0
			}
			if startIndex > len(runes) {
				startIndex = len(runes)
			}

			endIndex := len(runes)
			if len(args) == 3 {
				lengthValue, err := args[2].Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				if lengthValue.IsNull() {
					return sqltypes.NullValue(), nil
				}

				length, err := int64FromNumericValue(lengthValue, args[2].typ, "SUBSTRING length")
				if err != nil {
					return sqltypes.Value{}, err
				}
				if length <= 0 {
					return sqltypes.StringValue(""), nil
				}

				endIndex = startIndex + int(length)
				if endIndex > len(runes) {
					endIndex = len(runes)
				}
			}

			return sqltypes.StringValue(string(runes[startIndex:endIndex])), nil
		},
	}, nil
}

func (c compiler) compileConcatCall(desc sqltypes.TypeDesc, args []CompiledExpr) (CompiledExpr, error) {
	if len(args) == 0 {
		return CompiledExpr{}, fmt.Errorf("%w: CONCAT expects at least 1 argument", ErrUnsupportedExpression)
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			var builder strings.Builder
			for _, arg := range args {
				value, err := arg.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				if value.IsNull() {
					return sqltypes.NullValue(), nil
				}

				text, err := stringFromValue(value, "CONCAT argument")
				if err != nil {
					return sqltypes.Value{}, err
				}
				builder.WriteString(text)
			}

			return sqltypes.StringValue(builder.String()), nil
		},
	}, nil
}

func (c compiler) compileCharLengthCall(desc sqltypes.TypeDesc, args []CompiledExpr) (CompiledExpr, error) {
	if len(args) != 1 {
		return CompiledExpr{}, fmt.Errorf("%w: CHAR_LENGTH expects 1 argument", ErrUnsupportedExpression)
	}
	if isUnknownTypeDesc(desc) {
		desc = sqltypes.TypeDesc{Kind: sqltypes.TypeKindBigInt, Nullable: isNullableType(args[0].typ)}
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			value, err := args[0].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if value.IsNull() {
				return sqltypes.NullValue(), nil
			}

			text, err := stringFromValue(value, "CHAR_LENGTH argument")
			if err != nil {
				return sqltypes.Value{}, err
			}

			return sqltypes.Int64Value(int64(utf8.RuneCountInString(text))), nil
		},
	}, nil
}

func (c compiler) compileOctetLengthCall(desc sqltypes.TypeDesc, args []CompiledExpr) (CompiledExpr, error) {
	if len(args) != 1 {
		return CompiledExpr{}, fmt.Errorf("%w: OCTET_LENGTH expects 1 argument", ErrUnsupportedExpression)
	}
	if isUnknownTypeDesc(desc) {
		desc = sqltypes.TypeDesc{Kind: sqltypes.TypeKindBigInt, Nullable: isNullableType(args[0].typ)}
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			value, err := args[0].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if value.IsNull() {
				return sqltypes.NullValue(), nil
			}

			switch value.Kind() {
			case sqltypes.ValueKindString:
				return sqltypes.Int64Value(int64(len([]byte(value.Raw().(string))))), nil
			case sqltypes.ValueKindBytes:
				return sqltypes.Int64Value(int64(len(value.Raw().([]byte)))), nil
			default:
				return sqltypes.Value{}, fmt.Errorf(
					"%w: OCTET_LENGTH requires character or binary input, found %s",
					ErrInvalidExpressionType,
					value.Kind(),
				)
			}
		},
	}, nil
}

func (c compiler) compileCoalesceCall(desc sqltypes.TypeDesc, args []CompiledExpr) (CompiledExpr, error) {
	if len(args) == 0 {
		return CompiledExpr{}, fmt.Errorf("%w: COALESCE expects at least 1 argument", ErrUnsupportedExpression)
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			for _, arg := range args {
				value, err := arg.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				if value.IsNull() {
					continue
				}

				return coerceResultValue(value, arg.typ, desc)
			}

			return sqltypes.NullValue(), nil
		},
	}, nil
}

func (c compiler) compileNullIfCall(desc sqltypes.TypeDesc, args []CompiledExpr) (CompiledExpr, error) {
	if len(args) != 2 {
		return CompiledExpr{}, fmt.Errorf("%w: NULLIF expects 2 arguments", ErrUnsupportedExpression)
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			leftValue, err := args[0].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if leftValue.IsNull() {
				return sqltypes.NullValue(), nil
			}

			rightValue, err := args[1].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if rightValue.IsNull() {
				return coerceResultValue(leftValue, args[0].typ, desc)
			}

			comparison, err := compareValues(leftValue, args[0].typ, rightValue, args[1].typ)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if comparison == 0 {
				return sqltypes.NullValue(), nil
			}

			return coerceResultValue(leftValue, args[0].typ, desc)
		},
	}, nil
}

func (c compiler) compileExtremumCall(desc sqltypes.TypeDesc, args []CompiledExpr, greatest bool) (CompiledExpr, error) {
	if len(args) == 0 {
		return CompiledExpr{}, fmt.Errorf("%w: extremum expects at least 1 argument", ErrUnsupportedExpression)
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			bestValue, err := args[0].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if bestValue.IsNull() {
				return sqltypes.NullValue(), nil
			}
			bestType := args[0].typ

			for _, arg := range args[1:] {
				value, err := arg.Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				if value.IsNull() {
					return sqltypes.NullValue(), nil
				}

				comparison, err := compareValues(bestValue, bestType, value, arg.typ)
				if err != nil {
					return sqltypes.Value{}, err
				}
				if (greatest && comparison < 0) || (!greatest && comparison > 0) {
					bestValue = value
					bestType = arg.typ
				}
			}

			return coerceResultValue(bestValue, bestType, desc)
		},
	}, nil
}

func compileCurrentDateCall(desc sqltypes.TypeDesc, args []CompiledExpr) (CompiledExpr, error) {
	if len(args) != 0 {
		return CompiledExpr{}, fmt.Errorf("%w: CURRENT_DATE expects no arguments", ErrUnsupportedExpression)
	}
	if isUnknownTypeDesc(desc) {
		desc = sqltypes.TypeDesc{Kind: sqltypes.TypeKindDate}
	}

	return CompiledExpr{
		typ: desc,
		eval: func(Row) (sqltypes.Value, error) {
			now := time.Now().UTC()
			value := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
			return sqltypes.DateTimeValue(value), nil
		},
	}, nil
}

func compileCurrentTimeCall(desc sqltypes.TypeDesc, args []CompiledExpr) (CompiledExpr, error) {
	if len(args) != 0 {
		return CompiledExpr{}, fmt.Errorf("%w: CURRENT_TIME expects no arguments", ErrUnsupportedExpression)
	}
	if isUnknownTypeDesc(desc) {
		desc = sqltypes.TypeDesc{Kind: sqltypes.TypeKindTimeWithTimeZone}
	}

	return CompiledExpr{
		typ: desc,
		eval: func(Row) (sqltypes.Value, error) {
			now := time.Now()
			value := time.Date(
				1,
				time.January,
				1,
				now.Hour(),
				now.Minute(),
				now.Second(),
				truncateSubsecond(now.Nanosecond(), desc.Precision),
				now.Location(),
			)
			return sqltypes.DateTimeValue(value), nil
		},
	}, nil
}

func compileCurrentTimestampCall(desc sqltypes.TypeDesc, args []CompiledExpr) (CompiledExpr, error) {
	if len(args) != 0 {
		return CompiledExpr{}, fmt.Errorf("%w: CURRENT_TIMESTAMP expects no arguments", ErrUnsupportedExpression)
	}
	if isUnknownTypeDesc(desc) {
		desc = sqltypes.TypeDesc{Kind: sqltypes.TypeKindTimestampWithTimeZone}
	}

	return CompiledExpr{
		typ: desc,
		eval: func(Row) (sqltypes.Value, error) {
			now := time.Now()
			value := time.Date(
				now.Year(),
				now.Month(),
				now.Day(),
				now.Hour(),
				now.Minute(),
				now.Second(),
				truncateSubsecond(now.Nanosecond(), desc.Precision),
				now.Location(),
			)
			return sqltypes.DateTimeValue(value), nil
		},
	}, nil
}

func (c compiler) compilePositionCall(desc sqltypes.TypeDesc, args []CompiledExpr) (CompiledExpr, error) {
	if len(args) != 2 {
		return CompiledExpr{}, fmt.Errorf("%w: POSITION expects 2 arguments", ErrUnsupportedExpression)
	}
	if isUnknownTypeDesc(desc) {
		desc = sqltypes.TypeDesc{Kind: sqltypes.TypeKindBigInt, Nullable: true}
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			searchValue, err := args[0].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			textValue, err := args[1].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if searchValue.IsNull() || textValue.IsNull() {
				return sqltypes.NullValue(), nil
			}

			search, err := stringFromValue(searchValue, "POSITION search")
			if err != nil {
				return sqltypes.Value{}, err
			}
			text, err := stringFromValue(textValue, "POSITION text")
			if err != nil {
				return sqltypes.Value{}, err
			}

			offset := strings.Index(text, search)
			if offset < 0 {
				return sqltypes.Int64Value(0), nil
			}

			position := int64(utf8.RuneCountInString(text[:offset]) + 1)
			return sqltypes.Int64Value(position), nil
		},
	}, nil
}

func (c compiler) compileOverlayCall(desc sqltypes.TypeDesc, args []CompiledExpr) (CompiledExpr, error) {
	if len(args) < 3 || len(args) > 4 {
		return CompiledExpr{}, fmt.Errorf("%w: OVERLAY expects 3 or 4 arguments", ErrUnsupportedExpression)
	}
	if isUnknownTypeDesc(desc) {
		desc = args[0].typ
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			textValue, err := args[0].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			placingValue, err := args[1].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			startValue, err := args[2].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if textValue.IsNull() || placingValue.IsNull() || startValue.IsNull() {
				return sqltypes.NullValue(), nil
			}

			text, err := stringFromValue(textValue, "OVERLAY text")
			if err != nil {
				return sqltypes.Value{}, err
			}
			placing, err := stringFromValue(placingValue, "OVERLAY placing")
			if err != nil {
				return sqltypes.Value{}, err
			}
			start, err := int64FromNumericValue(startValue, args[2].typ, "OVERLAY start")
			if err != nil {
				return sqltypes.Value{}, err
			}

			textRunes := []rune(text)
			placingRunes := []rune(placing)
			startIndex := int(start - 1)
			if startIndex < 0 {
				startIndex = 0
			}
			if startIndex > len(textRunes) {
				startIndex = len(textRunes)
			}

			length := int64(len(placingRunes))
			if len(args) == 4 {
				lengthValue, err := args[3].Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				if lengthValue.IsNull() {
					return sqltypes.NullValue(), nil
				}

				length, err = int64FromNumericValue(lengthValue, args[3].typ, "OVERLAY length")
				if err != nil {
					return sqltypes.Value{}, err
				}
				if length < 0 {
					return sqltypes.Value{}, fmt.Errorf("%w: OVERLAY length %d", ErrInvalidExpressionType, length)
				}
			}

			endIndex := startIndex + int(length)
			if endIndex < startIndex {
				endIndex = startIndex
			}
			if endIndex > len(textRunes) {
				endIndex = len(textRunes)
			}

			result := string(textRunes[:startIndex]) + placing + string(textRunes[endIndex:])
			return coerceResultValue(sqltypes.StringValue(result), inferStringLiteralType(&parser.StringLiteral{Value: result}), desc)
		},
	}, nil
}

func (c compiler) compileRegexpLikeCall(desc sqltypes.TypeDesc, args []CompiledExpr) (CompiledExpr, error) {
	if len(args) < 2 || len(args) > 3 {
		return CompiledExpr{}, fmt.Errorf("%w: REGEXP_LIKE expects 2 or 3 arguments", ErrUnsupportedExpression)
	}
	if isUnknownTypeDesc(desc) {
		desc = booleanDesc(true)
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			text, matcher, err := evalRegexpMatchInputs(row, args)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if matcher == nil {
				return sqltypes.NullValue(), nil
			}

			return sqltypes.BoolValue(matcher.MatchString(text)), nil
		},
	}, nil
}

func (c compiler) compileRegexpReplaceCall(desc sqltypes.TypeDesc, args []CompiledExpr) (CompiledExpr, error) {
	if len(args) < 3 || len(args) > 4 {
		return CompiledExpr{}, fmt.Errorf("%w: REGEXP_REPLACE expects 3 or 4 arguments", ErrUnsupportedExpression)
	}
	if isUnknownTypeDesc(desc) {
		desc = args[0].typ
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			textValue, err := args[0].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			patternValue, err := args[1].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			replacementValue, err := args[2].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if textValue.IsNull() || patternValue.IsNull() || replacementValue.IsNull() {
				return sqltypes.NullValue(), nil
			}

			text, err := stringFromValue(textValue, "REGEXP_REPLACE text")
			if err != nil {
				return sqltypes.Value{}, err
			}
			pattern, err := stringFromValue(patternValue, "REGEXP_REPLACE pattern")
			if err != nil {
				return sqltypes.Value{}, err
			}
			replacement, err := stringFromValue(replacementValue, "REGEXP_REPLACE replacement")
			if err != nil {
				return sqltypes.Value{}, err
			}

			flags := ""
			if len(args) == 4 {
				flagsValue, err := args[3].Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				if flagsValue.IsNull() {
					return sqltypes.NullValue(), nil
				}

				flags, err = stringFromValue(flagsValue, "REGEXP_REPLACE flags")
				if err != nil {
					return sqltypes.Value{}, err
				}
			}

			matcher, err := compileRegexpPattern(pattern, flags)
			if err != nil {
				return sqltypes.Value{}, err
			}

			result := matcher.ReplaceAllString(text, replacement)
			return coerceResultValue(sqltypes.StringValue(result), inferStringLiteralType(&parser.StringLiteral{Value: result}), desc)
		},
	}, nil
}

func (c compiler) compileRegexpSubstrCall(desc sqltypes.TypeDesc, args []CompiledExpr) (CompiledExpr, error) {
	if len(args) < 2 || len(args) > 3 {
		return CompiledExpr{}, fmt.Errorf("%w: REGEXP_SUBSTR expects 2 or 3 arguments", ErrUnsupportedExpression)
	}
	if isUnknownTypeDesc(desc) {
		desc = args[0].typ
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			text, matcher, err := evalRegexpMatchInputs(row, args)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if matcher == nil {
				return sqltypes.NullValue(), nil
			}

			match := matcher.FindString(text)
			if match == "" && !matcher.MatchString(text) {
				return sqltypes.NullValue(), nil
			}

			return coerceResultValue(sqltypes.StringValue(match), inferStringLiteralType(&parser.StringLiteral{Value: match}), desc)
		},
	}, nil
}

func (c compiler) compileCeilFloorCall(
	name string,
	desc sqltypes.TypeDesc,
	args []CompiledExpr,
	ceil bool,
) (CompiledExpr, error) {
	if len(args) != 1 {
		return CompiledExpr{}, fmt.Errorf("%w: %s expects 1 argument", ErrUnsupportedExpression, name)
	}
	if isUnknownTypeDesc(desc) {
		desc = args[0].typ
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			value, err := args[0].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if value.IsNull() {
				return sqltypes.NullValue(), nil
			}

			source, ok := sourceTypeForValue(value, args[0].typ)
			if !ok {
				return sqltypes.Value{}, fmt.Errorf("%w: %s argument missing numeric type", ErrInvalidExpressionType, name)
			}
			if isApproximateNumericTypeKind(source.Kind) {
				floatValue, err := float64FromNumericValue(value, args[0].typ, name)
				if err != nil {
					return sqltypes.Value{}, err
				}
				if ceil {
					floatValue = math.Ceil(floatValue)
				} else {
					floatValue = math.Floor(floatValue)
				}
				return castApproximateResult(floatValue, desc)
			}

			decimal, err := decimalFromNumericValue(value, args[0].typ, name)
			if err != nil {
				return sqltypes.Value{}, err
			}

			result, err := ceilFloorDecimal(decimal, ceil)
			if err != nil {
				return sqltypes.Value{}, err
			}
			return castExactNumericResult(result, desc)
		},
	}, nil
}

func (c compiler) compileRoundCall(
	name string,
	desc sqltypes.TypeDesc,
	args []CompiledExpr,
	truncate bool,
) (CompiledExpr, error) {
	if len(args) < 1 || len(args) > 2 {
		return CompiledExpr{}, fmt.Errorf("%w: %s expects 1 or 2 arguments", ErrUnsupportedExpression, name)
	}
	if isUnknownTypeDesc(desc) {
		desc = args[0].typ
	}

	mode := roundHalfAwayFromZero
	if truncate {
		mode = roundTowardZero
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			value, err := args[0].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if value.IsNull() {
				return sqltypes.NullValue(), nil
			}

			scale := int64(0)
			if len(args) == 2 {
				scaleValue, err := args[1].Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				if scaleValue.IsNull() {
					return sqltypes.NullValue(), nil
				}

				scale, err = int64FromNumericValue(scaleValue, args[1].typ, name+" scale")
				if err != nil {
					return sqltypes.Value{}, err
				}
			}

			source, ok := sourceTypeForValue(value, args[0].typ)
			if !ok {
				return sqltypes.Value{}, fmt.Errorf("%w: %s argument missing numeric type", ErrInvalidExpressionType, name)
			}
			if isApproximateNumericTypeKind(source.Kind) {
				floatValue, err := float64FromNumericValue(value, args[0].typ, name)
				if err != nil {
					return sqltypes.Value{}, err
				}

				var result float64
				if truncate {
					result = truncateFloat(floatValue, scale)
				} else {
					result = roundFloat(floatValue, scale)
				}

				return castApproximateResult(result, desc)
			}

			decimal, err := decimalFromNumericValue(value, args[0].typ, name)
			if err != nil {
				return sqltypes.Value{}, err
			}

			result, err := roundDecimal(decimal, scale, mode)
			if err != nil {
				return sqltypes.Value{}, err
			}
			return castExactNumericResult(result, desc)
		},
	}, nil
}

func (c compiler) compileModCall(desc sqltypes.TypeDesc, args []CompiledExpr) (CompiledExpr, error) {
	if len(args) != 2 {
		return CompiledExpr{}, fmt.Errorf("%w: MOD expects 2 arguments", ErrUnsupportedExpression)
	}
	if isUnknownTypeDesc(desc) {
		desc = inferBinaryType("%", args[0].typ, args[1].typ)
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			leftValue, err := args[0].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			rightValue, err := args[1].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}

			return numericBinaryValue("%", leftValue, args[0].typ, rightValue, args[1].typ, desc)
		},
	}, nil
}

func (c compiler) compileApproxUnaryNumericCall(
	name string,
	desc sqltypes.TypeDesc,
	args []CompiledExpr,
	fn func(float64) float64,
) (CompiledExpr, error) {
	if len(args) != 1 {
		return CompiledExpr{}, fmt.Errorf("%w: %s expects 1 argument", ErrUnsupportedExpression, name)
	}
	if isUnknownTypeDesc(desc) {
		desc = sqltypes.TypeDesc{Kind: sqltypes.TypeKindDoublePrecision, Nullable: isNullableType(args[0].typ)}
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			value, err := args[0].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if value.IsNull() {
				return sqltypes.NullValue(), nil
			}

			floatValue, err := float64FromNumericValue(value, args[0].typ, name)
			if err != nil {
				return sqltypes.Value{}, err
			}

			return castApproximateResult(fn(floatValue), desc)
		},
	}, nil
}

func (c compiler) compileApproxBinaryNumericCall(
	name string,
	desc sqltypes.TypeDesc,
	args []CompiledExpr,
	fn func(float64, float64) float64,
) (CompiledExpr, error) {
	if len(args) != 2 {
		return CompiledExpr{}, fmt.Errorf("%w: %s expects 2 arguments", ErrUnsupportedExpression, name)
	}
	if isUnknownTypeDesc(desc) {
		desc = sqltypes.TypeDesc{Kind: sqltypes.TypeKindDoublePrecision, Nullable: isNullableType(args[0].typ) || isNullableType(args[1].typ)}
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			leftValue, err := args[0].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			rightValue, err := args[1].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if leftValue.IsNull() || rightValue.IsNull() {
				return sqltypes.NullValue(), nil
			}

			leftFloat, err := float64FromNumericValue(leftValue, args[0].typ, name)
			if err != nil {
				return sqltypes.Value{}, err
			}
			rightFloat, err := float64FromNumericValue(rightValue, args[1].typ, name)
			if err != nil {
				return sqltypes.Value{}, err
			}

			return castApproximateResult(fn(leftFloat, rightFloat), desc)
		},
	}, nil
}

func (c compiler) compileLogCall(desc sqltypes.TypeDesc, args []CompiledExpr) (CompiledExpr, error) {
	switch len(args) {
	case 1:
		return c.compileApproxUnaryNumericCall("LOG", desc, args, math.Log)
	case 2:
		if isUnknownTypeDesc(desc) {
			desc = sqltypes.TypeDesc{Kind: sqltypes.TypeKindDoublePrecision, Nullable: isNullableType(args[0].typ) || isNullableType(args[1].typ)}
		}

		return CompiledExpr{
			typ: desc,
			eval: func(row Row) (sqltypes.Value, error) {
				baseValue, err := args[0].Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				value, err := args[1].Eval(row)
				if err != nil {
					return sqltypes.Value{}, err
				}
				if baseValue.IsNull() || value.IsNull() {
					return sqltypes.NullValue(), nil
				}

				base, err := float64FromNumericValue(baseValue, args[0].typ, "LOG base")
				if err != nil {
					return sqltypes.Value{}, err
				}
				operand, err := float64FromNumericValue(value, args[1].typ, "LOG value")
				if err != nil {
					return sqltypes.Value{}, err
				}

				return castApproximateResult(math.Log(operand)/math.Log(base), desc)
			},
		}, nil
	default:
		return CompiledExpr{}, fmt.Errorf("%w: LOG expects 1 or 2 arguments", ErrUnsupportedExpression)
	}
}

func (c compiler) compileSignCall(desc sqltypes.TypeDesc, args []CompiledExpr) (CompiledExpr, error) {
	if len(args) != 1 {
		return CompiledExpr{}, fmt.Errorf("%w: SIGN expects 1 argument", ErrUnsupportedExpression)
	}
	if isUnknownTypeDesc(desc) {
		desc = args[0].typ
	}

	return CompiledExpr{
		typ: desc,
		eval: func(row Row) (sqltypes.Value, error) {
			value, err := args[0].Eval(row)
			if err != nil {
				return sqltypes.Value{}, err
			}
			if value.IsNull() {
				return sqltypes.NullValue(), nil
			}

			source, ok := sourceTypeForValue(value, args[0].typ)
			if !ok {
				return sqltypes.Value{}, fmt.Errorf("%w: SIGN argument missing numeric type", ErrInvalidExpressionType)
			}
			if isApproximateNumericTypeKind(source.Kind) {
				floatValue, err := float64FromNumericValue(value, args[0].typ, "SIGN")
				if err != nil {
					return sqltypes.Value{}, err
				}
				sign := 0.0
				switch {
				case floatValue > 0:
					sign = 1
				case floatValue < 0:
					sign = -1
				}
				return castApproximateResult(sign, desc)
			}

			decimal, err := decimalFromNumericValue(value, args[0].typ, "SIGN")
			if err != nil {
				return sqltypes.Value{}, err
			}

			result := sqltypes.NewDecimalFromInt64(int64(decimal.Sign()))
			return castExactNumericResult(result, desc)
		},
	}, nil
}

func compileRandomCall(desc sqltypes.TypeDesc, args []CompiledExpr) (CompiledExpr, error) {
	if len(args) != 0 {
		return CompiledExpr{}, fmt.Errorf("%w: RANDOM expects no arguments", ErrUnsupportedExpression)
	}
	if isUnknownTypeDesc(desc) {
		desc = sqltypes.TypeDesc{Kind: sqltypes.TypeKindDoublePrecision}
	}

	return CompiledExpr{
		typ: desc,
		eval: func(Row) (sqltypes.Value, error) {
			return castApproximateResult(rand.Float64(), desc)
		},
	}, nil
}

func evalRegexpMatchInputs(row Row, args []CompiledExpr) (string, *regexp.Regexp, error) {
	textValue, err := args[0].Eval(row)
	if err != nil {
		return "", nil, err
	}
	patternValue, err := args[1].Eval(row)
	if err != nil {
		return "", nil, err
	}
	if textValue.IsNull() || patternValue.IsNull() {
		return "", nil, nil
	}

	text, err := stringFromValue(textValue, "regexp text")
	if err != nil {
		return "", nil, err
	}
	pattern, err := stringFromValue(patternValue, "regexp pattern")
	if err != nil {
		return "", nil, err
	}

	flags := ""
	if len(args) == 3 {
		flagsValue, err := args[2].Eval(row)
		if err != nil {
			return "", nil, err
		}
		if flagsValue.IsNull() {
			return "", nil, nil
		}

		flags, err = stringFromValue(flagsValue, "regexp flags")
		if err != nil {
			return "", nil, err
		}
	}

	matcher, err := compileRegexpPattern(pattern, flags)
	if err != nil {
		return "", nil, err
	}

	return text, matcher, nil
}

func compileRegexpPattern(pattern, flags string) (*regexp.Regexp, error) {
	var inline strings.Builder
	seen := make(map[rune]struct{}, len(flags))
	for _, flag := range flags {
		if _, ok := seen[flag]; ok {
			continue
		}
		seen[flag] = struct{}{}

		switch flag {
		case 'i', 'm', 's':
			inline.WriteRune(flag)
		case 'c', 'g':
			// `c` keeps Go's default case-sensitive mode; `g` is a no-op because
			// replacement already runs globally.
		default:
			return nil, fmt.Errorf("%w: unsupported regexp flag %q", ErrInvalidExpressionType, string(flag))
		}
	}

	expr := pattern
	if inline.Len() > 0 {
		expr = "(?" + inline.String() + ")" + expr
	}

	return regexp.Compile(expr)
}

func castApproximateResult(value float64, target sqltypes.TypeDesc) (sqltypes.Value, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return sqltypes.Value{}, fmt.Errorf("%w: %s", sqltypes.ErrNonFiniteNumeric, target.Kind)
	}
	if isUnknownTypeDesc(target) {
		return sqltypes.Float64Value(value), nil
	}

	return castRuntimeValue(
		sqltypes.Float64Value(value),
		sqltypes.TypeDesc{Kind: sqltypes.TypeKindDoublePrecision},
		target,
		false,
	)
}

func castExactNumericResult(value sqltypes.Decimal, target sqltypes.TypeDesc) (sqltypes.Value, error) {
	if isUnknownTypeDesc(target) {
		return sqltypes.DecimalValue(value), nil
	}

	return castRuntimeValue(
		sqltypes.DecimalValue(value),
		sqltypes.TypeDesc{Kind: sqltypes.TypeKindNumeric},
		target,
		false,
	)
}

func decimalFromNumericValue(value sqltypes.Value, hint sqltypes.TypeDesc, context string) (sqltypes.Decimal, error) {
	source, ok := sourceTypeForValue(value, hint)
	if !ok {
		return sqltypes.Decimal{}, fmt.Errorf("%w: %s missing numeric type", ErrInvalidExpressionType, context)
	}

	decimalValue, err := castRuntimeValue(value, source, sqltypes.TypeDesc{Kind: sqltypes.TypeKindNumeric}, false)
	if err != nil {
		return sqltypes.Decimal{}, fmt.Errorf("%s: %w", context, err)
	}

	decimal, ok := decimalValue.Raw().(sqltypes.Decimal)
	if !ok {
		return sqltypes.Decimal{}, fmt.Errorf("%w: %s did not produce DECIMAL", ErrInvalidExpressionType, context)
	}

	return decimal, nil
}

func float64FromNumericValue(value sqltypes.Value, hint sqltypes.TypeDesc, context string) (float64, error) {
	source, ok := sourceTypeForValue(value, hint)
	if !ok {
		return 0, fmt.Errorf("%w: %s missing numeric type", ErrInvalidExpressionType, context)
	}

	floatValue, err := castRuntimeValue(value, source, sqltypes.TypeDesc{Kind: sqltypes.TypeKindDoublePrecision}, false)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", context, err)
	}

	return floatValue.Raw().(float64), nil
}

func ceilFloorDecimal(value sqltypes.Decimal, ceil bool) (sqltypes.Decimal, error) {
	normalized := sqltypes.DecimalValue(value).Raw().(sqltypes.Decimal)
	scale := normalized.Scale()
	if scale <= 0 {
		return normalized, nil
	}

	factor := pow10(int64(scale))
	quotient, remainder := new(big.Int).QuoRem(normalized.Coefficient(), factor, new(big.Int))
	if remainder.Sign() == 0 {
		return sqltypes.NewDecimal(quotient, 0)
	}
	if ceil && normalized.Sign() > 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if !ceil && normalized.Sign() < 0 {
		quotient.Sub(quotient, big.NewInt(1))
	}

	return sqltypes.NewDecimal(quotient, 0)
}

func roundDecimal(value sqltypes.Decimal, scale int64, mode roundMode) (sqltypes.Decimal, error) {
	normalized := sqltypes.DecimalValue(value).Raw().(sqltypes.Decimal)
	currentScale := int64(normalized.Scale())
	if scale >= currentScale {
		return normalized, nil
	}
	if int64(int32(scale)) != scale {
		return sqltypes.Decimal{}, fmt.Errorf("%w: rounding scale %d", ErrInvalidExpressionType, scale)
	}

	shift := currentScale - scale
	factor := pow10(shift)
	quotient, remainder := new(big.Int).QuoRem(normalized.Coefficient(), factor, new(big.Int))

	if mode == roundHalfAwayFromZero && remainder.Sign() != 0 {
		doubleRemainder := new(big.Int).Mul(new(big.Int).Abs(remainder), big.NewInt(2))
		if doubleRemainder.Cmp(factor) >= 0 {
			if normalized.Sign() >= 0 {
				quotient.Add(quotient, big.NewInt(1))
			} else {
				quotient.Sub(quotient, big.NewInt(1))
			}
		}
	}

	return sqltypes.NewDecimal(quotient, int32(scale))
}

func roundFloat(value float64, scale int64) float64 {
	if scale > 308 {
		return value
	}

	factor := pow10Float(scale)
	if factor == 0 {
		return 0
	}
	if factor < 1 {
		return math.Round(value/factor) * factor
	}

	return math.Round(value*factor) / factor
}

func truncateFloat(value float64, scale int64) float64 {
	if scale > 308 {
		return value
	}

	factor := pow10Float(scale)
	if factor == 0 {
		return 0
	}
	if factor < 1 {
		return math.Trunc(value/factor) * factor
	}

	return math.Trunc(value*factor) / factor
}

func pow10Float(exponent int64) float64 {
	switch {
	case exponent > 308:
		return math.Inf(1)
	case exponent < -323:
		return 0
	default:
		return math.Pow10(int(exponent))
	}
}

func isApproximateNumericTypeKind(kind sqltypes.TypeKind) bool {
	switch kind {
	case sqltypes.TypeKindReal, sqltypes.TypeKindDoublePrecision:
		return true
	default:
		return false
	}
}

func inferBinaryType(operator string, left, right sqltypes.TypeDesc) sqltypes.TypeDesc {
	switch operator {
	case "AND", "OR":
		return booleanDesc(isNullableType(left) || isNullableType(right))
	case "=", "!=", "<>", "<", "<=", ">", ">=", "BETWEEN", "IN", "LIKE":
		return booleanDesc(isNullableType(left) || isNullableType(right))
	case "+", "-", "*", "/", "%", "||":
		if desc, ok := sqltypes.CommonSuperType(left, right); ok {
			return desc
		}
	}

	return sqltypes.TypeDesc{}
}

func inferIntegerLiteralType(node *parser.IntegerLiteral) sqltypes.TypeDesc {
	if node == nil {
		return sqltypes.TypeDesc{}
	}

	parsed, err := parseIntegerLiteralText(node.Text)
	if err != nil {
		return sqltypes.TypeDesc{}
	}

	switch {
	case parsed.IsInt64() && parsed.Int64() >= math.MinInt32 && parsed.Int64() <= math.MaxInt32:
		return sqltypes.TypeDesc{Kind: sqltypes.TypeKindInteger}
	case parsed.IsInt64():
		return sqltypes.TypeDesc{Kind: sqltypes.TypeKindBigInt}
	default:
		return sqltypes.TypeDesc{
			Kind:      sqltypes.TypeKindNumeric,
			Precision: digitsForBigInt(parsed),
		}
	}
}

func inferFloatLiteralType(node *parser.FloatLiteral) sqltypes.TypeDesc {
	if node == nil {
		return sqltypes.TypeDesc{}
	}

	text := strings.TrimSpace(node.Text)
	if strings.HasPrefix(strings.ToLower(text), "0x") {
		return inferIntegerLiteralType(&parser.IntegerLiteral{Text: text})
	}

	decimal, err := sqltypes.ParseDecimal(text)
	if err != nil {
		return sqltypes.TypeDesc{Kind: sqltypes.TypeKindNumeric}
	}

	return decimalTypeDesc(decimal)
}

func inferStringLiteralType(node *parser.StringLiteral) sqltypes.TypeDesc {
	if node == nil {
		return sqltypes.TypeDesc{}
	}

	length := utf8.RuneCountInString(node.Value)
	if length < 1 {
		length = 1
	}

	return sqltypes.TypeDesc{
		Kind:   sqltypes.TypeKindVarChar,
		Length: uint32(length),
	}
}

func integerLiteralValue(text string, desc sqltypes.TypeDesc) (sqltypes.Value, error) {
	if isUnknownTypeDesc(desc) {
		desc = inferIntegerLiteralType(&parser.IntegerLiteral{Text: text})
	}

	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return sqltypes.Value{}, fmt.Errorf("%w: empty integer literal", ErrInvalidExpressionType)
	}

	switch desc.Kind {
	case sqltypes.TypeKindSmallInt, sqltypes.TypeKindInteger, sqltypes.TypeKindBigInt:
		parsed, err := parseIntegerLiteralText(trimmed)
		if err != nil {
			return sqltypes.Value{}, err
		}

		return castBigIntToType(parsed, desc)
	case sqltypes.TypeKindNumeric, sqltypes.TypeKindDecimal:
		if strings.HasPrefix(strings.ToLower(trimmed), "0x") {
			parsed, err := parseIntegerLiteralText(trimmed)
			if err != nil {
				return sqltypes.Value{}, err
			}
			decimal, err := sqltypes.NewDecimal(parsed, 0)
			if err != nil {
				return sqltypes.Value{}, err
			}
			return sqltypes.DecimalValue(decimal), nil
		}

		decimal, err := sqltypes.ParseDecimal(trimmed)
		if err != nil {
			return sqltypes.Value{}, err
		}

		return sqltypes.DecimalValue(decimal), nil
	case sqltypes.TypeKindReal, sqltypes.TypeKindDoublePrecision:
		return floatLiteralValue(trimmed, desc)
	default:
		return sqltypes.Value{}, fmt.Errorf("%w: integer literal cannot produce %s", ErrInvalidExpressionType, desc.Kind)
	}
}

func floatLiteralValue(text string, desc sqltypes.TypeDesc) (sqltypes.Value, error) {
	if isUnknownTypeDesc(desc) {
		desc = inferFloatLiteralType(&parser.FloatLiteral{Text: text})
	}

	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return sqltypes.Value{}, fmt.Errorf("%w: empty numeric literal", ErrInvalidExpressionType)
	}

	if strings.HasPrefix(strings.ToLower(trimmed), "0x") {
		return integerLiteralValue(trimmed, desc)
	}

	switch desc.Kind {
	case sqltypes.TypeKindReal:
		parsed, err := strconv.ParseFloat(trimmed, 32)
		if err != nil {
			return sqltypes.Value{}, err
		}
		return sqltypes.Float32Value(float32(parsed)), nil
	case sqltypes.TypeKindDoublePrecision:
		parsed, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return sqltypes.Value{}, err
		}
		return sqltypes.Float64Value(parsed), nil
	case sqltypes.TypeKindSmallInt, sqltypes.TypeKindInteger, sqltypes.TypeKindBigInt:
		decimal, err := sqltypes.ParseDecimal(trimmed)
		if err != nil {
			return sqltypes.Value{}, err
		}
		source := decimalTypeDesc(decimal)
		return castRuntimeValue(sqltypes.DecimalValue(decimal), source, desc, false)
	case sqltypes.TypeKindNumeric, sqltypes.TypeKindDecimal:
		decimal, err := sqltypes.ParseDecimal(trimmed)
		if err != nil {
			return sqltypes.Value{}, err
		}
		return sqltypes.DecimalValue(decimal), nil
	default:
		return sqltypes.Value{}, fmt.Errorf("%w: numeric literal cannot produce %s", ErrInvalidExpressionType, desc.Kind)
	}
}

func negateNumericValue(value sqltypes.Value, source, target sqltypes.TypeDesc) (sqltypes.Value, error) {
	resolvedTarget := target
	if isUnknownTypeDesc(resolvedTarget) {
		resolvedTarget = source
	}

	coerced, err := coerceRuntimeValue(value, source, resolvedTarget)
	if err != nil {
		return sqltypes.Value{}, err
	}

	switch coerced.Kind() {
	case sqltypes.ValueKindInt16, sqltypes.ValueKindInt32, sqltypes.ValueKindInt64:
		integer, err := bigIntFromValue(coerced)
		if err != nil {
			return sqltypes.Value{}, err
		}
		integer.Neg(integer)
		return castBigIntToType(integer, resolvedTarget)
	case sqltypes.ValueKindDecimal:
		decimal := coerced.Raw().(sqltypes.Decimal)
		coefficient := decimal.Coefficient()
		coefficient.Neg(coefficient)
		negated, err := sqltypes.NewDecimal(coefficient, decimal.Scale())
		if err != nil {
			return sqltypes.Value{}, err
		}
		return sqltypes.DecimalValue(negated), nil
	case sqltypes.ValueKindFloat32:
		return sqltypes.Float32Value(-coerced.Raw().(float32)), nil
	case sqltypes.ValueKindFloat64:
		return sqltypes.Float64Value(-coerced.Raw().(float64)), nil
	default:
		return sqltypes.Value{}, fmt.Errorf("%w: unary - requires numeric input, found %s", ErrInvalidExpressionType, coerced.Kind())
	}
}

func absNumericValue(value sqltypes.Value, source, target sqltypes.TypeDesc) (sqltypes.Value, error) {
	if value.IsNull() {
		return sqltypes.NullValue(), nil
	}

	resolvedTarget := target
	if isUnknownTypeDesc(resolvedTarget) {
		resolvedTarget = source
	}

	coerced, err := coerceRuntimeValue(value, source, resolvedTarget)
	if err != nil {
		return sqltypes.Value{}, err
	}

	switch coerced.Kind() {
	case sqltypes.ValueKindInt16, sqltypes.ValueKindInt32, sqltypes.ValueKindInt64:
		integer, err := bigIntFromValue(coerced)
		if err != nil {
			return sqltypes.Value{}, err
		}
		integer.Abs(integer)
		return castBigIntToType(integer, resolvedTarget)
	case sqltypes.ValueKindDecimal:
		decimal := coerced.Raw().(sqltypes.Decimal)
		coefficient := decimal.Coefficient()
		coefficient.Abs(coefficient)
		absolute, err := sqltypes.NewDecimal(coefficient, decimal.Scale())
		if err != nil {
			return sqltypes.Value{}, err
		}
		return sqltypes.DecimalValue(absolute), nil
	case sqltypes.ValueKindFloat32:
		result := float32(math.Abs(float64(coerced.Raw().(float32))))
		if math.IsNaN(float64(result)) || math.IsInf(float64(result), 0) {
			return sqltypes.Value{}, fmt.Errorf("%w: %s", sqltypes.ErrNonFiniteNumeric, coerced.Kind())
		}
		return sqltypes.Float32Value(result), nil
	case sqltypes.ValueKindFloat64:
		result := math.Abs(coerced.Raw().(float64))
		if math.IsNaN(result) || math.IsInf(result, 0) {
			return sqltypes.Value{}, fmt.Errorf("%w: %s", sqltypes.ErrNonFiniteNumeric, coerced.Kind())
		}
		return sqltypes.Float64Value(result), nil
	default:
		return sqltypes.Value{}, fmt.Errorf("%w: ABS requires numeric input, found %s", ErrInvalidExpressionType, coerced.Kind())
	}
}

func numericBinaryValue(
	operator string,
	leftValue sqltypes.Value,
	leftType sqltypes.TypeDesc,
	rightValue sqltypes.Value,
	rightType sqltypes.TypeDesc,
	target sqltypes.TypeDesc,
) (sqltypes.Value, error) {
	if leftValue.IsNull() || rightValue.IsNull() {
		return sqltypes.NullValue(), nil
	}

	resolvedTarget, err := resolveNumericTargetType(leftType, rightType, target)
	if err != nil {
		return sqltypes.Value{}, err
	}

	leftCoerced, err := coerceRuntimeValue(leftValue, leftType, resolvedTarget)
	if err != nil {
		return sqltypes.Value{}, err
	}
	rightCoerced, err := coerceRuntimeValue(rightValue, rightType, resolvedTarget)
	if err != nil {
		return sqltypes.Value{}, err
	}

	switch resolvedTarget.Kind {
	case sqltypes.TypeKindSmallInt, sqltypes.TypeKindInteger, sqltypes.TypeKindBigInt:
		return exactIntegerBinaryValue(operator, leftCoerced, rightCoerced, resolvedTarget)
	case sqltypes.TypeKindNumeric, sqltypes.TypeKindDecimal:
		return decimalBinaryValue(operator, leftCoerced, rightCoerced, resolvedTarget)
	case sqltypes.TypeKindReal, sqltypes.TypeKindDoublePrecision:
		return approximateBinaryValue(operator, leftCoerced, rightCoerced, resolvedTarget)
	default:
		return sqltypes.Value{}, fmt.Errorf("%w: operator %s requires numeric operands, found %s", ErrInvalidExpressionType, operator, resolvedTarget.Kind)
	}
}

func exactIntegerBinaryValue(operator string, left, right sqltypes.Value, target sqltypes.TypeDesc) (sqltypes.Value, error) {
	leftInt, err := bigIntFromValue(left)
	if err != nil {
		return sqltypes.Value{}, err
	}
	rightInt, err := bigIntFromValue(right)
	if err != nil {
		return sqltypes.Value{}, err
	}
	if rightInt.Sign() == 0 && (operator == "/" || operator == "%") {
		return sqltypes.Value{}, ErrDivisionByZero
	}

	result := new(big.Int)
	switch operator {
	case "+":
		result.Add(leftInt, rightInt)
	case "-":
		result.Sub(leftInt, rightInt)
	case "*":
		result.Mul(leftInt, rightInt)
	case "/":
		result.Quo(leftInt, rightInt)
	case "%":
		result.Rem(leftInt, rightInt)
	default:
		return sqltypes.Value{}, fmt.Errorf("%w: integer operator %s", ErrUnsupportedExpression, operator)
	}

	return castBigIntToType(result, target)
}

func decimalBinaryValue(operator string, left, right sqltypes.Value, target sqltypes.TypeDesc) (sqltypes.Value, error) {
	leftDecimal, ok := left.Raw().(sqltypes.Decimal)
	if !ok {
		return sqltypes.Value{}, fmt.Errorf("%w: expected DECIMAL left operand, found %s", ErrInvalidExpressionType, left.Kind())
	}
	rightDecimal, ok := right.Raw().(sqltypes.Decimal)
	if !ok {
		return sqltypes.Value{}, fmt.Errorf("%w: expected DECIMAL right operand, found %s", ErrInvalidExpressionType, right.Kind())
	}

	leftCoeff, rightCoeff, scale := alignDecimalCoefficients(leftDecimal, rightDecimal)

	var (
		coefficient *big.Int
		resultScale int32
	)

	switch operator {
	case "+":
		coefficient = new(big.Int).Add(leftCoeff, rightCoeff)
		resultScale = scale
	case "-":
		coefficient = new(big.Int).Sub(leftCoeff, rightCoeff)
		resultScale = scale
	case "*":
		coefficient = new(big.Int).Mul(leftDecimal.Coefficient(), rightDecimal.Coefficient())
		resultScale = leftDecimal.Scale() + rightDecimal.Scale()
	case "/":
		if rightDecimal.Sign() == 0 {
			return sqltypes.Value{}, ErrDivisionByZero
		}
		resultScale = int32(target.Scale)
		if resultScale < 0 {
			resultScale = 0
		}
		numerator := new(big.Int).Mul(leftDecimal.Coefficient(), pow10(int64(resultScale)+int64(rightDecimal.Scale())))
		denominator := new(big.Int).Mul(rightDecimal.Coefficient(), pow10(int64(leftDecimal.Scale())))
		coefficient = numerator.Quo(numerator, denominator)
	case "%":
		if rightDecimal.Sign() == 0 {
			return sqltypes.Value{}, ErrDivisionByZero
		}
		coefficient = new(big.Int).Rem(leftCoeff, rightCoeff)
		resultScale = scale
	default:
		return sqltypes.Value{}, fmt.Errorf("%w: decimal operator %s", ErrUnsupportedExpression, operator)
	}

	decimal, err := sqltypes.NewDecimal(coefficient, resultScale)
	if err != nil {
		return sqltypes.Value{}, err
	}

	source := decimalTypeDesc(decimal)
	if source.Kind == sqltypes.TypeKindInvalid {
		source = sqltypes.TypeDesc{Kind: sqltypes.TypeKindNumeric}
	}

	return castRuntimeValue(sqltypes.DecimalValue(decimal), source, target, false)
}

func approximateBinaryValue(operator string, left, right sqltypes.Value, target sqltypes.TypeDesc) (sqltypes.Value, error) {
	leftFloat, err := float64FromValue(left)
	if err != nil {
		return sqltypes.Value{}, err
	}
	rightFloat, err := float64FromValue(right)
	if err != nil {
		return sqltypes.Value{}, err
	}
	if rightFloat == 0 && (operator == "/" || operator == "%") {
		return sqltypes.Value{}, ErrDivisionByZero
	}

	var result float64
	switch operator {
	case "+":
		result = leftFloat + rightFloat
	case "-":
		result = leftFloat - rightFloat
	case "*":
		result = leftFloat * rightFloat
	case "/":
		result = leftFloat / rightFloat
	case "%":
		result = math.Mod(leftFloat, rightFloat)
	default:
		return sqltypes.Value{}, fmt.Errorf("%w: floating operator %s", ErrUnsupportedExpression, operator)
	}
	if math.IsNaN(result) || math.IsInf(result, 0) {
		return sqltypes.Value{}, fmt.Errorf("%w: %s", sqltypes.ErrNonFiniteNumeric, target.Kind)
	}

	switch target.Kind {
	case sqltypes.TypeKindReal:
		return sqltypes.Float32Value(float32(result)), nil
	case sqltypes.TypeKindDoublePrecision:
		return sqltypes.Float64Value(result), nil
	default:
		return sqltypes.Value{}, fmt.Errorf("%w: unsupported approximate target %s", ErrInvalidExpressionType, target.Kind)
	}
}

func concatValues(leftValue sqltypes.Value, leftType sqltypes.TypeDesc, rightValue sqltypes.Value, rightType sqltypes.TypeDesc) (sqltypes.Value, error) {
	if leftValue.IsNull() || rightValue.IsNull() {
		return sqltypes.NullValue(), nil
	}

	leftSource, leftOK := sourceTypeForValue(leftValue, leftType)
	rightSource, rightOK := sourceTypeForValue(rightValue, rightType)
	if leftOK && rightOK {
		if common, ok := sqltypes.CommonSuperType(leftSource, rightSource); ok && !isUnknownTypeDesc(common) {
			var err error
			leftValue, err = castRuntimeValue(leftValue, leftSource, common, false)
			if err != nil {
				return sqltypes.Value{}, err
			}
			rightValue, err = castRuntimeValue(rightValue, rightSource, common, false)
			if err != nil {
				return sqltypes.Value{}, err
			}
		}
	}

	switch {
	case leftValue.Kind() == sqltypes.ValueKindString && rightValue.Kind() == sqltypes.ValueKindString:
		return sqltypes.StringValue(leftValue.Raw().(string) + rightValue.Raw().(string)), nil
	case leftValue.Kind() == sqltypes.ValueKindBytes && rightValue.Kind() == sqltypes.ValueKindBytes:
		bytes := append(append([]byte(nil), leftValue.Raw().([]byte)...), rightValue.Raw().([]byte)...)
		return sqltypes.BytesValue(bytes), nil
	default:
		return sqltypes.Value{}, fmt.Errorf(
			"%w: operator || requires matching character or binary operands, found %s and %s",
			ErrInvalidExpressionType,
			leftValue.Kind(),
			rightValue.Kind(),
		)
	}
}

func compareValues(leftValue sqltypes.Value, leftType sqltypes.TypeDesc, rightValue sqltypes.Value, rightType sqltypes.TypeDesc) (int, error) {
	leftSource, leftOK := sourceTypeForValue(leftValue, leftType)
	rightSource, rightOK := sourceTypeForValue(rightValue, rightType)
	if leftOK && rightOK {
		if common, ok := sqltypes.CommonSuperType(leftSource, rightSource); ok && !isUnknownTypeDesc(common) {
			var err error
			leftValue, err = castRuntimeValue(leftValue, leftSource, common, false)
			if err != nil {
				return 0, err
			}
			rightValue, err = castRuntimeValue(rightValue, rightSource, common, false)
			if err != nil {
				return 0, err
			}
		}
	}

	return leftValue.Compare(rightValue)
}

// CompareValues compares two values using the executor's SQL runtime
// comparison rules for the supplied analyzed types.
func CompareValues(
	leftValue sqltypes.Value,
	leftType sqltypes.TypeDesc,
	rightValue sqltypes.Value,
	rightType sqltypes.TypeDesc,
) (int, error) {
	return compareValues(leftValue, leftType, rightValue, rightType)
}

func applyComparisonOperator(operator string, comparison int) bool {
	switch operator {
	case "=":
		return comparison == 0
	case "!=", "<>":
		return comparison != 0
	case "<":
		return comparison < 0
	case "<=":
		return comparison <= 0
	case ">":
		return comparison > 0
	case ">=":
		return comparison >= 0
	default:
		return false
	}
}

func resolveNumericTargetType(left, right, target sqltypes.TypeDesc) (sqltypes.TypeDesc, error) {
	if !isUnknownTypeDesc(target) {
		return target, nil
	}

	common, ok := sqltypes.CommonSuperType(left, right)
	if !ok || isUnknownTypeDesc(common) {
		return sqltypes.TypeDesc{}, fmt.Errorf("%w: missing numeric result type", ErrInvalidExpressionType)
	}

	return common, nil
}

func coerceRuntimeValue(value sqltypes.Value, source, target sqltypes.TypeDesc) (sqltypes.Value, error) {
	if value.IsNull() {
		return sqltypes.NullValue(), nil
	}
	if isUnknownTypeDesc(target) {
		return value, nil
	}

	resolvedSource, ok := sourceTypeForValue(value, source)
	if !ok {
		return sqltypes.Value{}, fmt.Errorf("%w: missing source type for %s", ErrInvalidExpressionType, value.Kind())
	}
	if sameTypeDesc(resolvedSource, target) {
		return value, nil
	}

	return castRuntimeValue(value, resolvedSource, target, false)
}

func castRuntimeValue(value sqltypes.Value, source, target sqltypes.TypeDesc, try bool) (sqltypes.Value, error) {
	if try {
		return sqltypes.TryCast(value, source, target)
	}

	return sqltypes.Cast(value, source, target)
}

func sourceTypeForValue(value sqltypes.Value, hint sqltypes.TypeDesc) (sqltypes.TypeDesc, bool) {
	if !isUnknownTypeDesc(hint) {
		return hint, true
	}
	if value.IsNull() {
		return sqltypes.TypeDesc{}, true
	}

	switch value.Kind() {
	case sqltypes.ValueKindBool:
		return sqltypes.TypeDesc{Kind: sqltypes.TypeKindBoolean}, true
	case sqltypes.ValueKindInt16:
		return sqltypes.TypeDesc{Kind: sqltypes.TypeKindSmallInt}, true
	case sqltypes.ValueKindInt32:
		return sqltypes.TypeDesc{Kind: sqltypes.TypeKindInteger}, true
	case sqltypes.ValueKindInt64:
		return sqltypes.TypeDesc{Kind: sqltypes.TypeKindBigInt}, true
	case sqltypes.ValueKindFloat32:
		return sqltypes.TypeDesc{Kind: sqltypes.TypeKindReal}, true
	case sqltypes.ValueKindFloat64:
		return sqltypes.TypeDesc{Kind: sqltypes.TypeKindDoublePrecision}, true
	case sqltypes.ValueKindString:
		text := value.Raw().(string)
		length := utf8.RuneCountInString(text)
		if length < 1 {
			length = 1
		}
		return sqltypes.TypeDesc{Kind: sqltypes.TypeKindVarChar, Length: uint32(length)}, true
	case sqltypes.ValueKindBytes:
		length := len(value.Raw().([]byte))
		if length < 1 {
			length = 1
		}
		return sqltypes.TypeDesc{Kind: sqltypes.TypeKindVarBinary, Length: uint32(length)}, true
	case sqltypes.ValueKindDecimal:
		return decimalTypeDesc(value.Raw().(sqltypes.Decimal)), true
	case sqltypes.ValueKindTimeOfDay:
		return sqltypes.TypeDesc{Kind: sqltypes.TypeKindTime}, true
	case sqltypes.ValueKindInterval:
		return sqltypes.TypeDesc{Kind: sqltypes.TypeKindInterval}, true
	case sqltypes.ValueKindArray:
		return sqltypes.TypeDesc{Kind: sqltypes.TypeKindArray}, true
	case sqltypes.ValueKindRow:
		return sqltypes.TypeDesc{Kind: sqltypes.TypeKindRow}, true
	default:
		return sqltypes.TypeDesc{}, false
	}
}

func sameTypeDesc(left, right sqltypes.TypeDesc) bool {
	return left.Kind == right.Kind &&
		left.Precision == right.Precision &&
		left.Scale == right.Scale &&
		left.Length == right.Length
}

func coerceResultValue(value sqltypes.Value, source, target sqltypes.TypeDesc) (sqltypes.Value, error) {
	if value.IsNull() || isUnknownTypeDesc(target) {
		return value, nil
	}

	return coerceRuntimeValue(value, source, target)
}

func stringFromValue(value sqltypes.Value, context string) (string, error) {
	raw, ok := value.Raw().(string)
	if !ok {
		return "", fmt.Errorf("%w: %s is %s", ErrInvalidExpressionType, context, value.Kind())
	}

	return raw, nil
}

func int64FromNumericValue(value sqltypes.Value, hint sqltypes.TypeDesc, context string) (int64, error) {
	source, ok := sourceTypeForValue(value, hint)
	if !ok {
		return 0, fmt.Errorf("%w: %s missing numeric type", ErrInvalidExpressionType, context)
	}

	integer, err := castRuntimeValue(value, source, sqltypes.TypeDesc{Kind: sqltypes.TypeKindBigInt}, false)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", context, err)
	}

	return integer.Raw().(int64), nil
}

func decimalTypeDesc(decimal sqltypes.Decimal) sqltypes.TypeDesc {
	text := decimal.String()

	scale := uint32(0)
	precision := uint32(0)
	if strings.Contains(text, ".") {
		parts := strings.SplitN(text, ".", 2)
		scale = uint32(len(parts[1]))
		precision = uint32(len(strings.TrimPrefix(parts[0], "-")) + len(parts[1]))
	} else {
		precision = uint32(len(strings.TrimPrefix(text, "-")))
	}
	if precision == 0 {
		precision = 1
	}

	return sqltypes.TypeDesc{
		Kind:      sqltypes.TypeKindNumeric,
		Precision: precision,
		Scale:     scale,
	}
}

func truncateSubsecond(nanos int, precision uint32) int {
	unit := precisionUnitNanos(precision)
	if unit <= 1 {
		return nanos
	}

	return nanos - (nanos % unit)
}

func precisionUnitNanos(precision uint32) int {
	precision = clampTemporalPrecision(precision)
	unit := 1
	for digits := precision; digits < 9; digits++ {
		unit *= 10
	}

	return unit
}

func clampTemporalPrecision(precision uint32) uint32 {
	if precision > 9 {
		return 9
	}

	return precision
}

func castBigIntToType(value *big.Int, target sqltypes.TypeDesc) (sqltypes.Value, error) {
	if target.Kind == sqltypes.TypeKindNumeric || target.Kind == sqltypes.TypeKindDecimal {
		decimal, err := sqltypes.NewDecimal(value, 0)
		if err != nil {
			return sqltypes.Value{}, err
		}
		return castRuntimeValue(
			sqltypes.DecimalValue(decimal),
			sqltypes.TypeDesc{Kind: sqltypes.TypeKindNumeric},
			target,
			false,
		)
	}
	if !value.IsInt64() {
		return sqltypes.Value{}, fmt.Errorf("%w: integer result exceeds BIGINT", sqltypes.ErrCastOverflow)
	}

	return castRuntimeValue(
		sqltypes.Int64Value(value.Int64()),
		sqltypes.TypeDesc{Kind: sqltypes.TypeKindBigInt},
		target,
		false,
	)
}

func bigIntFromValue(value sqltypes.Value) (*big.Int, error) {
	switch value.Kind() {
	case sqltypes.ValueKindInt16:
		return big.NewInt(int64(value.Raw().(int16))), nil
	case sqltypes.ValueKindInt32:
		return big.NewInt(int64(value.Raw().(int32))), nil
	case sqltypes.ValueKindInt64:
		return big.NewInt(value.Raw().(int64)), nil
	default:
		return nil, fmt.Errorf("%w: expected integer, found %s", ErrInvalidExpressionType, value.Kind())
	}
}

func float64FromValue(value sqltypes.Value) (float64, error) {
	switch value.Kind() {
	case sqltypes.ValueKindFloat32:
		return float64(value.Raw().(float32)), nil
	case sqltypes.ValueKindFloat64:
		return value.Raw().(float64), nil
	default:
		return 0, fmt.Errorf("%w: expected floating-point value, found %s", ErrInvalidExpressionType, value.Kind())
	}
}

func alignDecimalCoefficients(left, right sqltypes.Decimal) (*big.Int, *big.Int, int32) {
	leftScale := left.Scale()
	rightScale := right.Scale()
	scale := leftScale
	if rightScale > scale {
		scale = rightScale
	}

	leftCoefficient := left.Coefficient()
	leftCoefficient.Mul(leftCoefficient, pow10(int64(scale-leftScale)))

	rightCoefficient := right.Coefficient()
	rightCoefficient.Mul(rightCoefficient, pow10(int64(scale-rightScale)))

	return leftCoefficient, rightCoefficient, scale
}

func isSQLBoolPredicate(value sqltypes.Value, want bool) (bool, error) {
	if value.IsNull() {
		return false, nil
	}
	raw, ok := value.Raw().(bool)
	if !ok {
		return false, fmt.Errorf("%w: expected BOOLEAN, found %s", ErrInvalidExpressionType, value.Kind())
	}

	return raw == want, nil
}

type sqlBool uint8

const (
	sqlUnknown sqlBool = iota
	sqlFalse
	sqlTrue
)

func sqlBoolFromValue(value sqltypes.Value) (sqlBool, error) {
	if value.IsNull() {
		return sqlUnknown, nil
	}

	raw, ok := value.Raw().(bool)
	if !ok {
		return sqlUnknown, fmt.Errorf("%w: expected BOOLEAN, found %s", ErrInvalidExpressionType, value.Kind())
	}
	if raw {
		return sqlTrue, nil
	}

	return sqlFalse, nil
}

func valueFromSQLBool(value sqlBool) sqltypes.Value {
	switch value {
	case sqlTrue:
		return sqltypes.BoolValue(true)
	case sqlFalse:
		return sqltypes.BoolValue(false)
	default:
		return sqltypes.NullValue()
	}
}

func notSQLBool(value sqlBool) sqlBool {
	switch value {
	case sqlTrue:
		return sqlFalse
	case sqlFalse:
		return sqlTrue
	default:
		return sqlUnknown
	}
}

func andSQLBool(left, right sqlBool) sqlBool {
	switch {
	case left == sqlFalse || right == sqlFalse:
		return sqlFalse
	case left == sqlTrue && right == sqlTrue:
		return sqlTrue
	default:
		return sqlUnknown
	}
}

func orSQLBool(left, right sqlBool) sqlBool {
	switch {
	case left == sqlTrue || right == sqlTrue:
		return sqlTrue
	case left == sqlFalse && right == sqlFalse:
		return sqlFalse
	default:
		return sqlUnknown
	}
}

func booleanDesc(nullable bool) sqltypes.TypeDesc {
	return sqltypes.TypeDesc{
		Kind:     sqltypes.TypeKindBoolean,
		Nullable: nullable,
	}
}

func isNullableType(desc sqltypes.TypeDesc) bool {
	return isUnknownTypeDesc(desc) || desc.Nullable
}

func isUnknownTypeDesc(desc sqltypes.TypeDesc) bool {
	return desc == (sqltypes.TypeDesc{})
}

func likeMatch(text, pattern, escapeText string) (bool, error) {
	var escape rune
	hasEscape := false
	if escapeText != "" {
		runes := []rune(escapeText)
		if len(runes) != 1 {
			return false, fmt.Errorf("%w: %q", ErrInvalidLikeEscape, escapeText)
		}
		escape = runes[0]
		hasEscape = true
	}

	var (
		builder  strings.Builder
		escaping bool
	)
	builder.WriteString("(?s)^")

	for _, r := range pattern {
		switch {
		case escaping:
			builder.WriteString(regexp.QuoteMeta(string(r)))
			escaping = false
		case hasEscape && r == escape:
			escaping = true
		case r == '%':
			builder.WriteString(".*")
		case r == '_':
			builder.WriteString(".")
		default:
			builder.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	if escaping {
		return false, fmt.Errorf("%w: dangling escape in %q", ErrInvalidLikeEscape, pattern)
	}

	builder.WriteString("$")
	matcher, err := regexp.Compile(builder.String())
	if err != nil {
		return false, err
	}

	return matcher.MatchString(text), nil
}

func typeDescFromTypeName(node *parser.TypeName) (sqltypes.TypeDesc, error) {
	if node == nil {
		return sqltypes.TypeDesc{}, fmt.Errorf("%w: missing type name", sqltypes.ErrInvalidTypeDesc)
	}
	if node.Qualifier != nil {
		return sqltypes.TypeDesc{}, fmt.Errorf(
			"%w: qualified type name is not supported in Phase 1",
			sqltypes.ErrInvalidTypeDesc,
		)
	}
	if len(node.Names) == 0 {
		return sqltypes.TypeDesc{}, fmt.Errorf("%w: missing type name", sqltypes.ErrInvalidTypeDesc)
	}

	parts := make([]string, 0, len(node.Names))
	for _, name := range node.Names {
		if name == nil || strings.TrimSpace(name.Name) == "" {
			return sqltypes.TypeDesc{}, fmt.Errorf("%w: missing type name", sqltypes.ErrInvalidTypeDesc)
		}
		parts = append(parts, name.Name)
	}

	text := canonicalTypeNameText(strings.Join(parts, " "))
	if len(node.Args) > 0 {
		args := make([]string, 0, len(node.Args))
		for _, arg := range node.Args {
			rendered, err := renderTypeArgument(arg)
			if err != nil {
				return sqltypes.TypeDesc{}, err
			}
			args = append(args, rendered)
		}
		text += "(" + strings.Join(args, ",") + ")"
	}

	return sqltypes.ParseTypeDesc(text)
}

func canonicalTypeNameText(text string) string {
	switch strings.ToUpper(strings.TrimSpace(text)) {
	case "CHARACTER":
		return "CHAR"
	case "CHARACTER VARYING":
		return "VARCHAR"
	default:
		return text
	}
}

func renderTypeArgument(node parser.Node) (string, error) {
	switch node := node.(type) {
	case *parser.IntegerLiteral:
		return node.Text, nil
	case *parser.FloatLiteral:
		return node.Text, nil
	case *parser.UnaryExpr:
		switch node.Operator {
		case "+", "-":
			value, err := renderTypeArgument(node.Operand)
			if err != nil {
				return "", err
			}
			return node.Operator + value, nil
		default:
			return "", fmt.Errorf("%w: unsupported unary operator %q in type argument", sqltypes.ErrInvalidTypeDesc, node.Operator)
		}
	default:
		return "", fmt.Errorf("%w: unsupported type argument %T", sqltypes.ErrInvalidTypeDesc, node)
	}
}

func digitsForBigInt(value *big.Int) uint32 {
	if value == nil {
		return 1
	}

	text := new(big.Int).Abs(value).String()
	if text == "" {
		return 1
	}

	return uint32(len(text))
}

func pow10(exponent int64) *big.Int {
	if exponent <= 0 {
		return big.NewInt(1)
	}

	base := big.NewInt(10)
	return new(big.Int).Exp(base, big.NewInt(exponent), nil)
}
