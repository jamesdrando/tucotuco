package executor

import (
	"errors"
	"strings"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/parser"
	"github.com/jamesdrando/tucotuco/internal/types"
)

func TestCompileExprBoundReferencesByOrdinal(t *testing.T) {
	t.Parallel()

	id := identifier("id")
	name := qualifiedName("items", "name")
	item := &parser.SelectItem{Expr: name}
	stringType := mustTypeDesc(t, "VARCHAR(5) NOT NULL")

	metadata := Metadata{
		Types: map[parser.Node]types.TypeDesc{
			id: {Kind: types.TypeKindInteger},
		},
		Bindings: map[parser.Node]OrdinalBinding{
			id:   {Ordinal: 0, Type: types.TypeDesc{Kind: types.TypeKindInteger}},
			name: {Ordinal: 1, Type: stringType},
		},
	}

	row := NewRow(types.Int32Value(7), types.StringValue("alpha"))

	compiledID := mustCompileExpr(t, id, metadata)
	if compiledID.Type() != (types.TypeDesc{Kind: types.TypeKindInteger}) {
		t.Fatalf("identifier Type() = %#v, want %#v", compiledID.Type(), types.TypeDesc{Kind: types.TypeKindInteger})
	}
	assertEvalValue(t, compiledID, row, types.Int32Value(7))

	compiledItem := mustCompileExpr(t, item, metadata)
	if compiledItem.Type() != stringType {
		t.Fatalf("select item Type() = %#v, want %#v", compiledItem.Type(), stringType)
	}
	assertEvalValue(t, compiledItem, row, types.StringValue("alpha"))
}

func TestCompileExprLiterals(t *testing.T) {
	t.Parallel()

	nullNode := &parser.NullLiteral{}
	nullType := mustTypeDesc(t, "BOOLEAN")

	testCases := []struct {
		name     string
		node     parser.Node
		metadata ExpressionMetadata
		want     types.Value
		wantType types.TypeDesc
	}{
		{
			name:     "integer literal infers INTEGER",
			node:     &parser.IntegerLiteral{Text: "42"},
			want:     types.Int32Value(42),
			wantType: types.TypeDesc{Kind: types.TypeKindInteger},
		},
		{
			name:     "leading-zero integer literal stays decimal",
			node:     &parser.IntegerLiteral{Text: "010"},
			want:     types.Int32Value(10),
			wantType: types.TypeDesc{Kind: types.TypeKindInteger},
		},
		{
			name:     "integer literal honors metadata type",
			node:     &parser.IntegerLiteral{Text: "42"},
			metadata: Metadata{Types: map[parser.Node]types.TypeDesc{}},
			want:     types.Int64Value(42),
			wantType: mustTypeDesc(t, "BIGINT"),
		},
		{
			name:     "float literal infers NUMERIC",
			node:     &parser.FloatLiteral{Text: "1.25"},
			want:     mustDecimalValue(t, "1.25"),
			wantType: types.TypeDesc{Kind: types.TypeKindNumeric, Precision: 3, Scale: 2},
		},
		{
			name:     "string literal infers VARCHAR length",
			node:     &parser.StringLiteral{Value: "go"},
			want:     types.StringValue("go"),
			wantType: types.TypeDesc{Kind: types.TypeKindVarChar, Length: 2},
		},
		{
			name:     "boolean literal infers BOOLEAN",
			node:     &parser.BoolLiteral{Value: true},
			want:     types.BoolValue(true),
			wantType: types.TypeDesc{Kind: types.TypeKindBoolean},
		},
		{
			name: "null literal uses metadata type",
			node: nullNode,
			metadata: Metadata{
				Types: map[parser.Node]types.TypeDesc{
					nullNode: nullType,
				},
			},
			want:     types.NullValue(),
			wantType: nullType,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			metadata := testCase.metadata
			if testCase.name == "integer literal honors metadata type" {
				typed := testCase.node.(*parser.IntegerLiteral)
				metadata = Metadata{
					Types: map[parser.Node]types.TypeDesc{
						typed: mustTypeDesc(t, "BIGINT"),
					},
				}
			}

			compiled := mustCompileExpr(t, testCase.node, metadata)
			if compiled.Type() != testCase.wantType {
				t.Fatalf("Type() = %#v, want %#v", compiled.Type(), testCase.wantType)
			}
			assertEvalValue(t, compiled, NewRow(), testCase.want)
		})
	}
}

func TestCompileExprUnaryAndBinaryOps(t *testing.T) {
	t.Parallel()

	id := identifier("id")
	name := identifier("name")
	flag := identifier("flag")
	metadata := Metadata{
		Bindings: map[parser.Node]OrdinalBinding{
			id:   {Ordinal: 0, Type: types.TypeDesc{Kind: types.TypeKindInteger}},
			name: {Ordinal: 1, Type: mustTypeDesc(t, "VARCHAR(5) NOT NULL")},
			flag: {Ordinal: 2, Type: mustTypeDesc(t, "BOOLEAN NOT NULL")},
		},
	}
	row := NewRow(types.Int32Value(7), types.StringValue("alpha"), types.BoolValue(false))

	testCases := []struct {
		name     string
		node     parser.Node
		want     types.Value
		wantType types.TypeDesc
	}{
		{
			name:     "unary plus preserves numeric value",
			node:     &parser.UnaryExpr{Operator: "+", Operand: id},
			want:     types.Int32Value(7),
			wantType: types.TypeDesc{Kind: types.TypeKindInteger},
		},
		{
			name:     "unary minus negates integer literal",
			node:     &parser.UnaryExpr{Operator: "-", Operand: &parser.IntegerLiteral{Text: "5"}},
			want:     types.Int32Value(-5),
			wantType: types.TypeDesc{Kind: types.TypeKindInteger},
		},
		{
			name:     "NOT flips boolean truth value",
			node:     &parser.UnaryExpr{Operator: "NOT", Operand: flag},
			want:     types.BoolValue(true),
			wantType: types.TypeDesc{Kind: types.TypeKindBoolean},
		},
		{
			name:     "addition coerces numeric operands",
			node:     &parser.BinaryExpr{Operator: "+", Left: id, Right: &parser.IntegerLiteral{Text: "5"}},
			want:     types.Int32Value(12),
			wantType: types.TypeDesc{Kind: types.TypeKindInteger},
		},
		{
			name: "string concatenation joins operands",
			node: &parser.BinaryExpr{
				Operator: "||",
				Left:     name,
				Right:    &parser.StringLiteral{Value: "!"},
			},
			want: types.StringValue("alpha!"),
		},
		{
			name: "comparison returns boolean",
			node: &parser.BinaryExpr{
				Operator: ">=",
				Left:     id,
				Right:    &parser.IntegerLiteral{Text: "7"},
			},
			want:     types.BoolValue(true),
			wantType: types.TypeDesc{Kind: types.TypeKindBoolean},
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			compiled := mustCompileExpr(t, testCase.node, metadata)
			if testCase.wantType != (types.TypeDesc{}) && compiled.Type() != testCase.wantType {
				t.Fatalf("Type() = %#v, want %#v", compiled.Type(), testCase.wantType)
			}
			assertEvalValue(t, compiled, row, testCase.want)
		})
	}
}

func TestCompileExprBooleanPredicatesAndNullPropagation(t *testing.T) {
	t.Parallel()

	assertEvalValue(
		t,
		mustCompileExpr(t, &parser.BinaryExpr{
			Operator: "AND",
			Left:     &parser.BoolLiteral{Value: true},
			Right:    &parser.NullLiteral{},
		}, nil),
		NewRow(),
		types.NullValue(),
	)

	assertEvalValue(
		t,
		mustCompileExpr(t, &parser.BinaryExpr{
			Operator: "OR",
			Left:     &parser.BoolLiteral{Value: true},
			Right:    &parser.NullLiteral{},
		}, nil),
		NewRow(),
		types.BoolValue(true),
	)

	value := identifier("value")
	metadata := Metadata{
		Bindings: map[parser.Node]OrdinalBinding{
			value: {Ordinal: 0, Type: types.TypeDesc{Kind: types.TypeKindInteger}},
		},
	}

	assertEvalValue(
		t,
		mustCompileExpr(t, &parser.BinaryExpr{
			Operator: "=",
			Left:     value,
			Right:    &parser.NullLiteral{},
		}, metadata),
		NewRow(types.Int32Value(3)),
		types.NullValue(),
	)

	assertEvalValue(
		t,
		mustCompileExpr(t, &parser.BetweenExpr{
			Expr:  value,
			Lower: &parser.IntegerLiteral{Text: "1"},
			Upper: &parser.IntegerLiteral{Text: "5"},
		}, metadata),
		NewRow(types.Int32Value(3)),
		types.BoolValue(true),
	)

	assertEvalValue(
		t,
		mustCompileExpr(t, &parser.InExpr{
			Expr: value,
			List: []parser.Node{
				&parser.IntegerLiteral{Text: "1"},
				&parser.NullLiteral{},
				&parser.IntegerLiteral{Text: "5"},
			},
		}, metadata),
		NewRow(types.Int32Value(3)),
		types.NullValue(),
	)

	assertEvalValue(
		t,
		mustCompileExpr(t, &parser.LikeExpr{
			Expr:    &parser.StringLiteral{Value: "ab_cd"},
			Pattern: &parser.StringLiteral{Value: "ab!_cd"},
			Escape:  &parser.StringLiteral{Value: "!"},
		}, nil),
		NewRow(),
		types.BoolValue(true),
	)

	left := identifier("left")
	right := identifier("right")
	distinctMetadata := Metadata{
		Bindings: map[parser.Node]OrdinalBinding{
			left:  {Ordinal: 0, Type: mustTypeDesc(t, "INTEGER")},
			right: {Ordinal: 1, Type: mustTypeDesc(t, "INTEGER")},
		},
	}

	assertEvalValue(
		t,
		mustCompileExpr(t, &parser.IsExpr{Expr: left, Predicate: "NULL"}, distinctMetadata),
		NewRow(types.NullValue(), types.Int32Value(1)),
		types.BoolValue(true),
	)

	assertEvalValue(
		t,
		mustCompileExpr(t, &parser.IsExpr{
			Expr:      left,
			Predicate: "DISTINCT FROM",
			Right:     right,
		}, distinctMetadata),
		NewRow(types.NullValue(), types.Int32Value(1)),
		types.BoolValue(true),
	)
}

func TestCompileExprCastBehavior(t *testing.T) {
	t.Parallel()

	text := identifier("text")
	metadata := Metadata{
		Bindings: map[parser.Node]OrdinalBinding{
			text: {Ordinal: 0, Type: mustTypeDesc(t, "VARCHAR(3)")},
		},
	}
	node := &parser.CastExpr{
		Expr: text,
		Type: typeName("INTEGER"),
	}

	compiled := mustCompileExpr(t, node, metadata)
	if compiled.Type() != mustTypeDesc(t, "INTEGER") {
		t.Fatalf("Type() = %#v, want %#v", compiled.Type(), mustTypeDesc(t, "INTEGER"))
	}

	assertEvalValue(t, compiled, NewRow(types.StringValue("42")), types.Int32Value(42))
	assertEvalValue(t, compiled, NewRow(types.NullValue()), types.NullValue())

	if _, err := compiled.Eval(NewRow(types.StringValue("abc"))); !errors.Is(err, types.ErrCastFailure) {
		t.Fatalf("Eval() error = %v, want %v", err, types.ErrCastFailure)
	}
}

func TestCompileExprFunctionCallsAndCase(t *testing.T) {
	t.Parallel()

	text := identifier("text")
	value := identifier("value")
	nullable := identifier("nullable")
	flag := identifier("flag")
	metadata := Metadata{
		Bindings: map[parser.Node]OrdinalBinding{
			text:     {Ordinal: 0, Type: mustTypeDesc(t, "VARCHAR(5)")},
			value:    {Ordinal: 1, Type: mustTypeDesc(t, "INTEGER")},
			nullable: {Ordinal: 2, Type: mustTypeDesc(t, "INTEGER")},
			flag:     {Ordinal: 3, Type: mustTypeDesc(t, "BOOLEAN")},
		},
	}
	row := NewRow(types.StringValue("alpha"), types.Int32Value(7), types.NullValue(), types.BoolValue(false))

	testCases := []struct {
		name string
		node parser.Node
		want types.Value
	}{
		{
			name: "LOWER executes",
			node: &parser.FunctionCall{
				Name: qualifiedName("LOWER"),
				Args: []parser.Node{text},
			},
			want: types.StringValue("alpha"),
		},
		{
			name: "COALESCE returns first non null value",
			node: &parser.FunctionCall{
				Name: qualifiedName("COALESCE"),
				Args: []parser.Node{nullable, value, &parser.IntegerLiteral{Text: "9"}},
			},
			want: types.Int32Value(7),
		},
		{
			name: "NULLIF returns null when values match",
			node: &parser.FunctionCall{
				Name: qualifiedName("NULLIF"),
				Args: []parser.Node{value, &parser.IntegerLiteral{Text: "7"}},
			},
			want: types.NullValue(),
		},
		{
			name: "CASE expression returns matched branch",
			node: &parser.CaseExpr{
				Whens: []*parser.WhenClause{
					{Condition: flag, Result: &parser.StringLiteral{Value: "yes"}},
				},
				Else: &parser.StringLiteral{Value: "no"},
			},
			want: types.StringValue("no"),
		},
		{
			name: "simple CASE compares operand",
			node: &parser.CaseExpr{
				Operand: value,
				Whens: []*parser.WhenClause{
					{Condition: &parser.IntegerLiteral{Text: "1"}, Result: &parser.StringLiteral{Value: "one"}},
					{Condition: &parser.IntegerLiteral{Text: "7"}, Result: &parser.StringLiteral{Value: "seven"}},
				},
				Else: &parser.StringLiteral{Value: "other"},
			},
			want: types.StringValue("seven"),
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			compiled := mustCompileExpr(t, testCase.node, metadata)
			assertEvalValue(t, compiled, row, testCase.want)
		})
	}
}

func TestCompileExprCompileErrors(t *testing.T) {
	t.Parallel()

	negativeOrdinal := identifier("negative")

	testCases := []struct {
		name     string
		node     parser.Node
		metadata ExpressionMetadata
		wantErr  error
	}{
		{
			name:    "nil node is unsupported",
			wantErr: ErrUnsupportedExpression,
		},
		{
			name:    "parameter markers are not compiled yet",
			node:    &parser.ParamLiteral{Text: "?"},
			wantErr: ErrUnsupportedExpression,
		},
		{
			name:    "missing binding is rejected",
			node:    identifier("missing"),
			wantErr: ErrUnboundExpression,
		},
		{
			name: "negative ordinals are rejected",
			node: negativeOrdinal,
			metadata: Metadata{
				Bindings: map[parser.Node]OrdinalBinding{
					negativeOrdinal: {Ordinal: -1, Type: types.TypeDesc{Kind: types.TypeKindInteger}},
				},
			},
			wantErr: ErrUnboundExpression,
		},
		{
			name: "unsupported unary operators fail compile",
			node: &parser.UnaryExpr{
				Operator: "~",
				Operand:  &parser.IntegerLiteral{Text: "1"},
			},
			wantErr: ErrUnsupportedExpression,
		},
		{
			name: "unsupported binary operators fail compile",
			node: &parser.BinaryExpr{
				Operator: "^",
				Left:     &parser.IntegerLiteral{Text: "1"},
				Right:    &parser.IntegerLiteral{Text: "2"},
			},
			wantErr: ErrUnsupportedExpression,
		},
		{
			name: "invalid cast target descriptors surface type errors",
			node: &parser.CastExpr{
				Expr: &parser.IntegerLiteral{Text: "1"},
				Type: &parser.TypeName{
					Qualifier: qualifiedName("pg_catalog"),
					Names:     []*parser.Identifier{identifier("int4")},
				},
			},
			wantErr: types.ErrInvalidTypeDesc,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			_, err := CompileExpr(testCase.node, testCase.metadata)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("CompileExpr() error = %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestCompiledExprRuntimeErrors(t *testing.T) {
	t.Parallel()

	var zero CompiledExpr
	if _, err := zero.Eval(NewRow()); !errors.Is(err, ErrUnsupportedExpression) {
		t.Fatalf("zero Eval() error = %v, want %v", err, ErrUnsupportedExpression)
	}

	outOfRange := identifier("value")
	outOfRangeExpr := mustCompileExpr(t, outOfRange, Metadata{
		Bindings: map[parser.Node]OrdinalBinding{
			outOfRange: {Ordinal: 1, Type: types.TypeDesc{Kind: types.TypeKindInteger}},
		},
	})
	if _, err := outOfRangeExpr.Eval(NewRow(types.Int32Value(1))); !errors.Is(err, ErrRowOrdinalOutOfRange) {
		t.Fatalf("out of range Eval() error = %v, want %v", err, ErrRowOrdinalOutOfRange)
	}

	division := mustCompileExpr(t, &parser.BinaryExpr{
		Operator: "/",
		Left:     &parser.IntegerLiteral{Text: "7"},
		Right:    &parser.IntegerLiteral{Text: "0"},
	}, nil)
	if _, err := division.Eval(NewRow()); !errors.Is(err, ErrDivisionByZero) {
		t.Fatalf("division Eval() error = %v, want %v", err, ErrDivisionByZero)
	}

	notString := mustCompileExpr(t, &parser.UnaryExpr{
		Operator: "NOT",
		Operand:  &parser.StringLiteral{Value: "alpha"},
	}, nil)
	if _, err := notString.Eval(NewRow()); !errors.Is(err, ErrInvalidExpressionType) {
		t.Fatalf("NOT string Eval() error = %v, want %v", err, ErrInvalidExpressionType)
	}

	like := mustCompileExpr(t, &parser.LikeExpr{
		Expr:    &parser.StringLiteral{Value: "alpha"},
		Pattern: &parser.StringLiteral{Value: "a%"},
		Escape:  &parser.StringLiteral{Value: "!!"},
	}, nil)
	if _, err := like.Eval(NewRow()); !errors.Is(err, ErrInvalidLikeEscape) {
		t.Fatalf("LIKE Eval() error = %v, want %v", err, ErrInvalidLikeEscape)
	}

	emptyEscape := mustCompileExpr(t, &parser.LikeExpr{
		Expr:    &parser.StringLiteral{Value: "alpha"},
		Pattern: &parser.StringLiteral{Value: "a%"},
		Escape:  &parser.StringLiteral{Value: ""},
	}, nil)
	if _, err := emptyEscape.Eval(NewRow()); !errors.Is(err, ErrInvalidLikeEscape) {
		t.Fatalf("LIKE empty escape Eval() error = %v, want %v", err, ErrInvalidLikeEscape)
	}

	left := identifier("left")
	right := identifier("right")
	nonFinite := mustCompileExpr(t, &parser.BinaryExpr{
		Operator: "*",
		Left:     left,
		Right:    right,
	}, Metadata{
		Bindings: map[parser.Node]OrdinalBinding{
			left:  {Ordinal: 0, Type: mustTypeDesc(t, "DOUBLE PRECISION")},
			right: {Ordinal: 1, Type: mustTypeDesc(t, "DOUBLE PRECISION")},
		},
	})
	if _, err := nonFinite.Eval(NewRow(types.Float64Value(1e308), types.Float64Value(1e308))); !errors.Is(err, types.ErrNonFiniteNumeric) {
		t.Fatalf("non-finite Eval() error = %v, want %v", err, types.ErrNonFiniteNumeric)
	}
}

func mustCompileExpr(t *testing.T, node parser.Node, metadata ExpressionMetadata) CompiledExpr {
	t.Helper()

	compiled, err := CompileExpr(node, metadata)
	if err != nil {
		t.Fatalf("CompileExpr() error = %v", err)
	}

	return compiled
}

func assertEvalValue(t *testing.T, expr CompiledExpr, row Row, want types.Value) {
	t.Helper()

	got, err := expr.Eval(row)
	if err != nil {
		t.Fatalf("Eval() error = %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("Eval() = %#v, want %#v", got, want)
	}
}

func mustTypeDesc(t *testing.T, text string) types.TypeDesc {
	t.Helper()

	desc, err := types.ParseTypeDesc(text)
	if err != nil {
		t.Fatalf("ParseTypeDesc(%q) error = %v", text, err)
	}

	return desc
}

func mustDecimalValue(t *testing.T, text string) types.Value {
	t.Helper()

	decimal, err := types.ParseDecimal(text)
	if err != nil {
		t.Fatalf("ParseDecimal(%q) error = %v", text, err)
	}

	return types.DecimalValue(decimal)
}

func identifier(name string) *parser.Identifier {
	return &parser.Identifier{Name: name}
}

func qualifiedName(parts ...string) *parser.QualifiedName {
	names := make([]*parser.Identifier, 0, len(parts))
	for _, part := range parts {
		names = append(names, identifier(part))
	}

	return &parser.QualifiedName{Parts: names}
}

func typeName(name string, args ...parser.Node) *parser.TypeName {
	parts := strings.Fields(name)
	names := make([]*parser.Identifier, 0, len(parts))
	for _, part := range parts {
		names = append(names, identifier(part))
	}

	return &parser.TypeName{Names: names, Args: args}
}
