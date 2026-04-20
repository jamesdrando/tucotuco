package executor

import (
	"errors"
	"io"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/types"
)

func TestHashAggregateLifecycle(t *testing.T) {
	t.Parallel()

	child := &hashAggregateTestOperator{}
	agg := NewHashAggregate(child, nil, AggregateSpec{Name: AggregateCount, CountStar: true})

	if _, err := agg.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() before Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	if err := agg.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := agg.Open(); !errors.Is(err, ErrOperatorOpen) {
		t.Fatalf("second Open() error = %v, want %v", err, ErrOperatorOpen)
	}

	if err := agg.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if _, err := agg.Next(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Next() after Close error = %v, want %v", err, ErrOperatorClosed)
	}

	if err := agg.Open(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Open() after Close error = %v, want %v", err, ErrOperatorClosed)
	}

	if err := agg.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestHashAggregateGlobalEmptyInputReturnsPostgresStyleResults(t *testing.T) {
	t.Parallel()

	child := &hashAggregateTestOperator{}
	agg := NewHashAggregate(child, nil,
		AggregateSpec{Name: AggregateCount, CountStar: true},
		AggregateSpec{Name: AggregateCount, Expr: hashAggregateOrdinalExpr(0, types.TypeDesc{Kind: types.TypeKindInteger})},
		AggregateSpec{Name: AggregateSum, Expr: hashAggregateOrdinalExpr(0, types.TypeDesc{Kind: types.TypeKindInteger})},
		AggregateSpec{Name: AggregateAvg, Expr: hashAggregateOrdinalExpr(0, types.TypeDesc{Kind: types.TypeKindInteger})},
		AggregateSpec{Name: AggregateMin, Expr: hashAggregateOrdinalExpr(0, types.TypeDesc{Kind: types.TypeKindInteger})},
		AggregateSpec{Name: AggregateMax, Expr: hashAggregateOrdinalExpr(0, types.TypeDesc{Kind: types.TypeKindInteger})},
		AggregateSpec{Name: AggregateEvery, Expr: hashAggregateOrdinalExpr(1, types.TypeDesc{Kind: types.TypeKindBoolean})},
	)

	if err := agg.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	row, err := agg.Next()
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}

	assertHashAggregateRow(
		t,
		row,
		types.Int64Value(0),
		types.Int64Value(0),
		types.NullValue(),
		types.NullValue(),
		types.NullValue(),
		types.NullValue(),
		types.NullValue(),
	)
	if row.Handle.Valid() {
		t.Fatalf("row.Handle = %#v, want zero synthetic handle", row.Handle)
	}

	if _, err := agg.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next() error = %v, want %v", err, io.EOF)
	}
	if _, err := agg.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("third Next() error = %v, want %v", err, io.EOF)
	}

	if err := agg.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestHashAggregateGroupedAggregatesIgnoreNullsAndPreserveGroupOrder(t *testing.T) {
	t.Parallel()

	child := &hashAggregateTestOperator{
		nextResults: hashAggregateResults(
			NewRow(types.StringValue("a"), types.Int32Value(1), types.BoolValue(true)),
			NewRow(types.StringValue("b"), types.NullValue(), types.NullValue()),
			NewRow(types.StringValue("a"), types.Int32Value(2), types.NullValue()),
			NewRow(types.StringValue("c"), types.Int32Value(3), types.BoolValue(false)),
			NewRow(types.StringValue("b"), types.Int32Value(4), types.BoolValue(true)),
			NewRow(types.StringValue("d"), types.NullValue(), types.NullValue()),
		),
	}
	agg := NewHashAggregate(
		child,
		[]CompiledExpr{hashAggregateOrdinalExpr(0, types.TypeDesc{Kind: types.TypeKindVarChar, Length: 1})},
		AggregateSpec{Name: AggregateCount, Expr: hashAggregateOrdinalExpr(1, types.TypeDesc{Kind: types.TypeKindInteger})},
		AggregateSpec{Name: AggregateSum, Expr: hashAggregateOrdinalExpr(1, types.TypeDesc{Kind: types.TypeKindInteger})},
		AggregateSpec{Name: AggregateAvg, Expr: hashAggregateOrdinalExpr(1, types.TypeDesc{Kind: types.TypeKindInteger})},
		AggregateSpec{Name: AggregateMin, Expr: hashAggregateOrdinalExpr(1, types.TypeDesc{Kind: types.TypeKindInteger})},
		AggregateSpec{Name: AggregateMax, Expr: hashAggregateOrdinalExpr(1, types.TypeDesc{Kind: types.TypeKindInteger})},
		AggregateSpec{Name: AggregateEvery, Expr: hashAggregateOrdinalExpr(2, types.TypeDesc{Kind: types.TypeKindBoolean})},
	)

	if err := agg.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	want := []Row{
		NewRow(
			types.StringValue("a"),
			types.Int64Value(2),
			types.Int64Value(3),
			types.DecimalValue(mustHashAggregateDecimal(t, "1.5")),
			types.Int32Value(1),
			types.Int32Value(2),
			types.BoolValue(true),
		),
		NewRow(
			types.StringValue("b"),
			types.Int64Value(1),
			types.Int64Value(4),
			types.DecimalValue(mustHashAggregateDecimal(t, "4")),
			types.Int32Value(4),
			types.Int32Value(4),
			types.BoolValue(true),
		),
		NewRow(
			types.StringValue("c"),
			types.Int64Value(1),
			types.Int64Value(3),
			types.DecimalValue(mustHashAggregateDecimal(t, "3")),
			types.Int32Value(3),
			types.Int32Value(3),
			types.BoolValue(false),
		),
		NewRow(
			types.StringValue("d"),
			types.Int64Value(0),
			types.NullValue(),
			types.NullValue(),
			types.NullValue(),
			types.NullValue(),
			types.NullValue(),
		),
	}

	for index, expected := range want {
		row, err := agg.Next()
		if err != nil {
			t.Fatalf("Next() row %d error = %v", index, err)
		}
		if row.Handle.Valid() {
			t.Fatalf("row %d handle = %#v, want zero synthetic handle", index, row.Handle)
		}
		assertHashAggregateRowEqual(t, row, expected)
	}

	if _, err := agg.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("final Next() error = %v, want %v", err, io.EOF)
	}

	if err := agg.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestHashAggregateApproximateAvgReturnsFloat64(t *testing.T) {
	t.Parallel()

	child := &hashAggregateTestOperator{
		nextResults: hashAggregateResults(
			NewRow(types.Float64Value(1.0)),
			NewRow(types.NullValue()),
			NewRow(types.Float64Value(2.0)),
		),
	}
	agg := NewHashAggregate(child, nil,
		AggregateSpec{Name: AggregateAvg, Expr: hashAggregateOrdinalExpr(0, types.TypeDesc{Kind: types.TypeKindDoublePrecision})},
	)

	if err := agg.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	row, err := agg.Next()
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	assertHashAggregateRow(t, row, types.Float64Value(1.5))
}

func TestHashAggregateGroupsSignedZeroTogether(t *testing.T) {
	t.Parallel()

	child := &hashAggregateTestOperator{
		nextResults: hashAggregateResults(
			NewRow(types.Float64Value(0.0)),
			NewRow(types.Float64Value(-0.0)),
		),
	}
	agg := NewHashAggregate(
		child,
		[]CompiledExpr{hashAggregateOrdinalExpr(0, types.TypeDesc{Kind: types.TypeKindDoublePrecision})},
		AggregateSpec{Name: AggregateCount, CountStar: true},
	)

	if err := agg.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	row, err := agg.Next()
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	assertHashAggregateRow(t, row, types.Float64Value(0.0), types.Int64Value(2))

	if _, err := agg.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next() error = %v, want %v", err, io.EOF)
	}
}

func TestHashAggregateOpenFailureLeavesOperatorRetryable(t *testing.T) {
	t.Parallel()

	openErr := errors.New("child open failed")
	child := &hashAggregateTestOperator{
		openErrs: []error{openErr, nil},
		nextResults: hashAggregateResults(
			NewRow(types.Int32Value(7)),
		),
	}
	agg := NewHashAggregate(child, nil, AggregateSpec{Name: AggregateCount, CountStar: true})

	if err := agg.Open(); !errors.Is(err, openErr) {
		t.Fatalf("Open() error = %v, want %v", err, openErr)
	}

	if _, err := agg.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() after failed Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	if err := agg.Open(); err != nil {
		t.Fatalf("second Open() error = %v", err)
	}

	row, err := agg.Next()
	if err != nil {
		t.Fatalf("Next() after retry error = %v", err)
	}
	assertHashAggregateRow(t, row, types.Int64Value(1))

	if err := agg.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestHashAggregateClosePropagatesToChildOnce(t *testing.T) {
	t.Parallel()

	closeErr := errors.New("child close failed")
	child := &hashAggregateTestOperator{closeErr: closeErr}
	agg := NewHashAggregate(child, nil, AggregateSpec{Name: AggregateCount, CountStar: true})

	if err := agg.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := agg.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close() error = %v, want %v", err, closeErr)
	}
	if child.closeCalls != 1 {
		t.Fatalf("child Close() calls = %d, want 1", child.closeCalls)
	}

	if err := agg.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if child.closeCalls != 1 {
		t.Fatalf("child Close() calls after second Close = %d, want 1", child.closeCalls)
	}
}

func TestHashAggregateMaterializationErrorIsTerminal(t *testing.T) {
	t.Parallel()

	runtimeErr := errors.New("aggregate expression failed")
	child := &hashAggregateTestOperator{
		nextResults: hashAggregateResults(
			NewRow(types.Int32Value(1)),
		),
	}
	agg := NewHashAggregate(child, nil, AggregateSpec{
		Name: AggregateSum,
		Expr: hashAggregateTestExpr(types.TypeDesc{Kind: types.TypeKindInteger}, func(Row) (types.Value, error) {
			return types.Value{}, runtimeErr
		}),
	})

	if err := agg.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	row, err := agg.Next()
	if !errors.Is(err, runtimeErr) {
		t.Fatalf("first Next() error = %v, want %v", err, runtimeErr)
	}
	if row.Len() != 0 {
		t.Fatalf("row.Len() on error = %d, want 0", row.Len())
	}

	if _, err := agg.Next(); !errors.Is(err, runtimeErr) {
		t.Fatalf("second Next() error = %v, want %v", err, runtimeErr)
	}
	if child.nextCalls != 1 {
		t.Fatalf("child Next() calls = %d, want 1", child.nextCalls)
	}
}

type hashAggregateTestOperator struct {
	openErrs    []error
	nextResults []hashAggregateNextResult
	closeErr    error
	openCalls   int
	nextCalls   int
	closeCalls  int
	opened      bool
	closed      bool
}

func (s *hashAggregateTestOperator) Open() error {
	s.openCalls++
	if s.closed {
		return ErrOperatorClosed
	}
	if len(s.openErrs) > 0 {
		err := s.openErrs[0]
		s.openErrs = s.openErrs[1:]
		if err != nil {
			return err
		}
	}

	s.opened = true

	return nil
}

func (s *hashAggregateTestOperator) Next() (Row, error) {
	s.nextCalls++
	if s.closed {
		return Row{}, ErrOperatorClosed
	}
	if !s.opened {
		return Row{}, ErrOperatorNotOpen
	}
	if len(s.nextResults) == 0 {
		return Row{}, io.EOF
	}

	result := s.nextResults[0]
	s.nextResults = s.nextResults[1:]

	return result.row, result.err
}

func (s *hashAggregateTestOperator) Close() error {
	if s.closeCalls == 0 {
		s.opened = false
		s.closed = true
	}

	s.closeCalls++
	if s.closeCalls > 1 {
		return nil
	}

	return s.closeErr
}

type hashAggregateNextResult struct {
	row Row
	err error
}

func hashAggregateResults(rows ...Row) []hashAggregateNextResult {
	results := make([]hashAggregateNextResult, 0, len(rows))
	for _, row := range rows {
		results = append(results, hashAggregateNextResult{row: row})
	}

	return results
}

func hashAggregateTestExpr(desc types.TypeDesc, eval func(Row) (types.Value, error)) CompiledExpr {
	return CompiledExpr{
		typ:  desc,
		eval: eval,
	}
}

func hashAggregateOrdinalExpr(ordinal int, desc types.TypeDesc) CompiledExpr {
	return hashAggregateTestExpr(desc, func(row Row) (types.Value, error) {
		value, ok := row.Value(ordinal)
		if !ok {
			return types.Value{}, ErrRowOrdinalOutOfRange
		}

		return value, nil
	})
}

func mustHashAggregateDecimal(t *testing.T, text string) types.Decimal {
	t.Helper()

	value, err := types.ParseDecimal(text)
	if err != nil {
		t.Fatalf("ParseDecimal(%q) error = %v", text, err)
	}

	return value
}

func assertHashAggregateRow(t *testing.T, row Row, want ...types.Value) {
	t.Helper()

	if row.Len() != len(want) {
		t.Fatalf("row.Len() = %d, want %d", row.Len(), len(want))
	}

	for index, expected := range want {
		value, ok := row.Value(index)
		if !ok {
			t.Fatalf("Value(%d) = (_, false), want (_, true)", index)
		}
		if !value.Equal(expected) {
			t.Fatalf("Value(%d) = %v, want %v", index, value, expected)
		}
	}
}

func assertHashAggregateRowEqual(t *testing.T, got Row, want Row) {
	t.Helper()

	if got.Handle != want.Handle {
		t.Fatalf("row.Handle = %#v, want %#v", got.Handle, want.Handle)
	}

	assertHashAggregateRow(t, got, want.Values()...)
}
