package executor

import (
	"errors"
	"io"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/types"
)

func TestNestedLoopJoinLifecycle(t *testing.T) {
	t.Parallel()

	left := &joinTestOperator{}
	right := &joinTestOperator{}
	join := NewNestedLoopJoin(JoinCross, left, right, 1, 1, nil)

	if _, err := join.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() before Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	if err := join.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if left.openCalls != 1 {
		t.Fatalf("left Open() calls = %d, want 1", left.openCalls)
	}
	if right.openCalls != 0 {
		t.Fatalf("right Open() calls after Open = %d, want 0", right.openCalls)
	}

	if err := join.Open(); !errors.Is(err, ErrOperatorOpen) {
		t.Fatalf("second Open() error = %v, want %v", err, ErrOperatorOpen)
	}

	if err := join.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if left.closeCalls != 1 {
		t.Fatalf("left Close() calls = %d, want 1", left.closeCalls)
	}
	if right.closeCalls != 0 {
		t.Fatalf("right Close() calls = %d, want 0", right.closeCalls)
	}

	if _, err := join.Next(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Next() after Close error = %v, want %v", err, ErrOperatorClosed)
	}

	if err := join.Open(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Open() after Close error = %v, want %v", err, ErrOperatorClosed)
	}

	if err := join.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if left.closeCalls != 1 {
		t.Fatalf("left Close() calls after second Close = %d, want 1", left.closeCalls)
	}
	if right.closeCalls != 0 {
		t.Fatalf("right Close() calls after second Close = %d, want 0", right.closeCalls)
	}
}

func TestNestedLoopJoinSemanticsAndSyntheticHandles(t *testing.T) {
	t.Parallel()

	predicate := joinPredicatePtr(joinTestPredicate(func(row Row) (types.Value, error) {
		leftKey, ok := row.Value(1)
		if !ok {
			t.Fatal("predicate row.Value(1) = (_, false), want (_, true)")
		}
		rightKey, ok := row.Value(2)
		if !ok {
			t.Fatal("predicate row.Value(2) = (_, false), want (_, true)")
		}

		if leftKey.IsNull() || rightKey.IsNull() {
			return types.NullValue(), nil
		}

		return types.BoolValue(leftKey.Equal(rightKey)), nil
	}))

	testCases := []struct {
		name string
		kind JoinType
		on   *CompiledExpr
		want []Row
	}{
		{
			name: "inner",
			kind: JoinInner,
			on:   predicate,
			want: []Row{
				NewRow(
					types.StringValue("a"),
					types.Int32Value(1),
					types.Int32Value(1),
					types.StringValue("one"),
				),
			},
		},
		{
			name: "cross",
			kind: JoinCross,
			want: []Row{
				NewRow(
					types.StringValue("a"),
					types.Int32Value(1),
					types.Int32Value(1),
					types.StringValue("one"),
				),
				NewRow(
					types.StringValue("a"),
					types.Int32Value(1),
					types.Int32Value(3),
					types.StringValue("three"),
				),
				NewRow(
					types.StringValue("b"),
					types.Int32Value(2),
					types.Int32Value(1),
					types.StringValue("one"),
				),
				NewRow(
					types.StringValue("b"),
					types.Int32Value(2),
					types.Int32Value(3),
					types.StringValue("three"),
				),
			},
		},
		{
			name: "left",
			kind: JoinLeft,
			on:   predicate,
			want: []Row{
				NewRow(
					types.StringValue("a"),
					types.Int32Value(1),
					types.Int32Value(1),
					types.StringValue("one"),
				),
				NewRow(
					types.StringValue("b"),
					types.Int32Value(2),
					types.NullValue(),
					types.NullValue(),
				),
			},
		},
		{
			name: "right",
			kind: JoinRight,
			on:   predicate,
			want: []Row{
				NewRow(
					types.StringValue("a"),
					types.Int32Value(1),
					types.Int32Value(1),
					types.StringValue("one"),
				),
				NewRow(
					types.NullValue(),
					types.NullValue(),
					types.Int32Value(3),
					types.StringValue("three"),
				),
			},
		},
		{
			name: "full",
			kind: JoinFull,
			on:   predicate,
			want: []Row{
				NewRow(
					types.StringValue("a"),
					types.Int32Value(1),
					types.Int32Value(1),
					types.StringValue("one"),
				),
				NewRow(
					types.StringValue("b"),
					types.Int32Value(2),
					types.NullValue(),
					types.NullValue(),
				),
				NewRow(
					types.NullValue(),
					types.NullValue(),
					types.Int32Value(3),
					types.StringValue("three"),
				),
			},
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			leftRows := []Row{
				NewRowWithHandle(
					storage.RowHandle{Page: 1, Slot: 1},
					types.StringValue("a"),
					types.Int32Value(1),
				),
				NewRowWithHandle(
					storage.RowHandle{Page: 1, Slot: 2},
					types.StringValue("b"),
					types.Int32Value(2),
				),
			}
			rightRows := []Row{
				NewRowWithHandle(
					storage.RowHandle{Page: 2, Slot: 1},
					types.Int32Value(1),
					types.StringValue("one"),
				),
				NewRowWithHandle(
					storage.RowHandle{Page: 2, Slot: 2},
					types.Int32Value(3),
					types.StringValue("three"),
				),
			}

			left := &joinTestOperator{nextResults: joinResults(leftRows...)}
			right := &joinTestOperator{nextResults: joinResults(rightRows...)}
			join := NewNestedLoopJoin(testCase.kind, left, right, 2, 2, testCase.on)

			if err := join.Open(); err != nil {
				t.Fatalf("Open() error = %v", err)
			}

			got := collectJoinRows(t, join)
			assertJoinRowsEqual(t, got, testCase.want)

			if left.openCalls != 1 {
				t.Fatalf("left Open() calls = %d, want 1", left.openCalls)
			}
			if right.openCalls != 1 {
				t.Fatalf("right Open() calls = %d, want 1", right.openCalls)
			}
			if right.nextCalls != len(rightRows)+1 {
				t.Fatalf("right Next() calls = %d, want %d", right.nextCalls, len(rightRows)+1)
			}

			if err := join.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
			if left.closeCalls != 1 {
				t.Fatalf("left Close() calls = %d, want 1", left.closeCalls)
			}
			if right.closeCalls != 1 {
				t.Fatalf("right Close() calls = %d, want 1", right.closeCalls)
			}
		})
	}
}

func TestNestedLoopJoinOpenFailureLeavesOperatorRetryable(t *testing.T) {
	t.Parallel()

	openErr := errors.New("left open failed")
	left := &joinTestOperator{openErrs: []error{openErr, nil}}
	right := &joinTestOperator{}
	join := NewNestedLoopJoin(JoinInner, left, right, 1, 1, nil)

	if err := join.Open(); !errors.Is(err, openErr) {
		t.Fatalf("first Open() error = %v, want %v", err, openErr)
	}
	if _, err := join.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() after failed Open error = %v, want %v", err, ErrOperatorNotOpen)
	}
	if right.openCalls != 0 {
		t.Fatalf("right Open() calls after failed Open = %d, want 0", right.openCalls)
	}

	if err := join.Open(); err != nil {
		t.Fatalf("second Open() error = %v", err)
	}

	if err := join.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if left.closeCalls != 1 {
		t.Fatalf("left Close() calls = %d, want 1", left.closeCalls)
	}
	if right.closeCalls != 0 {
		t.Fatalf("right Close() calls = %d, want 0", right.closeCalls)
	}
}

func TestNestedLoopJoinClosePropagatesBothChildErrorsOnce(t *testing.T) {
	t.Parallel()

	leftCloseErr := errors.New("left close failed")
	rightCloseErr := errors.New("right close failed")
	left := &joinTestOperator{
		nextResults: joinResults(
			NewRowWithHandle(storage.RowHandle{Page: 1, Slot: 1}, types.Int32Value(1)),
		),
		closeErr: leftCloseErr,
	}
	right := &joinTestOperator{
		nextResults: joinResults(
			NewRowWithHandle(storage.RowHandle{Page: 2, Slot: 1}, types.Int32Value(9)),
		),
		closeErr: rightCloseErr,
	}
	join := NewNestedLoopJoin(JoinCross, left, right, 1, 1, nil)

	if err := join.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	row, err := join.Next()
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	assertJoinRowEqual(t, row, NewRow(types.Int32Value(1), types.Int32Value(9)))

	err = join.Close()
	if !errors.Is(err, leftCloseErr) || !errors.Is(err, rightCloseErr) {
		t.Fatalf("Close() error = %v, want joined left/right close errors", err)
	}
	if left.closeCalls != 1 {
		t.Fatalf("left Close() calls = %d, want 1", left.closeCalls)
	}
	if right.closeCalls != 1 {
		t.Fatalf("right Close() calls = %d, want 1", right.closeCalls)
	}

	if err := join.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if left.closeCalls != 1 {
		t.Fatalf("left Close() calls after second Close = %d, want 1", left.closeCalls)
	}
	if right.closeCalls != 1 {
		t.Fatalf("right Close() calls after second Close = %d, want 1", right.closeCalls)
	}
}

func TestNestedLoopJoinMaterializationErrorIsTerminal(t *testing.T) {
	t.Parallel()

	runtimeErr := errors.New("right materialization failed")
	left := &joinTestOperator{
		nextResults: joinResults(
			NewRowWithHandle(storage.RowHandle{Page: 1, Slot: 1}, types.Int32Value(1)),
		),
	}
	right := &joinTestOperator{
		nextResults: []joinNextResult{
			{row: NewRowWithHandle(storage.RowHandle{Page: 2, Slot: 1}, types.Int32Value(9))},
			{err: runtimeErr},
		},
	}
	join := NewNestedLoopJoin(JoinCross, left, right, 1, 1, nil)

	if err := join.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if _, err := join.Next(); !errors.Is(err, runtimeErr) {
		t.Fatalf("first Next() error = %v, want %v", err, runtimeErr)
	}
	if _, err := join.Next(); !errors.Is(err, runtimeErr) {
		t.Fatalf("second Next() error = %v, want %v", err, runtimeErr)
	}
	if right.nextCalls != 2 {
		t.Fatalf("right Next() calls = %d, want 2", right.nextCalls)
	}

	if err := join.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if left.closeCalls != 1 {
		t.Fatalf("left Close() calls = %d, want 1", left.closeCalls)
	}
	if right.closeCalls != 1 {
		t.Fatalf("right Close() calls = %d, want 1", right.closeCalls)
	}
}

type joinTestOperator struct {
	openErrs    []error
	nextResults []joinNextResult
	closeErr    error
	openCalls   int
	nextCalls   int
	closeCalls  int
	opened      bool
	closed      bool
}

func (o *joinTestOperator) Open() error {
	o.openCalls++
	if o.closed {
		return ErrOperatorClosed
	}
	if len(o.openErrs) != 0 {
		err := o.openErrs[0]
		o.openErrs = o.openErrs[1:]
		if err != nil {
			return err
		}
	}

	o.opened = true

	return nil
}

func (o *joinTestOperator) Next() (Row, error) {
	o.nextCalls++
	if o.closed {
		return Row{}, ErrOperatorClosed
	}
	if !o.opened {
		return Row{}, ErrOperatorNotOpen
	}
	if len(o.nextResults) == 0 {
		return Row{}, io.EOF
	}

	result := o.nextResults[0]
	o.nextResults = o.nextResults[1:]

	return result.row, result.err
}

func (o *joinTestOperator) Close() error {
	o.closeCalls++
	if o.closeCalls > 1 {
		return nil
	}

	o.opened = false
	o.closed = true

	return o.closeErr
}

type joinNextResult struct {
	row Row
	err error
}

func joinResults(rows ...Row) []joinNextResult {
	results := make([]joinNextResult, 0, len(rows))
	for _, row := range rows {
		results = append(results, joinNextResult{row: row})
	}

	return results
}

func joinTestPredicate(eval func(Row) (types.Value, error)) CompiledExpr {
	return CompiledExpr{
		typ:  types.TypeDesc{Kind: types.TypeKindBoolean, Nullable: true},
		eval: eval,
	}
}

func joinPredicatePtr(expr CompiledExpr) *CompiledExpr {
	return &expr
}

func collectJoinRows(t *testing.T, join *NestedLoopJoin) []Row {
	t.Helper()

	rows := make([]Row, 0)
	for {
		row, err := join.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}

		rows = append(rows, row)
	}

	return rows
}

func assertJoinRowsEqual(t *testing.T, got []Row, want []Row) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("row count = %d, want %d", len(got), len(want))
	}

	for index := range want {
		assertJoinRowEqual(t, got[index], want[index])
	}
}

func assertJoinRowEqual(t *testing.T, got Row, want Row) {
	t.Helper()

	if got.Handle != want.Handle {
		t.Fatalf("row.Handle = %#v, want %#v", got.Handle, want.Handle)
	}
	if got.Handle.Valid() {
		t.Fatalf("row.Handle.Valid() = true, want false for synthetic join row")
	}
	if got.Len() != want.Len() {
		t.Fatalf("row.Len() = %d, want %d", got.Len(), want.Len())
	}

	for index, expected := range want.Values() {
		value, ok := got.Value(index)
		if !ok {
			t.Fatalf("Value(%d) = (_, false), want (_, true)", index)
		}
		if !value.Equal(expected) {
			t.Fatalf("Value(%d) = %v, want %v", index, value, expected)
		}
	}
}
