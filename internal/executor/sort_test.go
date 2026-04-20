package executor

import (
	"errors"
	"io"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/types"
)

func TestSortLifecycle(t *testing.T) {
	t.Parallel()

	child := &sortTestOperator{}
	sort := NewSort(child, SortKey{Expr: sortTestOrdinalExpr(0)})

	if _, err := sort.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() before Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	if err := sort.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if child.openCalls != 1 {
		t.Fatalf("child Open() calls = %d, want 1", child.openCalls)
	}

	if err := sort.Open(); !errors.Is(err, ErrOperatorOpen) {
		t.Fatalf("second Open() error = %v, want %v", err, ErrOperatorOpen)
	}

	if err := sort.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if child.closeCalls != 1 {
		t.Fatalf("child Close() calls = %d, want 1", child.closeCalls)
	}

	if _, err := sort.Next(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Next() after Close error = %v, want %v", err, ErrOperatorClosed)
	}

	if err := sort.Open(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Open() after Close error = %v, want %v", err, ErrOperatorClosed)
	}

	if err := sort.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if child.closeCalls != 1 {
		t.Fatalf("child Close() calls after second Close = %d, want 1", child.closeCalls)
	}
}

func TestSortOrdersRowsStably(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		keys []SortKey
		rows []Row
		want []Row
	}{
		{
			name: "ascending default nulls last preserves equal-key order",
			keys: []SortKey{
				{Expr: sortTestOrdinalExpr(0)},
			},
			rows: []Row{
				NewRowWithHandle(storage.RowHandle{Page: 1, Slot: 1}, types.Int32Value(2), types.StringValue("two")),
				NewRowWithHandle(storage.RowHandle{Page: 1, Slot: 2}, types.Int32Value(1), types.StringValue("first")),
				NewRowWithHandle(storage.RowHandle{Page: 1, Slot: 3}, types.Int32Value(1), types.StringValue("second")),
				NewRowWithHandle(storage.RowHandle{Page: 1, Slot: 4}, types.NullValue(), types.StringValue("null")),
			},
			want: []Row{
				NewRowWithHandle(storage.RowHandle{Page: 1, Slot: 2}, types.Int32Value(1), types.StringValue("first")),
				NewRowWithHandle(storage.RowHandle{Page: 1, Slot: 3}, types.Int32Value(1), types.StringValue("second")),
				NewRowWithHandle(storage.RowHandle{Page: 1, Slot: 1}, types.Int32Value(2), types.StringValue("two")),
				NewRowWithHandle(storage.RowHandle{Page: 1, Slot: 4}, types.NullValue(), types.StringValue("null")),
			},
		},
		{
			name: "descending primary and ascending secondary use stable comparison",
			keys: []SortKey{
				{Expr: sortTestOrdinalExpr(0), Direction: SortDescending},
				{Expr: sortTestOrdinalExpr(1)},
			},
			rows: []Row{
				NewRowWithHandle(storage.RowHandle{Page: 2, Slot: 1}, types.Int32Value(2), types.StringValue("beta")),
				NewRowWithHandle(storage.RowHandle{Page: 2, Slot: 2}, types.NullValue(), types.StringValue("omega")),
				NewRowWithHandle(storage.RowHandle{Page: 2, Slot: 3}, types.Int32Value(2), types.StringValue("alpha")),
				NewRowWithHandle(storage.RowHandle{Page: 2, Slot: 4}, types.Int32Value(1), types.StringValue("gamma")),
			},
			want: []Row{
				NewRowWithHandle(storage.RowHandle{Page: 2, Slot: 2}, types.NullValue(), types.StringValue("omega")),
				NewRowWithHandle(storage.RowHandle{Page: 2, Slot: 3}, types.Int32Value(2), types.StringValue("alpha")),
				NewRowWithHandle(storage.RowHandle{Page: 2, Slot: 1}, types.Int32Value(2), types.StringValue("beta")),
				NewRowWithHandle(storage.RowHandle{Page: 2, Slot: 4}, types.Int32Value(1), types.StringValue("gamma")),
			},
		},
		{
			name: "explicit nulls first overrides ascending default",
			keys: []SortKey{
				{Expr: sortTestOrdinalExpr(0), Nulls: SortNullsFirst},
			},
			rows: []Row{
				NewRowWithHandle(storage.RowHandle{Page: 3, Slot: 1}, types.Int32Value(2), types.StringValue("two")),
				NewRowWithHandle(storage.RowHandle{Page: 3, Slot: 2}, types.NullValue(), types.StringValue("null")),
				NewRowWithHandle(storage.RowHandle{Page: 3, Slot: 3}, types.Int32Value(1), types.StringValue("one")),
			},
			want: []Row{
				NewRowWithHandle(storage.RowHandle{Page: 3, Slot: 2}, types.NullValue(), types.StringValue("null")),
				NewRowWithHandle(storage.RowHandle{Page: 3, Slot: 3}, types.Int32Value(1), types.StringValue("one")),
				NewRowWithHandle(storage.RowHandle{Page: 3, Slot: 1}, types.Int32Value(2), types.StringValue("two")),
			},
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			child := &sortTestOperator{
				nextResults: sortResults(testCase.rows...),
			}
			sort := NewSort(child, testCase.keys...)

			if err := sort.Open(); err != nil {
				t.Fatalf("Open() error = %v", err)
			}

			got := make([]Row, 0, len(testCase.want))
			for {
				row, err := sort.Next()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("Next() error = %v", err)
				}

				got = append(got, row)
			}

			assertSortRowsEqual(t, got, testCase.want)

			if err := sort.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
		})
	}
}

func TestSortReturnsEOFRepeatedly(t *testing.T) {
	t.Parallel()

	child := &sortTestOperator{
		nextResults: sortResults(
			NewRow(types.Int32Value(7)),
		),
	}
	sort := NewSort(child, SortKey{Expr: sortTestOrdinalExpr(0)})

	if err := sort.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	row, err := sort.Next()
	if err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	assertSortRowEqual(t, row, NewRow(types.Int32Value(7)))

	if _, err := sort.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next() error = %v, want %v", err, io.EOF)
	}
	if _, err := sort.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("third Next() error = %v, want %v", err, io.EOF)
	}

	if child.nextCalls != 2 {
		t.Fatalf("child Next() calls = %d, want 2", child.nextCalls)
	}

	if err := sort.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestSortOpenFailureLeavesOperatorRetryable(t *testing.T) {
	t.Parallel()

	openErr := errors.New("open failed")
	child := &sortTestOperator{
		openErrs: []error{openErr, nil},
		nextResults: sortResults(
			NewRow(types.Int32Value(5)),
		),
	}
	sort := NewSort(child, SortKey{Expr: sortTestOrdinalExpr(0)})

	if err := sort.Open(); !errors.Is(err, openErr) {
		t.Fatalf("first Open() error = %v, want %v", err, openErr)
	}

	if _, err := sort.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() after failed Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	if err := sort.Open(); err != nil {
		t.Fatalf("second Open() error = %v", err)
	}

	row, err := sort.Next()
	if err != nil {
		t.Fatalf("Next() after retry error = %v", err)
	}
	assertSortRowEqual(t, row, NewRow(types.Int32Value(5)))

	if err := sort.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestSortClosePropagatesToChildOnce(t *testing.T) {
	t.Parallel()

	closeErr := errors.New("close failed")
	child := &sortTestOperator{closeErr: closeErr}
	sort := NewSort(child)

	if err := sort.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := sort.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close() error = %v, want %v", err, closeErr)
	}
	if child.closeCalls != 1 {
		t.Fatalf("child Close() calls = %d, want 1", child.closeCalls)
	}

	if err := sort.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if child.closeCalls != 1 {
		t.Fatalf("child Close() calls after second Close = %d, want 1", child.closeCalls)
	}
}

func TestSortReturnsMaterializationErrorsWithoutRetrying(t *testing.T) {
	t.Parallel()

	runtimeErr := errors.New("sort key failed")
	child := &sortTestOperator{
		nextResults: sortResults(
			NewRow(types.Int32Value(1)),
		),
	}
	sort := NewSort(child, SortKey{
		Expr: sortTestExpr(func(Row) (types.Value, error) {
			return types.Value{}, runtimeErr
		}),
	})

	if err := sort.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	row, err := sort.Next()
	if !errors.Is(err, runtimeErr) {
		t.Fatalf("first Next() error = %v, want %v", err, runtimeErr)
	}
	if row.Len() != 0 {
		t.Fatalf("row.Len() on error = %d, want 0", row.Len())
	}
	if row.Handle != (storage.RowHandle{}) {
		t.Fatalf("row.Handle on error = %#v, want zero handle", row.Handle)
	}

	if _, err := sort.Next(); !errors.Is(err, runtimeErr) {
		t.Fatalf("second Next() error = %v, want %v", err, runtimeErr)
	}
	if child.nextCalls != 1 {
		t.Fatalf("child Next() calls = %d, want 1", child.nextCalls)
	}

	if err := sort.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

type sortTestOperator struct {
	openErrs    []error
	nextResults []sortNextResult
	closeErr    error
	openCalls   int
	nextCalls   int
	closeCalls  int
	opened      bool
	closed      bool
}

func (s *sortTestOperator) Open() error {
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

func (s *sortTestOperator) Next() (Row, error) {
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

func (s *sortTestOperator) Close() error {
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

type sortNextResult struct {
	row Row
	err error
}

func sortResults(rows ...Row) []sortNextResult {
	results := make([]sortNextResult, 0, len(rows))
	for _, row := range rows {
		results = append(results, sortNextResult{row: row})
	}

	return results
}

func sortTestExpr(eval func(Row) (types.Value, error)) CompiledExpr {
	return CompiledExpr{
		typ:  types.TypeDesc{Kind: types.TypeKindInteger, Nullable: true},
		eval: eval,
	}
}

func sortTestOrdinalExpr(ordinal int) CompiledExpr {
	return sortTestExpr(func(row Row) (types.Value, error) {
		value, ok := row.Value(ordinal)
		if !ok {
			return types.Value{}, ErrRowOrdinalOutOfRange
		}

		return value, nil
	})
}

func assertSortRowsEqual(t *testing.T, got []Row, want []Row) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("row count = %d, want %d", len(got), len(want))
	}

	for index := range want {
		assertSortRowEqual(t, got[index], want[index])
	}
}

func assertSortRowEqual(t *testing.T, got Row, want Row) {
	t.Helper()

	if got.Handle != want.Handle {
		t.Fatalf("row.Handle = %#v, want %#v", got.Handle, want.Handle)
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
