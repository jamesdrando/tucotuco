package executor

import (
	"errors"
	"io"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/types"
)

func TestFilterLifecycle(t *testing.T) {
	t.Parallel()

	child := &stubFilterOperator{}
	filter := NewFilter(child, filterCompiledPredicate(func(Row) (types.Value, error) {
		return types.BoolValue(true), nil
	}))

	if _, err := filter.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() before Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	if err := filter.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := filter.Open(); !errors.Is(err, ErrOperatorOpen) {
		t.Fatalf("second Open() error = %v, want %v", err, ErrOperatorOpen)
	}

	if err := filter.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if _, err := filter.Next(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Next() after Close error = %v, want %v", err, ErrOperatorClosed)
	}

	if err := filter.Open(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Open() after Close error = %v, want %v", err, ErrOperatorClosed)
	}

	if err := filter.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestFilterPassesOnlySQLTrueAndPreservesHandle(t *testing.T) {
	t.Parallel()

	passingHandle := storage.RowHandle{Page: 3, Slot: 7}
	passingRow := NewRowWithHandle(
		passingHandle,
		types.BoolValue(true),
		types.StringValue("alpha"),
	)

	child := &stubFilterOperator{
		nextResults: []filterNextResult{
			{row: NewRowWithHandle(storage.RowHandle{Page: 4, Slot: 2}, types.BoolValue(false), types.StringValue("beta"))},
			{row: NewRowWithHandle(storage.RowHandle{Page: 5, Slot: 1}, types.NullValue(), types.StringValue("gamma"))},
			{row: passingRow},
		},
	}
	filter := NewFilter(child, filterCompiledPredicate(func(row Row) (types.Value, error) {
		value, ok := row.Value(0)
		if !ok {
			t.Fatal("predicate row.Value(0) = (_, false), want (_, true)")
		}

		return value, nil
	}))

	if err := filter.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	got, err := filter.Next()
	if err != nil {
		t.Fatalf("first Next() error = %v", err)
	}

	if got.Handle != passingHandle {
		t.Fatalf("row.Handle = %#v, want %#v", got.Handle, passingHandle)
	}

	assertFilterValue(t, got, 0, types.BoolValue(true))
	assertFilterValue(t, got, 1, types.StringValue("alpha"))

	if _, err := filter.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next() error = %v, want %v", err, io.EOF)
	}

	if err := filter.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestFilterNextReturnsEOFRepeatedly(t *testing.T) {
	t.Parallel()

	child := &stubFilterOperator{
		nextResults: []filterNextResult{
			{row: NewRow(types.BoolValue(false))},
		},
	}
	filter := NewFilter(child, filterCompiledPredicate(func(row Row) (types.Value, error) {
		value, ok := row.Value(0)
		if !ok {
			t.Fatal("predicate row.Value(0) = (_, false), want (_, true)")
		}

		return value, nil
	}))

	if err := filter.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if _, err := filter.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("first Next() error = %v, want %v", err, io.EOF)
	}

	if _, err := filter.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next() error = %v, want %v", err, io.EOF)
	}

	if err := filter.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestFilterOpenFailureLeavesOperatorNotOpen(t *testing.T) {
	t.Parallel()

	openErr := errors.New("child open failed")
	child := &stubFilterOperator{
		openErrs: []error{openErr, nil},
	}
	filter := NewFilter(child, filterCompiledPredicate(func(Row) (types.Value, error) {
		return types.BoolValue(true), nil
	}))

	if err := filter.Open(); !errors.Is(err, openErr) {
		t.Fatalf("Open() error = %v, want %v", err, openErr)
	}

	if _, err := filter.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() after failed Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	if err := filter.Open(); err != nil {
		t.Fatalf("second Open() error = %v", err)
	}

	if err := filter.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestFilterClosePropagatesToChildOnce(t *testing.T) {
	t.Parallel()

	closeErr := errors.New("child close failed")
	child := &stubFilterOperator{closeErr: closeErr}
	filter := NewFilter(child, filterCompiledPredicate(func(Row) (types.Value, error) {
		return types.BoolValue(true), nil
	}))

	if err := filter.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := filter.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close() error = %v, want %v", err, closeErr)
	}

	if child.closeCalls != 1 {
		t.Fatalf("child close calls = %d, want 1", child.closeCalls)
	}

	if err := filter.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	if child.closeCalls != 1 {
		t.Fatalf("child close calls after second Close = %d, want 1", child.closeCalls)
	}
}

type stubFilterOperator struct {
	openErrs    []error
	nextResults []filterNextResult
	closeErr    error
	openCalls   int
	nextCalls   int
	closeCalls  int
	opened      bool
	closed      bool
}

func (s *stubFilterOperator) Open() error {
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

func (s *stubFilterOperator) Next() (Row, error) {
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

func (s *stubFilterOperator) Close() error {
	s.closeCalls++
	s.opened = false
	s.closed = true

	return s.closeErr
}

type filterNextResult struct {
	row Row
	err error
}

func filterCompiledPredicate(eval func(Row) (types.Value, error)) CompiledExpr {
	return CompiledExpr{
		typ:  types.TypeDesc{Kind: types.TypeKindBoolean, Nullable: true},
		eval: eval,
	}
}

func assertFilterValue(t *testing.T, row Row, index int, want types.Value) {
	t.Helper()

	value, ok := row.Value(index)
	if !ok {
		t.Fatalf("Value(%d) = (_, false), want (_, true)", index)
	}
	if !value.Equal(want) {
		t.Fatalf("Value(%d) = %v, want %v", index, value, want)
	}
}
