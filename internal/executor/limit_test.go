package executor

import (
	"errors"
	"io"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/types"
)

func TestLimitLifecycle(t *testing.T) {
	t.Parallel()

	child := &limitStubOperator{}
	limit := NewLimit(child, 1)

	if _, err := limit.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() before Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	if err := limit.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := limit.Open(); !errors.Is(err, ErrOperatorOpen) {
		t.Fatalf("second Open() error = %v, want %v", err, ErrOperatorOpen)
	}

	if err := limit.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if _, err := limit.Next(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Next() after Close error = %v, want %v", err, ErrOperatorClosed)
	}

	if err := limit.Open(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Open() after Close error = %v, want %v", err, ErrOperatorClosed)
	}

	if err := limit.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	if child.openCalls != 1 {
		t.Fatalf("child Open() calls = %d, want 1", child.openCalls)
	}
	if child.closeCalls != 1 {
		t.Fatalf("child Close() calls = %d, want 1", child.closeCalls)
	}
}

func TestLimitZeroCountReturnsEOFWithoutAdvancingChild(t *testing.T) {
	t.Parallel()

	child := &limitStubOperator{
		rows: []Row{
			NewRow(types.Int32Value(1)),
		},
	}
	limit := NewLimit(child, 0)

	if err := limit.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if _, err := limit.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("first Next() error = %v, want %v", err, io.EOF)
	}

	if _, err := limit.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next() error = %v, want %v", err, io.EOF)
	}

	if child.nextCalls != 0 {
		t.Fatalf("child Next() calls = %d, want 0", child.nextCalls)
	}

	if err := limit.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestLimitStopsAfterConfiguredRowCount(t *testing.T) {
	t.Parallel()

	child := &limitStubOperator{
		rows: []Row{
			NewRow(types.Int32Value(1)),
			NewRow(types.Int32Value(2)),
			NewRow(types.Int32Value(3)),
		},
	}
	limit := NewLimit(child, 2)

	if err := limit.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	first, err := limit.Next()
	if err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	assertLimitIntValue(t, first, 0, 1)

	second, err := limit.Next()
	if err != nil {
		t.Fatalf("second Next() error = %v", err)
	}
	assertLimitIntValue(t, second, 0, 2)

	if _, err := limit.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("third Next() error = %v, want %v", err, io.EOF)
	}

	if _, err := limit.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("fourth Next() error = %v, want %v", err, io.EOF)
	}

	if child.nextCalls != 2 {
		t.Fatalf("child Next() calls = %d, want 2", child.nextCalls)
	}

	if err := limit.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestLimitReturnsChildEOFRepeatedly(t *testing.T) {
	t.Parallel()

	child := &limitStubOperator{
		rows: []Row{
			NewRow(types.Int32Value(1)),
		},
	}
	limit := NewLimit(child, 3)

	if err := limit.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	first, err := limit.Next()
	if err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	assertLimitIntValue(t, first, 0, 1)

	if _, err := limit.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next() error = %v, want %v", err, io.EOF)
	}

	if _, err := limit.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("third Next() error = %v, want %v", err, io.EOF)
	}

	if child.nextCalls != 2 {
		t.Fatalf("child Next() calls = %d, want 2", child.nextCalls)
	}

	if err := limit.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestLimitOpenFailureLeavesOperatorRetryable(t *testing.T) {
	t.Parallel()

	openErr := errors.New("open failed")
	child := &limitStubOperator{openErr: openErr}
	limit := NewLimit(child, 1)

	if err := limit.Open(); !errors.Is(err, openErr) {
		t.Fatalf("Open() error = %v, want %v", err, openErr)
	}

	if _, err := limit.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() after failed Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	child.openErr = nil

	if err := limit.Open(); err != nil {
		t.Fatalf("Open() retry error = %v", err)
	}

	if child.closeCalls != 0 {
		t.Fatalf("child Close() calls after failed Open = %d, want 0", child.closeCalls)
	}

	if err := limit.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestLimitPreservesChildRowHandle(t *testing.T) {
	t.Parallel()

	want := NewRowWithHandle(
		storage.RowHandle{Page: 4, Slot: 9},
		types.Int32Value(7),
		types.StringValue("alpha"),
	)
	child := &limitStubOperator{rows: []Row{want}}
	limit := NewLimit(child, 1)

	if err := limit.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	got, err := limit.Next()
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}

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

	if err := limit.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestLimitClosePropagatesToChildOnce(t *testing.T) {
	t.Parallel()

	child := &limitStubOperator{}
	limit := NewLimit(child, 1)

	if err := limit.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := limit.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := limit.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	if child.closeCalls != 1 {
		t.Fatalf("child Close() calls = %d, want 1", child.closeCalls)
	}
}

func TestLimitWithOffsetSkipsLeadingRows(t *testing.T) {
	t.Parallel()

	child := &limitStubOperator{
		rows: []Row{
			NewRow(types.Int32Value(1)),
			NewRow(types.Int32Value(2)),
			NewRow(types.Int32Value(3)),
		},
	}
	limit := NewLimitWithOffset(child, 2, 1)

	if err := limit.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	first, err := limit.Next()
	if err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	assertLimitIntValue(t, first, 0, 2)

	second, err := limit.Next()
	if err != nil {
		t.Fatalf("second Next() error = %v", err)
	}
	assertLimitIntValue(t, second, 0, 3)

	if _, err := limit.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("third Next() error = %v, want %v", err, io.EOF)
	}

	if child.nextCalls != 3 {
		t.Fatalf("child Next() calls = %d, want 3", child.nextCalls)
	}

	if err := limit.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

type limitStubOperator struct {
	lifecycle  lifecycle
	rows       []Row
	index      int
	openErr    error
	closeErr   error
	openCalls  int
	nextCalls  int
	closeCalls int
}

func (s *limitStubOperator) Open() error {
	s.openCalls++

	if err := s.lifecycle.Open(); err != nil {
		return err
	}
	if s.openErr != nil {
		s.lifecycle = lifecycle{}

		return s.openErr
	}

	s.index = 0

	return nil
}

func (s *limitStubOperator) Next() (Row, error) {
	if err := s.lifecycle.Next(); err != nil {
		return Row{}, err
	}

	s.nextCalls++

	if s.index >= len(s.rows) {
		return Row{}, io.EOF
	}

	row := s.rows[s.index]
	s.index++

	return row, nil
}

func (s *limitStubOperator) Close() error {
	s.closeCalls++

	if err := s.lifecycle.Close(); err != nil {
		return err
	}

	return s.closeErr
}

func assertLimitIntValue(t *testing.T, row Row, index int, want int32) {
	t.Helper()

	value, ok := row.Value(index)
	if !ok {
		t.Fatalf("Value(%d) = (_, false), want (_, true)", index)
	}
	if !value.Equal(types.Int32Value(want)) {
		t.Fatalf("Value(%d) = %v, want %v", index, value, types.Int32Value(want))
	}
}
