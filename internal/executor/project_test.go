package executor

import (
	"errors"
	"io"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/types"
)

func TestProjectLifecycle(t *testing.T) {
	t.Parallel()

	child := &projectTestOperator{}
	project := NewProject(child, projectTestExpr(func(Row) (types.Value, error) {
		return types.Int32Value(1), nil
	}))

	if _, err := project.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() before Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	if err := project.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if child.openCount != 1 {
		t.Fatalf("child open count = %d, want 1", child.openCount)
	}

	if err := project.Open(); !errors.Is(err, ErrOperatorOpen) {
		t.Fatalf("second Open() error = %v, want %v", err, ErrOperatorOpen)
	}
	if child.openCount != 1 {
		t.Fatalf("child open count after duplicate Open = %d, want 1", child.openCount)
	}

	if err := project.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if child.closeCount != 1 {
		t.Fatalf("child close count = %d, want 1", child.closeCount)
	}

	if _, err := project.Next(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Next() after Close error = %v, want %v", err, ErrOperatorClosed)
	}

	if err := project.Open(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Open() after Close error = %v, want %v", err, ErrOperatorClosed)
	}

	if err := project.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if child.closeCount != 1 {
		t.Fatalf("child close count after second Close = %d, want 1", child.closeCount)
	}
}

func TestProjectEvaluatesExpressionsInOrderAndPreservesHandle(t *testing.T) {
	t.Parallel()

	handle := storage.RowHandle{Page: 4, Slot: 7}
	child := &projectTestOperator{}
	child.nextFn = func() (Row, error) {
		if child.nextCount == 1 {
			return NewRowWithHandle(handle, types.Int32Value(7), types.StringValue("alpha")), nil
		}

		return Row{}, io.EOF
	}

	order := make([]string, 0, 3)
	project := NewProject(
		child,
		projectTestExpr(func(row Row) (types.Value, error) {
			order = append(order, "first")
			value, _ := row.Value(1)
			return value, nil
		}),
		projectTestExpr(func(row Row) (types.Value, error) {
			order = append(order, "second")
			value, _ := row.Value(0)
			return value, nil
		}),
		projectTestExpr(func(Row) (types.Value, error) {
			order = append(order, "third")
			return types.StringValue("omega"), nil
		}),
	)

	if err := project.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	row, err := project.Next()
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}

	if row.Handle != handle {
		t.Fatalf("row.Handle = %#v, want %#v", row.Handle, handle)
	}
	if len(order) != 3 || order[0] != "first" || order[1] != "second" || order[2] != "third" {
		t.Fatalf("evaluation order = %v, want [first second third]", order)
	}
	if row.Len() != 3 {
		t.Fatalf("row.Len() = %d, want 3", row.Len())
	}
	assertProjectValue(t, row, 0, types.StringValue("alpha"))
	assertProjectValue(t, row, 1, types.Int32Value(7))
	assertProjectValue(t, row, 2, types.StringValue("omega"))

	if err := project.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestProjectAllocatesFreshOutputRows(t *testing.T) {
	t.Parallel()

	child := &projectTestOperator{}
	child.nextFn = func() (Row, error) {
		switch child.nextCount {
		case 1:
			return NewRow(types.Int32Value(1)), nil
		case 2:
			return NewRow(types.Int32Value(2)), nil
		default:
			return Row{}, io.EOF
		}
	}

	project := NewProject(child, projectTestOrdinalExpr(0))

	if err := project.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	first, err := project.Next()
	if err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	second, err := project.Next()
	if err != nil {
		t.Fatalf("second Next() error = %v", err)
	}

	assertProjectValue(t, first, 0, types.Int32Value(1))
	assertProjectValue(t, second, 0, types.Int32Value(2))

	first.values[0] = types.Int32Value(99)
	assertProjectValue(t, second, 0, types.Int32Value(2))

	if err := project.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestProjectReturnsRuntimeExpressionErrors(t *testing.T) {
	t.Parallel()

	runtimeErr := errors.New("projection failed")
	child := &projectTestOperator{}
	child.nextFn = func() (Row, error) {
		if child.nextCount == 1 {
			return NewRow(types.Int32Value(7)), nil
		}

		return Row{}, io.EOF
	}

	laterCalls := 0
	project := NewProject(
		child,
		projectTestExpr(func(Row) (types.Value, error) {
			return types.StringValue("alpha"), nil
		}),
		projectTestExpr(func(Row) (types.Value, error) {
			return types.Value{}, runtimeErr
		}),
		projectTestExpr(func(Row) (types.Value, error) {
			laterCalls++
			return types.Int32Value(1), nil
		}),
	)

	if err := project.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	row, err := project.Next()
	if !errors.Is(err, runtimeErr) {
		t.Fatalf("Next() error = %v, want %v", err, runtimeErr)
	}
	if row.Len() != 0 {
		t.Fatalf("row.Len() on error = %d, want 0", row.Len())
	}
	if row.Handle != (storage.RowHandle{}) {
		t.Fatalf("row.Handle on error = %#v, want zero handle", row.Handle)
	}
	if laterCalls != 0 {
		t.Fatalf("later expression calls = %d, want 0", laterCalls)
	}

	if err := project.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestProjectReturnsEOFRepeatedly(t *testing.T) {
	t.Parallel()

	child := &projectTestOperator{}
	child.nextFn = func() (Row, error) {
		if child.nextCount == 1 {
			return NewRow(types.Int32Value(3)), nil
		}

		return Row{}, io.EOF
	}

	project := NewProject(child, projectTestOrdinalExpr(0))

	if err := project.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	row, err := project.Next()
	if err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	assertProjectValue(t, row, 0, types.Int32Value(3))

	if _, err := project.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next() error = %v, want %v", err, io.EOF)
	}
	if _, err := project.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("third Next() error = %v, want %v", err, io.EOF)
	}

	if err := project.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestProjectOpenFailureRollsBackLifecycle(t *testing.T) {
	t.Parallel()

	openErr := errors.New("open failed")
	child := &projectTestOperator{}
	child.openFn = func() error {
		if child.openCount == 1 {
			return openErr
		}

		return nil
	}
	child.nextFn = func() (Row, error) {
		return Row{}, io.EOF
	}

	project := NewProject(child)

	if err := project.Open(); !errors.Is(err, openErr) {
		t.Fatalf("first Open() error = %v, want %v", err, openErr)
	}
	if child.closeCount != 0 {
		t.Fatalf("child close count after failed Open = %d, want 0", child.closeCount)
	}

	if _, err := project.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() after failed Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	if err := project.Open(); err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	if child.openCount != 2 {
		t.Fatalf("child open count = %d, want 2", child.openCount)
	}

	if err := project.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if child.closeCount != 1 {
		t.Fatalf("child close count after successful Close = %d, want 1", child.closeCount)
	}
}

func TestProjectClosePropagatesChildErrorOnce(t *testing.T) {
	t.Parallel()

	closeErr := errors.New("close failed")
	child := &projectTestOperator{
		closeFn: func() error {
			return closeErr
		},
	}
	project := NewProject(child)

	if err := project.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := project.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close() error = %v, want %v", err, closeErr)
	}
	if child.closeCount != 1 {
		t.Fatalf("child close count = %d, want 1", child.closeCount)
	}

	if _, err := project.Next(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Next() after Close error = %v, want %v", err, ErrOperatorClosed)
	}

	if err := project.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if child.closeCount != 1 {
		t.Fatalf("child close count after second Close = %d, want 1", child.closeCount)
	}
}

type projectTestOperator struct {
	openFn     func() error
	nextFn     func() (Row, error)
	closeFn    func() error
	openCount  int
	nextCount  int
	closeCount int
}

func (o *projectTestOperator) Open() error {
	o.openCount++
	if o.openFn != nil {
		return o.openFn()
	}

	return nil
}

func (o *projectTestOperator) Next() (Row, error) {
	o.nextCount++
	if o.nextFn != nil {
		return o.nextFn()
	}

	return Row{}, io.EOF
}

func (o *projectTestOperator) Close() error {
	o.closeCount++
	if o.closeFn != nil {
		return o.closeFn()
	}

	return nil
}

func projectTestExpr(eval func(Row) (types.Value, error)) CompiledExpr {
	return CompiledExpr{eval: eval}
}

func projectTestOrdinalExpr(index int) CompiledExpr {
	return projectTestExpr(func(row Row) (types.Value, error) {
		value, ok := row.Value(index)
		if !ok {
			return types.Value{}, ErrRowOrdinalOutOfRange
		}

		return value, nil
	})
}

func assertProjectValue(t *testing.T, row Row, index int, want types.Value) {
	t.Helper()

	value, ok := row.Value(index)
	if !ok {
		t.Fatalf("Value(%d) = (_, false), want (_, true)", index)
	}
	if !value.Equal(want) {
		t.Fatalf("Value(%d) = %v, want %v", index, value, want)
	}
}
