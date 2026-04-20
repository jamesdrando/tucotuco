package executor

import (
	"errors"
	"io"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/types"
)

var dmlTestTable = storage.TableID{Schema: "public", Name: "widgets"}

func TestInsertLifecycle(t *testing.T) {
	t.Parallel()

	insert := NewInsertValues(&stubDMLStorage{}, nil, dmlTestTable, NewRow(types.Int32Value(1)))

	if _, err := insert.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() before Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	if err := insert.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := insert.Open(); !errors.Is(err, ErrOperatorOpen) {
		t.Fatalf("second Open() error = %v, want %v", err, ErrOperatorOpen)
	}

	if err := insert.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if _, err := insert.Next(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Next() after Close error = %v, want %v", err, ErrOperatorClosed)
	}

	if err := insert.Open(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Open() after Close error = %v, want %v", err, ErrOperatorClosed)
	}

	if err := insert.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestInsertValuesWritesRowsAndTracksAffectedRows(t *testing.T) {
	t.Parallel()

	store := &stubDMLStorage{
		insertHandles: []storage.RowHandle{
			{Page: 1, Slot: 11},
			{Page: 1, Slot: 12},
		},
	}
	insert := NewInsertValues(
		store,
		nil,
		dmlTestTable,
		NewRow(types.Int32Value(1), types.StringValue("alpha")),
		NewRow(types.Int32Value(2), types.StringValue("beta")),
	)

	if err := insert.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	first, err := insert.Next()
	if err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	assertDMLRow(t, first, storage.RowHandle{Page: 1, Slot: 11}, types.Int32Value(1), types.StringValue("alpha"))
	if got := insert.AffectedRows(); got != 1 {
		t.Fatalf("AffectedRows() after first row = %d, want 1", got)
	}

	second, err := insert.Next()
	if err != nil {
		t.Fatalf("second Next() error = %v", err)
	}
	assertDMLRow(t, second, storage.RowHandle{Page: 1, Slot: 12}, types.Int32Value(2), types.StringValue("beta"))
	if got := insert.AffectedRows(); got != 2 {
		t.Fatalf("AffectedRows() after second row = %d, want 2", got)
	}

	if len(store.insertCalls) != 2 {
		t.Fatalf("insert call count = %d, want 2", len(store.insertCalls))
	}
	assertStorageRow(t, store.insertCalls[0].row, types.Int32Value(1), types.StringValue("alpha"))
	assertStorageRow(t, store.insertCalls[1].row, types.Int32Value(2), types.StringValue("beta"))

	if _, err := insert.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("third Next() error = %v, want %v", err, io.EOF)
	}
	if _, err := insert.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("fourth Next() error = %v, want %v", err, io.EOF)
	}

	if err := insert.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestInsertFromChildOpenFailureAndClosePropagation(t *testing.T) {
	t.Parallel()

	openErr := errors.New("child open failed")
	closeErr := errors.New("child close failed")
	child := &stubDMLOperator{
		openErrs: []error{openErr, nil},
		nextResults: []dmlNextResult{
			{row: NewRowWithHandle(storage.RowHandle{Page: 9, Slot: 9}, types.Int32Value(7), types.StringValue("gamma"))},
		},
		closeErr: closeErr,
	}
	store := &stubDMLStorage{
		insertHandles: []storage.RowHandle{{Page: 2, Slot: 5}},
	}
	insert := NewInsertFromChild(store, nil, dmlTestTable, child)

	if err := insert.Open(); !errors.Is(err, openErr) {
		t.Fatalf("Open() error = %v, want %v", err, openErr)
	}
	if _, err := insert.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() after failed Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	if err := insert.Open(); err != nil {
		t.Fatalf("second Open() error = %v", err)
	}

	row, err := insert.Next()
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	assertDMLRow(t, row, storage.RowHandle{Page: 2, Slot: 5}, types.Int32Value(7), types.StringValue("gamma"))

	if _, err := insert.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next() error = %v, want %v", err, io.EOF)
	}

	if err := insert.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close() error = %v, want %v", err, closeErr)
	}
	if child.closeCalls != 1 {
		t.Fatalf("child close calls = %d, want 1", child.closeCalls)
	}

	if err := insert.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if child.closeCalls != 1 {
		t.Fatalf("child close calls after second Close = %d, want 1", child.closeCalls)
	}
}

func TestInsertStorageErrorsBecomeTerminal(t *testing.T) {
	t.Parallel()

	insertErr := errors.New("insert failed")
	store := &stubDMLStorage{
		insertHandles: []storage.RowHandle{{Page: 1, Slot: 1}},
		insertErrs:    []error{nil, insertErr},
	}
	insert := NewInsertValues(
		store,
		nil,
		dmlTestTable,
		NewRow(types.Int32Value(1)),
		NewRow(types.Int32Value(2)),
	)

	if err := insert.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	row, err := insert.Next()
	if err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	assertDMLRow(t, row, storage.RowHandle{Page: 1, Slot: 1}, types.Int32Value(1))

	if _, err := insert.Next(); !errors.Is(err, insertErr) {
		t.Fatalf("second Next() error = %v, want %v", err, insertErr)
	}
	if _, err := insert.Next(); !errors.Is(err, insertErr) {
		t.Fatalf("third Next() error = %v, want %v", err, insertErr)
	}
	if got := insert.AffectedRows(); got != 1 {
		t.Fatalf("AffectedRows() after error = %d, want 1", got)
	}
}

func TestUpdateLifecycle(t *testing.T) {
	t.Parallel()

	update := NewUpdate(&stubDMLStorage{}, nil, dmlTestTable, 1, &stubDMLOperator{})

	if _, err := update.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() before Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	if err := update.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := update.Open(); !errors.Is(err, ErrOperatorOpen) {
		t.Fatalf("second Open() error = %v, want %v", err, ErrOperatorOpen)
	}

	if err := update.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if _, err := update.Next(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Next() after Close error = %v, want %v", err, ErrOperatorClosed)
	}

	if err := update.Open(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Open() after Close error = %v, want %v", err, ErrOperatorClosed)
	}
}

func TestUpdateAppliesAssignmentsPreservesTrailingValuesAndTracksAffectedRows(t *testing.T) {
	t.Parallel()

	child := &stubDMLOperator{
		nextResults: []dmlNextResult{
			{
				row: NewRowWithHandle(
					storage.RowHandle{Page: 4, Slot: 7},
					types.Int32Value(3),
					types.Int32Value(9),
					types.StringValue("joined"),
				),
			},
		},
	}
	store := &stubDMLStorage{}
	update := NewUpdate(
		store,
		nil,
		dmlTestTable,
		2,
		child,
		UpdateAssignment{
			Ordinals: []int{0, 1},
			Values: []CompiledExpr{
				testInt32TransformExpr(0, 1),
				testInt32TransformExpr(0, 1),
			},
		},
	)

	if err := update.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	row, err := update.Next()
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}

	assertDMLRow(
		t,
		row,
		storage.RowHandle{Page: 4, Slot: 7},
		types.Int32Value(4),
		types.Int32Value(4),
		types.StringValue("joined"),
	)
	if got := update.AffectedRows(); got != 1 {
		t.Fatalf("AffectedRows() = %d, want 1", got)
	}
	if len(store.updateCalls) != 1 {
		t.Fatalf("update call count = %d, want 1", len(store.updateCalls))
	}
	if got := store.updateCalls[0].handle; got != (storage.RowHandle{Page: 4, Slot: 7}) {
		t.Fatalf("update handle = %#v, want %#v", got, storage.RowHandle{Page: 4, Slot: 7})
	}
	assertStorageRow(t, store.updateCalls[0].row, types.Int32Value(4), types.Int32Value(4))

	if _, err := update.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next() error = %v, want %v", err, io.EOF)
	}
	if _, err := update.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("third Next() error = %v, want %v", err, io.EOF)
	}

	if err := update.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestUpdateContractAndErrorPaths(t *testing.T) {
	t.Parallel()

	evalErr := errors.New("eval failed")
	updateErr := errors.New("update failed")

	tests := []struct {
		name         string
		operator     *Update
		wantOpenErr  error
		wantNextErr  error
		wantUpdates  int
		wantAffected int64
	}{
		{
			name:        "assignment shape mismatch",
			operator:    NewUpdate(&stubDMLStorage{}, nil, dmlTestTable, 1, &stubDMLOperator{}, UpdateAssignment{Ordinals: []int{0}, Values: nil}),
			wantOpenErr: errUpdateAssignmentShape,
		},
		{
			name:        "target ordinal out of range",
			operator:    NewUpdate(&stubDMLStorage{}, nil, dmlTestTable, 1, &stubDMLOperator{}, UpdateAssignment{Ordinals: []int{1}, Values: []CompiledExpr{testConstantExpr(types.Int32Value(1))}}),
			wantOpenErr: errUpdateTargetOrdinalOutOfRange,
		},
		{
			name: "missing handle",
			operator: NewUpdate(
				&stubDMLStorage{},
				nil,
				dmlTestTable,
				1,
				&stubDMLOperator{nextResults: []dmlNextResult{{row: NewRow(types.Int32Value(1))}}},
			),
			wantNextErr: errWriteMissingHandle,
		},
		{
			name: "short target prefix",
			operator: NewUpdate(
				&stubDMLStorage{},
				nil,
				dmlTestTable,
				2,
				&stubDMLOperator{
					nextResults: []dmlNextResult{
						{row: NewRowWithHandle(storage.RowHandle{Page: 1, Slot: 1}, types.Int32Value(1))},
					},
				},
			),
			wantNextErr: errUpdateRowTooShort,
		},
		{
			name: "expression error is terminal",
			operator: NewUpdate(
				&stubDMLStorage{},
				nil,
				dmlTestTable,
				1,
				&stubDMLOperator{
					nextResults: []dmlNextResult{
						{row: NewRowWithHandle(storage.RowHandle{Page: 1, Slot: 2}, types.Int32Value(3))},
					},
				},
				UpdateAssignment{
					Ordinals: []int{0},
					Values:   []CompiledExpr{testErrorExpr(evalErr)},
				},
			),
			wantNextErr: evalErr,
		},
		{
			name: "storage error is terminal",
			operator: NewUpdate(
				&stubDMLStorage{updateErrs: []error{updateErr}},
				nil,
				dmlTestTable,
				1,
				&stubDMLOperator{
					nextResults: []dmlNextResult{
						{row: NewRowWithHandle(storage.RowHandle{Page: 1, Slot: 3}, types.Int32Value(4))},
					},
				},
			),
			wantNextErr: updateErr,
			wantUpdates: 1,
		},
	}

	for _, testCase := range tests {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			store, _ := testCase.operator.store.(*stubDMLStorage)

			if err := testCase.operator.Open(); testCase.wantOpenErr != nil {
				if !errors.Is(err, testCase.wantOpenErr) {
					t.Fatalf("Open() error = %v, want %v", err, testCase.wantOpenErr)
				}
				if _, err := testCase.operator.Next(); !errors.Is(err, ErrOperatorNotOpen) {
					t.Fatalf("Next() after failed Open error = %v, want %v", err, ErrOperatorNotOpen)
				}

				return
			} else if err != nil {
				t.Fatalf("Open() error = %v", err)
			}

			if _, err := testCase.operator.Next(); !errors.Is(err, testCase.wantNextErr) {
				t.Fatalf("Next() error = %v, want %v", err, testCase.wantNextErr)
			}
			if _, err := testCase.operator.Next(); !errors.Is(err, testCase.wantNextErr) {
				t.Fatalf("second Next() error = %v, want %v", err, testCase.wantNextErr)
			}
			if got := testCase.operator.AffectedRows(); got != testCase.wantAffected {
				t.Fatalf("AffectedRows() = %d, want %d", got, testCase.wantAffected)
			}
			if store != nil && len(store.updateCalls) != testCase.wantUpdates {
				t.Fatalf("update call count = %d, want %d", len(store.updateCalls), testCase.wantUpdates)
			}
		})
	}
}

func TestUpdateOpenFailureAndClosePropagation(t *testing.T) {
	t.Parallel()

	openErr := errors.New("child open failed")
	closeErr := errors.New("child close failed")
	child := &stubDMLOperator{
		openErrs: []error{openErr, nil},
		nextResults: []dmlNextResult{
			{row: NewRowWithHandle(storage.RowHandle{Page: 7, Slot: 3}, types.Int32Value(1))},
		},
		closeErr: closeErr,
	}
	update := NewUpdate(
		&stubDMLStorage{},
		nil,
		dmlTestTable,
		1,
		child,
		UpdateAssignment{
			Ordinals: []int{0},
			Values:   []CompiledExpr{testConstantExpr(types.Int32Value(8))},
		},
	)

	if err := update.Open(); !errors.Is(err, openErr) {
		t.Fatalf("Open() error = %v, want %v", err, openErr)
	}
	if _, err := update.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() after failed Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	if err := update.Open(); err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	if _, err := update.Next(); err != nil {
		t.Fatalf("Next() error = %v", err)
	}

	if err := update.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close() error = %v, want %v", err, closeErr)
	}
	if child.closeCalls != 1 {
		t.Fatalf("child close calls = %d, want 1", child.closeCalls)
	}
}

func TestDeleteLifecycle(t *testing.T) {
	t.Parallel()

	del := NewDelete(&stubDMLStorage{}, nil, dmlTestTable, &stubDMLOperator{})

	if _, err := del.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() before Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	if err := del.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := del.Open(); !errors.Is(err, ErrOperatorOpen) {
		t.Fatalf("second Open() error = %v, want %v", err, ErrOperatorOpen)
	}

	if err := del.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if _, err := del.Next(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Next() after Close error = %v, want %v", err, ErrOperatorClosed)
	}
}

func TestDeleteDeletesRowsTracksAffectedRowsAndPropagatesClose(t *testing.T) {
	t.Parallel()

	closeErr := errors.New("child close failed")
	child := &stubDMLOperator{
		nextResults: []dmlNextResult{
			{row: NewRowWithHandle(storage.RowHandle{Page: 5, Slot: 1}, types.Int32Value(1), types.StringValue("alpha"))},
			{row: NewRowWithHandle(storage.RowHandle{Page: 5, Slot: 2}, types.Int32Value(2), types.StringValue("beta"))},
		},
		closeErr: closeErr,
	}
	store := &stubDMLStorage{}
	del := NewDelete(store, nil, dmlTestTable, child)

	if err := del.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	first, err := del.Next()
	if err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	assertDMLRow(t, first, storage.RowHandle{Page: 5, Slot: 1}, types.Int32Value(1), types.StringValue("alpha"))

	second, err := del.Next()
	if err != nil {
		t.Fatalf("second Next() error = %v", err)
	}
	assertDMLRow(t, second, storage.RowHandle{Page: 5, Slot: 2}, types.Int32Value(2), types.StringValue("beta"))

	if got := del.AffectedRows(); got != 2 {
		t.Fatalf("AffectedRows() = %d, want 2", got)
	}
	if len(store.deleteCalls) != 2 {
		t.Fatalf("delete call count = %d, want 2", len(store.deleteCalls))
	}
	if got := store.deleteCalls[0].handle; got != (storage.RowHandle{Page: 5, Slot: 1}) {
		t.Fatalf("first delete handle = %#v, want %#v", got, storage.RowHandle{Page: 5, Slot: 1})
	}
	if got := store.deleteCalls[1].handle; got != (storage.RowHandle{Page: 5, Slot: 2}) {
		t.Fatalf("second delete handle = %#v, want %#v", got, storage.RowHandle{Page: 5, Slot: 2})
	}

	if _, err := del.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("third Next() error = %v, want %v", err, io.EOF)
	}
	if _, err := del.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("fourth Next() error = %v, want %v", err, io.EOF)
	}

	if err := del.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close() error = %v, want %v", err, closeErr)
	}
	if child.closeCalls != 1 {
		t.Fatalf("child close calls = %d, want 1", child.closeCalls)
	}
}

func TestDeleteContractAndErrorPaths(t *testing.T) {
	t.Parallel()

	deleteErr := errors.New("delete failed")
	openErr := errors.New("child open failed")

	tests := []struct {
		name         string
		operator     *Delete
		wantOpenErr  error
		wantNextErr  error
		wantDeletes  int
		wantAffected int64
	}{
		{
			name:        "open failure rolls back lifecycle",
			operator:    NewDelete(&stubDMLStorage{}, nil, dmlTestTable, &stubDMLOperator{openErrs: []error{openErr}}),
			wantOpenErr: openErr,
		},
		{
			name: "missing handle",
			operator: NewDelete(
				&stubDMLStorage{},
				nil,
				dmlTestTable,
				&stubDMLOperator{nextResults: []dmlNextResult{{row: NewRow(types.Int32Value(1))}}},
			),
			wantNextErr: errWriteMissingHandle,
		},
		{
			name: "storage error is terminal",
			operator: NewDelete(
				&stubDMLStorage{deleteErrs: []error{deleteErr}},
				nil,
				dmlTestTable,
				&stubDMLOperator{
					nextResults: []dmlNextResult{
						{row: NewRowWithHandle(storage.RowHandle{Page: 6, Slot: 4}, types.Int32Value(1))},
					},
				},
			),
			wantNextErr: deleteErr,
			wantDeletes: 1,
		},
	}

	for _, testCase := range tests {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			store, _ := testCase.operator.store.(*stubDMLStorage)

			if err := testCase.operator.Open(); testCase.wantOpenErr != nil {
				if !errors.Is(err, testCase.wantOpenErr) {
					t.Fatalf("Open() error = %v, want %v", err, testCase.wantOpenErr)
				}
				if _, err := testCase.operator.Next(); !errors.Is(err, ErrOperatorNotOpen) {
					t.Fatalf("Next() after failed Open error = %v, want %v", err, ErrOperatorNotOpen)
				}

				return
			} else if err != nil {
				t.Fatalf("Open() error = %v", err)
			}

			if _, err := testCase.operator.Next(); !errors.Is(err, testCase.wantNextErr) {
				t.Fatalf("Next() error = %v, want %v", err, testCase.wantNextErr)
			}
			if _, err := testCase.operator.Next(); !errors.Is(err, testCase.wantNextErr) {
				t.Fatalf("second Next() error = %v, want %v", err, testCase.wantNextErr)
			}
			if got := testCase.operator.AffectedRows(); got != testCase.wantAffected {
				t.Fatalf("AffectedRows() = %d, want %d", got, testCase.wantAffected)
			}
			if store != nil && len(store.deleteCalls) != testCase.wantDeletes {
				t.Fatalf("delete call count = %d, want %d", len(store.deleteCalls), testCase.wantDeletes)
			}
		})
	}
}

type stubDMLStorage struct {
	insertHandles []storage.RowHandle
	insertErrs    []error
	updateErrs    []error
	deleteErrs    []error
	insertCalls   []dmlInsertCall
	updateCalls   []dmlUpdateCall
	deleteCalls   []dmlDeleteCall
}

func (s *stubDMLStorage) Insert(
	tx storage.Transaction,
	table storage.TableID,
	row storage.Row,
) (storage.RowHandle, error) {
	s.insertCalls = append(s.insertCalls, dmlInsertCall{
		tx:    tx,
		table: table,
		row:   row.Clone(),
	})

	err := popDMLError(&s.insertErrs)
	if err != nil {
		return storage.RowHandle{}, err
	}
	if len(s.insertHandles) == 0 {
		return storage.RowHandle{Page: 1, Slot: uint64(len(s.insertCalls))}, nil
	}

	handle := s.insertHandles[0]
	s.insertHandles = s.insertHandles[1:]
	return handle, nil
}

func (s *stubDMLStorage) Scan(
	storage.Transaction,
	storage.TableID,
	storage.ScanOptions,
) (storage.RowIterator, error) {
	panic("unexpected Scan call")
}

func (s *stubDMLStorage) Update(
	tx storage.Transaction,
	table storage.TableID,
	handle storage.RowHandle,
	row storage.Row,
) error {
	s.updateCalls = append(s.updateCalls, dmlUpdateCall{
		tx:     tx,
		table:  table,
		handle: handle,
		row:    row.Clone(),
	})

	return popDMLError(&s.updateErrs)
}

func (s *stubDMLStorage) Delete(
	tx storage.Transaction,
	table storage.TableID,
	handle storage.RowHandle,
) error {
	s.deleteCalls = append(s.deleteCalls, dmlDeleteCall{
		tx:     tx,
		table:  table,
		handle: handle,
	})

	return popDMLError(&s.deleteErrs)
}

func (*stubDMLStorage) NewTransaction(storage.TransactionOptions) (storage.Transaction, error) {
	panic("unexpected NewTransaction call")
}

type dmlInsertCall struct {
	tx    storage.Transaction
	table storage.TableID
	row   storage.Row
}

type dmlUpdateCall struct {
	tx     storage.Transaction
	table  storage.TableID
	handle storage.RowHandle
	row    storage.Row
}

type dmlDeleteCall struct {
	tx     storage.Transaction
	table  storage.TableID
	handle storage.RowHandle
}

type stubDMLOperator struct {
	openErrs    []error
	nextResults []dmlNextResult
	closeErr    error
	openCalls   int
	nextCalls   int
	closeCalls  int
	opened      bool
	closed      bool
}

func (s *stubDMLOperator) Open() error {
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

func (s *stubDMLOperator) Next() (Row, error) {
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

func (s *stubDMLOperator) Close() error {
	s.closeCalls++
	s.opened = false
	s.closed = true
	return s.closeErr
}

type dmlNextResult struct {
	row Row
	err error
}

func popDMLError(errs *[]error) error {
	if len(*errs) == 0 {
		return nil
	}

	err := (*errs)[0]
	*errs = (*errs)[1:]
	return err
}

func testConstantExpr(value types.Value) CompiledExpr {
	return CompiledExpr{
		eval: func(Row) (types.Value, error) {
			return value, nil
		},
	}
}

func testErrorExpr(err error) CompiledExpr {
	return CompiledExpr{
		eval: func(Row) (types.Value, error) {
			return types.Value{}, err
		},
	}
}

func testInt32TransformExpr(index int, delta int32) CompiledExpr {
	return CompiledExpr{
		eval: func(row Row) (types.Value, error) {
			value, ok := row.Value(index)
			if !ok {
				return types.Value{}, errors.New("row ordinal out of range")
			}

			raw, ok := value.Raw().(int32)
			if !ok {
				return types.Value{}, errors.New("value is not INT32")
			}

			return types.Int32Value(raw + delta), nil
		},
	}
}

func assertDMLRow(t *testing.T, row Row, wantHandle storage.RowHandle, wantValues ...types.Value) {
	t.Helper()

	if row.Handle != wantHandle {
		t.Fatalf("row.Handle = %#v, want %#v", row.Handle, wantHandle)
	}
	if row.Len() != len(wantValues) {
		t.Fatalf("row.Len() = %d, want %d", row.Len(), len(wantValues))
	}

	for index, want := range wantValues {
		got, ok := row.Value(index)
		if !ok {
			t.Fatalf("row.Value(%d) = (_, false), want (_, true)", index)
		}
		if !got.Equal(want) {
			t.Fatalf("row.Value(%d) = %#v, want %#v", index, got, want)
		}
	}
}

func assertStorageRow(t *testing.T, row storage.Row, wantValues ...types.Value) {
	t.Helper()

	if row.Len() != len(wantValues) {
		t.Fatalf("row.Len() = %d, want %d", row.Len(), len(wantValues))
	}

	for index, want := range wantValues {
		got, ok := row.Value(index)
		if !ok {
			t.Fatalf("row.Value(%d) = (_, false), want (_, true)", index)
		}
		if !got.Equal(want) {
			t.Fatalf("row.Value(%d) = %#v, want %#v", index, got, want)
		}
	}
}
