package memory_test

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/storage/memory"
	"github.com/jamesdrando/tucotuco/internal/types"
)

var testTable = storage.TableID{Schema: "public", Name: "widgets"}

func TestStoreImplementsStorage(t *testing.T) {
	t.Parallel()

	var _ storage.Storage = (*memory.Store)(nil)
}

func TestNewTransactionOptions(t *testing.T) {
	t.Parallel()

	store := memory.New()

	defaultTx := mustNewTransaction(t, store, storage.TransactionOptions{})
	if got := defaultTx.IsolationLevel(); got != storage.IsolationReadCommitted {
		t.Fatalf("default isolation = %q, want %q", got, storage.IsolationReadCommitted)
	}
	if defaultTx.ReadOnly() {
		t.Fatal("default transaction is read-only, want writable")
	}

	customTx := mustNewTransaction(t, store, storage.TransactionOptions{
		Isolation: storage.IsolationSerializable,
		ReadOnly:  true,
	})
	if got := customTx.IsolationLevel(); got != storage.IsolationSerializable {
		t.Fatalf("custom isolation = %q, want %q", got, storage.IsolationSerializable)
	}
	if !customTx.ReadOnly() {
		t.Fatal("custom transaction is writable, want read-only")
	}
}

func TestScanEmptyTable(t *testing.T) {
	t.Parallel()

	store := memory.New()
	tx := mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true})

	iter := mustScan(t, store, tx, testTable, storage.ScanOptions{})
	defer closeIterator(t, iter)

	if _, err := iter.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("Next() error = %v, want io.EOF", err)
	}
}

func TestInsertCommitAndScan(t *testing.T) {
	t.Parallel()

	store := memory.New()
	writeTx := mustNewTransaction(t, store, storage.TransactionOptions{})
	handle, err := store.Insert(writeTx, testTable, storage.NewRow(
		types.Int32Value(1),
		types.StringValue("alpha"),
		types.BytesValue([]byte{1, 2, 3}),
	))
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if !handle.Valid() {
		t.Fatal("Insert() returned an invalid handle")
	}

	if err := writeTx.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	readTx := mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true})
	records := collectRecords(t, mustScan(t, store, readTx, testTable, storage.ScanOptions{}))
	if len(records) != 1 {
		t.Fatalf("len(records) = %d, want 1", len(records))
	}
	if got := records[0].Handle; got != handle {
		t.Fatalf("scan handle = %v, want %v", got, handle)
	}

	value, ok := records[0].Row.Value(1)
	if !ok || !value.Equal(types.StringValue("alpha")) {
		t.Fatalf("row value = (%v, %t), want (%v, true)", value, ok, types.StringValue("alpha"))
	}
}

func TestInsertRollbackDiscardsRows(t *testing.T) {
	t.Parallel()

	store := memory.New()
	tx := mustNewTransaction(t, store, storage.TransactionOptions{})

	if _, err := store.Insert(tx, testTable, storage.NewRow(types.Int32Value(1))); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	readTx := mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true})
	records := collectRecords(t, mustScan(t, store, readTx, testTable, storage.ScanOptions{}))
	if len(records) != 0 {
		t.Fatalf("len(records) = %d, want 0", len(records))
	}
}

func TestReadOnlyTransactionRejectsWrites(t *testing.T) {
	t.Parallel()

	store := memory.New()
	tx := mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true})

	writeOps := []struct {
		name string
		run  func() error
	}{
		{
			name: "insert",
			run: func() error {
				_, err := store.Insert(tx, testTable, storage.NewRow(types.Int32Value(1)))
				return err
			},
		},
		{
			name: "update",
			run: func() error {
				return store.Update(tx, testTable, storage.RowHandle{Page: 1, Slot: 1}, storage.NewRow(types.Int32Value(1)))
			},
		},
		{
			name: "delete",
			run: func() error {
				return store.Delete(tx, testTable, storage.RowHandle{Page: 1, Slot: 1})
			},
		},
	}

	for _, tc := range writeOps {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if err := tc.run(); !errors.Is(err, memory.ErrReadOnlyTransaction) {
				t.Fatalf("error = %v, want %v", err, memory.ErrReadOnlyTransaction)
			}
		})
	}
}

func TestUpdateLifecycle(t *testing.T) {
	t.Parallel()

	store := memory.New()
	handle := insertCommittedRow(t, store, storage.NewRow(types.Int32Value(1), types.StringValue("before")))

	t.Run("commit", func(t *testing.T) {
		t.Parallel()

		tx := mustNewTransaction(t, store, storage.TransactionOptions{})
		if err := store.Update(tx, testTable, handle, storage.NewRow(types.Int32Value(1), types.StringValue("after"))); err != nil {
			t.Fatalf("Update() error = %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit() error = %v", err)
		}

		records := collectRecords(t, mustScan(t, store, mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true}), testTable, storage.ScanOptions{}))
		assertSingleStringValue(t, records, "after")
	})

	t.Run("rollback", func(t *testing.T) {
		t.Parallel()

		localStore := memory.New()
		localHandle := insertCommittedRow(t, localStore, storage.NewRow(types.Int32Value(1), types.StringValue("before")))

		tx := mustNewTransaction(t, localStore, storage.TransactionOptions{})
		if err := localStore.Update(tx, testTable, localHandle, storage.NewRow(types.Int32Value(1), types.StringValue("after"))); err != nil {
			t.Fatalf("Update() error = %v", err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback() error = %v", err)
		}

		records := collectRecords(t, mustScan(t, localStore, mustNewTransaction(t, localStore, storage.TransactionOptions{ReadOnly: true}), testTable, storage.ScanOptions{}))
		assertSingleStringValue(t, records, "before")
	})
}

func TestDeleteLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("commit", func(t *testing.T) {
		t.Parallel()

		store := memory.New()
		handle := insertCommittedRow(t, store, storage.NewRow(types.Int32Value(1), types.StringValue("alpha")))

		tx := mustNewTransaction(t, store, storage.TransactionOptions{})
		if err := store.Delete(tx, testTable, handle); err != nil {
			t.Fatalf("Delete() error = %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit() error = %v", err)
		}

		records := collectRecords(t, mustScan(t, store, mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true}), testTable, storage.ScanOptions{}))
		if len(records) != 0 {
			t.Fatalf("len(records) = %d, want 0", len(records))
		}
	})

	t.Run("rollback", func(t *testing.T) {
		t.Parallel()

		store := memory.New()
		handle := insertCommittedRow(t, store, storage.NewRow(types.Int32Value(1), types.StringValue("alpha")))

		tx := mustNewTransaction(t, store, storage.TransactionOptions{})
		if err := store.Delete(tx, testTable, handle); err != nil {
			t.Fatalf("Delete() error = %v", err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback() error = %v", err)
		}

		records := collectRecords(t, mustScan(t, store, mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true}), testTable, storage.ScanOptions{}))
		assertSingleStringValue(t, records, "alpha")
	})
}

func TestTransactionSeesOwnWrites(t *testing.T) {
	t.Parallel()

	store := memory.New()
	tx := mustNewTransaction(t, store, storage.TransactionOptions{})

	handle, err := store.Insert(tx, testTable, storage.NewRow(types.Int32Value(1), types.StringValue("alpha")))
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	assertSingleStringValue(t, collectRecords(t, mustScan(t, store, tx, testTable, storage.ScanOptions{})), "alpha")

	if err := store.Update(tx, testTable, handle, storage.NewRow(types.Int32Value(1), types.StringValue("beta"))); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	assertSingleStringValue(t, collectRecords(t, mustScan(t, store, tx, testTable, storage.ScanOptions{})), "beta")

	if err := store.Delete(tx, testTable, handle); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if records := collectRecords(t, mustScan(t, store, tx, testTable, storage.ScanOptions{})); len(records) != 0 {
		t.Fatalf("len(records) = %d, want 0", len(records))
	}
}

func TestScanUsesStatementSnapshot(t *testing.T) {
	t.Parallel()

	store := memory.New()
	insertCommittedRow(t, store, storage.NewRow(types.Int32Value(1), types.StringValue("seed")))

	tx := mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true})
	iter := mustScan(t, store, tx, testTable, storage.ScanOptions{})

	insertCommittedRow(t, store, storage.NewRow(types.Int32Value(2), types.StringValue("late")))

	records := collectRecords(t, iter)
	got := recordInts(t, records)
	want := []int32{1}
	if len(got) != len(want) {
		t.Fatalf("record count = %d, want %d (%v)", len(got), len(want), want)
	}
	for index := range got {
		if got[index] != want[index] {
			t.Fatalf("records = %v, want %v", got, want)
		}
	}
}

func TestReadCommittedRefreshesSnapshotPerScan(t *testing.T) {
	t.Parallel()

	store := memory.New()
	insertCommittedRow(t, store, storage.NewRow(types.Int32Value(1), types.StringValue("seed")))

	tx := mustNewTransaction(t, store, storage.TransactionOptions{})
	if _, err := store.Insert(tx, testTable, storage.NewRow(types.Int32Value(10), types.StringValue("pending"))); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	first := recordInts(t, collectRecords(t, mustScan(t, store, tx, testTable, storage.ScanOptions{})))
	if len(first) != 2 || first[0] != 1 || first[1] != 10 {
		t.Fatalf("first scan = %v, want [1 10]", first)
	}

	insertCommittedRow(t, store, storage.NewRow(types.Int32Value(2), types.StringValue("late")))

	second := recordInts(t, collectRecords(t, mustScan(t, store, tx, testTable, storage.ScanOptions{})))
	want := []int32{1, 10, 2}
	if len(second) != len(want) {
		t.Fatalf("second scan length = %d, want %d (%v)", len(second), len(want), want)
	}
	for index := range second {
		if second[index] != want[index] {
			t.Fatalf("second scan = %v, want %v", second, want)
		}
	}
}

func TestRepeatableReadKeepsPinnedSnapshot(t *testing.T) {
	t.Parallel()

	store := memory.New()
	insertCommittedRow(t, store, storage.NewRow(types.Int32Value(1), types.StringValue("seed")))

	tx := mustNewTransaction(t, store, storage.TransactionOptions{Isolation: storage.IsolationRepeatableRead})
	if _, err := store.Insert(tx, testTable, storage.NewRow(types.Int32Value(10), types.StringValue("pending"))); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	first := recordInts(t, collectRecords(t, mustScan(t, store, tx, testTable, storage.ScanOptions{})))
	if len(first) != 2 || first[0] != 1 || first[1] != 10 {
		t.Fatalf("first scan = %v, want [1 10]", first)
	}

	insertCommittedRow(t, store, storage.NewRow(types.Int32Value(2), types.StringValue("late")))

	second := recordInts(t, collectRecords(t, mustScan(t, store, tx, testTable, storage.ScanOptions{})))
	want := []int32{1, 10}
	if len(second) != len(want) {
		t.Fatalf("second scan length = %d, want %d (%v)", len(second), len(want), want)
	}
	for index := range second {
		if second[index] != want[index] {
			t.Fatalf("second scan = %v, want %v", second, want)
		}
	}
}

func TestSerializableConflictOnConcurrentChange(t *testing.T) {
	t.Parallel()

	store := memory.New()
	insertCommittedRow(t, store, storage.NewRow(types.Int32Value(1), types.StringValue("seed")))

	tx := mustNewTransaction(t, store, storage.TransactionOptions{Isolation: storage.IsolationSerializable})
	if _, err := store.Insert(tx, testTable, storage.NewRow(types.Int32Value(10), types.StringValue("pending"))); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	insertCommittedRow(t, store, storage.NewRow(types.Int32Value(2), types.StringValue("late")))

	if err := tx.Commit(); !errors.Is(err, memory.ErrSerializationConflict) {
		t.Fatalf("Commit() error = %v, want %v", err, memory.ErrSerializationConflict)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback() after conflict error = %v", err)
	}
}

func TestSerializableCommitIgnoresTombstoneOnlyChanges(t *testing.T) {
	t.Parallel()

	store := memory.New()
	tx := mustNewTransaction(t, store, storage.TransactionOptions{Isolation: storage.IsolationSerializable, ReadOnly: true})

	iter := mustScan(t, store, tx, testTable, storage.ScanOptions{})
	defer closeIterator(t, iter)
	if _, err := iter.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("Next() error = %v, want io.EOF", err)
	}

	writeTx := mustNewTransaction(t, store, storage.TransactionOptions{})
	handle, err := store.Insert(writeTx, testTable, storage.NewRow(types.Int32Value(1), types.StringValue("transient")))
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if err := store.Delete(writeTx, testTable, handle); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if err := writeTx.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
}

func TestInvalidTransactionRejected(t *testing.T) {
	t.Parallel()

	store := memory.New()
	otherStore := memory.New()
	tx := mustNewTransaction(t, otherStore, storage.TransactionOptions{})

	ops := []struct {
		name string
		run  func() error
	}{
		{
			name: "scan",
			run: func() error {
				_, err := store.Scan(tx, testTable, storage.ScanOptions{})
				return err
			},
		},
		{
			name: "insert",
			run: func() error {
				_, err := store.Insert(tx, testTable, storage.NewRow(types.Int32Value(1)))
				return err
			},
		},
		{
			name: "update",
			run: func() error {
				return store.Update(tx, testTable, storage.RowHandle{Page: 1, Slot: 1}, storage.NewRow(types.Int32Value(1)))
			},
		},
		{
			name: "delete",
			run: func() error {
				return store.Delete(tx, testTable, storage.RowHandle{Page: 1, Slot: 1})
			},
		},
	}

	for _, tc := range ops {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if err := tc.run(); !errors.Is(err, memory.ErrInvalidTransaction) {
				t.Fatalf("error = %v, want %v", err, memory.ErrInvalidTransaction)
			}
		})
	}
}

func TestClosedTransactionRejected(t *testing.T) {
	t.Parallel()

	store := memory.New()
	tx := mustNewTransaction(t, store, storage.TransactionOptions{})
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, memory.ErrTransactionClosed) {
		t.Fatalf("second Commit() error = %v, want %v", err, memory.ErrTransactionClosed)
	}
	if err := tx.Rollback(); !errors.Is(err, memory.ErrTransactionClosed) {
		t.Fatalf("Rollback() after commit error = %v, want %v", err, memory.ErrTransactionClosed)
	}

	if _, err := store.Scan(tx, testTable, storage.ScanOptions{}); !errors.Is(err, memory.ErrTransactionClosed) {
		t.Fatalf("Scan() error = %v, want %v", err, memory.ErrTransactionClosed)
	}
	if _, err := store.Insert(tx, testTable, storage.NewRow(types.Int32Value(1))); !errors.Is(err, memory.ErrTransactionClosed) {
		t.Fatalf("Insert() error = %v, want %v", err, memory.ErrTransactionClosed)
	}
}

func TestInvalidTableRejected(t *testing.T) {
	t.Parallel()

	store := memory.New()
	tx := mustNewTransaction(t, store, storage.TransactionOptions{})
	invalidTable := storage.TableID{Schema: "public"}

	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "scan",
			run: func() error {
				_, err := store.Scan(tx, invalidTable, storage.ScanOptions{})
				return err
			},
		},
		{
			name: "insert",
			run: func() error {
				_, err := store.Insert(tx, invalidTable, storage.NewRow(types.Int32Value(1)))
				return err
			},
		},
		{
			name: "update",
			run: func() error {
				return store.Update(tx, invalidTable, storage.RowHandle{Page: 1, Slot: 1}, storage.NewRow(types.Int32Value(1)))
			},
		},
		{
			name: "delete",
			run: func() error {
				return store.Delete(tx, invalidTable, storage.RowHandle{Page: 1, Slot: 1})
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if err := tc.run(); !errors.Is(err, memory.ErrInvalidTable) {
				t.Fatalf("error = %v, want %v", err, memory.ErrInvalidTable)
			}
		})
	}
}

func TestMissingRowRejected(t *testing.T) {
	t.Parallel()

	store := memory.New()
	tx := mustNewTransaction(t, store, storage.TransactionOptions{})
	handle := storage.RowHandle{Page: 1, Slot: 99}

	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "update",
			run: func() error {
				return store.Update(tx, testTable, handle, storage.NewRow(types.Int32Value(1)))
			},
		},
		{
			name: "delete",
			run: func() error {
				return store.Delete(tx, testTable, handle)
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if err := tc.run(); !errors.Is(err, memory.ErrRowNotFound) {
				t.Fatalf("error = %v, want %v", err, memory.ErrRowNotFound)
			}
		})
	}
}

func TestDeletePendingInsertRemovesIt(t *testing.T) {
	t.Parallel()

	store := memory.New()
	tx := mustNewTransaction(t, store, storage.TransactionOptions{})

	handle, err := store.Insert(tx, testTable, storage.NewRow(types.Int32Value(1), types.StringValue("alpha")))
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if err := store.Delete(tx, testTable, handle); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	records := collectRecords(t, mustScan(t, store, mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true}), testTable, storage.ScanOptions{}))
	if len(records) != 0 {
		t.Fatalf("len(records) = %d, want 0", len(records))
	}
}

func TestUpdateAfterPendingDeleteFails(t *testing.T) {
	t.Parallel()

	store := memory.New()
	handle := insertCommittedRow(t, store, storage.NewRow(types.Int32Value(1), types.StringValue("alpha")))

	tx := mustNewTransaction(t, store, storage.TransactionOptions{})
	if err := store.Delete(tx, testTable, handle); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if err := store.Update(tx, testTable, handle, storage.NewRow(types.Int32Value(1), types.StringValue("beta"))); !errors.Is(err, memory.ErrRowNotFound) {
		t.Fatalf("Update() error = %v, want %v", err, memory.ErrRowNotFound)
	}
}

func TestScanLimit(t *testing.T) {
	t.Parallel()

	store := memory.New()
	for _, value := range []int32{1, 2, 3, 4} {
		insertCommittedRow(t, store, storage.NewRow(types.Int32Value(value), types.StringValue("row")))
	}

	records := collectRecords(t, mustScan(t, store, mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true}), testTable, storage.ScanOptions{Limit: 2}))
	if len(records) != 2 {
		t.Fatalf("len(records) = %d, want 2", len(records))
	}
}

func TestScanConstraints(t *testing.T) {
	t.Parallel()

	store := memory.New()
	for _, value := range []int32{1, 2, 3, 4, 5} {
		insertCommittedRow(t, store, storage.NewRow(types.Int32Value(value), types.StringValue("row")))
	}

	tests := []struct {
		name        string
		constraints []storage.ScanConstraint
		want        []int32
	}{
		{
			name: "equal one",
			constraints: []storage.ScanConstraint{
				{Column: 0, Op: storage.ComparisonEqual, Value: types.Int32Value(1)},
			},
			want: []int32{1},
		},
		{
			name: "equal three",
			constraints: []storage.ScanConstraint{
				{Column: 0, Op: storage.ComparisonEqual, Value: types.Int32Value(3)},
			},
			want: []int32{3},
		},
		{
			name: "not equal one",
			constraints: []storage.ScanConstraint{
				{Column: 0, Op: storage.ComparisonNotEqual, Value: types.Int32Value(1)},
			},
			want: []int32{2, 3, 4, 5},
		},
		{
			name: "not equal five",
			constraints: []storage.ScanConstraint{
				{Column: 0, Op: storage.ComparisonNotEqual, Value: types.Int32Value(5)},
			},
			want: []int32{1, 2, 3, 4},
		},
		{
			name: "less two",
			constraints: []storage.ScanConstraint{
				{Column: 0, Op: storage.ComparisonLess, Value: types.Int32Value(2)},
			},
			want: []int32{1},
		},
		{
			name: "less four",
			constraints: []storage.ScanConstraint{
				{Column: 0, Op: storage.ComparisonLess, Value: types.Int32Value(4)},
			},
			want: []int32{1, 2, 3},
		},
		{
			name: "less or equal two",
			constraints: []storage.ScanConstraint{
				{Column: 0, Op: storage.ComparisonLessOrEqual, Value: types.Int32Value(2)},
			},
			want: []int32{1, 2},
		},
		{
			name: "less or equal five",
			constraints: []storage.ScanConstraint{
				{Column: 0, Op: storage.ComparisonLessOrEqual, Value: types.Int32Value(5)},
			},
			want: []int32{1, 2, 3, 4, 5},
		},
		{
			name: "greater one",
			constraints: []storage.ScanConstraint{
				{Column: 0, Op: storage.ComparisonGreater, Value: types.Int32Value(1)},
			},
			want: []int32{2, 3, 4, 5},
		},
		{
			name: "greater four",
			constraints: []storage.ScanConstraint{
				{Column: 0, Op: storage.ComparisonGreater, Value: types.Int32Value(4)},
			},
			want: []int32{5},
		},
		{
			name: "greater or equal one",
			constraints: []storage.ScanConstraint{
				{Column: 0, Op: storage.ComparisonGreaterOrEqual, Value: types.Int32Value(1)},
			},
			want: []int32{1, 2, 3, 4, 5},
		},
		{
			name: "greater or equal four",
			constraints: []storage.ScanConstraint{
				{Column: 0, Op: storage.ComparisonGreaterOrEqual, Value: types.Int32Value(4)},
			},
			want: []int32{4, 5},
		},
		{
			name: "compound intersection",
			constraints: []storage.ScanConstraint{
				{Column: 0, Op: storage.ComparisonGreaterOrEqual, Value: types.Int32Value(2)},
				{Column: 0, Op: storage.ComparisonLessOrEqual, Value: types.Int32Value(4)},
			},
			want: []int32{2, 3, 4},
		},
		{
			name: "compound exact miss",
			constraints: []storage.ScanConstraint{
				{Column: 0, Op: storage.ComparisonGreater, Value: types.Int32Value(2)},
				{Column: 0, Op: storage.ComparisonLess, Value: types.Int32Value(3)},
			},
			want: nil,
		},
		{
			name: "string equality",
			constraints: []storage.ScanConstraint{
				{Column: 1, Op: storage.ComparisonEqual, Value: types.StringValue("row")},
			},
			want: []int32{1, 2, 3, 4, 5},
		},
		{
			name: "string inequality miss",
			constraints: []storage.ScanConstraint{
				{Column: 1, Op: storage.ComparisonNotEqual, Value: types.StringValue("row")},
			},
			want: nil,
		},
		{
			name: "missing column",
			constraints: []storage.ScanConstraint{
				{Column: 7, Op: storage.ComparisonEqual, Value: types.Int32Value(1)},
			},
			want: nil,
		},
		{
			name: "null comparison never matches",
			constraints: []storage.ScanConstraint{
				{Column: 0, Op: storage.ComparisonEqual, Value: types.NullValue()},
			},
			want: nil,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			records := collectRecords(t, mustScan(t, store, mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true}), testTable, storage.ScanOptions{
				Constraints: tc.constraints,
			}))
			got := recordInts(t, records)
			if len(got) != len(tc.want) {
				t.Fatalf("record count = %d, want %d (%v)", len(got), len(tc.want), tc.want)
			}
			for index := range got {
				if got[index] != tc.want[index] {
					t.Fatalf("records = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestScanConstraintTypeError(t *testing.T) {
	t.Parallel()

	store := memory.New()
	insertCommittedRow(t, store, storage.NewRow(types.Int32Value(1), types.StringValue("row")))

	tx := mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true})
	_, err := store.Scan(tx, testTable, storage.ScanOptions{
		Constraints: []storage.ScanConstraint{
			{Column: 0, Op: storage.ComparisonEqual, Value: types.StringValue("row")},
		},
	})
	if err == nil {
		t.Fatal("Scan() error = nil, want a type comparison error")
	}
}

func TestIteratorCloseAndCopies(t *testing.T) {
	t.Parallel()

	store := memory.New()
	insertCommittedRow(t, store, storage.NewRow(types.BytesValue([]byte{1, 2, 3}), types.StringValue("row")))

	tx := mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true})
	iter := mustScan(t, store, tx, testTable, storage.ScanOptions{})

	record, err := iter.Next()
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}

	value, ok := record.Row.Value(0)
	if !ok {
		t.Fatal("Value(0) = (_, false), want (_, true)")
	}
	buf := value.Raw().([]byte)
	buf[0] = 99

	fresh := collectRecords(t, mustScan(t, store, mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true}), testTable, storage.ScanOptions{}))
	freshValue, ok := fresh[0].Row.Value(0)
	if !ok {
		t.Fatal("fresh Value(0) = (_, false), want (_, true)")
	}
	if !bytes.Equal(freshValue.Raw().([]byte), []byte{1, 2, 3}) {
		t.Fatalf("stored bytes = %v, want %v", freshValue.Raw(), []byte{1, 2, 3})
	}

	if err := iter.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := iter.Next(); !errors.Is(err, memory.ErrIteratorClosed) {
		t.Fatalf("Next() after Close error = %v, want %v", err, memory.ErrIteratorClosed)
	}
}

func TestConcurrentScansAndWrites(t *testing.T) {
	t.Parallel()

	store := memory.New()
	for _, value := range []int32{1, 2, 3, 4, 5} {
		insertCommittedRow(t, store, storage.NewRow(types.Int32Value(value), types.StringValue("seed")))
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 12)

	for index := 0; index < 6; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			tx, err := store.NewTransaction(storage.TransactionOptions{ReadOnly: true})
			if err != nil {
				errCh <- err
				return
			}

			iter, err := store.Scan(tx, testTable, storage.ScanOptions{})
			if err != nil {
				errCh <- err
				return
			}
			defer func() {
				if closeErr := iter.Close(); closeErr != nil {
					errCh <- closeErr
				}
			}()

			for {
				_, nextErr := iter.Next()
				if errors.Is(nextErr, io.EOF) {
					return
				}
				if nextErr != nil {
					errCh <- nextErr
					return
				}
			}
		}()
	}

	for index := 0; index < 6; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			tx, err := store.NewTransaction(storage.TransactionOptions{})
			if err != nil {
				errCh <- err
				return
			}
			if _, err := store.Insert(tx, testTable, storage.NewRow(types.Int32Value(int32(index+10)), types.StringValue("writer"))); err != nil {
				errCh <- err
				return
			}
			if err := tx.Commit(); err != nil {
				errCh <- err
			}
		}(index)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent operation error = %v", err)
		}
	}

	records := collectRecords(t, mustScan(t, store, mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true}), testTable, storage.ScanOptions{}))
	if len(records) != 11 {
		t.Fatalf("len(records) = %d, want 11", len(records))
	}
}

func mustNewTransaction(t *testing.T, store *memory.Store, options storage.TransactionOptions) storage.Transaction {
	t.Helper()

	tx, err := store.NewTransaction(options)
	if err != nil {
		t.Fatalf("NewTransaction() error = %v", err)
	}

	return tx
}

func mustScan(t *testing.T, store *memory.Store, tx storage.Transaction, table storage.TableID, options storage.ScanOptions) storage.RowIterator {
	t.Helper()

	iter, err := store.Scan(tx, table, options)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	return iter
}

func collectRecords(t *testing.T, iter storage.RowIterator) []storage.Record {
	t.Helper()
	defer closeIterator(t, iter)

	var records []storage.Record
	for {
		record, err := iter.Next()
		if errors.Is(err, io.EOF) {
			return records
		}
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}

		records = append(records, record)
	}
}

func closeIterator(t *testing.T, iter storage.RowIterator) {
	t.Helper()

	if err := iter.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func insertCommittedRow(t *testing.T, store *memory.Store, row storage.Row) storage.RowHandle {
	t.Helper()

	tx := mustNewTransaction(t, store, storage.TransactionOptions{})
	handle, err := store.Insert(tx, testTable, row)
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	return handle
}

func assertSingleStringValue(t *testing.T, records []storage.Record, want string) {
	t.Helper()

	if len(records) != 1 {
		t.Fatalf("len(records) = %d, want 1", len(records))
	}

	value, ok := records[0].Row.Value(1)
	if !ok {
		t.Fatal("Value(1) = (_, false), want (_, true)")
	}
	if !value.Equal(types.StringValue(want)) {
		t.Fatalf("Value(1) = %v, want %v", value, types.StringValue(want))
	}
}

func recordInts(t *testing.T, records []storage.Record) []int32 {
	t.Helper()

	values := make([]int32, 0, len(records))
	for _, record := range records {
		value, ok := record.Row.Value(0)
		if !ok {
			t.Fatal("Value(0) = (_, false), want (_, true)")
		}

		raw, ok := value.Raw().(int32)
		if !ok {
			t.Fatalf("Value(0).Raw() type = %T, want int32", value.Raw())
		}
		values = append(values, raw)
	}

	return values
}
