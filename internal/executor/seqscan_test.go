package executor

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/storage/memory"
	"github.com/jamesdrando/tucotuco/internal/types"
)

func TestSeqScanLifecycle(t *testing.T) {
	t.Parallel()

	store := memory.New()
	tx := mustSeqScanTransaction(t, store, storage.TransactionOptions{ReadOnly: true})

	scan := NewSeqScan(store, tx, storage.TableID{Schema: "public", Name: "items"}, storage.ScanOptions{})

	if _, err := scan.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() before Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	if err := scan.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := scan.Open(); !errors.Is(err, ErrOperatorOpen) {
		t.Fatalf("second Open() error = %v, want %v", err, ErrOperatorOpen)
	}

	if err := scan.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if _, err := scan.Next(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Next() after Close error = %v, want %v", err, ErrOperatorClosed)
	}

	if err := scan.Open(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Open() after Close error = %v, want %v", err, ErrOperatorClosed)
	}

	if err := scan.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
}

func TestSeqScanNextReturnsEOFRepeatedly(t *testing.T) {
	t.Parallel()

	store := memory.New()
	table := storage.TableID{Schema: "public", Name: "items"}
	insertCommittedSeqScanRow(t, store, table, storage.NewRow(types.Int32Value(1), types.StringValue("alpha")))
	insertCommittedSeqScanRow(t, store, table, storage.NewRow(types.Int32Value(2), types.StringValue("beta")))

	tx := mustSeqScanTransaction(t, store, storage.TransactionOptions{ReadOnly: true})
	scan := NewSeqScan(store, tx, table, storage.ScanOptions{})

	if err := scan.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	first, err := scan.Next()
	if err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	assertSeqScanIntValue(t, first, 0, 1)

	second, err := scan.Next()
	if err != nil {
		t.Fatalf("second Next() error = %v", err)
	}
	assertSeqScanIntValue(t, second, 0, 2)

	if _, err := scan.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("third Next() error = %v, want %v", err, io.EOF)
	}

	if _, err := scan.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("fourth Next() error = %v, want %v", err, io.EOF)
	}

	if err := scan.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
}

func TestSeqScanConvertsStorageRecordsToExecutorRows(t *testing.T) {
	t.Parallel()

	store := memory.New()
	table := storage.TableID{Schema: "public", Name: "items"}
	handle := insertCommittedSeqScanRow(t, store, table, storage.NewRow(
		types.BytesValue([]byte{1, 2, 3}),
		types.StringValue("alpha"),
	))

	tx := mustSeqScanTransaction(t, store, storage.TransactionOptions{ReadOnly: true})
	scan := NewSeqScan(store, tx, table, storage.ScanOptions{})

	if err := scan.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	row, err := scan.Next()
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}

	if row.Handle != handle {
		t.Fatalf("row.Handle = %#v, want %#v", row.Handle, handle)
	}

	value, ok := row.Value(0)
	if !ok {
		t.Fatal("Value(0) = (_, false), want (_, true)")
	}
	if !bytes.Equal(value.Raw().([]byte), []byte{1, 2, 3}) {
		t.Fatalf("Value(0) bytes = %v, want %v", value.Raw(), []byte{1, 2, 3})
	}

	second, ok := row.Value(1)
	if !ok || !second.Equal(types.StringValue("alpha")) {
		t.Fatalf("Value(1) = (%v, %t), want (%v, true)", second, ok, types.StringValue("alpha"))
	}

	mutated := value.Raw().([]byte)
	mutated[0] = 9

	if err := scan.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	verifyTx := mustSeqScanTransaction(t, store, storage.TransactionOptions{ReadOnly: true})
	verifyScan := NewSeqScan(store, verifyTx, table, storage.ScanOptions{})
	if err := verifyScan.Open(); err != nil {
		t.Fatalf("verify Open() error = %v", err)
	}

	fresh, err := verifyScan.Next()
	if err != nil {
		t.Fatalf("verify Next() error = %v", err)
	}

	freshValue, ok := fresh.Value(0)
	if !ok {
		t.Fatal("verify Value(0) = (_, false), want (_, true)")
	}
	if !bytes.Equal(freshValue.Raw().([]byte), []byte{1, 2, 3}) {
		t.Fatalf("verify Value(0) bytes = %v, want %v", freshValue.Raw(), []byte{1, 2, 3})
	}

	if err := verifyScan.Close(); err != nil {
		t.Fatalf("verify Close() error = %v", err)
	}
	if err := verifyTx.Rollback(); err != nil {
		t.Fatalf("verify Rollback() error = %v", err)
	}
}

func TestSeqScanForwardsScanOptions(t *testing.T) {
	t.Parallel()

	store := memory.New()
	table := storage.TableID{Schema: "public", Name: "items"}
	insertCommittedSeqScanRow(t, store, table, storage.NewRow(types.Int32Value(1), types.StringValue("alpha")))
	insertCommittedSeqScanRow(t, store, table, storage.NewRow(types.Int32Value(2), types.StringValue("beta")))
	insertCommittedSeqScanRow(t, store, table, storage.NewRow(types.Int32Value(3), types.StringValue("beta")))

	tx := mustSeqScanTransaction(t, store, storage.TransactionOptions{ReadOnly: true})
	scan := NewSeqScan(store, tx, table, storage.ScanOptions{
		Constraints: []storage.ScanConstraint{
			{Column: 1, Op: storage.ComparisonEqual, Value: types.StringValue("beta")},
		},
		Limit: 1,
	})

	if err := scan.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	row, err := scan.Next()
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	assertSeqScanIntValue(t, row, 0, 2)

	if _, err := scan.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next() error = %v, want %v", err, io.EOF)
	}

	if err := scan.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
}

func TestSeqScanOpenFailureLeavesOperatorNotOpen(t *testing.T) {
	t.Parallel()

	store := memory.New()
	tx := mustSeqScanTransaction(t, store, storage.TransactionOptions{ReadOnly: true})
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	scan := NewSeqScan(store, tx, storage.TableID{Schema: "public", Name: "items"}, storage.ScanOptions{})

	if err := scan.Open(); !errors.Is(err, memory.ErrTransactionClosed) {
		t.Fatalf("Open() error = %v, want %v", err, memory.ErrTransactionClosed)
	}

	if _, err := scan.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() after failed Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	if err := scan.Close(); err != nil {
		t.Fatalf("Close() after failed Open error = %v", err)
	}
	if err := scan.Close(); err != nil {
		t.Fatalf("second Close() after failed Open error = %v", err)
	}
}

func mustSeqScanTransaction(t *testing.T, store *memory.Store, options storage.TransactionOptions) storage.Transaction {
	t.Helper()

	tx, err := store.NewTransaction(options)
	if err != nil {
		t.Fatalf("NewTransaction() error = %v", err)
	}

	return tx
}

func insertCommittedSeqScanRow(t *testing.T, store *memory.Store, table storage.TableID, row storage.Row) storage.RowHandle {
	t.Helper()

	tx := mustSeqScanTransaction(t, store, storage.TransactionOptions{})
	handle, err := store.Insert(tx, table, row)
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	return handle
}

func assertSeqScanIntValue(t *testing.T, row Row, index int, want int32) {
	t.Helper()

	value, ok := row.Value(index)
	if !ok {
		t.Fatalf("Value(%d) = (_, false), want (_, true)", index)
	}
	if !value.Equal(types.Int32Value(want)) {
		t.Fatalf("Value(%d) = %v, want %v", index, value, types.Int32Value(want))
	}
}
