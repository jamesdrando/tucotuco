package executor

import (
	"bytes"
	"errors"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/types"
)

func TestRowDefendsAgainstMutablePayloadLeaks(t *testing.T) {
	t.Parallel()

	inputs := []types.Value{
		types.StringValue("alpha"),
		types.BytesValue([]byte{1, 2, 3}),
		types.ArrayValue(types.Array{
			types.Int32Value(7),
			types.BytesValue([]byte{4, 5}),
		}),
		types.RowValue(types.Row{
			types.StringValue("nested"),
			types.BytesValue([]byte{6, 7}),
		}),
	}

	row := NewRowWithHandle(storage.RowHandle{Page: 1, Slot: 9}, inputs...)
	inputs[0] = types.StringValue("beta")

	values := row.Values()
	values[0] = types.StringValue("gamma")

	got, ok := row.Value(0)
	if !ok || !got.Equal(types.StringValue("alpha")) {
		t.Fatalf("Value(0) = (%v, %t), want (%v, true)", got, ok, types.StringValue("alpha"))
	}

	bytesValue, ok := row.Value(1)
	if !ok {
		t.Fatal("Value(1) = (_, false), want (_, true)")
	}
	mutatedBytes := bytesValue.Raw().([]byte)
	mutatedBytes[0] = 99

	arrayValue, ok := row.Value(2)
	if !ok {
		t.Fatal("Value(2) = (_, false), want (_, true)")
	}
	mutatedArray := arrayValue.Raw().(types.Array)
	mutatedArray[0] = types.Int32Value(99)
	mutatedNestedBytes := mutatedArray[1].Raw().([]byte)
	mutatedNestedBytes[0] = 88

	rowValue, ok := row.Value(3)
	if !ok {
		t.Fatal("Value(3) = (_, false), want (_, true)")
	}
	mutatedRow := rowValue.Raw().(types.Row)
	mutatedRow[0] = types.StringValue("changed")
	mutatedRowBytes := mutatedRow[1].Raw().([]byte)
	mutatedRowBytes[0] = 77

	clone := row.Clone()
	clone.values[0] = types.StringValue("delta")
	clone.Handle = storage.RowHandle{Page: 2, Slot: 5}

	got, ok = row.Value(1)
	if !ok {
		t.Fatal("Value(1) = (_, false), want (_, true)")
	}
	if !bytes.Equal(got.Raw().([]byte), []byte{1, 2, 3}) {
		t.Fatalf("Value(1) bytes = %v, want %v", got.Raw(), []byte{1, 2, 3})
	}

	got, ok = row.Value(2)
	if !ok {
		t.Fatal("Value(2) = (_, false), want (_, true)")
	}
	gotArray := got.Raw().(types.Array)
	if !gotArray[0].Equal(types.Int32Value(7)) {
		t.Fatalf("Value(2)[0] = %v, want %v", gotArray[0], types.Int32Value(7))
	}
	if !bytes.Equal(gotArray[1].Raw().([]byte), []byte{4, 5}) {
		t.Fatalf("Value(2)[1] bytes = %v, want %v", gotArray[1].Raw(), []byte{4, 5})
	}

	got, ok = row.Value(3)
	if !ok {
		t.Fatal("Value(3) = (_, false), want (_, true)")
	}
	gotRow := got.Raw().(types.Row)
	if !gotRow[0].Equal(types.StringValue("nested")) {
		t.Fatalf("Value(3)[0] = %v, want %v", gotRow[0], types.StringValue("nested"))
	}
	if !bytes.Equal(gotRow[1].Raw().([]byte), []byte{6, 7}) {
		t.Fatalf("Value(3)[1] bytes = %v, want %v", gotRow[1].Raw(), []byte{6, 7})
	}

	if row.Handle != (storage.RowHandle{Page: 1, Slot: 9}) {
		t.Fatalf("Handle = %#v, want %#v", row.Handle, storage.RowHandle{Page: 1, Slot: 9})
	}

	if _, ok := row.Value(-1); ok {
		t.Fatal("Value(-1) reported ok for an invalid index")
	}

	if _, ok := row.Value(row.Len()); ok {
		t.Fatal("Value(Len()) reported ok for an invalid index")
	}
}

func TestNewRowFromStorageCopiesRowState(t *testing.T) {
	t.Parallel()

	record := storage.Record{
		Handle: storage.RowHandle{Page: 7, Slot: 3},
		Row: storage.NewRow(
			types.BytesValue([]byte{1, 2, 3}),
			types.StringValue("alpha"),
		),
	}

	row := NewRowFromStorage(record)

	record.Handle = storage.RowHandle{Page: 9, Slot: 11}
	record.Row = storage.NewRow(
		types.BytesValue([]byte{8, 8, 8}),
		types.StringValue("mutated"),
	)

	storageRow := row.StorageRow()
	value, ok := storageRow.Value(0)
	if !ok {
		t.Fatal("StorageRow().Value(0) = (_, false), want (_, true)")
	}
	mutated := value.Raw().([]byte)
	mutated[0] = 9

	fresh, ok := row.Value(0)
	if !ok {
		t.Fatal("Value(0) = (_, false), want (_, true)")
	}
	if !bytes.Equal(fresh.Raw().([]byte), []byte{1, 2, 3}) {
		t.Fatalf("Value(0) bytes = %v, want %v", fresh.Raw(), []byte{1, 2, 3})
	}

	second, ok := row.Value(1)
	if !ok || !second.Equal(types.StringValue("alpha")) {
		t.Fatalf("Value(1) = (%v, %t), want (%v, true)", second, ok, types.StringValue("alpha"))
	}

	if row.Handle != (storage.RowHandle{Page: 7, Slot: 3}) {
		t.Fatalf("Handle = %#v, want %#v", row.Handle, storage.RowHandle{Page: 7, Slot: 3})
	}
}

func TestLifecycleStateTransitions(t *testing.T) {
	t.Parallel()

	var guard lifecycle

	if err := guard.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() before Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	if err := guard.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := guard.Open(); !errors.Is(err, ErrOperatorOpen) {
		t.Fatalf("second Open() error = %v, want %v", err, ErrOperatorOpen)
	}

	if err := guard.Next(); err != nil {
		t.Fatalf("Next() after Open error = %v", err)
	}

	if err := guard.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := guard.Next(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Next() after Close error = %v, want %v", err, ErrOperatorClosed)
	}

	if err := guard.Open(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Open() after Close error = %v, want %v", err, ErrOperatorClosed)
	}

	if err := guard.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestLifecycleCloseBeforeOpenIsTerminal(t *testing.T) {
	t.Parallel()

	var guard lifecycle

	if err := guard.Close(); err != nil {
		t.Fatalf("Close() before Open error = %v", err)
	}

	if err := guard.Next(); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Next() after early Close error = %v, want %v", err, ErrOperatorClosed)
	}
}
