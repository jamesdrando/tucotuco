package memory_test

import (
	"errors"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/storage/memory"
	"github.com/jamesdrando/tucotuco/internal/storage/storagetest"
	"github.com/jamesdrando/tucotuco/internal/types"
)

func TestStoreImplementsStorage(t *testing.T) {
	t.Parallel()

	var _ storage.Storage = (*memory.Store)(nil)
}

func TestStoreContract(t *testing.T) {
	t.Parallel()

	storagetest.RunStorageContract(t, storagetest.Harness{
		NewStore: func(t *testing.T) storage.Storage {
			t.Helper()
			return memory.New()
		},
		Table: storage.TableID{Schema: "public", Name: "widgets"},
		Matchers: storagetest.ErrorMatchers{
			ReadOnlyTransaction: func(err error) bool {
				return errors.Is(err, memory.ErrReadOnlyTransaction)
			},
			InvalidTransaction: func(err error) bool {
				return errors.Is(err, memory.ErrInvalidTransaction)
			},
			TransactionClosed: func(err error) bool {
				return errors.Is(err, memory.ErrTransactionClosed)
			},
			InvalidTable: func(err error) bool {
				return errors.Is(err, memory.ErrInvalidTable)
			},
			RowNotFound: func(err error) bool {
				return errors.Is(err, memory.ErrRowNotFound)
			},
			IteratorClosed: func(err error) bool {
				return errors.Is(err, memory.ErrIteratorClosed)
			},
			SerializationConflict: func(err error) bool {
				return errors.Is(err, memory.ErrSerializationConflict)
			},
		},
	})
}

func TestCommittedInsertRetainsReservedHandle(t *testing.T) {
	t.Parallel()

	store := memory.New()
	table := storage.TableID{Schema: "public", Name: "widgets"}

	writeTx, err := store.NewTransaction(storage.TransactionOptions{})
	if err != nil {
		t.Fatalf("NewTransaction() error = %v", err)
	}

	handle, err := store.Insert(writeTx, table, storage.NewRow(
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

	readTx, err := store.NewTransaction(storage.TransactionOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("NewTransaction() error = %v", err)
	}
	iter, err := store.Scan(readTx, table, storage.ScanOptions{})
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	defer func() {
		if closeErr := iter.Close(); closeErr != nil {
			t.Fatalf("Close() error = %v", closeErr)
		}
	}()

	record, err := iter.Next()
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if record.Handle != handle {
		t.Fatalf("scan handle = %v, want %v", record.Handle, handle)
	}
}
