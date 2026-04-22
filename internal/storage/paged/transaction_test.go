package paged

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/catalog"
	"github.com/jamesdrando/tucotuco/internal/storage"
)

func TestRelationTxInsertRollback(t *testing.T) {
	root := t.TempDir()
	manager, relation := openTransactionTestRelation(t, root, "tx_insert_rollback")
	defer func() {
		_ = manager.Close()
	}()

	tx, err := relation.BeginTransaction(storage.TransactionOptions{})
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	if _, err := tx.Insert(pagedTestRow(1, "pending")); err != nil {
		t.Fatalf("tx insert: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	records := collectPagedRecords(t, mustScanRelation(t, relation, storage.ScanOptions{}))
	if len(records) != 0 {
		t.Fatalf("len(records) = %d, want 0", len(records))
	}
}

func TestRelationTxUpdateRollback(t *testing.T) {
	root := t.TempDir()
	manager, relation := openTransactionTestRelation(t, root, "tx_update_rollback")
	defer func() {
		_ = manager.Close()
	}()

	handle, err := relation.Insert(pagedTestRow(1, "before"))
	if err != nil {
		t.Fatalf("insert seed row: %v", err)
	}

	tx, err := relation.BeginTransaction(storage.TransactionOptions{})
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	if err := tx.Update(handle, pagedTestRow(1, strings.Repeat("after", 16))); err != nil {
		t.Fatalf("tx update: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	got, err := relation.Lookup(handle)
	if err != nil {
		t.Fatalf("lookup after rollback: %v", err)
	}
	assertStorageRowEqual(t, got, pagedTestRow(1, "before"))
}

func TestRelationTxDeleteRollback(t *testing.T) {
	root := t.TempDir()
	manager, relation := openTransactionTestRelation(t, root, "tx_delete_rollback")
	defer func() {
		_ = manager.Close()
	}()

	handle, err := relation.Insert(pagedTestRow(1, "seed"))
	if err != nil {
		t.Fatalf("insert seed row: %v", err)
	}

	tx, err := relation.BeginTransaction(storage.TransactionOptions{})
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	if err := tx.Delete(handle); err != nil {
		t.Fatalf("tx delete: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	got, err := relation.Lookup(handle)
	if err != nil {
		t.Fatalf("lookup after rollback: %v", err)
	}
	assertStorageRowEqual(t, got, pagedTestRow(1, "seed"))
}

func TestRelationTxReadOnlyRejectsWrites(t *testing.T) {
	root := t.TempDir()
	manager, relation := openTransactionTestRelation(t, root, "tx_read_only")
	defer func() {
		_ = manager.Close()
	}()

	handle, err := relation.Insert(pagedTestRow(1, "seed"))
	if err != nil {
		t.Fatalf("insert seed row: %v", err)
	}

	tx, err := relation.BeginTransaction(storage.TransactionOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("begin read-only transaction: %v", err)
	}

	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "insert",
			run: func() error {
				_, err := tx.Insert(pagedTestRow(2, "pending"))
				return err
			},
		},
		{
			name: "update",
			run: func() error {
				return tx.Update(handle, pagedTestRow(1, "updated"))
			},
		},
		{
			name: "delete",
			run: func() error {
				return tx.Delete(handle)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, ErrReadOnlyTransaction) {
				t.Fatalf("%s error = %v, want %v", tc.name, err, ErrReadOnlyTransaction)
			}
		})
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback read-only transaction: %v", err)
	}
}

func TestRelationTxSeesOwnWrites(t *testing.T) {
	root := t.TempDir()
	manager, relation := openTransactionTestRelation(t, root, "tx_own_writes")
	defer func() {
		_ = manager.Close()
	}()

	committedHandle, err := relation.Insert(pagedTestRow(1, "seed"))
	if err != nil {
		t.Fatalf("insert committed row: %v", err)
	}

	tx, err := relation.BeginTransaction(storage.TransactionOptions{})
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}

	stagedHandle, err := tx.Insert(pagedTestRow(2, "pending"))
	if err != nil {
		t.Fatalf("tx insert: %v", err)
	}

	stagedRow, err := tx.Lookup(stagedHandle)
	if err != nil {
		t.Fatalf("lookup staged insert: %v", err)
	}
	assertStorageRowEqual(t, stagedRow, pagedTestRow(2, "pending"))

	got := scanRecordNotes(t, mustScanTx(t, tx, storage.ScanOptions{}))
	want := []string{"seed", "pending"}
	assertStringSliceEqual(t, got, want)

	if err := tx.Update(committedHandle, pagedTestRow(1, "updated")); err != nil {
		t.Fatalf("tx update committed row: %v", err)
	}

	updatedRow, err := tx.Lookup(committedHandle)
	if err != nil {
		t.Fatalf("lookup staged update: %v", err)
	}
	assertStorageRowEqual(t, updatedRow, pagedTestRow(1, "updated"))

	got = scanRecordNotes(t, mustScanTx(t, tx, storage.ScanOptions{}))
	want = []string{"updated", "pending"}
	assertStringSliceEqual(t, got, want)

	if err := tx.Delete(committedHandle); err != nil {
		t.Fatalf("tx delete committed row: %v", err)
	}
	if _, err := tx.Lookup(committedHandle); !errors.Is(err, ErrRowNotFound) {
		t.Fatalf("lookup deleted committed row error = %v, want %v", err, ErrRowNotFound)
	}

	got = scanRecordNotes(t, mustScanTx(t, tx, storage.ScanOptions{}))
	want = []string{"pending"}
	assertStringSliceEqual(t, got, want)

	if err := tx.Delete(stagedHandle); err != nil {
		t.Fatalf("tx delete pending insert: %v", err)
	}
	if _, err := tx.Lookup(stagedHandle); !errors.Is(err, ErrRowNotFound) {
		t.Fatalf("lookup deleted pending insert error = %v, want %v", err, ErrRowNotFound)
	}

	records := collectPagedRecords(t, mustScanTx(t, tx, storage.ScanOptions{}))
	if len(records) != 0 {
		t.Fatalf("len(records) = %d, want 0", len(records))
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
}

func TestRelationTxDurabilityAcrossReopen(t *testing.T) {
	t.Run("commit_persists", func(t *testing.T) {
		root := t.TempDir()
		desc := pagedTestTableDescriptor("tx_commit_reopen")

		manager, relation := openTransactionTestRelationWithDescriptor(t, root, desc)
		seedHandle, err := relation.Insert(pagedTestRow(1, "tiny"))
		if err != nil {
			_ = manager.Close()
			t.Fatalf("insert seed row: %v", err)
		}

		tx, err := relation.BeginTransaction(storage.TransactionOptions{})
		if err != nil {
			_ = manager.Close()
			t.Fatalf("begin transaction: %v", err)
		}
		if err := tx.Update(seedHandle, pagedTestRow(1, strings.Repeat("c", 96))); err != nil {
			_ = manager.Close()
			t.Fatalf("tx update: %v", err)
		}
		if _, err := tx.Insert(pagedTestRow(2, "committed")); err != nil {
			_ = manager.Close()
			t.Fatalf("tx insert: %v", err)
		}
		if err := tx.Commit(); err != nil {
			_ = manager.Close()
			t.Fatalf("commit: %v", err)
		}

		if err := manager.Close(); err != nil {
			t.Fatalf("close committed manager: %v", err)
		}

		reopenedManager, reopenedRelation := openTransactionTestRelationWithDescriptor(t, root, desc)
		defer func() {
			_ = reopenedManager.Close()
		}()

		got, err := reopenedRelation.Lookup(seedHandle)
		if err != nil {
			t.Fatalf("lookup committed update after reopen: %v", err)
		}
		assertStorageRowEqual(t, got, pagedTestRow(1, strings.Repeat("c", 96)))

		notes := scanRecordNotes(t, mustScanRelation(t, reopenedRelation, storage.ScanOptions{}))
		assertStringSliceEqual(t, notes, []string{strings.Repeat("c", 96), "committed"})
	})

	t.Run("rollback_discards", func(t *testing.T) {
		root := t.TempDir()
		desc := pagedTestTableDescriptor("tx_rollback_reopen")

		manager, relation := openTransactionTestRelationWithDescriptor(t, root, desc)
		seedHandle, err := relation.Insert(pagedTestRow(1, "seed"))
		if err != nil {
			_ = manager.Close()
			t.Fatalf("insert seed row: %v", err)
		}

		tx, err := relation.BeginTransaction(storage.TransactionOptions{})
		if err != nil {
			_ = manager.Close()
			t.Fatalf("begin transaction: %v", err)
		}
		if err := tx.Update(seedHandle, pagedTestRow(1, strings.Repeat("r", 96))); err != nil {
			_ = manager.Close()
			t.Fatalf("tx update: %v", err)
		}
		if _, err := tx.Insert(pagedTestRow(2, "rolled")); err != nil {
			_ = manager.Close()
			t.Fatalf("tx insert: %v", err)
		}
		if err := tx.Rollback(); err != nil {
			_ = manager.Close()
			t.Fatalf("rollback: %v", err)
		}

		if err := manager.Close(); err != nil {
			t.Fatalf("close rolled-back manager: %v", err)
		}

		reopenedManager, reopenedRelation := openTransactionTestRelationWithDescriptor(t, root, desc)
		defer func() {
			_ = reopenedManager.Close()
		}()

		got, err := reopenedRelation.Lookup(seedHandle)
		if err != nil {
			t.Fatalf("lookup rolled-back row after reopen: %v", err)
		}
		assertStorageRowEqual(t, got, pagedTestRow(1, "seed"))

		notes := scanRecordNotes(t, mustScanRelation(t, reopenedRelation, storage.ScanOptions{}))
		assertStringSliceEqual(t, notes, []string{"seed"})
	})
}

func openTransactionTestRelation(t *testing.T, root, name string) (*HeapManager, *Relation) {
	t.Helper()

	return openTransactionTestRelationWithDescriptor(t, root, pagedTestTableDescriptor(name))
}

func openTransactionTestRelationWithDescriptor(t *testing.T, root string, desc *catalog.TableDescriptor) (*HeapManager, *Relation) {
	t.Helper()

	manager, err := OpenHeapManager(root, 512, 4)
	if err != nil {
		t.Fatalf("open heap manager: %v", err)
	}
	if err := manager.CreateTable(nil, desc); err != nil && !errors.Is(err, ErrRelationExists) {
		_ = manager.Close()
		t.Fatalf("create table: %v", err)
	}

	relation, err := manager.OpenRelation(desc)
	if err != nil {
		_ = manager.Close()
		t.Fatalf("open relation: %v", err)
	}

	return manager, relation
}

func mustScanRelation(t *testing.T, relation *Relation, options storage.ScanOptions) storage.RowIterator {
	t.Helper()

	iter, err := relation.Scan(options)
	if err != nil {
		t.Fatalf("relation.Scan() error = %v", err)
	}

	return iter
}

func mustScanTx(t *testing.T, tx *RelationTx, options storage.ScanOptions) storage.RowIterator {
	t.Helper()

	iter, err := tx.Scan(options)
	if err != nil {
		t.Fatalf("tx.Scan() error = %v", err)
	}

	return iter
}

func collectPagedRecords(t *testing.T, iter storage.RowIterator) []storage.Record {
	t.Helper()
	defer closePagedIterator(t, iter)

	var records []storage.Record
	for {
		record, err := iter.Next()
		if errors.Is(err, io.EOF) {
			return records
		}
		if err != nil {
			t.Fatalf("iterator.Next() error = %v", err)
		}

		records = append(records, record)
	}
}

func closePagedIterator(t *testing.T, iter storage.RowIterator) {
	t.Helper()

	if err := iter.Close(); err != nil {
		t.Fatalf("iterator.Close() error = %v", err)
	}
}

func scanRecordNotes(t *testing.T, iter storage.RowIterator) []string {
	t.Helper()

	records := collectPagedRecords(t, iter)
	notes := make([]string, 0, len(records))
	for _, record := range records {
		value, ok := record.Row.Value(1)
		if !ok {
			t.Fatal("row.Value(1) = (_, false), want (_, true)")
		}

		raw, ok := value.Raw().(string)
		if !ok {
			t.Fatalf("row.Value(1).Raw() type = %T, want string", value.Raw())
		}
		notes = append(notes, raw)
	}

	return notes
}

func assertStringSliceEqual(t *testing.T, got, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("len(got) = %d, want %d (%v)", len(got), len(want), want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("got[%d] = %q, want %q (full got %v)", index, got[index], want[index], got)
		}
	}
}
