// Package storagetest provides reusable behavioral contract tests for storage implementations.
package storagetest

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"sync"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/types"
)

// ErrorMatchers adapts backend-specific sentinel errors into the shared
// storage behavior contract.
type ErrorMatchers struct {
	InvalidTransaction    func(error) bool
	TransactionClosed     func(error) bool
	ReadOnlyTransaction   func(error) bool
	InvalidTable          func(error) bool
	RowNotFound           func(error) bool
	IteratorClosed        func(error) bool
	SerializationConflict func(error) bool
}

// Harness supplies the backend under test plus its error mapping.
type Harness struct {
	NewStore func(*testing.T) storage.Storage
	Matchers ErrorMatchers
	Table    storage.TableID
}

// RunStorageContract executes the shared storage behavior suite against the
// supplied backend harness.
func RunStorageContract(t *testing.T, harness Harness) {
	t.Helper()

	table := harness.Table
	if !table.Valid() {
		table = storage.TableID{Schema: "public", Name: "widgets"}
	}

	t.Run("new transaction options", func(t *testing.T) {
		store := harness.newStore(t)

		defaultTx := mustNewTransaction(t, store, storage.TransactionOptions{})
		if got := defaultTx.IsolationLevel(); got != storage.IsolationReadCommitted {
			t.Fatalf("default isolation = %q, want %q", got, storage.IsolationReadCommitted)
		}
		if defaultTx.ReadOnly() {
			t.Fatal("default transaction is read-only, want writable")
		}
		if err := defaultTx.Rollback(); err != nil {
			t.Fatalf("Rollback() error = %v", err)
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
		if err := customTx.Rollback(); err != nil {
			t.Fatalf("Rollback() error = %v", err)
		}
	})

	t.Run("scan empty table", func(t *testing.T) {
		store := harness.newStore(t)
		tx := mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true})

		iter := mustScan(t, store, tx, table, storage.ScanOptions{})
		defer closeIterator(t, iter)

		if _, err := iter.Next(); !errors.Is(err, io.EOF) {
			t.Fatalf("Next() error = %v, want io.EOF", err)
		}
	})

	t.Run("insert commit and scan", func(t *testing.T) {
		store := harness.newStore(t)
		writeTx := mustNewTransaction(t, store, storage.TransactionOptions{})
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
		if err := store.Update(writeTx, table, handle, storage.NewRow(
			types.Int32Value(1),
			types.StringValue("alpha-committed"),
			types.BytesValue([]byte{1, 2, 3}),
		)); err != nil {
			t.Fatalf("Update() via inserted handle error = %v", err)
		}
		assertSingleStringValueByID(t, collectRecords(t, mustScan(t, store, writeTx, table, storage.ScanOptions{})), 1, "alpha-committed")

		if err := writeTx.Commit(); err != nil {
			t.Fatalf("Commit() error = %v", err)
		}

		records := collectReadOnlyRecords(t, store, table, storage.ScanOptions{})
		if len(records) != 1 {
			t.Fatalf("len(records) = %d, want 1", len(records))
		}
		if !records[0].Handle.Valid() {
			t.Fatal("scan handle is invalid")
		}

		value, ok := records[0].Row.Value(1)
		if !ok || !value.Equal(types.StringValue("alpha-committed")) {
			t.Fatalf("row value = (%v, %t), want (%v, true)", value, ok, types.StringValue("alpha-committed"))
		}
	})

	t.Run("insert rollback discards rows", func(t *testing.T) {
		store := harness.newStore(t)
		tx := mustNewTransaction(t, store, storage.TransactionOptions{})

		if _, err := store.Insert(tx, table, storage.NewRow(types.Int32Value(1))); err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback() error = %v", err)
		}

		records := collectReadOnlyRecords(t, store, table, storage.ScanOptions{})
		if len(records) != 0 {
			t.Fatalf("len(records) = %d, want 0", len(records))
		}
	})

	t.Run("read only transaction rejects writes", func(t *testing.T) {
		store := harness.newStore(t)
		handle := insertCommittedRow(t, store, table, storage.NewRow(types.Int32Value(1), types.StringValue("seed")))
		tx := mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true})

		writeOps := []struct {
			name string
			run  func() error
		}{
			{
				name: "insert",
				run: func() error {
					_, err := store.Insert(tx, table, storage.NewRow(types.Int32Value(1)))
					return err
				},
			},
			{
				name: "update",
				run: func() error {
					return store.Update(tx, table, handle, storage.NewRow(types.Int32Value(1)))
				},
			},
			{
				name: "delete",
				run: func() error {
					return store.Delete(tx, table, handle)
				},
			},
		}

		for _, tc := range writeOps {
			t.Run(tc.name, func(t *testing.T) {
				if !harness.matchers().ReadOnlyTransaction(tc.run()) {
					t.Fatalf("error did not match read-only transaction contract")
				}
			})
		}
	})

	t.Run("update lifecycle", func(t *testing.T) {
		t.Run("commit", func(t *testing.T) {
			store := harness.newStore(t)
			handle := insertCommittedRow(t, store, table, storage.NewRow(types.Int32Value(1), types.StringValue("before")))

			tx := mustNewTransaction(t, store, storage.TransactionOptions{})
			if err := store.Update(tx, table, handle, storage.NewRow(types.Int32Value(1), types.StringValue("after"))); err != nil {
				t.Fatalf("Update() error = %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit() error = %v", err)
			}

			records := collectReadOnlyRecords(t, store, table, storage.ScanOptions{})
			assertSingleStringValue(t, records, "after")
		})

		t.Run("rollback", func(t *testing.T) {
			store := harness.newStore(t)
			handle := insertCommittedRow(t, store, table, storage.NewRow(types.Int32Value(1), types.StringValue("before")))

			tx := mustNewTransaction(t, store, storage.TransactionOptions{})
			if err := store.Update(tx, table, handle, storage.NewRow(types.Int32Value(1), types.StringValue("after"))); err != nil {
				t.Fatalf("Update() error = %v", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatalf("Rollback() error = %v", err)
			}

			records := collectReadOnlyRecords(t, store, table, storage.ScanOptions{})
			assertSingleStringValue(t, records, "before")
		})
	})

	t.Run("delete lifecycle", func(t *testing.T) {
		t.Run("commit", func(t *testing.T) {
			store := harness.newStore(t)
			handle := insertCommittedRow(t, store, table, storage.NewRow(types.Int32Value(1), types.StringValue("alpha")))

			tx := mustNewTransaction(t, store, storage.TransactionOptions{})
			if err := store.Delete(tx, table, handle); err != nil {
				t.Fatalf("Delete() error = %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit() error = %v", err)
			}

			records := collectReadOnlyRecords(t, store, table, storage.ScanOptions{})
			if len(records) != 0 {
				t.Fatalf("len(records) = %d, want 0", len(records))
			}
		})

		t.Run("rollback", func(t *testing.T) {
			store := harness.newStore(t)
			handle := insertCommittedRow(t, store, table, storage.NewRow(types.Int32Value(1), types.StringValue("alpha")))

			tx := mustNewTransaction(t, store, storage.TransactionOptions{})
			if err := store.Delete(tx, table, handle); err != nil {
				t.Fatalf("Delete() error = %v", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatalf("Rollback() error = %v", err)
			}

			records := collectReadOnlyRecords(t, store, table, storage.ScanOptions{})
			assertSingleStringValue(t, records, "alpha")
		})
	})

	t.Run("transaction sees own writes", func(t *testing.T) {
		store := harness.newStore(t)
		committedHandle := insertCommittedRow(t, store, table, storage.NewRow(types.Int32Value(1), types.StringValue("seed")))

		tx := mustNewTransaction(t, store, storage.TransactionOptions{})
		handle, err := store.Insert(tx, table, storage.NewRow(types.Int32Value(2), types.StringValue("pending")))
		if err != nil {
			t.Fatalf("Insert() error = %v", err)
		}

		assertSingleStringValueByID(t, collectRecords(t, mustScan(t, store, tx, table, storage.ScanOptions{})), 2, "pending")

		if err := store.Update(tx, table, committedHandle, storage.NewRow(types.Int32Value(1), types.StringValue("updated"))); err != nil {
			t.Fatalf("Update() error = %v", err)
		}
		assertSingleStringValueByID(t, collectRecords(t, mustScan(t, store, tx, table, storage.ScanOptions{})), 1, "updated")

		if err := store.Delete(tx, table, committedHandle); err != nil {
			t.Fatalf("Delete() error = %v", err)
		}
		if records := collectRecords(t, mustScan(t, store, tx, table, storage.ScanOptions{})); len(records) != 1 {
			t.Fatalf("len(records) = %d, want 1", len(records))
		}

		if err := store.Delete(tx, table, handle); err != nil {
			t.Fatalf("Delete() error = %v", err)
		}
		if records := collectRecords(t, mustScan(t, store, tx, table, storage.ScanOptions{})); len(records) != 0 {
			t.Fatalf("len(records) = %d, want 0", len(records))
		}
	})

	t.Run("scan uses statement snapshot", func(t *testing.T) {
		store := harness.newStore(t)
		insertCommittedRow(t, store, table, storage.NewRow(types.Int32Value(1), types.StringValue("seed")))

		tx := mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true})
		iter := mustScan(t, store, tx, table, storage.ScanOptions{})

		commitRow(t, store, table, storage.NewRow(types.Int32Value(2), types.StringValue("late")))

		records := collectRecords(t, iter)
		got := recordInts(t, records)
		want := []int32{1}
		assertInt32SliceEqual(t, got, want)
	})

	t.Run("read committed refreshes snapshot per scan", func(t *testing.T) {
		store := harness.newStore(t)
		insertCommittedRow(t, store, table, storage.NewRow(types.Int32Value(1), types.StringValue("seed")))

		tx := mustNewTransaction(t, store, storage.TransactionOptions{})
		if _, err := store.Insert(tx, table, storage.NewRow(types.Int32Value(10), types.StringValue("pending"))); err != nil {
			t.Fatalf("Insert() error = %v", err)
		}

		first := recordInts(t, collectRecords(t, mustScan(t, store, tx, table, storage.ScanOptions{})))
		assertInt32ContentsEqual(t, first, []int32{1, 10})

		commitRow(t, store, table, storage.NewRow(types.Int32Value(2), types.StringValue("late")))

		second := recordInts(t, collectRecords(t, mustScan(t, store, tx, table, storage.ScanOptions{})))
		assertInt32ContentsEqual(t, second, []int32{1, 10, 2})
	})

	t.Run("repeatable read keeps pinned snapshot", func(t *testing.T) {
		store := harness.newStore(t)
		insertCommittedRow(t, store, table, storage.NewRow(types.Int32Value(1), types.StringValue("seed")))

		tx := mustNewTransaction(t, store, storage.TransactionOptions{Isolation: storage.IsolationRepeatableRead})
		if _, err := store.Insert(tx, table, storage.NewRow(types.Int32Value(10), types.StringValue("pending"))); err != nil {
			t.Fatalf("Insert() error = %v", err)
		}

		first := recordInts(t, collectRecords(t, mustScan(t, store, tx, table, storage.ScanOptions{})))
		assertInt32ContentsEqual(t, first, []int32{1, 10})

		commitRow(t, store, table, storage.NewRow(types.Int32Value(2), types.StringValue("late")))

		second := recordInts(t, collectRecords(t, mustScan(t, store, tx, table, storage.ScanOptions{})))
		assertInt32ContentsEqual(t, second, []int32{1, 10})
	})

	t.Run("serializable conflict on concurrent change", func(t *testing.T) {
		store := harness.newStore(t)
		insertCommittedRow(t, store, table, storage.NewRow(types.Int32Value(1), types.StringValue("seed")))

		tx := mustNewTransaction(t, store, storage.TransactionOptions{Isolation: storage.IsolationSerializable})
		if _, err := store.Insert(tx, table, storage.NewRow(types.Int32Value(10), types.StringValue("pending"))); err != nil {
			t.Fatalf("Insert() error = %v", err)
		}

		commitRow(t, store, table, storage.NewRow(types.Int32Value(2), types.StringValue("late")))

		if !harness.matchers().SerializationConflict(tx.Commit()) {
			t.Fatalf("Commit() error did not match serialization conflict contract")
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback() after conflict error = %v", err)
		}
	})

	t.Run("serializable commit ignores tombstone only changes", func(t *testing.T) {
		store := harness.newStore(t)
		tx := mustNewTransaction(t, store, storage.TransactionOptions{Isolation: storage.IsolationSerializable, ReadOnly: true})

		iter := mustScan(t, store, tx, table, storage.ScanOptions{})
		defer closeIterator(t, iter)
		if _, err := iter.Next(); !errors.Is(err, io.EOF) {
			t.Fatalf("Next() error = %v, want io.EOF", err)
		}

		writeTx := mustNewTransaction(t, store, storage.TransactionOptions{})
		handle, err := store.Insert(writeTx, table, storage.NewRow(types.Int32Value(1), types.StringValue("transient")))
		if err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
		if err := store.Delete(writeTx, table, handle); err != nil {
			t.Fatalf("Delete() error = %v", err)
		}
		if err := writeTx.Commit(); err != nil {
			t.Fatalf("Commit() error = %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit() error = %v", err)
		}
	})

	t.Run("invalid transaction rejected", func(t *testing.T) {
		store := harness.newStore(t)
		otherStore := harness.newStore(t)
		tx := mustNewTransaction(t, otherStore, storage.TransactionOptions{})

		ops := []struct {
			name string
			run  func() error
		}{
			{
				name: "scan",
				run: func() error {
					_, err := store.Scan(tx, table, storage.ScanOptions{})
					return err
				},
			},
			{
				name: "insert",
				run: func() error {
					_, err := store.Insert(tx, table, storage.NewRow(types.Int32Value(1)))
					return err
				},
			},
			{
				name: "update",
				run: func() error {
					return store.Update(tx, table, storage.RowHandle{Page: 1, Slot: 1}, storage.NewRow(types.Int32Value(1)))
				},
			},
			{
				name: "delete",
				run: func() error {
					return store.Delete(tx, table, storage.RowHandle{Page: 1, Slot: 1})
				},
			},
		}

		for _, tc := range ops {
			t.Run(tc.name, func(t *testing.T) {
				if !harness.matchers().InvalidTransaction(tc.run()) {
					t.Fatalf("error did not match invalid transaction contract")
				}
			})
		}
	})

	t.Run("closed transaction rejected", func(t *testing.T) {
		store := harness.newStore(t)
		tx := mustNewTransaction(t, store, storage.TransactionOptions{})
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit() error = %v", err)
		}
		if !harness.matchers().TransactionClosed(tx.Commit()) {
			t.Fatalf("second Commit() error did not match transaction closed contract")
		}
		if !harness.matchers().TransactionClosed(tx.Rollback()) {
			t.Fatalf("Rollback() after commit error did not match transaction closed contract")
		}

		if _, err := store.Scan(tx, table, storage.ScanOptions{}); !harness.matchers().TransactionClosed(err) {
			t.Fatalf("Scan() error did not match transaction closed contract: %v", err)
		}
		if _, err := store.Insert(tx, table, storage.NewRow(types.Int32Value(1))); !harness.matchers().TransactionClosed(err) {
			t.Fatalf("Insert() error did not match transaction closed contract: %v", err)
		}
	})

	t.Run("invalid table rejected", func(t *testing.T) {
		store := harness.newStore(t)
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
			t.Run(tc.name, func(t *testing.T) {
				if !harness.matchers().InvalidTable(tc.run()) {
					t.Fatalf("error did not match invalid table contract")
				}
			})
		}
	})

	t.Run("missing row rejected", func(t *testing.T) {
		store := harness.newStore(t)
		tx := mustNewTransaction(t, store, storage.TransactionOptions{})
		handle := storage.RowHandle{Page: 1, Slot: 99}

		tests := []struct {
			name string
			run  func() error
		}{
			{
				name: "update",
				run: func() error {
					return store.Update(tx, table, handle, storage.NewRow(types.Int32Value(1)))
				},
			},
			{
				name: "delete",
				run: func() error {
					return store.Delete(tx, table, handle)
				},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if !harness.matchers().RowNotFound(tc.run()) {
					t.Fatalf("error did not match row not found contract")
				}
			})
		}
	})

	t.Run("delete pending insert removes it", func(t *testing.T) {
		store := harness.newStore(t)
		tx := mustNewTransaction(t, store, storage.TransactionOptions{})

		handle, err := store.Insert(tx, table, storage.NewRow(types.Int32Value(1), types.StringValue("alpha")))
		if err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
		if err := store.Delete(tx, table, handle); err != nil {
			t.Fatalf("Delete() error = %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit() error = %v", err)
		}

		records := collectReadOnlyRecords(t, store, table, storage.ScanOptions{})
		if len(records) != 0 {
			t.Fatalf("len(records) = %d, want 0", len(records))
		}
	})

	t.Run("update after pending delete fails", func(t *testing.T) {
		store := harness.newStore(t)
		handle := insertCommittedRow(t, store, table, storage.NewRow(types.Int32Value(1), types.StringValue("alpha")))

		tx := mustNewTransaction(t, store, storage.TransactionOptions{})
		if err := store.Delete(tx, table, handle); err != nil {
			t.Fatalf("Delete() error = %v", err)
		}
		if err := store.Update(tx, table, handle, storage.NewRow(types.Int32Value(1), types.StringValue("beta"))); !harness.matchers().RowNotFound(err) {
			t.Fatalf("Update() error did not match row not found contract: %v", err)
		}
	})

	t.Run("scan limit", func(t *testing.T) {
		store := harness.newStore(t)
		for _, value := range []int32{1, 2, 3, 4} {
			commitRow(t, store, table, storage.NewRow(types.Int32Value(value), types.StringValue("row")))
		}

		records := collectReadOnlyRecords(t, store, table, storage.ScanOptions{Limit: 2})
		if len(records) != 2 {
			t.Fatalf("len(records) = %d, want 2", len(records))
		}
	})

	t.Run("scan constraints", func(t *testing.T) {
		store := harness.newStore(t)
		for _, value := range []int32{1, 2, 3, 4, 5} {
			commitRow(t, store, table, storage.NewRow(types.Int32Value(value), types.StringValue("row")))
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
			t.Run(tc.name, func(t *testing.T) {
				records := collectReadOnlyRecords(t, store, table, storage.ScanOptions{Constraints: tc.constraints})
				got := recordInts(t, records)
				assertInt32SliceEqual(t, got, tc.want)
			})
		}
	})

	t.Run("scan constraint type error", func(t *testing.T) {
		store := harness.newStore(t)
		insertCommittedRow(t, store, table, storage.NewRow(types.Int32Value(1), types.StringValue("row")))

		tx := mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true})
		_, err := store.Scan(tx, table, storage.ScanOptions{
			Constraints: []storage.ScanConstraint{
				{Column: 0, Op: storage.ComparisonEqual, Value: types.StringValue("row")},
			},
		})
		if err == nil {
			t.Fatal("Scan() error = nil, want a type comparison error")
		}
	})

	t.Run("iterator close and copies", func(t *testing.T) {
		store := harness.newStore(t)
		insertCommittedRow(t, store, table, storage.NewRow(types.BytesValue([]byte{1, 2, 3}), types.StringValue("row")))

		tx := mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true})
		iter := mustScan(t, store, tx, table, storage.ScanOptions{})

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

		fresh := collectReadOnlyRecords(t, store, table, storage.ScanOptions{})
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
		if _, err := iter.Next(); !harness.matchers().IteratorClosed(err) {
			t.Fatalf("Next() after Close error did not match iterator closed contract: %v", err)
		}
	})

	t.Run("concurrent scans and writes", func(t *testing.T) {
		store := harness.newStore(t)
		for _, value := range []int32{1, 2, 3, 4, 5} {
			commitRow(t, store, table, storage.NewRow(types.Int32Value(value), types.StringValue("seed")))
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

				iter, err := store.Scan(tx, table, storage.ScanOptions{})
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
				if _, err := store.Insert(tx, table, storage.NewRow(types.Int32Value(int32(index+10)), types.StringValue("writer"))); err != nil {
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

		records := collectReadOnlyRecords(t, store, table, storage.ScanOptions{})
		if len(records) != 11 {
			t.Fatalf("len(records) = %d, want 11", len(records))
		}
	})
}

func (h Harness) newStore(t *testing.T) storage.Storage {
	t.Helper()

	if h.NewStore == nil {
		t.Fatal("storagetest.Harness.NewStore is nil")
	}

	return h.NewStore(t)
}

func (h Harness) matchers() ErrorMatchers {
	return h.Matchers
}

func mustNewTransaction(t *testing.T, store storage.Storage, options storage.TransactionOptions) storage.Transaction {
	t.Helper()

	tx, err := store.NewTransaction(options)
	if err != nil {
		t.Fatalf("NewTransaction() error = %v", err)
	}

	return tx
}

func mustScan(
	t *testing.T,
	store storage.Storage,
	tx storage.Transaction,
	table storage.TableID,
	options storage.ScanOptions,
) storage.RowIterator {
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

func collectReadOnlyRecords(t *testing.T, store storage.Storage, table storage.TableID, options storage.ScanOptions) []storage.Record {
	t.Helper()

	tx := mustNewTransaction(t, store, storage.TransactionOptions{ReadOnly: true})
	defer func() {
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback() error = %v", err)
		}
	}()

	return collectRecords(t, mustScan(t, store, tx, table, options))
}

func closeIterator(t *testing.T, iter storage.RowIterator) {
	t.Helper()

	if err := iter.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func insertCommittedRow(t *testing.T, store storage.Storage, table storage.TableID, row storage.Row) storage.RowHandle {
	t.Helper()

	before := collectReadOnlyRecords(t, store, table, storage.ScanOptions{})

	tx := mustNewTransaction(t, store, storage.TransactionOptions{})
	handle, err := store.Insert(tx, table, row)
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if !handle.Valid() {
		t.Fatal("Insert() returned an invalid handle")
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	records := collectReadOnlyRecords(t, store, table, storage.ScanOptions{})
	if len(records) != len(before)+1 {
		t.Fatalf("len(records) after committed insert = %d, want %d", len(records), len(before)+1)
	}

	for _, record := range records {
		if recordsContainHandle(before, record.Handle) {
			continue
		}
		assertRowEqual(t, record.Row, row)
		if !record.Handle.Valid() {
			t.Fatal("committed scan handle is invalid")
		}
		return record.Handle
	}

	t.Fatal("committed insert handle not found in post-commit scan")
	return storage.RowHandle{}
}

func commitRow(t *testing.T, store storage.Storage, table storage.TableID, row storage.Row) {
	t.Helper()

	tx := mustNewTransaction(t, store, storage.TransactionOptions{})
	handle, err := store.Insert(tx, table, row)
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if !handle.Valid() {
		t.Fatal("Insert() returned an invalid handle")
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
}

func recordsContainHandle(records []storage.Record, handle storage.RowHandle) bool {
	for _, record := range records {
		if record.Handle == handle {
			return true
		}
	}
	return false
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

func assertSingleStringValueByID(t *testing.T, records []storage.Record, id int32, want string) {
	t.Helper()

	for _, record := range records {
		idValue, ok := record.Row.Value(0)
		if !ok {
			t.Fatal("Value(0) = (_, false), want (_, true)")
		}
		rawID, ok := idValue.Raw().(int32)
		if !ok {
			t.Fatalf("Value(0).Raw() type = %T, want int32", idValue.Raw())
		}
		if rawID != id {
			continue
		}

		value, ok := record.Row.Value(1)
		if !ok {
			t.Fatal("Value(1) = (_, false), want (_, true)")
		}
		if !value.Equal(types.StringValue(want)) {
			t.Fatalf("Value(1) = %v, want %v", value, types.StringValue(want))
		}
		return
	}

	t.Fatalf("record with id %d not found in %v", id, records)
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

func assertInt32SliceEqual(t *testing.T, got, want []int32) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("record count = %d, want %d (%v)", len(got), len(want), want)
	}
	for index := range got {
		if got[index] != want[index] {
			t.Fatalf("records = %v, want %v", got, want)
		}
	}
}

func assertInt32ContentsEqual(t *testing.T, got, want []int32) {
	t.Helper()

	gotCopy := slices.Clone(got)
	wantCopy := slices.Clone(want)
	slices.Sort(gotCopy)
	slices.Sort(wantCopy)
	assertInt32SliceEqual(t, gotCopy, wantCopy)
}

func assertRowEqual(t *testing.T, got, want storage.Row) {
	t.Helper()

	gotValues := got.Values()
	wantValues := want.Values()
	if len(gotValues) != len(wantValues) {
		t.Fatalf("row length = %d, want %d", len(gotValues), len(wantValues))
	}
	for index := range wantValues {
		if !gotValues[index].Equal(wantValues[index]) {
			t.Fatalf("row value[%d] = %#v, want %#v", index, gotValues[index], wantValues[index])
		}
	}
}
