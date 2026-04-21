package paged

import (
	"errors"
	"strings"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/storage"
)

func TestRelationInsertWorksWithSingleFrameCache(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenHeapManager(root, 512, 1)
	if err != nil {
		t.Fatalf("open heap manager: %v", err)
	}
	defer func() {
		_ = manager.Close()
	}()

	desc := pagedTestTableDescriptor("single_frame")
	if err := manager.CreateTable(nil, desc); err != nil {
		t.Fatalf("create table: %v", err)
	}
	relation, err := manager.OpenRelation(desc)
	if err != nil {
		t.Fatalf("open relation: %v", err)
	}

	want := pagedTestRow(1, "cached")
	handle, err := relation.Insert(want)
	if err != nil {
		t.Fatalf("insert row: %v", err)
	}
	if handle != (storage.RowHandle{Page: 1, Slot: 0}) {
		t.Fatalf("insert handle = %#v, want page 1 slot 0", handle)
	}

	got, err := relation.Lookup(handle)
	if err != nil {
		t.Fatalf("lookup row: %v", err)
	}
	assertStorageRowEqual(t, got, want)
}

func TestRelationUpdatePreservesHandleThroughRedirect(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenHeapManager(root, 256, 2)
	if err != nil {
		t.Fatalf("open heap manager: %v", err)
	}
	defer func() {
		_ = manager.Close()
	}()

	desc := pagedTestTableDescriptor("updates")
	if err := manager.CreateTable(nil, desc); err != nil {
		t.Fatalf("create table: %v", err)
	}
	relation, err := manager.OpenRelation(desc)
	if err != nil {
		t.Fatalf("open relation: %v", err)
	}

	handle, err := relation.Insert(pagedTestRow(1, "tiny"))
	if err != nil {
		t.Fatalf("insert row: %v", err)
	}

	want := pagedTestRow(1, strings.Repeat("z", 96))
	if err := relation.Update(handle, want); err != nil {
		t.Fatalf("update row: %v", err)
	}

	got, err := relation.Lookup(handle)
	if err != nil {
		t.Fatalf("lookup updated row: %v", err)
	}
	assertStorageRowEqual(t, got, want)

	page, err := relation.fetchHeapPage(PageID(handle.Page))
	if err != nil {
		t.Fatalf("fetch root page: %v", err)
	}
	defer relation.releasePinnedHeapPage(page)

	slot, err := page.heap.readSlot(handle.Slot)
	if err != nil {
		t.Fatalf("read root slot: %v", err)
	}
	if slot.Flags != slotFlagRedirect {
		t.Fatalf("root slot flags = 0x%x, want redirect", slot.Flags)
	}
}

func TestRelationDeleteRemovesRedirectedHandle(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenHeapManager(root, 256, 2)
	if err != nil {
		t.Fatalf("open heap manager: %v", err)
	}
	defer func() {
		_ = manager.Close()
	}()

	desc := pagedTestTableDescriptor("deletes")
	if err := manager.CreateTable(nil, desc); err != nil {
		t.Fatalf("create table: %v", err)
	}
	relation, err := manager.OpenRelation(desc)
	if err != nil {
		t.Fatalf("open relation: %v", err)
	}

	handle, err := relation.Insert(pagedTestRow(1, "tiny"))
	if err != nil {
		t.Fatalf("insert row: %v", err)
	}
	if err := relation.Update(handle, pagedTestRow(1, strings.Repeat("q", 96))); err != nil {
		t.Fatalf("update row: %v", err)
	}
	if err := relation.Delete(handle); err != nil {
		t.Fatalf("delete row: %v", err)
	}
	if _, err := relation.Lookup(handle); !errors.Is(err, ErrRowNotFound) {
		t.Fatalf("lookup deleted row error = %v, want %v", err, ErrRowNotFound)
	}
}
