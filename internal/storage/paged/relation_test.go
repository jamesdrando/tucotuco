package paged

import (
	"errors"
	"strings"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/catalog"
	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/types"
	"github.com/jamesdrando/tucotuco/internal/wal"
)

func TestHeapManagerCreateTableInitializesMetadataPage(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenHeapManager(root, 512, 2)
	if err != nil {
		t.Fatalf("open heap manager: %v", err)
	}
	defer func() {
		_ = manager.Close()
	}()

	desc := pagedTestTableDescriptor("items")
	if err := manager.CreateTable(nil, desc); err != nil {
		t.Fatalf("create table: %v", err)
	}

	relation, err := manager.OpenRelation(desc)
	if err != nil {
		t.Fatalf("open relation: %v", err)
	}

	metadata, err := relation.Metadata()
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	if metadata.Table != desc.ID {
		t.Fatalf("metadata table = %#v, want %#v", metadata.Table, desc.ID)
	}
	if metadata.PageSize != 512 {
		t.Fatalf("metadata page size = %d, want 512", metadata.PageSize)
	}
	if metadata.FirstHeapPage != 0 || metadata.LastHeapPage != 0 || metadata.InsertHint != 0 {
		t.Fatalf("unexpected empty-relation metadata: %#v", metadata)
	}

	store, err := OpenFileStore(relationFilePath(root, desc.ID), 512)
	if err != nil {
		t.Fatalf("open file store: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	page := make([]byte, 512)
	if err := store.ReadPage(0, page); err != nil {
		t.Fatalf("read metadata page: %v", err)
	}
	header, err := ValidatePageImage(page, 512, 0)
	if err != nil {
		t.Fatalf("validate metadata page: %v", err)
	}
	if header.PageType != PageTypeMetadata {
		t.Fatalf("page 0 type = %d, want metadata", header.PageType)
	}
	if header.Lower != pageHeaderSize || header.SlotCount != 0 {
		t.Fatalf("unexpected metadata header bounds: %#v", header)
	}

	rawMetadata, err := decodeRelationMetadata(page[header.Upper:header.Special])
	if err != nil {
		t.Fatalf("decode metadata payload: %v", err)
	}
	if rawMetadata != metadata {
		t.Fatalf("decoded metadata = %#v, want %#v", rawMetadata, metadata)
	}
}

func TestHeapManagerCreateTableWritesMetadataWALRecord(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenHeapManager(root, 512, 2)
	if err != nil {
		t.Fatalf("open heap manager: %v", err)
	}
	defer func() {
		_ = manager.Close()
	}()

	desc := pagedTestTableDescriptor("wal_metadata")
	if err := manager.CreateTable(nil, desc); err != nil {
		t.Fatalf("create table: %v", err)
	}

	records, err := manager.wal.Records()
	if err != nil {
		t.Fatalf("read wal records: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("wal record count = %d, want 1", len(records))
	}

	record := records[0]
	if record.Type != wal.RecordTypePageImage {
		t.Fatalf("wal record type = %d, want page image", record.Type)
	}
	if record.Resource != relationFileName(desc.ID) {
		t.Fatalf("wal resource = %q, want %q", record.Resource, relationFileName(desc.ID))
	}
	if record.PageID != 0 {
		t.Fatalf("wal page id = %d, want 0", record.PageID)
	}

	store, err := OpenFileStore(relationFilePath(root, desc.ID), 512)
	if err != nil {
		t.Fatalf("open file store: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	page := make([]byte, 512)
	if err := store.ReadPage(0, page); err != nil {
		t.Fatalf("read metadata page: %v", err)
	}
	header, err := ValidatePageImage(page, 512, 0)
	if err != nil {
		t.Fatalf("validate metadata page: %v", err)
	}
	if header.PageLSN != uint64(record.LSN) {
		t.Fatalf("metadata page lsn = %d, want %d", header.PageLSN, record.LSN)
	}
}

func TestRelationInsertSelectsExistingPageBeforeAllocatingNewOne(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenHeapManager(root, 512, 4)
	if err != nil {
		t.Fatalf("open heap manager: %v", err)
	}
	defer func() {
		_ = manager.Close()
	}()

	desc := pagedTestTableDescriptor("widgets")
	if err := manager.CreateTable(nil, desc); err != nil {
		t.Fatalf("create table: %v", err)
	}
	relation, err := manager.OpenRelation(desc)
	if err != nil {
		t.Fatalf("open relation: %v", err)
	}

	rows := []storage.Row{
		pagedTestRow(1, strings.Repeat("a", 80)),
		pagedTestRow(2, strings.Repeat("b", 80)),
		pagedTestRow(3, strings.Repeat("c", 80)),
		pagedTestRow(4, strings.Repeat("d", 80)),
	}

	handles := make([]storage.RowHandle, 0, len(rows))
	for _, row := range rows {
		handle, err := relation.Insert(row)
		if err != nil {
			t.Fatalf("insert %#v: %v", row, err)
		}
		handles = append(handles, handle)
	}

	want := []storage.RowHandle{
		{Page: 1, Slot: 0},
		{Page: 1, Slot: 1},
		{Page: 1, Slot: 2},
		{Page: 2, Slot: 0},
	}
	for index := range want {
		if handles[index] != want[index] {
			t.Fatalf("handle[%d] = %#v, want %#v", index, handles[index], want[index])
		}
	}

	metadata, err := relation.Metadata()
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	if metadata.FirstHeapPage != 1 || metadata.LastHeapPage != 2 || metadata.InsertHint != 2 {
		t.Fatalf("unexpected post-insert metadata: %#v", metadata)
	}

	pageCount, err := relation.store.PageCount()
	if err != nil {
		t.Fatalf("page count: %v", err)
	}
	if pageCount != 3 {
		t.Fatalf("page count = %d, want 3", pageCount)
	}

	assertHeapPageSlotCount(t, relation, 1, 3)
	assertHeapPageSlotCount(t, relation, 2, 1)
}

func TestRelationInsertWritesWALForMetadataAndHeapPage(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenHeapManager(root, 512, 4)
	if err != nil {
		t.Fatalf("open heap manager: %v", err)
	}

	desc := pagedTestTableDescriptor("wal_insert")
	if err := manager.CreateTable(nil, desc); err != nil {
		_ = manager.Close()
		t.Fatalf("create table: %v", err)
	}
	relation, err := manager.OpenRelation(desc)
	if err != nil {
		_ = manager.Close()
		t.Fatalf("open relation: %v", err)
	}
	if _, err := relation.Insert(pagedTestRow(1, strings.Repeat("a", 80))); err != nil {
		_ = manager.Close()
		t.Fatalf("insert row: %v", err)
	}

	records, err := manager.wal.Records()
	if err != nil {
		_ = manager.Close()
		t.Fatalf("read wal records: %v", err)
	}
	latestByPage := latestRelationPageLSNs(records, desc.ID)
	if latestByPage[0] == 0 || latestByPage[1] == 0 {
		_ = manager.Close()
		t.Fatalf("latest page lsns = %#v, want non-zero page 0 and 1 lsns", latestByPage)
	}

	if err := manager.Close(); err != nil {
		t.Fatalf("close manager: %v", err)
	}

	store, err := OpenFileStore(relationFilePath(root, desc.ID), 512)
	if err != nil {
		t.Fatalf("open file store: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	assertPersistedPageLSN(t, store, 512, 0, latestByPage[0])
	assertPersistedPageLSN(t, store, 512, 1, latestByPage[1])
}

func TestRelationLookupRoutesPageAndSlotHandle(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenHeapManager(root, 512, 4)
	if err != nil {
		t.Fatalf("open heap manager: %v", err)
	}
	defer func() {
		_ = manager.Close()
	}()

	desc := pagedTestTableDescriptor("routing")
	if err := manager.CreateTable(nil, desc); err != nil {
		t.Fatalf("create table: %v", err)
	}
	relation, err := manager.OpenRelation(desc)
	if err != nil {
		t.Fatalf("open relation: %v", err)
	}

	firstHandle, err := relation.Insert(pagedTestRow(1, "alpha"))
	if err != nil {
		t.Fatalf("insert first row: %v", err)
	}

	var finalHandle storage.RowHandle
	finalRow := pagedTestRow(4, strings.Repeat("z", 80))
	for i := 2; i <= 4; i++ {
		row := pagedTestRow(int32(i), strings.Repeat(string(rune('a'+i)), 80))
		if i == 4 {
			row = finalRow
		}

		handle, insertErr := relation.Insert(row)
		if insertErr != nil {
			t.Fatalf("insert row %d: %v", i, insertErr)
		}
		if i == 4 {
			finalHandle = handle
		}
	}

	firstRow, err := relation.Lookup(firstHandle)
	if err != nil {
		t.Fatalf("lookup first row: %v", err)
	}
	assertStorageRowEqual(t, firstRow, pagedTestRow(1, "alpha"))

	routedRow, err := relation.Lookup(finalHandle)
	if err != nil {
		t.Fatalf("lookup routed row: %v", err)
	}
	assertStorageRowEqual(t, routedRow, finalRow)

	if _, err := relation.Lookup(storage.RowHandle{Page: 0, Slot: 1}); !errors.Is(err, ErrRowNotFound) {
		t.Fatalf("lookup page 0 error = %v, want %v", err, ErrRowNotFound)
	}
	if _, err := relation.Lookup(storage.RowHandle{Page: 2, Slot: 9}); !errors.Is(err, ErrRowNotFound) {
		t.Fatalf("lookup missing slot error = %v, want %v", err, ErrRowNotFound)
	}
}

func TestRelationUpdateRewritesInPlaceAndDeleteMarksRowDead(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenHeapManager(root, 512, 4)
	if err != nil {
		t.Fatalf("open heap manager: %v", err)
	}
	defer func() {
		_ = manager.Close()
	}()

	desc := pagedTestTableDescriptor("rewrite")
	if err := manager.CreateTable(nil, desc); err != nil {
		t.Fatalf("create table: %v", err)
	}
	relation, err := manager.OpenRelation(desc)
	if err != nil {
		t.Fatalf("open relation: %v", err)
	}

	handle, err := relation.Insert(pagedTestRow(1, "alpha"))
	if err != nil {
		t.Fatalf("insert row: %v", err)
	}

	updated := pagedTestRow(1, "beta")
	if err := relation.Update(handle, updated); err != nil {
		t.Fatalf("update row: %v", err)
	}

	got, err := relation.Lookup(handle)
	if err != nil {
		t.Fatalf("lookup updated row: %v", err)
	}
	assertStorageRowEqual(t, got, updated)

	slot := readHeapPageSlot(t, relation, 1, 0)
	if slot.Flags != slotFlagLive {
		t.Fatalf("slot flags after in-place update = 0x%x, want live", slot.Flags)
	}

	if err := relation.Delete(handle); err != nil {
		t.Fatalf("delete row: %v", err)
	}
	if _, err := relation.Lookup(handle); !errors.Is(err, ErrRowNotFound) {
		t.Fatalf("lookup deleted row error = %v, want %v", err, ErrRowNotFound)
	}

	slot = readHeapPageSlot(t, relation, 1, 0)
	if slot.Flags != slotFlagDead {
		t.Fatalf("slot flags after delete = 0x%x, want dead", slot.Flags)
	}
}

func TestRelationUpdateRewritesRedirectTargetWithoutGrowingChains(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenHeapManager(root, 512, 4)
	if err != nil {
		t.Fatalf("open heap manager: %v", err)
	}
	defer func() {
		_ = manager.Close()
	}()

	desc := pagedTestTableDescriptor("redirects")
	if err := manager.CreateTable(nil, desc); err != nil {
		t.Fatalf("create table: %v", err)
	}
	relation, err := manager.OpenRelation(desc)
	if err != nil {
		t.Fatalf("open relation: %v", err)
	}

	handle, err := relation.Insert(pagedTestRow(1, "seed"))
	if err != nil {
		t.Fatalf("insert root row: %v", err)
	}
	if _, err := relation.Insert(pagedTestRow(2, "pad")); err != nil {
		t.Fatalf("insert filler row: %v", err)
	}

	firstUpdate := pagedTestRow(1, strings.Repeat("x", 120))
	if err := relation.Update(handle, firstUpdate); err != nil {
		t.Fatalf("first redirecting update: %v", err)
	}

	firstRedirect := assertHeapPageRedirect(t, relation, 1, 0)
	if firstRedirect != (storage.RowHandle{Page: 1, Slot: 2}) {
		t.Fatalf("first redirect handle = %#v, want page 1 slot 2", firstRedirect)
	}
	firstRoutedRow, err := relation.Lookup(handle)
	if err != nil {
		t.Fatalf("lookup after first redirect: %v", err)
	}
	assertStorageRowEqual(t, firstRoutedRow, firstUpdate)

	secondUpdate := pagedTestRow(1, strings.Repeat("y", 160))
	if err := relation.Update(handle, secondUpdate); err != nil {
		t.Fatalf("second redirecting update: %v", err)
	}

	secondRedirect := assertHeapPageRedirect(t, relation, 1, 0)
	if secondRedirect != (storage.RowHandle{Page: 2, Slot: 0}) {
		t.Fatalf("second redirect handle = %#v, want page 2 slot 0", secondRedirect)
	}
	intermediate := readHeapPageSlot(t, relation, 1, 2)
	if intermediate.Flags != slotFlagDead {
		t.Fatalf("intermediate slot flags = 0x%x, want dead", intermediate.Flags)
	}

	secondRoutedRow, err := relation.Lookup(handle)
	if err != nil {
		t.Fatalf("lookup after second redirect: %v", err)
	}
	assertStorageRowEqual(t, secondRoutedRow, secondUpdate)

	if err := relation.Delete(handle); err != nil {
		t.Fatalf("delete redirected row: %v", err)
	}
	if _, err := relation.Lookup(handle); !errors.Is(err, ErrRowNotFound) {
		t.Fatalf("lookup deleted redirected row error = %v, want %v", err, ErrRowNotFound)
	}

	rootSlot := readHeapPageSlot(t, relation, 1, 0)
	if rootSlot.Flags != slotFlagDead {
		t.Fatalf("root redirect slot flags after delete = 0x%x, want dead", rootSlot.Flags)
	}
	targetSlot := readHeapPageSlot(t, relation, 2, 0)
	if targetSlot.Flags != slotFlagDead {
		t.Fatalf("redirect target slot flags after delete = 0x%x, want dead", targetSlot.Flags)
	}
}

func TestRelationRedirectMutationsPersistLatestPageLSNs(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenHeapManager(root, 256, 4)
	if err != nil {
		t.Fatalf("open heap manager: %v", err)
	}

	desc := pagedTestTableDescriptor("wal_redirects")
	if err := manager.CreateTable(nil, desc); err != nil {
		_ = manager.Close()
		t.Fatalf("create table: %v", err)
	}
	relation, err := manager.OpenRelation(desc)
	if err != nil {
		_ = manager.Close()
		t.Fatalf("open relation: %v", err)
	}

	handle, err := relation.Insert(pagedTestRow(1, "tiny"))
	if err != nil {
		_ = manager.Close()
		t.Fatalf("insert row: %v", err)
	}
	if err := relation.Update(handle, pagedTestRow(1, strings.Repeat("z", 96))); err != nil {
		_ = manager.Close()
		t.Fatalf("update row: %v", err)
	}
	if err := relation.Delete(handle); err != nil {
		_ = manager.Close()
		t.Fatalf("delete row: %v", err)
	}

	records, err := manager.wal.Records()
	if err != nil {
		_ = manager.Close()
		t.Fatalf("read wal records: %v", err)
	}
	latestByPage := latestRelationPageLSNs(records, desc.ID)
	if latestByPage[1] == 0 || latestByPage[2] == 0 {
		_ = manager.Close()
		t.Fatalf("latest page lsns = %#v, want non-zero page 1 and 2 lsns", latestByPage)
	}

	if err := manager.Close(); err != nil {
		t.Fatalf("close manager: %v", err)
	}

	store, err := OpenFileStore(relationFilePath(root, desc.ID), 256)
	if err != nil {
		t.Fatalf("open file store: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	assertPersistedPageLSN(t, store, 256, 1, latestByPage[1])
	assertPersistedPageLSN(t, store, 256, 2, latestByPage[2])
}

func TestRelationInsertRejectsUnsupportedSchemaTypes(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenHeapManager(root, 512, 2)
	if err != nil {
		t.Fatalf("open heap manager: %v", err)
	}
	defer func() {
		_ = manager.Close()
	}()

	desc := &catalog.TableDescriptor{
		ID: storage.TableID{Schema: "public", Name: "json_only"},
		Columns: []catalog.ColumnDescriptor{
			{
				Name: "payload",
				Type: types.TypeDesc{Kind: types.TypeKindJSON, Nullable: false},
			},
		},
	}
	if err := manager.CreateTable(nil, desc); err != nil {
		t.Fatalf("create table: %v", err)
	}
	relation, err := manager.OpenRelation(desc)
	if err != nil {
		t.Fatalf("open relation: %v", err)
	}

	if _, err := relation.Insert(storage.NewRow(types.StringValue(`{"ok":true}`))); !errors.Is(err, ErrUnsupportedType) {
		t.Fatalf("insert unsupported type error = %v, want %v", err, ErrUnsupportedType)
	}
}

func TestHeapManagerReopensExistingRelation(t *testing.T) {
	root := t.TempDir()
	desc := pagedTestTableDescriptor("reopen")

	manager, err := OpenHeapManager(root, 512, 2)
	if err != nil {
		t.Fatalf("open heap manager: %v", err)
	}

	if err := manager.CreateTable(nil, desc); err != nil {
		t.Fatalf("create table: %v", err)
	}
	relation, err := manager.OpenRelation(desc)
	if err != nil {
		t.Fatalf("open relation: %v", err)
	}

	row := pagedTestRow(7, "persisted")
	handle, err := relation.Insert(row)
	if err != nil {
		t.Fatalf("insert row: %v", err)
	}

	if err := manager.Close(); err != nil {
		t.Fatalf("close first manager: %v", err)
	}

	reopenedManager, err := OpenHeapManager(root, 512, 2)
	if err != nil {
		t.Fatalf("reopen heap manager: %v", err)
	}
	defer func() {
		_ = reopenedManager.Close()
	}()

	reopenedRelation, err := reopenedManager.OpenRelation(desc)
	if err != nil {
		t.Fatalf("open reopened relation: %v", err)
	}

	reopenedMetadata, err := reopenedRelation.Metadata()
	if err != nil {
		t.Fatalf("metadata after reopen: %v", err)
	}
	if reopenedMetadata.Table != desc.ID || reopenedMetadata.FirstHeapPage != 1 || reopenedMetadata.LastHeapPage != 1 {
		t.Fatalf("unexpected reopened metadata: %#v", reopenedMetadata)
	}

	reopenedRow, err := reopenedRelation.Lookup(handle)
	if err != nil {
		t.Fatalf("lookup after reopen: %v", err)
	}
	assertStorageRowEqual(t, reopenedRow, row)
}

func pagedTestTableDescriptor(name string) *catalog.TableDescriptor {
	return &catalog.TableDescriptor{
		ID: storage.TableID{Schema: "public", Name: name},
		Columns: []catalog.ColumnDescriptor{
			{
				Name: "id",
				Type: types.TypeDesc{Kind: types.TypeKindInteger, Nullable: false},
			},
			{
				Name: "note",
				Type: types.TypeDesc{Kind: types.TypeKindVarChar, Length: 128, Nullable: true},
			},
		},
	}
}

func pagedTestRow(id int32, note string) storage.Row {
	return storage.NewRow(types.Int32Value(id), types.StringValue(note))
}

func assertHeapPageSlotCount(t *testing.T, relation *Relation, pageID PageID, want uint16) {
	t.Helper()

	page, err := relation.manager.Fetch(pageID)
	if err != nil {
		t.Fatalf("fetch page %d: %v", pageID, err)
	}
	defer func() {
		_ = relation.manager.Unpin(page, false)
	}()

	header, err := page.Header()
	if err != nil {
		t.Fatalf("header page %d: %v", pageID, err)
	}
	if header.SlotCount != want {
		t.Fatalf("page %d slot count = %d, want %d", pageID, header.SlotCount, want)
	}
}

func readHeapPageSlot(t *testing.T, relation *Relation, pageID PageID, slotIndex uint64) slotEntry {
	t.Helper()

	page, err := relation.manager.Fetch(pageID)
	if err != nil {
		t.Fatalf("fetch page %d: %v", pageID, err)
	}
	defer func() {
		_ = relation.manager.Unpin(page, false)
	}()

	heap, err := newHeapPage(page)
	if err != nil {
		t.Fatalf("decode heap page %d: %v", pageID, err)
	}
	slot, err := heap.readSlot(slotIndex)
	if err != nil {
		t.Fatalf("read page %d slot %d: %v", pageID, slotIndex, err)
	}
	return slot
}

func assertHeapPageRedirect(t *testing.T, relation *Relation, pageID PageID, slotIndex uint64) storage.RowHandle {
	t.Helper()

	page, err := relation.manager.Fetch(pageID)
	if err != nil {
		t.Fatalf("fetch page %d: %v", pageID, err)
	}
	defer func() {
		_ = relation.manager.Unpin(page, false)
	}()

	heap, err := newHeapPage(page)
	if err != nil {
		t.Fatalf("decode heap page %d: %v", pageID, err)
	}
	slot, err := heap.readSlot(slotIndex)
	if err != nil {
		t.Fatalf("read page %d slot %d: %v", pageID, slotIndex, err)
	}
	if slot.Flags != slotFlagRedirect {
		t.Fatalf("page %d slot %d flags = 0x%x, want redirect", pageID, slotIndex, slot.Flags)
	}
	handle, err := heap.redirectHandle(slot)
	if err != nil {
		t.Fatalf("decode redirect page %d slot %d: %v", pageID, slotIndex, err)
	}
	return handle
}

func assertStorageRowEqual(t *testing.T, got, want storage.Row) {
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

func latestRelationPageLSNs(records []wal.PersistedRecord, id storage.TableID) map[PageID]wal.LSN {
	resource := relationFileName(id)
	latest := make(map[PageID]wal.LSN)
	for _, record := range records {
		if record.Resource != resource {
			continue
		}
		pageID := PageID(record.PageID)
		if record.LSN > latest[pageID] {
			latest[pageID] = record.LSN
		}
	}
	return latest
}

func assertPersistedPageLSN(t *testing.T, store *FileStore, pageSize int, pageID PageID, want wal.LSN) {
	t.Helper()

	page := make([]byte, pageSize)
	if err := store.ReadPage(pageID, page); err != nil {
		t.Fatalf("read page %d: %v", pageID, err)
	}
	header, err := ValidatePageImage(page, pageSize, pageID)
	if err != nil {
		t.Fatalf("validate page %d: %v", pageID, err)
	}
	if header.PageLSN != uint64(want) {
		t.Fatalf("page %d lsn = %d, want %d", pageID, header.PageLSN, want)
	}
}
