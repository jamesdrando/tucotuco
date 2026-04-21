package paged

import (
	"bytes"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/catalog"
	"github.com/jamesdrando/tucotuco/internal/storage"
	walpkg "github.com/jamesdrando/tucotuco/internal/wal"
)

const recoveryTestPageSize = 512

type recoveryLookupExpectation struct {
	handle storage.RowHandle
	want   storage.Row
}

type recoveryReopenFixture struct {
	lookups           []recoveryLookupExpectation
	corruptPageIDs    []PageID
	corruptPageImages map[PageID][]byte
	latestPageImages  map[PageID][]byte
}

func TestHeapManagerReplaysWALOnReopenForCorruptRelationPages(t *testing.T) {
	testCases := []struct {
		name    string
		prepare func(t *testing.T, root string, desc *catalog.TableDescriptor) recoveryReopenFixture
	}{
		{
			name:    "restores_stale_relation_pages",
			prepare: prepareStaleRelationRecoveryFixture,
		},
		{
			name:    "restores_invalid_relation_pages",
			prepare: prepareInvalidRelationRecoveryFixture,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			desc := pagedTestTableDescriptor(tc.name)
			fixture := tc.prepare(t, root, desc)

			overwriteRelationPageImages(t, root, desc, recoveryTestPageSize, fixture.corruptPageImages)

			reopenedManager, reopenedRelation := openRecoveryTestRelation(t, root, desc)
			for _, lookup := range fixture.lookups {
				row, err := reopenedRelation.Lookup(lookup.handle)
				if err != nil {
					_ = reopenedManager.Close()
					t.Fatalf("lookup %s after reopen: %v", lookup.handle, err)
				}
				assertStorageRowEqual(t, row, lookup.want)
			}
			if err := reopenedManager.Close(); err != nil {
				t.Fatalf("close reopened manager: %v", err)
			}

			assertRelationPageImagesEqual(t, root, desc, recoveryTestPageSize, fixture.corruptPageIDs, fixture.latestPageImages)
		})
	}
}

func prepareStaleRelationRecoveryFixture(t *testing.T, root string, desc *catalog.TableDescriptor) recoveryReopenFixture {
	t.Helper()

	manager, relation := createRecoveryTestRelation(t, root, desc)

	firstRow := pagedTestRow(1, "stale-base-row")
	firstHandle, err := relation.Insert(firstRow)
	if err != nil {
		t.Fatalf("insert first row: %v", err)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("close initial manager: %v", err)
	}

	stalePageImages := readRelationPageImages(t, root, desc, recoveryTestPageSize, 0, 1)

	manager, relation = openRecoveryTestRelation(t, root, desc)
	secondRow := pagedTestRow(2, "redo-restored-row")
	secondHandle, err := relation.Insert(secondRow)
	if err != nil {
		t.Fatalf("insert second row: %v", err)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("close stale-source manager: %v", err)
	}

	latestPageImages := selectRelationPageImages(t, latestRelationPageImages(t, root, desc), 0, 1)
	for _, pageID := range []PageID{0, 1} {
		if bytes.Equal(stalePageImages[pageID], latestPageImages[pageID]) {
			t.Fatalf("page %d did not diverge between stale and latest images", pageID)
		}
	}

	return recoveryReopenFixture{
		lookups: []recoveryLookupExpectation{
			{handle: firstHandle, want: firstRow},
			{handle: secondHandle, want: secondRow},
		},
		corruptPageIDs:    []PageID{0, 1},
		corruptPageImages: stalePageImages,
		latestPageImages:  latestPageImages,
	}
}

func prepareInvalidRelationRecoveryFixture(t *testing.T, root string, desc *catalog.TableDescriptor) recoveryReopenFixture {
	t.Helper()

	manager, relation := createRecoveryTestRelation(t, root, desc)

	row := pagedTestRow(7, "redo-from-invalid-page")
	handle, err := relation.Insert(row)
	if err != nil {
		t.Fatalf("insert row: %v", err)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("close invalid-source manager: %v", err)
	}

	return recoveryReopenFixture{
		lookups: []recoveryLookupExpectation{
			{handle: handle, want: row},
		},
		corruptPageIDs:    []PageID{0, 1},
		corruptPageImages: zeroedRelationPageImages(recoveryTestPageSize, 0, 1),
		latestPageImages:  selectRelationPageImages(t, latestRelationPageImages(t, root, desc), 0, 1),
	}
}

func createRecoveryTestRelation(t *testing.T, root string, desc *catalog.TableDescriptor) (*HeapManager, *Relation) {
	t.Helper()

	manager, err := OpenHeapManager(root, recoveryTestPageSize, 2)
	if err != nil {
		t.Fatalf("open heap manager: %v", err)
	}
	if err := manager.CreateTable(nil, desc); err != nil {
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

func openRecoveryTestRelation(t *testing.T, root string, desc *catalog.TableDescriptor) (*HeapManager, *Relation) {
	t.Helper()

	manager, err := OpenHeapManager(root, recoveryTestPageSize, 2)
	if err != nil {
		t.Fatalf("open heap manager: %v", err)
	}
	relation, err := manager.OpenRelation(desc)
	if err != nil {
		_ = manager.Close()
		t.Fatalf("open relation: %v", err)
	}
	return manager, relation
}

func readRelationPageImages(
	t *testing.T,
	root string,
	desc *catalog.TableDescriptor,
	pageSize int,
	pageIDs ...PageID,
) map[PageID][]byte {
	t.Helper()

	store, err := OpenFileStore(relationFilePath(root, desc.ID), pageSize)
	if err != nil {
		t.Fatalf("open relation store: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	images := make(map[PageID][]byte, len(pageIDs))
	for _, pageID := range pageIDs {
		page := make([]byte, pageSize)
		if err := store.ReadPage(pageID, page); err != nil {
			t.Fatalf("read relation page %d: %v", pageID, err)
		}
		images[pageID] = page
	}
	return images
}

func latestRelationPageImages(t *testing.T, root string, desc *catalog.TableDescriptor) map[PageID][]byte {
	t.Helper()

	log, err := walpkg.Open(walFilePath(root))
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() {
		_ = log.Close()
	}()

	records, err := log.Records()
	if err != nil {
		t.Fatalf("read wal records: %v", err)
	}

	resource := relationFileName(desc.ID)
	latestLSNByPage := make(map[PageID]walpkg.LSN)
	latestImageByPage := make(map[PageID][]byte)
	for _, record := range records {
		if record.Type != walpkg.RecordTypePageImage || record.Resource != resource {
			continue
		}

		pageID := PageID(record.PageID)
		if record.LSN <= latestLSNByPage[pageID] {
			continue
		}
		latestLSNByPage[pageID] = record.LSN
		latestImageByPage[pageID] = append([]byte(nil), record.Payload...)
	}
	if len(latestImageByPage) == 0 {
		t.Fatalf("no wal page images for relation %s", desc.ID)
	}
	return latestImageByPage
}

func selectRelationPageImages(t *testing.T, images map[PageID][]byte, pageIDs ...PageID) map[PageID][]byte {
	t.Helper()

	selected := make(map[PageID][]byte, len(pageIDs))
	for _, pageID := range pageIDs {
		image, ok := images[pageID]
		if !ok {
			t.Fatalf("missing page image for page %d", pageID)
		}
		selected[pageID] = append([]byte(nil), image...)
	}
	return selected
}

func overwriteRelationPageImages(
	t *testing.T,
	root string,
	desc *catalog.TableDescriptor,
	pageSize int,
	images map[PageID][]byte,
) {
	t.Helper()

	store, err := OpenFileStore(relationFilePath(root, desc.ID), pageSize)
	if err != nil {
		t.Fatalf("open relation store for overwrite: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	for pageID, image := range images {
		if len(image) != pageSize {
			t.Fatalf("page %d image size = %d, want %d", pageID, len(image), pageSize)
		}
		if err := store.WritePage(pageID, image); err != nil {
			t.Fatalf("write page %d: %v", pageID, err)
		}
	}
	if err := store.Sync(); err != nil {
		t.Fatalf("sync overwritten relation pages: %v", err)
	}
}

func zeroedRelationPageImages(pageSize int, pageIDs ...PageID) map[PageID][]byte {
	images := make(map[PageID][]byte, len(pageIDs))
	for _, pageID := range pageIDs {
		images[pageID] = make([]byte, pageSize)
	}
	return images
}

func assertRelationPageImagesEqual(
	t *testing.T,
	root string,
	desc *catalog.TableDescriptor,
	pageSize int,
	pageIDs []PageID,
	want map[PageID][]byte,
) {
	t.Helper()

	got := readRelationPageImages(t, root, desc, pageSize, pageIDs...)
	for _, pageID := range pageIDs {
		if !bytes.Equal(got[pageID], want[pageID]) {
			t.Fatalf("persisted page %d does not match latest wal image", pageID)
		}
	}
}
