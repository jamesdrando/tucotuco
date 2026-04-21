package paged

import (
	"strings"
	"testing"

	walpkg "github.com/jamesdrando/tucotuco/internal/wal"
)

func TestRelationWALTracksMultiPageMutations(t *testing.T) {
	root := t.TempDir()
	manager, err := OpenHeapManager(root, 512, 1)
	if err != nil {
		t.Fatalf("open heap manager: %v", err)
	}

	desc := pagedTestTableDescriptor("wal_pages")
	if err := manager.CreateTable(nil, desc); err != nil {
		t.Fatalf("create table: %v", err)
	}
	relation, err := manager.OpenRelation(desc)
	if err != nil {
		t.Fatalf("open relation: %v", err)
	}

	rows := []string{
		strings.Repeat("a", 80),
		strings.Repeat("b", 80),
		strings.Repeat("c", 80),
		strings.Repeat("d", 80),
	}
	for index, note := range rows {
		if _, err := relation.Insert(pagedTestRow(int32(index+1), note)); err != nil {
			t.Fatalf("insert row %d: %v", index+1, err)
		}
	}

	if err := manager.Close(); err != nil {
		t.Fatalf("close manager: %v", err)
	}

	log, err := walpkg.Open(walFilePath(root))
	if err != nil {
		t.Fatalf("open wal file: %v", err)
	}
	defer func() {
		_ = log.Close()
	}()

	records, err := log.Records()
	if err != nil {
		t.Fatalf("read wal records: %v", err)
	}
	if len(records) < 9 {
		t.Fatalf("record count = %d, want at least 9", len(records))
	}

	lastByPage := make(map[PageID]walpkg.PersistedRecord)
	var previousLSN walpkg.LSN
	resource := relationFileName(desc.ID)
	for _, record := range records {
		if record.LSN == 0 || record.LSN <= previousLSN {
			t.Fatalf("non-monotonic LSNs: prev=%d current=%d", previousLSN, record.LSN)
		}
		previousLSN = record.LSN
		if record.Resource != resource {
			t.Fatalf("record resource = %q, want %q", record.Resource, resource)
		}

		header, err := ValidatePageImage(record.Payload, 512, PageID(record.PageID))
		if err != nil {
			t.Fatalf("validate WAL page image %d at LSN %d: %v", record.PageID, record.LSN, err)
		}
		if header.PageLSN != uint64(record.LSN) {
			t.Fatalf("wal page %d header LSN = %d, want %d", record.PageID, header.PageLSN, record.LSN)
		}
		lastByPage[PageID(record.PageID)] = record
	}

	store, err := OpenFileStore(relationFilePath(root, desc.ID), 512)
	if err != nil {
		t.Fatalf("open relation file: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	pageCount, err := store.PageCount()
	if err != nil {
		t.Fatalf("page count: %v", err)
	}
	if pageCount != 3 {
		t.Fatalf("page count = %d, want 3", pageCount)
	}

	for _, pageID := range []PageID{0, 1, 2} {
		record, ok := lastByPage[pageID]
		if !ok {
			t.Fatalf("missing WAL record for page %d", pageID)
		}

		page := make([]byte, 512)
		if err := store.ReadPage(pageID, page); err != nil {
			t.Fatalf("read page %d: %v", pageID, err)
		}
		header, err := ValidatePageImage(page, 512, pageID)
		if err != nil {
			t.Fatalf("validate persisted page %d: %v", pageID, err)
		}
		if header.PageLSN == 0 {
			t.Fatalf("persisted page %d has zero PageLSN", pageID)
		}
		if header.PageLSN != uint64(record.LSN) {
			t.Fatalf("persisted page %d LSN = %d, want %d", pageID, header.PageLSN, record.LSN)
		}
	}
}
