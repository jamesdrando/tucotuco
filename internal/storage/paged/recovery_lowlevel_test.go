package paged

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/wal"
)

func TestRecoveryLowLevelRedoIsIdempotentWithPageLSNGuard(t *testing.T) {
	const pageSize = 512

	root := t.TempDir()
	log := mustOpenLowLevelWAL(t, root)
	store := mustOpenLowLevelStore(t, root, pageSize)

	first := appendLowLevelPageImageRecord(t, log, 1, PageTypeHeap, pageSize, 0x11)
	second := appendLowLevelPageImageRecord(t, log, 1, PageTypeHeap, pageSize, 0x77)
	if first.LSN >= second.LSN {
		t.Fatalf("wal LSNs out of order: first=%d second=%d", first.LSN, second.LSN)
	}

	records := mustPersistedRecords(t, log)
	if applied := replayLowLevelPageImageRecords(t, store, records); applied != 2 {
		t.Fatalf("first redo pass applied %d records, want 2", applied)
	}

	assertStoredPageEquals(t, store, 1, second.Payload)

	if applied := replayLowLevelPageImageRecords(t, store, records); applied != 0 {
		t.Fatalf("second redo pass applied %d records, want 0", applied)
	}

	assertStoredPageEquals(t, store, 1, second.Payload)
}

func TestRecoveryLowLevelRepairsCrashTailBeforePageValidation(t *testing.T) {
	const pageSize = 512

	root := t.TempDir()
	log := mustOpenLowLevelWAL(t, root)
	store := mustOpenLowLevelStore(t, root, pageSize)

	pageID, err := store.AllocatePage()
	if err != nil {
		t.Fatalf("allocate crash-tail page: %v", err)
	}
	if pageID != 1 {
		t.Fatalf("allocated page id = %d, want 1", pageID)
	}

	tail := make([]byte, pageSize)
	if err := store.ReadPage(pageID, tail); err != nil {
		t.Fatalf("read crash-tail page: %v", err)
	}
	if _, err := ValidatePageImage(tail, pageSize, pageID); err == nil {
		t.Fatal("expected zeroed crash-tail page to fail validation before redo")
	}

	record := appendLowLevelPageImageRecord(t, log, pageID, PageTypeHeap, pageSize, 0x42)
	if applied := replayLowLevelPageImageRecords(t, store, mustPersistedRecords(t, log)); applied != 1 {
		t.Fatalf("crash-tail redo applied %d records, want 1", applied)
	}

	assertStoredPageEquals(t, store, pageID, record.Payload)
}

func mustOpenLowLevelWAL(t *testing.T, root string) *wal.Log {
	t.Helper()

	log, err := wal.Open(filepath.Join(root, "recovery-lowlevel.wal"))
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() {
		if err := log.Close(); err != nil {
			t.Fatalf("close wal: %v", err)
		}
	})
	return log
}

func mustOpenLowLevelStore(t *testing.T, root string, pageSize int) *FileStore {
	t.Helper()

	store, err := OpenFileStore(filepath.Join(root, "recovery-lowlevel.rel"), pageSize)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	})
	return store
}

func appendLowLevelPageImageRecord(
	t *testing.T,
	log *wal.Log,
	pageID PageID,
	pageType PageType,
	pageSize int,
	marker byte,
) wal.PersistedRecord {
	t.Helper()

	page, err := NewPageImage(pageID, pageType, pageSize)
	if err != nil {
		t.Fatalf("new page image: %v", err)
	}
	page[pageHeaderSize] = marker
	page[len(page)-1] = marker ^ 0xff

	lsn, err := log.AppendWith(func(lsn wal.LSN) (wal.Record, error) {
		payload, err := finalizedWALPayload(page, lsn)
		if err != nil {
			return wal.Record{}, err
		}
		return wal.Record{
			Type:     wal.RecordTypePageImage,
			Resource: "recovery-lowlevel.rel",
			PageID:   uint64(pageID),
			Payload:  payload,
		}, nil
	})
	if err != nil {
		t.Fatalf("append wal record: %v", err)
	}

	for _, record := range mustPersistedRecords(t, log) {
		if record.LSN != lsn {
			continue
		}
		return record
	}

	t.Fatalf("wal record %d not found after append", lsn)
	return wal.PersistedRecord{}
}

func mustPersistedRecords(t *testing.T, log *wal.Log) []wal.PersistedRecord {
	t.Helper()

	records, err := log.Records()
	if err != nil {
		t.Fatalf("read wal records: %v", err)
	}
	return records
}

// replayLowLevelPageImageRecords mirrors page-image redo semantics directly so
// recovery behavior can be pinned without relation/catalog dependencies.
func replayLowLevelPageImageRecords(t *testing.T, store *FileStore, records []wal.PersistedRecord) int {
	t.Helper()

	applied := 0
	pageSize := store.PageSize()

	for _, record := range records {
		if record.Type != wal.RecordTypePageImage {
			continue
		}

		pageID := PageID(record.PageID)
		header, err := ValidatePageImage(record.Payload, pageSize, pageID)
		if err != nil {
			t.Fatalf("validate WAL payload for page %d at LSN %d: %v", pageID, record.LSN, err)
		}
		if header.PageLSN != uint64(record.LSN) {
			t.Fatalf("WAL payload page %d carries PageLSN %d, want %d", pageID, header.PageLSN, record.LSN)
		}

		current := make([]byte, pageSize)
		if err := store.ReadPage(pageID, current); err == nil {
			if existing, err := ValidatePageImage(current, pageSize, pageID); err == nil && existing.PageLSN >= uint64(record.LSN) {
				continue
			}
		}

		if err := store.WritePage(pageID, record.Payload); err != nil {
			t.Fatalf("write recovered page %d: %v", pageID, err)
		}
		applied++
	}

	return applied
}

func assertStoredPageEquals(t *testing.T, store *FileStore, pageID PageID, want []byte) {
	t.Helper()

	got := make([]byte, store.PageSize())
	if err := store.ReadPage(pageID, got); err != nil {
		t.Fatalf("read stored page %d: %v", pageID, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("stored page %d does not match expected WAL image", pageID)
	}

	header, err := ValidatePageImage(got, store.PageSize(), pageID)
	if err != nil {
		t.Fatalf("validate stored page %d: %v", pageID, err)
	}

	wantHeader, err := DecodePageHeader(want)
	if err != nil {
		t.Fatalf("decode expected page %d header: %v", pageID, err)
	}
	if header.PageLSN != wantHeader.PageLSN {
		t.Fatalf("stored page %d PageLSN = %d, want %d", pageID, header.PageLSN, wantHeader.PageLSN)
	}
}
