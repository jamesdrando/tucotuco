package paged

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/wal"
)

func TestNewPageInitializesHeapHeader(t *testing.T) {
	store := newMemoryStore(t, 8192, 1)
	mgr, err := NewManager(store, 2)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() {
		_ = mgr.Close()
	}()

	page, err := mgr.NewPage(PageTypeHeap)
	if err != nil {
		t.Fatalf("new page: %v", err)
	}
	header, err := page.Header()
	if err != nil {
		t.Fatalf("decode header: %v", err)
	}
	if header.PageID != page.ID() {
		t.Fatalf("page id mismatch: header=%d page=%d", header.PageID, page.ID())
	}
	if header.PageType != PageTypeHeap {
		t.Fatalf("page type mismatch: %d", header.PageType)
	}
	if header.Lower != pageHeaderSize || header.Upper != 8192 || header.Special != 8192 {
		t.Fatalf("unexpected free-space bounds: %#v", header)
	}
	if header.Checksum != ComputePageChecksum(page.Bytes()) {
		t.Fatalf("checksum mismatch")
	}
	if err := mgr.Unpin(page, false); err != nil {
		t.Fatalf("unpin new page: %v", err)
	}
	if err := mgr.Flush(page.ID()); err != nil {
		t.Fatalf("flush new page: %v", err)
	}
	if got := store.writeCount(page.ID()); got != 1 {
		t.Fatalf("expected one write, got %d", got)
	}
}

func TestFetchDirtyFlushesBeforeEviction(t *testing.T) {
	store := newMemoryStore(t, 8192, 3)
	seed, err := NewPageImage(1, PageTypeHeap, 8192)
	if err != nil {
		t.Fatalf("seed page: %v", err)
	}
	seed[pageHeaderSize] = 0x11
	header, err := DecodePageHeader(seed)
	if err != nil {
		t.Fatalf("decode seed header: %v", err)
	}
	header.Checksum = 0
	if err := EncodePageHeader(seed, header); err != nil {
		t.Fatalf("encode seed header: %v", err)
	}
	header.Checksum = ComputePageChecksum(seed)
	if err := EncodePageHeader(seed, header); err != nil {
		t.Fatalf("encode seed checksum: %v", err)
	}
	if err := store.WritePage(1, seed); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	mgr, err := NewManager(store, 2)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() {
		_ = mgr.Close()
	}()

	page1, err := mgr.Fetch(1)
	if err != nil {
		t.Fatalf("fetch page1: %v", err)
	}
	page1.Bytes()[pageHeaderSize+1] = 0x7f
	if err := mgr.Unpin(page1, true); err != nil {
		t.Fatalf("unpin page1: %v", err)
	}

	page2, err := mgr.Fetch(2)
	if err != nil {
		t.Fatalf("fetch page2: %v", err)
	}
	if err := mgr.Unpin(page2, false); err != nil {
		t.Fatalf("unpin page2: %v", err)
	}

	page3, err := mgr.NewPage(PageTypeHeap)
	if err != nil {
		t.Fatalf("new page3: %v", err)
	}
	if page3.ID() != 3 {
		t.Fatalf("expected page 3, got %d", page3.ID())
	}
	if err := mgr.Unpin(page3, false); err != nil {
		t.Fatalf("unpin page3: %v", err)
	}

	if got := store.writeCount(1); got == 0 {
		t.Fatalf("expected dirty page 1 to be flushed on eviction")
	}
	persisted, ok := store.pageBytes(1)
	if !ok {
		t.Fatalf("page 1 missing from store")
	}
	if persisted[pageHeaderSize+1] != 0x7f {
		t.Fatalf("dirty byte was not persisted")
	}
}

func TestFetchFailsWhenAllPagesPinned(t *testing.T) {
	store := newMemoryStore(t, 8192, 1)
	mgr, err := NewManager(store, 1)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() {
		_ = mgr.Close()
	}()

	page, err := mgr.NewPage(PageTypeHeap)
	if err != nil {
		t.Fatalf("new page: %v", err)
	}
	if _, err := mgr.NewPage(PageTypeHeap); err != ErrCacheFull {
		t.Fatalf("expected cache full error, got %v", err)
	}
	if err := mgr.Unpin(page, false); err != nil {
		t.Fatalf("unpin page: %v", err)
	}
}

func TestValidatePageImageRejectsCorruption(t *testing.T) {
	page, err := NewPageImage(7, PageTypeHeap, 8192)
	if err != nil {
		t.Fatalf("new page: %v", err)
	}
	page[0] ^= 0x01
	if _, err := ValidatePageImage(page, 8192, 7); err == nil {
		t.Fatal("expected validation error for corrupted page")
	}
}

func TestPageImageEncodingIsStable(t *testing.T) {
	page, err := NewPageImage(9, PageTypeMetadata, 8192)
	if err != nil {
		t.Fatalf("new page: %v", err)
	}
	header, err := DecodePageHeader(page)
	if err != nil {
		t.Fatalf("decode header: %v", err)
	}
	if header.PageType != PageTypeMetadata {
		t.Fatalf("unexpected page type: %d", header.PageType)
	}

	clone := append([]byte(nil), page...)
	if !bytes.Equal(page, clone) {
		t.Fatal("page image should be deterministic")
	}
}

func TestFlushSyncsWALBeforeWritingDirtyPage(t *testing.T) {
	store := newMemoryStore(t, 8192, 2)
	events := make([]string, 0, 2)
	store.writeHook = func(pageID PageID, _ []byte) error {
		events = append(events, fmt.Sprintf("write:%d", pageID))
		return nil
	}

	logger := &recordingWAL{
		onSync: func(lsn wal.LSN) {
			events = append(events, fmt.Sprintf("sync:%d", lsn))
		},
	}
	mgr, err := newManager(store, 2, logger)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() {
		_ = mgr.Close()
	}()

	page, err := mgr.Fetch(1)
	if err != nil {
		t.Fatalf("fetch page: %v", err)
	}
	page.Bytes()[pageHeaderSize+2] = 0x5a
	if err := stampPageLSN(page.Bytes(), wal.LSN(128)); err != nil {
		t.Fatalf("stamp page lsn: %v", err)
	}
	if err := mgr.Unpin(page, true); err != nil {
		t.Fatalf("unpin page: %v", err)
	}
	if err := mgr.Flush(1); err != nil {
		t.Fatalf("flush page: %v", err)
	}

	if len(events) != 2 || events[0] != "sync:128" || events[1] != "write:1" {
		t.Fatalf("event order = %#v, want sync before write", events)
	}
	if len(logger.synced) != 1 || logger.synced[0] != wal.LSN(128) {
		t.Fatalf("synced lsns = %#v, want [128]", logger.synced)
	}

	persisted, ok := store.pageBytes(1)
	if !ok {
		t.Fatal("persisted page missing")
	}
	header, err := ValidatePageImage(persisted, 8192, 1)
	if err != nil {
		t.Fatalf("validate persisted page: %v", err)
	}
	if header.PageLSN != 128 {
		t.Fatalf("persisted page lsn = %d, want 128", header.PageLSN)
	}
}

func TestFlushRetainsDirtyPageWhenWALSyncFails(t *testing.T) {
	store := newMemoryStore(t, 8192, 2)
	logger := &recordingWAL{syncErr: errors.New("sync failed")}

	mgr, err := newManager(store, 2, logger)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	defer func() {
		_ = mgr.Close()
	}()

	page, err := mgr.Fetch(1)
	if err != nil {
		t.Fatalf("fetch page: %v", err)
	}
	page.Bytes()[pageHeaderSize+3] = 0x6b
	if err := stampPageLSN(page.Bytes(), wal.LSN(256)); err != nil {
		t.Fatalf("stamp page lsn: %v", err)
	}
	if err := mgr.Unpin(page, true); err != nil {
		t.Fatalf("unpin page: %v", err)
	}

	if err := mgr.Flush(1); err == nil {
		t.Fatal("expected wal sync failure")
	}
	if got := store.writeCount(1); got != 0 {
		t.Fatalf("write count = %d, want 0", got)
	}
	stats := mgr.Stats()
	if stats.Dirty != 1 {
		t.Fatalf("dirty frame count = %d, want 1", stats.Dirty)
	}
}

type recordingWAL struct {
	synced  []wal.LSN
	syncErr error
	onSync  func(wal.LSN)
}

func (w *recordingWAL) Sync(lsn wal.LSN) error {
	if w.onSync != nil {
		w.onSync(lsn)
	}
	w.synced = append(w.synced, lsn)
	return w.syncErr
}
