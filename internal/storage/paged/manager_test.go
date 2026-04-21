package paged

import (
	"bytes"
	"testing"
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
