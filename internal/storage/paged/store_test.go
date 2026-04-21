package paged

import (
	"errors"
	"sync"
	"testing"
)

type memoryStore struct {
	mu        sync.Mutex
	pageSize  int
	pages     map[PageID][]byte
	writes    map[PageID]int
	writeHook func(PageID, []byte) error
	closed    bool
}

func newMemoryStore(t *testing.T, pageSize int, pageCount int) *memoryStore {
	t.Helper()

	store := &memoryStore{
		pageSize: pageSize,
		pages:    make(map[PageID][]byte, pageCount),
		writes:   make(map[PageID]int),
	}
	for i := 0; i < pageCount; i++ {
		pageID := PageID(i)
		page, err := NewPageImage(pageID, PageTypeMetadata, pageSize)
		if i > 0 {
			if err != nil {
				t.Fatalf("init page %d: %v", i, err)
			}
			if err := InitPageImage(page, pageID, PageTypeHeap, pageSize); err != nil {
				t.Fatalf("init page %d: %v", i, err)
			}
		}
		if err != nil {
			t.Fatalf("init page %d: %v", i, err)
		}
		store.pages[pageID] = page
	}
	return store
}

func (s *memoryStore) PageSize() int {
	return s.pageSize
}

func (s *memoryStore) PageCount() (PageID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return PageID(len(s.pages)), nil
}

func (s *memoryStore) ReadPage(pageID PageID, dst []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	page, ok := s.pages[pageID]
	if !ok {
		return ErrPageNotFound
	}
	copy(dst, page)
	return nil
}

func (s *memoryStore) WritePage(pageID PageID, src []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.writeHook != nil {
		if err := s.writeHook(pageID, src); err != nil {
			return err
		}
	}
	page := make([]byte, len(src))
	copy(page, src)
	s.pages[pageID] = page
	s.writes[pageID]++
	return nil
}

func (s *memoryStore) AllocatePage() (PageID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pageID := PageID(len(s.pages))
	s.pages[pageID] = make([]byte, s.pageSize)
	return pageID, nil
}

func (s *memoryStore) Sync() error {
	return nil
}

func (s *memoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	return nil
}

func (s *memoryStore) pageBytes(pageID PageID) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	page, ok := s.pages[pageID]
	return append([]byte(nil), page...), ok
}

func (s *memoryStore) writeCount(pageID PageID) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.writes[pageID]
}

func (s *memoryStore) corruptPage(pageID PageID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if page, ok := s.pages[pageID]; ok {
		page[0] ^= 0xff
		s.pages[pageID] = page
	}
}

func (s *memoryStore) removePage(pageID PageID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.pages, pageID)
}

func (s *memoryStore) closedErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errors.New("closed")
	}
	return nil
}
