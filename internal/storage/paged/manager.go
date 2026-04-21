package paged

import (
	"container/list"
	"errors"
	"fmt"
	"sync"

	"github.com/jamesdrando/tucotuco/internal/wal"
)

var (
	// ErrClosed reports that the buffer pool has been closed.
	ErrClosed = errors.New("paged: buffer pool closed")
	// ErrCacheFull reports that no unpinned frame was available for eviction.
	ErrCacheFull = errors.New("paged: buffer pool is full")
	// ErrPagePinned reports that a page is still pinned when a caller expected it to be unpinned.
	ErrPagePinned = errors.New("paged: page is pinned")
	// ErrPageNotFound reports that a requested page does not exist in the store.
	ErrPageNotFound = errors.New("paged: page not found")
	// ErrStalePage reports that a caller tried to release a page that has already been evicted.
	ErrStalePage = errors.New("paged: stale page handle")
	// ErrInvalidConfig reports a bad manager configuration.
	ErrInvalidConfig = errors.New("paged: invalid configuration")
)

// ManagerStats exposes a small amount of internal state for tests.
type ManagerStats struct {
	Cached    int
	Pinned    int
	Dirty     int
	Evictable int
}

type frame struct {
	page     *Page
	pins     int
	dirty    bool
	elem     *list.Element
	pageType PageType
}

type walSyncer interface {
	Sync(lsn wal.LSN) error
}

// Manager coordinates page caching, pinning, dirty tracking, and eviction.
type Manager struct {
	mu        sync.Mutex
	store     PageStore
	wal       walSyncer
	pageSize  int
	cacheSize int
	frames    map[PageID]*frame
	lru       *list.List
	closed    bool
}

// NewManager constructs a buffer pool manager on top of a page store.
func NewManager(store PageStore, cacheSize int) (*Manager, error) {
	return newManager(store, cacheSize, nil)
}

func newManager(store PageStore, cacheSize int, wal walSyncer) (*Manager, error) {
	if store == nil {
		return nil, ErrInvalidConfig
	}
	if cacheSize <= 0 {
		return nil, ErrInvalidConfig
	}
	if store.PageSize() < pageHeaderSize {
		return nil, fmt.Errorf("paged: store page size %d smaller than header size %d", store.PageSize(), pageHeaderSize)
	}
	if _, err := store.PageCount(); err != nil {
		return nil, err
	}

	return &Manager{
		store:     store,
		wal:       wal,
		pageSize:  store.PageSize(),
		cacheSize: cacheSize,
		frames:    make(map[PageID]*frame, cacheSize),
		lru:       list.New(),
	}, nil
}

// Fetch pins an existing page and returns it from cache or the backing store.
func (m *Manager) Fetch(pageID PageID) (*Page, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.ensureOpen(); err != nil {
		return nil, err
	}

	if fr, ok := m.frames[pageID]; ok {
		fr.pins++
		m.removeFromLRU(fr)
		return fr.page, nil
	}

	if err := m.makeRoomLocked(); err != nil {
		return nil, err
	}

	pageBytes := make([]byte, m.pageSize)
	if err := m.store.ReadPage(pageID, pageBytes); err != nil {
		return nil, ErrPageNotFound
	}
	if _, err := ValidatePageImage(pageBytes, m.pageSize, pageID); err != nil {
		return nil, err
	}

	token := &pageToken{}
	page := &Page{id: pageID, data: pageBytes, token: token}
	fr := &frame{page: page, pins: 1, dirty: false, pageType: pageTypeFromImage(pageBytes)}
	m.frames[pageID] = fr
	return page, nil
}

// NewPage allocates a new page in the store, pins it, and returns the mutable page image.
func (m *Manager) NewPage(pageType PageType) (*Page, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.ensureOpen(); err != nil {
		return nil, err
	}
	if pageType != PageTypeMetadata && pageType != PageTypeHeap {
		return nil, fmt.Errorf("paged: unsupported page type %d", pageType)
	}
	if err := m.makeRoomLocked(); err != nil {
		return nil, err
	}

	pageID, err := m.store.AllocatePage()
	if err != nil {
		return nil, err
	}

	pageBytes, err := NewPageImage(pageID, pageType, m.pageSize)
	if err != nil {
		return nil, err
	}

	token := &pageToken{}
	page := &Page{id: pageID, data: pageBytes, token: token}
	m.frames[pageID] = &frame{
		page:     page,
		pins:     1,
		dirty:    true,
		pageType: pageType,
	}
	return page, nil
}

// Unpin releases a pin on page and optionally marks it dirty.
func (m *Manager) Unpin(page *Page, dirty bool) error {
	if page == nil {
		return ErrInvalidConfig
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.ensureOpen(); err != nil {
		return err
	}

	fr, ok := m.frames[page.ID()]
	if !ok || fr.page.token != page.token {
		return ErrStalePage
	}
	if fr.pins == 0 {
		return ErrPagePinned
	}
	if dirty {
		fr.dirty = true
		if header, err := DecodePageHeader(fr.page.data); err == nil {
			header.Flags |= PageFlagDirtyHint
			_ = EncodePageHeader(fr.page.data, header)
		}
	}

	fr.pins--
	if fr.pins == 0 {
		m.pushFrontLRU(fr)
	}
	return nil
}

// Flush writes a single cached page to the backing store if it is dirty.
func (m *Manager) Flush(pageID PageID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.ensureOpen(); err != nil {
		return err
	}

	fr, ok := m.frames[pageID]
	if !ok {
		return ErrPageNotFound
	}
	if fr.page == nil {
		return ErrPageNotFound
	}
	if !fr.dirty {
		return nil
	}

	if err := m.flushFrameLocked(fr); err != nil {
		return err
	}
	return nil
}

// FlushAll writes every dirty cached page back to the store.
func (m *Manager) FlushAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.ensureOpen(); err != nil {
		return err
	}

	firstErr := m.flushDirtyFramesLocked()
	if err := m.store.Sync(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// Close flushes the cache and closes the underlying store.
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}

	firstErr := m.flushDirtyFramesLocked()
	if err := m.store.Sync(); err != nil && firstErr == nil {
		firstErr = err
	}
	m.closed = true
	m.mu.Unlock()

	if err := m.store.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// Stats reports a small snapshot of cache state for tests.
func (m *Manager) Stats() ManagerStats {
	m.mu.Lock()
	defer m.mu.Unlock()

	stats := ManagerStats{}
	for _, fr := range m.frames {
		stats.Cached++
		stats.Pinned += fr.pins
		if fr.dirty {
			stats.Dirty++
		}
		if fr.pins == 0 {
			stats.Evictable++
		}
	}
	return stats
}

func (m *Manager) ensureOpen() error {
	if m.closed {
		return ErrClosed
	}
	return nil
}

func (m *Manager) makeRoomLocked() error {
	if len(m.frames) < m.cacheSize {
		return nil
	}

	victimElem := m.lru.Back()
	if victimElem == nil {
		return ErrCacheFull
	}

	fr := victimElem.Value.(*frame)
	if fr.pins != 0 {
		return ErrCacheFull
	}
	if err := m.flushFrameLocked(fr); err != nil {
		return err
	}

	delete(m.frames, fr.page.id)
	m.lru.Remove(victimElem)
	fr.elem = nil
	return nil
}

func (m *Manager) flushFrameLocked(fr *frame) error {
	if fr == nil || fr.page == nil || !fr.dirty {
		return nil
	}

	pageBytes := fr.page.data
	header, err := DecodePageHeader(pageBytes)
	if err != nil {
		return err
	}
	if m.wal != nil {
		if header.PageLSN == 0 {
			return errors.New("paged: dirty page missing wal lsn")
		}
		if err := m.wal.Sync(wal.LSN(header.PageLSN)); err != nil {
			return err
		}
	}
	header.Flags &^= PageFlagDirtyHint
	header.Checksum = 0
	if err := EncodePageHeader(pageBytes, header); err != nil {
		return err
	}
	header.Checksum = ComputePageChecksum(pageBytes)
	if err := EncodePageHeader(pageBytes, header); err != nil {
		return err
	}
	if err := m.store.WritePage(fr.page.id, pageBytes); err != nil {
		return err
	}

	fr.dirty = false
	return nil
}

func (m *Manager) flushDirtyFramesLocked() error {
	var firstErr error
	for _, fr := range m.frames {
		if fr.dirty {
			if err := m.flushFrameLocked(fr); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (m *Manager) pushFrontLRU(fr *frame) {
	if fr == nil || fr.page == nil {
		return
	}
	if fr.elem != nil {
		m.lru.Remove(fr.elem)
	}
	fr.elem = m.lru.PushFront(fr)
}

func (m *Manager) removeFromLRU(fr *frame) {
	if fr == nil || fr.elem == nil {
		return
	}
	m.lru.Remove(fr.elem)
	fr.elem = nil
}

func pageTypeFromImage(page []byte) PageType {
	if len(page) < pageHeaderSize {
		return PageTypeHeap
	}
	return PageType(page[0x06])
}
