package paged

import (
	"fmt"
	"io"
	"os"
	"sync"
)

// PageStore is the persistence contract used by the buffer pool.
type PageStore interface {
	PageSize() int
	PageCount() (PageID, error)
	ReadPage(pageID PageID, dst []byte) error
	WritePage(pageID PageID, src []byte) error
	AllocatePage() (PageID, error)
	Sync() error
	Close() error
}

// FileStore persists relation pages in a single file.
type FileStore struct {
	mu       sync.Mutex
	file     *os.File
	pageSize int
}

// OpenFileStore opens or creates a page file.
func OpenFileStore(path string, pageSize int) (*FileStore, error) {
	if pageSize < pageHeaderSize {
		return nil, fmt.Errorf("paged: page size %d smaller than header size %d", pageSize, pageHeaderSize)
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}

	store := &FileStore{file: file, pageSize: pageSize}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	if info.Size() == 0 {
		if err := file.Truncate(int64(pageSize)); err != nil {
			_ = file.Close()
			return nil, err
		}
		page, err := NewPageImage(0, PageTypeMetadata, pageSize)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		if _, err := file.WriteAt(page, 0); err != nil {
			_ = file.Close()
			return nil, err
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return nil, err
		}
	}

	return store, nil
}

// PageSize returns the on-disk page size.
func (s *FileStore) PageSize() int {
	return s.pageSize
}

// PageCount reports how many pages currently exist in the file.
func (s *FileStore) PageCount() (PageID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	info, err := s.file.Stat()
	if err != nil {
		return 0, err
	}
	return PageID(info.Size() / int64(s.pageSize)), nil
}

// ReadPage copies a stored page into dst.
func (s *FileStore) ReadPage(pageID PageID, dst []byte) error {
	if len(dst) != s.pageSize {
		return fmt.Errorf("paged: destination size %d does not match page size %d", len(dst), s.pageSize)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.file.ReadAt(dst, int64(pageID)*int64(s.pageSize)); err != nil {
		if err == io.EOF {
			return fmt.Errorf("paged: page %d not found", pageID)
		}
		return err
	}
	return nil
}

// WritePage persists a full page image.
func (s *FileStore) WritePage(pageID PageID, src []byte) error {
	if len(src) != s.pageSize {
		return fmt.Errorf("paged: source size %d does not match page size %d", len(src), s.pageSize)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.file.WriteAt(src, int64(pageID)*int64(s.pageSize))
	return err
}

// AllocatePage extends the file by one zeroed page and returns its id.
func (s *FileStore) AllocatePage() (PageID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	info, err := s.file.Stat()
	if err != nil {
		return 0, err
	}

	pageID := PageID(info.Size() / int64(s.pageSize))
	if err := s.file.Truncate(int64(pageID+1) * int64(s.pageSize)); err != nil {
		return 0, err
	}
	return pageID, nil
}

// Sync flushes buffered file contents to stable storage.
func (s *FileStore) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.file.Sync()
}

// Close closes the underlying file.
func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.file.Close()
}

// pageToken distinguishes a cached page instance from any later reload.
type pageToken struct{}
