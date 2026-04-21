package paged

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/jamesdrando/tucotuco/internal/wal"
)

func recoverPagedRelations(root string, pageSize int, log *wal.Log) error {
	if root == "" || pageSize < pageHeaderSize || log == nil {
		return nil
	}

	records, err := log.Records()
	if err != nil {
		return err
	}

	files := make(map[string]*recoveryFile)
	defer closeRecoveryFiles(files)

	for _, record := range records {
		if err := replayRecoveryRecord(root, pageSize, record, files); err != nil {
			return err
		}
	}

	return syncRecoveryFiles(files)
}

func replayRecoveryRecord(
	root string,
	pageSize int,
	record wal.PersistedRecord,
	files map[string]*recoveryFile,
) error {
	if record.Type != wal.RecordTypePageImage {
		return nil
	}
	if len(record.Payload) != pageSize {
		return fmt.Errorf(
			"paged: WAL page image for %q page %d has size %d, want %d",
			record.Resource,
			record.PageID,
			len(record.Payload),
			pageSize,
		)
	}

	pageID := PageID(record.PageID)
	if _, err := ValidatePageImage(record.Payload, pageSize, pageID); err != nil {
		return fmt.Errorf("paged: WAL page image for %q page %d is invalid: %w", record.Resource, record.PageID, err)
	}

	file, err := openRecoveryFile(root, record.Resource, pageSize, files)
	if err != nil {
		return err
	}
	if file == nil {
		return nil
	}

	redo, err := file.needsRedo(pageID, record.LSN)
	if err != nil {
		return err
	}
	if !redo {
		return nil
	}

	if err := file.writePage(pageID, record.Payload); err != nil {
		return err
	}
	return nil
}

type recoveryFile struct {
	path     string
	file     *os.File
	pageSize int
	dirty    bool
}

func openRecoveryFile(
	root string,
	resource string,
	pageSize int,
	files map[string]*recoveryFile,
) (*recoveryFile, error) {
	if file := files[resource]; file != nil {
		return file, nil
	}

	path, ok, err := recoveryResourcePath(root, resource)
	if err != nil || !ok {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	recovery := &recoveryFile{
		path:     path,
		file:     file,
		pageSize: pageSize,
	}
	files[resource] = recovery
	return recovery, nil
}

func recoveryResourcePath(root, resource string) (string, bool, error) {
	if resource == "" || filepath.Base(resource) != resource {
		return "", false, fmt.Errorf("paged: invalid WAL resource name %q", resource)
	}

	path := filepath.Join(root, resource)
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", false, nil
		}
		return "", false, err
	}
	if info.IsDir() {
		return "", false, fmt.Errorf("paged: WAL resource %q resolves to a directory", resource)
	}
	return path, true, nil
}

func (f *recoveryFile) needsRedo(pageID PageID, targetLSN wal.LSN) (bool, error) {
	if f == nil || f.file == nil {
		return false, nil
	}

	page := make([]byte, f.pageSize)
	offset := int64(pageID) * int64(f.pageSize)
	n, err := f.file.ReadAt(page, offset)
	switch {
	case err == nil:
	case errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF):
		if n == 0 {
			return true, nil
		}
		return true, nil
	default:
		return false, err
	}
	if n != len(page) {
		return true, nil
	}

	header, err := ValidatePageImage(page, f.pageSize, pageID)
	if err != nil {
		return true, nil
	}
	if wal.LSN(header.PageLSN) >= targetLSN {
		return false, nil
	}
	return true, nil
}

func (f *recoveryFile) writePage(pageID PageID, payload []byte) error {
	if f == nil || f.file == nil {
		return nil
	}

	offset := int64(pageID) * int64(f.pageSize)
	if _, err := f.file.WriteAt(payload, offset); err != nil {
		return fmt.Errorf("paged: redo write %s page %d: %w", f.path, pageID, err)
	}
	f.dirty = true
	return nil
}

func syncRecoveryFiles(files map[string]*recoveryFile) error {
	var firstErr error
	for _, file := range files {
		if file == nil || file.file == nil || !file.dirty {
			continue
		}
		if err := file.file.Sync(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("paged: sync recovered relation %s: %w", file.path, err)
		}
	}
	return firstErr
}

func closeRecoveryFiles(files map[string]*recoveryFile) {
	for _, file := range files {
		if file == nil || file.file == nil {
			continue
		}
		_ = file.file.Close()
	}
}
