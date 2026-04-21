package paged

import (
	"path/filepath"

	"github.com/jamesdrando/tucotuco/internal/wal"
)

const defaultWALFileName = "tucotuco.wal"

func walFilePath(root string) string {
	return filepath.Join(root, defaultWALFileName)
}

func finalizedWALPayload(page []byte, lsn wal.LSN) ([]byte, error) {
	payload := append([]byte(nil), page...)
	header, err := DecodePageHeader(payload)
	if err != nil {
		return nil, err
	}
	header.PageLSN = uint64(lsn)
	header.Flags &^= PageFlagDirtyHint
	header.Checksum = 0
	if err := EncodePageHeader(payload, header); err != nil {
		return nil, err
	}
	header.Checksum = ComputePageChecksum(payload)
	if err := EncodePageHeader(payload, header); err != nil {
		return nil, err
	}
	return payload, nil
}

func stampPageLSN(page []byte, lsn wal.LSN) error {
	header, err := DecodePageHeader(page)
	if err != nil {
		return err
	}
	header.PageLSN = uint64(lsn)
	header.Checksum = 0
	return EncodePageHeader(page, header)
}

func (r *Relation) appendPageWAL(page *Page) (wal.LSN, error) {
	if page == nil || r == nil || r.wal == nil || r.desc == nil {
		return 0, ErrInvalidRelation
	}

	lsn, err := r.wal.AppendWith(func(lsn wal.LSN) (wal.Record, error) {
		payload, err := finalizedWALPayload(page.Bytes(), lsn)
		if err != nil {
			return wal.Record{}, err
		}
		return wal.Record{
			Type:     wal.RecordTypePageImage,
			Resource: relationFileName(r.desc.ID),
			PageID:   uint64(page.ID()),
			Payload:  payload,
		}, nil
	})
	if err != nil {
		return 0, err
	}
	if err := stampPageLSN(page.Bytes(), lsn); err != nil {
		return 0, err
	}
	return lsn, nil
}

func (r *Relation) mutateAndLogPage(page *Page, mutate func() error) error {
	if page == nil {
		return ErrInvalidRelation
	}

	before := append([]byte(nil), page.Bytes()...)
	if err := mutate(); err != nil {
		copy(page.Bytes(), before)
		return err
	}
	if _, err := r.appendPageWAL(page); err != nil {
		copy(page.Bytes(), before)
		return err
	}
	return nil
}

func (r *Relation) mutateAndLogHeapPage(page *pinnedHeapPage, mutate func(heap *heapPage) error) error {
	if page == nil || page.page == nil || page.heap == nil {
		return ErrInvalidRelation
	}

	before := append([]byte(nil), page.page.Bytes()...)
	if err := mutate(page.heap); err != nil {
		copy(page.page.Bytes(), before)
		_ = page.heap.syncHeader()
		return err
	}
	if _, err := r.appendPageWAL(page.page); err != nil {
		copy(page.page.Bytes(), before)
		_ = page.heap.syncHeader()
		return err
	}
	page.dirty = true
	return page.heap.syncHeader()
}
