package paged

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	pageMagic         uint32 = 0x4f435554 // "TUCO" in little-endian byte order.
	pageFormatVersion uint16 = 1

	pageHeaderSize = 64
	pageSlotSize   = 8
)

// PageID identifies a relation-relative page number.
type PageID uint64

// PageType identifies the logical page family stored in the page header.
type PageType uint8

const (
	// PageTypeMetadata is the reserved relation metadata page at page zero.
	PageTypeMetadata PageType = 0
	// PageTypeHeap is a slotted heap data page.
	PageTypeHeap PageType = 1
)

// PageFlags are persisted header hints reserved for later storage features.
type PageFlags uint8

const (
	// PageFlagDirtyHint marks pages modified since the last clean flush.
	PageFlagDirtyHint PageFlags = 1 << iota
	// PageFlagHasRedirects marks pages that currently contain redirect slots.
	PageFlagHasRedirects
	// PageFlagHasDeadTuples marks pages with dead tuples awaiting reclamation.
	PageFlagHasDeadTuples
	// PageFlagAllVisible is reserved for later MVCC visibility-map wiring.
	PageFlagAllVisible
	// PageFlagReserved must remain zero in Phase 2.
	PageFlagReserved
)

// PageHeader mirrors the 64-byte on-disk page header defined in docs/storage.md.
type PageHeader struct {
	Magic         uint32
	FormatVersion uint16
	PageType      PageType
	Flags         PageFlags
	PageID        PageID
	PageLSN       uint64
	Checksum      uint32
	Lower         uint16
	Upper         uint16
	Special       uint16
	SlotCount     uint16
	DeadBytes     uint16
	Reserved0     uint16
	Reserved1     uint64
	Reserved2     [16]byte
}

// Validate reports whether the header fields satisfy the page-layout contract
// for the supplied page size.
func (h PageHeader) Validate(pageSize int) error {
	if pageSize < pageHeaderSize {
		return fmt.Errorf("paged: page size %d smaller than header size %d", pageSize, pageHeaderSize)
	}
	if h.Magic != pageMagic {
		return fmt.Errorf("paged: invalid page magic 0x%x", h.Magic)
	}
	if h.FormatVersion != pageFormatVersion {
		return fmt.Errorf("paged: unsupported page format version %d", h.FormatVersion)
	}
	if h.Flags&PageFlagReserved != 0 {
		return errors.New("paged: reserved page flag bit must be zero")
	}
	if h.Lower < pageHeaderSize {
		return fmt.Errorf("paged: lower pointer %d below header size %d", h.Lower, pageHeaderSize)
	}
	if h.Lower > h.Upper {
		return fmt.Errorf("paged: lower pointer %d exceeds upper pointer %d", h.Lower, h.Upper)
	}
	if h.Upper > h.Special {
		return fmt.Errorf("paged: upper pointer %d exceeds special pointer %d", h.Upper, h.Special)
	}
	if int(h.Special) > pageSize {
		return fmt.Errorf("paged: special pointer %d exceeds page size %d", h.Special, pageSize)
	}
	if h.Lower != uint16(pageHeaderSize+int(h.SlotCount)*pageSlotSize) {
		return fmt.Errorf("paged: slot directory length %d does not match slot count %d", h.Lower, h.SlotCount)
	}
	return nil
}

// DecodePageHeader parses the 64-byte header stored at the front of a page.
func DecodePageHeader(page []byte) (PageHeader, error) {
	if len(page) < pageHeaderSize {
		return PageHeader{}, fmt.Errorf("paged: page image too small: %d", len(page))
	}

	return PageHeader{
		Magic:         binary.LittleEndian.Uint32(page[0x00:0x04]),
		FormatVersion: binary.LittleEndian.Uint16(page[0x04:0x06]),
		PageType:      PageType(page[0x06]),
		Flags:         PageFlags(page[0x07]),
		PageID:        PageID(binary.LittleEndian.Uint64(page[0x08:0x10])),
		PageLSN:       binary.LittleEndian.Uint64(page[0x10:0x18]),
		Checksum:      binary.LittleEndian.Uint32(page[0x18:0x1c]),
		Lower:         binary.LittleEndian.Uint16(page[0x1c:0x1e]),
		Upper:         binary.LittleEndian.Uint16(page[0x1e:0x20]),
		Special:       binary.LittleEndian.Uint16(page[0x20:0x22]),
		SlotCount:     binary.LittleEndian.Uint16(page[0x22:0x24]),
		DeadBytes:     binary.LittleEndian.Uint16(page[0x24:0x26]),
		Reserved0:     binary.LittleEndian.Uint16(page[0x26:0x28]),
		Reserved1:     binary.LittleEndian.Uint64(page[0x28:0x30]),
	}, nil
}

// EncodePageHeader writes the header into dst using little-endian byte order.
func EncodePageHeader(dst []byte, header PageHeader) error {
	if len(dst) < pageHeaderSize {
		return fmt.Errorf("paged: destination too small: %d", len(dst))
	}

	binary.LittleEndian.PutUint32(dst[0x00:0x04], header.Magic)
	binary.LittleEndian.PutUint16(dst[0x04:0x06], header.FormatVersion)
	dst[0x06] = byte(header.PageType)
	dst[0x07] = byte(header.Flags)
	binary.LittleEndian.PutUint64(dst[0x08:0x10], uint64(header.PageID))
	binary.LittleEndian.PutUint64(dst[0x10:0x18], header.PageLSN)
	binary.LittleEndian.PutUint32(dst[0x18:0x1c], header.Checksum)
	binary.LittleEndian.PutUint16(dst[0x1c:0x1e], header.Lower)
	binary.LittleEndian.PutUint16(dst[0x1e:0x20], header.Upper)
	binary.LittleEndian.PutUint16(dst[0x20:0x22], header.Special)
	binary.LittleEndian.PutUint16(dst[0x22:0x24], header.SlotCount)
	binary.LittleEndian.PutUint16(dst[0x24:0x26], header.DeadBytes)
	binary.LittleEndian.PutUint16(dst[0x26:0x28], header.Reserved0)
	binary.LittleEndian.PutUint64(dst[0x28:0x30], header.Reserved1)
	copy(dst[0x30:0x40], header.Reserved2[:])
	return nil
}

// ComputePageChecksum calculates the checksum used by the page header.
func ComputePageChecksum(page []byte) uint32 {
	h := crc32.NewIEEE()
	_, _ = h.Write(page[:0x18])
	_, _ = h.Write([]byte{0, 0, 0, 0})
	_, _ = h.Write(page[0x1c:])
	return h.Sum32()
}

// ValidatePageImage verifies the checksum and header invariants for page.
func ValidatePageImage(page []byte, pageSize int, expectedID PageID) (PageHeader, error) {
	if len(page) != pageSize {
		return PageHeader{}, fmt.Errorf("paged: page image size %d does not match page size %d", len(page), pageSize)
	}

	header, err := DecodePageHeader(page)
	if err != nil {
		return PageHeader{}, err
	}
	if err := header.Validate(pageSize); err != nil {
		return PageHeader{}, err
	}
	if header.PageID != expectedID {
		return PageHeader{}, fmt.Errorf("paged: page id mismatch: header=%d expected=%d", header.PageID, expectedID)
	}
	if ComputePageChecksum(page) != header.Checksum {
		return PageHeader{}, errors.New("paged: checksum mismatch")
	}
	return header, nil
}

// InitPageImage initializes dst as a zeroed page image with a valid header.
func InitPageImage(dst []byte, pageID PageID, pageType PageType, pageSize int) error {
	if len(dst) != pageSize {
		return fmt.Errorf("paged: page image size %d does not match page size %d", len(dst), pageSize)
	}
	if pageSize < pageHeaderSize {
		return fmt.Errorf("paged: page size %d smaller than header size %d", pageSize, pageHeaderSize)
	}

	for i := range dst {
		dst[i] = 0
	}

	header := PageHeader{
		Magic:         pageMagic,
		FormatVersion: pageFormatVersion,
		PageType:      pageType,
		Flags:         0,
		PageID:        pageID,
		PageLSN:       0,
		Checksum:      0,
		Lower:         pageHeaderSize,
		Upper:         uint16(pageSize),
		Special:       uint16(pageSize),
		SlotCount:     0,
		DeadBytes:     0,
		Reserved0:     0,
		Reserved1:     0,
	}

	if err := EncodePageHeader(dst, header); err != nil {
		return err
	}
	header.Checksum = ComputePageChecksum(dst)
	return EncodePageHeader(dst, header)
}

// NewPageImage allocates and initializes a page image for the given page id.
func NewPageImage(pageID PageID, pageType PageType, pageSize int) ([]byte, error) {
	page := make([]byte, pageSize)
	if err := InitPageImage(page, pageID, pageType, pageSize); err != nil {
		return nil, err
	}
	return page, nil
}

// Page is a pinned mutable page image returned by the buffer pool.
type Page struct {
	id    PageID
	data  []byte
	token *pageToken
}

// ID returns the relation-relative page number.
func (p *Page) ID() PageID {
	if p == nil {
		return 0
	}
	return p.id
}

// Bytes returns the mutable page image.
func (p *Page) Bytes() []byte {
	if p == nil {
		return nil
	}
	return p.data
}

// Header decodes the page header from the pinned page image.
func (p *Page) Header() (PageHeader, error) {
	if p == nil {
		return PageHeader{}, errors.New("paged: nil page")
	}
	return DecodePageHeader(p.data)
}
