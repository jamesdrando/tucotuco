package paged

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/jamesdrando/tucotuco/internal/storage"
)

const redirectHandleSize = 16

// Slot flags mirror the slot-state contract in docs/storage.md.
const (
	slotFlagLive uint16 = 1 << iota
	slotFlagDead
	slotFlagRedirect
	slotFlagUnused
)

type slotEntry struct {
	Offset     uint16
	Length     uint16
	Flags      uint16
	Generation uint16
}

type heapPage struct {
	page   *Page
	header PageHeader
}

type retainedPayload struct {
	slotIndex uint16
	slot      slotEntry
	payload   []byte
}

func newHeapPage(page *Page) (*heapPage, error) {
	if page == nil {
		return nil, ErrInvalidRelation
	}

	header, err := page.Header()
	if err != nil {
		return nil, err
	}
	if header.PageType != PageTypeHeap {
		return nil, fmt.Errorf("paged: page %d is type %d, want heap", page.ID(), header.PageType)
	}

	return &heapPage{page: page, header: header}, nil
}

func (p *heapPage) canFit(tupleLen int) bool {
	if p == nil || tupleLen < 0 {
		return false
	}
	return p.freeSpace() >= tupleLen+pageSlotSize
}

func (p *heapPage) freeSpace() int {
	if p == nil {
		return 0
	}
	return int(p.header.Upper) - int(p.header.Lower)
}

func (p *heapPage) insertTuple(tuple []byte) (uint16, error) {
	if p == nil {
		return 0, ErrInvalidRelation
	}
	if err := p.syncHeader(); err != nil {
		return 0, err
	}
	if !p.canFit(len(tuple)) {
		return 0, ErrRowTooLarge
	}
	if len(tuple) == 0 {
		return 0, errors.New("paged: tuple payload is empty")
	}

	slotIndex := p.header.SlotCount
	payloadEnd := int(p.header.Upper)
	payloadStart := payloadEnd - len(tuple)
	if payloadStart < int(p.header.Lower)+pageSlotSize {
		return 0, ErrRowTooLarge
	}

	copy(p.page.data[payloadStart:payloadEnd], tuple)
	slot := slotEntry{
		Offset:     uint16(payloadStart),
		Length:     uint16(len(tuple)),
		Flags:      slotFlagLive,
		Generation: 1,
	}
	p.writeSlot(slotIndex, slot)
	p.header.SlotCount++
	p.header.Lower += pageSlotSize
	p.header.Upper = uint16(payloadStart)
	if err := p.flushHeader(); err != nil {
		return 0, err
	}

	return slotIndex, nil
}

func (p *heapPage) readSlot(slotIndex uint64) (slotEntry, error) {
	if p == nil {
		return slotEntry{}, ErrInvalidRelation
	}
	if err := p.syncHeader(); err != nil {
		return slotEntry{}, err
	}
	if slotIndex >= uint64(p.header.SlotCount) {
		return slotEntry{}, ErrRowNotFound
	}

	offset := pageHeaderSize + int(slotIndex)*pageSlotSize
	raw := p.page.data[offset : offset+pageSlotSize]
	return slotEntry{
		Offset:     binary.LittleEndian.Uint16(raw[0:2]),
		Length:     binary.LittleEndian.Uint16(raw[2:4]),
		Flags:      binary.LittleEndian.Uint16(raw[4:6]),
		Generation: binary.LittleEndian.Uint16(raw[6:8]),
	}, nil
}

func (p *heapPage) writeSlot(slotIndex uint16, slot slotEntry) {
	offset := pageHeaderSize + int(slotIndex)*pageSlotSize
	raw := p.page.data[offset : offset+pageSlotSize]
	binary.LittleEndian.PutUint16(raw[0:2], slot.Offset)
	binary.LittleEndian.PutUint16(raw[2:4], slot.Length)
	binary.LittleEndian.PutUint16(raw[4:6], slot.Flags)
	binary.LittleEndian.PutUint16(raw[6:8], slot.Generation)
}

func (p *heapPage) tuple(slotIndex uint64) ([]byte, slotEntry, error) {
	slot, err := p.readSlot(slotIndex)
	if err != nil {
		return nil, slotEntry{}, err
	}

	switch slot.Flags {
	case slotFlagLive, slotFlagRedirect:
	case slotFlagDead, slotFlagUnused:
		return nil, slot, ErrRowNotFound
	default:
		return nil, slot, fmt.Errorf("paged: page %d slot %d has invalid flags 0x%x", p.page.ID(), slotIndex, slot.Flags)
	}

	end := int(slot.Offset) + int(slot.Length)
	if slot.Length == 0 || int(slot.Offset) < pageHeaderSize || end > len(p.page.data) {
		return nil, slot, fmt.Errorf("paged: page %d slot %d points outside the page", p.page.ID(), slotIndex)
	}

	return p.page.data[slot.Offset:end], slot, nil
}

func (p *heapPage) redirectHandle(slot slotEntry) (storage.RowHandle, error) {
	if slot.Flags != slotFlagRedirect {
		return storage.RowHandle{}, ErrRowNotFound
	}
	if slot.Length != redirectHandleSize {
		return storage.RowHandle{}, fmt.Errorf("paged: redirect slot length %d, want %d", slot.Length, redirectHandleSize)
	}

	start := int(slot.Offset)
	end := start + redirectHandleSize
	if start < pageHeaderSize || end > len(p.page.data) {
		return storage.RowHandle{}, fmt.Errorf("paged: redirect slot points outside the page")
	}

	payload := p.page.data[start:end]
	return storage.RowHandle{
		Page: binary.LittleEndian.Uint64(payload[0:8]),
		Slot: binary.LittleEndian.Uint64(payload[8:16]),
	}, nil
}

func (p *heapPage) rewriteTuple(slotIndex uint64, tuple []byte) error {
	if p == nil {
		return ErrInvalidRelation
	}
	if err := p.syncHeader(); err != nil {
		return err
	}
	if len(tuple) == 0 {
		return errors.New("paged: tuple payload is empty")
	}

	slot, err := p.readSlot(slotIndex)
	if err != nil {
		return err
	}
	switch slot.Flags {
	case slotFlagLive:
	case slotFlagDead, slotFlagUnused:
		return ErrRowNotFound
	case slotFlagRedirect:
		return errors.New("paged: cannot rewrite redirect slot in place")
	default:
		return fmt.Errorf("paged: page %d slot %d has invalid flags 0x%x", p.page.ID(), slotIndex, slot.Flags)
	}
	if len(tuple) > int(slot.Length) {
		return ErrRowTooLarge
	}

	start, end, err := p.payloadBounds(slot)
	if err != nil {
		return err
	}

	copy(p.page.data[start:start+len(tuple)], tuple)
	for index := start + len(tuple); index < end; index++ {
		p.page.data[index] = 0
	}
	if delta := int(slot.Length) - len(tuple); delta > 0 {
		p.noteDeadBytes(delta)
		slot.Length = uint16(len(tuple))
		p.writeSlot(uint16(slotIndex), slot)
	}

	return p.flushHeader()
}

func (p *heapPage) endTupleVersion(slotIndex uint64, version uint64, successor storage.RowHandle) error {
	if p == nil {
		return ErrInvalidRelation
	}
	if err := p.syncHeader(); err != nil {
		return err
	}

	slot, err := p.readSlot(slotIndex)
	if err != nil {
		return err
	}
	switch slot.Flags {
	case slotFlagLive:
	case slotFlagDead, slotFlagUnused:
		return ErrRowNotFound
	case slotFlagRedirect:
		return errors.New("paged: cannot end redirect slot version")
	default:
		return fmt.Errorf("paged: page %d slot %d has invalid flags 0x%x", p.page.ID(), slotIndex, slot.Flags)
	}

	start, end, err := p.payloadBounds(slot)
	if err != nil {
		return err
	}
	header, err := decodeTupleHeader(p.page.data[start:end])
	if err != nil {
		return err
	}
	if !header.visible() {
		return ErrRowNotFound
	}

	header.Flags = tupleFlagDeleted
	header.Xmax = version
	header.ForwardPtr = 0
	if successor.Valid() {
		packed, err := encodeForwardPtr(successor)
		if err != nil {
			return err
		}
		header.ForwardPtr = packed
	}

	return encodeTupleHeader(p.page.data[start:start+tupleHeaderSize], header)
}

func (p *heapPage) installRedirect(slotIndex uint64, target storage.RowHandle) error {
	if p == nil {
		return ErrInvalidRelation
	}
	if err := p.syncHeader(); err != nil {
		return err
	}
	if !target.Valid() || target.Page == 0 {
		return ErrRowNotFound
	}

	slot, err := p.readSlot(slotIndex)
	if err != nil {
		return err
	}
	switch slot.Flags {
	case slotFlagLive, slotFlagRedirect:
	case slotFlagDead, slotFlagUnused:
		return ErrRowNotFound
	default:
		return fmt.Errorf("paged: page %d slot %d has invalid flags 0x%x", p.page.ID(), slotIndex, slot.Flags)
	}
	if int(slot.Length) < redirectHandleSize {
		return fmt.Errorf("paged: slot %d payload length %d too small for redirect", slotIndex, slot.Length)
	}

	start, end, err := p.payloadBounds(slot)
	if err != nil {
		return err
	}

	writeRedirectPayload(p.page.data[start:start+redirectHandleSize], target)
	for index := start + redirectHandleSize; index < end; index++ {
		p.page.data[index] = 0
	}
	if delta := int(slot.Length) - redirectHandleSize; delta > 0 {
		p.noteDeadBytes(delta)
	}

	slot.Flags = slotFlagRedirect
	slot.Length = redirectHandleSize
	p.writeSlot(uint16(slotIndex), slot)
	p.header.Flags |= PageFlagHasRedirects
	return p.flushHeader()
}

func (p *heapPage) markTerminal(slotIndex uint64, version uint64) error {
	if p == nil || version == 0 {
		return ErrInvalidRelation
	}
	if err := p.syncHeader(); err != nil {
		return err
	}

	slot, err := p.readSlot(slotIndex)
	if err != nil {
		return err
	}
	switch slot.Flags {
	case slotFlagLive:
	case slotFlagDead, slotFlagUnused, slotFlagRedirect:
		return ErrRowNotFound
	default:
		return fmt.Errorf("paged: page %d slot %d has invalid flags 0x%x", p.page.ID(), slotIndex, slot.Flags)
	}

	start, end, err := p.payloadBounds(slot)
	if err != nil {
		return err
	}
	header, err := decodeTupleHeader(p.page.data[start:end])
	if err != nil {
		return err
	}
	if !header.visible() {
		return ErrRowNotFound
	}

	header.Flags = tupleFlagDeleted
	header.Xmax = version
	header.ForwardPtr = 0
	if err := encodeTupleHeader(p.page.data[start:start+tupleHeaderSize], header); err != nil {
		return err
	}

	slot.Flags = slotFlagDead
	p.writeSlot(uint16(slotIndex), slot)
	p.noteDeadBytes(int(slot.Length))
	return p.flushHeader()
}

func (p *heapPage) payloadBounds(slot slotEntry) (int, int, error) {
	start := int(slot.Offset)
	end := start + int(slot.Length)
	if slot.Length == 0 || start < pageHeaderSize || end > len(p.page.data) {
		return 0, 0, fmt.Errorf("paged: page %d slot payload points outside the page", p.page.ID())
	}
	return start, end, nil
}

func (p *heapPage) noteDeadBytes(delta int) {
	if p == nil || delta <= 0 {
		return
	}

	const maxUint16 = ^uint16(0)
	remaining := int(maxUint16 - p.header.DeadBytes)
	if delta > remaining {
		p.header.DeadBytes = maxUint16
	} else {
		p.header.DeadBytes += uint16(delta)
	}
	p.header.Flags |= PageFlagHasDeadTuples
}

func (p *heapPage) needsVacuum() (bool, error) {
	if p == nil {
		return false, ErrInvalidRelation
	}
	if err := p.syncHeader(); err != nil {
		return false, err
	}
	if p.header.DeadBytes > 0 {
		return true, nil
	}

	for slotIndex := uint64(0); slotIndex < uint64(p.header.SlotCount); slotIndex++ {
		slot, err := p.readSlot(slotIndex)
		if err != nil {
			return false, err
		}

		switch slot.Flags {
		case slotFlagLive:
			start, end, err := p.payloadBounds(slot)
			if err != nil {
				return false, err
			}
			header, err := decodeTupleHeader(p.page.data[start:end])
			if err != nil {
				return false, err
			}
			if !header.visible() {
				return true, nil
			}
		case slotFlagDead, slotFlagUnused:
			if slot.Offset != 0 || slot.Length != 0 {
				return true, nil
			}
		case slotFlagRedirect:
			continue
		default:
			return false, fmt.Errorf("paged: page %d slot %d has invalid flags 0x%x", p.page.ID(), slotIndex, slot.Flags)
		}
	}

	return false, nil
}

func (p *heapPage) vacuum() (bool, error) {
	if p == nil {
		return false, ErrInvalidRelation
	}
	if err := p.syncHeader(); err != nil {
		return false, err
	}

	survivors := make([]retainedPayload, 0, p.header.SlotCount)
	modified := p.header.DeadBytes > 0
	hasRedirects := false

	for slotIndex := uint16(0); slotIndex < p.header.SlotCount; slotIndex++ {
		slot, err := p.readSlot(uint64(slotIndex))
		if err != nil {
			return false, err
		}

		switch slot.Flags {
		case slotFlagLive:
			start, end, err := p.payloadBounds(slot)
			if err != nil {
				return false, err
			}
			header, err := decodeTupleHeader(p.page.data[start:end])
			if err != nil {
				return false, err
			}
			if !header.visible() {
				if slot.Offset != 0 || slot.Length != 0 || slot.Flags != slotFlagDead {
					slot.Offset = 0
					slot.Length = 0
					slot.Flags = slotFlagDead
					p.writeSlot(slotIndex, slot)
					modified = true
				}
				continue
			}

			survivors = append(survivors, retainedPayload{
				slotIndex: slotIndex,
				slot:      slot,
				payload:   append([]byte(nil), p.page.data[start:end]...),
			})
		case slotFlagRedirect:
			start, end, err := p.payloadBounds(slot)
			if err != nil {
				return false, err
			}
			hasRedirects = true
			survivors = append(survivors, retainedPayload{
				slotIndex: slotIndex,
				slot:      slot,
				payload:   append([]byte(nil), p.page.data[start:end]...),
			})
		case slotFlagDead:
			if slot.Offset != 0 || slot.Length != 0 {
				slot.Offset = 0
				slot.Length = 0
				p.writeSlot(slotIndex, slot)
				modified = true
			}
		case slotFlagUnused:
			if slot.Offset != 0 || slot.Length != 0 {
				slot.Offset = 0
				slot.Length = 0
				p.writeSlot(slotIndex, slot)
				modified = true
			}
		default:
			return false, fmt.Errorf("paged: page %d slot %d has invalid flags 0x%x", p.page.ID(), slotIndex, slot.Flags)
		}
	}

	flags := p.header.Flags &^ (PageFlagHasDeadTuples | PageFlagHasRedirects)
	if hasRedirects {
		flags |= PageFlagHasRedirects
	}
	if !modified && p.header.Flags == flags {
		return false, nil
	}

	for index := int(p.header.Lower); index < int(p.header.Special); index++ {
		p.page.data[index] = 0
	}

	upper := int(p.header.Special)
	for _, survivor := range survivors {
		upper -= len(survivor.payload)
		copy(p.page.data[upper:upper+len(survivor.payload)], survivor.payload)
		survivor.slot.Offset = uint16(upper)
		p.writeSlot(survivor.slotIndex, survivor.slot)
	}

	p.header.Upper = uint16(upper)
	p.header.DeadBytes = 0
	p.header.Flags = flags
	return true, p.flushHeader()
}

func writeRedirectPayload(dst []byte, target storage.RowHandle) {
	binary.LittleEndian.PutUint64(dst[0:8], target.Page)
	binary.LittleEndian.PutUint64(dst[8:16], target.Slot)
}

func (p *heapPage) syncHeader() error {
	header, err := p.page.Header()
	if err != nil {
		return err
	}
	p.header = header
	return nil
}

func (p *heapPage) flushHeader() error {
	return EncodePageHeader(p.page.data, p.header)
}
