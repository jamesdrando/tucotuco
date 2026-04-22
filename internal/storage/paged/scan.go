package paged

import (
	"errors"
	"fmt"
	"io"

	"github.com/jamesdrando/tucotuco/internal/storage"
)

var (
	// ErrIteratorClosed reports iteration attempted after Close.
	ErrIteratorClosed = errors.New("paged: iterator is closed")
)

type committedSnapshot struct {
	records []storage.Record
	index   map[storage.RowHandle]int
}

type committedSnapshotRef struct {
	handle     storage.RowHandle
	target     storage.RowHandle
	redirected bool
}

// Scan opens an iterator over the currently committed rows in page/slot order.
func (r *Relation) Scan(options storage.ScanOptions) (storage.RowIterator, error) {
	snapshot, err := r.snapshotCommitted()
	if err != nil {
		return nil, err
	}

	records, err := filterScanRecords(snapshot.records, options.Normalized())
	if err != nil {
		return nil, err
	}

	return newRecordIterator(records), nil
}

func (r *Relation) snapshotCommitted() (committedSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.snapshotCommittedLocked()
}

func (r *Relation) snapshotCommittedLocked() (committedSnapshot, error) {
	if r == nil {
		return committedSnapshot{}, ErrInvalidRelation
	}
	if r.closed {
		return committedSnapshot{}, ErrClosed
	}

	pageCount, err := r.store.PageCount()
	if err != nil {
		return committedSnapshot{}, err
	}

	visibleRows := make(map[storage.RowHandle]storage.Row)
	targets := make(map[storage.RowHandle]struct{})
	refs := make([]committedSnapshotRef, 0)

	for pageID := PageID(1); pageID < pageCount; pageID++ {
		page, err := r.manager.Fetch(pageID)
		if err != nil {
			return committedSnapshot{}, err
		}

		heap, err := newHeapPage(page)
		if err != nil {
			_ = r.manager.Unpin(page, false)
			return committedSnapshot{}, err
		}

		slotCount := uint64(heap.header.SlotCount)
		for slotIndex := uint64(0); slotIndex < slotCount; slotIndex++ {
			slot, err := heap.readSlot(slotIndex)
			if err != nil {
				_ = r.manager.Unpin(page, false)
				return committedSnapshot{}, err
			}

			handle := storage.RowHandle{Page: uint64(pageID), Slot: slotIndex}
			switch slot.Flags {
			case slotFlagLive:
				tuple, _, err := heap.tuple(slotIndex)
				if err != nil {
					_ = r.manager.Unpin(page, false)
					return committedSnapshot{}, err
				}

				header, row, err := decodeStoredRowTuple(r.desc, tuple)
				if err != nil {
					_ = r.manager.Unpin(page, false)
					return committedSnapshot{}, err
				}
				if !header.visible() {
					continue
				}

				visibleRows[handle] = row.Clone()
				refs = append(refs, committedSnapshotRef{handle: handle})
			case slotFlagRedirect:
				target, err := heap.redirectHandle(slot)
				if err != nil {
					_ = r.manager.Unpin(page, false)
					return committedSnapshot{}, err
				}

				targets[target] = struct{}{}
				refs = append(refs, committedSnapshotRef{
					handle:     handle,
					target:     target,
					redirected: true,
				})
			case slotFlagDead, slotFlagUnused:
				continue
			default:
				_ = r.manager.Unpin(page, false)
				return committedSnapshot{}, fmt.Errorf(
					"paged: page %d slot %d has invalid flags 0x%x",
					pageID,
					slotIndex,
					slot.Flags,
				)
			}
		}

		if err := r.manager.Unpin(page, false); err != nil {
			return committedSnapshot{}, err
		}
	}

	records := make([]storage.Record, 0, len(refs))
	index := make(map[storage.RowHandle]int)
	for _, ref := range refs {
		if ref.redirected {
			row, ok := visibleRows[ref.target]
			if !ok {
				continue
			}

			records = append(records, storage.Record{
				Handle: ref.handle,
				Row:    row.Clone(),
			})
			index[ref.handle] = len(records) - 1
			continue
		}

		if _, ok := targets[ref.handle]; ok {
			continue
		}

		row, ok := visibleRows[ref.handle]
		if !ok {
			continue
		}

		records = append(records, storage.Record{
			Handle: ref.handle,
			Row:    row.Clone(),
		})
		index[ref.handle] = len(records) - 1
	}

	return committedSnapshot{records: records, index: index}, nil
}

func (s committedSnapshot) lookup(handle storage.RowHandle) (storage.Row, bool) {
	position, ok := s.index[handle]
	if !ok || position < 0 || position >= len(s.records) {
		return storage.Row{}, false
	}

	return s.records[position].Row.Clone(), true
}

func filterScanRecords(records []storage.Record, options storage.ScanOptions) ([]storage.Record, error) {
	filtered := make([]storage.Record, 0, len(records))
	for _, record := range records {
		match, err := matchesConstraints(record.Row, options.Constraints)
		if err != nil {
			return nil, err
		}
		if !match {
			continue
		}

		filtered = append(filtered, cloneRecord(record))
		if options.Limit > 0 && len(filtered) >= options.Limit {
			break
		}
	}

	return filtered, nil
}

func matchesConstraints(row storage.Row, constraints []storage.ScanConstraint) (bool, error) {
	for _, constraint := range constraints {
		rowValue, ok := row.Value(constraint.Column)
		if !ok || rowValue.IsNull() || constraint.Value.IsNull() {
			return false, nil
		}

		comparison, err := rowValue.Compare(constraint.Value)
		if err != nil {
			return false, fmt.Errorf("paged: compare column %d: %w", constraint.Column, err)
		}

		if !constraintMatches(constraint.Op, comparison) {
			return false, nil
		}
	}

	return true, nil
}

func constraintMatches(op storage.ComparisonOp, comparison int) bool {
	switch op {
	case storage.ComparisonEqual:
		return comparison == 0
	case storage.ComparisonNotEqual:
		return comparison != 0
	case storage.ComparisonLess:
		return comparison < 0
	case storage.ComparisonLessOrEqual:
		return comparison <= 0
	case storage.ComparisonGreater:
		return comparison > 0
	case storage.ComparisonGreaterOrEqual:
		return comparison >= 0
	default:
		return false
	}
}

func cloneRecord(record storage.Record) storage.Record {
	return storage.Record{
		Handle: record.Handle,
		Row:    record.Row.Clone(),
	}
}

type recordIterator struct {
	records []storage.Record
	index   int
	closed  bool
}

func newRecordIterator(records []storage.Record) storage.RowIterator {
	cloned := make([]storage.Record, len(records))
	for index, record := range records {
		cloned[index] = cloneRecord(record)
	}

	return &recordIterator{records: cloned}
}

func (it *recordIterator) Next() (storage.Record, error) {
	if it.closed {
		return storage.Record{}, ErrIteratorClosed
	}
	if it.index >= len(it.records) {
		return storage.Record{}, io.EOF
	}

	record := cloneRecord(it.records[it.index])
	it.index++
	return record, nil
}

func (it *recordIterator) Close() error {
	it.closed = true
	it.records = nil
	return nil
}
