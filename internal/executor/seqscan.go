package executor

import "github.com/jamesdrando/tucotuco/internal/storage"

// SeqScan is the Phase 1 sequential table-scan operator.
type SeqScan struct {
	lifecycle lifecycle
	store     storage.Storage
	tx        storage.Transaction
	table     storage.TableID
	options   storage.ScanOptions
	iter      storage.RowIterator
}

var _ Operator = (*SeqScan)(nil)

// NewSeqScan constructs a sequential scan over one storage table.
func NewSeqScan(
	store storage.Storage,
	tx storage.Transaction,
	table storage.TableID,
	options storage.ScanOptions,
) *SeqScan {
	return &SeqScan{
		store:   store,
		tx:      tx,
		table:   table,
		options: options.Normalized(),
	}
}

// Open prepares the storage iterator used by the sequential scan.
func (s *SeqScan) Open() error {
	if err := s.lifecycle.Open(); err != nil {
		return err
	}

	iter, err := s.store.Scan(s.tx, s.table, s.options)
	if err != nil {
		if iter != nil {
			_ = iter.Close()
		}

		// Roll back the optimistic lifecycle transition so callers can observe
		// the original Open failure without the operator becoming terminal.
		s.lifecycle = lifecycle{}

		return err
	}

	s.iter = iter

	return nil
}

// Next returns the next visible row from storage.
func (s *SeqScan) Next() (Row, error) {
	if err := s.lifecycle.Next(); err != nil {
		return Row{}, err
	}

	record, err := s.iter.Next()
	if err != nil {
		return Row{}, err
	}

	return NewRowFromStorage(record), nil
}

// Close releases the storage iterator and terminally closes the operator.
func (s *SeqScan) Close() error {
	iter := s.iter
	s.iter = nil

	if err := s.lifecycle.Close(); err != nil {
		return err
	}
	if iter == nil {
		return nil
	}

	return iter.Close()
}
