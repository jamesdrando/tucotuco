package memory

import (
	"github.com/jamesdrando/tucotuco/internal/catalog"
	"github.com/jamesdrando/tucotuco/internal/storage"
)

// CreateTable resets any stale heap state for desc so recreated tables do not
// inherit rows from earlier incarnations.
func (s *Store) CreateTable(_ storage.Transaction, desc *catalog.TableDescriptor) error {
	if desc == nil || !desc.ID.Valid() {
		return ErrInvalidTable
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.tables == nil {
		s.tables = make(map[storage.TableID]*tableHeap)
	}

	s.tables[desc.ID] = &tableHeap{}

	return nil
}

// DropTable removes all heap state for desc.
func (s *Store) DropTable(_ storage.Transaction, desc *catalog.TableDescriptor) error {
	if desc == nil || !desc.ID.Valid() {
		return ErrInvalidTable
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.tables, desc.ID)

	return nil
}
