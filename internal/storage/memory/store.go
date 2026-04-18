package memory

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/jamesdrando/tucotuco/internal/storage"
)

const memoryPageID uint64 = 1

var (
	// ErrInvalidTransaction reports a transaction handle that does not belong
	// to this store.
	ErrInvalidTransaction = errors.New("memory storage: invalid transaction")
	// ErrTransactionClosed reports work attempted after commit or rollback.
	ErrTransactionClosed = errors.New("memory storage: transaction is no longer active")
	// ErrReadOnlyTransaction reports a write attempted inside a read-only
	// transaction.
	ErrReadOnlyTransaction = errors.New("memory storage: transaction is read only")
	// ErrInvalidTable reports a table identifier without a table name.
	ErrInvalidTable = errors.New("memory storage: invalid table identifier")
	// ErrRowNotFound reports an update or delete that targets a missing row.
	ErrRowNotFound = errors.New("memory storage: row not found")
	// ErrIteratorClosed reports iteration attempted after Close.
	ErrIteratorClosed = errors.New("memory storage: iterator is closed")
)

// Store is the Phase 1 in-memory storage engine.
//
// Committed table state is protected by an RWMutex so scans can snapshot rows
// concurrently while writes remain exclusive.
type Store struct {
	mu     sync.RWMutex
	tables map[storage.TableID]*tableHeap
}

// New constructs an empty in-memory store.
func New() *Store {
	return &Store{
		tables: make(map[storage.TableID]*tableHeap),
	}
}

var _ storage.Storage = (*Store)(nil)

// Insert stages a row append inside tx and returns its reserved handle.
func (s *Store) Insert(tx storage.Transaction, table storage.TableID, row storage.Row) (storage.RowHandle, error) {
	if !table.Valid() {
		return storage.RowHandle{}, ErrInvalidTable
	}

	memTx, err := s.requireWritableTransaction(tx)
	if err != nil {
		return storage.RowHandle{}, err
	}

	handle := s.reserveHandle(table)

	memTx.mu.Lock()
	defer memTx.mu.Unlock()

	if err := memTx.ensureWritableLocked(); err != nil {
		return storage.RowHandle{}, err
	}

	tableChanges := memTx.tableChangesLocked(table)
	tableChanges[handle.Slot] = pendingChange{
		op:  pendingInsert,
		row: row.Clone(),
	}

	return handle, nil
}

// Scan snapshots matching rows visible to tx.
func (s *Store) Scan(tx storage.Transaction, table storage.TableID, options storage.ScanOptions) (storage.RowIterator, error) {
	if !table.Valid() {
		return nil, ErrInvalidTable
	}

	memTx, err := s.requireTransaction(tx)
	if err != nil {
		return nil, err
	}

	normalized := options.Normalized()
	changes, err := memTx.snapshotTableChanges(table)
	if err != nil {
		return nil, err
	}

	committed := s.snapshotTable(table)
	records, err := materializeRecords(committed, changes, normalized)
	if err != nil {
		return nil, err
	}

	return &iterator{records: records}, nil
}

// Update stages a row replacement inside tx.
func (s *Store) Update(tx storage.Transaction, table storage.TableID, handle storage.RowHandle, row storage.Row) error {
	if !table.Valid() {
		return ErrInvalidTable
	}
	if !handle.Valid() {
		return ErrRowNotFound
	}

	memTx, err := s.requireWritableTransaction(tx)
	if err != nil {
		return err
	}

	change, ok, err := memTx.lookupTableChange(table, handle.Slot)
	if err != nil {
		return err
	}
	if ok {
		switch change.op {
		case pendingInsert:
			return memTx.stageChange(table, handle.Slot, pendingChange{
				op:  pendingInsert,
				row: row.Clone(),
			})
		case pendingUpdate:
			return memTx.stageChange(table, handle.Slot, pendingChange{
				op:  pendingUpdate,
				row: row.Clone(),
			})
		case pendingDelete:
			return ErrRowNotFound
		}
	}

	if !s.committedRowExists(table, handle.Slot) {
		return ErrRowNotFound
	}

	return memTx.stageChange(table, handle.Slot, pendingChange{
		op:  pendingUpdate,
		row: row.Clone(),
	})
}

// Delete stages row removal inside tx.
func (s *Store) Delete(tx storage.Transaction, table storage.TableID, handle storage.RowHandle) error {
	if !table.Valid() {
		return ErrInvalidTable
	}
	if !handle.Valid() {
		return ErrRowNotFound
	}

	memTx, err := s.requireWritableTransaction(tx)
	if err != nil {
		return err
	}

	change, ok, err := memTx.lookupTableChange(table, handle.Slot)
	if err != nil {
		return err
	}
	if ok {
		switch change.op {
		case pendingInsert:
			return memTx.deletePendingInsert(table, handle.Slot)
		case pendingUpdate, pendingDelete:
			return memTx.stageChange(table, handle.Slot, pendingChange{op: pendingDelete})
		}
	}

	if !s.committedRowExists(table, handle.Slot) {
		return ErrRowNotFound
	}

	return memTx.stageChange(table, handle.Slot, pendingChange{op: pendingDelete})
}

// NewTransaction starts a new in-memory transaction.
func (s *Store) NewTransaction(options storage.TransactionOptions) (storage.Transaction, error) {
	normalized := options.Normalized()

	tx := &transaction{
		store:     s,
		isolation: normalized.Isolation,
		readOnly:  normalized.ReadOnly,
		pending:   make(map[storage.TableID]map[uint64]pendingChange),
	}
	tx.state.Store(uint32(txStateActive))

	return tx, nil
}

type tableHeap struct {
	nextSlot uint64
	heap     []heapEntry
}

type heapEntry struct {
	state rowState
	row   storage.Row
}

type rowState uint8

const (
	rowStateAbsent rowState = iota
	rowStateLive
	rowStateDeleted
)

type transaction struct {
	store     *Store
	isolation storage.IsolationLevel
	readOnly  bool

	state atomic.Uint32

	mu      sync.Mutex
	pending map[storage.TableID]map[uint64]pendingChange
}

type txState uint32

const (
	txStateActive txState = iota + 1
	txStateCommitted
	txStateRolledBack
)

type pendingOp uint8

const (
	pendingInsert pendingOp = iota + 1
	pendingUpdate
	pendingDelete
)

type pendingChange struct {
	op  pendingOp
	row storage.Row
}

func (tx *transaction) IsolationLevel() storage.IsolationLevel {
	return tx.isolation
}

func (tx *transaction) ReadOnly() bool {
	return tx.readOnly
}

func (tx *transaction) Commit() error {
	if !tx.state.CompareAndSwap(uint32(txStateActive), uint32(txStateCommitted)) {
		return ErrTransactionClosed
	}

	pending := tx.drainPending()
	tx.store.applyCommittedChanges(pending)

	return nil
}

func (tx *transaction) Rollback() error {
	if !tx.state.CompareAndSwap(uint32(txStateActive), uint32(txStateRolledBack)) {
		return ErrTransactionClosed
	}

	tx.drainPending()

	return nil
}

func (tx *transaction) ensureActive() error {
	if tx.state.Load() != uint32(txStateActive) {
		return ErrTransactionClosed
	}

	return nil
}

func (tx *transaction) ensureWritableLocked() error {
	if tx.state.Load() != uint32(txStateActive) {
		return ErrTransactionClosed
	}
	if tx.readOnly {
		return ErrReadOnlyTransaction
	}

	return nil
}

func (tx *transaction) lookupTableChange(table storage.TableID, slot uint64) (pendingChange, bool, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureActive(); err != nil {
		return pendingChange{}, false, err
	}

	tableChanges := tx.pending[table]
	if tableChanges == nil {
		return pendingChange{}, false, nil
	}

	change, ok := tableChanges[slot]
	if !ok {
		return pendingChange{}, false, nil
	}

	return pendingChange{
		op:  change.op,
		row: change.row.Clone(),
	}, true, nil
}

func (tx *transaction) stageChange(table storage.TableID, slot uint64, change pendingChange) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureWritableLocked(); err != nil {
		return err
	}

	tableChanges := tx.tableChangesLocked(table)
	tableChanges[slot] = pendingChange{
		op:  change.op,
		row: change.row.Clone(),
	}

	return nil
}

func (tx *transaction) deletePendingInsert(table storage.TableID, slot uint64) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureWritableLocked(); err != nil {
		return err
	}

	tableChanges := tx.pending[table]
	if tableChanges == nil {
		return ErrRowNotFound
	}

	change, ok := tableChanges[slot]
	if !ok {
		return ErrRowNotFound
	}
	if change.op != pendingInsert {
		tableChanges[slot] = pendingChange{op: pendingDelete}
		return nil
	}

	delete(tableChanges, slot)
	if len(tableChanges) == 0 {
		delete(tx.pending, table)
	}

	return nil
}

func (tx *transaction) snapshotTableChanges(table storage.TableID) (map[uint64]pendingChange, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureActive(); err != nil {
		return nil, err
	}

	tableChanges := tx.pending[table]
	if len(tableChanges) == 0 {
		return nil, nil
	}

	snapshot := make(map[uint64]pendingChange, len(tableChanges))
	for slot, change := range tableChanges {
		snapshot[slot] = pendingChange{
			op:  change.op,
			row: change.row.Clone(),
		}
	}

	return snapshot, nil
}

func (tx *transaction) drainPending() map[storage.TableID]map[uint64]pendingChange {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if len(tx.pending) == 0 {
		return nil
	}

	drained := make(map[storage.TableID]map[uint64]pendingChange, len(tx.pending))
	for table, tableChanges := range tx.pending {
		cloned := make(map[uint64]pendingChange, len(tableChanges))
		for slot, change := range tableChanges {
			cloned[slot] = pendingChange{
				op:  change.op,
				row: change.row.Clone(),
			}
		}
		drained[table] = cloned
	}

	tx.pending = make(map[storage.TableID]map[uint64]pendingChange)

	return drained
}

func (tx *transaction) tableChangesLocked(table storage.TableID) map[uint64]pendingChange {
	tableChanges := tx.pending[table]
	if tableChanges == nil {
		tableChanges = make(map[uint64]pendingChange)
		tx.pending[table] = tableChanges
	}

	return tableChanges
}

func (s *Store) requireTransaction(tx storage.Transaction) (*transaction, error) {
	memTx, ok := tx.(*transaction)
	if !ok || memTx == nil || memTx.store != s {
		return nil, ErrInvalidTransaction
	}

	if err := memTx.ensureActive(); err != nil {
		return nil, err
	}

	return memTx, nil
}

func (s *Store) requireWritableTransaction(tx storage.Transaction) (*transaction, error) {
	memTx, err := s.requireTransaction(tx)
	if err != nil {
		return nil, err
	}
	if memTx.readOnly {
		return nil, ErrReadOnlyTransaction
	}

	return memTx, nil
}

func (s *Store) reserveHandle(table storage.TableID) storage.RowHandle {
	s.mu.Lock()
	defer s.mu.Unlock()

	heap := s.tables[table]
	if heap == nil {
		heap = &tableHeap{}
		s.tables[table] = heap
	}

	heap.nextSlot++

	return storage.RowHandle{Page: memoryPageID, Slot: heap.nextSlot}
}

func (s *Store) committedRowExists(table storage.TableID, slot uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	heap := s.tables[table]
	if heap == nil || slot == 0 || slot > uint64(len(heap.heap)) {
		return false
	}

	return heap.heap[slot-1].state == rowStateLive
}

func (s *Store) snapshotTable(table storage.TableID) []heapEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	heap := s.tables[table]
	if heap == nil || len(heap.heap) == 0 {
		return nil
	}

	snapshot := make([]heapEntry, len(heap.heap))
	for index, entry := range heap.heap {
		snapshot[index].state = entry.state
		if entry.state == rowStateLive {
			snapshot[index].row = entry.row.Clone()
		}
	}

	return snapshot
}

func (s *Store) applyCommittedChanges(changes map[storage.TableID]map[uint64]pendingChange) {
	if len(changes) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for table, tableChanges := range changes {
		if len(tableChanges) == 0 {
			continue
		}

		heap := s.tables[table]
		if heap == nil {
			heap = &tableHeap{}
			s.tables[table] = heap
		}

		for slot, change := range tableChanges {
			if slot == 0 {
				continue
			}

			if heap.nextSlot < slot {
				heap.nextSlot = slot
			}

			ensureHeapLength(&heap.heap, int(slot))

			switch change.op {
			case pendingInsert, pendingUpdate:
				heap.heap[slot-1] = heapEntry{
					state: rowStateLive,
					row:   change.row.Clone(),
				}
			case pendingDelete:
				heap.heap[slot-1] = heapEntry{state: rowStateDeleted}
			}
		}
	}
}

func ensureHeapLength(heap *[]heapEntry, size int) {
	for len(*heap) < size {
		*heap = append(*heap, heapEntry{state: rowStateAbsent})
	}
}

func materializeRecords(committed []heapEntry, changes map[uint64]pendingChange, options storage.ScanOptions) ([]storage.Record, error) {
	maxSlot := len(committed)
	for slot := range changes {
		if int(slot) > maxSlot {
			maxSlot = int(slot)
		}
	}

	records := make([]storage.Record, 0, maxSlot)
	for slot := 1; slot <= maxSlot; slot++ {
		row, ok := visibleRowAtSlot(committed, changes, uint64(slot))
		if !ok {
			continue
		}

		match, err := matchesConstraints(row, options.Constraints)
		if err != nil {
			return nil, err
		}
		if !match {
			continue
		}

		records = append(records, storage.Record{
			Handle: storage.RowHandle{Page: memoryPageID, Slot: uint64(slot)},
			Row:    row.Clone(),
		})

		if options.Limit > 0 && len(records) >= options.Limit {
			break
		}
	}

	return records, nil
}

func visibleRowAtSlot(committed []heapEntry, changes map[uint64]pendingChange, slot uint64) (storage.Row, bool) {
	if change, ok := changes[slot]; ok {
		switch change.op {
		case pendingInsert, pendingUpdate:
			return change.row.Clone(), true
		case pendingDelete:
			return storage.Row{}, false
		}
	}

	if slot == 0 || slot > uint64(len(committed)) {
		return storage.Row{}, false
	}

	entry := committed[slot-1]
	if entry.state != rowStateLive {
		return storage.Row{}, false
	}

	return entry.row.Clone(), true
}

func matchesConstraints(row storage.Row, constraints []storage.ScanConstraint) (bool, error) {
	for _, constraint := range constraints {
		rowValue, ok := row.Value(constraint.Column)
		if !ok || rowValue.IsNull() || constraint.Value.IsNull() {
			return false, nil
		}

		comparison, err := rowValue.Compare(constraint.Value)
		if err != nil {
			return false, fmt.Errorf("memory storage: compare column %d: %w", constraint.Column, err)
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

type iterator struct {
	records []storage.Record
	index   int
	closed  bool
}

func (it *iterator) Next() (storage.Record, error) {
	if it.closed {
		return storage.Record{}, ErrIteratorClosed
	}
	if it.index >= len(it.records) {
		return storage.Record{}, io.EOF
	}

	record := it.records[it.index]
	it.index++

	return storage.Record{
		Handle: record.Handle,
		Row:    record.Row.Clone(),
	}, nil
}

func (it *iterator) Close() error {
	it.closed = true
	it.records = nil

	return nil
}
