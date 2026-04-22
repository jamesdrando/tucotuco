package paged

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/jamesdrando/tucotuco/internal/storage"
)

var (
	// ErrTransactionClosed reports work attempted after commit or rollback.
	ErrTransactionClosed = errors.New("paged: transaction is no longer active")
	// ErrReadOnlyTransaction reports a write attempted inside a read-only transaction.
	ErrReadOnlyTransaction = errors.New("paged: transaction is read only")
	// ErrSerializationConflict reports a serializable transaction whose pinned snapshot diverged.
	ErrSerializationConflict = errors.New("paged: serialization conflict")
)

// RelationTx is a relation-scoped transaction that stages writes in memory
// until commit.
type RelationTx struct {
	relation  *Relation
	isolation storage.IsolationLevel
	readOnly  bool

	state atomic.Uint32

	mu           sync.Mutex
	pending      map[storage.RowHandle]txPendingChange
	insertOrder  []storage.RowHandle
	nextTempSlot uint64

	snapshot       committedSnapshot
	snapshotPinned bool
}

type relationTxState uint32

const (
	relationTxStateActive relationTxState = iota + 1
	relationTxStateCommitting
	relationTxStateCommitted
	relationTxStateRolledBack
)

type txPendingOp uint8

const (
	txPendingInsert txPendingOp = iota + 1
	txPendingUpdate
	txPendingDelete
)

type txPendingChange struct {
	op  txPendingOp
	row storage.Row
}

// BeginTransaction starts a new relation-scoped transaction over committed
// row state plus staged local writes.
func (r *Relation) BeginTransaction(options storage.TransactionOptions) (*RelationTx, error) {
	if r == nil {
		return nil, ErrInvalidRelation
	}

	normalized := options.Normalized()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, ErrClosed
	}

	tx := &RelationTx{
		relation:    r,
		isolation:   normalized.Isolation,
		readOnly:    normalized.ReadOnly,
		pending:     make(map[storage.RowHandle]txPendingChange),
		insertOrder: make([]storage.RowHandle, 0),
	}
	tx.state.Store(uint32(relationTxStateActive))

	return tx, nil
}

// IsolationLevel reports the transaction isolation level.
func (tx *RelationTx) IsolationLevel() storage.IsolationLevel {
	if tx == nil {
		return ""
	}
	return tx.isolation
}

// ReadOnly reports whether the transaction may stage writes.
func (tx *RelationTx) ReadOnly() bool {
	return tx != nil && tx.readOnly
}

// Insert stages row for commit and returns a transaction-local handle.
func (tx *RelationTx) Insert(row storage.Row) (storage.RowHandle, error) {
	if err := tx.ensureWritable(); err != nil {
		return storage.RowHandle{}, err
	}
	if err := tx.observeCommittedSnapshot(); err != nil {
		return storage.RowHandle{}, err
	}
	if err := tx.validateRow(row); err != nil {
		return storage.RowHandle{}, err
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureWritableLocked(); err != nil {
		return storage.RowHandle{}, err
	}

	tx.nextTempSlot++
	handle := storage.RowHandle{Page: 0, Slot: tx.nextTempSlot}
	tx.pending[handle] = txPendingChange{
		op:  txPendingInsert,
		row: row.Clone(),
	}
	tx.insertOrder = append(tx.insertOrder, handle)

	return handle, nil
}

// Update stages row replacement for handle inside the transaction.
func (tx *RelationTx) Update(handle storage.RowHandle, row storage.Row) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	if err := tx.validateRow(row); err != nil {
		return err
	}

	snapshot, err := tx.committedSnapshot()
	if err != nil {
		return err
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureWritableLocked(); err != nil {
		return err
	}

	if change, ok := tx.pending[handle]; ok {
		switch change.op {
		case txPendingInsert, txPendingUpdate:
			tx.pending[handle] = txPendingChange{
				op:  change.op,
				row: row.Clone(),
			}
			return nil
		case txPendingDelete:
			return ErrRowNotFound
		}
	}

	if handle.Page == 0 {
		return ErrRowNotFound
	}
	if _, ok := snapshot.lookup(handle); !ok {
		return ErrRowNotFound
	}

	tx.pending[handle] = txPendingChange{
		op:  txPendingUpdate,
		row: row.Clone(),
	}

	return nil
}

// Delete stages row removal for handle inside the transaction.
func (tx *RelationTx) Delete(handle storage.RowHandle) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}

	snapshot, err := tx.committedSnapshot()
	if err != nil {
		return err
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureWritableLocked(); err != nil {
		return err
	}

	if change, ok := tx.pending[handle]; ok {
		switch change.op {
		case txPendingInsert:
			delete(tx.pending, handle)
			return nil
		case txPendingUpdate, txPendingDelete:
			tx.pending[handle] = txPendingChange{op: txPendingDelete}
			return nil
		}
	}

	if handle.Page == 0 {
		return ErrRowNotFound
	}
	if _, ok := snapshot.lookup(handle); !ok {
		return ErrRowNotFound
	}

	tx.pending[handle] = txPendingChange{op: txPendingDelete}

	return nil
}

// Lookup resolves handle against the transaction snapshot plus pending writes.
func (tx *RelationTx) Lookup(handle storage.RowHandle) (storage.Row, error) {
	if err := tx.ensureActive(); err != nil {
		return storage.Row{}, err
	}

	tx.mu.Lock()
	change, ok := tx.pending[handle]
	tx.mu.Unlock()
	if ok {
		switch change.op {
		case txPendingInsert, txPendingUpdate:
			return change.row.Clone(), nil
		case txPendingDelete:
			return storage.Row{}, ErrRowNotFound
		}
	}

	if handle.Page == 0 {
		return storage.Row{}, ErrRowNotFound
	}

	snapshot, err := tx.committedSnapshot()
	if err != nil {
		return storage.Row{}, err
	}

	row, ok := snapshot.lookup(handle)
	if !ok {
		return storage.Row{}, ErrRowNotFound
	}

	return row, nil
}

// Scan iterates over committed rows plus the transaction's staged changes.
func (tx *RelationTx) Scan(options storage.ScanOptions) (storage.RowIterator, error) {
	if err := tx.ensureActive(); err != nil {
		return nil, err
	}

	snapshot, err := tx.committedSnapshot()
	if err != nil {
		return nil, err
	}
	pending, insertOrder, err := tx.snapshotPending()
	if err != nil {
		return nil, err
	}

	records := make([]storage.Record, 0, len(snapshot.records)+len(insertOrder))
	for _, record := range snapshot.records {
		change, ok := pending[record.Handle]
		if ok {
			switch change.op {
			case txPendingDelete:
				continue
			case txPendingUpdate:
				record.Row = change.row.Clone()
			}
		}

		records = append(records, cloneRecord(record))
	}

	for _, handle := range insertOrder {
		change, ok := pending[handle]
		if !ok || change.op != txPendingInsert {
			continue
		}

		records = append(records, storage.Record{
			Handle: handle,
			Row:    change.row.Clone(),
		})
	}

	filtered, err := filterScanRecords(records, options.Normalized())
	if err != nil {
		return nil, err
	}

	return newRecordIterator(filtered), nil
}

// Commit replays the staged writes through the existing committed relation
// mutation paths.
func (tx *RelationTx) Commit() error {
	if tx == nil {
		return ErrTransactionClosed
	}
	if !tx.state.CompareAndSwap(uint32(relationTxStateActive), uint32(relationTxStateCommitting)) {
		return ErrTransactionClosed
	}

	pending, insertOrder, snapshot, pinned, err := tx.snapshotForCommit()
	if err != nil {
		tx.state.Store(uint32(relationTxStateActive))
		return err
	}

	relation := tx.relation
	if relation == nil {
		tx.state.Store(uint32(relationTxStateActive))
		return ErrInvalidRelation
	}

	relation.mu.Lock()
	defer relation.mu.Unlock()

	if relation.closed {
		tx.state.Store(uint32(relationTxStateActive))
		return ErrClosed
	}

	if tx.isolation == storage.IsolationSerializable && pinned {
		current, err := relation.snapshotCommittedLocked()
		if err != nil {
			tx.state.Store(uint32(relationTxStateActive))
			return err
		}
		if !committedSnapshotsEqual(snapshot, current) {
			tx.state.Store(uint32(relationTxStateActive))
			return ErrSerializationConflict
		}
	}

	handles := sortedCommittedPendingHandles(pending)
	for _, handle := range handles {
		if _, err := relation.lookupLocked(handle); err != nil {
			tx.state.Store(uint32(relationTxStateActive))
			return err
		}
	}

	for _, handle := range handles {
		change := pending[handle]
		var err error
		switch change.op {
		case txPendingDelete:
			err = relation.deleteLocked(handle)
		case txPendingUpdate:
			err = relation.updateLocked(handle, change.row)
		}
		if err != nil {
			tx.state.Store(uint32(relationTxStateActive))
			return err
		}
	}

	for _, handle := range insertOrder {
		change, ok := pending[handle]
		if !ok || change.op != txPendingInsert {
			continue
		}
		if _, err := relation.insertLocked(change.row); err != nil {
			tx.state.Store(uint32(relationTxStateActive))
			return err
		}
	}

	tx.resetPending()
	tx.state.Store(uint32(relationTxStateCommitted))

	return nil
}

// Rollback discards the staged writes without touching the relation or WAL.
func (tx *RelationTx) Rollback() error {
	if tx == nil {
		return ErrTransactionClosed
	}
	if !tx.state.CompareAndSwap(uint32(relationTxStateActive), uint32(relationTxStateRolledBack)) {
		return ErrTransactionClosed
	}

	tx.resetPending()
	return nil
}

func (tx *RelationTx) ensureActive() error {
	if tx == nil || tx.state.Load() != uint32(relationTxStateActive) {
		return ErrTransactionClosed
	}
	return nil
}

func (tx *RelationTx) ensureWritable() error {
	if err := tx.ensureActive(); err != nil {
		return err
	}
	if tx.readOnly {
		return ErrReadOnlyTransaction
	}
	return nil
}

func (tx *RelationTx) ensureWritableLocked() error {
	if tx.state.Load() != uint32(relationTxStateActive) {
		return ErrTransactionClosed
	}
	if tx.readOnly {
		return ErrReadOnlyTransaction
	}
	return nil
}

func (tx *RelationTx) validateRow(row storage.Row) error {
	if tx == nil || tx.relation == nil {
		return ErrInvalidRelation
	}

	tuple, err := encodeRowTuple(tx.relation.desc, row, 1)
	if err != nil {
		return err
	}
	if len(tuple)+pageSlotSize > tx.relation.store.PageSize()-pageHeaderSize {
		return ErrRowTooLarge
	}

	return nil
}

func (tx *RelationTx) observeCommittedSnapshot() error {
	switch tx.isolation {
	case storage.IsolationRepeatableRead, storage.IsolationSerializable:
		_, err := tx.committedSnapshot()
		return err
	default:
		return nil
	}
}

func (tx *RelationTx) committedSnapshot() (committedSnapshot, error) {
	if err := tx.ensureActive(); err != nil {
		return committedSnapshot{}, err
	}
	if tx == nil || tx.relation == nil {
		return committedSnapshot{}, ErrInvalidRelation
	}

	switch tx.isolation {
	case storage.IsolationRepeatableRead, storage.IsolationSerializable:
		tx.mu.Lock()
		snapshot, ok := tx.snapshot, tx.snapshotPinned
		tx.mu.Unlock()
		if ok {
			return snapshot, nil
		}

		captured, err := tx.relation.snapshotCommitted()
		if err != nil {
			return committedSnapshot{}, err
		}

		tx.mu.Lock()
		defer tx.mu.Unlock()

		if tx.snapshotPinned {
			return tx.snapshot, nil
		}

		tx.snapshot = captured
		tx.snapshotPinned = true
		return captured, nil
	default:
		return tx.relation.snapshotCommitted()
	}
}

func (tx *RelationTx) snapshotPending() (map[storage.RowHandle]txPendingChange, []storage.RowHandle, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureActive(); err != nil {
		return nil, nil, err
	}

	return cloneTxPendingChanges(tx.pending), append([]storage.RowHandle(nil), tx.insertOrder...), nil
}

func (tx *RelationTx) snapshotForCommit() (map[storage.RowHandle]txPendingChange, []storage.RowHandle, committedSnapshot, bool, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	snapshot := tx.snapshot
	return cloneTxPendingChanges(tx.pending), append([]storage.RowHandle(nil), tx.insertOrder...), snapshot, tx.snapshotPinned, nil
}

func (tx *RelationTx) resetPending() {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.pending = make(map[storage.RowHandle]txPendingChange)
	tx.insertOrder = nil
	tx.snapshot = committedSnapshot{}
	tx.snapshotPinned = false
}

func cloneTxPendingChanges(changes map[storage.RowHandle]txPendingChange) map[storage.RowHandle]txPendingChange {
	if len(changes) == 0 {
		return nil
	}

	cloned := make(map[storage.RowHandle]txPendingChange, len(changes))
	for handle, change := range changes {
		cloned[handle] = txPendingChange{
			op:  change.op,
			row: change.row.Clone(),
		}
	}

	return cloned
}

func sortedCommittedPendingHandles(pending map[storage.RowHandle]txPendingChange) []storage.RowHandle {
	handles := make([]storage.RowHandle, 0, len(pending))
	for handle := range pending {
		if handle.Page == 0 {
			continue
		}
		handles = append(handles, handle)
	}

	sort.Slice(handles, func(i, j int) bool {
		if handles[i].Page != handles[j].Page {
			return handles[i].Page < handles[j].Page
		}
		return handles[i].Slot < handles[j].Slot
	})

	return handles
}

func committedSnapshotsEqual(left, right committedSnapshot) bool {
	if len(left.records) != len(right.records) {
		return false
	}

	for index := range left.records {
		if left.records[index].Handle != right.records[index].Handle {
			return false
		}
		if !relationTxRowsEqual(left.records[index].Row, right.records[index].Row) {
			return false
		}
	}

	return true
}

func relationTxRowsEqual(left, right storage.Row) bool {
	if left.Len() != right.Len() {
		return false
	}

	for index := 0; index < left.Len(); index++ {
		leftValue, ok := left.Value(index)
		if !ok {
			return false
		}

		rightValue, ok := right.Value(index)
		if !ok {
			return false
		}

		if !leftValue.Equal(rightValue) {
			return false
		}
	}

	return true
}
