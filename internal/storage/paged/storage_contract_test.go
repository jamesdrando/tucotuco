package paged

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync/atomic"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/catalog"
	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/storage/storagetest"
	"github.com/jamesdrando/tucotuco/internal/types"
)

var (
	errContractInvalidTransaction = errors.New("paged contract adapter: invalid transaction")
	errContractInvalidTable       = errors.New("paged contract adapter: invalid table identifier")
	errContractUnsupportedValue   = errors.New("paged contract adapter: unsupported value kind")
)

type contractStore struct {
	manager  *HeapManager
	table    storage.TableID
	relation *Relation
}

type contractTx struct {
	store     *contractStore
	isolation storage.IsolationLevel
	readOnly  bool

	state      atomic.Uint32
	relationTx *RelationTx
}

type contractTxState uint32

const (
	contractTxStateActive contractTxState = iota + 1
	contractTxStateCommitting
	contractTxStateCommitted
	contractTxStateRolledBack
)

var _ storage.Storage = (*contractStore)(nil)

var contractTableID = storage.TableID{Schema: "public", Name: "widgets"}

func TestPagedContractStoreImplementsStorage(t *testing.T) {
	t.Parallel()

	var _ storage.Storage = (*contractStore)(nil)
}

func TestPagedStorageContract(t *testing.T) {
	storagetest.RunStorageContract(t, storagetest.Harness{
		NewStore: func(t *testing.T) storage.Storage {
			return newContractStore(t)
		},
		Table: contractTableID,
		Matchers: storagetest.ErrorMatchers{
			InvalidTransaction: func(err error) bool {
				return errors.Is(err, errContractInvalidTransaction)
			},
			TransactionClosed: func(err error) bool {
				return errors.Is(err, ErrTransactionClosed)
			},
			ReadOnlyTransaction: func(err error) bool {
				return errors.Is(err, ErrReadOnlyTransaction)
			},
			InvalidTable: func(err error) bool {
				return errors.Is(err, errContractInvalidTable)
			},
			RowNotFound: func(err error) bool {
				return errors.Is(err, ErrRowNotFound)
			},
			IteratorClosed: func(err error) bool {
				return errors.Is(err, ErrIteratorClosed)
			},
			SerializationConflict: func(err error) bool {
				return errors.Is(err, ErrSerializationConflict)
			},
		},
	})
}

func newContractStore(t *testing.T) *contractStore {
	t.Helper()

	manager, err := OpenHeapManager(t.TempDir(), 512, 4)
	if err != nil {
		t.Fatalf("open heap manager: %v", err)
	}
	t.Cleanup(func() {
		_ = manager.Close()
	})

	desc := contractTableDescriptor(contractTableID)
	if err := manager.CreateTable(nil, desc); err != nil {
		t.Fatalf("create contract table: %v", err)
	}
	relation, err := manager.OpenRelation(desc)
	if err != nil {
		t.Fatalf("open contract relation: %v", err)
	}

	return &contractStore{
		manager:  manager,
		table:    contractTableID,
		relation: relation,
	}
}

func (s *contractStore) validateTable(table storage.TableID) error {
	if !table.Valid() || table != s.table {
		return errContractInvalidTable
	}

	return nil
}

func (s *contractStore) Insert(tx storage.Transaction, table storage.TableID, row storage.Row) (storage.RowHandle, error) {
	if err := s.validateTable(table); err != nil {
		return storage.RowHandle{}, errContractInvalidTable
	}

	contractTx, err := s.requireWritableTransaction(tx)
	if err != nil {
		return storage.RowHandle{}, err
	}

	relationTx, err := contractTx.ensureRelationTx(table)
	if err != nil {
		return storage.RowHandle{}, err
	}

	payload, err := encodeContractRow(row)
	if err != nil {
		return storage.RowHandle{}, err
	}

	return relationTx.Insert(storage.NewRow(types.BytesValue(payload)))
}

func (s *contractStore) Scan(tx storage.Transaction, table storage.TableID, options storage.ScanOptions) (storage.RowIterator, error) {
	if err := s.validateTable(table); err != nil {
		return nil, errContractInvalidTable
	}

	contractTx, err := s.requireTransaction(tx)
	if err != nil {
		return nil, err
	}

	relationTx, err := contractTx.ensureRelationTx(table)
	if err != nil {
		return nil, err
	}

	iter, err := relationTx.Scan(storage.ScanOptions{})
	if err != nil {
		return nil, err
	}

	records, err := decodeContractIterator(iter)
	if err != nil {
		return nil, err
	}

	filtered, err := filterScanRecords(records, options.Normalized())
	if err != nil {
		return nil, err
	}

	return newRecordIterator(filtered), nil
}

func (s *contractStore) Update(tx storage.Transaction, table storage.TableID, handle storage.RowHandle, row storage.Row) error {
	if err := s.validateTable(table); err != nil {
		return errContractInvalidTable
	}

	contractTx, err := s.requireWritableTransaction(tx)
	if err != nil {
		return err
	}

	relationTx, err := contractTx.ensureRelationTx(table)
	if err != nil {
		return err
	}

	payload, err := encodeContractRow(row)
	if err != nil {
		return err
	}

	return relationTx.Update(handle, storage.NewRow(types.BytesValue(payload)))
}

func (s *contractStore) Delete(tx storage.Transaction, table storage.TableID, handle storage.RowHandle) error {
	if err := s.validateTable(table); err != nil {
		return errContractInvalidTable
	}

	contractTx, err := s.requireWritableTransaction(tx)
	if err != nil {
		return err
	}

	relationTx, err := contractTx.ensureRelationTx(table)
	if err != nil {
		return err
	}

	return relationTx.Delete(handle)
}

func (s *contractStore) NewTransaction(options storage.TransactionOptions) (storage.Transaction, error) {
	normalized := options.Normalized()

	tx := &contractTx{
		store:     s,
		isolation: normalized.Isolation,
		readOnly:  normalized.ReadOnly,
	}
	tx.state.Store(uint32(contractTxStateActive))

	return tx, nil
}

func (s *contractStore) requireTransaction(tx storage.Transaction) (*contractTx, error) {
	contractTx, ok := tx.(*contractTx)
	if !ok || contractTx == nil || contractTx.store != s {
		return nil, errContractInvalidTransaction
	}
	if err := contractTx.ensureActive(); err != nil {
		return nil, err
	}
	return contractTx, nil
}

func (s *contractStore) requireWritableTransaction(tx storage.Transaction) (*contractTx, error) {
	contractTx, err := s.requireTransaction(tx)
	if err != nil {
		return nil, err
	}
	if contractTx.readOnly {
		return nil, ErrReadOnlyTransaction
	}
	return contractTx, nil
}

func (tx *contractTx) IsolationLevel() storage.IsolationLevel {
	if tx == nil {
		return ""
	}
	return tx.isolation
}

func (tx *contractTx) ReadOnly() bool {
	return tx != nil && tx.readOnly
}

func (tx *contractTx) Commit() error {
	if tx == nil {
		return ErrTransactionClosed
	}
	if !tx.state.CompareAndSwap(uint32(contractTxStateActive), uint32(contractTxStateCommitting)) {
		return ErrTransactionClosed
	}

	if tx.relationTx != nil {
		if err := tx.relationTx.Commit(); err != nil {
			tx.state.Store(uint32(contractTxStateActive))
			return err
		}
	}

	tx.relationTx = nil
	tx.state.Store(uint32(contractTxStateCommitted))
	return nil
}

func (tx *contractTx) Rollback() error {
	if tx == nil {
		return ErrTransactionClosed
	}
	if !tx.state.CompareAndSwap(uint32(contractTxStateActive), uint32(contractTxStateRolledBack)) {
		return ErrTransactionClosed
	}

	if tx.relationTx != nil {
		if err := tx.relationTx.Rollback(); err != nil {
			return err
		}
	}

	tx.relationTx = nil
	return nil
}

func (tx *contractTx) ensureActive() error {
	if tx == nil || tx.state.Load() != uint32(contractTxStateActive) {
		return ErrTransactionClosed
	}
	return nil
}

func (tx *contractTx) ensureRelationTx(table storage.TableID) (*RelationTx, error) {
	if err := tx.ensureActive(); err != nil {
		return nil, err
	}
	if err := tx.store.validateTable(table); err != nil {
		return nil, err
	}

	if tx.relationTx != nil {
		return tx.relationTx, nil
	}

	started, err := tx.store.relation.BeginTransaction(storage.TransactionOptions{
		Isolation: tx.isolation,
		ReadOnly:  tx.readOnly,
	})
	if err != nil {
		return nil, err
	}

	if tx.state.Load() != uint32(contractTxStateActive) {
		_ = started.Rollback()
		return nil, ErrTransactionClosed
	}

	if tx.relationTx != nil {
		_ = started.Rollback()
		return tx.relationTx, nil
	}

	tx.relationTx = started
	return started, nil
}

func contractTableDescriptor(id storage.TableID) *catalog.TableDescriptor {
	return &catalog.TableDescriptor{
		ID: id,
		Columns: []catalog.ColumnDescriptor{
			{
				Name: "payload",
				Type: types.TypeDesc{Kind: types.TypeKindVarBinary, Length: 4096, Nullable: false},
			},
		},
	}
}

func decodeContractIterator(iter storage.RowIterator) ([]storage.Record, error) {
	defer func() {
		_ = iter.Close()
	}()

	records := make([]storage.Record, 0)
	for {
		record, err := iter.Next()
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		if err != nil {
			return nil, err
		}

		row, err := decodeContractStorageRow(record.Row)
		if err != nil {
			return nil, err
		}

		records = append(records, storage.Record{
			Handle: record.Handle,
			Row:    row,
		})
	}
}

func decodeContractStorageRow(row storage.Row) (storage.Row, error) {
	value, ok := row.Value(0)
	if !ok {
		return storage.Row{}, fmt.Errorf("paged contract adapter: payload column missing")
	}

	raw, ok := value.Raw().([]byte)
	if !ok || value.Kind() != types.ValueKindBytes {
		return storage.Row{}, fmt.Errorf("paged contract adapter: payload type = %T, want []byte", value.Raw())
	}

	return decodeContractRow(raw)
}

func encodeContractRow(row storage.Row) ([]byte, error) {
	values := row.Values()
	payload := make([]byte, 0, len(values)*16)
	payload = appendUvarint(payload, uint64(len(values)))

	for _, value := range values {
		payload = append(payload, byte(value.Kind()))

		switch value.Kind() {
		case types.ValueKindNull:
		case types.ValueKindBool:
			if value.Raw().(bool) {
				payload = append(payload, 1)
			} else {
				payload = append(payload, 0)
			}
		case types.ValueKindInt16:
			buf := make([]byte, 2)
			binary.LittleEndian.PutUint16(buf, uint16(value.Raw().(int16)))
			payload = append(payload, buf...)
		case types.ValueKindInt32:
			buf := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, uint32(value.Raw().(int32)))
			payload = append(payload, buf...)
		case types.ValueKindInt64:
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(value.Raw().(int64)))
			payload = append(payload, buf...)
		case types.ValueKindFloat32:
			buf := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, math.Float32bits(value.Raw().(float32)))
			payload = append(payload, buf...)
		case types.ValueKindFloat64:
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, math.Float64bits(value.Raw().(float64)))
			payload = append(payload, buf...)
		case types.ValueKindString:
			raw := []byte(value.Raw().(string))
			payload = appendUvarint(payload, uint64(len(raw)))
			payload = append(payload, raw...)
		case types.ValueKindBytes:
			raw := value.Raw().([]byte)
			payload = appendUvarint(payload, uint64(len(raw)))
			payload = append(payload, raw...)
		default:
			return nil, fmt.Errorf("%w: %s", errContractUnsupportedValue, value.Kind())
		}
	}

	return payload, nil
}

func decodeContractRow(payload []byte) (storage.Row, error) {
	columnCount, remainder, err := consumeUvarint(payload)
	if err != nil {
		return storage.Row{}, err
	}

	values := make([]types.Value, 0, int(columnCount))
	for index := uint64(0); index < columnCount; index++ {
		if len(remainder) == 0 {
			return storage.Row{}, fmt.Errorf("paged contract adapter: row payload truncated at value %d", index)
		}

		kind := types.ValueKind(remainder[0])
		remainder = remainder[1:]

		switch kind {
		case types.ValueKindNull:
			values = append(values, types.NullValue())
		case types.ValueKindBool:
			if len(remainder) < 1 {
				return storage.Row{}, fmt.Errorf("paged contract adapter: bool payload truncated")
			}
			values = append(values, types.BoolValue(remainder[0] != 0))
			remainder = remainder[1:]
		case types.ValueKindInt16:
			if len(remainder) < 2 {
				return storage.Row{}, fmt.Errorf("paged contract adapter: int16 payload truncated")
			}
			values = append(values, types.Int16Value(int16(binary.LittleEndian.Uint16(remainder[:2]))))
			remainder = remainder[2:]
		case types.ValueKindInt32:
			if len(remainder) < 4 {
				return storage.Row{}, fmt.Errorf("paged contract adapter: int32 payload truncated")
			}
			values = append(values, types.Int32Value(int32(binary.LittleEndian.Uint32(remainder[:4]))))
			remainder = remainder[4:]
		case types.ValueKindInt64:
			if len(remainder) < 8 {
				return storage.Row{}, fmt.Errorf("paged contract adapter: int64 payload truncated")
			}
			values = append(values, types.Int64Value(int64(binary.LittleEndian.Uint64(remainder[:8]))))
			remainder = remainder[8:]
		case types.ValueKindFloat32:
			if len(remainder) < 4 {
				return storage.Row{}, fmt.Errorf("paged contract adapter: float32 payload truncated")
			}
			values = append(values, types.Float32Value(math.Float32frombits(binary.LittleEndian.Uint32(remainder[:4]))))
			remainder = remainder[4:]
		case types.ValueKindFloat64:
			if len(remainder) < 8 {
				return storage.Row{}, fmt.Errorf("paged contract adapter: float64 payload truncated")
			}
			values = append(values, types.Float64Value(math.Float64frombits(binary.LittleEndian.Uint64(remainder[:8]))))
			remainder = remainder[8:]
		case types.ValueKindString:
			var raw []byte
			raw, remainder, err = consumeLengthPrefixed(remainder)
			if err != nil {
				return storage.Row{}, err
			}
			values = append(values, types.StringValue(string(raw)))
		case types.ValueKindBytes:
			var raw []byte
			raw, remainder, err = consumeLengthPrefixed(remainder)
			if err != nil {
				return storage.Row{}, err
			}
			values = append(values, types.BytesValue(raw))
		default:
			return storage.Row{}, fmt.Errorf("%w: %s", errContractUnsupportedValue, kind)
		}
	}

	if len(remainder) != 0 {
		return storage.Row{}, fmt.Errorf("paged contract adapter: row payload has %d trailing bytes", len(remainder))
	}

	return storage.NewRow(values...), nil
}

func appendUvarint(dst []byte, value uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	size := binary.PutUvarint(buf[:], value)
	return append(dst, buf[:size]...)
}

func consumeUvarint(payload []byte) (uint64, []byte, error) {
	value, size := binary.Uvarint(payload)
	if size <= 0 {
		return 0, nil, fmt.Errorf("paged contract adapter: invalid uvarint payload")
	}
	return value, payload[size:], nil
}

func consumeLengthPrefixed(payload []byte) ([]byte, []byte, error) {
	length, remainder, err := consumeUvarint(payload)
	if err != nil {
		return nil, nil, err
	}
	if uint64(len(remainder)) < length {
		return nil, nil, fmt.Errorf("paged contract adapter: payload length %d exceeds remaining %d", length, len(remainder))
	}
	return bytes.Clone(remainder[:length]), remainder[length:], nil
}
