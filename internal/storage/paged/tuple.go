package paged

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/jamesdrando/tucotuco/internal/catalog"
	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/types"
)

const (
	tupleVersion    uint16 = 1
	tupleHeaderSize        = 44

	forwardPtrSlotBits = 16
	forwardPtrSlotMask = (1 << forwardPtrSlotBits) - 1
	forwardPtrPageMask = ^uint64(forwardPtrSlotMask)
)

// Tuple flags reserve space for later MVCC and redirect semantics.
const (
	tupleFlagLive uint16 = 1 << iota
	tupleFlagDeleted
	tupleFlagRedirected
)

type tupleHeader struct {
	Version    uint16
	Flags      uint16
	PayloadLen uint32
	NullmapLen uint32
	Xmin       uint64
	Xmax       uint64
	ForwardPtr uint64
	Reserved   uint64
}

func (h tupleHeader) visible() bool {
	return h.Flags == tupleFlagLive && h.Xmax == 0 && h.ForwardPtr == 0
}

func (h tupleHeader) replacement() bool {
	return h.Flags == tupleFlagDeleted && h.Xmax != 0 && h.ForwardPtr != 0
}

func (h tupleHeader) deleted() bool {
	return h.Flags == tupleFlagDeleted && h.Xmax != 0 && h.ForwardPtr == 0
}

func decodeTupleHeader(tuple []byte) (tupleHeader, error) {
	if len(tuple) < tupleHeaderSize {
		return tupleHeader{}, fmt.Errorf("paged: tuple image too small: %d", len(tuple))
	}

	return tupleHeader{
		Version:    binary.LittleEndian.Uint16(tuple[0x00:0x02]),
		Flags:      binary.LittleEndian.Uint16(tuple[0x02:0x04]),
		PayloadLen: binary.LittleEndian.Uint32(tuple[0x04:0x08]),
		NullmapLen: binary.LittleEndian.Uint32(tuple[0x08:0x0c]),
		Xmin:       binary.LittleEndian.Uint64(tuple[0x0c:0x14]),
		Xmax:       binary.LittleEndian.Uint64(tuple[0x14:0x1c]),
		ForwardPtr: binary.LittleEndian.Uint64(tuple[0x1c:0x24]),
		Reserved:   binary.LittleEndian.Uint64(tuple[0x24:0x2c]),
	}, nil
}

func encodeTupleHeader(dst []byte, header tupleHeader) error {
	if len(dst) < tupleHeaderSize {
		return fmt.Errorf("paged: tuple header destination too small: %d", len(dst))
	}

	binary.LittleEndian.PutUint16(dst[0x00:0x02], header.Version)
	binary.LittleEndian.PutUint16(dst[0x02:0x04], header.Flags)
	binary.LittleEndian.PutUint32(dst[0x04:0x08], header.PayloadLen)
	binary.LittleEndian.PutUint32(dst[0x08:0x0c], header.NullmapLen)
	binary.LittleEndian.PutUint64(dst[0x0c:0x14], header.Xmin)
	binary.LittleEndian.PutUint64(dst[0x14:0x1c], header.Xmax)
	binary.LittleEndian.PutUint64(dst[0x1c:0x24], header.ForwardPtr)
	binary.LittleEndian.PutUint64(dst[0x24:0x2c], header.Reserved)
	return nil
}

func encodeRowTuple(desc *catalog.TableDescriptor, row storage.Row, xmin uint64) ([]byte, error) {
	if desc == nil || !desc.ID.Valid() {
		return nil, ErrInvalidRelation
	}
	if row.Len() != len(desc.Columns) {
		return nil, fmt.Errorf("paged: row has %d values, want %d", row.Len(), len(desc.Columns))
	}

	nullableCount := 0
	for _, column := range desc.Columns {
		if column.Type.Nullable {
			nullableCount++
		}
	}

	nullmapLen := (nullableCount + 7) / 8
	body := make([]byte, nullmapLen)
	nullableIndex := 0

	for index, column := range desc.Columns {
		value, ok := row.Value(index)
		if !ok {
			return nil, fmt.Errorf("paged: row missing value at index %d", index)
		}

		if column.Type.Nullable {
			if value.IsNull() {
				body[nullableIndex/8] |= 1 << (nullableIndex % 8)
				nullableIndex++
				continue
			}
			nullableIndex++
		} else if value.IsNull() {
			return nil, fmt.Errorf("paged: column %q does not allow NULL", column.Name)
		}

		encoded, err := encodeColumnValue(column.Type, value)
		if err != nil {
			return nil, fmt.Errorf("paged: encode column %q: %w", column.Name, err)
		}
		body = append(body, encoded...)
	}

	tuple := make([]byte, tupleHeaderSize+len(body))
	header := tupleHeader{
		Version:    tupleVersion,
		Flags:      tupleFlagLive,
		PayloadLen: uint32(len(body)),
		NullmapLen: uint32(nullmapLen),
		Xmin:       xmin,
	}
	if err := encodeTupleHeader(tuple, header); err != nil {
		return nil, err
	}
	copy(tuple[tupleHeaderSize:], body)
	return tuple, nil
}

func decodeRowTuple(desc *catalog.TableDescriptor, tuple []byte) (storage.Row, error) {
	header, row, err := decodeStoredRowTuple(desc, tuple)
	if err != nil {
		return storage.Row{}, err
	}
	if !header.visible() {
		return storage.Row{}, ErrRowNotFound
	}
	return row, nil
}

func decodeStoredRowTuple(desc *catalog.TableDescriptor, tuple []byte) (tupleHeader, storage.Row, error) {
	if desc == nil || !desc.ID.Valid() {
		return tupleHeader{}, storage.Row{}, ErrInvalidRelation
	}

	header, err := decodeTupleHeader(tuple)
	if err != nil {
		return tupleHeader{}, storage.Row{}, err
	}
	if header.Version != tupleVersion {
		return tupleHeader{}, storage.Row{}, fmt.Errorf("paged: unsupported tuple version %d", header.Version)
	}
	if header.Flags != tupleFlagLive && header.Flags != tupleFlagDeleted && header.Flags != tupleFlagRedirected {
		return tupleHeader{}, storage.Row{}, fmt.Errorf("paged: unsupported tuple flags 0x%x", header.Flags)
	}

	totalLen := tupleHeaderSize + int(header.PayloadLen)
	if len(tuple) < totalLen {
		return tupleHeader{}, storage.Row{}, fmt.Errorf("paged: tuple payload truncated: have %d want %d", len(tuple), totalLen)
	}

	body := tuple[tupleHeaderSize:totalLen]
	if int(header.NullmapLen) > len(body) {
		return tupleHeader{}, storage.Row{}, fmt.Errorf(
			"paged: null bitmap length %d exceeds tuple payload %d",
			header.NullmapLen,
			len(body),
		)
	}

	nullmap := body[:header.NullmapLen]
	payload := body[header.NullmapLen:]
	nullableIndex := 0
	offset := 0
	values := make([]types.Value, len(desc.Columns))

	for index, column := range desc.Columns {
		if column.Type.Nullable {
			if bitIsSet(nullmap, nullableIndex) {
				values[index] = types.NullValue()
				nullableIndex++
				continue
			}
			nullableIndex++
		}

		value, consumed, err := decodeColumnValue(column.Type, payload[offset:])
		if err != nil {
			return tupleHeader{}, storage.Row{}, fmt.Errorf("paged: decode column %q: %w", column.Name, err)
		}
		offset += consumed
		values[index] = value
	}

	if offset != len(payload) {
		return tupleHeader{}, storage.Row{}, fmt.Errorf("paged: tuple payload has %d trailing bytes", len(payload)-offset)
	}

	return header, storage.NewRow(values...), nil
}

func encodeForwardPtr(handle storage.RowHandle) (uint64, error) {
	if !handle.Valid() || handle.Page == 0 {
		return 0, ErrRowNotFound
	}
	if handle.Page > forwardPtrPageMask>>forwardPtrSlotBits {
		return 0, fmt.Errorf("paged: row handle page %d exceeds packed forward pointer", handle.Page)
	}
	if handle.Slot > forwardPtrSlotMask {
		return 0, fmt.Errorf("paged: row handle slot %d exceeds packed forward pointer", handle.Slot)
	}

	return (handle.Page << forwardPtrSlotBits) | handle.Slot, nil
}

func decodeForwardPtr(ptr uint64) (storage.RowHandle, error) {
	if ptr == 0 {
		return storage.RowHandle{}, ErrRowNotFound
	}

	handle := storage.RowHandle{
		Page: ptr >> forwardPtrSlotBits,
		Slot: ptr & forwardPtrSlotMask,
	}
	if !handle.Valid() || handle.Page == 0 {
		return storage.RowHandle{}, fmt.Errorf("paged: invalid packed forward pointer 0x%x", ptr)
	}
	return handle, nil
}

func encodeColumnValue(desc types.TypeDesc, value types.Value) ([]byte, error) {
	switch desc.Kind {
	case types.TypeKindBoolean:
		boolValue, ok := value.Raw().(bool)
		if !ok || value.Kind() != types.ValueKindBool {
			return nil, fmt.Errorf("%w: expected BOOLEAN, got %s", ErrTypeMismatch, value.Kind())
		}
		if boolValue {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case types.TypeKindSmallInt:
		n, err := exactInt64(value)
		if err != nil {
			return nil, err
		}
		if n < math.MinInt16 || n > math.MaxInt16 {
			return nil, fmt.Errorf("%w: SMALLINT out of range", ErrTypeMismatch)
		}
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, uint16(int16(n)))
		return buf, nil
	case types.TypeKindInteger:
		n, err := exactInt64(value)
		if err != nil {
			return nil, err
		}
		if n < math.MinInt32 || n > math.MaxInt32 {
			return nil, fmt.Errorf("%w: INTEGER out of range", ErrTypeMismatch)
		}
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(int32(n)))
		return buf, nil
	case types.TypeKindBigInt:
		n, err := exactInt64(value)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(n))
		return buf, nil
	case types.TypeKindReal:
		floatValue, ok := value.Raw().(float32)
		if !ok || value.Kind() != types.ValueKindFloat32 {
			return nil, fmt.Errorf("%w: expected REAL, got %s", ErrTypeMismatch, value.Kind())
		}
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(floatValue))
		return buf, nil
	case types.TypeKindDoublePrecision:
		switch value.Kind() {
		case types.ValueKindFloat32:
			return encodeColumnValue(
				types.TypeDesc{Kind: types.TypeKindDoublePrecision, Nullable: desc.Nullable},
				types.Float64Value(float64(value.Raw().(float32))),
			)
		case types.ValueKindFloat64:
			floatValue := value.Raw().(float64)
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, math.Float64bits(floatValue))
			return buf, nil
		default:
			return nil, fmt.Errorf("%w: expected DOUBLE PRECISION, got %s", ErrTypeMismatch, value.Kind())
		}
	case types.TypeKindChar, types.TypeKindVarChar, types.TypeKindText, types.TypeKindCLOB:
		stringValue, ok := value.Raw().(string)
		if !ok || value.Kind() != types.ValueKindString {
			return nil, fmt.Errorf("%w: expected string-compatible value, got %s", ErrTypeMismatch, value.Kind())
		}
		return encodeLengthPrefixedBytes([]byte(stringValue)), nil
	case types.TypeKindBinary, types.TypeKindVarBinary, types.TypeKindBLOB:
		bytesValue, ok := value.Raw().([]byte)
		if !ok || value.Kind() != types.ValueKindBytes {
			return nil, fmt.Errorf("%w: expected binary value, got %s", ErrTypeMismatch, value.Kind())
		}
		return encodeLengthPrefixedBytes(bytesValue), nil
	case types.TypeKindDate, types.TypeKindTimeWithTimeZone, types.TypeKindTimestamp, types.TypeKindTimestampWithTimeZone:
		timeValue, ok := value.Raw().(time.Time)
		if !ok || value.Kind() != types.ValueKindDateTime {
			return nil, fmt.Errorf("%w: expected time value, got %s", ErrTypeMismatch, value.Kind())
		}
		encoded, err := timeValue.MarshalBinary()
		if err != nil {
			return nil, err
		}
		return encodeLengthPrefixedBytes(encoded), nil
	case types.TypeKindTime:
		duration, ok := value.Raw().(time.Duration)
		if !ok || value.Kind() != types.ValueKindTimeOfDay {
			return nil, fmt.Errorf("%w: expected TIME value, got %s", ErrTypeMismatch, value.Kind())
		}
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(duration))
		return buf, nil
	case types.TypeKindInterval:
		interval, ok := value.Raw().(types.Interval)
		if !ok || value.Kind() != types.ValueKindInterval {
			return nil, fmt.Errorf("%w: expected INTERVAL value, got %s", ErrTypeMismatch, value.Kind())
		}
		buf := make([]byte, 24)
		binary.LittleEndian.PutUint64(buf[0:8], uint64(interval.Months))
		binary.LittleEndian.PutUint64(buf[8:16], uint64(interval.Days))
		binary.LittleEndian.PutUint64(buf[16:24], uint64(interval.Nanos))
		return buf, nil
	case types.TypeKindNumeric, types.TypeKindDecimal:
		decimal, err := decimalValue(value)
		if err != nil {
			return nil, err
		}
		return encodeDecimal(decimal), nil
	default:
		return nil, fmt.Errorf("%w: unsupported type %s", ErrUnsupportedType, desc.Kind)
	}
}

func decodeColumnValue(desc types.TypeDesc, payload []byte) (types.Value, int, error) {
	switch desc.Kind {
	case types.TypeKindBoolean:
		if len(payload) < 1 {
			return types.Value{}, 0, ioShortValue(desc.Kind, 1, len(payload))
		}
		return types.BoolValue(payload[0] != 0), 1, nil
	case types.TypeKindSmallInt:
		if len(payload) < 2 {
			return types.Value{}, 0, ioShortValue(desc.Kind, 2, len(payload))
		}
		return types.Int16Value(int16(binary.LittleEndian.Uint16(payload[0:2]))), 2, nil
	case types.TypeKindInteger:
		if len(payload) < 4 {
			return types.Value{}, 0, ioShortValue(desc.Kind, 4, len(payload))
		}
		return types.Int32Value(int32(binary.LittleEndian.Uint32(payload[0:4]))), 4, nil
	case types.TypeKindBigInt:
		if len(payload) < 8 {
			return types.Value{}, 0, ioShortValue(desc.Kind, 8, len(payload))
		}
		return types.Int64Value(int64(binary.LittleEndian.Uint64(payload[0:8]))), 8, nil
	case types.TypeKindReal:
		if len(payload) < 4 {
			return types.Value{}, 0, ioShortValue(desc.Kind, 4, len(payload))
		}
		return types.Float32Value(math.Float32frombits(binary.LittleEndian.Uint32(payload[0:4]))), 4, nil
	case types.TypeKindDoublePrecision:
		if len(payload) < 8 {
			return types.Value{}, 0, ioShortValue(desc.Kind, 8, len(payload))
		}
		return types.Float64Value(math.Float64frombits(binary.LittleEndian.Uint64(payload[0:8]))), 8, nil
	case types.TypeKindChar, types.TypeKindVarChar, types.TypeKindText, types.TypeKindCLOB:
		bytesValue, consumed, err := decodeLengthPrefixedBytes(payload)
		if err != nil {
			return types.Value{}, 0, err
		}
		return types.StringValue(string(bytesValue)), consumed, nil
	case types.TypeKindBinary, types.TypeKindVarBinary, types.TypeKindBLOB:
		bytesValue, consumed, err := decodeLengthPrefixedBytes(payload)
		if err != nil {
			return types.Value{}, 0, err
		}
		return types.BytesValue(bytesValue), consumed, nil
	case types.TypeKindDate, types.TypeKindTimeWithTimeZone, types.TypeKindTimestamp, types.TypeKindTimestampWithTimeZone:
		bytesValue, consumed, err := decodeLengthPrefixedBytes(payload)
		if err != nil {
			return types.Value{}, 0, err
		}
		var timeValue time.Time
		if err := timeValue.UnmarshalBinary(bytesValue); err != nil {
			return types.Value{}, 0, err
		}
		return types.DateTimeValue(timeValue), consumed, nil
	case types.TypeKindTime:
		if len(payload) < 8 {
			return types.Value{}, 0, ioShortValue(desc.Kind, 8, len(payload))
		}
		return types.TimeOfDayValue(time.Duration(int64(binary.LittleEndian.Uint64(payload[0:8])))), 8, nil
	case types.TypeKindInterval:
		if len(payload) < 24 {
			return types.Value{}, 0, ioShortValue(desc.Kind, 24, len(payload))
		}
		interval := types.NewInterval(
			int64(binary.LittleEndian.Uint64(payload[0:8])),
			int64(binary.LittleEndian.Uint64(payload[8:16])),
			int64(binary.LittleEndian.Uint64(payload[16:24])),
		)
		return types.IntervalValue(interval), 24, nil
	case types.TypeKindNumeric, types.TypeKindDecimal:
		decimal, consumed, err := decodeDecimal(payload)
		if err != nil {
			return types.Value{}, 0, err
		}
		return types.DecimalValue(decimal), consumed, nil
	default:
		return types.Value{}, 0, fmt.Errorf("%w: unsupported type %s", ErrUnsupportedType, desc.Kind)
	}
}

func encodeLengthPrefixedBytes(value []byte) []byte {
	buf := make([]byte, 4+len(value))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(value)))
	copy(buf[4:], value)
	return buf
}

func decodeLengthPrefixedBytes(payload []byte) ([]byte, int, error) {
	if len(payload) < 4 {
		return nil, 0, ioShortValue(types.TypeKindVarBinary, 4, len(payload))
	}

	length := int(binary.LittleEndian.Uint32(payload[0:4]))
	if length < 0 || len(payload) < 4+length {
		return nil, 0, fmt.Errorf("paged: length-prefixed value truncated: need %d have %d", length, len(payload)-4)
	}

	return bytes.Clone(payload[4 : 4+length]), 4 + length, nil
}

func encodeDecimal(decimal types.Decimal) []byte {
	coefficient := decimal.Coefficient()
	sign := byte(0)
	if coefficient.Sign() < 0 {
		sign = 1
		coefficient.Abs(coefficient)
	}

	coeffBytes := coefficient.Bytes()
	buf := make([]byte, 9+len(coeffBytes))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(decimal.Scale()))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(coeffBytes)))
	buf[8] = sign
	copy(buf[9:], coeffBytes)
	return buf
}

func decodeDecimal(payload []byte) (types.Decimal, int, error) {
	if len(payload) < 9 {
		return types.Decimal{}, 0, ioShortValue(types.TypeKindDecimal, 9, len(payload))
	}

	scale := int32(binary.LittleEndian.Uint32(payload[0:4]))
	coeffLen := int(binary.LittleEndian.Uint32(payload[4:8]))
	if coeffLen < 0 || len(payload) < 9+coeffLen {
		return types.Decimal{}, 0, fmt.Errorf("paged: decimal coefficient truncated: need %d have %d", coeffLen, len(payload)-9)
	}

	coeff := new(big.Int).SetBytes(payload[9 : 9+coeffLen])
	if payload[8] == 1 {
		coeff.Neg(coeff)
	}
	decimal, err := types.NewDecimal(coeff, scale)
	if err != nil {
		return types.Decimal{}, 0, err
	}
	return decimal, 9 + coeffLen, nil
}

func exactInt64(value types.Value) (int64, error) {
	switch value.Kind() {
	case types.ValueKindInt16:
		return int64(value.Raw().(int16)), nil
	case types.ValueKindInt32:
		return int64(value.Raw().(int32)), nil
	case types.ValueKindInt64:
		return value.Raw().(int64), nil
	default:
		return 0, fmt.Errorf("%w: expected exact integer, got %s", ErrTypeMismatch, value.Kind())
	}
}

func decimalValue(value types.Value) (types.Decimal, error) {
	switch value.Kind() {
	case types.ValueKindDecimal:
		decimal, ok := value.Raw().(types.Decimal)
		if !ok {
			return types.Decimal{}, fmt.Errorf("%w: invalid DECIMAL payload", ErrTypeMismatch)
		}
		return decimal, nil
	case types.ValueKindInt16, types.ValueKindInt32, types.ValueKindInt64:
		n, err := exactInt64(value)
		if err != nil {
			return types.Decimal{}, err
		}
		return types.NewDecimalFromInt64(n), nil
	default:
		return types.Decimal{}, fmt.Errorf("%w: expected DECIMAL-compatible value, got %s", ErrTypeMismatch, value.Kind())
	}
}

func bitIsSet(bitmap []byte, index int) bool {
	if index < 0 {
		return false
	}
	byteIndex := index / 8
	if byteIndex >= len(bitmap) {
		return false
	}
	return bitmap[byteIndex]&(1<<(index%8)) != 0
}

func ioShortValue(kind types.TypeKind, need, have int) error {
	return fmt.Errorf("paged: %s value truncated: need %d bytes have %d", kind, need, have)
}
