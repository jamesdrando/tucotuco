package types

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"
	"strings"
	"time"
)

var (
	// ErrNullComparison reports an attempt to order NULL values.
	ErrNullComparison = errors.New("types: cannot compare NULL values")
	// ErrIncomparable reports a comparison between incompatible values.
	ErrIncomparable = errors.New("types: incomparable values")
	// ErrNonFiniteNumeric reports a comparison involving NaN or infinity.
	ErrNonFiniteNumeric = errors.New("types: non-finite numeric value")
)

// Array stores a SQL array as an ordered sequence of values.
type Array []Value

// Equal reports whether two arrays are element-wise equal.
func (a Array) Equal(other Array) bool {
	return equalValueSequences(a, other)
}

// Compare compares two arrays lexicographically.
func (a Array) Compare(other Array) (int, error) {
	return compareValueSequences(a, other)
}

// Row stores a SQL row constructor as an ordered sequence of values.
type Row []Value

// Equal reports whether two rows are element-wise equal.
func (r Row) Equal(other Row) bool {
	return equalValueSequences(r, other)
}

// Compare compares two rows lexicographically.
func (r Row) Compare(other Row) (int, error) {
	return compareValueSequences(r, other)
}

// ValueKind identifies the concrete representation stored in a Value.
type ValueKind uint8

const (
	ValueKindNull ValueKind = iota
	ValueKindBool
	ValueKindInt16
	ValueKindInt32
	ValueKindInt64
	ValueKindFloat32
	ValueKindFloat64
	ValueKindString
	ValueKindBytes
	ValueKindDateTime
	ValueKindTimeOfDay
	ValueKindInterval
	ValueKindDecimal
	ValueKindArray
	ValueKindRow
)

// ValueKindTime is kept as a compatibility alias for time.Time-backed
// date/timestamp values.
const ValueKindTime = ValueKindDateTime

var valueKindNames = [...]string{
	ValueKindNull:      "NULL",
	ValueKindBool:      "BOOL",
	ValueKindInt16:     "INT16",
	ValueKindInt32:     "INT32",
	ValueKindInt64:     "INT64",
	ValueKindFloat32:   "FLOAT32",
	ValueKindFloat64:   "FLOAT64",
	ValueKindString:    "STRING",
	ValueKindBytes:     "BYTES",
	ValueKindDateTime:  "DATETIME",
	ValueKindTimeOfDay: "TIME",
	ValueKindInterval:  "INTERVAL",
	ValueKindDecimal:   "DECIMAL",
	ValueKindArray:     "ARRAY",
	ValueKindRow:       "ROW",
}

// String returns the display name for a value kind.
func (k ValueKind) String() string {
	if int(k) < len(valueKindNames) {
		return valueKindNames[k]
	}

	return fmt.Sprintf("ValueKind(%d)", k)
}

// Value is the runtime representation of a SQL value.
type Value struct {
	kind  ValueKind
	value any
}

// NullValue constructs a NULL value.
func NullValue() Value {
	return Value{kind: ValueKindNull}
}

// BoolValue constructs a BOOLEAN value.
func BoolValue(value bool) Value {
	return Value{kind: ValueKindBool, value: value}
}

// Int16Value constructs a SMALLINT value.
func Int16Value(value int16) Value {
	return Value{kind: ValueKindInt16, value: value}
}

// Int32Value constructs an INTEGER value.
func Int32Value(value int32) Value {
	return Value{kind: ValueKindInt32, value: value}
}

// Int64Value constructs a BIGINT value.
func Int64Value(value int64) Value {
	return Value{kind: ValueKindInt64, value: value}
}

// Float32Value constructs a REAL value.
func Float32Value(value float32) Value {
	return Value{kind: ValueKindFloat32, value: value}
}

// Float64Value constructs a DOUBLE PRECISION value.
func Float64Value(value float64) Value {
	return Value{kind: ValueKindFloat64, value: value}
}

// StringValue constructs a character string value.
func StringValue(value string) Value {
	return Value{kind: ValueKindString, value: value}
}

// BytesValue constructs a binary string value.
func BytesValue(value []byte) Value {
	return Value{kind: ValueKindBytes, value: bytes.Clone(value)}
}

// DateTimeValue constructs a time.Time-backed value for DATE, TIME WITH TIME
// ZONE, and TIMESTAMP-family SQL values.
func DateTimeValue(value time.Time) Value {
	return Value{kind: ValueKindDateTime, value: value.Round(0)}
}

// TimeValue constructs a time.Time-backed temporal value. It is retained for
// compatibility; new code should prefer DateTimeValue.
func TimeValue(value time.Time) Value {
	return DateTimeValue(value)
}

// TimeOfDayValue constructs a SQL TIME value backed by time.Duration since
// midnight.
func TimeOfDayValue(value time.Duration) Value {
	return Value{kind: ValueKindTimeOfDay, value: value}
}

// IntervalValue constructs an INTERVAL value.
func IntervalValue(value Interval) Value {
	return Value{kind: ValueKindInterval, value: value.Normalize()}
}

// DecimalValue constructs a DECIMAL value.
func DecimalValue(value Decimal) Value {
	return Value{kind: ValueKindDecimal, value: normalizeDecimal(value)}
}

// ArrayValue constructs an ARRAY value.
func ArrayValue(value Array) Value {
	return Value{kind: ValueKindArray, value: slices.Clone(value)}
}

// RowValue constructs a ROW value.
func RowValue(value Row) Value {
	return Value{kind: ValueKindRow, value: slices.Clone(value)}
}

// Kind reports the stored value kind.
func (v Value) Kind() ValueKind {
	return v.kind
}

// Raw returns the Go value for the stored representation. Mutable containers
// are cloned so callers cannot mutate the stored value through the returned
// data.
func (v Value) Raw() any {
	switch v.kind {
	case ValueKindNull:
		return nil
	case ValueKindBytes:
		return bytes.Clone(v.value.([]byte))
	case ValueKindArray:
		return Array(slices.Clone(v.value.(Array)))
	case ValueKindRow:
		return Row(slices.Clone(v.value.(Row)))
	default:
		return v.value
	}
}

// IsNull reports whether the value is NULL.
func (v Value) IsNull() bool {
	return v.kind == ValueKindNull
}

// Equal reports structural equality for runtime values. This is not SQL's
// three-valued `=` operator: two NULL values compare equal here so values can
// be compared inside containers and tests.
func (v Value) Equal(other Value) bool {
	switch {
	case v.IsNull() || other.IsNull():
		return v.IsNull() && other.IsNull()
	case v.kind == ValueKindFloat32 && other.kind == ValueKindFloat32:
		return equalFloat32(v.value.(float32), other.value.(float32))
	case v.kind == ValueKindFloat64 && other.kind == ValueKindFloat64:
		return equalFloat64(v.value.(float64), other.value.(float64))
	case isNumericKind(v.kind) && isNumericKind(other.kind):
		comparison, err := compareNumericValues(v, other)
		return err == nil && comparison == 0
	case v.kind != other.kind:
		return false
	}

	switch v.kind {
	case ValueKindBool:
		return v.value.(bool) == other.value.(bool)
	case ValueKindString:
		return v.value.(string) == other.value.(string)
	case ValueKindBytes:
		return bytes.Equal(v.value.([]byte), other.value.([]byte))
	case ValueKindDateTime:
		return v.value.(time.Time).Equal(other.value.(time.Time))
	case ValueKindTimeOfDay:
		return v.value.(time.Duration) == other.value.(time.Duration)
	case ValueKindInterval:
		return v.value.(Interval).Equal(other.value.(Interval))
	case ValueKindDecimal:
		return v.value.(Decimal).Equal(other.value.(Decimal))
	case ValueKindArray:
		return v.value.(Array).Equal(other.value.(Array))
	case ValueKindRow:
		return v.value.(Row).Equal(other.value.(Row))
	case ValueKindFloat32:
		return equalFloat32(v.value.(float32), other.value.(float32))
	case ValueKindFloat64:
		return equalFloat64(v.value.(float64), other.value.(float64))
	case ValueKindInt16:
		return v.value.(int16) == other.value.(int16)
	case ValueKindInt32:
		return v.value.(int32) == other.value.(int32)
	case ValueKindInt64:
		return v.value.(int64) == other.value.(int64)
	default:
		return false
	}
}

// Compare orders two values. Values must be non-NULL and comparable.
func (v Value) Compare(other Value) (int, error) {
	switch {
	case v.IsNull() || other.IsNull():
		return 0, ErrNullComparison
	case isNumericKind(v.kind) && isNumericKind(other.kind):
		return compareNumericValues(v, other)
	case v.kind != other.kind:
		return 0, fmt.Errorf("%w: %s and %s", ErrIncomparable, v.kind, other.kind)
	}

	switch v.kind {
	case ValueKindBool:
		return compareBool(v.value.(bool), other.value.(bool)), nil
	case ValueKindString:
		return strings.Compare(v.value.(string), other.value.(string)), nil
	case ValueKindBytes:
		return bytes.Compare(v.value.([]byte), other.value.([]byte)), nil
	case ValueKindDateTime:
		return compareTime(v.value.(time.Time), other.value.(time.Time)), nil
	case ValueKindTimeOfDay:
		return compareDuration(v.value.(time.Duration), other.value.(time.Duration)), nil
	case ValueKindInterval:
		return v.value.(Interval).Compare(other.value.(Interval)), nil
	case ValueKindDecimal:
		return v.value.(Decimal).Compare(other.value.(Decimal)), nil
	case ValueKindArray:
		return v.value.(Array).Compare(other.value.(Array))
	case ValueKindRow:
		return v.value.(Row).Compare(other.value.(Row))
	default:
		return 0, fmt.Errorf("%w: %s", ErrIncomparable, v.kind)
	}
}

func compareBool(left, right bool) int {
	switch {
	case left == right:
		return 0
	case !left && right:
		return -1
	default:
		return 1
	}
}

func compareTime(left, right time.Time) int {
	switch {
	case left.Equal(right):
		return 0
	case left.Before(right):
		return -1
	default:
		return 1
	}
}

func compareDuration(left, right time.Duration) int {
	switch {
	case left == right:
		return 0
	case left < right:
		return -1
	default:
		return 1
	}
}

func compareNumericValues(left, right Value) (int, error) {
	leftRat, err := numericRat(left)
	if err != nil {
		return 0, err
	}

	rightRat, err := numericRat(right)
	if err != nil {
		return 0, err
	}

	return leftRat.Cmp(rightRat), nil
}

func numericRat(value Value) (*big.Rat, error) {
	switch value.kind {
	case ValueKindInt16:
		return new(big.Rat).SetInt64(int64(value.value.(int16))), nil
	case ValueKindInt32:
		return new(big.Rat).SetInt64(int64(value.value.(int32))), nil
	case ValueKindInt64:
		return new(big.Rat).SetInt64(value.value.(int64)), nil
	case ValueKindFloat32:
		return floatRat(float64(value.value.(float32)), value.kind)
	case ValueKindFloat64:
		return floatRat(value.value.(float64), value.kind)
	case ValueKindDecimal:
		return value.value.(Decimal).rat(), nil
	default:
		return nil, fmt.Errorf("%w: %s is not numeric", ErrIncomparable, value.kind)
	}
}

func floatRat(value float64, kind ValueKind) (*big.Rat, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil, fmt.Errorf("%w: %s", ErrNonFiniteNumeric, kind)
	}

	rat := new(big.Rat)
	if rat = rat.SetFloat64(value); rat == nil {
		return nil, fmt.Errorf("%w: %s", ErrNonFiniteNumeric, kind)
	}

	return rat, nil
}

func equalFloat32(left, right float32) bool {
	if math.IsNaN(float64(left)) && math.IsNaN(float64(right)) {
		return true
	}

	return left == right
}

func equalFloat64(left, right float64) bool {
	if math.IsNaN(left) && math.IsNaN(right) {
		return true
	}

	return left == right
}

func equalValueSequences[S ~[]Value](left, right S) bool {
	if len(left) != len(right) {
		return false
	}

	for index := range left {
		if !left[index].Equal(right[index]) {
			return false
		}
	}

	return true
}

func compareValueSequences[S ~[]Value](left, right S) (int, error) {
	for index := 0; index < min(len(left), len(right)); index++ {
		comparison, err := left[index].Compare(right[index])
		if err != nil {
			return 0, err
		}
		if comparison != 0 {
			return comparison, nil
		}
	}

	switch {
	case len(left) < len(right):
		return -1, nil
	case len(left) > len(right):
		return 1, nil
	default:
		return 0, nil
	}
}

func isNumericKind(kind ValueKind) bool {
	switch kind {
	case ValueKindInt16, ValueKindInt32, ValueKindInt64, ValueKindFloat32, ValueKindFloat64, ValueKindDecimal:
		return true
	default:
		return false
	}
}
