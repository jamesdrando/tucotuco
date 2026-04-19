package types

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	// ErrInvalidCast reports malformed cast metadata or unsupported cast pairs.
	ErrInvalidCast = errors.New("types: invalid cast")
	// ErrCastFailure reports a runtime conversion failure for an otherwise valid
	// CAST/TRY_CAST operation.
	ErrCastFailure = errors.New("types: cast failed")
	// ErrCastOverflow reports a CAST/TRY_CAST failure caused by target range,
	// precision, or length limits.
	ErrCastOverflow = errors.New("types: cast overflow")
)

var (
	minInt16Big = big.NewInt(math.MinInt16)
	maxInt16Big = big.NewInt(math.MaxInt16)
	minInt32Big = big.NewInt(math.MinInt32)
	maxInt32Big = big.NewInt(math.MaxInt32)
	minInt64Big = big.NewInt(math.MinInt64)
	maxInt64Big = big.NewInt(math.MaxInt64)
)

// Cast converts value from source to target using the currently supported
// executor-visible SQL runtime conversions.
//
// NULL inputs always return NULL. For non-NULL inputs, source must describe the
// runtime representation accurately because multiple SQL type families share the
// same Go backing type.
func Cast(value Value, source, target TypeDesc) (Value, error) {
	if err := validateCastTarget(target); err != nil {
		return NullValue(), err
	}
	if value.IsNull() {
		return NullValue(), nil
	}
	if err := validateCastSource(value, source); err != nil {
		return NullValue(), err
	}

	return castValue(value, source, target)
}

// TryCast converts value from source to target like Cast, but returns NULL for
// runtime conversion failures instead of surfacing those failures as errors.
//
// Invalid descriptors and unsupported cast definitions still return errors so
// missing implementation or analyzer bugs do not get silently hidden.
func TryCast(value Value, source, target TypeDesc) (Value, error) {
	casted, err := Cast(value, source, target)
	if err != nil {
		if errors.Is(err, ErrCastFailure) {
			return NullValue(), nil
		}

		return NullValue(), err
	}

	return casted, nil
}

func validateCastTarget(target TypeDesc) error {
	if isUnknownTypeDesc(target) {
		return invalidCastf("missing target type")
	}
	if err := target.Validate(); err != nil {
		return invalidCastf("invalid target type %q: %v", target, err)
	}

	return nil
}

func validateCastSource(value Value, source TypeDesc) error {
	if isUnknownTypeDesc(source) {
		return invalidCastf("missing source type for %s value", value.Kind())
	}
	if err := source.Validate(); err != nil {
		return invalidCastf("invalid source type %q: %v", source, err)
	}
	if !valueMatchesTypeDesc(value, source) {
		return invalidCastf("%s value does not match source type %s", value.Kind(), source.Kind)
	}
	if err := validateSourceValueShape(value, source); err != nil {
		return err
	}

	return nil
}

func validateSourceValueShape(value Value, source TypeDesc) error {
	switch source.Kind {
	case TypeKindChar, TypeKindVarChar:
		if characterLength(value.Raw().(string)) > int(source.Length) {
			return invalidCastf("%q exceeds source %s", value.Raw().(string), source.format())
		}
	case TypeKindBinary, TypeKindVarBinary:
		if len(value.Raw().([]byte)) > int(source.Length) {
			return invalidCastf("binary source length %d exceeds %s", len(value.Raw().([]byte)), source.format())
		}
	case TypeKindDate:
		if !matchesNormalizedUTCDateTime(value.Raw().(time.Time), normalizeDate(value.Raw().(time.Time))) {
			return invalidCastf("DATE source value is not normalized for %s", source.format())
		}
	case TypeKindTime:
		timeOfDay := value.Raw().(time.Duration)
		if !isNormalizedTimeOfDay(timeOfDay, source.Precision) {
			return invalidCastf("TIME source value is not normalized for %s", source.format())
		}
	case TypeKindTimeWithTimeZone:
		if !matchesWallClockAndOffset(value.Raw().(time.Time), normalizeTimeWithTimeZone(value.Raw().(time.Time), source.Precision)) {
			return invalidCastf("TIME WITH TIME ZONE source value is not normalized for %s", source.format())
		}
	case TypeKindTimestamp:
		if !matchesNormalizedUTCDateTime(value.Raw().(time.Time), normalizeTimestamp(value.Raw().(time.Time), source.Precision)) {
			return invalidCastf("TIMESTAMP source value is not normalized for %s", source.format())
		}
	case TypeKindTimestampWithTimeZone:
		if !matchesWallClockAndOffset(value.Raw().(time.Time), normalizeTimestampWithTimeZone(value.Raw().(time.Time), source.Precision)) {
			return invalidCastf("TIMESTAMP WITH TIME ZONE source value is not normalized for %s", source.format())
		}
	}

	return nil
}

func valueMatchesTypeDesc(value Value, desc TypeDesc) bool {
	switch desc.Kind {
	case TypeKindSmallInt:
		return value.Kind() == ValueKindInt16
	case TypeKindInteger:
		return value.Kind() == ValueKindInt32
	case TypeKindBigInt:
		return value.Kind() == ValueKindInt64
	case TypeKindNumeric, TypeKindDecimal:
		return value.Kind() == ValueKindDecimal
	case TypeKindBoolean:
		return value.Kind() == ValueKindBool
	case TypeKindReal:
		return value.Kind() == ValueKindFloat32
	case TypeKindDoublePrecision:
		return value.Kind() == ValueKindFloat64
	case TypeKindChar, TypeKindVarChar, TypeKindText, TypeKindCLOB:
		return value.Kind() == ValueKindString
	case TypeKindBinary, TypeKindVarBinary, TypeKindBLOB:
		return value.Kind() == ValueKindBytes
	case TypeKindDate, TypeKindTimeWithTimeZone, TypeKindTimestamp, TypeKindTimestampWithTimeZone:
		return value.Kind() == ValueKindDateTime
	case TypeKindTime:
		return value.Kind() == ValueKindTimeOfDay
	case TypeKindInterval:
		return value.Kind() == ValueKindInterval
	case TypeKindArray:
		return value.Kind() == ValueKindArray
	case TypeKindRow:
		return value.Kind() == ValueKindRow
	default:
		return false
	}
}

func castValue(value Value, source, target TypeDesc) (Value, error) {
	switch {
	case isCharacterKind(target.Kind):
		return castToCharacter(value, source, target)
	case isBinaryKind(target.Kind):
		return castToBinary(value, source, target)
	case target.Kind == TypeKindBoolean:
		return castToBoolean(value, source)
	case isExactNumericKind(target.Kind) || isApproximateNumericKind(target.Kind):
		return castToNumeric(value, source, target)
	case isDateTimeKind(target.Kind) || isTimeOfDayKind(target.Kind):
		return castToTemporal(value, source, target)
	}

	switch target.Kind {
	case TypeKindInterval:
		if source.Kind != TypeKindInterval {
			return Value{}, invalidCastf("cannot cast %s to %s", source.Kind, target.Kind)
		}
		return IntervalValue(value.Raw().(Interval)), nil
	case TypeKindArray:
		if source.Kind != TypeKindArray {
			return Value{}, invalidCastf("cannot cast %s to %s", source.Kind, target.Kind)
		}
		return ArrayValue(value.Raw().(Array)), nil
	case TypeKindRow:
		if source.Kind != TypeKindRow {
			return Value{}, invalidCastf("cannot cast %s to %s", source.Kind, target.Kind)
		}
		return RowValue(value.Raw().(Row)), nil
	default:
		return Value{}, invalidCastf("cannot cast %s to %s", source.Kind, target.Kind)
	}
}

func castToCharacter(value Value, source, target TypeDesc) (Value, error) {
	var (
		text string
		err  error
	)

	switch {
	case isCharacterKind(source.Kind):
		text = value.Raw().(string)
	case isExactNumericKind(source.Kind):
		text = formatExactNumericText(value)
	case isApproximateNumericKind(source.Kind):
		text, err = formatApproxNumericText(value)
	case source.Kind == TypeKindBoolean:
		text = formatBooleanText(value.Raw().(bool))
	case isDateTimeKind(source.Kind) || isTimeOfDayKind(source.Kind):
		text = formatTemporalText(value, source)
	default:
		return Value{}, invalidCastf("cannot cast %s to %s", source.Kind, target.Kind)
	}
	if err != nil {
		return Value{}, err
	}

	text, err = applyCharacterConstraints(text, target)
	if err != nil {
		return Value{}, err
	}

	return StringValue(text), nil
}

func castToBinary(value Value, source, target TypeDesc) (Value, error) {
	if !isBinaryKind(source.Kind) {
		return Value{}, invalidCastf("cannot cast %s to %s", source.Kind, target.Kind)
	}

	bytes := value.Raw().([]byte)
	constrained, err := applyBinaryConstraints(bytes, target)
	if err != nil {
		return Value{}, err
	}

	return BytesValue(constrained), nil
}

func castToBoolean(value Value, source TypeDesc) (Value, error) {
	switch {
	case source.Kind == TypeKindBoolean:
		return BoolValue(value.Raw().(bool)), nil
	case isCharacterKind(source.Kind):
		text := strings.TrimSpace(value.Raw().(string))
		switch {
		case strings.EqualFold(text, "TRUE"):
			return BoolValue(true), nil
		case strings.EqualFold(text, "FALSE"):
			return BoolValue(false), nil
		default:
			return Value{}, castFailuref("cannot parse %q as BOOLEAN", text)
		}
	default:
		return Value{}, invalidCastf("cannot cast %s to BOOLEAN", source.Kind)
	}
}

func castToNumeric(value Value, source, target TypeDesc) (Value, error) {
	switch {
	case isExactIntegerKind(target.Kind):
		return castToExactInteger(value, source, target.Kind)
	case isExactDecimalKind(target.Kind):
		return castToExactDecimal(value, source, target)
	case isApproximateNumericKind(target.Kind):
		return castToApproximateNumeric(value, source, target.Kind)
	default:
		return Value{}, invalidCastf("cannot cast %s to %s", source.Kind, target.Kind)
	}
}

func castToExactInteger(value Value, source TypeDesc, targetKind TypeKind) (Value, error) {
	integer, err := integralBigInt(value, source)
	if err != nil {
		return Value{}, err
	}

	switch targetKind {
	case TypeKindSmallInt:
		if !fitsBigIntRange(integer, minInt16Big, maxInt16Big) {
			return Value{}, castOverflowf("%s does not fit SMALLINT", integer.String())
		}
		return Int16Value(int16(integer.Int64())), nil
	case TypeKindInteger:
		if !fitsBigIntRange(integer, minInt32Big, maxInt32Big) {
			return Value{}, castOverflowf("%s does not fit INTEGER", integer.String())
		}
		return Int32Value(int32(integer.Int64())), nil
	case TypeKindBigInt:
		if !fitsBigIntRange(integer, minInt64Big, maxInt64Big) {
			return Value{}, castOverflowf("%s does not fit BIGINT", integer.String())
		}
		return Int64Value(integer.Int64()), nil
	default:
		return Value{}, invalidCastf("cannot cast %s to %s", source.Kind, targetKind)
	}
}

func castToExactDecimal(value Value, source, target TypeDesc) (Value, error) {
	decimal, err := decimalForCast(value, source)
	if err != nil {
		return Value{}, err
	}
	if !decimalFitsTarget(decimal, target) {
		return Value{}, castOverflowf("%s does not fit %s", decimal.String(), target.format())
	}

	return DecimalValue(decimal), nil
}

func castToApproximateNumeric(value Value, source TypeDesc, targetKind TypeKind) (Value, error) {
	floatValue, err := float64ForCast(value, source)
	if err != nil {
		return Value{}, err
	}

	switch targetKind {
	case TypeKindReal:
		if math.IsInf(floatValue, 0) || (floatValue != 0 && math.Abs(floatValue) > math.MaxFloat32) {
			return Value{}, castOverflowf("%v does not fit REAL", floatValue)
		}
		return Float32Value(float32(floatValue)), nil
	case TypeKindDoublePrecision:
		if math.IsInf(floatValue, 0) || math.IsNaN(floatValue) {
			return Value{}, castOverflowf("%v does not fit DOUBLE PRECISION", floatValue)
		}
		return Float64Value(floatValue), nil
	default:
		return Value{}, invalidCastf("cannot cast %s to %s", source.Kind, targetKind)
	}
}

func castToTemporal(value Value, source, target TypeDesc) (Value, error) {
	switch target.Kind {
	case TypeKindDate:
		return castToDate(value, source)
	case TypeKindTime:
		return castToTime(value, source, target.Precision)
	case TypeKindTimeWithTimeZone:
		return castToTimeWithTimeZone(value, source, target.Precision)
	case TypeKindTimestamp:
		return castToTimestamp(value, source, target.Precision)
	case TypeKindTimestampWithTimeZone:
		return castToTimestampWithTimeZone(value, source, target.Precision)
	default:
		return Value{}, invalidCastf("cannot cast %s to %s", source.Kind, target.Kind)
	}
}

func castToDate(value Value, source TypeDesc) (Value, error) {
	switch {
	case source.Kind == TypeKindDate, source.Kind == TypeKindTimestamp, source.Kind == TypeKindTimestampWithTimeZone:
		return DateTimeValue(normalizeDate(value.Raw().(time.Time))), nil
	case isCharacterKind(source.Kind):
		parsed, err := parseDateText(value.Raw().(string))
		if err != nil {
			return Value{}, err
		}
		return DateTimeValue(parsed), nil
	default:
		return Value{}, invalidCastf("cannot cast %s to DATE", source.Kind)
	}
}

func castToTime(value Value, source TypeDesc, precision uint32) (Value, error) {
	switch {
	case source.Kind == TypeKindTime:
		return TimeOfDayValue(normalizeTimeOfDay(value.Raw().(time.Duration), precision)), nil
	case source.Kind == TypeKindTimeWithTimeZone:
		return TimeOfDayValue(normalizeTimeOfDay(timeOfDayFromTime(value.Raw().(time.Time)), precision)), nil
	case isCharacterKind(source.Kind):
		parsed, err := parseTimeText(value.Raw().(string), precision)
		if err != nil {
			return Value{}, err
		}
		return TimeOfDayValue(parsed), nil
	default:
		return Value{}, invalidCastf("cannot cast %s to TIME", source.Kind)
	}
}

func castToTimeWithTimeZone(value Value, source TypeDesc, precision uint32) (Value, error) {
	switch {
	case source.Kind == TypeKindTime:
		return DateTimeValue(normalizeTimeWithTimeZone(timeWithTimeZoneFromDuration(value.Raw().(time.Duration), time.UTC), precision)), nil
	case source.Kind == TypeKindTimeWithTimeZone:
		return DateTimeValue(normalizeTimeWithTimeZone(value.Raw().(time.Time), precision)), nil
	case isCharacterKind(source.Kind):
		parsed, err := parseTimeWithTimeZoneText(value.Raw().(string), precision)
		if err != nil {
			return Value{}, err
		}
		return DateTimeValue(parsed), nil
	default:
		return Value{}, invalidCastf("cannot cast %s to TIME WITH TIME ZONE", source.Kind)
	}
}

func castToTimestamp(value Value, source TypeDesc, precision uint32) (Value, error) {
	switch {
	case source.Kind == TypeKindDate, source.Kind == TypeKindTimestamp:
		return DateTimeValue(normalizeTimestamp(value.Raw().(time.Time), precision)), nil
	case source.Kind == TypeKindTimestampWithTimeZone:
		return DateTimeValue(normalizeTimestamp(value.Raw().(time.Time).UTC(), precision)), nil
	case isCharacterKind(source.Kind):
		parsed, err := parseTimestampText(value.Raw().(string), precision)
		if err != nil {
			return Value{}, err
		}
		return DateTimeValue(parsed), nil
	default:
		return Value{}, invalidCastf("cannot cast %s to TIMESTAMP", source.Kind)
	}
}

func castToTimestampWithTimeZone(value Value, source TypeDesc, precision uint32) (Value, error) {
	switch {
	case source.Kind == TypeKindDate, source.Kind == TypeKindTimestamp:
		return DateTimeValue(normalizeTimestampWithTimeZone(value.Raw().(time.Time).UTC(), precision)), nil
	case source.Kind == TypeKindTimestampWithTimeZone:
		return DateTimeValue(normalizeTimestampWithTimeZone(value.Raw().(time.Time), precision)), nil
	case isCharacterKind(source.Kind):
		parsed, err := parseTimestampWithTimeZoneText(value.Raw().(string), precision)
		if err != nil {
			return Value{}, err
		}
		return DateTimeValue(parsed), nil
	default:
		return Value{}, invalidCastf("cannot cast %s to TIMESTAMP WITH TIME ZONE", source.Kind)
	}
}

func applyCharacterConstraints(text string, target TypeDesc) (string, error) {
	length := characterLength(text)

	switch target.Kind {
	case TypeKindChar:
		if length > int(target.Length) {
			return "", castOverflowf("%q exceeds %s", text, target.format())
		}
		if length < int(target.Length) {
			text += strings.Repeat(" ", int(target.Length)-length)
		}
	case TypeKindVarChar:
		if length > int(target.Length) {
			return "", castOverflowf("%q exceeds %s", text, target.format())
		}
	case TypeKindText, TypeKindCLOB:
	default:
		return "", invalidCastf("cannot cast to %s", target.Kind)
	}

	return text, nil
}

func applyBinaryConstraints(value []byte, target TypeDesc) ([]byte, error) {
	bytes := append([]byte(nil), value...)

	switch target.Kind {
	case TypeKindBinary:
		if len(bytes) > int(target.Length) {
			return nil, castOverflowf("binary value length %d exceeds %s", len(bytes), target.format())
		}
		if len(bytes) < int(target.Length) {
			bytes = append(bytes, make([]byte, int(target.Length)-len(bytes))...)
		}
	case TypeKindVarBinary:
		if len(bytes) > int(target.Length) {
			return nil, castOverflowf("binary value length %d exceeds %s", len(bytes), target.format())
		}
	case TypeKindBLOB:
	default:
		return nil, invalidCastf("cannot cast to %s", target.Kind)
	}

	return bytes, nil
}

func formatExactNumericText(value Value) string {
	switch value.Kind() {
	case ValueKindInt16:
		return strconv.FormatInt(int64(value.Raw().(int16)), 10)
	case ValueKindInt32:
		return strconv.FormatInt(int64(value.Raw().(int32)), 10)
	case ValueKindInt64:
		return strconv.FormatInt(value.Raw().(int64), 10)
	case ValueKindDecimal:
		return value.Raw().(Decimal).String()
	default:
		return ""
	}
}

func formatApproxNumericText(value Value) (string, error) {
	switch value.Kind() {
	case ValueKindFloat32:
		floatValue := float64(value.Raw().(float32))
		if math.IsNaN(floatValue) || math.IsInf(floatValue, 0) {
			return "", castFailuref("cannot format non-finite REAL value")
		}
		return strconv.FormatFloat(floatValue, 'g', -1, 32), nil
	case ValueKindFloat64:
		floatValue := value.Raw().(float64)
		if math.IsNaN(floatValue) || math.IsInf(floatValue, 0) {
			return "", castFailuref("cannot format non-finite DOUBLE PRECISION value")
		}
		return strconv.FormatFloat(floatValue, 'g', -1, 64), nil
	default:
		return "", invalidCastf("cannot format %s as character text", value.Kind())
	}
}

func formatBooleanText(value bool) string {
	if value {
		return "TRUE"
	}

	return "FALSE"
}

func formatTemporalText(value Value, source TypeDesc) string {
	switch source.Kind {
	case TypeKindDate:
		return formatDateText(normalizeDate(value.Raw().(time.Time)))
	case TypeKindTime:
		return formatTimeOfDayText(normalizeTimeOfDay(value.Raw().(time.Duration), source.Precision), source.Precision)
	case TypeKindTimeWithTimeZone:
		return formatTimeWithTimeZoneText(normalizeTimeWithTimeZone(value.Raw().(time.Time), source.Precision), source.Precision)
	case TypeKindTimestamp:
		return formatTimestampText(normalizeTimestamp(value.Raw().(time.Time), source.Precision), source.Precision, false)
	case TypeKindTimestampWithTimeZone:
		return formatTimestampText(normalizeTimestampWithTimeZone(value.Raw().(time.Time), source.Precision), source.Precision, true)
	default:
		return ""
	}
}

func decimalForCast(value Value, source TypeDesc) (Decimal, error) {
	switch {
	case isCharacterKind(source.Kind):
		text := strings.TrimSpace(value.Raw().(string))
		decimal, err := ParseDecimal(text)
		if err != nil {
			return Decimal{}, castFailuref("cannot parse %q as %s", text, source.Kind)
		}
		return decimal, nil
	case isApproximateNumericKind(source.Kind):
		text, err := formatApproxNumericText(value)
		if err != nil {
			return Decimal{}, err
		}

		decimal, err := ParseDecimal(text)
		if err != nil {
			return Decimal{}, castFailuref("cannot convert %s value %q to DECIMAL", source.Kind, text)
		}

		return decimal, nil
	case isExactNumericKind(source.Kind):
		rat, err := numericRatForCast(value, source)
		if err != nil {
			return Decimal{}, err
		}
		return decimalFromRat(rat)
	default:
		return Decimal{}, invalidCastf("cannot cast %s to DECIMAL", source.Kind)
	}
}

func float64ForCast(value Value, source TypeDesc) (float64, error) {
	switch {
	case isCharacterKind(source.Kind):
		text := strings.TrimSpace(value.Raw().(string))
		floatValue, err := strconv.ParseFloat(text, 64)
		if err != nil || math.IsNaN(floatValue) || math.IsInf(floatValue, 0) {
			return 0, castFailuref("cannot parse %q as %s", text, source.Kind)
		}
		return floatValue, nil
	case isExactNumericKind(source.Kind):
		switch value.Kind() {
		case ValueKindInt16:
			return float64(value.Raw().(int16)), nil
		case ValueKindInt32:
			return float64(value.Raw().(int32)), nil
		case ValueKindInt64:
			return float64(value.Raw().(int64)), nil
		case ValueKindDecimal:
			floatValue, _ := new(big.Float).SetRat(value.Raw().(Decimal).rat()).Float64()
			if math.IsNaN(floatValue) || math.IsInf(floatValue, 0) {
				return 0, castOverflowf("%s does not fit floating point", value.Raw().(Decimal).String())
			}
			return floatValue, nil
		default:
			return 0, invalidCastf("cannot cast %s to floating point", source.Kind)
		}
	case source.Kind == TypeKindReal:
		floatValue := float64(value.Raw().(float32))
		if math.IsNaN(floatValue) || math.IsInf(floatValue, 0) {
			return 0, castFailuref("cannot cast non-finite REAL value")
		}
		return floatValue, nil
	case source.Kind == TypeKindDoublePrecision:
		floatValue := value.Raw().(float64)
		if math.IsNaN(floatValue) || math.IsInf(floatValue, 0) {
			return 0, castFailuref("cannot cast non-finite DOUBLE PRECISION value")
		}
		return floatValue, nil
	default:
		return 0, invalidCastf("cannot cast %s to floating point", source.Kind)
	}
}

func integralBigInt(value Value, source TypeDesc) (*big.Int, error) {
	switch {
	case isCharacterKind(source.Kind):
		decimal, err := decimalForCast(value, source)
		if err != nil {
			return nil, err
		}

		return integralBigIntFromRat(decimal.rat(), source)
	case isExactNumericKind(source.Kind), isApproximateNumericKind(source.Kind):
		rat, err := numericRatForCast(value, source)
		if err != nil {
			return nil, err
		}

		return integralBigIntFromRat(rat, source)
	default:
		return nil, invalidCastf("cannot cast %s to integer", source.Kind)
	}
}

func integralBigIntFromRat(rat *big.Rat, source TypeDesc) (*big.Int, error) {
	if !rat.IsInt() {
		return nil, castFailuref("%s value is not an exact integer", source.Kind)
	}

	return new(big.Int).Set(rat.Num()), nil
}

func numericRatForCast(value Value, source TypeDesc) (*big.Rat, error) {
	rat, err := numericRat(value)
	if err == nil {
		return rat, nil
	}

	return nil, castFailuref("cannot cast %s value: %v", source.Kind, err)
}

func decimalFromRat(rat *big.Rat) (Decimal, error) {
	numerator := new(big.Int).Set(rat.Num())
	denominator := new(big.Int).Set(rat.Denom())
	if denominator.Sign() == 0 {
		return Decimal{}, castFailuref("cannot convert rational with zero denominator")
	}

	scale := int64(0)
	remainder := new(big.Int)
	two := big.NewInt(2)
	five := big.NewInt(5)
	one := big.NewInt(1)
	for denominator.Cmp(one) != 0 {
		remainder.Mod(denominator, two)
		switch {
		case remainder.Sign() == 0:
			denominator.Quo(denominator, two)
			numerator.Mul(numerator, five)
			scale++
		default:
			remainder.Mod(denominator, five)
			if remainder.Sign() != 0 {
				return Decimal{}, castFailuref("cannot represent rational %s exactly as DECIMAL", rat.RatString())
			}
			denominator.Quo(denominator, five)
			numerator.Mul(numerator, two)
			scale++
		}
		if scale > math.MaxInt32 {
			return Decimal{}, castOverflowf("decimal scale exceeds supported range")
		}
	}

	decimal, err := NewDecimal(numerator, int32(scale))
	if err != nil {
		return Decimal{}, castFailuref("cannot construct DECIMAL: %v", err)
	}

	return decimal, nil
}

func decimalFitsTarget(value Decimal, target TypeDesc) bool {
	if target.Precision == 0 {
		return true
	}

	actual := actualDecimalEnvelope(value)
	expected := decimalEnvelopeOf(target)
	return actual.fitsIn(expected)
}

func actualDecimalEnvelope(value Decimal) decimalEnvelope {
	normalized := normalizeDecimal(value)
	if normalized.Sign() == 0 {
		return decimalEnvelope{}
	}

	digits := uint32(len(new(big.Int).Abs(normalized.Coefficient()).String()))
	scale := normalized.Scale()
	if scale < 0 {
		return decimalEnvelope{
			integerDigits: digits + uint32(-scale),
			scale:         0,
		}
	}

	integerDigits := uint32(0)
	if digits > uint32(scale) {
		integerDigits = digits - uint32(scale)
	}

	return decimalEnvelope{
		integerDigits: integerDigits,
		scale:         uint32(scale),
	}
}

func fitsBigIntRange(value, minValue, maxValue *big.Int) bool {
	return value.Cmp(minValue) >= 0 && value.Cmp(maxValue) <= 0
}

func parseDateText(text string) (time.Time, error) {
	parsed, err := time.ParseInLocation("2006-01-02", strings.TrimSpace(text), time.UTC)
	if err != nil {
		return time.Time{}, castFailuref("cannot parse %q as DATE", text)
	}

	return normalizeDate(parsed), nil
}

func parseTimeText(text string, precision uint32) (time.Duration, error) {
	parsed, err := parseTimeOfDay(strings.TrimSpace(text))
	if err != nil {
		return 0, castFailuref("cannot parse %q as TIME", text)
	}

	return normalizeTimeOfDay(parsed, precision), nil
}

func parseTimeWithTimeZoneText(text string, precision uint32) (time.Time, error) {
	layouts := []string{
		"15:04:05.999999999Z07:00",
		"15:04:05Z07:00",
	}
	trimmed := strings.TrimSpace(text)
	for _, layout := range layouts {
		parsed, err := time.Parse(layout, trimmed)
		if err == nil {
			return normalizeTimeWithTimeZone(parsed, precision), nil
		}
	}

	return time.Time{}, castFailuref("cannot parse %q as TIME WITH TIME ZONE", text)
}

func parseTimestampText(text string, precision uint32) (time.Time, error) {
	layouts := []string{
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
	}
	trimmed := strings.TrimSpace(text)
	for _, layout := range layouts {
		parsed, err := time.ParseInLocation(layout, trimmed, time.UTC)
		if err == nil {
			return normalizeTimestamp(parsed, precision), nil
		}
	}

	return time.Time{}, castFailuref("cannot parse %q as TIMESTAMP", text)
}

func parseTimestampWithTimeZoneText(text string, precision uint32) (time.Time, error) {
	layouts := []string{
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02T15:04:05.999999999Z07:00",
		"2006-01-02T15:04:05Z07:00",
		time.RFC3339Nano,
	}
	trimmed := strings.TrimSpace(text)
	for _, layout := range layouts {
		parsed, err := time.Parse(layout, trimmed)
		if err == nil {
			return normalizeTimestampWithTimeZone(parsed, precision), nil
		}
	}

	return time.Time{}, castFailuref("cannot parse %q as TIMESTAMP WITH TIME ZONE", text)
}

func parseTimeOfDay(text string) (time.Duration, error) {
	layouts := []string{
		"15:04:05.999999999",
		"15:04:05",
	}

	for _, layout := range layouts {
		parsed, err := time.Parse(layout, text)
		if err == nil {
			return timeOfDayFromTime(parsed), nil
		}
	}

	return 0, errors.New("parse time of day")
}

func normalizeDate(value time.Time) time.Time {
	year, month, day := value.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func normalizeTimestamp(value time.Time, precision uint32) time.Time {
	utc := value.UTC()
	return time.Date(
		utc.Year(),
		utc.Month(),
		utc.Day(),
		utc.Hour(),
		utc.Minute(),
		utc.Second(),
		truncateSubsecond(utc.Nanosecond(), precision),
		time.UTC,
	)
}

func normalizeTimestampWithTimeZone(value time.Time, precision uint32) time.Time {
	return time.Date(
		value.Year(),
		value.Month(),
		value.Day(),
		value.Hour(),
		value.Minute(),
		value.Second(),
		truncateSubsecond(value.Nanosecond(), precision),
		value.Location(),
	)
}

func normalizeTimeWithTimeZone(value time.Time, precision uint32) time.Time {
	return time.Date(
		1,
		time.January,
		1,
		value.Hour(),
		value.Minute(),
		value.Second(),
		truncateSubsecond(value.Nanosecond(), precision),
		value.Location(),
	)
}

func normalizeTimeOfDay(value time.Duration, precision uint32) time.Duration {
	unit := time.Duration(precisionUnitNanos(precision))
	return value - time.Duration(value.Nanoseconds()%int64(unit))
}

func isNormalizedTimeOfDay(value time.Duration, precision uint32) bool {
	return value >= 0 && value < 24*time.Hour && value == normalizeTimeOfDay(value, precision)
}

func matchesNormalizedUTCDateTime(value, normalized time.Time) bool {
	_, offsetSeconds := value.Zone()
	return offsetSeconds == 0 && sameDateTimeFields(value, normalized)
}

func matchesWallClockAndOffset(value, normalized time.Time) bool {
	_, valueOffsetSeconds := value.Zone()
	_, normalizedOffsetSeconds := normalized.Zone()
	return valueOffsetSeconds == normalizedOffsetSeconds && sameDateTimeFields(value, normalized)
}

func sameDateTimeFields(left, right time.Time) bool {
	return left.Year() == right.Year() &&
		left.Month() == right.Month() &&
		left.Day() == right.Day() &&
		left.Hour() == right.Hour() &&
		left.Minute() == right.Minute() &&
		left.Second() == right.Second() &&
		left.Nanosecond() == right.Nanosecond()
}

func characterLength(text string) int {
	return utf8.RuneCountInString(text)
}

func timeOfDayFromTime(value time.Time) time.Duration {
	return time.Duration(value.Hour())*time.Hour +
		time.Duration(value.Minute())*time.Minute +
		time.Duration(value.Second())*time.Second +
		time.Duration(value.Nanosecond())
}

func timeWithTimeZoneFromDuration(value time.Duration, location *time.Location) time.Time {
	value = normalizeTimeOfDay(value, 9)
	hours := int(value / time.Hour)
	value -= time.Duration(hours) * time.Hour
	minutes := int(value / time.Minute)
	value -= time.Duration(minutes) * time.Minute
	seconds := int(value / time.Second)
	value -= time.Duration(seconds) * time.Second

	return time.Date(1, time.January, 1, hours, minutes, seconds, int(value), location)
}

func formatDateText(value time.Time) string {
	return fmt.Sprintf("%04d-%02d-%02d", value.Year(), int(value.Month()), value.Day())
}

func formatTimeOfDayText(value time.Duration, precision uint32) string {
	hour, minute, second, nanos := clockFromDuration(value)
	return formatClockText(hour, minute, second, nanos, precision)
}

func formatTimeWithTimeZoneText(value time.Time, precision uint32) string {
	return formatClockText(value.Hour(), value.Minute(), value.Second(), value.Nanosecond(), precision) + formatOffset(value)
}

func formatTimestampText(value time.Time, precision uint32, withTimeZone bool) string {
	text := formatDateText(value) + " " + formatClockText(value.Hour(), value.Minute(), value.Second(), value.Nanosecond(), precision)
	if withTimeZone {
		text += formatOffset(value)
	}

	return text
}

func formatClockText(hour, minute, second, nanos int, precision uint32) string {
	text := fmt.Sprintf("%02d:%02d:%02d", hour, minute, second)
	precision = clampTemporalPrecision(precision)
	if precision == 0 {
		return text
	}

	fractional := fmt.Sprintf("%09d", truncateSubsecond(nanos, precision))
	return text + "." + fractional[:precision]
}

func formatOffset(value time.Time) string {
	_, offsetSeconds := value.Zone()
	sign := "+"
	if offsetSeconds < 0 {
		sign = "-"
		offsetSeconds = -offsetSeconds
	}

	return fmt.Sprintf("%s%02d:%02d", sign, offsetSeconds/3600, (offsetSeconds%3600)/60)
}

func clockFromDuration(value time.Duration) (hour, minute, second, nanos int) {
	hour = int(value / time.Hour)
	value -= time.Duration(hour) * time.Hour
	minute = int(value / time.Minute)
	value -= time.Duration(minute) * time.Minute
	second = int(value / time.Second)
	value -= time.Duration(second) * time.Second
	nanos = int(value)

	return hour, minute, second, nanos
}

func truncateSubsecond(nanos int, precision uint32) int {
	unit := precisionUnitNanos(precision)
	if unit <= 1 {
		return nanos
	}

	return nanos - (nanos % unit)
}

func precisionUnitNanos(precision uint32) int {
	precision = clampTemporalPrecision(precision)
	unit := 1
	for digits := precision; digits < 9; digits++ {
		unit *= 10
	}

	return unit
}

func clampTemporalPrecision(precision uint32) uint32 {
	if precision > 9 {
		return 9
	}

	return precision
}

func invalidCastf(format string, args ...any) error {
	return errors.Join(ErrInvalidCast, fmt.Errorf(format, args...))
}

func castFailuref(format string, args ...any) error {
	return errors.Join(ErrCastFailure, fmt.Errorf(format, args...))
}

func castOverflowf(format string, args ...any) error {
	return errors.Join(ErrCastFailure, ErrCastOverflow, fmt.Errorf(format, args...))
}
