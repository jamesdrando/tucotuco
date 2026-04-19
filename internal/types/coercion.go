package types

// CanImplicitlyCoerce reports whether a value described by from can be used
// where to is expected without an explicit CAST.
//
// Type nullability is treated separately from the semantic type family. The
// zero-value TypeDesc is accepted as the untyped NULL/unknown sentinel so the
// analyzer can resolve contextual NULL expressions later.
func CanImplicitlyCoerce(from, to TypeDesc) bool {
	if isUnknownTypeDesc(from) {
		return true
	}
	if isUnknownTypeDesc(to) {
		return false
	}
	if err := from.Validate(); err != nil {
		return false
	}
	if err := to.Validate(); err != nil {
		return false
	}

	if from.Kind == to.Kind && sameSemanticType(from, to) {
		return true
	}

	switch {
	case isExactIntegerKind(from.Kind):
		return canCoerceExactInteger(from, to)
	case isExactDecimalKind(from.Kind):
		return canCoerceExactDecimal(from, to)
	case isApproximateNumericKind(from.Kind):
		return canCoerceApproximateNumeric(from, to)
	case isCharacterKind(from.Kind):
		return canCoerceCharacter(from, to)
	case isBinaryKind(from.Kind):
		return canCoerceBinary(from, to)
	case isDateTimeKind(from.Kind):
		return canCoerceDateTime(from, to)
	case isTimeOfDayKind(from.Kind):
		return canCoerceTimeOfDay(from, to)
	case from.Kind == TypeKindBoolean:
		return to.Kind == TypeKindBoolean
	case from.Kind == TypeKindJSON:
		return to.Kind == TypeKindJSON
	case from.Kind == TypeKindArray:
		return to.Kind == TypeKindArray
	case from.Kind == TypeKindRow:
		return to.Kind == TypeKindRow
	default:
		return false
	}
}

// CommonSuperType returns the least common supertype for left and right, when
// one exists.
//
// The returned descriptor preserves nullability by OR-ing the input nullability
// flags. The zero-value TypeDesc is treated as untyped NULL/unknown.
func CommonSuperType(left, right TypeDesc) (TypeDesc, bool) {
	if isUnknownTypeDesc(left) && isUnknownTypeDesc(right) {
		return TypeDesc{}, true
	}
	if isUnknownTypeDesc(left) {
		result := right
		result.Nullable = true
		return result, true
	}
	if isUnknownTypeDesc(right) {
		result := left
		result.Nullable = true
		return result, true
	}
	if err := left.Validate(); err != nil {
		return TypeDesc{}, false
	}
	if err := right.Validate(); err != nil {
		return TypeDesc{}, false
	}

	if left.Kind == right.Kind && sameSemanticType(left, right) {
		result := left
		result.Nullable = left.Nullable || right.Nullable
		return result, true
	}

	if result, ok := commonNumeric(left, right); ok {
		result.Nullable = left.Nullable || right.Nullable
		return result, true
	}
	if result, ok := commonCharacter(left, right); ok {
		result.Nullable = left.Nullable || right.Nullable
		return result, true
	}
	if result, ok := commonBinary(left, right); ok {
		result.Nullable = left.Nullable || right.Nullable
		return result, true
	}
	if result, ok := commonDateTime(left, right); ok {
		result.Nullable = left.Nullable || right.Nullable
		return result, true
	}
	if result, ok := commonTimeOfDay(left, right); ok {
		result.Nullable = left.Nullable || right.Nullable
		return result, true
	}
	if left.Kind == right.Kind {
		result := left
		result.Nullable = left.Nullable || right.Nullable
		return result, true
	}

	return TypeDesc{}, false
}

func isUnknownTypeDesc(desc TypeDesc) bool {
	return desc == (TypeDesc{})
}

func sameSemanticType(left, right TypeDesc) bool {
	switch left.Kind {
	case TypeKindSmallInt, TypeKindInteger, TypeKindBigInt:
		return right.Kind == left.Kind
	case TypeKindNumeric, TypeKindDecimal:
		if left.Precision == 0 || right.Precision == 0 {
			return left.Precision == right.Precision && left.Scale == right.Scale
		}
		return left.Precision == right.Precision && left.Scale == right.Scale
	case TypeKindReal, TypeKindDoublePrecision, TypeKindBoolean, TypeKindJSON, TypeKindArray, TypeKindRow:
		return true
	case TypeKindChar, TypeKindVarChar, TypeKindBinary, TypeKindVarBinary:
		return left.Length == right.Length
	case TypeKindText, TypeKindCLOB:
		return true
	case TypeKindDate:
		return true
	case TypeKindTime, TypeKindTimeWithTimeZone, TypeKindTimestamp, TypeKindTimestampWithTimeZone:
		return left.Precision == right.Precision
	default:
		return false
	}
}

func canCoerceExactInteger(from, to TypeDesc) bool {
	switch to.Kind {
	case TypeKindSmallInt, TypeKindInteger, TypeKindBigInt:
		return exactIntegerRank(to.Kind) >= exactIntegerRank(from.Kind)
	case TypeKindNumeric, TypeKindDecimal:
		return canCoerceExactIntegerToDecimal(from, to)
	case TypeKindDoublePrecision:
		return true
	default:
		return false
	}
}

func canCoerceExactDecimal(from, to TypeDesc) bool {
	switch to.Kind {
	case TypeKindNumeric, TypeKindDecimal:
		return canCoerceExactDecimalToDecimal(from, to)
	case TypeKindDoublePrecision:
		return true
	default:
		return false
	}
}

func canCoerceApproximateNumeric(from, to TypeDesc) bool {
	switch to.Kind {
	case TypeKindReal:
		return from.Kind == TypeKindReal
	case TypeKindDoublePrecision:
		return true
	default:
		return false
	}
}

func canCoerceCharacter(from, to TypeDesc) bool {
	switch to.Kind {
	case TypeKindChar, TypeKindVarChar:
		if !isBoundedCharacterKind(from.Kind) {
			return false
		}
		return boundedLength(from) <= boundedLength(to)
	case TypeKindText:
		return characterRank(from.Kind) <= characterRank(TypeKindText)
	case TypeKindCLOB:
		return characterRank(from.Kind) <= characterRank(TypeKindCLOB)
	default:
		return false
	}
}

func canCoerceBinary(from, to TypeDesc) bool {
	switch to.Kind {
	case TypeKindBinary, TypeKindVarBinary:
		if !isBoundedBinaryKind(from.Kind) {
			return false
		}
		return boundedLength(from) <= boundedLength(to)
	case TypeKindBLOB:
		return true
	default:
		return false
	}
}

func canCoerceDateTime(from, to TypeDesc) bool {
	return dateTimeRank(to.Kind) >= dateTimeRank(from.Kind) && to.Precision >= from.Precision
}

func canCoerceTimeOfDay(from, to TypeDesc) bool {
	return timeOfDayRank(to.Kind) >= timeOfDayRank(from.Kind) && to.Precision >= from.Precision
}

func canCoerceExactIntegerToDecimal(from, to TypeDesc) bool {
	envelope := exactNumericEnvelopeOf(from)
	target := decimalEnvelopeOf(to)
	if target.unbounded {
		return true
	}

	return envelope.fitsIn(target)
}

func canCoerceExactDecimalToDecimal(from, to TypeDesc) bool {
	source := decimalEnvelopeOf(from)
	target := decimalEnvelopeOf(to)
	if target.unbounded {
		return true
	}
	if source.unbounded {
		return false
	}

	return source.fitsIn(target)
}

func commonNumeric(left, right TypeDesc) (TypeDesc, bool) {
	leftExact := isExactNumericKind(left.Kind)
	rightExact := isExactNumericKind(right.Kind)
	leftApprox := isApproximateNumericKind(left.Kind)
	rightApprox := isApproximateNumericKind(right.Kind)

	switch {
	case leftExact && rightExact:
		leftEnvelope := exactNumericEnvelopeOf(left)
		rightEnvelope := exactNumericEnvelopeOf(right)
		if leftEnvelope.unbounded || rightEnvelope.unbounded {
			return TypeDesc{Kind: TypeKindNumeric}, true
		}

		if leftEnvelope.kind == exactNumericKindInteger && rightEnvelope.kind == exactNumericKindInteger {
			return TypeDesc{Kind: biggerIntegerKind(left.Kind, right.Kind)}, true
		}

		integerDigits := maxUint32(leftEnvelope.integerDigits, rightEnvelope.integerDigits)
		scale := maxUint32(leftEnvelope.scale, rightEnvelope.scale)
		precision := integerDigits + scale

		return TypeDesc{
			Kind:      TypeKindNumeric,
			Precision: precision,
			Scale:     scale,
		}, true
	case leftApprox && rightApprox:
		if left.Kind == TypeKindDoublePrecision || right.Kind == TypeKindDoublePrecision {
			return TypeDesc{Kind: TypeKindDoublePrecision}, true
		}

		return TypeDesc{Kind: TypeKindReal}, true
	case (leftExact && rightApprox) || (leftApprox && rightExact):
		return TypeDesc{Kind: TypeKindDoublePrecision}, true
	default:
		return TypeDesc{}, false
	}
}

func commonCharacter(left, right TypeDesc) (TypeDesc, bool) {
	if !isCharacterKind(left.Kind) || !isCharacterKind(right.Kind) {
		return TypeDesc{}, false
	}

	if left.Kind == TypeKindCLOB || right.Kind == TypeKindCLOB {
		return TypeDesc{Kind: TypeKindCLOB}, true
	}
	if left.Kind == TypeKindText || right.Kind == TypeKindText {
		return TypeDesc{Kind: TypeKindText}, true
	}
	if left.Kind == TypeKindVarChar || right.Kind == TypeKindVarChar {
		return TypeDesc{Kind: TypeKindVarChar, Length: maxUint32(left.Length, right.Length)}, true
	}

	return TypeDesc{Kind: TypeKindChar, Length: maxUint32(left.Length, right.Length)}, true
}

func commonBinary(left, right TypeDesc) (TypeDesc, bool) {
	if !isBinaryKind(left.Kind) || !isBinaryKind(right.Kind) {
		return TypeDesc{}, false
	}

	if left.Kind == TypeKindBLOB || right.Kind == TypeKindBLOB {
		return TypeDesc{Kind: TypeKindBLOB}, true
	}
	if left.Kind == TypeKindVarBinary || right.Kind == TypeKindVarBinary {
		return TypeDesc{Kind: TypeKindVarBinary, Length: maxUint32(left.Length, right.Length)}, true
	}

	return TypeDesc{Kind: TypeKindBinary, Length: maxUint32(left.Length, right.Length)}, true
}

func commonDateTime(left, right TypeDesc) (TypeDesc, bool) {
	if !isDateTimeKind(left.Kind) || !isDateTimeKind(right.Kind) {
		return TypeDesc{}, false
	}

	leftRank := dateTimeRank(left.Kind)
	rightRank := dateTimeRank(right.Kind)
	if leftRank == 0 || rightRank == 0 {
		return TypeDesc{}, false
	}

	result := TypeDesc{
		Kind:      moreGeneralDateTimeKind(left.Kind, right.Kind),
		Precision: maxUint32(left.Precision, right.Precision),
	}
	return result, true
}

func commonTimeOfDay(left, right TypeDesc) (TypeDesc, bool) {
	if !isTimeOfDayKind(left.Kind) || !isTimeOfDayKind(right.Kind) {
		return TypeDesc{}, false
	}

	result := TypeDesc{
		Kind:      moreGeneralTimeOfDayKind(left.Kind, right.Kind),
		Precision: maxUint32(left.Precision, right.Precision),
	}
	return result, true
}

func exactNumericEnvelopeOf(desc TypeDesc) exactNumericEnvelope {
	switch desc.Kind {
	case TypeKindSmallInt:
		return exactNumericEnvelope{
			kind:          exactNumericKindInteger,
			integerDigits: 5,
		}
	case TypeKindInteger:
		return exactNumericEnvelope{
			kind:          exactNumericKindInteger,
			integerDigits: 10,
		}
	case TypeKindBigInt:
		return exactNumericEnvelope{
			kind:          exactNumericKindInteger,
			integerDigits: 19,
		}
	case TypeKindNumeric, TypeKindDecimal:
		if desc.Precision == 0 {
			return exactNumericEnvelope{kind: exactNumericKindDecimal, unbounded: true}
		}
		return exactNumericEnvelope{
			kind:          exactNumericKindDecimal,
			integerDigits: desc.Precision - desc.Scale,
			scale:         desc.Scale,
		}
	default:
		return exactNumericEnvelope{}
	}
}

func decimalEnvelopeOf(desc TypeDesc) decimalEnvelope {
	switch desc.Kind {
	case TypeKindSmallInt:
		return decimalEnvelope{
			unbounded:     false,
			integerDigits: 5,
			scale:         0,
		}
	case TypeKindInteger:
		return decimalEnvelope{
			unbounded:     false,
			integerDigits: 10,
			scale:         0,
		}
	case TypeKindBigInt:
		return decimalEnvelope{
			unbounded:     false,
			integerDigits: 19,
			scale:         0,
		}
	case TypeKindNumeric, TypeKindDecimal:
		if desc.Precision == 0 {
			return decimalEnvelope{unbounded: true}
		}
		return decimalEnvelope{
			unbounded:     false,
			integerDigits: desc.Precision - desc.Scale,
			scale:         desc.Scale,
		}
	default:
		return decimalEnvelope{unbounded: true}
	}
}

func boundedLength(desc TypeDesc) uint32 {
	return desc.Length
}

func exactIntegerRank(kind TypeKind) uint8 {
	switch kind {
	case TypeKindSmallInt:
		return 1
	case TypeKindInteger:
		return 2
	case TypeKindBigInt:
		return 3
	default:
		return 0
	}
}

func biggerIntegerKind(left, right TypeKind) TypeKind {
	if exactIntegerRank(left) >= exactIntegerRank(right) {
		return left
	}

	return right
}

func isExactIntegerKind(kind TypeKind) bool {
	return exactIntegerRank(kind) > 0
}

func isExactDecimalKind(kind TypeKind) bool {
	switch kind {
	case TypeKindNumeric, TypeKindDecimal:
		return true
	default:
		return false
	}
}

func isExactNumericKind(kind TypeKind) bool {
	return isExactIntegerKind(kind) || isExactDecimalKind(kind)
}

func isApproximateNumericKind(kind TypeKind) bool {
	switch kind {
	case TypeKindReal, TypeKindDoublePrecision:
		return true
	default:
		return false
	}
}

func isCharacterKind(kind TypeKind) bool {
	switch kind {
	case TypeKindChar, TypeKindVarChar, TypeKindText, TypeKindCLOB:
		return true
	default:
		return false
	}
}

func isBinaryKind(kind TypeKind) bool {
	switch kind {
	case TypeKindBinary, TypeKindVarBinary, TypeKindBLOB:
		return true
	default:
		return false
	}
}

func isBoundedCharacterKind(kind TypeKind) bool {
	switch kind {
	case TypeKindChar, TypeKindVarChar:
		return true
	default:
		return false
	}
}

func characterRank(kind TypeKind) uint8 {
	switch kind {
	case TypeKindChar:
		return 1
	case TypeKindVarChar:
		return 2
	case TypeKindText:
		return 3
	case TypeKindCLOB:
		return 4
	default:
		return 0
	}
}

func isBoundedBinaryKind(kind TypeKind) bool {
	switch kind {
	case TypeKindBinary, TypeKindVarBinary:
		return true
	default:
		return false
	}
}

func isDateTimeKind(kind TypeKind) bool {
	switch kind {
	case TypeKindDate, TypeKindTimestamp, TypeKindTimestampWithTimeZone:
		return true
	default:
		return false
	}
}

func isTimeOfDayKind(kind TypeKind) bool {
	switch kind {
	case TypeKindTime, TypeKindTimeWithTimeZone:
		return true
	default:
		return false
	}
}

func dateTimeRank(kind TypeKind) uint8 {
	switch kind {
	case TypeKindDate:
		return 1
	case TypeKindTimestamp:
		return 2
	case TypeKindTimestampWithTimeZone:
		return 3
	default:
		return 0
	}
}

func moreGeneralDateTimeKind(left, right TypeKind) TypeKind {
	if dateTimeRank(left) >= dateTimeRank(right) {
		return left
	}

	return right
}

func timeOfDayRank(kind TypeKind) uint8 {
	switch kind {
	case TypeKindTime:
		return 1
	case TypeKindTimeWithTimeZone:
		return 2
	default:
		return 0
	}
}

func moreGeneralTimeOfDayKind(left, right TypeKind) TypeKind {
	if timeOfDayRank(left) >= timeOfDayRank(right) {
		return left
	}

	return right
}

func maxUint32(left, right uint32) uint32 {
	if left >= right {
		return left
	}

	return right
}

type exactNumericKind uint8

const (
	exactNumericKindInteger exactNumericKind = iota + 1
	exactNumericKindDecimal
)

type exactNumericEnvelope struct {
	kind          exactNumericKind
	integerDigits uint32
	scale         uint32
	unbounded     bool
}

func (e exactNumericEnvelope) fitsIn(other decimalEnvelope) bool {
	if e.unbounded || other.unbounded {
		return !e.unbounded || other.unbounded
	}
	return e.integerDigits <= other.integerDigits && e.scale <= other.scale
}

type decimalEnvelope struct {
	integerDigits uint32
	scale         uint32
	unbounded     bool
}

func (e decimalEnvelope) fitsIn(other decimalEnvelope) bool {
	if e.unbounded || other.unbounded {
		return !e.unbounded || other.unbounded
	}
	return e.integerDigits <= other.integerDigits && e.scale <= other.scale
}
