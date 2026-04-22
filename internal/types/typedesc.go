package types

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	// ErrInvalidTypeKind reports an unknown SQL type kind.
	ErrInvalidTypeKind = errors.New("types: invalid type kind")
	// ErrInvalidTypeDesc reports malformed or inconsistent type metadata.
	ErrInvalidTypeDesc = errors.New("types: invalid type descriptor")
)

// TypeKind identifies a SQL type family.
type TypeKind uint8

// TypeKind constants identify the supported SQL type families.
const (
	TypeKindInvalid TypeKind = iota
	TypeKindSmallInt
	TypeKindInteger
	TypeKindBigInt
	TypeKindNumeric
	TypeKindDecimal
	TypeKindBoolean
	TypeKindReal
	TypeKindDoublePrecision
	TypeKindChar
	TypeKindVarChar
	TypeKindText
	TypeKindCLOB
	TypeKindBinary
	TypeKindVarBinary
	TypeKindBLOB
	TypeKindDate
	TypeKindTime
	TypeKindTimeWithTimeZone
	TypeKindTimestamp
	TypeKindTimestampWithTimeZone
	TypeKindInterval
	TypeKindJSON
	TypeKindArray
	TypeKindRow
)

var typeKindNames = [...]string{
	TypeKindInvalid:               "INVALID",
	TypeKindSmallInt:              "SMALLINT",
	TypeKindInteger:               "INTEGER",
	TypeKindBigInt:                "BIGINT",
	TypeKindNumeric:               "NUMERIC",
	TypeKindDecimal:               "DECIMAL",
	TypeKindBoolean:               "BOOLEAN",
	TypeKindReal:                  "REAL",
	TypeKindDoublePrecision:       "DOUBLE PRECISION",
	TypeKindChar:                  "CHAR",
	TypeKindVarChar:               "VARCHAR",
	TypeKindText:                  "TEXT",
	TypeKindCLOB:                  "CLOB",
	TypeKindBinary:                "BINARY",
	TypeKindVarBinary:             "VARBINARY",
	TypeKindBLOB:                  "BLOB",
	TypeKindDate:                  "DATE",
	TypeKindTime:                  "TIME",
	TypeKindTimeWithTimeZone:      "TIME WITH TIME ZONE",
	TypeKindTimestamp:             "TIMESTAMP",
	TypeKindTimestampWithTimeZone: "TIMESTAMP WITH TIME ZONE",
	TypeKindInterval:              "INTERVAL",
	TypeKindJSON:                  "JSON",
	TypeKindArray:                 "ARRAY",
	TypeKindRow:                   "ROW",
}

var typeKindByName = map[string]TypeKind{
	"SMALLINT":                 TypeKindSmallInt,
	"INTEGER":                  TypeKindInteger,
	"INT":                      TypeKindInteger,
	"BIGINT":                   TypeKindBigInt,
	"NUMERIC":                  TypeKindNumeric,
	"DECIMAL":                  TypeKindDecimal,
	"BOOLEAN":                  TypeKindBoolean,
	"REAL":                     TypeKindReal,
	"DOUBLE PRECISION":         TypeKindDoublePrecision,
	"FLOAT":                    TypeKindDoublePrecision,
	"CHAR":                     TypeKindChar,
	"VARCHAR":                  TypeKindVarChar,
	"TEXT":                     TypeKindText,
	"CLOB":                     TypeKindCLOB,
	"BINARY":                   TypeKindBinary,
	"VARBINARY":                TypeKindVarBinary,
	"BLOB":                     TypeKindBLOB,
	"DATE":                     TypeKindDate,
	"TIME":                     TypeKindTime,
	"TIME WITH TIME ZONE":      TypeKindTimeWithTimeZone,
	"TIMESTAMP":                TypeKindTimestamp,
	"TIMESTAMP WITH TIME ZONE": TypeKindTimestampWithTimeZone,
	"INTERVAL":                 TypeKindInterval,
	"JSON":                     TypeKindJSON,
	"ARRAY":                    TypeKindArray,
	"ROW":                      TypeKindRow,
}

// String returns the canonical SQL spelling for a type kind.
func (k TypeKind) String() string {
	if int(k) < len(typeKindNames) && typeKindNames[k] != "" {
		return typeKindNames[k]
	}

	return fmt.Sprintf("TypeKind(%d)", k)
}

// ParseTypeKind resolves a SQL type name to its canonical kind.
func ParseTypeKind(text string) (TypeKind, error) {
	normalized := normalizeTypeText(text)
	if normalized == "" {
		return TypeKindInvalid, fmt.Errorf("%w: empty kind", ErrInvalidTypeKind)
	}

	kind, ok := typeKindByName[normalized]
	if !ok {
		return TypeKindInvalid, fmt.Errorf("%w: %q", ErrInvalidTypeKind, text)
	}

	return kind, nil
}

// TypeDesc describes a SQL type together with optional metadata such as
// length, precision, scale, and nullability.
type TypeDesc struct {
	Kind      TypeKind
	Precision uint32
	Scale     uint32
	Length    uint32
	Nullable  bool
}

// Validate reports whether the descriptor metadata is consistent for the
// descriptor's kind.
func (d TypeDesc) Validate() error {
	if d.Kind == TypeKindInvalid {
		return fmt.Errorf("%w: missing kind", ErrInvalidTypeDesc)
	}

	if _, err := ParseTypeKind(d.Kind.String()); err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidTypeDesc, err)
	}

	switch d.Kind {
	case TypeKindChar, TypeKindVarChar, TypeKindBinary, TypeKindVarBinary:
		if d.Length == 0 {
			return fmt.Errorf("%w: %s requires a positive length", ErrInvalidTypeDesc, d.Kind)
		}
		if d.Precision != 0 || d.Scale != 0 {
			return fmt.Errorf("%w: %s does not use precision or scale", ErrInvalidTypeDesc, d.Kind)
		}
	case TypeKindNumeric, TypeKindDecimal:
		if d.Length != 0 {
			return fmt.Errorf("%w: %s does not use length", ErrInvalidTypeDesc, d.Kind)
		}
		if d.Scale > 0 && d.Precision == 0 {
			return fmt.Errorf("%w: %s scale requires precision", ErrInvalidTypeDesc, d.Kind)
		}
		if d.Precision > 0 && d.Scale > d.Precision {
			return fmt.Errorf("%w: %s scale exceeds precision", ErrInvalidTypeDesc, d.Kind)
		}
	case TypeKindTime, TypeKindTimeWithTimeZone, TypeKindTimestamp, TypeKindTimestampWithTimeZone:
		if d.Length != 0 || d.Scale != 0 {
			return fmt.Errorf("%w: %s only supports precision", ErrInvalidTypeDesc, d.Kind)
		}
	default:
		if d.Length != 0 || d.Precision != 0 || d.Scale != 0 {
			return fmt.Errorf("%w: %s does not use length, precision, or scale", ErrInvalidTypeDesc, d.Kind)
		}
	}

	return nil
}

// String returns the canonical SQL type spelling for the descriptor.
func (d TypeDesc) String() string {
	base := d.format()
	if !d.Nullable {
		return base + " NOT NULL"
	}

	return base
}

// MarshalText implements encoding.TextMarshaler.
func (d TypeDesc) MarshalText() ([]byte, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}

	return []byte(d.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (d *TypeDesc) UnmarshalText(text []byte) error {
	if d == nil {
		return fmt.Errorf("%w: nil destination", ErrInvalidTypeDesc)
	}

	parsed, err := ParseTypeDesc(string(text))
	if err != nil {
		return err
	}

	*d = parsed
	return nil
}

// ParseTypeDesc parses the canonical text form produced by TypeDesc.String.
func ParseTypeDesc(text string) (TypeDesc, error) {
	normalized := normalizeTypeText(text)
	if normalized == "" {
		return TypeDesc{}, fmt.Errorf("%w: empty descriptor", ErrInvalidTypeDesc)
	}

	nullable := true
	switch {
	case strings.HasSuffix(normalized, " NOT NULL"):
		nullable = false
		normalized = strings.TrimSpace(strings.TrimSuffix(normalized, " NOT NULL"))
	case strings.HasSuffix(normalized, " NULL"):
		normalized = strings.TrimSpace(strings.TrimSuffix(normalized, " NULL"))
	}

	descriptor := TypeDesc{Nullable: nullable}

	if withTZBase, ok := strings.CutSuffix(normalized, " WITH TIME ZONE"); ok {
		base, params, hasParams, err := cutTypeParameters(withTZBase)
		if err != nil {
			return TypeDesc{}, err
		}

		switch base {
		case "TIME":
			descriptor.Kind = TypeKindTimeWithTimeZone
		case "TIMESTAMP":
			descriptor.Kind = TypeKindTimestampWithTimeZone
		default:
			return TypeDesc{}, fmt.Errorf("%w: %q", ErrInvalidTypeDesc, text)
		}

		if hasParams {
			precision, err := parsePositiveUint32(params)
			if err != nil {
				return TypeDesc{}, err
			}
			descriptor.Precision = precision
		}

		if err := descriptor.Validate(); err != nil {
			return TypeDesc{}, err
		}

		return descriptor, nil
	}

	base, params, hasParams, err := cutTypeParameters(normalized)
	if err != nil {
		return TypeDesc{}, err
	}

	kind, err := ParseTypeKind(base)
	if err != nil {
		return TypeDesc{}, fmt.Errorf("%w: %v", ErrInvalidTypeDesc, err)
	}
	descriptor.Kind = kind

	switch kind {
	case TypeKindChar, TypeKindVarChar, TypeKindBinary, TypeKindVarBinary:
		if !hasParams {
			return TypeDesc{}, fmt.Errorf("%w: %s requires length", ErrInvalidTypeDesc, kind)
		}
		length, err := parsePositiveUint32(params)
		if err != nil {
			return TypeDesc{}, err
		}
		descriptor.Length = length
	case TypeKindNumeric, TypeKindDecimal:
		if hasParams {
			parts := splitTypeParameters(params)
			switch len(parts) {
			case 1:
				precision, err := parsePositiveUint32(parts[0])
				if err != nil {
					return TypeDesc{}, err
				}
				descriptor.Precision = precision
			case 2:
				precision, err := parsePositiveUint32(parts[0])
				if err != nil {
					return TypeDesc{}, err
				}
				scale, err := parseUint32(parts[1])
				if err != nil {
					return TypeDesc{}, err
				}
				descriptor.Precision = precision
				descriptor.Scale = scale
			default:
				return TypeDesc{}, fmt.Errorf("%w: %s expects one or two parameters", ErrInvalidTypeDesc, kind)
			}
		}
	case TypeKindTime, TypeKindTimestamp:
		if hasParams {
			precision, err := parsePositiveUint32(params)
			if err != nil {
				return TypeDesc{}, err
			}
			descriptor.Precision = precision
		}
	default:
		if hasParams {
			return TypeDesc{}, fmt.Errorf("%w: %s does not accept parameters", ErrInvalidTypeDesc, kind)
		}
	}

	if err := descriptor.Validate(); err != nil {
		return TypeDesc{}, err
	}

	return descriptor, nil
}

func (d TypeDesc) format() string {
	switch d.Kind {
	case TypeKindChar, TypeKindVarChar, TypeKindBinary, TypeKindVarBinary:
		if d.Length > 0 {
			return fmt.Sprintf("%s(%d)", d.Kind, d.Length)
		}
	case TypeKindNumeric, TypeKindDecimal:
		if d.Precision > 0 {
			return fmt.Sprintf("%s(%d,%d)", d.Kind, d.Precision, d.Scale)
		}
	case TypeKindTime, TypeKindTimestamp:
		if d.Precision > 0 {
			return fmt.Sprintf("%s(%d)", d.Kind, d.Precision)
		}
	case TypeKindTimeWithTimeZone:
		if d.Precision > 0 {
			return fmt.Sprintf("TIME(%d) WITH TIME ZONE", d.Precision)
		}
	case TypeKindTimestampWithTimeZone:
		if d.Precision > 0 {
			return fmt.Sprintf("TIMESTAMP(%d) WITH TIME ZONE", d.Precision)
		}
	}

	return d.Kind.String()
}

func normalizeTypeText(text string) string {
	return strings.ToUpper(strings.Join(strings.Fields(strings.TrimSpace(text)), " "))
}

func cutTypeParameters(text string) (base string, params string, hasParams bool, err error) {
	open := strings.IndexByte(text, '(')
	if open < 0 {
		return strings.TrimSpace(text), "", false, nil
	}
	if !strings.HasSuffix(text, ")") {
		return "", "", false, fmt.Errorf("%w: malformed parameter list", ErrInvalidTypeDesc)
	}
	if strings.Count(text, "(") != 1 || strings.Count(text, ")") != 1 || open > strings.LastIndexByte(text, ')') {
		return "", "", false, fmt.Errorf("%w: malformed parameter list", ErrInvalidTypeDesc)
	}

	base = strings.TrimSpace(text[:open])
	params = strings.TrimSpace(text[open+1 : len(text)-1])
	if base == "" || params == "" {
		return "", "", false, fmt.Errorf("%w: malformed parameter list", ErrInvalidTypeDesc)
	}

	return base, params, true, nil
}

func splitTypeParameters(params string) []string {
	parts := strings.Split(params, ",")
	for index := range parts {
		parts[index] = strings.TrimSpace(parts[index])
	}

	return parts
}

func parsePositiveUint32(text string) (uint32, error) {
	value, err := parseUint32(text)
	if err != nil {
		return 0, err
	}
	if value == 0 {
		return 0, fmt.Errorf("%w: expected positive integer", ErrInvalidTypeDesc)
	}

	return value, nil
}

func parseUint32(text string) (uint32, error) {
	value, err := strconv.ParseUint(strings.TrimSpace(text), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid integer %q", ErrInvalidTypeDesc, text)
	}

	return uint32(value), nil
}
