package types

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
)

var (
	// ErrInvalidDecimal reports malformed decimal text or an invalid decimal
	// configuration.
	ErrInvalidDecimal = errors.New("types: invalid decimal")
)

var bigTen = big.NewInt(10)

// Decimal stores an arbitrary-precision base-10 value as coefficient and scale,
// where the numeric value is coefficient * 10^-scale.
type Decimal struct {
	coeff *big.Int
	scale int32
}

// NewDecimal constructs a normalized decimal from a coefficient and scale.
func NewDecimal(coefficient *big.Int, scale int32) (Decimal, error) {
	if coefficient == nil {
		return Decimal{}, nil
	}

	return normalizeDecimal(Decimal{
		coeff: new(big.Int).Set(coefficient),
		scale: scale,
	}), nil
}

// NewDecimalFromInt64 constructs a decimal from an integer.
func NewDecimalFromInt64(value int64) Decimal {
	decimal, _ := NewDecimal(big.NewInt(value), 0)
	return decimal
}

// ParseDecimal parses a base-10 decimal string with optional sign, decimal
// point, and scientific notation.
func ParseDecimal(text string) (Decimal, error) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return Decimal{}, fmt.Errorf("%w: empty decimal", ErrInvalidDecimal)
	}

	sign := 1
	switch trimmed[0] {
	case '+':
		trimmed = trimmed[1:]
	case '-':
		sign = -1
		trimmed = trimmed[1:]
	}
	if trimmed == "" {
		return Decimal{}, fmt.Errorf("%w: missing digits", ErrInvalidDecimal)
	}

	exponent := int64(0)
	if index := strings.IndexAny(trimmed, "eE"); index >= 0 {
		parsedExponent, err := strconv.ParseInt(trimmed[index+1:], 10, 32)
		if err != nil {
			return Decimal{}, fmt.Errorf("%w: invalid exponent", ErrInvalidDecimal)
		}
		exponent = parsedExponent
		trimmed = trimmed[:index]
		if trimmed == "" {
			return Decimal{}, fmt.Errorf("%w: missing significand", ErrInvalidDecimal)
		}
	}

	if strings.Count(trimmed, ".") > 1 {
		return Decimal{}, fmt.Errorf("%w: multiple decimal points", ErrInvalidDecimal)
	}

	integerPart := trimmed
	fractionalPart := ""
	if index := strings.IndexByte(trimmed, '.'); index >= 0 {
		integerPart = trimmed[:index]
		fractionalPart = trimmed[index+1:]
	}
	if integerPart == "" && fractionalPart == "" {
		return Decimal{}, fmt.Errorf("%w: missing digits", ErrInvalidDecimal)
	}

	digits := integerPart + fractionalPart
	for _, r := range digits {
		if r < '0' || r > '9' {
			return Decimal{}, fmt.Errorf("%w: invalid digit %q", ErrInvalidDecimal, r)
		}
	}
	if digits == "" {
		digits = "0"
	}

	coefficient := new(big.Int)
	if _, ok := coefficient.SetString(digits, 10); !ok {
		return Decimal{}, fmt.Errorf("%w: invalid coefficient", ErrInvalidDecimal)
	}
	if sign < 0 {
		coefficient.Neg(coefficient)
	}

	scale64 := int64(len(fractionalPart)) - exponent
	if scale64 < math.MinInt32 || scale64 > math.MaxInt32 {
		return Decimal{}, fmt.Errorf("%w: scale out of range", ErrInvalidDecimal)
	}

	return NewDecimal(coefficient, int32(scale64))
}

// Coefficient returns a copy of the decimal coefficient.
func (d Decimal) Coefficient() *big.Int {
	if d.coeff == nil {
		return new(big.Int)
	}

	return new(big.Int).Set(d.coeff)
}

// Scale returns the decimal's canonical base-10 scale.
func (d Decimal) Scale() int32 {
	return normalizeDecimal(d).scale
}

// Sign reports whether the decimal is negative, zero, or positive.
func (d Decimal) Sign() int {
	return normalizeDecimal(d).Coefficient().Sign()
}

// Equal reports whether two decimals represent the same numeric value.
func (d Decimal) Equal(other Decimal) bool {
	return d.Compare(other) == 0
}

// Compare compares two decimals.
func (d Decimal) Compare(other Decimal) int {
	left := normalizeDecimal(d)
	right := normalizeDecimal(other)

	switch {
	case left.scale == right.scale:
		return left.Coefficient().Cmp(right.Coefficient())
	case left.scale > right.scale:
		scaledRight := right.Coefficient()
		scaledRight.Mul(scaledRight, pow10(int64(left.scale-right.scale)))
		return left.Coefficient().Cmp(scaledRight)
	default:
		scaledLeft := left.Coefficient()
		scaledLeft.Mul(scaledLeft, pow10(int64(right.scale-left.scale)))
		return scaledLeft.Cmp(right.Coefficient())
	}
}

// String returns the canonical decimal text form.
func (d Decimal) String() string {
	normalized := normalizeDecimal(d)
	coefficient := normalized.Coefficient()
	if coefficient.Sign() == 0 {
		return "0"
	}

	sign := ""
	if coefficient.Sign() < 0 {
		sign = "-"
		coefficient.Abs(coefficient)
	}

	digits := coefficient.String()
	scale := normalized.scale

	switch {
	case scale == 0:
		return sign + digits
	case scale < 0:
		return sign + digits + strings.Repeat("0", int(-scale))
	case len(digits) > int(scale):
		index := len(digits) - int(scale)
		return sign + digits[:index] + "." + digits[index:]
	default:
		return sign + "0." + strings.Repeat("0", int(scale)-len(digits)) + digits
	}
}

// MarshalText implements encoding.TextMarshaler.
func (d Decimal) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (d *Decimal) UnmarshalText(text []byte) error {
	parsed, err := ParseDecimal(string(text))
	if err != nil {
		return err
	}

	*d = parsed
	return nil
}

func (d Decimal) rat() *big.Rat {
	normalized := normalizeDecimal(d)
	coefficient := normalized.Coefficient()

	switch {
	case normalized.scale == 0:
		return new(big.Rat).SetInt(coefficient)
	case normalized.scale > 0:
		return new(big.Rat).SetFrac(coefficient, pow10(int64(normalized.scale)))
	default:
		coefficient.Mul(coefficient, pow10(int64(-normalized.scale)))
		return new(big.Rat).SetInt(coefficient)
	}
}

func normalizeDecimal(d Decimal) Decimal {
	if d.coeff == nil || d.coeff.Sign() == 0 {
		return Decimal{}
	}

	coefficient := new(big.Int).Set(d.coeff)
	scale := d.scale
	remainder := new(big.Int)
	for scale > math.MinInt32 {
		remainder.Mod(coefficient, bigTen)
		if remainder.Sign() != 0 {
			break
		}
		coefficient.Quo(coefficient, bigTen)
		scale--
	}

	return Decimal{
		coeff: coefficient,
		scale: scale,
	}
}

func pow10(exponent int64) *big.Int {
	if exponent <= 0 {
		return big.NewInt(1)
	}

	return new(big.Int).Exp(bigTen, big.NewInt(exponent), nil)
}
