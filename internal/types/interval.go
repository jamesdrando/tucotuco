package types

import "time"

const intervalDay = int64(24 * time.Hour)

// Interval stores SQL interval values as year-month and day-time components.
type Interval struct {
	Months int64
	Days   int64
	Nanos  int64
}

// NewInterval constructs a normalized interval.
func NewInterval(months, days, nanos int64) Interval {
	return Interval{
		Months: months,
		Days:   days,
		Nanos:  nanos,
	}.Normalize()
}

// Normalize carries whole-day nanoseconds into the Days field and keeps the
// day-time sign consistent.
func (i Interval) Normalize() Interval {
	i.Days += i.Nanos / intervalDay
	i.Nanos %= intervalDay

	switch {
	case i.Nanos < 0 && i.Days > 0:
		i.Days--
		i.Nanos += intervalDay
	case i.Nanos > 0 && i.Days < 0:
		i.Days++
		i.Nanos -= intervalDay
	}

	return i
}

// Equal reports whether two intervals have the same normalized components.
func (i Interval) Equal(other Interval) bool {
	left := i.Normalize()
	right := other.Normalize()

	return left.Months == right.Months && left.Days == right.Days && left.Nanos == right.Nanos
}

// Compare compares two intervals lexicographically by month, day, then
// sub-day nanosecond components.
func (i Interval) Compare(other Interval) int {
	left := i.Normalize()
	right := other.Normalize()

	switch {
	case left.Months < right.Months:
		return -1
	case left.Months > right.Months:
		return 1
	case left.Days < right.Days:
		return -1
	case left.Days > right.Days:
		return 1
	case left.Nanos < right.Nanos:
		return -1
	case left.Nanos > right.Nanos:
		return 1
	default:
		return 0
	}
}
