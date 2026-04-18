package types

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestValueIsNull(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		value Value
		want  bool
	}{
		{name: "zero value", value: Value{}, want: true},
		{name: "explicit null", value: NullValue(), want: true},
		{name: "bool", value: BoolValue(true), want: false},
		{name: "decimal", value: DecimalValue(mustParseDecimal(t, "1.23")), want: false},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.value.IsNull(); got != testCase.want {
				t.Fatalf("IsNull() = %t, want %t", got, testCase.want)
			}
		})
	}
}

func TestValueEqual(t *testing.T) {
	t.Parallel()

	instant := time.Date(2026, time.April, 18, 12, 30, 0, 0, time.FixedZone("UTC-5", -5*60*60))
	sqlTime := 12*time.Hour + 30*time.Minute + 15*time.Second

	sourceBytes := []byte{1, 2, 3}
	bytesValue := BytesValue(sourceBytes)
	sourceBytes[0] = 9

	testCases := []struct {
		name  string
		left  Value
		right Value
		want  bool
	}{
		{name: "null equals null", left: NullValue(), right: NullValue(), want: true},
		{name: "null not equal bool", left: NullValue(), right: BoolValue(true), want: false},
		{name: "cross integer family", left: Int32Value(42), right: Int64Value(42), want: true},
		{name: "decimal normalized", left: DecimalValue(mustParseDecimal(t, "1.2300")), right: DecimalValue(mustParseDecimal(t, "1.23")), want: true},
		{name: "decimal equals integer", left: DecimalValue(mustParseDecimal(t, "42.0")), right: Int16Value(42), want: true},
		{name: "bytes cloned on construction", left: bytesValue, right: BytesValue([]byte{1, 2, 3}), want: true},
		{name: "datetime equality", left: TimeValue(instant), right: DateTimeValue(instant.UTC()), want: true},
		{name: "sql time equality", left: TimeOfDayValue(sqlTime), right: TimeOfDayValue(sqlTime), want: true},
		{name: "temporal representations stay distinct", left: TimeOfDayValue(sqlTime), right: TimeValue(instant), want: false},
		{name: "array equality", left: ArrayValue(Array{Int32Value(1), NullValue(), StringValue("ok")}), right: ArrayValue(Array{Int64Value(1), NullValue(), StringValue("ok")}), want: true},
		{name: "row inequality", left: RowValue(Row{Int32Value(1), StringValue("a")}), right: RowValue(Row{Int32Value(1), StringValue("b")}), want: false},
		{name: "nan equality is structural", left: Float64Value(math.NaN()), right: Float64Value(math.NaN()), want: true},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.left.Equal(testCase.right); got != testCase.want {
				t.Fatalf("Equal() = %t, want %t", got, testCase.want)
			}
		})
	}
}

func TestValueCompare(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		left    Value
		right   Value
		want    int
		wantErr error
	}{
		{name: "null comparison", left: NullValue(), right: Int32Value(1), wantErr: ErrNullComparison},
		{name: "bool order", left: BoolValue(false), right: BoolValue(true), want: -1},
		{name: "cross numeric order", left: Int16Value(5), right: Int64Value(6), want: -1},
		{name: "decimal equals integer", left: DecimalValue(mustParseDecimal(t, "42.000")), right: Int32Value(42), want: 0},
		{name: "string order", left: StringValue("ant"), right: StringValue("bee"), want: -1},
		{name: "bytes order", left: BytesValue([]byte{1, 2}), right: BytesValue([]byte{1, 3}), want: -1},
		{name: "datetime order", left: TimeValue(time.Date(2026, 4, 18, 1, 0, 0, 0, time.UTC)), right: DateTimeValue(time.Date(2026, 4, 18, 2, 0, 0, 0, time.UTC)), want: -1},
		{name: "sql time order", left: TimeOfDayValue(9*time.Hour + 15*time.Minute), right: TimeOfDayValue(10 * time.Hour), want: -1},
		{name: "interval normalized equality", left: IntervalValue(NewInterval(0, 1, 0)), right: IntervalValue(NewInterval(0, 0, int64(24*time.Hour))), want: 0},
		{name: "array lexicographic", left: ArrayValue(Array{Int32Value(1), Int32Value(2)}), right: ArrayValue(Array{Int32Value(1), Int32Value(3)}), want: -1},
		{name: "row prefix order", left: RowValue(Row{Int32Value(1)}), right: RowValue(Row{Int32Value(1), Int32Value(2)}), want: -1},
		{name: "temporal kinds are incomparable", left: TimeOfDayValue(time.Hour), right: TimeValue(time.Date(2026, 4, 18, 1, 0, 0, 0, time.UTC)), wantErr: ErrIncomparable},
		{name: "incomparable kinds", left: StringValue("1"), right: BoolValue(true), wantErr: ErrIncomparable},
		{name: "non finite numeric", left: Float64Value(math.NaN()), right: Float64Value(1), wantErr: ErrNonFiniteNumeric},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got, err := testCase.left.Compare(testCase.right)
			if testCase.wantErr != nil {
				if !errors.Is(err, testCase.wantErr) {
					t.Fatalf("Compare() error = %v, want %v", err, testCase.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Compare() returned error: %v", err)
			}
			if got != testCase.want {
				t.Fatalf("Compare() = %d, want %d", got, testCase.want)
			}
		})
	}
}

func TestValueRawClonesMutableValues(t *testing.T) {
	t.Parallel()

	bytesValue := BytesValue([]byte{1, 2, 3})
	rawBytes := bytesValue.Raw().([]byte)
	rawBytes[0] = 9
	if !bytesValue.Equal(BytesValue([]byte{1, 2, 3})) {
		t.Fatal("mutating Raw() bytes changed the stored value")
	}

	arrayValue := ArrayValue(Array{Int32Value(1), Int32Value(2)})
	rawArray := arrayValue.Raw().(Array)
	rawArray[0] = Int32Value(9)
	if !arrayValue.Equal(ArrayValue(Array{Int32Value(1), Int32Value(2)})) {
		t.Fatal("mutating Raw() array changed the stored value")
	}

	dateTimeValue := DateTimeValue(time.Date(2026, time.April, 18, 12, 30, 0, 0, time.UTC))
	rawDateTime, ok := dateTimeValue.Raw().(time.Time)
	if !ok {
		t.Fatalf("Raw() type = %T, want time.Time", dateTimeValue.Raw())
	}
	if !rawDateTime.Equal(time.Date(2026, time.April, 18, 12, 30, 0, 0, time.UTC)) {
		t.Fatalf("Raw() datetime = %v, want original instant", rawDateTime)
	}

	timeOfDayValue := TimeOfDayValue(8*time.Hour + 45*time.Minute)
	rawTimeOfDay, ok := timeOfDayValue.Raw().(time.Duration)
	if !ok {
		t.Fatalf("Raw() type = %T, want time.Duration", timeOfDayValue.Raw())
	}
	if rawTimeOfDay != 8*time.Hour+45*time.Minute {
		t.Fatalf("Raw() duration = %v, want %v", rawTimeOfDay, 8*time.Hour+45*time.Minute)
	}
}

func mustParseDecimal(t *testing.T, text string) Decimal {
	t.Helper()

	decimal, err := ParseDecimal(text)
	if err != nil {
		t.Fatalf("ParseDecimal(%q) returned error: %v", text, err)
	}

	return decimal
}
