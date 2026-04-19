package types

import (
	"errors"
	"testing"
	"time"
)

func TestCast(t *testing.T) {
	t.Parallel()

	chicago := time.FixedZone("UTC-5", -5*60*60)

	testCases := []struct {
		name   string
		value  Value
		source string
		target string
		want   Value
	}{
		{
			name:   "smallint to bigint",
			value:  Int16Value(42),
			source: "SMALLINT",
			target: "BIGINT",
			want:   Int64Value(42),
		},
		{
			name:   "decimal to integer exact",
			value:  DecimalValue(mustParseDecimal(t, "42.000")),
			source: "NUMERIC(6,3)",
			target: "INTEGER",
			want:   Int32Value(42),
		},
		{
			name:   "varchar to numeric",
			value:  StringValue("123.45"),
			source: "VARCHAR(16)",
			target: "NUMERIC(5,2)",
			want:   DecimalValue(mustParseDecimal(t, "123.45")),
		},
		{
			name:   "zero fits scaled numeric",
			value:  Int32Value(0),
			source: "INTEGER",
			target: "NUMERIC(1,1)",
			want:   DecimalValue(mustParseDecimal(t, "0")),
		},
		{
			name:   "double precision to real",
			value:  Float64Value(12.5),
			source: "DOUBLE PRECISION",
			target: "REAL",
			want:   Float32Value(12.5),
		},
		{
			name:   "text to boolean",
			value:  StringValue(" true "),
			source: "TEXT",
			target: "BOOLEAN",
			want:   BoolValue(true),
		},
		{
			name:   "varchar to char pads",
			value:  StringValue("go"),
			source: "VARCHAR(2)",
			target: "CHAR(4)",
			want:   StringValue("go  "),
		},
		{
			name:   "varbinary to binary pads",
			value:  BytesValue([]byte{1, 2}),
			source: "VARBINARY(2)",
			target: "BINARY(4)",
			want:   BytesValue([]byte{1, 2, 0, 0}),
		},
		{
			name:   "text to date",
			value:  StringValue("2026-04-18"),
			source: "TEXT",
			target: "DATE",
			want:   DateTimeValue(time.Date(2026, time.April, 18, 0, 0, 0, 0, time.UTC)),
		},
		{
			name:   "text to time truncates precision",
			value:  StringValue("12:30:15.987654321"),
			source: "TEXT",
			target: "TIME(3)",
			want:   TimeOfDayValue(12*time.Hour + 30*time.Minute + 15*time.Second + 987*time.Millisecond),
		},
		{
			name:   "text to timestamp with time zone",
			value:  StringValue("2026-04-18 12:30:15.987654-05:00"),
			source: "TEXT",
			target: "TIMESTAMP(3) WITH TIME ZONE",
			want:   DateTimeValue(time.Date(2026, time.April, 18, 12, 30, 15, 987000000, chicago)),
		},
		{
			name:   "date to timestamp",
			value:  DateTimeValue(time.Date(2026, time.April, 18, 0, 0, 0, 0, time.UTC)),
			source: "DATE",
			target: "TIMESTAMP",
			want:   DateTimeValue(time.Date(2026, time.April, 18, 0, 0, 0, 0, time.UTC)),
		},
		{
			name:   "timestamp with time zone to date",
			value:  DateTimeValue(time.Date(2026, time.April, 18, 23, 30, 0, 0, chicago)),
			source: "TIMESTAMP WITH TIME ZONE",
			target: "DATE",
			want:   DateTimeValue(time.Date(2026, time.April, 18, 0, 0, 0, 0, time.UTC)),
		},
		{
			name:   "time with time zone to time",
			value:  DateTimeValue(time.Date(1, time.January, 1, 12, 30, 15, 123456000, chicago)),
			source: "TIME(6) WITH TIME ZONE",
			target: "TIME(3)",
			want:   TimeOfDayValue(12*time.Hour + 30*time.Minute + 15*time.Second + 123*time.Millisecond),
		},
		{
			name:   "timestamp with time zone to varchar formatting",
			value:  DateTimeValue(time.Date(2026, time.April, 18, 12, 30, 15, 123456000, chicago)),
			source: "TIMESTAMP(6) WITH TIME ZONE",
			target: "VARCHAR(64)",
			want:   StringValue("2026-04-18 12:30:15.123456-05:00"),
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			source := mustTypeDesc(t, testCase.source)
			target := mustTypeDesc(t, testCase.target)

			got, err := Cast(testCase.value, source, target)
			if err != nil {
				t.Fatalf("Cast() returned error: %v", err)
			}
			if !got.Equal(testCase.want) {
				t.Fatalf("Cast() = %#v, want %#v", got, testCase.want)
			}
		})
	}
}

func TestCastNull(t *testing.T) {
	t.Parallel()

	got, err := Cast(NullValue(), TypeDesc{}, mustTypeDesc(t, "INTEGER NOT NULL"))
	if err != nil {
		t.Fatalf("Cast() returned error: %v", err)
	}
	if !got.IsNull() {
		t.Fatalf("Cast() = %#v, want NULL", got)
	}
}

func TestCastApproximateNumericToExactDecimal(t *testing.T) {
	t.Parallel()

	got, err := Cast(
		Float64Value(0.1),
		mustTypeDesc(t, "DOUBLE PRECISION"),
		mustTypeDesc(t, "DECIMAL(2,1)"),
	)
	if err != nil {
		t.Fatalf("Cast() returned error: %v", err)
	}
	if !got.Equal(DecimalValue(mustParseDecimal(t, "0.1"))) {
		t.Fatalf("Cast() = %#v, want %#v", got, DecimalValue(mustParseDecimal(t, "0.1")))
	}

	_, err = Cast(
		Float64Value(12.5),
		mustTypeDesc(t, "DOUBLE PRECISION"),
		mustTypeDesc(t, "DECIMAL(2,1)"),
	)
	if !errors.Is(err, ErrCastOverflow) {
		t.Fatalf("Cast() error = %v, want %v", err, ErrCastOverflow)
	}
}

func TestCastValidatesSourceDescriptorShape(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		value   Value
		source  string
		target  string
		want    Value
		wantErr error
	}{
		{
			name:   "source varchar length counts characters",
			value:  StringValue("éé"),
			source: "VARCHAR(2)",
			target: "TEXT",
			want:   StringValue("éé"),
		},
		{
			name:   "normalized date source accepted",
			value:  DateTimeValue(time.Date(2026, time.April, 18, 0, 0, 0, 0, time.UTC)),
			source: "DATE",
			target: "VARCHAR(32)",
			want:   StringValue("2026-04-18"),
		},
		{
			name:   "normalized time source accepted",
			value:  TimeOfDayValue(12*time.Hour + 30*time.Minute + 15*time.Second + 123*time.Millisecond),
			source: "TIME(3)",
			target: "VARCHAR(32)",
			want:   StringValue("12:30:15.123"),
		},
		{
			name:    "overlength varchar source rejected",
			value:   StringValue("ééé"),
			source:  "VARCHAR(2)",
			target:  "TEXT",
			wantErr: ErrInvalidCast,
		},
		{
			name:    "non normalized date source rejected",
			value:   DateTimeValue(time.Date(2026, time.April, 18, 12, 0, 0, 0, time.UTC)),
			source:  "DATE",
			target:  "VARCHAR(32)",
			wantErr: ErrInvalidCast,
		},
		{
			name:    "non normalized time source rejected",
			value:   TimeOfDayValue(12*time.Hour + 30*time.Minute + 15*time.Second + 123456*time.Microsecond),
			source:  "TIME(3)",
			target:  "VARCHAR(32)",
			wantErr: ErrInvalidCast,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got, err := Cast(testCase.value, mustTypeDesc(t, testCase.source), mustTypeDesc(t, testCase.target))
			if testCase.wantErr != nil {
				if !errors.Is(err, testCase.wantErr) {
					t.Fatalf("Cast() error = %v, want %v", err, testCase.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Cast() returned error: %v", err)
			}
			if !got.Equal(testCase.want) {
				t.Fatalf("Cast() = %#v, want %#v", got, testCase.want)
			}
		})
	}
}

func TestCastCharacterConstraintsUseCharacterCounts(t *testing.T) {
	t.Parallel()

	got, err := Cast(
		StringValue("éé"),
		mustTypeDesc(t, "TEXT"),
		mustTypeDesc(t, "CHAR(3)"),
	)
	if err != nil {
		t.Fatalf("Cast() returned error: %v", err)
	}
	if !got.Equal(StringValue("éé ")) {
		t.Fatalf("Cast() = %#v, want %#v", got, StringValue("éé "))
	}

	_, err = Cast(
		StringValue("ééé"),
		mustTypeDesc(t, "TEXT"),
		mustTypeDesc(t, "VARCHAR(2)"),
	)
	if !errors.Is(err, ErrCastOverflow) {
		t.Fatalf("Cast() error = %v, want %v", err, ErrCastOverflow)
	}
}

func TestCastErrors(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		value   Value
		source  string
		target  string
		wantErr error
	}{
		{
			name:    "integer overflow to smallint",
			value:   Int32Value(40000),
			source:  "INTEGER",
			target:  "SMALLINT",
			wantErr: ErrCastOverflow,
		},
		{
			name:    "fractional decimal to integer",
			value:   DecimalValue(mustParseDecimal(t, "42.5")),
			source:  "NUMERIC(4,1)",
			target:  "INTEGER",
			wantErr: ErrCastFailure,
		},
		{
			name:    "numeric precision overflow",
			value:   StringValue("123.45"),
			source:  "VARCHAR(16)",
			target:  "NUMERIC(4,2)",
			wantErr: ErrCastOverflow,
		},
		{
			name:    "invalid integer text",
			value:   StringValue("not-a-number"),
			source:  "TEXT",
			target:  "INTEGER",
			wantErr: ErrCastFailure,
		},
		{
			name:    "invalid date text",
			value:   StringValue("2026-02-30"),
			source:  "TEXT",
			target:  "DATE",
			wantErr: ErrCastFailure,
		},
		{
			name:    "varchar too short",
			value:   StringValue("alphabet"),
			source:  "TEXT",
			target:  "VARCHAR(3)",
			wantErr: ErrCastOverflow,
		},
		{
			name:    "binary too short",
			value:   BytesValue([]byte{1, 2, 3}),
			source:  "BLOB",
			target:  "BINARY(2)",
			wantErr: ErrCastOverflow,
		},
		{
			name:    "unsupported boolean to integer cast",
			value:   BoolValue(true),
			source:  "BOOLEAN",
			target:  "INTEGER",
			wantErr: ErrInvalidCast,
		},
		{
			name:    "source descriptor mismatch",
			value:   Int32Value(1),
			source:  "BIGINT",
			target:  "INTEGER",
			wantErr: ErrInvalidCast,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			source := mustTypeDesc(t, testCase.source)
			target := mustTypeDesc(t, testCase.target)

			_, err := Cast(testCase.value, source, target)
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("Cast() error = %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestTryCast(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		value  Value
		source string
		target string
		want   Value
	}{
		{
			name:   "parse failure becomes null",
			value:  StringValue("oops"),
			source: "TEXT",
			target: "INTEGER",
			want:   NullValue(),
		},
		{
			name:   "overflow becomes null",
			value:  Int32Value(40000),
			source: "INTEGER",
			target: "SMALLINT",
			want:   NullValue(),
		},
		{
			name:   "temporal parse failure becomes null",
			value:  StringValue("2026-02-30"),
			source: "TEXT",
			target: "DATE",
			want:   NullValue(),
		},
		{
			name:   "null stays null",
			value:  NullValue(),
			source: "TEXT",
			target: "DATE",
			want:   NullValue(),
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			source := mustTypeDesc(t, testCase.source)
			target := mustTypeDesc(t, testCase.target)

			got, err := TryCast(testCase.value, source, target)
			if err != nil {
				t.Fatalf("TryCast() returned error: %v", err)
			}
			if !got.Equal(testCase.want) {
				t.Fatalf("TryCast() = %#v, want %#v", got, testCase.want)
			}
		})
	}
}

func TestTryCastInvalidDefinitionStillErrors(t *testing.T) {
	t.Parallel()

	_, err := TryCast(BoolValue(true), mustTypeDesc(t, "BOOLEAN"), mustTypeDesc(t, "INTEGER"))
	if !errors.Is(err, ErrInvalidCast) {
		t.Fatalf("TryCast() error = %v, want %v", err, ErrInvalidCast)
	}
}
