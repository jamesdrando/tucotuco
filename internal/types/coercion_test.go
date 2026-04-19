package types

import (
	"testing"
)

func TestCanImplicitlyCoerce(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		from        string
		to          string
		fromUnknown bool
		toUnknown   bool
		want        bool
	}{
		{name: "unknown to integer", fromUnknown: true, to: "INTEGER", want: true},
		{name: "integer to unknown", from: "INTEGER", toUnknown: true, want: false},
		{name: "smallint to integer", from: "SMALLINT", to: "INTEGER", want: true},
		{name: "smallint to bigint", from: "SMALLINT", to: "BIGINT", want: true},
		{name: "integer to smallint", from: "INTEGER", to: "SMALLINT", want: false},
		{name: "bigint to integer", from: "BIGINT", to: "INTEGER", want: false},
		{name: "smallint to numeric exact", from: "SMALLINT", to: "NUMERIC(5,0)", want: true},
		{name: "smallint to numeric too small", from: "SMALLINT", to: "NUMERIC(4,0)", want: false},
		{name: "integer to numeric exact", from: "INTEGER", to: "NUMERIC(10,0)", want: true},
		{name: "integer to numeric too small", from: "INTEGER", to: "NUMERIC(9,0)", want: false},
		{name: "integer to numeric with scale", from: "INTEGER", to: "NUMERIC(12,2)", want: true},
		{name: "integer to numeric with insufficient scale", from: "INTEGER", to: "NUMERIC(10,2)", want: false},
		{name: "bigint to numeric exact", from: "BIGINT", to: "NUMERIC(19,0)", want: true},
		{name: "bigint to numeric too small", from: "BIGINT", to: "NUMERIC(18,0)", want: false},
		{name: "numeric to numeric widening", from: "NUMERIC(10,2)", to: "NUMERIC(12,2)", want: true},
		{name: "numeric to numeric same", from: "NUMERIC(10,2)", to: "NUMERIC(10,2)", want: true},
		{name: "numeric to numeric narrower scale", from: "NUMERIC(10,2)", to: "NUMERIC(10,1)", want: false},
		{name: "numeric to unconstrained numeric", from: "NUMERIC(10,2)", to: "NUMERIC", want: true},
		{name: "unconstrained numeric to constrained numeric", from: "NUMERIC", to: "NUMERIC(10,2)", want: false},
		{name: "decimal to numeric same family", from: "DECIMAL(10,2)", to: "NUMERIC(12,2)", want: true},
		{name: "decimal to double precision", from: "DECIMAL(10,2)", to: "DOUBLE PRECISION", want: true},
		{name: "decimal to real is not implicit", from: "DECIMAL(10,2)", to: "REAL", want: false},
		{name: "integer to double precision", from: "INTEGER", to: "DOUBLE PRECISION", want: true},
		{name: "integer to real is not implicit", from: "INTEGER", to: "REAL", want: false},
		{name: "real to double precision", from: "REAL", to: "DOUBLE PRECISION", want: true},
		{name: "double precision to real is narrowing", from: "DOUBLE PRECISION", to: "REAL", want: false},
		{name: "char to wider char", from: "CHAR(5)", to: "CHAR(10)", want: true},
		{name: "char to narrower char", from: "CHAR(10)", to: "CHAR(5)", want: false},
		{name: "varchar to wider varchar", from: "VARCHAR(5)", to: "VARCHAR(10)", want: true},
		{name: "varchar to narrower varchar", from: "VARCHAR(10)", to: "VARCHAR(5)", want: false},
		{name: "char to varchar same length", from: "CHAR(5)", to: "VARCHAR(5)", want: true},
		{name: "varchar to char same length", from: "VARCHAR(5)", to: "CHAR(5)", want: true},
		{name: "varchar to too small char", from: "VARCHAR(5)", to: "CHAR(4)", want: false},
		{name: "char to text", from: "CHAR(5)", to: "TEXT", want: true},
		{name: "varchar to text", from: "VARCHAR(10)", to: "TEXT", want: true},
		{name: "text to clob", from: "TEXT", to: "CLOB", want: true},
		{name: "clob to text is narrowing", from: "CLOB", to: "TEXT", want: false},
		{name: "varchar to clob", from: "VARCHAR(10)", to: "CLOB", want: true},
		{name: "text to varchar is narrowing", from: "TEXT", to: "VARCHAR(10)", want: false},
		{name: "binary to wider binary", from: "BINARY(4)", to: "BINARY(8)", want: true},
		{name: "binary to narrower binary", from: "BINARY(8)", to: "BINARY(4)", want: false},
		{name: "binary to varbinary", from: "BINARY(4)", to: "VARBINARY(8)", want: true},
		{name: "varbinary to wider varbinary", from: "VARBINARY(4)", to: "VARBINARY(8)", want: true},
		{name: "varbinary to narrower varbinary", from: "VARBINARY(8)", to: "VARBINARY(4)", want: false},
		{name: "varbinary to blob", from: "VARBINARY(8)", to: "BLOB", want: true},
		{name: "blob to varbinary is narrowing", from: "BLOB", to: "VARBINARY(8)", want: false},
		{name: "date to timestamp", from: "DATE", to: "TIMESTAMP", want: true},
		{name: "date to timestamp with tz", from: "DATE", to: "TIMESTAMP WITH TIME ZONE", want: true},
		{name: "timestamp to timestamp with tz", from: "TIMESTAMP(3)", to: "TIMESTAMP(6) WITH TIME ZONE", want: true},
		{name: "timestamp with tz to timestamp is narrowing", from: "TIMESTAMP WITH TIME ZONE", to: "TIMESTAMP", want: false},
		{name: "timestamp precision widening", from: "TIMESTAMP(3)", to: "TIMESTAMP(6)", want: true},
		{name: "timestamp precision narrowing", from: "TIMESTAMP(6)", to: "TIMESTAMP(3)", want: false},
		{name: "time to time with tz", from: "TIME", to: "TIME WITH TIME ZONE", want: true},
		{name: "time with tz to time is narrowing", from: "TIME WITH TIME ZONE", to: "TIME", want: false},
		{name: "boolean to boolean", from: "BOOLEAN", to: "BOOLEAN", want: true},
		{name: "boolean to integer", from: "BOOLEAN", to: "INTEGER", want: false},
		{name: "json to json", from: "JSON", to: "JSON", want: true},
		{name: "json to text", from: "JSON", to: "TEXT", want: false},
		{name: "array to array", from: "ARRAY", to: "ARRAY", want: true},
		{name: "array to row", from: "ARRAY", to: "ROW", want: false},
		{name: "row to row", from: "ROW", to: "ROW", want: true},
		{name: "row to json", from: "ROW", to: "JSON", want: false},
		{name: "nullable source still coerces", from: "INTEGER", to: "BIGINT NOT NULL", want: true},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			from := mustCoercionTypeDesc(t, testCase.from, testCase.fromUnknown)
			to := mustCoercionTypeDesc(t, testCase.to, testCase.toUnknown)
			if got := CanImplicitlyCoerce(from, to); got != testCase.want {
				t.Fatalf("CanImplicitlyCoerce(%#v, %#v) = %t, want %t", from, to, got, testCase.want)
			}
		})
	}
}

func TestCommonSuperType(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		left         string
		right        string
		leftUnknown  bool
		rightUnknown bool
		want         TypeDesc
		wantOK       bool
	}{
		{
			name:        "unknown and integer",
			leftUnknown: true,
			right:       "INTEGER",
			want:        mustTypeDesc(t, "INTEGER"),
			wantOK:      true,
		},
		{
			name:         "unknown and unknown",
			leftUnknown:  true,
			rightUnknown: true,
			want:         TypeDesc{},
			wantOK:       true,
		},
		{
			name:   "smallint and integer",
			left:   "SMALLINT",
			right:  "INTEGER",
			want:   mustTypeDesc(t, "INTEGER"),
			wantOK: true,
		},
		{
			name:   "smallint and bigint",
			left:   "SMALLINT",
			right:  "BIGINT",
			want:   mustTypeDesc(t, "BIGINT"),
			wantOK: true,
		},
		{
			name:   "integer and numeric with scale",
			left:   "INTEGER",
			right:  "NUMERIC(10,2)",
			want:   mustTypeDesc(t, "NUMERIC(12,2)"),
			wantOK: true,
		},
		{
			name:   "smallint and numeric with scale",
			left:   "SMALLINT",
			right:  "NUMERIC(10,2)",
			want:   mustTypeDesc(t, "NUMERIC(10,2)"),
			wantOK: true,
		},
		{
			name:   "numeric widening",
			left:   "NUMERIC(10,2)",
			right:  "NUMERIC(12,4)",
			want:   mustTypeDesc(t, "NUMERIC(12,4)"),
			wantOK: true,
		},
		{
			name:   "numeric and unconstrained numeric",
			left:   "NUMERIC",
			right:  "NUMERIC(10,2)",
			want:   mustTypeDesc(t, "NUMERIC"),
			wantOK: true,
		},
		{
			name:   "real and double precision",
			left:   "REAL",
			right:  "DOUBLE PRECISION",
			want:   mustTypeDesc(t, "DOUBLE PRECISION"),
			wantOK: true,
		},
		{
			name:   "integer and double precision",
			left:   "INTEGER",
			right:  "DOUBLE PRECISION",
			want:   mustTypeDesc(t, "DOUBLE PRECISION"),
			wantOK: true,
		},
		{
			name:   "char family same kind",
			left:   "CHAR(5)",
			right:  "CHAR(10)",
			want:   mustTypeDesc(t, "CHAR(10)"),
			wantOK: true,
		},
		{
			name:   "varchar family same kind",
			left:   "VARCHAR(5)",
			right:  "VARCHAR(10)",
			want:   mustTypeDesc(t, "VARCHAR(10)"),
			wantOK: true,
		},
		{
			name:   "char and varchar",
			left:   "CHAR(5)",
			right:  "VARCHAR(10)",
			want:   mustTypeDesc(t, "VARCHAR(10)"),
			wantOK: true,
		},
		{
			name:   "varchar and text",
			left:   "VARCHAR(10)",
			right:  "TEXT",
			want:   mustTypeDesc(t, "TEXT"),
			wantOK: true,
		},
		{
			name:   "text and clob",
			left:   "TEXT",
			right:  "CLOB",
			want:   mustTypeDesc(t, "CLOB"),
			wantOK: true,
		},
		{
			name:   "binary same kind",
			left:   "BINARY(4)",
			right:  "BINARY(8)",
			want:   mustTypeDesc(t, "BINARY(8)"),
			wantOK: true,
		},
		{
			name:   "binary and varbinary",
			left:   "BINARY(4)",
			right:  "VARBINARY(8)",
			want:   mustTypeDesc(t, "VARBINARY(8)"),
			wantOK: true,
		},
		{
			name:   "varbinary and blob",
			left:   "VARBINARY(8)",
			right:  "BLOB",
			want:   mustTypeDesc(t, "BLOB"),
			wantOK: true,
		},
		{
			name:   "date and timestamp",
			left:   "DATE",
			right:  "TIMESTAMP(6)",
			want:   mustTypeDesc(t, "TIMESTAMP(6)"),
			wantOK: true,
		},
		{
			name:   "date and timestamp with tz",
			left:   "DATE",
			right:  "TIMESTAMP(6) WITH TIME ZONE",
			want:   mustTypeDesc(t, "TIMESTAMP(6) WITH TIME ZONE"),
			wantOK: true,
		},
		{
			name:   "timestamp precision widening",
			left:   "TIMESTAMP(3)",
			right:  "TIMESTAMP(6)",
			want:   mustTypeDesc(t, "TIMESTAMP(6)"),
			wantOK: true,
		},
		{
			name:   "time with tz",
			left:   "TIME(3)",
			right:  "TIME(6) WITH TIME ZONE",
			want:   mustTypeDesc(t, "TIME(6) WITH TIME ZONE"),
			wantOK: true,
		},
		{
			name:   "boolean",
			left:   "BOOLEAN",
			right:  "BOOLEAN",
			want:   mustTypeDesc(t, "BOOLEAN"),
			wantOK: true,
		},
		{
			name:   "json",
			left:   "JSON",
			right:  "JSON",
			want:   mustTypeDesc(t, "JSON"),
			wantOK: true,
		},
		{
			name:   "array",
			left:   "ARRAY",
			right:  "ARRAY",
			want:   mustTypeDesc(t, "ARRAY"),
			wantOK: true,
		},
		{
			name:   "row",
			left:   "ROW",
			right:  "ROW",
			want:   mustTypeDesc(t, "ROW"),
			wantOK: true,
		},
		{
			name:   "nullable propagates",
			left:   "INTEGER NOT NULL",
			right:  "BIGINT",
			want:   mustTypeDesc(t, "BIGINT"),
			wantOK: true,
		},
		{
			name:   "incompatible families",
			left:   "INTEGER",
			right:  "TEXT",
			wantOK: false,
		},
		{
			name:   "time and date are incompatible",
			left:   "TIME",
			right:  "DATE",
			wantOK: false,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			left := mustCoercionTypeDesc(t, testCase.left, testCase.leftUnknown)
			right := mustCoercionTypeDesc(t, testCase.right, testCase.rightUnknown)
			got, ok := CommonSuperType(left, right)
			if ok != testCase.wantOK {
				t.Fatalf("CommonSuperType(%#v, %#v) ok = %t, want %t", left, right, ok, testCase.wantOK)
			}
			if !ok {
				return
			}
			if got != testCase.want {
				t.Fatalf("CommonSuperType(%#v, %#v) = %#v, want %#v", left, right, got, testCase.want)
			}
		})
	}
}

func mustCoercionTypeDesc(t *testing.T, text string, unknown bool) TypeDesc {
	t.Helper()

	if unknown {
		return TypeDesc{}
	}

	return mustTypeDesc(t, text)
}

func mustTypeDesc(t *testing.T, text string) TypeDesc {
	t.Helper()

	desc, err := ParseTypeDesc(text)
	if err != nil {
		t.Fatalf("ParseTypeDesc(%q) returned error: %v", text, err)
	}

	return desc
}
