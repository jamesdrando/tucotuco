package types

import (
	"errors"
	"testing"
)

func TestTypeKindParse(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		input string
		want  TypeKind
	}{
		{name: "canonical", input: "VARCHAR", want: TypeKindVarChar},
		{name: "lowercase alias", input: "int", want: TypeKindInteger},
		{name: "double precision alias", input: "float", want: TypeKindDoublePrecision},
		{name: "multi word", input: "timestamp with time zone", want: TypeKindTimestampWithTimeZone},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseTypeKind(testCase.input)
			if err != nil {
				t.Fatalf("ParseTypeKind(%q) returned error: %v", testCase.input, err)
			}
			if got != testCase.want {
				t.Fatalf("ParseTypeKind(%q) = %v, want %v", testCase.input, got, testCase.want)
			}
		})
	}
}

func TestTypeKindParseRejectsUnknownKind(t *testing.T) {
	t.Parallel()

	if _, err := ParseTypeKind("money"); !errors.Is(err, ErrInvalidTypeKind) {
		t.Fatalf("ParseTypeKind returned error %v, want %v", err, ErrInvalidTypeKind)
	}
}

func TestTypeDescValidate(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		desc    TypeDesc
		wantErr error
	}{
		{
			name: "varchar valid",
			desc: TypeDesc{Kind: TypeKindVarChar, Length: 128, Nullable: true},
		},
		{
			name: "decimal valid",
			desc: TypeDesc{Kind: TypeKindDecimal, Precision: 18, Scale: 6, Nullable: false},
		},
		{
			name: "timestamp precision valid",
			desc: TypeDesc{Kind: TypeKindTimestampWithTimeZone, Precision: 6, Nullable: true},
		},
		{
			name:    "missing kind",
			desc:    TypeDesc{Nullable: true},
			wantErr: ErrInvalidTypeDesc,
		},
		{
			name:    "char missing length",
			desc:    TypeDesc{Kind: TypeKindChar, Nullable: true},
			wantErr: ErrInvalidTypeDesc,
		},
		{
			name:    "boolean rejects length",
			desc:    TypeDesc{Kind: TypeKindBoolean, Length: 1, Nullable: true},
			wantErr: ErrInvalidTypeDesc,
		},
		{
			name:    "numeric scale without precision",
			desc:    TypeDesc{Kind: TypeKindNumeric, Scale: 2, Nullable: true},
			wantErr: ErrInvalidTypeDesc,
		},
		{
			name:    "numeric scale exceeds precision",
			desc:    TypeDesc{Kind: TypeKindNumeric, Precision: 4, Scale: 5, Nullable: true},
			wantErr: ErrInvalidTypeDesc,
		},
		{
			name:    "time rejects scale",
			desc:    TypeDesc{Kind: TypeKindTime, Precision: 3, Scale: 1, Nullable: true},
			wantErr: ErrInvalidTypeDesc,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			err := testCase.desc.Validate()
			if testCase.wantErr == nil {
				if err != nil {
					t.Fatalf("Validate() returned error: %v", err)
				}
				return
			}
			if !errors.Is(err, testCase.wantErr) {
				t.Fatalf("Validate() error = %v, want %v", err, testCase.wantErr)
			}
		})
	}
}

func TestTypeDescRoundTripText(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		desc TypeDesc
		want string
	}{
		{
			name: "integer nullable",
			desc: TypeDesc{Kind: TypeKindInteger, Nullable: true},
			want: "INTEGER",
		},
		{
			name: "varchar not null",
			desc: TypeDesc{Kind: TypeKindVarChar, Length: 255, Nullable: false},
			want: "VARCHAR(255) NOT NULL",
		},
		{
			name: "numeric with scale",
			desc: TypeDesc{Kind: TypeKindNumeric, Precision: 10, Scale: 2, Nullable: true},
			want: "NUMERIC(10,2)",
		},
		{
			name: "decimal precision only",
			desc: TypeDesc{Kind: TypeKindDecimal, Precision: 18, Nullable: true},
			want: "DECIMAL(18,0)",
		},
		{
			name: "time precision",
			desc: TypeDesc{Kind: TypeKindTime, Precision: 3, Nullable: true},
			want: "TIME(3)",
		},
		{
			name: "timestamp with timezone precision",
			desc: TypeDesc{Kind: TypeKindTimestampWithTimeZone, Precision: 6, Nullable: false},
			want: "TIMESTAMP(6) WITH TIME ZONE NOT NULL",
		},
		{
			name: "array",
			desc: TypeDesc{Kind: TypeKindArray, Nullable: true},
			want: "ARRAY",
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			text, err := testCase.desc.MarshalText()
			if err != nil {
				t.Fatalf("MarshalText() returned error: %v", err)
			}
			if got := string(text); got != testCase.want {
				t.Fatalf("MarshalText() = %q, want %q", got, testCase.want)
			}
			if got := testCase.desc.String(); got != testCase.want {
				t.Fatalf("String() = %q, want %q", got, testCase.want)
			}

			parsed, err := ParseTypeDesc(string(text))
			if err != nil {
				t.Fatalf("ParseTypeDesc(%q) returned error: %v", text, err)
			}
			if parsed != testCase.desc {
				t.Fatalf("ParseTypeDesc(%q) = %#v, want %#v", text, parsed, testCase.desc)
			}

			var unmarshaled TypeDesc
			if err := unmarshaled.UnmarshalText(text); err != nil {
				t.Fatalf("UnmarshalText() returned error: %v", err)
			}
			if unmarshaled != testCase.desc {
				t.Fatalf("UnmarshalText(%q) = %#v, want %#v", text, unmarshaled, testCase.desc)
			}
		})
	}
}

func TestParseTypeDescCanonicalizesAliases(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		input string
		want  TypeDesc
	}{
		{
			name:  "integer alias",
			input: "int not null",
			want:  TypeDesc{Kind: TypeKindInteger, Nullable: false},
		},
		{
			name:  "float alias",
			input: "float",
			want:  TypeDesc{Kind: TypeKindDoublePrecision, Nullable: true},
		},
		{
			name:  "explicit null",
			input: "numeric(10,2) null",
			want:  TypeDesc{Kind: TypeKindNumeric, Precision: 10, Scale: 2, Nullable: true},
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseTypeDesc(testCase.input)
			if err != nil {
				t.Fatalf("ParseTypeDesc(%q) returned error: %v", testCase.input, err)
			}
			if got != testCase.want {
				t.Fatalf("ParseTypeDesc(%q) = %#v, want %#v", testCase.input, got, testCase.want)
			}
		})
	}
}

func TestParseTypeDescRejectsInvalidText(t *testing.T) {
	t.Parallel()

	testCases := []string{
		"",
		"VARCHAR",
		"VARCHAR(0)",
		"NUMERIC(10,11)",
		"BOOLEAN(1)",
		"TIME WITH TIME ZONE(3)",
		"NUMERIC(10,)",
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase, func(t *testing.T) {
			t.Parallel()

			if _, err := ParseTypeDesc(testCase); !errors.Is(err, ErrInvalidTypeDesc) {
				t.Fatalf("ParseTypeDesc(%q) error = %v, want %v", testCase, err, ErrInvalidTypeDesc)
			}
		})
	}
}

func TestTypeDescMarshalRejectsInvalidDescriptor(t *testing.T) {
	t.Parallel()

	desc := TypeDesc{Kind: TypeKindBoolean, Length: 1, Nullable: true}
	if _, err := desc.MarshalText(); !errors.Is(err, ErrInvalidTypeDesc) {
		t.Fatalf("MarshalText() error = %v, want %v", err, ErrInvalidTypeDesc)
	}
}
