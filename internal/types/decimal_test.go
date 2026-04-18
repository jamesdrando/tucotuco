package types

import (
	"math/big"
	"testing"
)

func TestParseDecimalCanonicalString(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		input string
		want  string
	}{
		{name: "zero", input: "0", want: "0"},
		{name: "fraction", input: "001.2300", want: "1.23"},
		{name: "negative", input: "-10.500", want: "-10.5"},
		{name: "scientific positive", input: "1.23e3", want: "1230"},
		{name: "scientific negative", input: "1.2300e-2", want: "0.0123"},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			decimal, err := ParseDecimal(testCase.input)
			if err != nil {
				t.Fatalf("ParseDecimal(%q) returned error: %v", testCase.input, err)
			}
			if got := decimal.String(); got != testCase.want {
				t.Fatalf("ParseDecimal(%q).String() = %q, want %q", testCase.input, got, testCase.want)
			}
		})
	}
}

func TestDecimalCompareNormalization(t *testing.T) {
	t.Parallel()

	left, err := NewDecimal(big.NewInt(1230), 3)
	if err != nil {
		t.Fatalf("NewDecimal returned error: %v", err)
	}
	right, err := ParseDecimal("1.23")
	if err != nil {
		t.Fatalf("ParseDecimal returned error: %v", err)
	}
	if got := left.Compare(right); got != 0 {
		t.Fatalf("left.Compare(right) = %d, want 0", got)
	}
	if !left.Equal(right) {
		t.Fatal("left.Equal(right) = false, want true")
	}
}

func TestDecimalNegativeScale(t *testing.T) {
	t.Parallel()

	left, err := NewDecimal(big.NewInt(1200), -1)
	if err != nil {
		t.Fatalf("NewDecimal returned error: %v", err)
	}
	right, err := ParseDecimal("12000")
	if err != nil {
		t.Fatalf("ParseDecimal returned error: %v", err)
	}
	if got := left.String(); got != "12000" {
		t.Fatalf("left.String() = %q, want %q", got, "12000")
	}
	if got := left.Compare(right); got != 0 {
		t.Fatalf("left.Compare(right) = %d, want 0", got)
	}
}

func TestParseDecimalRejectsInvalidText(t *testing.T) {
	t.Parallel()

	testCases := []string{"", "-", ".", "1.2.3", "1e", "abc"}
	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase, func(t *testing.T) {
			t.Parallel()

			if _, err := ParseDecimal(testCase); err == nil {
				t.Fatalf("ParseDecimal(%q) returned nil error, want failure", testCase)
			}
		})
	}
}
