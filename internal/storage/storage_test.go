package storage

import (
	"bytes"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/types"
)

func TestRowDefendsAgainstMutablePayloadLeaks(t *testing.T) {
	t.Parallel()

	inputs := []types.Value{
		types.StringValue("alpha"),
		types.BytesValue([]byte{1, 2, 3}),
		types.ArrayValue(types.Array{
			types.Int32Value(7),
			types.BytesValue([]byte{4, 5}),
		}),
		types.RowValue(types.Row{
			types.StringValue("nested"),
			types.BytesValue([]byte{6, 7}),
		}),
	}

	row := NewRow(inputs...)
	inputs[0] = types.StringValue("beta")

	values := row.Values()
	values[0] = types.StringValue("gamma")

	got, ok := row.Value(0)
	if !ok || !got.Equal(types.StringValue("alpha")) {
		t.Fatalf("Value(0) = (%v, %t), want (%v, true)", got, ok, types.StringValue("alpha"))
	}

	bytesValue, ok := row.Value(1)
	if !ok {
		t.Fatal("Value(1) = (_, false), want (_, true)")
	}
	mutatedBytes := bytesValue.Raw().([]byte)
	mutatedBytes[0] = 99

	arrayValue, ok := row.Value(2)
	if !ok {
		t.Fatal("Value(2) = (_, false), want (_, true)")
	}
	mutatedArray := arrayValue.Raw().(types.Array)
	mutatedArray[0] = types.Int32Value(99)
	mutatedNestedBytes := mutatedArray[1].Raw().([]byte)
	mutatedNestedBytes[0] = 88

	rowValue, ok := row.Value(3)
	if !ok {
		t.Fatal("Value(3) = (_, false), want (_, true)")
	}
	mutatedRow := rowValue.Raw().(types.Row)
	mutatedRow[0] = types.StringValue("changed")
	mutatedRowBytes := mutatedRow[1].Raw().([]byte)
	mutatedRowBytes[0] = 77

	clone := row.Clone()
	clone.values[0] = types.StringValue("delta")

	got, ok = row.Value(1)
	if !ok {
		t.Fatal("Value(1) = (_, false), want (_, true)")
	}
	if !bytes.Equal(got.Raw().([]byte), []byte{1, 2, 3}) {
		t.Fatalf("Value(1) bytes = %v, want %v", got.Raw(), []byte{1, 2, 3})
	}

	got, ok = row.Value(2)
	if !ok {
		t.Fatal("Value(2) = (_, false), want (_, true)")
	}
	gotArray := got.Raw().(types.Array)
	if !gotArray[0].Equal(types.Int32Value(7)) {
		t.Fatalf("Value(2)[0] = %v, want %v", gotArray[0], types.Int32Value(7))
	}
	if !bytes.Equal(gotArray[1].Raw().([]byte), []byte{4, 5}) {
		t.Fatalf("Value(2)[1] bytes = %v, want %v", gotArray[1].Raw(), []byte{4, 5})
	}

	got, ok = row.Value(3)
	if !ok {
		t.Fatal("Value(3) = (_, false), want (_, true)")
	}
	gotRow := got.Raw().(types.Row)
	if !gotRow[0].Equal(types.StringValue("nested")) {
		t.Fatalf("Value(3)[0] = %v, want %v", gotRow[0], types.StringValue("nested"))
	}
	if !bytes.Equal(gotRow[1].Raw().([]byte), []byte{6, 7}) {
		t.Fatalf("Value(3)[1] bytes = %v, want %v", gotRow[1].Raw(), []byte{6, 7})
	}

	if _, ok := row.Value(-1); ok {
		t.Fatal("Value(-1) reported ok for an invalid index")
	}

	if _, ok := row.Value(row.Len()); ok {
		t.Fatal("Value(Len()) reported ok for an invalid index")
	}
}

func TestTableIDFormatting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		id    TableID
		want  string
		valid bool
	}{
		{
			name:  "unqualified",
			id:    TableID{Name: "widgets"},
			want:  "widgets",
			valid: true,
		},
		{
			name:  "qualified",
			id:    TableID{Schema: "public", Name: "widgets"},
			want:  "public.widgets",
			valid: true,
		},
		{
			name:  "invalid",
			id:    TableID{Schema: "public"},
			want:  "public",
			valid: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := tc.id.String(); got != tc.want {
				t.Fatalf("String() = %q, want %q", got, tc.want)
			}

			if got := tc.id.Valid(); got != tc.valid {
				t.Fatalf("Valid() = %t, want %t", got, tc.valid)
			}
		})
	}
}

func TestRowHandleValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		handle RowHandle
		valid  bool
	}{
		{
			name:   "zero",
			handle: RowHandle{},
			valid:  false,
		},
		{
			name:   "slot only",
			handle: RowHandle{Slot: 1},
			valid:  true,
		},
		{
			name:   "page and slot",
			handle: RowHandle{Page: 3, Slot: 9},
			valid:  true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := tc.handle.Valid(); got != tc.valid {
				t.Fatalf("Valid() = %t, want %t", got, tc.valid)
			}

			if got := tc.handle.String(); got == "" {
				t.Fatal("String() returned an empty value")
			}
		})
	}
}

func TestScanOptionsNormalized(t *testing.T) {
	t.Parallel()

	options := ScanOptions{
		Constraints: []ScanConstraint{
			{Column: 1, Op: ComparisonGreaterOrEqual, Value: types.BytesValue([]byte{1, 2, 3})},
		},
		Limit: -5,
	}

	normalized := options.Normalized()
	normalized.Constraints[0].Column = 7
	normalized.Constraints[0].Value = types.BytesValue([]byte{9, 9, 9})

	if normalized.Limit != 0 {
		t.Fatalf("normalized Limit = %d, want %d", normalized.Limit, 0)
	}

	if options.Constraints[0].Column != 1 {
		t.Fatalf("original constraint column = %d, want %d", options.Constraints[0].Column, 1)
	}

	rawConstraint := options.Constraints[0].Value.Raw().([]byte)
	if !bytes.Equal(rawConstraint, []byte{1, 2, 3}) {
		t.Fatalf("original constraint bytes = %v, want %v", rawConstraint, []byte{1, 2, 3})
	}
}

func TestComparisonOpString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		op   ComparisonOp
		want string
	}{
		{op: ComparisonEqual, want: "="},
		{op: ComparisonNotEqual, want: "<>"},
		{op: ComparisonLess, want: "<"},
		{op: ComparisonLessOrEqual, want: "<="},
		{op: ComparisonGreater, want: ">"},
		{op: ComparisonGreaterOrEqual, want: ">="},
		{op: ComparisonOp(99), want: "UNKNOWN"},
	}

	for _, tc := range tests {
		if got := tc.op.String(); got != tc.want {
			t.Fatalf("String() = %q, want %q", got, tc.want)
		}
	}
}

func TestTransactionOptionsNormalized(t *testing.T) {
	t.Parallel()

	normalized := (TransactionOptions{}).Normalized()
	if normalized.Isolation != IsolationReadCommitted {
		t.Fatalf("default isolation = %q, want %q", normalized.Isolation, IsolationReadCommitted)
	}

	custom := TransactionOptions{
		Isolation: IsolationSerializable,
		ReadOnly:  true,
	}.Normalized()

	if custom.Isolation != IsolationSerializable {
		t.Fatalf("custom isolation = %q, want %q", custom.Isolation, IsolationSerializable)
	}

	if !custom.ReadOnly {
		t.Fatal("custom ReadOnly = false, want true")
	}
}
