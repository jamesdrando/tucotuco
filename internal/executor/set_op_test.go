package executor

import (
	"errors"
	"io"
	"testing"

	sqltypes "github.com/jamesdrando/tucotuco/internal/types"
)

func TestSetOpUnionDistinctAndAll(t *testing.T) {
	t.Parallel()

	distinct := NewSetOp(
		&setOpTestOperator{rows: []Row{
			NewRow(sqltypes.Int32Value(1)),
			NewRow(sqltypes.Int32Value(1)),
		}},
		&setOpTestOperator{rows: []Row{
			NewRow(sqltypes.Int32Value(1)),
			NewRow(sqltypes.Int32Value(2)),
		}},
		"UNION",
		"",
		[]sqltypes.TypeDesc{{Kind: sqltypes.TypeKindInteger}},
		[]sqltypes.TypeDesc{{Kind: sqltypes.TypeKindInteger}},
		[]sqltypes.TypeDesc{{Kind: sqltypes.TypeKindInteger}},
	)
	assertSetOpRows(t, distinct, []Row{
		NewRow(sqltypes.Int32Value(1)),
		NewRow(sqltypes.Int32Value(2)),
	})

	all := NewSetOp(
		&setOpTestOperator{rows: []Row{
			NewRow(sqltypes.Int32Value(1)),
			NewRow(sqltypes.Int32Value(1)),
		}},
		&setOpTestOperator{rows: []Row{
			NewRow(sqltypes.Int32Value(1)),
			NewRow(sqltypes.Int32Value(2)),
		}},
		"UNION",
		"ALL",
		[]sqltypes.TypeDesc{{Kind: sqltypes.TypeKindInteger}},
		[]sqltypes.TypeDesc{{Kind: sqltypes.TypeKindInteger}},
		[]sqltypes.TypeDesc{{Kind: sqltypes.TypeKindInteger}},
	)
	assertSetOpRows(t, all, []Row{
		NewRow(sqltypes.Int32Value(1)),
		NewRow(sqltypes.Int32Value(1)),
		NewRow(sqltypes.Int32Value(1)),
		NewRow(sqltypes.Int32Value(2)),
	})
}

func TestSetOpIntersectAndExceptAll(t *testing.T) {
	t.Parallel()

	intersect := NewSetOp(
		&setOpTestOperator{rows: []Row{
			NewRow(sqltypes.Int32Value(1)),
			NewRow(sqltypes.Int32Value(1)),
			NewRow(sqltypes.Int32Value(2)),
		}},
		&setOpTestOperator{rows: []Row{
			NewRow(sqltypes.Int32Value(1)),
			NewRow(sqltypes.Int32Value(1)),
			NewRow(sqltypes.Int32Value(3)),
		}},
		"INTERSECT",
		"ALL",
		[]sqltypes.TypeDesc{{Kind: sqltypes.TypeKindInteger}},
		[]sqltypes.TypeDesc{{Kind: sqltypes.TypeKindInteger}},
		[]sqltypes.TypeDesc{{Kind: sqltypes.TypeKindInteger}},
	)
	assertSetOpRows(t, intersect, []Row{
		NewRow(sqltypes.Int32Value(1)),
		NewRow(sqltypes.Int32Value(1)),
	})

	except := NewSetOp(
		&setOpTestOperator{rows: []Row{
			NewRow(sqltypes.Int32Value(1)),
			NewRow(sqltypes.Int32Value(1)),
			NewRow(sqltypes.Int32Value(2)),
		}},
		&setOpTestOperator{rows: []Row{
			NewRow(sqltypes.Int32Value(1)),
		}},
		"EXCEPT",
		"ALL",
		[]sqltypes.TypeDesc{{Kind: sqltypes.TypeKindInteger}},
		[]sqltypes.TypeDesc{{Kind: sqltypes.TypeKindInteger}},
		[]sqltypes.TypeDesc{{Kind: sqltypes.TypeKindInteger}},
	)
	assertSetOpRows(t, except, []Row{
		NewRow(sqltypes.Int32Value(1)),
		NewRow(sqltypes.Int32Value(2)),
	})
}

func TestSetOpCoercesToOutputTypes(t *testing.T) {
	t.Parallel()

	op := NewSetOp(
		&setOpTestOperator{rows: []Row{
			NewRow(sqltypes.Int32Value(1)),
		}},
		&setOpTestOperator{rows: []Row{
			NewRow(sqltypes.Int64Value(2)),
		}},
		"UNION",
		"ALL",
		[]sqltypes.TypeDesc{{Kind: sqltypes.TypeKindInteger}},
		[]sqltypes.TypeDesc{{Kind: sqltypes.TypeKindBigInt}},
		[]sqltypes.TypeDesc{{Kind: sqltypes.TypeKindBigInt}},
	)

	rows := collectSetOpRows(t, op)
	if got, ok := rows[0].Value(0); !ok || got.Kind() != sqltypes.ValueKindInt64 || !got.Equal(sqltypes.Int64Value(1)) {
		t.Fatalf("first coerced value = (%v, %t), want BIGINT 1", got, ok)
	}
	if got, ok := rows[1].Value(0); !ok || got.Kind() != sqltypes.ValueKindInt64 || !got.Equal(sqltypes.Int64Value(2)) {
		t.Fatalf("second coerced value = (%v, %t), want BIGINT 2", got, ok)
	}
}

func TestSetOpRejectsNilChild(t *testing.T) {
	t.Parallel()

	op := NewSetOp(nil, &setOpTestOperator{}, "UNION", "", nil, nil, nil)
	if err := op.Open(); !errors.Is(err, errSetOpNilChild) {
		t.Fatalf("Open() error = %v, want %v", err, errSetOpNilChild)
	}
}

type setOpTestOperator struct {
	rows   []Row
	index  int
	open   bool
	closed bool
}

func (s *setOpTestOperator) Open() error {
	if s.closed {
		return ErrOperatorClosed
	}
	if s.open {
		return ErrOperatorOpen
	}
	s.open = true
	s.index = 0
	return nil
}

func (s *setOpTestOperator) Next() (Row, error) {
	if !s.open {
		if s.closed {
			return Row{}, ErrOperatorClosed
		}
		return Row{}, ErrOperatorNotOpen
	}
	if s.index >= len(s.rows) {
		return Row{}, io.EOF
	}

	row := s.rows[s.index].Clone()
	s.index++
	return row, nil
}

func (s *setOpTestOperator) Close() error {
	s.open = false
	s.closed = true
	return nil
}

func collectSetOpRows(t *testing.T, op *SetOp) []Row {
	t.Helper()

	if err := op.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := op.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	rows := make([]Row, 0)
	for {
		row, err := op.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return rows
			}
			t.Fatalf("Next() error = %v", err)
		}
		rows = append(rows, row)
	}
}

func assertSetOpRows(t *testing.T, op *SetOp, want []Row) {
	t.Helper()

	got := collectSetOpRows(t, op)
	if len(got) != len(want) {
		t.Fatalf("len(rows) = %d, want %d", len(got), len(want))
	}
	for index := range want {
		if got[index].Len() != want[index].Len() {
			t.Fatalf("row %d len = %d, want %d", index, got[index].Len(), want[index].Len())
		}
		for valueIndex, expected := range want[index].Values() {
			value, ok := got[index].Value(valueIndex)
			if !ok {
				t.Fatalf("row %d value %d missing", index, valueIndex)
			}
			if !value.Equal(expected) {
				t.Fatalf("row %d value %d = %v, want %v", index, valueIndex, value, expected)
			}
		}
	}
}
