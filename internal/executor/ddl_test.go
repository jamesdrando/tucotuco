package executor

import (
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/catalog"
	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/types"
)

func TestDDLOperatorsLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		operator func(t *testing.T) Operator
	}{
		{
			name: "create schema",
			operator: func(t *testing.T) Operator {
				t.Helper()

				return NewCreateSchema(catalog.NewMemory(), &catalog.SchemaDescriptor{Name: "tenant"})
			},
		},
		{
			name: "drop schema",
			operator: func(t *testing.T) Operator {
				t.Helper()

				cat := catalog.NewMemory()
				mustCreateDDLSchema(t, cat, "tenant")

				return NewDropSchema(cat, "tenant")
			},
		},
		{
			name: "create table",
			operator: func(t *testing.T) Operator {
				t.Helper()

				return NewCreateTable(newDDLCatalog(t), nil, nil, ddlTableDescriptor())
			},
		},
		{
			name: "drop table",
			operator: func(t *testing.T) Operator {
				t.Helper()

				cat := newDDLCatalog(t)
				mustCreateDDLTable(t, cat, ddlTableDescriptor())

				return NewDropTable(cat, nil, nil, ddlTableDescriptor().ID)
			},
		},
		{
			name: "create view",
			operator: func(t *testing.T) Operator {
				t.Helper()

				return NewCreateView(newDDLCatalog(t), ddlViewDescriptor())
			},
		},
		{
			name: "drop view",
			operator: func(t *testing.T) Operator {
				t.Helper()

				cat := newDDLCatalog(t)
				if err := cat.CreateView(ddlViewDescriptor()); err != nil {
					t.Fatalf("CreateView() error = %v", err)
				}

				return NewDropView(cat, ddlViewDescriptor().ID)
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			operator := tc.operator(t)

			if _, err := operator.Next(); !errors.Is(err, ErrOperatorNotOpen) {
				t.Fatalf("Next() before Open error = %v, want %v", err, ErrOperatorNotOpen)
			}

			if err := operator.Open(); err != nil {
				t.Fatalf("Open() error = %v", err)
			}

			if err := operator.Open(); !errors.Is(err, ErrOperatorOpen) {
				t.Fatalf("second Open() error = %v, want %v", err, ErrOperatorOpen)
			}

			if err := operator.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}

			if _, err := operator.Next(); !errors.Is(err, ErrOperatorClosed) {
				t.Fatalf("Next() after Close error = %v, want %v", err, ErrOperatorClosed)
			}

			if err := operator.Open(); !errors.Is(err, ErrOperatorClosed) {
				t.Fatalf("Open() after Close error = %v, want %v", err, ErrOperatorClosed)
			}

			if err := operator.Close(); err != nil {
				t.Fatalf("second Close() error = %v", err)
			}
		})
	}
}

func TestDDLOperatorsNextReturnsEOFRepeatedly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		operator func(t *testing.T) Operator
	}{
		{
			name: "create schema",
			operator: func(t *testing.T) Operator {
				t.Helper()

				cat := catalog.NewMemory()
				t.Cleanup(func() {
					if err := cat.CreateTable(ddlTableDescriptorInSchema("tenant")); err != nil {
						t.Fatalf("CreateTable() after schema create error = %v", err)
					}
				})

				return NewCreateSchema(cat, &catalog.SchemaDescriptor{Name: "tenant"})
			},
		},
		{
			name: "drop schema",
			operator: func(t *testing.T) Operator {
				t.Helper()

				cat := catalog.NewMemory()
				mustCreateDDLSchema(t, cat, "tenant")
				t.Cleanup(func() {
					if err := cat.CreateTable(ddlTableDescriptorInSchema("tenant")); !errors.Is(err, catalog.ErrSchemaNotFound) {
						t.Fatalf("CreateTable() after schema drop error = %v, want %v", err, catalog.ErrSchemaNotFound)
					}
				})

				return NewDropSchema(cat, "tenant")
			},
		},
		{
			name: "create table",
			operator: func(t *testing.T) Operator {
				t.Helper()

				cat := newDDLCatalog(t)
				desc := ddlTableDescriptor()
				t.Cleanup(func() {
					got, err := cat.LookupTable(desc.ID)
					if err != nil {
						t.Fatalf("LookupTable() error = %v", err)
					}
					if !reflect.DeepEqual(got, desc) {
						t.Fatalf("LookupTable() = %#v, want %#v", got, desc)
					}
				})

				return NewCreateTable(cat, nil, nil, desc)
			},
		},
		{
			name: "drop table",
			operator: func(t *testing.T) Operator {
				t.Helper()

				cat := newDDLCatalog(t)
				desc := ddlTableDescriptor()
				mustCreateDDLTable(t, cat, desc)
				t.Cleanup(func() {
					if _, err := cat.LookupTable(desc.ID); !errors.Is(err, catalog.ErrTableNotFound) {
						t.Fatalf("LookupTable() after drop error = %v, want %v", err, catalog.ErrTableNotFound)
					}
				})

				return NewDropTable(cat, nil, nil, desc.ID)
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			operator := tc.operator(t)

			if err := operator.Open(); err != nil {
				t.Fatalf("Open() error = %v", err)
			}

			if _, err := operator.Next(); !errors.Is(err, io.EOF) {
				t.Fatalf("first Next() error = %v, want %v", err, io.EOF)
			}

			if _, err := operator.Next(); !errors.Is(err, io.EOF) {
				t.Fatalf("second Next() error = %v, want %v", err, io.EOF)
			}

			if err := operator.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
		})
	}
}

func TestCreateSchemaUsesCatalogValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(t *testing.T, cat *catalog.Memory)
		desc  *catalog.SchemaDescriptor
		want  error
	}{
		{
			name: "duplicate schema",
			setup: func(t *testing.T, cat *catalog.Memory) {
				t.Helper()

				mustCreateDDLSchema(t, cat, "tenant")
			},
			desc: &catalog.SchemaDescriptor{Name: "tenant"},
			want: catalog.ErrSchemaExists,
		},
		{
			name:  "invalid schema descriptor",
			setup: func(*testing.T, *catalog.Memory) {},
			desc:  &catalog.SchemaDescriptor{Name: ""},
			want:  catalog.ErrInvalidSchemaDescriptor,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cat := catalog.NewMemory()
			tc.setup(t, cat)

			operator := NewCreateSchema(cat, tc.desc)

			if err := operator.Open(); !errors.Is(err, tc.want) {
				t.Fatalf("Open() error = %v, want %v", err, tc.want)
			}

			if _, err := operator.Next(); !errors.Is(err, ErrOperatorNotOpen) {
				t.Fatalf("Next() after failed Open error = %v, want %v", err, ErrOperatorNotOpen)
			}

			if err := operator.Close(); err != nil {
				t.Fatalf("Close() after failed Open error = %v", err)
			}

			if err := operator.Close(); err != nil {
				t.Fatalf("second Close() after failed Open error = %v", err)
			}
		})
	}
}

func TestDropSchemaUsesCatalogValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, cat *catalog.Memory)
		schemaName string
		want       error
	}{
		{
			name:       "missing schema",
			setup:      func(*testing.T, *catalog.Memory) {},
			schemaName: "missing",
			want:       catalog.ErrSchemaNotFound,
		},
		{
			name: "non-empty schema",
			setup: func(t *testing.T, cat *catalog.Memory) {
				t.Helper()

				mustCreateDDLSchema(t, cat, "tenant")
				mustCreateDDLTable(t, cat, ddlTableDescriptorInSchema("tenant"))
			},
			schemaName: "tenant",
			want:       catalog.ErrSchemaNotEmpty,
		},
		{
			name:       "invalid schema name",
			setup:      func(*testing.T, *catalog.Memory) {},
			schemaName: "",
			want:       catalog.ErrInvalidSchemaName,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cat := catalog.NewMemory()
			tc.setup(t, cat)

			operator := NewDropSchema(cat, tc.schemaName)

			if err := operator.Open(); !errors.Is(err, tc.want) {
				t.Fatalf("Open() error = %v, want %v", err, tc.want)
			}

			if _, err := operator.Next(); !errors.Is(err, ErrOperatorNotOpen) {
				t.Fatalf("Next() after failed Open error = %v, want %v", err, ErrOperatorNotOpen)
			}

			if err := operator.Close(); err != nil {
				t.Fatalf("Close() after failed Open error = %v", err)
			}

			if err := operator.Close(); err != nil {
				t.Fatalf("second Close() after failed Open error = %v", err)
			}
		})
	}
}

func TestCreateTableUsesCatalogValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(t *testing.T, cat *catalog.Memory)
		desc  *catalog.TableDescriptor
		want  error
	}{
		{
			name: "duplicate table",
			setup: func(t *testing.T, cat *catalog.Memory) {
				t.Helper()

				mustCreateDDLTable(t, cat, ddlTableDescriptor())
			},
			desc: ddlTableDescriptor(),
			want: catalog.ErrTableExists,
		},
		{
			name:  "missing schema",
			setup: func(*testing.T, *catalog.Memory) {},
			desc: func() *catalog.TableDescriptor {
				desc := ddlTableDescriptor()
				desc.ID.Schema = "missing"

				return desc
			}(),
			want: catalog.ErrSchemaNotFound,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cat := newDDLCatalog(t)
			tc.setup(t, cat)

			operator := NewCreateTable(cat, nil, nil, tc.desc)

			if err := operator.Open(); !errors.Is(err, tc.want) {
				t.Fatalf("Open() error = %v, want %v", err, tc.want)
			}

			if _, err := operator.Next(); !errors.Is(err, ErrOperatorNotOpen) {
				t.Fatalf("Next() after failed Open error = %v, want %v", err, ErrOperatorNotOpen)
			}

			if err := operator.Close(); err != nil {
				t.Fatalf("Close() after failed Open error = %v", err)
			}

			if err := operator.Close(); err != nil {
				t.Fatalf("second Close() after failed Open error = %v", err)
			}
		})
	}
}

func TestDropTableUsesCatalogValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		table storage.TableID
		want  error
	}{
		{
			name:  "missing table",
			table: storage.TableID{Schema: "public", Name: "missing"},
			want:  catalog.ErrTableNotFound,
		},
		{
			name:  "missing schema",
			table: storage.TableID{Schema: "missing", Name: "widgets"},
			want:  catalog.ErrSchemaNotFound,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			operator := NewDropTable(newDDLCatalog(t), nil, nil, tc.table)

			if err := operator.Open(); !errors.Is(err, tc.want) {
				t.Fatalf("Open() error = %v, want %v", err, tc.want)
			}

			if _, err := operator.Next(); !errors.Is(err, ErrOperatorNotOpen) {
				t.Fatalf("Next() after failed Open error = %v, want %v", err, ErrOperatorNotOpen)
			}

			if err := operator.Close(); err != nil {
				t.Fatalf("Close() after failed Open error = %v", err)
			}

			if err := operator.Close(); err != nil {
				t.Fatalf("second Close() after failed Open error = %v", err)
			}
		})
	}
}

func TestCreateTableCoordinatorFailureRollsBackCatalogAndAllowsRetry(t *testing.T) {
	t.Parallel()

	cat := newDDLCatalog(t)
	desc := ddlTableDescriptor()
	tx := &stubDDLTx{}
	coordErr := errors.New("create table coordination failed")
	coordinator := &stubTableCoordinator{
		createErrs: []error{coordErr},
	}
	operator := NewCreateTable(cat, tx, coordinator, desc)

	if err := operator.Open(); !errors.Is(err, coordErr) {
		t.Fatalf("Open() error = %v, want %v", err, coordErr)
	}

	if _, err := cat.LookupTable(desc.ID); !errors.Is(err, catalog.ErrTableNotFound) {
		t.Fatalf("LookupTable() after failed Open error = %v, want %v", err, catalog.ErrTableNotFound)
	}

	if _, err := operator.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() after failed Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	if coordinator.createCalls != 1 {
		t.Fatalf("CreateTable() coordination calls = %d, want 1", coordinator.createCalls)
	}
	if len(coordinator.createTxs) != 1 || coordinator.createTxs[0] != tx {
		t.Fatalf("CreateTable() coordination txs = %#v, want %#v", coordinator.createTxs, tx)
	}
	if len(coordinator.createDescs) != 1 || !reflect.DeepEqual(coordinator.createDescs[0], desc) {
		t.Fatalf("CreateTable() coordination descs = %#v, want %#v", coordinator.createDescs, desc)
	}

	if err := operator.Open(); err != nil {
		t.Fatalf("second Open() error = %v", err)
	}

	got, err := cat.LookupTable(desc.ID)
	if err != nil {
		t.Fatalf("LookupTable() after retry error = %v", err)
	}
	if !reflect.DeepEqual(got, desc) {
		t.Fatalf("LookupTable() after retry = %#v, want %#v", got, desc)
	}

	if coordinator.createCalls != 2 {
		t.Fatalf("CreateTable() coordination calls after retry = %d, want 2", coordinator.createCalls)
	}

	if _, err := operator.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("Next() after successful retry error = %v, want %v", err, io.EOF)
	}

	if err := operator.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestDropTableCoordinatorFailureRollsBackCatalogAndAllowsRetry(t *testing.T) {
	t.Parallel()

	cat := newDDLCatalog(t)
	desc := ddlTableDescriptor()
	mustCreateDDLTable(t, cat, desc)

	tx := &stubDDLTx{}
	coordErr := errors.New("drop table coordination failed")
	coordinator := &stubTableCoordinator{
		dropErrs: []error{coordErr},
	}
	operator := NewDropTable(cat, tx, coordinator, desc.ID)

	if err := operator.Open(); !errors.Is(err, coordErr) {
		t.Fatalf("Open() error = %v, want %v", err, coordErr)
	}

	got, err := cat.LookupTable(desc.ID)
	if err != nil {
		t.Fatalf("LookupTable() after failed Open error = %v", err)
	}
	if !reflect.DeepEqual(got, desc) {
		t.Fatalf("LookupTable() after failed Open = %#v, want %#v", got, desc)
	}

	if _, err := operator.Next(); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Next() after failed Open error = %v, want %v", err, ErrOperatorNotOpen)
	}

	if coordinator.dropCalls != 1 {
		t.Fatalf("DropTable() coordination calls = %d, want 1", coordinator.dropCalls)
	}
	if len(coordinator.dropTxs) != 1 || coordinator.dropTxs[0] != tx {
		t.Fatalf("DropTable() coordination txs = %#v, want %#v", coordinator.dropTxs, tx)
	}
	if len(coordinator.dropDescs) != 1 || !reflect.DeepEqual(coordinator.dropDescs[0], desc) {
		t.Fatalf("DropTable() coordination descs = %#v, want %#v", coordinator.dropDescs, desc)
	}

	if err := operator.Open(); err != nil {
		t.Fatalf("second Open() error = %v", err)
	}

	if _, err := cat.LookupTable(desc.ID); !errors.Is(err, catalog.ErrTableNotFound) {
		t.Fatalf("LookupTable() after retry error = %v, want %v", err, catalog.ErrTableNotFound)
	}

	if coordinator.dropCalls != 2 {
		t.Fatalf("DropTable() coordination calls after retry = %d, want 2", coordinator.dropCalls)
	}

	if _, err := operator.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("Next() after successful retry error = %v, want %v", err, io.EOF)
	}

	if err := operator.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func newDDLCatalog(t *testing.T) *catalog.Memory {
	t.Helper()

	cat := catalog.NewMemory()
	mustCreateDDLSchema(t, cat, "public")

	return cat
}

func mustCreateDDLSchema(t *testing.T, cat *catalog.Memory, name string) {
	t.Helper()

	if err := cat.CreateSchema(&catalog.SchemaDescriptor{Name: name}); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}
}

func mustCreateDDLTable(t *testing.T, cat *catalog.Memory, desc *catalog.TableDescriptor) {
	t.Helper()

	if err := cat.CreateTable(desc); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}
}

func ddlTableDescriptor() *catalog.TableDescriptor {
	return &catalog.TableDescriptor{
		ID: storage.TableID{Schema: "public", Name: "widgets"},
		Columns: []catalog.ColumnDescriptor{
			{
				Name: "id",
				Type: types.TypeDesc{Kind: types.TypeKindInteger},
				Constraints: []catalog.ConstraintDescriptor{
					{Name: "widgets_id_pk", Kind: catalog.ConstraintKindPrimaryKey},
				},
			},
			{
				Name:    "name",
				Type:    types.TypeDesc{Kind: types.TypeKindVarChar, Length: 32, Nullable: true},
				Default: &catalog.ExpressionDescriptor{SQL: "'unknown'"},
				Constraints: []catalog.ConstraintDescriptor{
					{Name: "widgets_name_nn", Kind: catalog.ConstraintKindNotNull},
					{Name: "widgets_name_unique", Kind: catalog.ConstraintKindUnique},
				},
			},
		},
		Constraints: []catalog.ConstraintDescriptor{
			{
				Name:       "widgets_name_check",
				Kind:       catalog.ConstraintKindCheck,
				Expression: &catalog.ExpressionDescriptor{SQL: "char_length(name) > 0"},
			},
		},
	}
}

func ddlTableDescriptorInSchema(schema string) *catalog.TableDescriptor {
	desc := ddlTableDescriptor()
	desc.ID.Schema = schema

	return desc
}

func ddlViewDescriptor() *catalog.ViewDescriptor {
	return &catalog.ViewDescriptor{
		ID: storage.TableID{Schema: "public", Name: "widget_names"},
		Columns: []catalog.ColumnDescriptor{
			{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindInteger}},
			{Name: "name", Type: types.TypeDesc{Kind: types.TypeKindVarChar, Length: 32, Nullable: true}},
		},
		Query: catalog.ExpressionDescriptor{SQL: "SELECT id, name FROM widgets"},
	}
}

type stubTableCoordinator struct {
	createErrs []error
	dropErrs   []error

	createCalls int
	dropCalls   int

	createTxs []storage.Transaction
	dropTxs   []storage.Transaction

	createDescs []*catalog.TableDescriptor
	dropDescs   []*catalog.TableDescriptor
}

func (s *stubTableCoordinator) CreateTable(tx storage.Transaction, desc *catalog.TableDescriptor) error {
	s.createCalls++
	s.createTxs = append(s.createTxs, tx)
	s.createDescs = append(s.createDescs, desc)

	if len(s.createErrs) == 0 {
		return nil
	}

	err := s.createErrs[0]
	s.createErrs = s.createErrs[1:]

	return err
}

func (s *stubTableCoordinator) DropTable(tx storage.Transaction, desc *catalog.TableDescriptor) error {
	s.dropCalls++
	s.dropTxs = append(s.dropTxs, tx)
	s.dropDescs = append(s.dropDescs, desc)

	if len(s.dropErrs) == 0 {
		return nil
	}

	err := s.dropErrs[0]
	s.dropErrs = s.dropErrs[1:]

	return err
}

type stubDDLTx struct{}

func (s *stubDDLTx) IsolationLevel() storage.IsolationLevel {
	return storage.IsolationReadCommitted
}

func (s *stubDDLTx) ReadOnly() bool {
	return false
}

func (s *stubDDLTx) Commit() error {
	return nil
}

func (s *stubDDLTx) Rollback() error {
	return nil
}
