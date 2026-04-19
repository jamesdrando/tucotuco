package catalog_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/catalog"
	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/types"
)

func int64ptr(value int64) *int64 {
	return &value
}

func TestMemoryCatalogLifecycle(t *testing.T) {
	t.Parallel()

	cat := catalog.NewMemory()
	schema := &catalog.SchemaDescriptor{Name: "public", DefaultCollation: "unicode_ci"}
	table := &catalog.TableDescriptor{
		ID: storage.TableID{Schema: "public", Name: "widgets"},
		Columns: []catalog.ColumnDescriptor{
			{
				Name: "id",
				Type: types.TypeDesc{Kind: types.TypeKindInteger, Nullable: false},
				Identity: &catalog.IdentityDescriptor{
					Generation: catalog.IdentityGenerationAlways,
					Start:      int64ptr(100),
					Increment:  int64ptr(5),
				},
				Constraints: []catalog.ConstraintDescriptor{
					{Name: "widgets_id_pk", Kind: catalog.ConstraintKindPrimaryKey},
				},
			},
			{
				Name:      "name",
				Type:      types.TypeDesc{Kind: types.TypeKindVarChar, Length: 32, Nullable: true},
				Default:   &catalog.ExpressionDescriptor{SQL: "'unknown'"},
				Collation: "unicode_ci",
				Constraints: []catalog.ConstraintDescriptor{
					{Name: "widgets_name_unique", Kind: catalog.ConstraintKindUnique},
				},
			},
			{Name: "valid_from", Type: types.TypeDesc{Kind: types.TypeKindTimestamp, Nullable: false}},
			{Name: "valid_to", Type: types.TypeDesc{Kind: types.TypeKindTimestamp, Nullable: false}},
		},
		Constraints: []catalog.ConstraintDescriptor{
			{
				Name:       "widgets_name_check",
				Kind:       catalog.ConstraintKindCheck,
				Expression: &catalog.ExpressionDescriptor{SQL: "char_length(name) > 0"},
			},
		},
		Periods: []catalog.PeriodDescriptor{
			{Kind: catalog.PeriodKindApplicationTime, StartColumn: "valid_from", EndColumn: "valid_to"},
		},
	}

	if err := cat.CreateSchema(schema); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}
	if err := cat.CreateTable(table); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	gotTable, err := cat.LookupTable(table.ID)
	if err != nil {
		t.Fatalf("LookupTable() error = %v", err)
	}
	if gotTable.ID != table.ID {
		t.Fatalf("LookupTable() id = %#v, want %#v", gotTable.ID, table.ID)
	}
	if len(gotTable.Columns) != len(table.Columns) {
		t.Fatalf("LookupTable() column count = %d, want %d", len(gotTable.Columns), len(table.Columns))
	}
	for index := range table.Columns {
		if gotTable.Columns[index].Name != table.Columns[index].Name {
			t.Fatalf("LookupTable() column %d name = %q, want %q", index, gotTable.Columns[index].Name, table.Columns[index].Name)
		}
		if gotTable.Columns[index].Type != table.Columns[index].Type {
			t.Fatalf("LookupTable() column %d type = %#v, want %#v", index, gotTable.Columns[index].Type, table.Columns[index].Type)
		}
	}
	if len(gotTable.Constraints) != 1 {
		t.Fatalf("LookupTable() constraint count = %d, want %d", len(gotTable.Constraints), 1)
	}
	if gotTable.Constraints[0].Name != "widgets_name_check" {
		t.Fatalf("LookupTable() constraint name = %q, want %q", gotTable.Constraints[0].Name, "widgets_name_check")
	}
	if gotTable.Constraints[0].Expression == nil || gotTable.Constraints[0].Expression.SQL != "char_length(name) > 0" {
		t.Fatalf("LookupTable() constraint expression = %#v, want %q", gotTable.Constraints[0].Expression, "char_length(name) > 0")
	}
	if len(gotTable.Periods) != 1 {
		t.Fatalf("LookupTable() period count = %d, want %d", len(gotTable.Periods), 1)
	}
	if gotTable.Periods[0].Kind != catalog.PeriodKindApplicationTime {
		t.Fatalf("LookupTable() period kind = %q, want %q", gotTable.Periods[0].Kind, catalog.PeriodKindApplicationTime)
	}
	if gotTable.Columns[0].Identity == nil || gotTable.Columns[0].Identity.Start == nil || *gotTable.Columns[0].Identity.Start != 100 {
		t.Fatalf("LookupTable() identity = %#v, want start %d", gotTable.Columns[0].Identity, 100)
	}
	if gotTable.Columns[1].Default == nil || gotTable.Columns[1].Default.SQL != "'unknown'" {
		t.Fatalf("LookupTable() default = %#v, want %q", gotTable.Columns[1].Default, "'unknown'")
	}
	if gotTable.Columns[1].Collation != "unicode_ci" {
		t.Fatalf("LookupTable() collation = %q, want %q", gotTable.Columns[1].Collation, "unicode_ci")
	}

	gotColumn, err := cat.LookupColumn(table.ID, "name")
	if err != nil {
		t.Fatalf("LookupColumn() error = %v", err)
	}
	if gotColumn.Name != "name" {
		t.Fatalf("LookupColumn() name = %q, want %q", gotColumn.Name, "name")
	}
	if gotColumn.Type != table.Columns[1].Type {
		t.Fatalf("LookupColumn() type = %#v, want %#v", gotColumn.Type, table.Columns[1].Type)
	}
	if gotColumn.Default == nil || gotColumn.Default.SQL != "'unknown'" {
		t.Fatalf("LookupColumn() default = %#v, want %q", gotColumn.Default, "'unknown'")
	}
	if len(gotColumn.Constraints) != 1 || gotColumn.Constraints[0].Kind != catalog.ConstraintKindUnique {
		t.Fatalf("LookupColumn() constraints = %#v, want UNIQUE", gotColumn.Constraints)
	}

	if err := cat.DropTable(table.ID); err != nil {
		t.Fatalf("DropTable() error = %v", err)
	}
	if err := cat.DropSchema(schema.Name); err != nil {
		t.Fatalf("DropSchema() error = %v", err)
	}
}

func TestMemoryCatalogZeroValueIsUsable(t *testing.T) {
	t.Parallel()

	var cat catalog.Memory
	schema := &catalog.SchemaDescriptor{Name: "public"}
	table := &catalog.TableDescriptor{
		ID: storage.TableID{Schema: "public", Name: "widgets"},
		Columns: []catalog.ColumnDescriptor{
			{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindInteger, Nullable: false}},
		},
	}

	if err := cat.CreateSchema(schema); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}
	if err := cat.CreateTable(table); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}
	if _, err := cat.LookupTable(table.ID); err != nil {
		t.Fatalf("LookupTable() error = %v", err)
	}
	if _, err := cat.LookupColumn(table.ID, "id"); err != nil {
		t.Fatalf("LookupColumn() error = %v", err)
	}
}

func TestMemoryCatalogRejectsDuplicatesAndMissingObjects(t *testing.T) {
	t.Parallel()

	cat := catalog.NewMemory()
	schema := &catalog.SchemaDescriptor{Name: "public"}
	tableID := storage.TableID{Schema: "public", Name: "widgets"}
	table := &catalog.TableDescriptor{
		ID: tableID,
		Columns: []catalog.ColumnDescriptor{
			{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindInteger, Nullable: false}},
		},
	}

	if err := cat.CreateSchema(schema); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}
	if err := cat.CreateSchema(schema); !errors.Is(err, catalog.ErrSchemaExists) {
		t.Fatalf("CreateSchema() duplicate error = %v, want %v", err, catalog.ErrSchemaExists)
	}
	if err := cat.CreateTable(table); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}
	if err := cat.CreateTable(table); !errors.Is(err, catalog.ErrTableExists) {
		t.Fatalf("CreateTable() duplicate error = %v, want %v", err, catalog.ErrTableExists)
	}
	if err := cat.CreateTable(&catalog.TableDescriptor{
		ID: storage.TableID{Schema: "missing", Name: "widgets"},
		Columns: []catalog.ColumnDescriptor{
			{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindInteger, Nullable: false}},
		},
	}); !errors.Is(err, catalog.ErrSchemaNotFound) {
		t.Fatalf("CreateTable() missing schema error = %v, want %v", err, catalog.ErrSchemaNotFound)
	}

	if err := cat.DropTable(storage.TableID{Schema: "public", Name: "missing"}); !errors.Is(err, catalog.ErrTableNotFound) {
		t.Fatalf("DropTable() missing table error = %v, want %v", err, catalog.ErrTableNotFound)
	}
	if err := cat.DropTable(storage.TableID{Schema: "missing", Name: "widgets"}); !errors.Is(err, catalog.ErrSchemaNotFound) {
		t.Fatalf("DropTable() missing schema error = %v, want %v", err, catalog.ErrSchemaNotFound)
	}
	if _, err := cat.LookupTable(storage.TableID{Schema: "public", Name: "missing"}); !errors.Is(err, catalog.ErrTableNotFound) {
		t.Fatalf("LookupTable() missing table error = %v, want %v", err, catalog.ErrTableNotFound)
	}
	if _, err := cat.LookupTable(storage.TableID{Schema: "missing", Name: "widgets"}); !errors.Is(err, catalog.ErrSchemaNotFound) {
		t.Fatalf("LookupTable() missing schema error = %v, want %v", err, catalog.ErrSchemaNotFound)
	}
	if _, err := cat.LookupColumn(tableID, "missing"); !errors.Is(err, catalog.ErrColumnNotFound) {
		t.Fatalf("LookupColumn() missing column error = %v, want %v", err, catalog.ErrColumnNotFound)
	}
	if err := cat.DropSchema(schema.Name); !errors.Is(err, catalog.ErrSchemaNotEmpty) {
		t.Fatalf("DropSchema() non-empty error = %v, want %v", err, catalog.ErrSchemaNotEmpty)
	}
	if err := cat.DropTable(tableID); err != nil {
		t.Fatalf("DropTable() error = %v", err)
	}
	if err := cat.DropSchema(schema.Name); err != nil {
		t.Fatalf("DropSchema() error = %v", err)
	}
	if err := cat.DropSchema(schema.Name); !errors.Is(err, catalog.ErrSchemaNotFound) {
		t.Fatalf("DropSchema() missing schema error = %v, want %v", err, catalog.ErrSchemaNotFound)
	}
}

func TestMemoryCatalogValidationErrorsWrapSentinels(t *testing.T) {
	t.Parallel()

	cat := catalog.NewMemory()

	if err := cat.CreateSchema(&catalog.SchemaDescriptor{Name: ""}); !errors.Is(err, catalog.ErrInvalidSchemaDescriptor) || !errors.Is(err, catalog.ErrInvalidSchemaName) {
		t.Fatalf("CreateSchema() error = %v, want chain to include %v and %v", err, catalog.ErrInvalidSchemaDescriptor, catalog.ErrInvalidSchemaName)
	}

	if err := cat.CreateSchema(&catalog.SchemaDescriptor{Name: "public"}); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	if err := cat.CreateTable(&catalog.TableDescriptor{
		ID: storage.TableID{Schema: "", Name: "widgets"},
		Columns: []catalog.ColumnDescriptor{
			{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindInteger}},
		},
	}); !errors.Is(err, catalog.ErrInvalidTableDescriptor) || !errors.Is(err, catalog.ErrInvalidTableIdentifier) {
		t.Fatalf("CreateTable() id error = %v, want chain to include %v and %v", err, catalog.ErrInvalidTableDescriptor, catalog.ErrInvalidTableIdentifier)
	}

	if err := cat.CreateTable(&catalog.TableDescriptor{
		ID: storage.TableID{Schema: "public", Name: "widgets"},
		Columns: []catalog.ColumnDescriptor{
			{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindChar}},
		},
	}); !errors.Is(err, catalog.ErrInvalidTableDescriptor) || !errors.Is(err, catalog.ErrInvalidColumnDescriptor) {
		t.Fatalf("CreateTable() column error = %v, want chain to include %v and %v", err, catalog.ErrInvalidTableDescriptor, catalog.ErrInvalidColumnDescriptor)
	}

	if err := cat.CreateTable(&catalog.TableDescriptor{
		ID: storage.TableID{Schema: "public", Name: "defaults"},
		Columns: []catalog.ColumnDescriptor{
			{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindInteger}, Default: &catalog.ExpressionDescriptor{SQL: ""}},
		},
	}); !errors.Is(err, catalog.ErrInvalidTableDescriptor) || !errors.Is(err, catalog.ErrInvalidColumnDescriptor) || !errors.Is(err, catalog.ErrInvalidExpressionDescriptor) {
		t.Fatalf("CreateTable() default error = %v, want chain to include %v, %v, and %v", err, catalog.ErrInvalidTableDescriptor, catalog.ErrInvalidColumnDescriptor, catalog.ErrInvalidExpressionDescriptor)
	}

	if err := cat.CreateTable(&catalog.TableDescriptor{
		ID: storage.TableID{Schema: "public", Name: "foreign_keys"},
		Columns: []catalog.ColumnDescriptor{
			{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindInteger}},
		},
		Constraints: []catalog.ConstraintDescriptor{
			{Kind: catalog.ConstraintKindForeignKey, Columns: []string{"id"}},
		},
	}); !errors.Is(err, catalog.ErrInvalidTableDescriptor) || !errors.Is(err, catalog.ErrInvalidConstraintDescriptor) {
		t.Fatalf("CreateTable() constraint error = %v, want chain to include %v and %v", err, catalog.ErrInvalidTableDescriptor, catalog.ErrInvalidConstraintDescriptor)
	}

	if err := cat.CreateTable(&catalog.TableDescriptor{
		ID: storage.TableID{Schema: "public", Name: "periods"},
		Columns: []catalog.ColumnDescriptor{
			{Name: "valid_from", Type: types.TypeDesc{Kind: types.TypeKindTimestamp, Nullable: false}},
			{Name: "valid_to", Type: types.TypeDesc{Kind: types.TypeKindTimestamp, Nullable: false}},
		},
		Periods: []catalog.PeriodDescriptor{
			{Kind: catalog.PeriodKindSystemTime, StartColumn: "", EndColumn: "valid_to"},
		},
	}); !errors.Is(err, catalog.ErrInvalidTableDescriptor) || !errors.Is(err, catalog.ErrInvalidPeriodDescriptor) {
		t.Fatalf("CreateTable() period error = %v, want chain to include %v and %v", err, catalog.ErrInvalidTableDescriptor, catalog.ErrInvalidPeriodDescriptor)
	}
}

func TestMemoryCatalogRejectsTableConstraintsWithoutColumns(t *testing.T) {
	t.Parallel()

	cat := catalog.NewMemory()
	if err := cat.CreateSchema(&catalog.SchemaDescriptor{Name: "public"}); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	testCases := []struct {
		name       string
		tableName  string
		constraint catalog.ConstraintDescriptor
	}{
		{
			name:      "primary key",
			tableName: "widgets_pk",
			constraint: catalog.ConstraintDescriptor{
				Kind: catalog.ConstraintKindPrimaryKey,
			},
		},
		{
			name:      "unique",
			tableName: "widgets_unique",
			constraint: catalog.ConstraintDescriptor{
				Kind: catalog.ConstraintKindUnique,
			},
		},
		{
			name:      "foreign key",
			tableName: "widgets_fk",
			constraint: catalog.ConstraintDescriptor{
				Kind: catalog.ConstraintKindForeignKey,
				Reference: &catalog.ReferenceDescriptor{
					Table:   storage.TableID{Schema: "public", Name: "parents"},
					Columns: []string{"id"},
				},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := cat.CreateTable(&catalog.TableDescriptor{
				ID: storage.TableID{Schema: "public", Name: tc.tableName},
				Columns: []catalog.ColumnDescriptor{
					{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindInteger, Nullable: false}},
				},
				Constraints: []catalog.ConstraintDescriptor{tc.constraint},
			})
			if !errors.Is(err, catalog.ErrInvalidTableDescriptor) || !errors.Is(err, catalog.ErrInvalidConstraintDescriptor) {
				t.Fatalf("CreateTable() error = %v, want chain to include %v and %v", err, catalog.ErrInvalidTableDescriptor, catalog.ErrInvalidConstraintDescriptor)
			}
		})
	}
}

func TestMemoryCatalogRejectsContradictoryGeneratedColumnStates(t *testing.T) {
	t.Parallel()

	cat := catalog.NewMemory()
	if err := cat.CreateSchema(&catalog.SchemaDescriptor{Name: "public"}); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	testCases := []struct {
		name      string
		tableName string
		column    catalog.ColumnDescriptor
	}{
		{
			name:      "generated with default",
			tableName: "widgets_generated_default",
			column: catalog.ColumnDescriptor{
				Name: "value",
				Type: types.TypeDesc{Kind: types.TypeKindInteger, Nullable: false},
				Default: &catalog.ExpressionDescriptor{
					SQL: "42",
				},
				Generated: &catalog.GenerationDescriptor{
					Expression: catalog.ExpressionDescriptor{SQL: "41 + 1"},
					Storage:    catalog.GenerationStorageStored,
				},
			},
		},
		{
			name:      "generated with identity",
			tableName: "widgets_generated_identity",
			column: catalog.ColumnDescriptor{
				Name: "value",
				Type: types.TypeDesc{Kind: types.TypeKindInteger, Nullable: false},
				Generated: &catalog.GenerationDescriptor{
					Expression: catalog.ExpressionDescriptor{SQL: "41 + 1"},
					Storage:    catalog.GenerationStorageStored,
				},
				Identity: &catalog.IdentityDescriptor{
					Generation: catalog.IdentityGenerationAlways,
				},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := cat.CreateTable(&catalog.TableDescriptor{
				ID:      storage.TableID{Schema: "public", Name: tc.tableName},
				Columns: []catalog.ColumnDescriptor{tc.column},
			})
			if !errors.Is(err, catalog.ErrInvalidTableDescriptor) || !errors.Is(err, catalog.ErrInvalidColumnDescriptor) {
				t.Fatalf("CreateTable() error = %v, want chain to include %v and %v", err, catalog.ErrInvalidTableDescriptor, catalog.ErrInvalidColumnDescriptor)
			}
		})
	}
}

func TestMemoryCatalogRejectsInvalidPeriodDefinitions(t *testing.T) {
	t.Parallel()

	cat := catalog.NewMemory()
	if err := cat.CreateSchema(&catalog.SchemaDescriptor{Name: "public"}); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	testCases := []struct {
		name      string
		tableName string
		table     catalog.TableDescriptor
	}{
		{
			name:      "same start and end column",
			tableName: "widgets_same_period_column",
			table: catalog.TableDescriptor{
				Columns: []catalog.ColumnDescriptor{
					{Name: "valid_at", Type: types.TypeDesc{Kind: types.TypeKindTimestamp, Nullable: false}},
				},
				Periods: []catalog.PeriodDescriptor{
					{Kind: catalog.PeriodKindApplicationTime, StartColumn: "valid_at", EndColumn: "valid_at"},
				},
			},
		},
		{
			name:      "non temporal period column type",
			tableName: "widgets_non_temporal_period",
			table: catalog.TableDescriptor{
				Columns: []catalog.ColumnDescriptor{
					{Name: "valid_from", Type: types.TypeDesc{Kind: types.TypeKindInteger, Nullable: false}},
					{Name: "valid_to", Type: types.TypeDesc{Kind: types.TypeKindTimestamp, Nullable: false}},
				},
				Periods: []catalog.PeriodDescriptor{
					{Kind: catalog.PeriodKindApplicationTime, StartColumn: "valid_from", EndColumn: "valid_to"},
				},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			table := tc.table
			table.ID = storage.TableID{Schema: "public", Name: tc.tableName}

			err := cat.CreateTable(&table)
			if !errors.Is(err, catalog.ErrInvalidTableDescriptor) || !errors.Is(err, catalog.ErrInvalidPeriodDescriptor) {
				t.Fatalf("CreateTable() error = %v, want chain to include %v and %v", err, catalog.ErrInvalidTableDescriptor, catalog.ErrInvalidPeriodDescriptor)
			}
		})
	}
}

func TestMemoryCatalogRejectsInvalidDescriptors(t *testing.T) {
	t.Parallel()

	cat := catalog.NewMemory()

	invalidSchemas := []struct {
		name string
		desc *catalog.SchemaDescriptor
	}{
		{name: "nil", desc: nil},
		{name: "blank", desc: &catalog.SchemaDescriptor{Name: ""}},
	}
	for _, tc := range invalidSchemas {
		tc := tc
		t.Run("schema/"+tc.name, func(t *testing.T) {
			t.Parallel()

			if err := cat.CreateSchema(tc.desc); !errors.Is(err, catalog.ErrInvalidSchemaDescriptor) {
				t.Fatalf("CreateSchema() error = %v, want %v", err, catalog.ErrInvalidSchemaDescriptor)
			}
		})
	}

	if err := cat.CreateSchema(&catalog.SchemaDescriptor{Name: "public"}); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	invalidTables := []struct {
		name string
		desc *catalog.TableDescriptor
	}{
		{name: "nil", desc: nil},
		{name: "missing schema", desc: &catalog.TableDescriptor{ID: storage.TableID{Name: "widgets"}, Columns: []catalog.ColumnDescriptor{{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindInteger}}}}},
		{name: "missing name", desc: &catalog.TableDescriptor{ID: storage.TableID{Schema: "public"}, Columns: []catalog.ColumnDescriptor{{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindInteger}}}}},
		{name: "no columns", desc: &catalog.TableDescriptor{ID: storage.TableID{Schema: "public", Name: "widgets"}}},
		{name: "bad column name", desc: &catalog.TableDescriptor{ID: storage.TableID{Schema: "public", Name: "widgets"}, Columns: []catalog.ColumnDescriptor{{Name: "", Type: types.TypeDesc{Kind: types.TypeKindInteger}}}}},
		{name: "bad type", desc: &catalog.TableDescriptor{ID: storage.TableID{Schema: "public", Name: "widgets"}, Columns: []catalog.ColumnDescriptor{{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindChar}}}}},
		{name: "duplicate columns", desc: &catalog.TableDescriptor{ID: storage.TableID{Schema: "public", Name: "widgets"}, Columns: []catalog.ColumnDescriptor{{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindInteger}}, {Name: "id", Type: types.TypeDesc{Kind: types.TypeKindInteger}}}}},
		{name: "blank default expression", desc: &catalog.TableDescriptor{ID: storage.TableID{Schema: "public", Name: "widgets"}, Columns: []catalog.ColumnDescriptor{{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindInteger}, Default: &catalog.ExpressionDescriptor{SQL: ""}}}}},
		{name: "generated column missing storage", desc: &catalog.TableDescriptor{ID: storage.TableID{Schema: "public", Name: "widgets"}, Columns: []catalog.ColumnDescriptor{{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindInteger}, Generated: &catalog.GenerationDescriptor{Expression: catalog.ExpressionDescriptor{SQL: "1 + 1"}}}}}},
		{name: "identity missing generation", desc: &catalog.TableDescriptor{ID: storage.TableID{Schema: "public", Name: "widgets"}, Columns: []catalog.ColumnDescriptor{{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindInteger}, Identity: &catalog.IdentityDescriptor{}}}}},
		{name: "bad table constraint", desc: &catalog.TableDescriptor{ID: storage.TableID{Schema: "public", Name: "widgets"}, Columns: []catalog.ColumnDescriptor{{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindInteger}}}, Constraints: []catalog.ConstraintDescriptor{{Kind: catalog.ConstraintKindForeignKey, Columns: []string{"id"}}}}},
		{name: "unknown constraint column", desc: &catalog.TableDescriptor{ID: storage.TableID{Schema: "public", Name: "widgets"}, Columns: []catalog.ColumnDescriptor{{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindInteger}}}, Constraints: []catalog.ConstraintDescriptor{{Kind: catalog.ConstraintKindUnique, Columns: []string{"missing"}}}}},
		{name: "bad period", desc: &catalog.TableDescriptor{ID: storage.TableID{Schema: "public", Name: "widgets"}, Columns: []catalog.ColumnDescriptor{{Name: "valid_from", Type: types.TypeDesc{Kind: types.TypeKindTimestamp, Nullable: false}}, {Name: "valid_to", Type: types.TypeDesc{Kind: types.TypeKindTimestamp, Nullable: false}}}, Periods: []catalog.PeriodDescriptor{{Kind: catalog.PeriodKindApplicationTime, StartColumn: "valid_from", EndColumn: ""}}}},
	}
	for _, tc := range invalidTables {
		tc := tc
		t.Run("table/"+tc.name, func(t *testing.T) {
			t.Parallel()

			if err := cat.CreateTable(tc.desc); !errors.Is(err, catalog.ErrInvalidTableDescriptor) {
				t.Fatalf("CreateTable() error = %v, want %v", err, catalog.ErrInvalidTableDescriptor)
			}
		})
	}
}

func TestMemoryCatalogReturnedCopiesAreIsolated(t *testing.T) {
	t.Parallel()

	cat := catalog.NewMemory()
	if err := cat.CreateSchema(&catalog.SchemaDescriptor{Name: "public"}); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}

	input := &catalog.TableDescriptor{
		ID: storage.TableID{Schema: "public", Name: "widgets"},
		Columns: []catalog.ColumnDescriptor{
			{
				Name: "id",
				Type: types.TypeDesc{Kind: types.TypeKindInteger, Nullable: false},
				Identity: &catalog.IdentityDescriptor{
					Generation: catalog.IdentityGenerationAlways,
					Start:      int64ptr(1),
					Increment:  int64ptr(1),
				},
				Constraints: []catalog.ConstraintDescriptor{
					{Name: "widgets_pk", Kind: catalog.ConstraintKindPrimaryKey},
				},
			},
			{
				Name:      "name",
				Type:      types.TypeDesc{Kind: types.TypeKindVarChar, Length: 32, Nullable: true},
				Default:   &catalog.ExpressionDescriptor{SQL: "'widget'"},
				Collation: "unicode_ci",
				Constraints: []catalog.ConstraintDescriptor{
					{Name: "widgets_name_unique", Kind: catalog.ConstraintKindUnique, Columns: []string{"name"}},
				},
			},
			{Name: "valid_from", Type: types.TypeDesc{Kind: types.TypeKindTimestamp, Nullable: false}},
			{Name: "valid_to", Type: types.TypeDesc{Kind: types.TypeKindTimestamp, Nullable: false}},
		},
		Constraints: []catalog.ConstraintDescriptor{
			{
				Name:       "widgets_name_check",
				Kind:       catalog.ConstraintKindCheck,
				Expression: &catalog.ExpressionDescriptor{SQL: "char_length(name) > 0"},
			},
		},
		Periods: []catalog.PeriodDescriptor{
			{Kind: catalog.PeriodKindApplicationTime, StartColumn: "valid_from", EndColumn: "valid_to"},
		},
	}
	if err := cat.CreateTable(input); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	input.ID.Name = "mutated"
	input.Columns[0].Name = "changed"
	input.Columns[0].Type = types.TypeDesc{Kind: types.TypeKindBigInt, Nullable: false}
	if input.Columns[0].Identity == nil || input.Columns[0].Identity.Start == nil {
		t.Fatalf("test setup: missing identity metadata on input")
	}
	*input.Columns[0].Identity.Start = 42
	input.Columns[0].Constraints[0].Name = "changed"
	input.Columns[1].Default.SQL = "'changed'"
	input.Columns[1].Constraints[0].Columns[0] = "changed"
	input.Constraints[0].Expression.SQL = "1 = 0"
	input.Periods[0].StartColumn = "changed"

	gotTable, err := cat.LookupTable(storage.TableID{Schema: "public", Name: "widgets"})
	if err != nil {
		t.Fatalf("LookupTable() error = %v", err)
	}
	gotTable.ID.Name = "changed"
	gotTable.Columns[0].Name = "changed"
	gotTable.Columns[0].Type = types.TypeDesc{Kind: types.TypeKindBigInt, Nullable: false}
	if gotTable.Columns[0].Identity == nil || gotTable.Columns[0].Identity.Start == nil {
		t.Fatalf("LookupTable() identity = %#v, want non-nil start", gotTable.Columns[0].Identity)
	}
	*gotTable.Columns[0].Identity.Start = 99
	gotTable.Columns[0].Constraints[0].Name = "changed"
	gotTable.Columns[1].Default.SQL = "'changed'"
	gotTable.Columns[1].Constraints[0].Columns[0] = "changed"
	gotTable.Constraints[0].Expression.SQL = "false"
	gotTable.Periods[0].StartColumn = "changed"

	gotColumn, err := cat.LookupColumn(storage.TableID{Schema: "public", Name: "widgets"}, "id")
	if err != nil {
		t.Fatalf("LookupColumn() error = %v", err)
	}
	gotColumn.Name = "changed"
	gotColumn.Type = types.TypeDesc{Kind: types.TypeKindBigInt, Nullable: false}
	if gotColumn.Identity == nil || gotColumn.Identity.Start == nil {
		t.Fatalf("LookupColumn() identity = %#v, want non-nil start", gotColumn.Identity)
	}
	*gotColumn.Identity.Start = 123
	gotColumn.Constraints[0].Name = "changed"

	reloadedTable, err := cat.LookupTable(storage.TableID{Schema: "public", Name: "widgets"})
	if err != nil {
		t.Fatalf("LookupTable() error = %v", err)
	}
	if reloadedTable.ID.Name != "widgets" {
		t.Fatalf("reloaded table name = %q, want %q", reloadedTable.ID.Name, "widgets")
	}
	if reloadedTable.Columns[0].Name != "id" {
		t.Fatalf("reloaded column name = %q, want %q", reloadedTable.Columns[0].Name, "id")
	}
	if reloadedTable.Columns[0].Type.Kind != types.TypeKindInteger {
		t.Fatalf("reloaded column type = %v, want %v", reloadedTable.Columns[0].Type.Kind, types.TypeKindInteger)
	}
	if reloadedTable.Columns[0].Identity == nil || reloadedTable.Columns[0].Identity.Start == nil || *reloadedTable.Columns[0].Identity.Start != 1 {
		t.Fatalf("reloaded identity = %#v, want start %d", reloadedTable.Columns[0].Identity, 1)
	}
	if reloadedTable.Columns[0].Constraints[0].Name != "widgets_pk" {
		t.Fatalf("reloaded column constraint name = %q, want %q", reloadedTable.Columns[0].Constraints[0].Name, "widgets_pk")
	}
	if reloadedTable.Columns[1].Default == nil || reloadedTable.Columns[1].Default.SQL != "'widget'" {
		t.Fatalf("reloaded default = %#v, want %q", reloadedTable.Columns[1].Default, "'widget'")
	}
	if reloadedTable.Columns[1].Constraints[0].Columns[0] != "name" {
		t.Fatalf("reloaded constraint columns = %#v, want %q", reloadedTable.Columns[1].Constraints[0].Columns, "name")
	}
	if reloadedTable.Constraints[0].Expression == nil || reloadedTable.Constraints[0].Expression.SQL != "char_length(name) > 0" {
		t.Fatalf("reloaded table constraint expression = %#v, want %q", reloadedTable.Constraints[0].Expression, "char_length(name) > 0")
	}
	if reloadedTable.Periods[0].StartColumn != "valid_from" {
		t.Fatalf("reloaded period start = %q, want %q", reloadedTable.Periods[0].StartColumn, "valid_from")
	}

	reloadedColumn, err := cat.LookupColumn(storage.TableID{Schema: "public", Name: "widgets"}, "id")
	if err != nil {
		t.Fatalf("LookupColumn() error = %v", err)
	}
	if reloadedColumn.Name != "id" {
		t.Fatalf("reloaded column name = %q, want %q", reloadedColumn.Name, "id")
	}
	if reloadedColumn.Type.Kind != types.TypeKindInteger {
		t.Fatalf("reloaded column type = %v, want %v", reloadedColumn.Type.Kind, types.TypeKindInteger)
	}
	if reloadedColumn.Identity == nil || reloadedColumn.Identity.Start == nil || *reloadedColumn.Identity.Start != 1 {
		t.Fatalf("reloaded column identity = %#v, want start %d", reloadedColumn.Identity, 1)
	}
	if reloadedColumn.Constraints[0].Name != "widgets_pk" {
		t.Fatalf("reloaded column constraint name = %q, want %q", reloadedColumn.Constraints[0].Name, "widgets_pk")
	}
}

func TestMemoryCatalogConcurrentAccess(t *testing.T) {
	t.Parallel()

	const workers = 16

	cat := catalog.NewMemory()

	var wg sync.WaitGroup
	errCh := make(chan error, workers*4)

	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			schemaName := fmt.Sprintf("schema_%02d", i)
			tableID := storage.TableID{Schema: schemaName, Name: "widgets"}
			desc := &catalog.TableDescriptor{
				ID: tableID,
				Columns: []catalog.ColumnDescriptor{
					{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindInteger, Nullable: false}},
					{Name: "name", Type: types.TypeDesc{Kind: types.TypeKindVarChar, Length: 32, Nullable: true}},
				},
			}

			if err := cat.CreateSchema(&catalog.SchemaDescriptor{Name: schemaName}); err != nil {
				errCh <- err
				return
			}
			if err := cat.CreateTable(desc); err != nil {
				errCh <- err
				return
			}

			for j := 0; j < 64; j++ {
				gotTable, err := cat.LookupTable(tableID)
				if err != nil {
					errCh <- err
					return
				}
				if gotTable.ID != tableID {
					errCh <- fmt.Errorf("lookup table id = %#v, want %#v", gotTable.ID, tableID)
					return
				}

				gotColumn, err := cat.LookupColumn(tableID, "name")
				if err != nil {
					errCh <- err
					return
				}
				if gotColumn.Name != "name" {
					errCh <- fmt.Errorf("lookup column name = %q, want %q", gotColumn.Name, "name")
					return
				}
			}

			if err := cat.DropTable(tableID); err != nil {
				errCh <- err
				return
			}
			if err := cat.DropSchema(schemaName); err != nil {
				errCh <- err
				return
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent access error = %v", err)
		}
	}
}
