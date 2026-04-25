package catalog

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/types"
)

func persistenceInt64Ptr(value int64) *int64 {
	return &value
}

func testSchemaDescriptor() *SchemaDescriptor {
	return &SchemaDescriptor{
		Name:             "public",
		DefaultCollation: "unicode_ci",
	}
}

func testTableDescriptor() *TableDescriptor {
	return &TableDescriptor{
		ID: storage.TableID{Schema: "public", Name: "widgets"},
		Columns: []ColumnDescriptor{
			{
				Name: "id",
				Type: types.TypeDesc{Kind: types.TypeKindInteger, Nullable: false},
				Identity: &IdentityDescriptor{
					Generation: IdentityGenerationAlways,
					Start:      persistenceInt64Ptr(100),
					Increment:  persistenceInt64Ptr(5),
				},
				Constraints: []ConstraintDescriptor{
					{Name: "widgets_id_pk", Kind: ConstraintKindPrimaryKey},
				},
			},
			{
				Name:      "name",
				Type:      types.TypeDesc{Kind: types.TypeKindVarChar, Length: 32, Nullable: true},
				Default:   &ExpressionDescriptor{SQL: "'unknown'"},
				Collation: "unicode_ci",
				Constraints: []ConstraintDescriptor{
					{Name: "widgets_name_unique", Kind: ConstraintKindUnique},
				},
			},
			{
				Name: "slug",
				Type: types.TypeDesc{Kind: types.TypeKindVarChar, Length: 32, Nullable: false},
				Generated: &GenerationDescriptor{
					Expression: ExpressionDescriptor{SQL: "lower(name)"},
					Storage:    GenerationStorageStored,
				},
			},
			{Name: "valid_from", Type: types.TypeDesc{Kind: types.TypeKindTimestamp, Nullable: false}},
			{Name: "valid_to", Type: types.TypeDesc{Kind: types.TypeKindTimestamp, Nullable: false}},
		},
		Constraints: []ConstraintDescriptor{
			{
				Name:       "widgets_name_check",
				Kind:       ConstraintKindCheck,
				Expression: &ExpressionDescriptor{SQL: "char_length(name) > 0"},
			},
		},
		Periods: []PeriodDescriptor{
			{Kind: PeriodKindApplicationTime, StartColumn: "valid_from", EndColumn: "valid_to"},
		},
	}
}

func testViewDescriptor() *ViewDescriptor {
	return &ViewDescriptor{
		ID: storage.TableID{Schema: "public", Name: "widget_names"},
		Columns: []ColumnDescriptor{
			{Name: "id", Type: types.TypeDesc{Kind: types.TypeKindInteger, Nullable: false}},
			{Name: "name", Type: types.TypeDesc{Kind: types.TypeKindVarChar, Length: 32, Nullable: true}},
		},
		Query:       ExpressionDescriptor{SQL: "SELECT id, name FROM widgets"},
		CheckOption: "CASCADED",
	}
}

func buildPersistenceCatalog(t *testing.T) *Memory {
	t.Helper()

	cat := NewMemory()
	if err := cat.CreateSchema(testSchemaDescriptor()); err != nil {
		t.Fatalf("CreateSchema() error = %v", err)
	}
	if err := cat.CreateTable(testTableDescriptor()); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	return cat
}

func TestLoadFileMissingFileReturnsEmptyCatalog(t *testing.T) {
	t.Parallel()

	cat, err := LoadFile(filepath.Join(t.TempDir(), "catalog.json"))
	if err != nil {
		t.Fatalf("LoadFile() error = %v", err)
	}
	if len(cat.schemas) != 0 {
		t.Fatalf("LoadFile() schema count = %d, want %d", len(cat.schemas), 0)
	}
}

func TestSaveFileAndLoadFileRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		seed   func(t *testing.T, cat *Memory)
		assert func(t *testing.T, cat *Memory)
	}{
		{
			name: "empty catalog",
			seed: func(t *testing.T, _ *Memory) {
				t.Helper()
			},
			assert: func(t *testing.T, cat *Memory) {
				t.Helper()
				if len(cat.schemas) != 0 {
					t.Fatalf("schema count = %d, want %d", len(cat.schemas), 0)
				}
			},
		},
		{
			name: "schema without tables",
			seed: func(t *testing.T, cat *Memory) {
				t.Helper()
				if err := cat.CreateSchema(testSchemaDescriptor()); err != nil {
					t.Fatalf("CreateSchema() error = %v", err)
				}
			},
			assert: func(t *testing.T, cat *Memory) {
				t.Helper()
				entry, ok := cat.schemas["public"]
				if !ok {
					t.Fatalf("schema entry missing for %q", "public")
				}
				if !reflect.DeepEqual(entry.descriptor, *testSchemaDescriptor()) {
					t.Fatalf("schema descriptor = %#v, want %#v", entry.descriptor, *testSchemaDescriptor())
				}
				if len(entry.tables) != 0 {
					t.Fatalf("table count = %d, want %d", len(entry.tables), 0)
				}
			},
		},
		{
			name: "schema and table metadata",
			seed: func(t *testing.T, cat *Memory) {
				t.Helper()
				if err := cat.CreateSchema(testSchemaDescriptor()); err != nil {
					t.Fatalf("CreateSchema() error = %v", err)
				}
				if err := cat.CreateTable(testTableDescriptor()); err != nil {
					t.Fatalf("CreateTable() error = %v", err)
				}
			},
			assert: func(t *testing.T, cat *Memory) {
				t.Helper()
				entry, ok := cat.schemas["public"]
				if !ok {
					t.Fatalf("schema entry missing for %q", "public")
				}
				if !reflect.DeepEqual(entry.descriptor, *testSchemaDescriptor()) {
					t.Fatalf("schema descriptor = %#v, want %#v", entry.descriptor, *testSchemaDescriptor())
				}

				got, err := cat.LookupTable(testTableDescriptor().ID)
				if err != nil {
					t.Fatalf("LookupTable() error = %v", err)
				}
				if !reflect.DeepEqual(got, testTableDescriptor()) {
					t.Fatalf("LookupTable() = %#v, want %#v", got, testTableDescriptor())
				}
			},
		},
		{
			name: "schema table and view metadata",
			seed: func(t *testing.T, cat *Memory) {
				t.Helper()
				if err := cat.CreateSchema(testSchemaDescriptor()); err != nil {
					t.Fatalf("CreateSchema() error = %v", err)
				}
				if err := cat.CreateTable(testTableDescriptor()); err != nil {
					t.Fatalf("CreateTable() error = %v", err)
				}
				if err := cat.CreateView(testViewDescriptor()); err != nil {
					t.Fatalf("CreateView() error = %v", err)
				}
			},
			assert: func(t *testing.T, cat *Memory) {
				t.Helper()
				got, err := cat.LookupView(testViewDescriptor().ID)
				if err != nil {
					t.Fatalf("LookupView() error = %v", err)
				}
				if !reflect.DeepEqual(got, testViewDescriptor()) {
					t.Fatalf("LookupView() = %#v, want %#v", got, testViewDescriptor())
				}
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "catalog.json")
			cat := NewMemory()
			tc.seed(t, cat)

			if err := SaveFile(path, cat); err != nil {
				t.Fatalf("SaveFile() error = %v", err)
			}

			loaded, err := LoadFile(path)
			if err != nil {
				t.Fatalf("LoadFile() error = %v", err)
			}

			tc.assert(t, loaded)
		})
	}
}

func TestSaveFileAndLoadFileRoundTripAfterDropSchema(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "catalog.json")
	cat := NewMemory()
	if err := cat.CreateSchema(&SchemaDescriptor{Name: "public"}); err != nil {
		t.Fatalf("CreateSchema(public) error = %v", err)
	}
	if err := cat.CreateSchema(&SchemaDescriptor{Name: "scratch"}); err != nil {
		t.Fatalf("CreateSchema(scratch) error = %v", err)
	}
	if err := cat.DropSchema("scratch"); err != nil {
		t.Fatalf("DropSchema() error = %v", err)
	}

	if err := SaveFile(path, cat); err != nil {
		t.Fatalf("SaveFile() error = %v", err)
	}

	loaded, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile() error = %v", err)
	}
	if _, err := loaded.LookupSchema("public"); err != nil {
		t.Fatalf("LookupSchema(public) error = %v", err)
	}
	if _, err := loaded.LookupSchema("scratch"); !errors.Is(err, ErrSchemaNotFound) {
		t.Fatalf("LookupSchema(scratch) error = %v, want %v", err, ErrSchemaNotFound)
	}
}

func TestLoadFilePreservesDescriptorIsolation(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := SaveFile(path, buildPersistenceCatalog(t)); err != nil {
		t.Fatalf("SaveFile() error = %v", err)
	}

	tableID := testTableDescriptor().ID
	tests := []struct {
		name string
		run  func(t *testing.T, cat *Memory)
	}{
		{
			name: "table lookup returns deep copy",
			run: func(t *testing.T, cat *Memory) {
				t.Helper()

				got, err := cat.LookupTable(tableID)
				if err != nil {
					t.Fatalf("LookupTable() error = %v", err)
				}

				got.ID.Name = "mutated"
				got.Columns[0].Name = "mutated_id"
				*got.Columns[0].Identity.Start = 999
				got.Columns[1].Default.SQL = "'changed'"
				got.Columns[2].Generated.Expression.SQL = "upper(name)"
				got.Constraints[0].Expression.SQL = "false"
				got.Periods[0].StartColumn = "mutated_start"

				fresh, err := cat.LookupTable(tableID)
				if err != nil {
					t.Fatalf("LookupTable() fresh error = %v", err)
				}
				if !reflect.DeepEqual(fresh, testTableDescriptor()) {
					t.Fatalf("fresh LookupTable() = %#v, want %#v", fresh, testTableDescriptor())
				}
			},
		},
		{
			name: "column lookup returns deep copy",
			run: func(t *testing.T, cat *Memory) {
				t.Helper()

				got, err := cat.LookupColumn(tableID, "name")
				if err != nil {
					t.Fatalf("LookupColumn() error = %v", err)
				}

				got.Name = "mutated_name"
				got.Type.Length = 128
				got.Default.SQL = "'changed'"
				got.Constraints[0].Name = "changed_unique"
				got.Collation = "binary"

				fresh, err := cat.LookupColumn(tableID, "name")
				if err != nil {
					t.Fatalf("LookupColumn() fresh error = %v", err)
				}
				want := testTableDescriptor().Columns[1]
				if !reflect.DeepEqual(fresh, &want) {
					t.Fatalf("fresh LookupColumn() = %#v, want %#v", fresh, &want)
				}
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			loaded, err := LoadFile(path)
			if err != nil {
				t.Fatalf("LoadFile() error = %v", err)
			}

			tc.run(t, loaded)
		})
	}
}

func TestLoadFileRejectsInvalidPersistenceFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err := LoadFile(path)
	if !errors.Is(err, ErrInvalidPersistenceFile) {
		t.Fatalf("LoadFile() error = %v, want %v", err, ErrInvalidPersistenceFile)
	}
}
