package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/types"
)

const (
	phase1FileFormat  = "tucotuco.catalog"
	phase1FileVersion = 1
)

var (
	// ErrInvalidPersistencePath reports an empty or blank persistence path.
	ErrInvalidPersistencePath = errors.New("catalog: invalid persistence path")
	// ErrInvalidPersistenceFile reports malformed or incompatible persisted catalog data.
	ErrInvalidPersistenceFile = errors.New("catalog: invalid persistence file")
	// ErrUnsupportedPersistenceVersion reports a persisted catalog file version this binary does not understand.
	ErrUnsupportedPersistenceVersion = errors.New("catalog: unsupported persistence version")
)

// LoadFile loads a catalog from path using the Phase 1 JSON file format.
// If path does not exist, LoadFile returns an empty catalog.
func LoadFile(path string) (*Memory, error) {
	if err := validatePersistencePath(path); err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return NewMemory(), nil
		}
		return nil, err
	}

	var file persistedCatalogFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("catalog: decode %q: %w", path, errors.Join(ErrInvalidPersistenceFile, err))
	}
	if file.Format != phase1FileFormat {
		return nil, fmt.Errorf("catalog: open %q: %w", path, errors.Join(ErrInvalidPersistenceFile, fmt.Errorf("unexpected format %q", file.Format)))
	}
	if file.Version != phase1FileVersion {
		return nil, fmt.Errorf("catalog: open %q: %w", path, errors.Join(ErrUnsupportedPersistenceVersion, fmt.Errorf("version %d", file.Version)))
	}

	cat := NewMemory()
	for schemaIndex := range file.Schemas {
		schemaDesc := file.Schemas[schemaIndex].toDescriptor()
		if err := cat.CreateSchema(schemaDesc); err != nil {
			return nil, fmt.Errorf(
				"catalog: load schema %d from %q: %w",
				schemaIndex,
				path,
				errors.Join(ErrInvalidPersistenceFile, err),
			)
		}
		for tableIndex := range file.Schemas[schemaIndex].Tables {
			tableDesc, err := file.Schemas[schemaIndex].Tables[tableIndex].toDescriptor(schemaDesc.Name)
			if err != nil {
				return nil, fmt.Errorf(
					"catalog: load schema %q table %d from %q: %w",
					schemaDesc.Name,
					tableIndex,
					path,
					errors.Join(ErrInvalidPersistenceFile, err),
				)
			}
			if err := cat.CreateTable(tableDesc); err != nil {
				return nil, fmt.Errorf(
					"catalog: load schema %q table %d from %q: %w",
					schemaDesc.Name,
					tableIndex,
					path,
					errors.Join(ErrInvalidPersistenceFile, err),
				)
			}
		}
	}

	return cat, nil
}

// SaveFile writes c to path using the Phase 1 JSON file format.
func SaveFile(path string, c *Memory) error {
	if err := validatePersistencePath(path); err != nil {
		return err
	}

	data, err := json.MarshalIndent(c.snapshotPersistenceFile(), "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	return writeFileAtomically(path, data)
}

func validatePersistencePath(path string) error {
	if strings.TrimSpace(path) == "" {
		return ErrInvalidPersistencePath
	}

	return nil
}

func (c *Memory) snapshotPersistenceFile() persistedCatalogFile {
	file := persistedCatalogFile{
		Format:  phase1FileFormat,
		Version: phase1FileVersion,
		Schemas: make([]persistedSchema, 0),
	}
	if c == nil {
		return file
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	schemaNames := make([]string, 0, len(c.schemas))
	for name := range c.schemas {
		schemaNames = append(schemaNames, name)
	}
	sort.Strings(schemaNames)

	file.Schemas = make([]persistedSchema, 0, len(schemaNames))
	for _, schemaName := range schemaNames {
		schema := c.schemas[schemaName]
		schemaDesc := cloneSchemaDescriptor(&schema.descriptor)
		entry := persistedSchema{
			Name:             schemaDesc.Name,
			DefaultCollation: schemaDesc.DefaultCollation,
			Tables:           make([]persistedTable, 0, len(schema.tables)),
		}

		tableNames := make([]string, 0, len(schema.tables))
		for name := range schema.tables {
			tableNames = append(tableNames, name)
		}
		sort.Strings(tableNames)

		for _, tableName := range tableNames {
			table := schema.tables[tableName]
			entry.Tables = append(entry.Tables, newPersistedTable(cloneTableDescriptor(&table.descriptor)))
		}

		file.Schemas = append(file.Schemas, entry)
	}

	return file
}

func writeFileAtomically(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}

	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}

	cleanup = false
	return nil
}

type persistedCatalogFile struct {
	Format  string            `json:"format"`
	Version int               `json:"version"`
	Schemas []persistedSchema `json:"schemas"`
}

type persistedSchema struct {
	Name             string           `json:"name"`
	DefaultCollation string           `json:"default_collation,omitempty"`
	Tables           []persistedTable `json:"tables"`
}

func (s persistedSchema) toDescriptor() *SchemaDescriptor {
	return &SchemaDescriptor{
		Name:             s.Name,
		DefaultCollation: s.DefaultCollation,
	}
}

type persistedTable struct {
	Name        string                 `json:"name"`
	Columns     []persistedColumn      `json:"columns"`
	Constraints []ConstraintDescriptor `json:"constraints,omitempty"`
	Periods     []PeriodDescriptor     `json:"periods,omitempty"`
}

func newPersistedTable(desc *TableDescriptor) persistedTable {
	table := persistedTable{
		Name:        desc.ID.Name,
		Columns:     make([]persistedColumn, 0, len(desc.Columns)),
		Constraints: desc.Constraints,
		Periods:     desc.Periods,
	}
	for index := range desc.Columns {
		table.Columns = append(table.Columns, newPersistedColumn(desc.Columns[index]))
	}

	return table
}

func (t persistedTable) toDescriptor(schemaName string) (*TableDescriptor, error) {
	desc := &TableDescriptor{
		ID:          storage.TableID{Schema: schemaName, Name: t.Name},
		Columns:     make([]ColumnDescriptor, 0, len(t.Columns)),
		Constraints: t.Constraints,
		Periods:     t.Periods,
	}
	for index := range t.Columns {
		column, err := t.Columns[index].toDescriptor()
		if err != nil {
			return nil, fmt.Errorf("column %d: %w", index, err)
		}
		desc.Columns = append(desc.Columns, *column)
	}

	return desc, nil
}

type persistedColumn struct {
	Name        string                 `json:"name"`
	Type        persistedTypeDesc      `json:"type"`
	Default     *ExpressionDescriptor  `json:"default,omitempty"`
	Generated   *GenerationDescriptor  `json:"generated,omitempty"`
	Identity    *IdentityDescriptor    `json:"identity,omitempty"`
	Constraints []ConstraintDescriptor `json:"constraints,omitempty"`
	Collation   string                 `json:"collation,omitempty"`
}

func newPersistedColumn(desc ColumnDescriptor) persistedColumn {
	return persistedColumn{
		Name:        desc.Name,
		Type:        newPersistedTypeDesc(desc.Type),
		Default:     desc.Default,
		Generated:   desc.Generated,
		Identity:    desc.Identity,
		Constraints: desc.Constraints,
		Collation:   desc.Collation,
	}
}

func (c persistedColumn) toDescriptor() (*ColumnDescriptor, error) {
	typeDesc, err := c.Type.toDescriptor()
	if err != nil {
		return nil, err
	}

	return &ColumnDescriptor{
		Name:        c.Name,
		Type:        typeDesc,
		Default:     c.Default,
		Generated:   c.Generated,
		Identity:    c.Identity,
		Constraints: c.Constraints,
		Collation:   c.Collation,
	}, nil
}

type persistedTypeDesc struct {
	Kind      string `json:"kind"`
	Precision uint32 `json:"precision,omitempty"`
	Scale     uint32 `json:"scale,omitempty"`
	Length    uint32 `json:"length,omitempty"`
	Nullable  bool   `json:"nullable"`
}

func newPersistedTypeDesc(desc types.TypeDesc) persistedTypeDesc {
	return persistedTypeDesc{
		Kind:      desc.Kind.String(),
		Precision: desc.Precision,
		Scale:     desc.Scale,
		Length:    desc.Length,
		Nullable:  desc.Nullable,
	}
}

func (d persistedTypeDesc) toDescriptor() (types.TypeDesc, error) {
	kind, err := types.ParseTypeKind(d.Kind)
	if err != nil {
		return types.TypeDesc{}, err
	}

	return types.TypeDesc{
		Kind:      kind,
		Precision: d.Precision,
		Scale:     d.Scale,
		Length:    d.Length,
		Nullable:  d.Nullable,
	}, nil
}
