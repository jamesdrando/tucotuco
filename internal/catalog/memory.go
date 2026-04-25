package catalog

import (
	"sync"

	"github.com/jamesdrando/tucotuco/internal/storage"
)

// Memory is a thread-safe in-memory Catalog implementation.
type Memory struct {
	mu      sync.RWMutex
	schemas map[string]*schemaEntry
}

var _ Catalog = (*Memory)(nil)

// NewMemory constructs an empty in-memory catalog.
func NewMemory() *Memory {
	return &Memory{
		schemas: make(map[string]*schemaEntry),
	}
}

// CreateSchema registers a new schema.
func (c *Memory) CreateSchema(desc *SchemaDescriptor) error {
	if err := validateSchemaDescriptor(desc); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.schemas == nil {
		c.schemas = make(map[string]*schemaEntry)
	}

	if _, exists := c.schemas[desc.Name]; exists {
		return ErrSchemaExists
	}

	clone := cloneSchemaDescriptor(desc)
	c.schemas[desc.Name] = &schemaEntry{
		descriptor: *clone,
		tables:     make(map[string]*tableEntry),
		views:      make(map[string]*viewEntry),
	}

	return nil
}

// DropSchema removes an empty schema.
func (c *Memory) DropSchema(name string) error {
	if err := validateSchemaName(name); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.schemas == nil {
		return ErrSchemaNotFound
	}

	schema, exists := c.schemas[name]
	if !exists {
		return ErrSchemaNotFound
	}
	if len(schema.tables) > 0 || len(schema.views) > 0 {
		return ErrSchemaNotEmpty
	}

	delete(c.schemas, name)
	return nil
}

// LookupSchema returns a defensive copy of the requested schema descriptor.
func (c *Memory) LookupSchema(name string) (*SchemaDescriptor, error) {
	if err := validateSchemaName(name); err != nil {
		return nil, err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.schemas == nil {
		return nil, ErrSchemaNotFound
	}

	schema, exists := c.schemas[name]
	if !exists {
		return nil, ErrSchemaNotFound
	}

	return cloneSchemaDescriptor(&schema.descriptor), nil
}

// CreateTable registers a new table in an existing schema.
func (c *Memory) CreateTable(desc *TableDescriptor) error {
	if err := validateTableDescriptor(desc); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.schemas == nil {
		c.schemas = make(map[string]*schemaEntry)
	}

	schema, exists := c.schemas[desc.ID.Schema]
	if !exists {
		return ErrSchemaNotFound
	}
	if _, exists := schema.tables[desc.ID.Name]; exists {
		return ErrTableExists
	}
	if _, exists := schema.views[desc.ID.Name]; exists {
		return ErrTableExists
	}

	clone := cloneTableDescriptor(desc)
	table := &tableEntry{
		descriptor: *clone,
		columns:    make(map[string]ColumnDescriptor, len(clone.Columns)),
	}
	for index := range clone.Columns {
		column := cloneColumnDescriptor(clone.Columns[index])
		table.columns[column.Name] = *column
	}

	schema.tables[desc.ID.Name] = table
	return nil
}

// DropTable removes a table from its schema.
func (c *Memory) DropTable(id storage.TableID) error {
	if err := validateTableID(id); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.schemas == nil {
		return ErrSchemaNotFound
	}

	schema, exists := c.schemas[id.Schema]
	if !exists {
		return ErrSchemaNotFound
	}
	if _, exists := schema.tables[id.Name]; !exists {
		return ErrTableNotFound
	}

	delete(schema.tables, id.Name)
	return nil
}

// LookupTable returns a defensive copy of the requested table descriptor.
func (c *Memory) LookupTable(id storage.TableID) (*TableDescriptor, error) {
	if err := validateTableID(id); err != nil {
		return nil, err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.schemas == nil {
		return nil, ErrSchemaNotFound
	}

	schema, exists := c.schemas[id.Schema]
	if !exists {
		return nil, ErrSchemaNotFound
	}
	table, exists := schema.tables[id.Name]
	if !exists {
		return nil, ErrTableNotFound
	}

	return cloneTableDescriptor(&table.descriptor), nil
}

// CreateView registers a new logical view in an existing schema.
func (c *Memory) CreateView(desc *ViewDescriptor) error {
	if err := validateViewDescriptor(desc); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.schemas == nil {
		c.schemas = make(map[string]*schemaEntry)
	}

	schema, exists := c.schemas[desc.ID.Schema]
	if !exists {
		return ErrSchemaNotFound
	}
	if schema.views == nil {
		schema.views = make(map[string]*viewEntry)
	}
	if _, exists := schema.tables[desc.ID.Name]; exists {
		return ErrViewExists
	}
	if _, exists := schema.views[desc.ID.Name]; exists {
		return ErrViewExists
	}

	clone := cloneViewDescriptor(desc)
	view := &viewEntry{
		descriptor: *clone,
		columns:    make(map[string]ColumnDescriptor, len(clone.Columns)),
	}
	for index := range clone.Columns {
		column := cloneColumnDescriptor(clone.Columns[index])
		view.columns[column.Name] = *column
	}

	schema.views[desc.ID.Name] = view
	return nil
}

// DropView removes a logical view from its schema.
func (c *Memory) DropView(id storage.TableID) error {
	if err := validateTableID(id); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.schemas == nil {
		return ErrSchemaNotFound
	}

	schema, exists := c.schemas[id.Schema]
	if !exists {
		return ErrSchemaNotFound
	}
	if _, exists := schema.views[id.Name]; !exists {
		return ErrViewNotFound
	}

	delete(schema.views, id.Name)
	return nil
}

// LookupView returns a defensive copy of the requested view descriptor.
func (c *Memory) LookupView(id storage.TableID) (*ViewDescriptor, error) {
	if err := validateTableID(id); err != nil {
		return nil, err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.schemas == nil {
		return nil, ErrSchemaNotFound
	}

	schema, exists := c.schemas[id.Schema]
	if !exists {
		return nil, ErrSchemaNotFound
	}
	view, exists := schema.views[id.Name]
	if !exists {
		return nil, ErrViewNotFound
	}

	return cloneViewDescriptor(&view.descriptor), nil
}

// LookupColumn returns a defensive copy of the requested column descriptor.
func (c *Memory) LookupColumn(id storage.TableID, name string) (*ColumnDescriptor, error) {
	if err := validateTableID(id); err != nil {
		return nil, err
	}
	if err := validateColumnName(name); err != nil {
		return nil, ErrInvalidColumnDescriptor
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.schemas == nil {
		return nil, ErrSchemaNotFound
	}

	schema, exists := c.schemas[id.Schema]
	if !exists {
		return nil, ErrSchemaNotFound
	}
	table, exists := schema.tables[id.Name]
	if !exists {
		view, viewExists := schema.views[id.Name]
		if !viewExists {
			return nil, ErrTableNotFound
		}

		column, exists := view.columns[name]
		if !exists {
			return nil, ErrColumnNotFound
		}

		return cloneColumnDescriptor(column), nil
	}

	column, exists := table.columns[name]
	if !exists {
		return nil, ErrColumnNotFound
	}

	return cloneColumnDescriptor(column), nil
}

type schemaEntry struct {
	descriptor SchemaDescriptor
	tables     map[string]*tableEntry
	views      map[string]*viewEntry
}

type tableEntry struct {
	descriptor TableDescriptor
	columns    map[string]ColumnDescriptor
}

type viewEntry struct {
	descriptor ViewDescriptor
	columns    map[string]ColumnDescriptor
}
