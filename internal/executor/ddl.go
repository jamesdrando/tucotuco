package executor

import (
	"errors"
	"io"

	"github.com/jamesdrando/tucotuco/internal/catalog"
	"github.com/jamesdrando/tucotuco/internal/storage"
)

var errDDLNilCatalog = errors.New("executor: DDL catalog is nil")

// TableCoordinator optionally mirrors executor DDL changes into a storage-side
// table lifecycle hook.
//
// The coordinator is local to the executor package so Phase 1 DDL can stay
// catalog-led until a broader public or storage contract is introduced.
type TableCoordinator interface {
	CreateTable(tx storage.Transaction, desc *catalog.TableDescriptor) error
	DropTable(tx storage.Transaction, desc *catalog.TableDescriptor) error
}

// CreateSchema is the executor-native CREATE SCHEMA operator.
//
// The operator performs its catalog-side work during Open and produces no rows.
type CreateSchema struct {
	lifecycle lifecycle
	catalog   catalog.Catalog
	desc      *catalog.SchemaDescriptor
}

var _ Operator = (*CreateSchema)(nil)

// NewCreateSchema constructs an executor-native CREATE SCHEMA operator.
func NewCreateSchema(cat catalog.Catalog, desc *catalog.SchemaDescriptor) *CreateSchema {
	return &CreateSchema{
		catalog: cat,
		desc:    desc,
	}
}

// Open creates the schema in the catalog.
func (c *CreateSchema) Open() error {
	if err := c.lifecycle.Open(); err != nil {
		return err
	}
	if c.catalog == nil {
		c.lifecycle = lifecycle{}

		return errDDLNilCatalog
	}
	if err := c.catalog.CreateSchema(c.desc); err != nil {
		c.lifecycle = lifecycle{}

		return err
	}

	return nil
}

// Next reports the CREATE SCHEMA operator's exhausted row stream.
func (c *CreateSchema) Next() (Row, error) {
	if err := c.lifecycle.Next(); err != nil {
		return Row{}, err
	}

	return Row{}, io.EOF
}

// Close terminally closes the CREATE SCHEMA operator.
func (c *CreateSchema) Close() error {
	return c.lifecycle.Close()
}

// DropSchema is the executor-native DROP SCHEMA operator.
//
// The operator performs its catalog-side work during Open and produces no rows.
type DropSchema struct {
	lifecycle lifecycle
	catalog   catalog.Catalog
	name      string
}

var _ Operator = (*DropSchema)(nil)

// NewDropSchema constructs an executor-native DROP SCHEMA operator.
func NewDropSchema(cat catalog.Catalog, name string) *DropSchema {
	return &DropSchema{
		catalog: cat,
		name:    name,
	}
}

// Open drops the schema from the catalog.
func (d *DropSchema) Open() error {
	if err := d.lifecycle.Open(); err != nil {
		return err
	}
	if d.catalog == nil {
		d.lifecycle = lifecycle{}

		return errDDLNilCatalog
	}
	if err := d.catalog.DropSchema(d.name); err != nil {
		d.lifecycle = lifecycle{}

		return err
	}

	return nil
}

// Next reports the DROP SCHEMA operator's exhausted row stream.
func (d *DropSchema) Next() (Row, error) {
	if err := d.lifecycle.Next(); err != nil {
		return Row{}, err
	}

	return Row{}, io.EOF
}

// Close terminally closes the DROP SCHEMA operator.
func (d *DropSchema) Close() error {
	return d.lifecycle.Close()
}

// CreateTable is the Phase 1 executor-native CREATE TABLE operator.
//
// The operator performs its catalog-side work during Open and produces no rows.
type CreateTable struct {
	lifecycle   lifecycle
	catalog     catalog.Catalog
	tx          storage.Transaction
	coordinator TableCoordinator
	desc        *catalog.TableDescriptor
}

var _ Operator = (*CreateTable)(nil)

// NewCreateTable constructs an executor-native CREATE TABLE operator.
func NewCreateTable(
	cat catalog.Catalog,
	tx storage.Transaction,
	coordinator TableCoordinator,
	desc *catalog.TableDescriptor,
) *CreateTable {
	return &CreateTable{
		catalog:     cat,
		tx:          tx,
		coordinator: coordinator,
		desc:        desc,
	}
}

// Open creates the table in the catalog and then runs any optional storage
// coordination.
func (c *CreateTable) Open() error {
	if err := c.lifecycle.Open(); err != nil {
		return err
	}
	if c.catalog == nil {
		c.lifecycle = lifecycle{}

		return errDDLNilCatalog
	}
	if err := c.catalog.CreateTable(c.desc); err != nil {
		c.lifecycle = lifecycle{}

		return err
	}
	if c.coordinator == nil {
		return nil
	}

	if err := c.coordinator.CreateTable(c.tx, c.desc); err != nil {
		rollbackErr := c.catalog.DropTable(c.desc.ID)
		c.lifecycle = lifecycle{}

		if rollbackErr != nil {
			return errors.Join(err, rollbackErr)
		}

		return err
	}

	return nil
}

// Next reports the CREATE TABLE operator's exhausted row stream.
func (c *CreateTable) Next() (Row, error) {
	if err := c.lifecycle.Next(); err != nil {
		return Row{}, err
	}

	return Row{}, io.EOF
}

// Close terminally closes the CREATE TABLE operator.
func (c *CreateTable) Close() error {
	return c.lifecycle.Close()
}

// DropTable is the Phase 1 executor-native DROP TABLE operator.
//
// The operator performs its catalog-side work during Open and produces no rows.
type DropTable struct {
	lifecycle   lifecycle
	catalog     catalog.Catalog
	tx          storage.Transaction
	coordinator TableCoordinator
	table       storage.TableID
}

var _ Operator = (*DropTable)(nil)

// NewDropTable constructs an executor-native DROP TABLE operator.
func NewDropTable(
	cat catalog.Catalog,
	tx storage.Transaction,
	coordinator TableCoordinator,
	table storage.TableID,
) *DropTable {
	return &DropTable{
		catalog:     cat,
		tx:          tx,
		coordinator: coordinator,
		table:       table,
	}
}

// Open drops the table from the catalog and then runs any optional storage
// coordination.
func (d *DropTable) Open() error {
	if err := d.lifecycle.Open(); err != nil {
		return err
	}
	if d.catalog == nil {
		d.lifecycle = lifecycle{}

		return errDDLNilCatalog
	}

	desc, err := d.catalog.LookupTable(d.table)
	if err != nil {
		d.lifecycle = lifecycle{}

		return err
	}
	if err := d.catalog.DropTable(d.table); err != nil {
		d.lifecycle = lifecycle{}

		return err
	}
	if d.coordinator == nil {
		return nil
	}

	if err := d.coordinator.DropTable(d.tx, desc); err != nil {
		rollbackErr := d.catalog.CreateTable(desc)
		d.lifecycle = lifecycle{}

		if rollbackErr != nil {
			return errors.Join(err, rollbackErr)
		}

		return err
	}

	return nil
}

// Next reports the DROP TABLE operator's exhausted row stream.
func (d *DropTable) Next() (Row, error) {
	if err := d.lifecycle.Next(); err != nil {
		return Row{}, err
	}

	return Row{}, io.EOF
}

// Close terminally closes the DROP TABLE operator.
func (d *DropTable) Close() error {
	return d.lifecycle.Close()
}

// CreateView is the executor-native CREATE VIEW operator.
//
// The operator performs its catalog-side work during Open and produces no rows.
type CreateView struct {
	lifecycle lifecycle
	catalog   catalog.Catalog
	desc      *catalog.ViewDescriptor
}

var _ Operator = (*CreateView)(nil)

// NewCreateView constructs an executor-native CREATE VIEW operator.
func NewCreateView(cat catalog.Catalog, desc *catalog.ViewDescriptor) *CreateView {
	return &CreateView{
		catalog: cat,
		desc:    desc,
	}
}

// Open creates the view in the catalog.
func (c *CreateView) Open() error {
	if err := c.lifecycle.Open(); err != nil {
		return err
	}
	if c.catalog == nil {
		c.lifecycle = lifecycle{}

		return errDDLNilCatalog
	}
	if err := c.catalog.CreateView(c.desc); err != nil {
		c.lifecycle = lifecycle{}

		return err
	}

	return nil
}

// Next reports the CREATE VIEW operator's exhausted row stream.
func (c *CreateView) Next() (Row, error) {
	if err := c.lifecycle.Next(); err != nil {
		return Row{}, err
	}

	return Row{}, io.EOF
}

// Close terminally closes the CREATE VIEW operator.
func (c *CreateView) Close() error {
	return c.lifecycle.Close()
}

// DropView is the executor-native DROP VIEW operator.
//
// The operator performs its catalog-side work during Open and produces no rows.
type DropView struct {
	lifecycle lifecycle
	catalog   catalog.Catalog
	view      storage.TableID
}

var _ Operator = (*DropView)(nil)

// NewDropView constructs an executor-native DROP VIEW operator.
func NewDropView(cat catalog.Catalog, view storage.TableID) *DropView {
	return &DropView{
		catalog: cat,
		view:    view,
	}
}

// Open drops the view from the catalog.
func (d *DropView) Open() error {
	if err := d.lifecycle.Open(); err != nil {
		return err
	}
	if d.catalog == nil {
		d.lifecycle = lifecycle{}

		return errDDLNilCatalog
	}
	if err := d.catalog.DropView(d.view); err != nil {
		d.lifecycle = lifecycle{}

		return err
	}

	return nil
}

// Next reports the DROP VIEW operator's exhausted row stream.
func (d *DropView) Next() (Row, error) {
	if err := d.lifecycle.Next(); err != nil {
		return Row{}, err
	}

	return Row{}, io.EOF
}

// Close terminally closes the DROP VIEW operator.
func (d *DropView) Close() error {
	return d.lifecycle.Close()
}
