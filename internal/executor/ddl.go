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
