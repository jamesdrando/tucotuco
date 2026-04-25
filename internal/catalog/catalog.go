package catalog

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/types"
)

var (
	// ErrInvalidSchemaDescriptor reports a nil or malformed schema descriptor.
	ErrInvalidSchemaDescriptor = errors.New("catalog: invalid schema descriptor")
	// ErrInvalidTableDescriptor reports a nil or malformed table descriptor.
	ErrInvalidTableDescriptor = errors.New("catalog: invalid table descriptor")
	// ErrInvalidViewDescriptor reports a nil or malformed view descriptor.
	ErrInvalidViewDescriptor = errors.New("catalog: invalid view descriptor")
	// ErrInvalidColumnDescriptor reports a nil or malformed column descriptor.
	ErrInvalidColumnDescriptor = errors.New("catalog: invalid column descriptor")
	// ErrInvalidExpressionDescriptor reports a nil or malformed SQL expression descriptor.
	ErrInvalidExpressionDescriptor = errors.New("catalog: invalid expression descriptor")
	// ErrInvalidConstraintDescriptor reports a nil or malformed constraint descriptor.
	ErrInvalidConstraintDescriptor = errors.New("catalog: invalid constraint descriptor")
	// ErrInvalidReferenceDescriptor reports a nil or malformed foreign-key reference descriptor.
	ErrInvalidReferenceDescriptor = errors.New("catalog: invalid reference descriptor")
	// ErrInvalidGenerationDescriptor reports a nil or malformed generated-column descriptor.
	ErrInvalidGenerationDescriptor = errors.New("catalog: invalid generated column descriptor")
	// ErrInvalidIdentityDescriptor reports a nil or malformed identity descriptor.
	ErrInvalidIdentityDescriptor = errors.New("catalog: invalid identity descriptor")
	// ErrInvalidPeriodDescriptor reports a nil or malformed period descriptor.
	ErrInvalidPeriodDescriptor = errors.New("catalog: invalid period descriptor")
	// ErrInvalidSchemaName reports an empty or blank schema name.
	ErrInvalidSchemaName = errors.New("catalog: invalid schema name")
	// ErrInvalidTableIdentifier reports an empty or blank table identifier.
	ErrInvalidTableIdentifier = errors.New("catalog: invalid table identifier")
	// ErrSchemaExists reports an attempt to create a duplicate schema.
	ErrSchemaExists = errors.New("catalog: schema already exists")
	// ErrSchemaNotFound reports a missing schema.
	ErrSchemaNotFound = errors.New("catalog: schema not found")
	// ErrSchemaNotEmpty reports an attempt to drop a schema that still owns tables.
	ErrSchemaNotEmpty = errors.New("catalog: schema is not empty")
	// ErrTableExists reports an attempt to create a duplicate table.
	ErrTableExists = errors.New("catalog: table already exists")
	// ErrTableNotFound reports a missing table.
	ErrTableNotFound = errors.New("catalog: table not found")
	// ErrViewExists reports an attempt to create a duplicate view.
	ErrViewExists = errors.New("catalog: view already exists")
	// ErrViewNotFound reports a missing view.
	ErrViewNotFound = errors.New("catalog: view not found")
	// ErrColumnNotFound reports a missing column.
	ErrColumnNotFound = errors.New("catalog: column not found")
)

// Catalog exposes schema, table, and view metadata operations.
type Catalog interface {
	CreateSchema(*SchemaDescriptor) error
	DropSchema(string) error
	LookupSchema(string) (*SchemaDescriptor, error)
	CreateTable(*TableDescriptor) error
	DropTable(storage.TableID) error
	LookupTable(storage.TableID) (*TableDescriptor, error)
	CreateView(*ViewDescriptor) error
	DropView(storage.TableID) error
	LookupView(storage.TableID) (*ViewDescriptor, error)
	LookupColumn(storage.TableID, string) (*ColumnDescriptor, error)
}

// SchemaDescriptor describes a schema and its schema-level defaults.
type SchemaDescriptor struct {
	Name             string
	DefaultCollation string
}

// TableDescriptor describes a table, its columns, and table-level DDL metadata.
type TableDescriptor struct {
	ID          storage.TableID
	Columns     []ColumnDescriptor
	Constraints []ConstraintDescriptor
	Periods     []PeriodDescriptor
}

// ViewDescriptor describes a logical view and its derived output columns.
type ViewDescriptor struct {
	ID          storage.TableID
	Columns     []ColumnDescriptor
	Query       ExpressionDescriptor
	CheckOption string
}

// ColumnDescriptor describes a single table column and its DDL metadata.
type ColumnDescriptor struct {
	Name        string
	Type        types.TypeDesc
	Default     *ExpressionDescriptor
	Generated   *GenerationDescriptor
	Identity    *IdentityDescriptor
	Constraints []ConstraintDescriptor
	Collation   string
}

// ExpressionDescriptor stores SQL text without binding the catalog to parser AST types.
type ExpressionDescriptor struct {
	SQL string
}

// ConstraintKind identifies the SQL constraint category.
type ConstraintKind string

const (
	// ConstraintKindNotNull models a NOT NULL constraint.
	ConstraintKindNotNull ConstraintKind = "NOT NULL"
	// ConstraintKindCheck models a CHECK constraint.
	ConstraintKindCheck ConstraintKind = "CHECK"
	// ConstraintKindUnique models a UNIQUE constraint.
	ConstraintKindUnique ConstraintKind = "UNIQUE"
	// ConstraintKindPrimaryKey models a PRIMARY KEY constraint.
	ConstraintKindPrimaryKey ConstraintKind = "PRIMARY KEY"
	// ConstraintKindForeignKey models a REFERENCES / FOREIGN KEY constraint.
	ConstraintKindForeignKey ConstraintKind = "FOREIGN KEY"
)

// ConstraintDescriptor describes a column-level or table-level constraint.
type ConstraintDescriptor struct {
	Name              string
	Kind              ConstraintKind
	Columns           []string
	Expression        *ExpressionDescriptor
	Reference         *ReferenceDescriptor
	Deferrable        bool
	InitiallyDeferred bool
}

// ReferentialAction describes a foreign-key update/delete action.
type ReferentialAction string

const (
	// ReferentialActionCascade cascades the parent change.
	ReferentialActionCascade ReferentialAction = "CASCADE"
	// ReferentialActionSetNull sets referencing columns to NULL.
	ReferentialActionSetNull ReferentialAction = "SET NULL"
	// ReferentialActionSetDefault sets referencing columns to their DEFAULT values.
	ReferentialActionSetDefault ReferentialAction = "SET DEFAULT"
	// ReferentialActionRestrict rejects the parent change immediately.
	ReferentialActionRestrict ReferentialAction = "RESTRICT"
	// ReferentialActionNoAction rejects the parent change at constraint-check time.
	ReferentialActionNoAction ReferentialAction = "NO ACTION"
)

// ReferenceDescriptor stores the referenced table metadata for a foreign key.
type ReferenceDescriptor struct {
	Table    storage.TableID
	Columns  []string
	OnDelete ReferentialAction
	OnUpdate ReferentialAction
}

// GenerationStorage describes how a generated column is materialized.
type GenerationStorage string

const (
	// GenerationStorageStored models a stored generated column.
	GenerationStorageStored GenerationStorage = "STORED"
)

// GenerationDescriptor stores generated-column metadata.
type GenerationDescriptor struct {
	Expression ExpressionDescriptor
	Storage    GenerationStorage
}

// IdentityGeneration describes how an identity column supplies values.
type IdentityGeneration string

const (
	// IdentityGenerationAlways models GENERATED ALWAYS AS IDENTITY.
	IdentityGenerationAlways IdentityGeneration = "ALWAYS"
	// IdentityGenerationByDefault models GENERATED BY DEFAULT AS IDENTITY.
	IdentityGenerationByDefault IdentityGeneration = "BY DEFAULT"
)

// IdentityDescriptor stores SQL identity-column metadata.
type IdentityDescriptor struct {
	Generation IdentityGeneration
	Start      *int64
	Increment  *int64
	MinValue   *int64
	MaxValue   *int64
	Cycle      bool
}

// PeriodKind identifies the SQL temporal period category.
type PeriodKind string

const (
	// PeriodKindSystemTime models PERIOD FOR SYSTEM_TIME.
	PeriodKindSystemTime PeriodKind = "SYSTEM_TIME"
	// PeriodKindApplicationTime models PERIOD FOR APPLICATION_TIME.
	PeriodKindApplicationTime PeriodKind = "APPLICATION_TIME"
)

// PeriodDescriptor stores the start and end columns for a table period.
type PeriodDescriptor struct {
	Kind        PeriodKind
	StartColumn string
	EndColumn   string
}

func validateSchemaDescriptor(desc *SchemaDescriptor) error {
	if desc == nil {
		return ErrInvalidSchemaDescriptor
	}
	if err := validateSchemaName(desc.Name); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSchemaDescriptor, err)
	}
	if err := validateOptionalText(desc.DefaultCollation, ErrInvalidSchemaDescriptor); err != nil {
		return err
	}

	return nil
}

func validateSchemaName(name string) error {
	if strings.TrimSpace(name) == "" {
		return ErrInvalidSchemaName
	}

	return nil
}

func validateTableDescriptor(desc *TableDescriptor) error {
	if desc == nil {
		return ErrInvalidTableDescriptor
	}
	if err := validateTableID(desc.ID); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidTableDescriptor, err)
	}
	if len(desc.Columns) == 0 {
		return ErrInvalidTableDescriptor
	}

	columns := make(map[string]ColumnDescriptor, len(desc.Columns))
	for index := range desc.Columns {
		if err := validateColumnDescriptor(&desc.Columns[index]); err != nil {
			return fmt.Errorf("%w: column %d: %w", ErrInvalidTableDescriptor, index, err)
		}
		if _, exists := columns[desc.Columns[index].Name]; exists {
			return fmt.Errorf("%w: duplicate column %q", ErrInvalidTableDescriptor, desc.Columns[index].Name)
		}
		columns[desc.Columns[index].Name] = desc.Columns[index]
	}
	for index := range desc.Constraints {
		if err := validateConstraintDescriptor(&desc.Constraints[index]); err != nil {
			return fmt.Errorf("%w: constraint %d: %w", ErrInvalidTableDescriptor, index, err)
		}
		if err := validateTableConstraintDescriptor(&desc.Constraints[index]); err != nil {
			return fmt.Errorf("%w: constraint %d: %w", ErrInvalidTableDescriptor, index, err)
		}
		for _, name := range desc.Constraints[index].Columns {
			if _, exists := columns[name]; !exists {
				return fmt.Errorf("%w: constraint %d: unknown column %q", ErrInvalidTableDescriptor, index, name)
			}
		}
	}

	seenPeriods := make(map[PeriodKind]struct{}, len(desc.Periods))
	for index := range desc.Periods {
		period := &desc.Periods[index]
		if err := validatePeriodDescriptor(period); err != nil {
			return fmt.Errorf("%w: period %d: %w", ErrInvalidTableDescriptor, index, err)
		}
		if _, exists := columns[period.StartColumn]; !exists {
			return fmt.Errorf("%w: period %d: unknown start column %q", ErrInvalidTableDescriptor, index, period.StartColumn)
		}
		if _, exists := columns[period.EndColumn]; !exists {
			return fmt.Errorf("%w: period %d: unknown end column %q", ErrInvalidTableDescriptor, index, period.EndColumn)
		}
		if err := validateTablePeriodDescriptor(period, columns); err != nil {
			return fmt.Errorf("%w: period %d: %w", ErrInvalidTableDescriptor, index, err)
		}
		if _, exists := seenPeriods[period.Kind]; exists {
			return fmt.Errorf("%w: duplicate period %q", ErrInvalidTableDescriptor, period.Kind)
		}
		seenPeriods[period.Kind] = struct{}{}
	}

	return nil
}

func validateViewDescriptor(desc *ViewDescriptor) error {
	if desc == nil {
		return ErrInvalidViewDescriptor
	}
	if err := validateTableID(desc.ID); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidViewDescriptor, err)
	}
	if len(desc.Columns) == 0 {
		return ErrInvalidViewDescriptor
	}
	for index := range desc.Columns {
		column := desc.Columns[index]
		column.Default = nil
		column.Generated = nil
		column.Identity = nil
		column.Constraints = nil
		if err := validateColumnDescriptor(&column); err != nil {
			return fmt.Errorf("%w: column %d: %w", ErrInvalidViewDescriptor, index, err)
		}
		for previous := 0; previous < index; previous++ {
			if desc.Columns[previous].Name == desc.Columns[index].Name {
				return fmt.Errorf("%w: duplicate column %q", ErrInvalidViewDescriptor, desc.Columns[index].Name)
			}
		}
	}
	if err := validateExpressionDescriptor(&desc.Query); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidViewDescriptor, err)
	}
	switch desc.CheckOption {
	case "", "CASCADED", "LOCAL":
	default:
		return fmt.Errorf("%w: invalid check option %q", ErrInvalidViewDescriptor, desc.CheckOption)
	}

	return nil
}

func validateTableID(id storage.TableID) error {
	if strings.TrimSpace(id.Schema) == "" || strings.TrimSpace(id.Name) == "" {
		return ErrInvalidTableIdentifier
	}

	return nil
}

func validateColumnDescriptor(desc *ColumnDescriptor) error {
	if desc == nil {
		return ErrInvalidColumnDescriptor
	}
	if err := validateColumnName(desc.Name); err != nil {
		return ErrInvalidColumnDescriptor
	}
	if err := desc.Type.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidColumnDescriptor, err)
	}
	if err := validateOptionalText(desc.Collation, ErrInvalidColumnDescriptor); err != nil {
		return err
	}
	if desc.Default != nil {
		if err := validateExpressionDescriptor(desc.Default); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidColumnDescriptor, err)
		}
	}
	if desc.Generated != nil {
		if err := validateGenerationDescriptor(desc.Generated); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidColumnDescriptor, err)
		}
	}
	if desc.Identity != nil {
		if err := validateIdentityDescriptor(desc.Identity); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidColumnDescriptor, err)
		}
	}
	if desc.Generated != nil && desc.Default != nil {
		return fmt.Errorf("%w: generated column cannot declare DEFAULT", ErrInvalidColumnDescriptor)
	}
	if desc.Generated != nil && desc.Identity != nil {
		return fmt.Errorf("%w: generated column cannot declare IDENTITY", ErrInvalidColumnDescriptor)
	}
	for index := range desc.Constraints {
		if err := validateConstraintDescriptor(&desc.Constraints[index]); err != nil {
			return fmt.Errorf("%w: constraint %d: %w", ErrInvalidColumnDescriptor, index, err)
		}
	}

	return nil
}

func validateColumnName(name string) error {
	if strings.TrimSpace(name) == "" {
		return ErrInvalidColumnDescriptor
	}

	return nil
}

func validateExpressionDescriptor(desc *ExpressionDescriptor) error {
	if desc == nil {
		return ErrInvalidExpressionDescriptor
	}
	if strings.TrimSpace(desc.SQL) == "" {
		return ErrInvalidExpressionDescriptor
	}

	return nil
}

func validateConstraintDescriptor(desc *ConstraintDescriptor) error {
	if desc == nil {
		return ErrInvalidConstraintDescriptor
	}
	if err := validateOptionalText(desc.Name, ErrInvalidConstraintDescriptor); err != nil {
		return err
	}
	if !desc.Kind.valid() {
		return ErrInvalidConstraintDescriptor
	}
	if err := validateNames(desc.Columns, ErrInvalidConstraintDescriptor); err != nil {
		return err
	}
	if desc.Expression != nil {
		if err := validateExpressionDescriptor(desc.Expression); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidConstraintDescriptor, err)
		}
	}
	if desc.Reference != nil {
		if err := validateReferenceDescriptor(desc.Reference); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidConstraintDescriptor, err)
		}
	}
	if desc.InitiallyDeferred && !desc.Deferrable {
		return ErrInvalidConstraintDescriptor
	}

	switch desc.Kind {
	case ConstraintKindCheck:
		if desc.Expression == nil {
			return ErrInvalidConstraintDescriptor
		}
	case ConstraintKindForeignKey:
		if desc.Reference == nil {
			return ErrInvalidConstraintDescriptor
		}
	}

	return nil
}

func validateTableConstraintDescriptor(desc *ConstraintDescriptor) error {
	if desc == nil {
		return ErrInvalidConstraintDescriptor
	}

	switch desc.Kind {
	case ConstraintKindPrimaryKey, ConstraintKindUnique, ConstraintKindForeignKey:
		if len(desc.Columns) == 0 {
			return fmt.Errorf("%w: %s requires constrained columns", ErrInvalidConstraintDescriptor, desc.Kind)
		}
	}

	return nil
}

func (kind ConstraintKind) valid() bool {
	switch kind {
	case ConstraintKindNotNull, ConstraintKindCheck, ConstraintKindUnique, ConstraintKindPrimaryKey, ConstraintKindForeignKey:
		return true
	default:
		return false
	}
}

func validateReferenceDescriptor(desc *ReferenceDescriptor) error {
	if desc == nil {
		return ErrInvalidReferenceDescriptor
	}
	if err := validateTableID(desc.Table); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidReferenceDescriptor, err)
	}
	if err := validateNames(desc.Columns, ErrInvalidReferenceDescriptor); err != nil {
		return err
	}
	if err := validateReferentialAction(desc.OnDelete); err != nil {
		return err
	}
	if err := validateReferentialAction(desc.OnUpdate); err != nil {
		return err
	}

	return nil
}

func validateReferentialAction(action ReferentialAction) error {
	switch action {
	case "", ReferentialActionCascade, ReferentialActionSetNull, ReferentialActionSetDefault, ReferentialActionRestrict, ReferentialActionNoAction:
		return nil
	default:
		return ErrInvalidReferenceDescriptor
	}
}

func validateGenerationDescriptor(desc *GenerationDescriptor) error {
	if desc == nil {
		return ErrInvalidGenerationDescriptor
	}
	if err := validateExpressionDescriptor(&desc.Expression); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidGenerationDescriptor, err)
	}
	switch desc.Storage {
	case GenerationStorageStored:
		return nil
	default:
		return ErrInvalidGenerationDescriptor
	}
}

func validateIdentityDescriptor(desc *IdentityDescriptor) error {
	if desc == nil {
		return ErrInvalidIdentityDescriptor
	}
	switch desc.Generation {
	case IdentityGenerationAlways, IdentityGenerationByDefault:
		return nil
	default:
		return ErrInvalidIdentityDescriptor
	}
}

func validatePeriodDescriptor(desc *PeriodDescriptor) error {
	if desc == nil {
		return ErrInvalidPeriodDescriptor
	}
	switch desc.Kind {
	case PeriodKindSystemTime, PeriodKindApplicationTime:
	default:
		return ErrInvalidPeriodDescriptor
	}
	if err := validateRequiredText(desc.StartColumn, ErrInvalidPeriodDescriptor); err != nil {
		return err
	}
	if err := validateRequiredText(desc.EndColumn, ErrInvalidPeriodDescriptor); err != nil {
		return err
	}

	return nil
}

func validateTablePeriodDescriptor(desc *PeriodDescriptor, columns map[string]ColumnDescriptor) error {
	if desc == nil {
		return ErrInvalidPeriodDescriptor
	}
	if desc.StartColumn == desc.EndColumn {
		return fmt.Errorf("%w: start and end columns must be distinct", ErrInvalidPeriodDescriptor)
	}
	if !periodColumnTypeCompatible(columns[desc.StartColumn].Type) {
		return fmt.Errorf("%w: start column %q must use a temporal type", ErrInvalidPeriodDescriptor, desc.StartColumn)
	}
	if !periodColumnTypeCompatible(columns[desc.EndColumn].Type) {
		return fmt.Errorf("%w: end column %q must use a temporal type", ErrInvalidPeriodDescriptor, desc.EndColumn)
	}

	return nil
}

func periodColumnTypeCompatible(desc types.TypeDesc) bool {
	switch desc.Kind {
	case types.TypeKindDate,
		types.TypeKindTime,
		types.TypeKindTimeWithTimeZone,
		types.TypeKindTimestamp,
		types.TypeKindTimestampWithTimeZone:
		return true
	default:
		return false
	}
}

func validateOptionalText(text string, err error) error {
	if text != "" && strings.TrimSpace(text) == "" {
		return err
	}

	return nil
}

func validateRequiredText(text string, err error) error {
	if strings.TrimSpace(text) == "" {
		return err
	}

	return nil
}

func validateNames(names []string, err error) error {
	for _, name := range names {
		if strings.TrimSpace(name) == "" {
			return err
		}
	}

	return nil
}

func cloneSchemaDescriptor(desc *SchemaDescriptor) *SchemaDescriptor {
	if desc == nil {
		return nil
	}

	clone := *desc
	return &clone
}

func cloneTableDescriptor(desc *TableDescriptor) *TableDescriptor {
	if desc == nil {
		return nil
	}

	clone := TableDescriptor{ID: desc.ID}
	if len(desc.Columns) > 0 {
		clone.Columns = make([]ColumnDescriptor, len(desc.Columns))
		for index := range desc.Columns {
			clone.Columns[index] = *cloneColumnDescriptor(desc.Columns[index])
		}
	}
	clone.Constraints = cloneConstraintDescriptors(desc.Constraints)
	clone.Periods = slices.Clone(desc.Periods)

	return &clone
}

func cloneViewDescriptor(desc *ViewDescriptor) *ViewDescriptor {
	if desc == nil {
		return nil
	}

	clone := ViewDescriptor{
		ID:          desc.ID,
		Query:       *cloneExpressionDescriptor(&desc.Query),
		CheckOption: desc.CheckOption,
	}
	if len(desc.Columns) > 0 {
		clone.Columns = make([]ColumnDescriptor, len(desc.Columns))
		for index := range desc.Columns {
			clone.Columns[index] = *cloneColumnDescriptor(desc.Columns[index])
		}
	}

	return &clone
}

func cloneColumnDescriptor(desc ColumnDescriptor) *ColumnDescriptor {
	clone := desc
	clone.Default = cloneExpressionDescriptor(desc.Default)
	clone.Generated = cloneGenerationDescriptor(desc.Generated)
	clone.Identity = cloneIdentityDescriptor(desc.Identity)
	clone.Constraints = cloneConstraintDescriptors(desc.Constraints)
	return &clone
}

func cloneExpressionDescriptor(desc *ExpressionDescriptor) *ExpressionDescriptor {
	if desc == nil {
		return nil
	}

	clone := *desc
	return &clone
}

func cloneConstraintDescriptors(descs []ConstraintDescriptor) []ConstraintDescriptor {
	if len(descs) == 0 {
		return nil
	}

	clones := make([]ConstraintDescriptor, len(descs))
	for index := range descs {
		clones[index] = cloneConstraintDescriptor(descs[index])
	}

	return clones
}

func cloneConstraintDescriptor(desc ConstraintDescriptor) ConstraintDescriptor {
	clone := desc
	clone.Columns = slices.Clone(desc.Columns)
	clone.Expression = cloneExpressionDescriptor(desc.Expression)
	clone.Reference = cloneReferenceDescriptor(desc.Reference)
	return clone
}

func cloneReferenceDescriptor(desc *ReferenceDescriptor) *ReferenceDescriptor {
	if desc == nil {
		return nil
	}

	clone := *desc
	clone.Columns = slices.Clone(desc.Columns)
	return &clone
}

func cloneGenerationDescriptor(desc *GenerationDescriptor) *GenerationDescriptor {
	if desc == nil {
		return nil
	}

	clone := *desc
	return &clone
}

func cloneIdentityDescriptor(desc *IdentityDescriptor) *IdentityDescriptor {
	if desc == nil {
		return nil
	}

	clone := *desc
	clone.Start = cloneInt64(desc.Start)
	clone.Increment = cloneInt64(desc.Increment)
	clone.MinValue = cloneInt64(desc.MinValue)
	clone.MaxValue = cloneInt64(desc.MaxValue)
	return &clone
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}

	clone := *value
	return &clone
}
