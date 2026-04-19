package ast

// SelectStmt represents a SELECT query with the Phase 1 clause set.
type SelectStmt struct {
	Span

	// Distinct reports whether the SELECT uses DISTINCT semantics.
	Distinct bool

	// SelectList stores the projected expressions or stars.
	SelectList []*SelectItem

	// From stores the table references or derived sources in the FROM clause.
	From []*FromSource

	// Where stores the optional search condition.
	Where Node

	// GroupBy stores the grouping expressions.
	GroupBy []Node

	// Having stores the optional group filter condition.
	Having Node

	// OrderBy stores the optional ordering terms.
	OrderBy []*OrderByItem

	// Limit stores the optional LIMIT expression.
	Limit Node
}

// Accept dispatches the node to its concrete visitor method.
func (s *SelectStmt) Accept(visitor Visitor) any {
	return visitor.VisitSelectStmt(s)
}

// SelectItem represents one SELECT-list expression with an optional alias.
type SelectItem struct {
	Span

	// Expr stores the projected expression or star.
	Expr Node

	// Alias stores the optional output alias.
	Alias *Identifier
}

// Accept dispatches the node to its concrete visitor method.
func (s *SelectItem) Accept(visitor Visitor) any {
	return visitor.VisitSelectItem(s)
}

// FromSource represents one FROM-clause relation or derived source.
type FromSource struct {
	Span

	// Source stores the referenced table name or derived query node.
	Source Node

	// Alias stores the optional correlation name.
	Alias *Identifier
}

// Accept dispatches the node to its concrete visitor method.
func (f *FromSource) Accept(visitor Visitor) any {
	return visitor.VisitFromSource(f)
}

// OrderByItem represents one ORDER BY expression and its direction.
type OrderByItem struct {
	Span

	// Expr is the value being ordered.
	Expr Node

	// Direction stores the exact ordering keyword such as ASC or DESC.
	Direction string
}

// Accept dispatches the node to its concrete visitor method.
func (o *OrderByItem) Accept(visitor Visitor) any {
	return visitor.VisitOrderByItem(o)
}

// InsertSource marks the allowed source forms for INSERT.
type InsertSource interface {
	Node
	insertSource()
}

// InsertValuesSource represents an INSERT ... VALUES source.
type InsertValuesSource struct {
	Span

	// Rows stores the explicit VALUES tuples.
	Rows [][]Node
}

// Accept dispatches the node to its concrete visitor method.
func (s *InsertValuesSource) Accept(visitor Visitor) any {
	return visitor.VisitInsertValuesSource(s)
}

func (*InsertValuesSource) insertSource() {}

// InsertQuerySource represents an INSERT ... SELECT source.
type InsertQuerySource struct {
	Span

	// Query stores the source query.
	Query Node
}

// Accept dispatches the node to its concrete visitor method.
func (s *InsertQuerySource) Accept(visitor Visitor) any {
	return visitor.VisitInsertQuerySource(s)
}

func (*InsertQuerySource) insertSource() {}

// InsertDefaultValuesSource represents an INSERT ... DEFAULT VALUES source.
type InsertDefaultValuesSource struct {
	Span
}

// Accept dispatches the node to its concrete visitor method.
func (s *InsertDefaultValuesSource) Accept(visitor Visitor) any {
	return visitor.VisitInsertDefaultValuesSource(s)
}

func (*InsertDefaultValuesSource) insertSource() {}

// InsertStmt represents an INSERT statement.
type InsertStmt struct {
	Span

	// Table identifies the target relation.
	Table *QualifiedName

	// Columns stores the optional target column list.
	Columns []*Identifier

	// Source stores exactly one INSERT source form.
	Source InsertSource
}

// Accept dispatches the node to its concrete visitor method.
func (s *InsertStmt) Accept(visitor Visitor) any {
	return visitor.VisitInsertStmt(s)
}

// UpdateAssignment represents one SET target/value group inside UPDATE.
type UpdateAssignment struct {
	Span

	// Columns stores one or more assigned target columns.
	Columns []*Identifier

	// Values stores the expression list assigned to Columns.
	Values []Node
}

// Accept dispatches the node to its concrete visitor method.
func (a *UpdateAssignment) Accept(visitor Visitor) any {
	return visitor.VisitUpdateAssignment(a)
}

// UpdateStmt represents an UPDATE statement.
type UpdateStmt struct {
	Span

	// Table identifies the target relation.
	Table *QualifiedName

	// Assignments stores the SET clause entries.
	Assignments []*UpdateAssignment

	// Where stores the optional search condition.
	Where Node
}

// Accept dispatches the node to its concrete visitor method.
func (s *UpdateStmt) Accept(visitor Visitor) any {
	return visitor.VisitUpdateStmt(s)
}

// DeleteStmt represents a DELETE statement.
type DeleteStmt struct {
	Span

	// Table identifies the target relation.
	Table *QualifiedName

	// Where stores the optional search condition.
	Where Node
}

// Accept dispatches the node to its concrete visitor method.
func (s *DeleteStmt) Accept(visitor Visitor) any {
	return visitor.VisitDeleteStmt(s)
}

// TypeName represents a possibly-qualified SQL type name.
type TypeName struct {
	Span

	// Qualifier stores the optional catalog or schema prefix.
	Qualifier *QualifiedName

	// Names stores one or more identifiers or keywords that make up the type name.
	Names []*Identifier

	// Args stores optional type arguments such as precision or length.
	Args []Node
}

// Accept dispatches the node to its concrete visitor method.
func (t *TypeName) Accept(visitor Visitor) any {
	return visitor.VisitTypeName(t)
}

// ReferenceSpec represents the target relation of a REFERENCES constraint.
type ReferenceSpec struct {
	Span

	// Table identifies the referenced relation.
	Table *QualifiedName

	// Columns stores the optional referenced column list.
	Columns []*Identifier
}

// Accept dispatches the node to its concrete visitor method.
func (r *ReferenceSpec) Accept(visitor Visitor) any {
	return visitor.VisitReferenceSpec(r)
}

// ConstraintKind enumerates the CREATE TABLE constraint shapes supported by the AST.
type ConstraintKind string

const (
	// ConstraintKindNull marks an explicit NULL column constraint.
	ConstraintKindNull ConstraintKind = "NULL"

	// ConstraintKindNotNull marks a NOT NULL column constraint.
	ConstraintKindNotNull ConstraintKind = "NOT NULL"

	// ConstraintKindCheck marks a CHECK constraint.
	ConstraintKindCheck ConstraintKind = "CHECK"

	// ConstraintKindUnique marks a UNIQUE constraint.
	ConstraintKindUnique ConstraintKind = "UNIQUE"

	// ConstraintKindPrimaryKey marks a PRIMARY KEY constraint.
	ConstraintKindPrimaryKey ConstraintKind = "PRIMARY KEY"

	// ConstraintKindReferences marks a column-level REFERENCES constraint.
	ConstraintKindReferences ConstraintKind = "REFERENCES"

	// ConstraintKindForeignKey marks a table-level FOREIGN KEY constraint.
	ConstraintKindForeignKey ConstraintKind = "FOREIGN KEY"
)

// ConstraintDef represents either a column or table constraint.
type ConstraintDef struct {
	Span

	// Name stores the optional constraint name.
	Name *Identifier

	// Kind identifies the constraint form.
	Kind ConstraintKind

	// Columns stores the local constrained columns for table-level constraints.
	Columns []*Identifier

	// Check stores the CHECK expression when Kind is ConstraintKindCheck.
	Check Node

	// Reference stores the target relation for REFERENCES or FOREIGN KEY constraints.
	Reference *ReferenceSpec
}

// Accept dispatches the node to its concrete visitor method.
func (c *ConstraintDef) Accept(visitor Visitor) any {
	return visitor.VisitConstraintDef(c)
}

// ColumnDef represents a single CREATE TABLE column definition.
type ColumnDef struct {
	Span

	// Name identifies the column being declared.
	Name *Identifier

	// Type stores the declared SQL data type.
	Type *TypeName

	// Default stores the optional DEFAULT expression.
	Default Node

	// Constraints stores the column constraints, including nullability.
	Constraints []*ConstraintDef
}

// Accept dispatches the node to its concrete visitor method.
func (c *ColumnDef) Accept(visitor Visitor) any {
	return visitor.VisitColumnDef(c)
}

// CreateTableStmt represents a CREATE TABLE statement.
type CreateTableStmt struct {
	Span

	// Name identifies the table being created.
	Name *QualifiedName

	// Columns stores the declared columns.
	Columns []*ColumnDef

	// Constraints stores the table-level constraints.
	Constraints []*ConstraintDef
}

// Accept dispatches the node to its concrete visitor method.
func (s *CreateTableStmt) Accept(visitor Visitor) any {
	return visitor.VisitCreateTableStmt(s)
}

// DropTableStmt represents a DROP TABLE statement.
type DropTableStmt struct {
	Span

	// Name identifies the table being dropped.
	Name *QualifiedName
}

// Accept dispatches the node to its concrete visitor method.
func (s *DropTableStmt) Accept(visitor Visitor) any {
	return visitor.VisitDropTableStmt(s)
}
