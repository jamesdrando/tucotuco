package analyzer

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jamesdrando/tucotuco/internal/catalog"
	"github.com/jamesdrando/tucotuco/internal/diag"
	"github.com/jamesdrando/tucotuco/internal/parser"
	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/token"
)

const (
	// DefaultSchema is the implicit schema used for unqualified table lookups.
	DefaultSchema = "public"

	sqlStateUndefinedTable  = "42P01"
	sqlStateUndefinedColumn = "42703"
	sqlStateAmbiguousColumn = "42702"
	sqlStateAmbiguousAlias  = "42P09"
)

// Resolver performs name resolution over the current parser CST.
type Resolver struct {
	catalog       catalog.Catalog
	defaultSchema string
}

// NewResolver constructs a resolver that uses the Phase 1 default schema.
func NewResolver(cat catalog.Catalog) *Resolver {
	return NewResolverWithSchema(cat, DefaultSchema)
}

// NewResolverWithSchema constructs a resolver with an explicit default schema.
func NewResolverWithSchema(cat catalog.Catalog, defaultSchema string) *Resolver {
	schema := strings.TrimSpace(defaultSchema)
	if schema == "" {
		schema = DefaultSchema
	}

	return &Resolver{
		catalog:       cat,
		defaultSchema: schema,
	}
}

// ResolveScript resolves table and column names for a parsed SQL script.
func (r *Resolver) ResolveScript(script *parser.Script) (*Bindings, []diag.Diagnostic) {
	pass := resolvePass{
		resolver: r,
		bindings: newBindings(),
	}
	pass.resolveScript(script)

	return pass.bindings, pass.diagnostics
}

// Bindings stores the relations and columns resolved for one script.
type Bindings struct {
	relations map[parser.Node]*RelationBinding
	columns   map[parser.Node]*ColumnBinding
	stars     map[*parser.Star][]*ColumnBinding
}

func newBindings() *Bindings {
	return &Bindings{
		relations: make(map[parser.Node]*RelationBinding),
		columns:   make(map[parser.Node]*ColumnBinding),
		stars:     make(map[*parser.Star][]*ColumnBinding),
	}
}

// Relation returns the resolved relation bound to a CST node.
func (b *Bindings) Relation(node parser.Node) (*RelationBinding, bool) {
	if b == nil {
		return nil, false
	}

	relation, ok := b.relations[node]
	return relation, ok
}

// Column returns the resolved column bound to a CST node.
func (b *Bindings) Column(node parser.Node) (*ColumnBinding, bool) {
	if b == nil {
		return nil, false
	}

	column, ok := b.columns[node]
	return column, ok
}

// Star returns the resolved column expansion for a star expression.
func (b *Bindings) Star(node *parser.Star) ([]*ColumnBinding, bool) {
	if b == nil {
		return nil, false
	}

	columns, ok := b.stars[node]
	if !ok {
		return nil, false
	}

	out := make([]*ColumnBinding, len(columns))
	copy(out, columns)
	return out, true
}

// RelationBinding describes one visible relation in a scope.
type RelationBinding struct {
	// Name is the visible relation or alias name in the current scope.
	Name string

	// TableID is the catalog identifier for catalog-backed relations.
	TableID storage.TableID

	// Descriptor is the resolved table descriptor for catalog-backed relations.
	Descriptor *catalog.TableDescriptor

	// Columns stores the visible columns exposed by the relation.
	Columns []*ColumnBinding

	columnIndex map[string][]*ColumnBinding
}

func newRelationBinding(name string, tableID storage.TableID, desc *catalog.TableDescriptor) *RelationBinding {
	return &RelationBinding{
		Name:        name,
		TableID:     tableID,
		Descriptor:  desc,
		columnIndex: make(map[string][]*ColumnBinding),
	}
}

func newCatalogRelation(name string, tableID storage.TableID, desc *catalog.TableDescriptor) *RelationBinding {
	relation := newRelationBinding(name, tableID, desc)
	if desc == nil {
		return relation
	}

	for index := range desc.Columns {
		column := &desc.Columns[index]
		relation.addColumn(column.Name, column, nil)
	}

	return relation
}

func (r *RelationBinding) addColumn(name string, desc *catalog.ColumnDescriptor, source parser.Node) *ColumnBinding {
	if r == nil || strings.TrimSpace(name) == "" {
		return nil
	}

	column := &ColumnBinding{
		Name:       name,
		Relation:   r,
		Descriptor: desc,
		Source:     source,
	}
	r.Columns = append(r.Columns, column)
	r.columnIndex[name] = append(r.columnIndex[name], column)

	return column
}

func (r *RelationBinding) addExistingColumn(column *ColumnBinding) *ColumnBinding {
	if column == nil {
		return nil
	}

	return r.addColumn(column.Name, column.Descriptor, column.Source)
}

func (r *RelationBinding) lookup(name string) []*ColumnBinding {
	if r == nil {
		return nil
	}

	return r.columnIndex[name]
}

// ColumnBinding describes one resolved column reference or projected column.
type ColumnBinding struct {
	// Name is the visible column name in the current scope.
	Name string

	// Relation identifies the relation that exposes the column.
	Relation *RelationBinding

	// Descriptor is the catalog descriptor for catalog-backed columns.
	Descriptor *catalog.ColumnDescriptor

	// Source is the CST node that produces the column for derived scopes.
	Source parser.Node
}

type resolvePass struct {
	resolver    *Resolver
	bindings    *Bindings
	diagnostics []diag.Diagnostic
}

type scope struct {
	relations []*RelationBinding
}

func newScope(relations ...*RelationBinding) *scope {
	out := &scope{}
	out.add(relations...)
	return out
}

func (s *scope) add(relations ...*RelationBinding) {
	if s == nil {
		return
	}

	for _, relation := range relations {
		if relation == nil {
			continue
		}
		s.relations = append(s.relations, relation)
	}
}

func (s *scope) append(other *scope) *scope {
	out := &scope{}
	if s != nil {
		out.add(s.relations...)
	}
	if other != nil {
		out.add(other.relations...)
	}

	return out
}

func (s *scope) lookupUnqualified(name string) []*ColumnBinding {
	if s == nil {
		return nil
	}

	var matches []*ColumnBinding
	for _, relation := range s.relations {
		matches = append(matches, relation.lookup(name)...)
	}

	return matches
}

func (s *scope) lookupRelations(name string) []*RelationBinding {
	if s == nil {
		return nil
	}

	var matches []*RelationBinding
	for _, relation := range s.relations {
		if relation != nil && relation.Name == name {
			matches = append(matches, relation)
		}
	}

	return matches
}

func (s *scope) allColumns() []*ColumnBinding {
	if s == nil {
		return nil
	}

	var columns []*ColumnBinding
	for _, relation := range s.relations {
		columns = append(columns, relation.Columns...)
	}

	return columns
}

func (p *resolvePass) resolveScript(script *parser.Script) {
	if script == nil {
		return
	}

	for _, node := range script.Nodes {
		p.resolveStatement(node)
	}
}

func (p *resolvePass) resolveStatement(node parser.Node) {
	switch node := node.(type) {
	case *parser.SelectStmt:
		p.resolveSelect(node)
	case *parser.InsertStmt:
		p.resolveInsert(node)
	case *parser.UpdateStmt:
		p.resolveUpdate(node)
	case *parser.DeleteStmt:
		p.resolveDelete(node)
	case *parser.CreateTableStmt:
		p.resolveCreateTable(node)
	case *parser.DropTableStmt:
		p.resolveDropTable(node)
	}
}

func (p *resolvePass) resolveSelect(stmt *parser.SelectStmt) *RelationBinding {
	if stmt == nil {
		return nil
	}

	baseScope := newScope()
	for _, source := range stmt.From {
		baseScope = baseScope.append(p.resolveFromNode(source))
	}

	projection := newRelationBinding("", storage.TableID{}, nil)
	for _, item := range stmt.SelectList {
		p.resolveSelectItem(item, baseScope, projection)
	}

	p.resolveExpr(stmt.Where, baseScope, nil)
	for _, expr := range stmt.GroupBy {
		p.resolveExpr(expr, baseScope, nil)
	}
	p.resolveExpr(stmt.Having, baseScope, nil)
	for _, item := range stmt.OrderBy {
		p.resolveOrderByItem(item, baseScope, projection)
	}

	return projection
}

func (p *resolvePass) resolveSelectItem(item *parser.SelectItem, baseScope *scope, projection *RelationBinding) {
	if item == nil {
		return
	}

	switch expr := item.Expr.(type) {
	case *parser.Star:
		for _, column := range p.resolveProjectionStar(expr, baseScope) {
			projection.addExistingColumn(column)
		}
	default:
		p.resolveExpr(expr, baseScope, nil)
		name := projectedColumnName(item)
		if name == "" {
			return
		}

		var desc *catalog.ColumnDescriptor
		if binding, ok := p.bindings.Column(item.Expr); ok {
			desc = binding.Descriptor
		}
		projection.addColumn(name, desc, item)
	}
}

func projectedColumnName(item *parser.SelectItem) string {
	if item == nil {
		return ""
	}
	if item.Alias != nil {
		return item.Alias.Name
	}

	switch expr := item.Expr.(type) {
	case *parser.Identifier:
		return expr.Name
	case *parser.QualifiedName:
		if len(expr.Parts) == 0 {
			return ""
		}
		return expr.Parts[len(expr.Parts)-1].Name
	default:
		return ""
	}
}

func (p *resolvePass) resolveProjectionStar(star *parser.Star, currentScope *scope) []*ColumnBinding {
	if star == nil {
		return nil
	}

	if star.Qualifier == nil {
		columns := currentScope.allColumns()
		if len(columns) == 0 {
			p.addError(sqlStateUndefinedColumn, star.Pos(), "cannot resolve * without a FROM source")
			return nil
		}

		p.bindings.stars[star] = copyColumns(columns)
		return columns
	}

	relation := p.resolveRelationQualifier(star.Qualifier, currentScope)
	if relation == nil {
		return nil
	}

	p.bindings.stars[star] = copyColumns(relation.Columns)
	return relation.Columns
}

func (p *resolvePass) resolveOrderByItem(item *parser.OrderByItem, baseScope *scope, projection *RelationBinding) {
	if item == nil {
		return
	}

	p.resolveExpr(item.Expr, baseScope, projection)
}

func (p *resolvePass) resolveInsert(stmt *parser.InsertStmt) {
	if stmt == nil {
		return
	}

	target := p.resolveTargetTable(stmt.Table)
	for _, column := range stmt.Columns {
		p.resolveColumnInRelation(column, target)
	}

	switch source := stmt.Source.(type) {
	case *parser.InsertValuesSource:
		for _, row := range source.Rows {
			for _, value := range row {
				p.resolveExpr(value, nil, nil)
			}
		}
	case *parser.InsertQuerySource:
		if query, ok := source.Query.(*parser.SelectStmt); ok {
			p.resolveSelect(query)
		}
	case *parser.InsertDefaultValuesSource:
	}
}

func (p *resolvePass) resolveUpdate(stmt *parser.UpdateStmt) {
	if stmt == nil {
		return
	}

	target := p.resolveTargetTable(stmt.Table)
	targetScope := newScope(target)
	for _, assignment := range stmt.Assignments {
		for _, column := range assignment.Columns {
			p.resolveColumnInRelation(column, target)
		}
		for _, value := range assignment.Values {
			p.resolveExpr(value, targetScope, nil)
		}
	}
	p.resolveExpr(stmt.Where, targetScope, nil)
}

func (p *resolvePass) resolveDelete(stmt *parser.DeleteStmt) {
	if stmt == nil {
		return
	}

	target := p.resolveTargetTable(stmt.Table)
	p.resolveExpr(stmt.Where, newScope(target), nil)
}

func (p *resolvePass) resolveCreateTable(stmt *parser.CreateTableStmt) {
	if stmt == nil {
		return
	}

	local := newRelationBinding(lastNamePart(stmt.Name), storage.TableID{}, nil)
	for _, column := range stmt.Columns {
		if column == nil || column.Name == nil {
			continue
		}
		local.addColumn(column.Name.Name, nil, column)
	}

	localScope := newScope(local)
	for _, column := range stmt.Columns {
		if column == nil {
			continue
		}

		p.resolveExpr(column.Default, localScope, nil)
		for _, constraint := range column.Constraints {
			p.resolveConstraint(constraint, localScope)
		}
	}
	for _, constraint := range stmt.Constraints {
		p.resolveConstraint(constraint, localScope)
	}
}

func (p *resolvePass) resolveConstraint(constraint *parser.ConstraintDef, localScope *scope) {
	if constraint == nil {
		return
	}

	for _, column := range constraint.Columns {
		p.resolveExpr(column, localScope, nil)
	}
	p.resolveExpr(constraint.Check, localScope, nil)
	if constraint.Reference != nil {
		p.resolveReference(constraint.Reference)
	}
}

func (p *resolvePass) resolveReference(ref *parser.ReferenceSpec) {
	if ref == nil {
		return
	}

	target := p.resolveTargetTable(ref.Table)
	for _, column := range ref.Columns {
		p.resolveColumnInRelation(column, target)
	}
}

func (p *resolvePass) resolveDropTable(stmt *parser.DropTableStmt) {
	if stmt == nil {
		return
	}

	p.resolveTargetTable(stmt.Name)
}

func (p *resolvePass) resolveFromNode(node parser.Node) *scope {
	switch node := node.(type) {
	case *parser.FromSource:
		return p.resolveFromSource(node)
	case *parser.JoinExpr:
		return p.resolveJoin(node)
	default:
		return newScope()
	}
}

func (p *resolvePass) resolveFromSource(source *parser.FromSource) *scope {
	if source == nil {
		return newScope()
	}

	switch inner := source.Source.(type) {
	case *parser.QualifiedName:
		relation := p.resolveCatalogRelation(inner, visibleName(source.Alias, inner))
		p.bindRelation(source, relation)
		return newScope(relation)
	case *parser.SelectStmt:
		relation := p.resolveSelect(inner)
		if relation != nil && source.Alias != nil {
			relation.Name = source.Alias.Name
		}
		p.bindRelation(source, relation)
		return newScope(relation)
	case *parser.JoinExpr:
		childScope := p.resolveJoin(inner)
		if source.Alias == nil {
			return childScope
		}

		relation := newRelationBinding(source.Alias.Name, storage.TableID{}, nil)
		for _, column := range childScope.allColumns() {
			relation.addExistingColumn(column)
		}
		p.bindRelation(source, relation)
		return newScope(relation)
	default:
		return newScope()
	}
}

func (p *resolvePass) resolveJoin(join *parser.JoinExpr) *scope {
	if join == nil {
		return newScope()
	}

	leftScope := p.resolveFromNode(join.Left)
	rightScope := p.resolveFromNode(join.Right)

	for _, column := range join.Using {
		p.resolveUsingColumn(column, leftScope, rightScope)
	}
	p.resolveExpr(join.Condition, leftScope.append(rightScope), nil)

	return leftScope.append(rightScope)
}

func (p *resolvePass) resolveUsingColumn(column *parser.Identifier, leftScope *scope, rightScope *scope) {
	if column == nil {
		return
	}

	p.resolveSideUsingColumn(column, leftScope, "left")
	p.resolveSideUsingColumn(column, rightScope, "right")
}

func (p *resolvePass) resolveSideUsingColumn(column *parser.Identifier, currentScope *scope, side string) {
	matches := currentScope.lookupUnqualified(column.Name)
	switch len(matches) {
	case 0:
		p.addError(sqlStateUndefinedColumn, column.Pos(), "column %q does not exist in the %s joined relation", column.Name, side)
	case 1:
		return
	default:
		p.addError(sqlStateAmbiguousColumn, column.Pos(), "column reference %q is ambiguous in the %s joined relation", column.Name, side)
	}
}

func (p *resolvePass) resolveExpr(node parser.Node, currentScope *scope, projection *RelationBinding) {
	switch node := node.(type) {
	case nil:
		return
	case *parser.Identifier:
		p.resolveIdentifier(node, currentScope, projection)
	case *parser.QualifiedName:
		p.resolveQualifiedName(node, currentScope)
	case *parser.UnaryExpr:
		p.resolveExpr(node.Operand, currentScope, projection)
	case *parser.BinaryExpr:
		p.resolveExpr(node.Left, currentScope, projection)
		p.resolveExpr(node.Right, currentScope, projection)
	case *parser.FunctionCall:
		for _, arg := range node.Args {
			switch arg := arg.(type) {
			case *parser.Star:
				if arg.Qualifier != nil {
					p.resolveRelationQualifier(arg.Qualifier, currentScope)
				}
			default:
				p.resolveExpr(arg, currentScope, projection)
			}
		}
	case *parser.CastExpr:
		p.resolveExpr(node.Expr, currentScope, projection)
	case *parser.WhenClause:
		p.resolveExpr(node.Condition, currentScope, projection)
		p.resolveExpr(node.Result, currentScope, projection)
	case *parser.CaseExpr:
		p.resolveExpr(node.Operand, currentScope, projection)
		for _, when := range node.Whens {
			p.resolveExpr(when, currentScope, projection)
		}
		p.resolveExpr(node.Else, currentScope, projection)
	case *parser.BetweenExpr:
		p.resolveExpr(node.Expr, currentScope, projection)
		p.resolveExpr(node.Lower, currentScope, projection)
		p.resolveExpr(node.Upper, currentScope, projection)
	case *parser.InExpr:
		p.resolveExpr(node.Expr, currentScope, projection)
		for _, item := range node.List {
			p.resolveExpr(item, currentScope, projection)
		}
	case *parser.LikeExpr:
		p.resolveExpr(node.Expr, currentScope, projection)
		p.resolveExpr(node.Pattern, currentScope, projection)
		p.resolveExpr(node.Escape, currentScope, projection)
	case *parser.IsExpr:
		p.resolveExpr(node.Expr, currentScope, projection)
		p.resolveExpr(node.Right, currentScope, projection)
	case *parser.SelectStmt:
		p.resolveSelect(node)
	}
}

func (p *resolvePass) resolveIdentifier(node *parser.Identifier, currentScope *scope, projection *RelationBinding) {
	if node == nil {
		return
	}

	if projection != nil {
		switch matches := projection.lookup(node.Name); len(matches) {
		case 1:
			p.bindColumn(node, matches[0])
			return
		case 0:
		default:
			p.addError(sqlStateAmbiguousColumn, node.Pos(), "column reference %q is ambiguous", node.Name)
			return
		}
	}

	matches := currentScope.lookupUnqualified(node.Name)
	switch len(matches) {
	case 0:
		p.addError(sqlStateUndefinedColumn, node.Pos(), "column %q does not exist", node.Name)
	case 1:
		p.bindColumn(node, matches[0])
	default:
		p.addError(sqlStateAmbiguousColumn, node.Pos(), "column reference %q is ambiguous", node.Name)
	}
}

func (p *resolvePass) resolveQualifiedName(node *parser.QualifiedName, currentScope *scope) {
	if node == nil {
		return
	}

	if len(node.Parts) == 1 {
		p.resolveIdentifier(node.Parts[0], currentScope, nil)
		if binding, ok := p.bindings.Column(node.Parts[0]); ok {
			p.bindColumn(node, binding)
		}
		return
	}

	if len(node.Parts) != 2 {
		p.addError(sqlStateUndefinedColumn, node.Pos(), "qualified column reference %q is not supported in Phase 1", qualifiedNameString(node))
		return
	}

	relationName := node.Parts[0].Name
	columnName := node.Parts[1].Name
	relations := currentScope.lookupRelations(relationName)
	switch len(relations) {
	case 0:
		p.addError(sqlStateUndefinedTable, node.Pos(), "relation %q does not exist", relationName)
		return
	case 1:
	default:
		p.addError(sqlStateAmbiguousAlias, node.Pos(), "relation reference %q is ambiguous", relationName)
		return
	}

	matches := relations[0].lookup(columnName)
	switch len(matches) {
	case 0:
		p.addError(sqlStateUndefinedColumn, node.Pos(), "column %q does not exist", qualifiedColumnName(relationName, columnName))
	case 1:
		p.bindColumn(node, matches[0])
	default:
		p.addError(sqlStateAmbiguousColumn, node.Pos(), "column reference %q is ambiguous", qualifiedColumnName(relationName, columnName))
	}
}

func (p *resolvePass) resolveColumnInRelation(node *parser.Identifier, relation *RelationBinding) {
	if node == nil || relation == nil {
		return
	}

	matches := relation.lookup(node.Name)
	switch len(matches) {
	case 0:
		p.addError(sqlStateUndefinedColumn, node.Pos(), "column %q does not exist", node.Name)
	case 1:
		p.bindColumn(node, matches[0])
	default:
		p.addError(sqlStateAmbiguousColumn, node.Pos(), "column reference %q is ambiguous", node.Name)
	}
}

func (p *resolvePass) resolveRelationQualifier(name *parser.QualifiedName, currentScope *scope) *RelationBinding {
	if name == nil {
		return nil
	}

	if len(name.Parts) != 1 {
		p.addError(sqlStateUndefinedTable, name.Pos(), "relation %q does not exist", qualifiedNameString(name))
		return nil
	}

	relations := currentScope.lookupRelations(name.Parts[0].Name)
	switch len(relations) {
	case 0:
		p.addError(sqlStateUndefinedTable, name.Pos(), "relation %q does not exist", name.Parts[0].Name)
		return nil
	case 1:
		return relations[0]
	default:
		p.addError(sqlStateAmbiguousAlias, name.Pos(), "relation reference %q is ambiguous", name.Parts[0].Name)
		return nil
	}
}

func (p *resolvePass) resolveTargetTable(name *parser.QualifiedName) *RelationBinding {
	return p.resolveCatalogRelation(name, lastNamePart(name))
}

func (p *resolvePass) resolveCatalogRelation(name *parser.QualifiedName, visible string) *RelationBinding {
	id, desc, ok := p.lookupCatalogTable(name)
	if !ok {
		return nil
	}

	relation := newCatalogRelation(visible, id, desc)
	p.bindRelation(name, relation)
	return relation
}

func (p *resolvePass) lookupCatalogTable(name *parser.QualifiedName) (storage.TableID, *catalog.TableDescriptor, bool) {
	if name == nil {
		return storage.TableID{}, nil, false
	}
	if p.resolver == nil || p.resolver.catalog == nil {
		p.addError(sqlStateUndefinedTable, name.Pos(), "catalog is unavailable")
		return storage.TableID{}, nil, false
	}

	id, ok := tableIDFromName(name, p.resolver.defaultSchema)
	if !ok {
		p.addError(sqlStateUndefinedTable, name.Pos(), "table reference %q is not supported in Phase 1", qualifiedNameString(name))
		return storage.TableID{}, nil, false
	}

	desc, err := p.resolver.catalog.LookupTable(id)
	if err != nil {
		if errors.Is(err, catalog.ErrSchemaNotFound) || errors.Is(err, catalog.ErrTableNotFound) {
			p.addError(sqlStateUndefinedTable, name.Pos(), "table %q does not exist", id.String())
			return storage.TableID{}, nil, false
		}

		p.addError(sqlStateUndefinedTable, name.Pos(), "could not resolve table %q: %v", id.String(), err)
		return storage.TableID{}, nil, false
	}

	return id, desc, true
}

func tableIDFromName(name *parser.QualifiedName, defaultSchema string) (storage.TableID, bool) {
	if name == nil || len(name.Parts) == 0 {
		return storage.TableID{}, false
	}

	switch len(name.Parts) {
	case 1:
		return storage.TableID{
			Schema: defaultSchema,
			Name:   name.Parts[0].Name,
		}, true
	case 2:
		return storage.TableID{
			Schema: name.Parts[0].Name,
			Name:   name.Parts[1].Name,
		}, true
	default:
		return storage.TableID{}, false
	}
}

func visibleName(alias *parser.Identifier, name *parser.QualifiedName) string {
	if alias != nil {
		return alias.Name
	}

	return lastNamePart(name)
}

func lastNamePart(name *parser.QualifiedName) string {
	if name == nil || len(name.Parts) == 0 {
		return ""
	}

	return name.Parts[len(name.Parts)-1].Name
}

func qualifiedNameString(name *parser.QualifiedName) string {
	if name == nil {
		return ""
	}

	parts := make([]string, 0, len(name.Parts))
	for _, part := range name.Parts {
		if part != nil {
			parts = append(parts, part.Name)
		}
	}

	return strings.Join(parts, ".")
}

func qualifiedColumnName(relationName string, columnName string) string {
	if relationName == "" {
		return columnName
	}

	return relationName + "." + columnName
}

func copyColumns(columns []*ColumnBinding) []*ColumnBinding {
	if len(columns) == 0 {
		return nil
	}

	out := make([]*ColumnBinding, len(columns))
	copy(out, columns)
	return out
}

func (p *resolvePass) bindRelation(node parser.Node, relation *RelationBinding) {
	if node == nil || relation == nil {
		return
	}

	p.bindings.relations[node] = relation
}

func (p *resolvePass) bindColumn(node parser.Node, column *ColumnBinding) {
	if node == nil || column == nil {
		return
	}

	p.bindings.columns[node] = column
}

func (p *resolvePass) addError(sqlState string, pos token.Pos, format string, args ...any) {
	p.diagnostics = append(p.diagnostics, diag.NewError(sqlState, fmt.Sprintf(format, args...), toDiagPosition(pos)))
}

func toDiagPosition(pos token.Pos) diag.Position {
	return diag.Position{
		Line:   pos.Line,
		Column: pos.Column,
		Offset: pos.Offset,
	}
}
