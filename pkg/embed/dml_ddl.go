package embed

import (
	"fmt"
	"strings"

	"github.com/jamesdrando/tucotuco/internal/analyzer"
	"github.com/jamesdrando/tucotuco/internal/catalog"
	"github.com/jamesdrando/tucotuco/internal/executor"
	"github.com/jamesdrando/tucotuco/internal/parser"
	"github.com/jamesdrando/tucotuco/internal/storage"
	"github.com/jamesdrando/tucotuco/internal/storage/memory"
	sqltypes "github.com/jamesdrando/tucotuco/internal/types"
)

func (s *session) exec(sql string) (execOutcome, error) {
	ctx, node, err := s.analyzeSingleStatement(sql)
	if err != nil {
		return execOutcome{}, err
	}

	switch stmt := node.(type) {
	case *parser.SelectStmt:
		return execOutcome{}, featureError(stmt, "SELECT statements must use Query")
	case *parser.BeginStmt, *parser.CommitStmt, *parser.RollbackStmt:
		return execOutcome{}, featureError(
			stmt,
			"transaction-control SQL is not supported here; use DB.Begin / Tx.Commit / Tx.Rollback",
		)
	case *parser.InsertStmt:
		result, err := s.execInsert(ctx, stmt)
		return execOutcome{result: result}, err
	case *parser.UpdateStmt:
		result, err := s.execUpdate(ctx, stmt)
		return execOutcome{result: result}, err
	case *parser.DeleteStmt:
		result, err := s.execDelete(ctx, stmt)
		return execOutcome{result: result}, err
	case *parser.CreateTableStmt:
		if !s.allowDDL {
			return execOutcome{}, featureError(stmt, "DDL is not supported inside explicit transactions in Phase 1 embed")
		}

		err := s.execCreateTable(ctx, stmt)
		return execOutcome{catalogChanged: err == nil}, err
	case *parser.DropTableStmt:
		if !s.allowDDL {
			return execOutcome{}, featureError(stmt, "DDL is not supported inside explicit transactions in Phase 1 embed")
		}

		err := s.execDropTable(stmt)
		return execOutcome{catalogChanged: err == nil}, err
	default:
		return execOutcome{}, featureError(node, "Exec does not support %T statements", node)
	}
}

func (s *session) execInsert(ctx *analysisContext, stmt *parser.InsertStmt) (CommandResult, error) {
	relation, err := resolvedTargetRelation(ctx.bindings, stmt.Table)
	if err != nil {
		return CommandResult{}, err
	}

	columnMap, omitted, err := buildInsertColumnMap(stmt, relation)
	if err != nil {
		return CommandResult{}, err
	}
	if err := checkOmittedInsertColumns(omitted); err != nil {
		return CommandResult{}, err
	}

	switch source := stmt.Source.(type) {
	case *parser.InsertValuesSource:
		rows, err := buildInsertRows(ctx, relation, columnMap, source.Rows)
		if err != nil {
			return CommandResult{}, err
		}

		return runWriteOperator(executor.NewInsertValues(s.store, s.tx, relation.TableID, rows...))
	case *parser.InsertQuerySource:
		query, ok := source.Query.(*parser.SelectStmt)
		if !ok {
			return CommandResult{}, featureError(source, "INSERT query source must be SELECT")
		}

		child, columns, err := buildSelectOperator(query, ctx.bindings, ctx.types, s.store, s.tx)
		if err != nil {
			return CommandResult{}, err
		}
		if len(columns) != countIncludedColumns(columnMap) {
			return CommandResult{}, internalError(source, "INSERT query output count does not match target mapping")
		}

		operator := executor.Operator(child)
		if len(omitted) != 0 {
			operator = newRemapOperator(child, columnMap, len(relation.Descriptor.Columns))
		}

		return runWriteOperator(executor.NewInsertFromChild(s.store, s.tx, relation.TableID, operator))
	case *parser.InsertDefaultValuesSource:
		return CommandResult{}, featureError(source, "INSERT DEFAULT VALUES is not yet executable in Phase 1 embed")
	default:
		return CommandResult{}, featureError(stmt, "unsupported INSERT source %T", source)
	}
}

func (s *session) execUpdate(ctx *analysisContext, stmt *parser.UpdateStmt) (CommandResult, error) {
	relation, err := resolvedTargetRelation(ctx.bindings, stmt.Table)
	if err != nil {
		return CommandResult{}, err
	}

	child, shape, err := buildTargetScan(ctx.bindings, ctx.types, s.store, s.tx, relation, stmt.Where)
	if err != nil {
		return CommandResult{}, err
	}

	assignments := make([]executor.UpdateAssignment, 0, len(stmt.Assignments))
	ordinals := relationColumnOrdinals(relation)
	for _, assignment := range stmt.Assignments {
		if assignment == nil {
			continue
		}

		update := executor.UpdateAssignment{
			Ordinals: make([]int, 0, len(assignment.Columns)),
			Values:   make([]executor.CompiledExpr, 0, len(assignment.Values)),
		}
		for _, column := range assignment.Columns {
			index, ok := ordinals[column.Name]
			if !ok {
				return CommandResult{}, internalError(column, "missing UPDATE target column %q", column.Name)
			}
			update.Ordinals = append(update.Ordinals, index)
		}
		for _, value := range assignment.Values {
			compiled, err := compileExpression(ctx.bindings, ctx.types, value, shape)
			if err != nil {
				return CommandResult{}, err
			}
			update.Values = append(update.Values, compiled)
		}
		assignments = append(assignments, update)
	}

	return runWriteOperator(
		executor.NewUpdate(
			s.store,
			s.tx,
			relation.TableID,
			len(relation.Descriptor.Columns),
			child,
			assignments...,
		),
	)
}

func (s *session) execDelete(ctx *analysisContext, stmt *parser.DeleteStmt) (CommandResult, error) {
	relation, err := resolvedTargetRelation(ctx.bindings, stmt.Table)
	if err != nil {
		return CommandResult{}, err
	}

	child, _, err := buildTargetScan(ctx.bindings, ctx.types, s.store, s.tx, relation, stmt.Where)
	if err != nil {
		return CommandResult{}, err
	}

	return runWriteOperator(executor.NewDelete(s.store, s.tx, relation.TableID, child))
}

func (s *session) execCreateTable(ctx *analysisContext, stmt *parser.CreateTableStmt) error {
	desc, err := createTableDescriptor(stmt, ctx.sql)
	if err != nil {
		return err
	}

	return runOperator(executor.NewCreateTable(s.cat, s.tx, s.store, desc))
}

func (s *session) execDropTable(stmt *parser.DropTableStmt) error {
	tableID, err := tableIDFromName(stmt.Name)
	if err != nil {
		return err
	}

	return runOperator(executor.NewDropTable(s.cat, s.tx, s.store, tableID))
}

func resolvedTargetRelation(bindings *analyzer.Bindings, table *parser.QualifiedName) (*analyzer.RelationBinding, error) {
	if bindings == nil || table == nil {
		return nil, internalError(table, "missing target relation metadata")
	}

	relation, ok := bindings.Relation(table)
	if !ok || relation == nil {
		return nil, internalError(table, "missing target relation metadata")
	}

	return relation, nil
}

func buildInsertColumnMap(
	stmt *parser.InsertStmt,
	relation *analyzer.RelationBinding,
) ([]int, []catalog.ColumnDescriptor, error) {
	if relation == nil || relation.Descriptor == nil {
		return nil, nil, internalError(stmt, "missing INSERT target descriptor")
	}

	targetColumns := relation.Descriptor.Columns
	columnOrdinals := relationColumnOrdinals(relation)
	mapping := make([]int, len(targetColumns))
	for index := range mapping {
		mapping[index] = -1
	}

	if len(stmt.Columns) == 0 {
		for index := range targetColumns {
			mapping[index] = index
		}
		return mapping, nil, nil
	}

	for sourceIndex, column := range stmt.Columns {
		if column == nil {
			continue
		}
		targetIndex, ok := columnOrdinals[column.Name]
		if !ok {
			return nil, nil, internalError(column, "missing INSERT target column %q", column.Name)
		}
		mapping[targetIndex] = sourceIndex
	}

	omitted := make([]catalog.ColumnDescriptor, 0)
	for targetIndex, sourceIndex := range mapping {
		if sourceIndex >= 0 {
			continue
		}
		omitted = append(omitted, targetColumns[targetIndex])
	}

	return mapping, omitted, nil
}

func checkOmittedInsertColumns(columns []catalog.ColumnDescriptor) error {
	for _, column := range columns {
		switch {
		case column.Default != nil:
			return featureError(nil, "INSERT execution does not yet synthesize DEFAULT values for omitted column %q", column.Name)
		case column.Generated != nil:
			return featureError(nil, "INSERT execution does not yet synthesize generated column %q", column.Name)
		case column.Identity != nil:
			return featureError(nil, "INSERT execution does not yet synthesize identity column %q", column.Name)
		}
	}

	return nil
}

func buildInsertRows(
	ctx *analysisContext,
	relation *analyzer.RelationBinding,
	mapping []int,
	rows [][]parser.Node,
) ([]executor.Row, error) {
	out := make([]executor.Row, 0, len(rows))
	for _, sourceRow := range rows {
		values := make([]sqltypes.Value, len(relation.Descriptor.Columns))
		for targetIndex := range values {
			sourceIndex := mapping[targetIndex]
			if sourceIndex < 0 {
				values[targetIndex] = sqltypes.NullValue()
				continue
			}
			if sourceIndex >= len(sourceRow) {
				return nil, internalError(nil, "INSERT row source index %d out of range", sourceIndex)
			}

			compiled, err := compileExpression(ctx.bindings, ctx.types, sourceRow[sourceIndex], rowShape{})
			if err != nil {
				return nil, err
			}
			value, err := compiled.Eval(executor.NewRow())
			if err != nil {
				return nil, err
			}
			values[targetIndex] = value
		}

		out = append(out, executor.NewRow(values...))
	}

	return out, nil
}

func countIncludedColumns(mapping []int) int {
	count := 0
	for _, index := range mapping {
		if index >= 0 {
			count++
		}
	}

	return count
}

func buildTargetScan(
	bindings *analyzer.Bindings,
	types *analyzer.Types,
	store *memory.Store,
	tx storage.Transaction,
	relation *analyzer.RelationBinding,
	where parser.Node,
) (executor.Operator, rowShape, error) {
	if relation == nil || relation.Descriptor == nil {
		return nil, rowShape{}, internalError(where, "missing target relation metadata")
	}

	shape := rowShape{
		columns: make([]shapeColumn, 0, len(relation.Descriptor.Columns)),
	}
	for _, column := range relation.Descriptor.Columns {
		shape.columns = append(shape.columns, shapeColumn{
			name: column.Name,
			typ:  column.Type,
		})
	}

	child := executor.NewSeqScan(store, tx, relation.TableID, storage.ScanOptions{})
	if where == nil {
		return child, shape, nil
	}

	predicate, err := compileExpression(bindings, types, where, shape)
	if err != nil {
		return nil, rowShape{}, err
	}

	return executor.NewFilter(child, predicate), shape, nil
}

func relationColumnOrdinals(relation *analyzer.RelationBinding) map[string]int {
	index := make(map[string]int, len(relation.Columns))
	for ordinal, column := range relation.Columns {
		if column == nil {
			continue
		}
		index[column.Name] = ordinal
	}
	return index
}

func createTableDescriptor(stmt *parser.CreateTableStmt, sql string) (*catalog.TableDescriptor, error) {
	tableID, err := tableIDFromName(stmt.Name)
	if err != nil {
		return nil, err
	}

	desc := &catalog.TableDescriptor{
		ID:          tableID,
		Columns:     make([]catalog.ColumnDescriptor, 0, len(stmt.Columns)),
		Constraints: make([]catalog.ConstraintDescriptor, 0, len(stmt.Constraints)),
	}
	for _, column := range stmt.Columns {
		if column == nil || column.Name == nil {
			continue
		}

		columnType, err := typeDescFromTypeName(column.Type)
		if err != nil {
			return nil, featureError(column, err.Error())
		}
		columnType = applyColumnNullability(columnType, column)

		columnDesc := catalog.ColumnDescriptor{
			Name:        column.Name.Name,
			Type:        columnType,
			Constraints: make([]catalog.ConstraintDescriptor, 0, len(column.Constraints)),
		}
		if column.Default != nil {
			columnDesc.Default = &catalog.ExpressionDescriptor{SQL: sqlSlice(sql, column.Default)}
		}
		for _, constraint := range column.Constraints {
			catalogConstraint, skip, err := constraintDescriptor(constraint, sql, column.Name.Name)
			if err != nil {
				return nil, err
			}
			if skip {
				continue
			}
			columnDesc.Constraints = append(columnDesc.Constraints, catalogConstraint)
		}

		desc.Columns = append(desc.Columns, columnDesc)
	}

	for _, constraint := range stmt.Constraints {
		catalogConstraint, skip, err := constraintDescriptor(constraint, sql, "")
		if err != nil {
			return nil, err
		}
		if skip {
			continue
		}
		desc.Constraints = append(desc.Constraints, catalogConstraint)
	}

	return desc, nil
}

func constraintDescriptor(
	constraint *parser.ConstraintDef,
	sql string,
	columnName string,
) (catalog.ConstraintDescriptor, bool, error) {
	if constraint == nil {
		return catalog.ConstraintDescriptor{}, true, nil
	}

	out := catalog.ConstraintDescriptor{}
	if constraint.Name != nil {
		out.Name = constraint.Name.Name
	}

	switch constraint.Kind {
	case parser.ConstraintKindNull:
		return catalog.ConstraintDescriptor{}, true, nil
	case parser.ConstraintKindNotNull:
		out.Kind = catalog.ConstraintKindNotNull
	case parser.ConstraintKindCheck:
		out.Kind = catalog.ConstraintKindCheck
		out.Expression = &catalog.ExpressionDescriptor{SQL: sqlSlice(sql, constraint.Check)}
	case parser.ConstraintKindUnique:
		out.Kind = catalog.ConstraintKindUnique
	case parser.ConstraintKindPrimaryKey:
		out.Kind = catalog.ConstraintKindPrimaryKey
	case parser.ConstraintKindReferences, parser.ConstraintKindForeignKey:
		out.Kind = catalog.ConstraintKindForeignKey
	default:
		return catalog.ConstraintDescriptor{}, false, featureError(constraint, "unsupported constraint kind %q", constraint.Kind)
	}

	if columnName != "" {
		out.Columns = []string{columnName}
	} else {
		for _, column := range constraint.Columns {
			if column == nil {
				continue
			}
			out.Columns = append(out.Columns, column.Name)
		}
	}

	if constraint.Reference != nil {
		refTable, err := tableIDFromName(constraint.Reference.Table)
		if err != nil {
			return catalog.ConstraintDescriptor{}, false, err
		}
		ref := &catalog.ReferenceDescriptor{Table: refTable}
		for _, column := range constraint.Reference.Columns {
			if column == nil {
				continue
			}
			ref.Columns = append(ref.Columns, column.Name)
		}
		out.Reference = ref
	}

	return out, false, nil
}

func tableIDFromName(name *parser.QualifiedName) (storage.TableID, error) {
	if name == nil || len(name.Parts) == 0 {
		return storage.TableID{}, featureError(name, "missing table name")
	}

	switch len(name.Parts) {
	case 1:
		if name.Parts[0] == nil {
			return storage.TableID{}, featureError(name, "missing table name")
		}
		return storage.TableID{Schema: defaultSchemaName, Name: name.Parts[0].Name}, nil
	case 2:
		if name.Parts[0] == nil || name.Parts[1] == nil {
			return storage.TableID{}, featureError(name, "missing table name")
		}
		return storage.TableID{Schema: name.Parts[0].Name, Name: name.Parts[1].Name}, nil
	default:
		return storage.TableID{}, featureError(name, "qualified table names deeper than schema.table are not supported in Phase 1")
	}
}

func sqlSlice(sql string, node parser.Node) string {
	if node == nil {
		return ""
	}

	pos := node.Pos().Offset
	end := node.End().Offset
	if pos < 0 || end < pos || end > len(sql) {
		return ""
	}

	return strings.TrimSpace(sql[pos:end])
}

func typeDescFromTypeName(node *parser.TypeName) (sqltypes.TypeDesc, error) {
	if node == nil {
		return sqltypes.TypeDesc{}, fmt.Errorf("%w: missing type name", sqltypes.ErrInvalidTypeDesc)
	}
	if node.Qualifier != nil {
		return sqltypes.TypeDesc{}, fmt.Errorf("%w: qualified type names are not supported in Phase 1", sqltypes.ErrInvalidTypeDesc)
	}
	if len(node.Names) == 0 {
		return sqltypes.TypeDesc{}, fmt.Errorf("%w: missing type name", sqltypes.ErrInvalidTypeDesc)
	}

	parts := make([]string, 0, len(node.Names))
	for _, name := range node.Names {
		if name == nil || strings.TrimSpace(name.Name) == "" {
			return sqltypes.TypeDesc{}, fmt.Errorf("%w: missing type name", sqltypes.ErrInvalidTypeDesc)
		}
		parts = append(parts, name.Name)
	}

	text := canonicalTypeNameText(strings.Join(parts, " "))
	if len(node.Args) > 0 {
		args := make([]string, 0, len(node.Args))
		for _, arg := range node.Args {
			rendered, err := renderTypeArgument(arg)
			if err != nil {
				return sqltypes.TypeDesc{}, err
			}
			args = append(args, rendered)
		}
		text += "(" + strings.Join(args, ",") + ")"
	}

	return sqltypes.ParseTypeDesc(text)
}

func canonicalTypeNameText(text string) string {
	switch strings.ToUpper(strings.TrimSpace(text)) {
	case "CHARACTER":
		return "CHAR"
	case "CHARACTER VARYING":
		return "VARCHAR"
	default:
		return text
	}
}

func renderTypeArgument(node parser.Node) (string, error) {
	switch node := node.(type) {
	case *parser.IntegerLiteral:
		return node.Text, nil
	case *parser.FloatLiteral:
		return node.Text, nil
	case *parser.UnaryExpr:
		switch node.Operator {
		case "+", "-":
			value, err := renderTypeArgument(node.Operand)
			if err != nil {
				return "", err
			}
			return node.Operator + value, nil
		default:
			return "", fmt.Errorf("%w: unsupported unary operator %q in type argument", sqltypes.ErrInvalidTypeDesc, node.Operator)
		}
	default:
		return "", fmt.Errorf("%w: unsupported type argument %T", sqltypes.ErrInvalidTypeDesc, node)
	}
}

func applyColumnNullability(desc sqltypes.TypeDesc, column *parser.ColumnDef) sqltypes.TypeDesc {
	if column == nil {
		return desc
	}

	for _, constraint := range column.Constraints {
		if constraint == nil {
			continue
		}
		switch constraint.Kind {
		case parser.ConstraintKindNull:
			desc.Nullable = true
		case parser.ConstraintKindNotNull, parser.ConstraintKindPrimaryKey:
			desc.Nullable = false
		}
	}

	return desc
}
