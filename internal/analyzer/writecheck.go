package analyzer

import (
	"github.com/jamesdrando/tucotuco/internal/catalog"
	"github.com/jamesdrando/tucotuco/internal/parser"
	"github.com/jamesdrando/tucotuco/internal/token"
	sqtypes "github.com/jamesdrando/tucotuco/internal/types"
)

const (
	sqlStateSyntaxError      = "42601"
	sqlStateNotNullViolation = "23502"
)

func (p *typeCheckPass) checkInsertValuesShape(pos token.Pos, targetCount int, valueCount int) {
	if targetCount == valueCount {
		return
	}

	p.addError(sqlStateSyntaxError, pos, "INSERT row has %d values for %d target columns", valueCount, targetCount)
}

func (p *typeCheckPass) checkInsertQueryShape(pos token.Pos, targetCount int, outputCount int) {
	if targetCount == outputCount {
		return
	}

	p.addError(sqlStateSyntaxError, pos, "INSERT query returns %d columns for %d target columns", outputCount, targetCount)
}

func (p *typeCheckPass) checkUpdateAssignmentShape(pos token.Pos, targetCount int, valueCount int) {
	if targetCount == valueCount {
		return
	}

	p.addError(sqlStateSyntaxError, pos, "UPDATE assignment has %d values for %d target columns", valueCount, targetCount)
}

func (p *typeCheckPass) checkInsertMissingColumns(stmt *parser.InsertStmt) {
	relation, ok := p.insertTargetRelation(stmt)
	if !ok {
		return
	}
	if _, ok := stmt.Source.(*parser.InsertDefaultValuesSource); ok {
		return
	}

	provided := insertProvidedColumns(stmt, relation)
	for _, column := range relation.Columns {
		if !requiresInsertValue(column) {
			continue
		}
		if _, ok := provided[column.Name]; ok {
			continue
		}

		p.addError(sqlStateNotNullViolation, stmt.Pos(), "INSERT omits required column %q", column.Name)
	}
}

func (p *typeCheckPass) checkInsertDefaultValues(stmt *parser.InsertStmt) {
	relation, ok := p.insertTargetRelation(stmt)
	if !ok {
		return
	}

	for _, column := range relation.Columns {
		if !requiresInsertValue(column) {
			continue
		}

		p.addError(sqlStateNotNullViolation, stmt.Pos(), "DEFAULT VALUES has no value for required column %q", column.Name)
	}
}

func (p *typeCheckPass) checkAssignmentList(targets []assignmentTarget, exprs []parser.Node, values []sqtypes.TypeDesc, context string) {
	limit := minInt(len(targets), minInt(len(exprs), len(values)))
	for index := 0; index < limit; index++ {
		target := targets[index]
		p.requireAssignable(exprs[index], values[index], target.typ, contextForTarget(context, target.name))
		p.requireNonNullWrite(exprs[index], target)
	}
}

func (p *typeCheckPass) requireNonNullWrite(node parser.Node, target assignmentTarget) {
	if node == nil || target.typ.Nullable {
		return
	}

	if _, ok := node.(*parser.NullLiteral); ok {
		p.addError(sqlStateNotNullViolation, node.Pos(), "null value in column %q violates NOT NULL constraint", target.name)
	}
}

func (p *typeCheckPass) requireNonNullDefault(node parser.Node, exprType sqtypes.TypeDesc, targetType sqtypes.TypeDesc, columnName string) {
	if node == nil || targetType.Nullable {
		return
	}
	if _, ok := node.(*parser.NullLiteral); ok || isNullableResult(exprType) {
		p.addError(sqlStateNotNullViolation, node.Pos(), "DEFAULT for column %q must not be NULL", columnName)
	}
}

func (p *typeCheckPass) insertTargetRelation(stmt *parser.InsertStmt) (*RelationBinding, bool) {
	if stmt == nil || stmt.Table == nil {
		return nil, false
	}

	bindings := p.bindings()
	if bindings == nil {
		return nil, false
	}

	relation, ok := bindings.Relation(stmt.Table)
	if !ok || relation == nil {
		return nil, false
	}

	return relation, true
}

func insertProvidedColumns(stmt *parser.InsertStmt, relation *RelationBinding) map[string]struct{} {
	provided := make(map[string]struct{})
	if stmt == nil || relation == nil {
		return provided
	}

	if _, ok := stmt.Source.(*parser.InsertDefaultValuesSource); ok {
		return provided
	}

	if len(stmt.Columns) == 0 {
		for _, column := range relation.Columns {
			if column == nil {
				continue
			}
			provided[column.Name] = struct{}{}
		}
		return provided
	}

	for _, column := range stmt.Columns {
		if column == nil {
			continue
		}
		provided[column.Name] = struct{}{}
	}

	return provided
}

func requiresInsertValue(binding *ColumnBinding) bool {
	if binding == nil || binding.Descriptor == nil {
		return false
	}

	return !binding.Descriptor.Type.Nullable && !hasServerSuppliedValue(binding.Descriptor)
}

func hasServerSuppliedValue(desc *catalog.ColumnDescriptor) bool {
	if desc == nil {
		return false
	}

	return desc.Default != nil || desc.Generated != nil || desc.Identity != nil
}

func contextForTarget(context string, targetName string) string {
	return context + " for column " + `"` + targetName + `"`
}
