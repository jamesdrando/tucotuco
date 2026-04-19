package ast

type noopVisitor struct{}

func (noopVisitor) VisitScript(*Script) any {
	return nil
}

func (noopVisitor) VisitSelectStmt(*SelectStmt) any {
	return nil
}

func (noopVisitor) VisitSelectItem(*SelectItem) any {
	return nil
}

func (noopVisitor) VisitFromSource(*FromSource) any {
	return nil
}

func (noopVisitor) VisitOrderByItem(*OrderByItem) any {
	return nil
}

func (noopVisitor) VisitInsertStmt(*InsertStmt) any {
	return nil
}

func (noopVisitor) VisitInsertValuesSource(*InsertValuesSource) any {
	return nil
}

func (noopVisitor) VisitInsertQuerySource(*InsertQuerySource) any {
	return nil
}

func (noopVisitor) VisitInsertDefaultValuesSource(*InsertDefaultValuesSource) any {
	return nil
}

func (noopVisitor) VisitUpdateAssignment(*UpdateAssignment) any {
	return nil
}

func (noopVisitor) VisitUpdateStmt(*UpdateStmt) any {
	return nil
}

func (noopVisitor) VisitDeleteStmt(*DeleteStmt) any {
	return nil
}

func (noopVisitor) VisitTypeName(*TypeName) any {
	return nil
}

func (noopVisitor) VisitReferenceSpec(*ReferenceSpec) any {
	return nil
}

func (noopVisitor) VisitConstraintDef(*ConstraintDef) any {
	return nil
}

func (noopVisitor) VisitColumnDef(*ColumnDef) any {
	return nil
}

func (noopVisitor) VisitCreateTableStmt(*CreateTableStmt) any {
	return nil
}

func (noopVisitor) VisitDropTableStmt(*DropTableStmt) any {
	return nil
}

func (noopVisitor) VisitIdentifier(*Identifier) any {
	return nil
}

func (noopVisitor) VisitQualifiedName(*QualifiedName) any {
	return nil
}

func (noopVisitor) VisitStar(*Star) any {
	return nil
}

func (noopVisitor) VisitBinaryExpr(*BinaryExpr) any {
	return nil
}

func (noopVisitor) VisitUnaryExpr(*UnaryExpr) any {
	return nil
}
