package ast

import "testing"

func TestStatementNodesReportSpansAndDispatch(t *testing.T) {
	t.Parallel()

	selectSpan := NewSpan(testPos(t, 54), testPos(t, 55))
	selectItemSpan := NewSpan(testPos(t, 56), testPos(t, 57))
	starSpan := NewSpan(testPos(t, 58), testPos(t, 59))
	fromSourceSpan := NewSpan(testPos(t, 60), testPos(t, 61))
	orderBySpan := NewSpan(testPos(t, 62), testPos(t, 63))
	insertSpan := NewSpan(testPos(t, 64), testPos(t, 65))
	valuesSourceSpan := NewSpan(testPos(t, 66), testPos(t, 67))
	querySourceSpan := NewSpan(testPos(t, 68), testPos(t, 69))
	defaultValuesSpan := NewSpan(testPos(t, 70), testPos(t, 71))
	updateSpan := NewSpan(testPos(t, 72), testPos(t, 73))
	deleteSpan := NewSpan(testPos(t, 74), testPos(t, 75))
	typeNameSpan := NewSpan(testPos(t, 76), testPos(t, 77))
	referenceSpan := NewSpan(testPos(t, 78), testPos(t, 79))
	constraintSpan := NewSpan(testPos(t, 80), testPos(t, 81))
	columnDefSpan := NewSpan(testPos(t, 82), testPos(t, 83))
	createTableSpan := NewSpan(testPos(t, 84), testPos(t, 85))
	dropTableSpan := NewSpan(testPos(t, 86), testPos(t, 87))

	schemaName := &Identifier{
		Span: NewSpan(testPos(t, 40), testPos(t, 41)),
		Name: "public",
	}
	tableName := &Identifier{
		Span: NewSpan(testPos(t, 42), testPos(t, 43)),
		Name: "widgets",
	}
	qualifiedTable := &QualifiedName{
		Span:  NewSpan(schemaName.Pos(), tableName.End()),
		Parts: []*Identifier{schemaName, tableName},
	}
	columnName := &Identifier{
		Span: NewSpan(testPos(t, 44), testPos(t, 45)),
		Name: "category_id",
	}
	referencedColumn := &Identifier{
		Span: NewSpan(testPos(t, 46), testPos(t, 47)),
		Name: "id",
	}
	selectAlias := &Identifier{
		Span: NewSpan(testPos(t, 48), testPos(t, 49)),
		Name: "category",
	}
	fromAlias := &Identifier{
		Span: NewSpan(testPos(t, 50), testPos(t, 51)),
		Name: "w",
	}
	constraintName := &Identifier{
		Span: NewSpan(testPos(t, 52), testPos(t, 53)),
		Name: "widgets_pk",
	}
	typeQualifier := &QualifiedName{
		Span: NewSpan(testPos(t, 88), testPos(t, 89)),
		Parts: []*Identifier{
			{
				Span: NewSpan(testPos(t, 90), testPos(t, 91)),
				Name: "pg_catalog",
			},
		},
	}
	typeWordOne := &Identifier{
		Span: NewSpan(testPos(t, 92), testPos(t, 93)),
		Name: "character",
	}
	typeWordTwo := &Identifier{
		Span: NewSpan(testPos(t, 94), testPos(t, 95)),
		Name: "varying",
	}
	integerLiteral := &IntegerLiteral{
		Span: NewSpan(testPos(t, 96), testPos(t, 97)),
		Text: "10",
	}
	stringLiteral := &StringLiteral{
		Span:  NewSpan(testPos(t, 98), testPos(t, 99)),
		Value: "new",
	}
	whereExpr := &BinaryExpr{
		Span:     NewSpan(columnName.Pos(), integerLiteral.End()),
		Operator: "=",
		Left:     columnName,
		Right:    integerLiteral,
	}
	selectItem := &SelectItem{
		Span:  selectItemSpan,
		Expr:  columnName,
		Alias: selectAlias,
	}
	starItem := &SelectItem{
		Span: selectItemSpan,
		Expr: &Star{Span: starSpan},
	}
	querySelectItem := &SelectItem{
		Span: selectItemSpan,
		Expr: referencedColumn,
	}
	subquery := &SelectStmt{
		Span:       querySourceSpan,
		SelectList: []*SelectItem{querySelectItem},
	}
	fromSource := &FromSource{
		Span:   fromSourceSpan,
		Source: subquery,
		Alias:  fromAlias,
	}
	orderBy := &OrderByItem{
		Span:      orderBySpan,
		Expr:      columnName,
		Direction: "DESC",
	}
	valuesSource := &InsertValuesSource{
		Span: valuesSourceSpan,
		Rows: [][]Node{{integerLiteral, stringLiteral}},
	}
	querySource := &InsertQuerySource{
		Span:  querySourceSpan,
		Query: subquery,
	}
	defaultValuesSource := &InsertDefaultValuesSource{
		Span: defaultValuesSpan,
	}
	updateAssignment := &UpdateAssignment{
		Span:    NewSpan(columnName.Pos(), stringLiteral.End()),
		Columns: []*Identifier{columnName},
		Values:  []Node{stringLiteral},
	}
	typeName := &TypeName{
		Span:      typeNameSpan,
		Qualifier: typeQualifier,
		Names:     []*Identifier{typeWordOne, typeWordTwo},
		Args:      []Node{integerLiteral},
	}
	reference := &ReferenceSpec{
		Span:    referenceSpan,
		Table:   qualifiedTable,
		Columns: []*Identifier{referencedColumn},
	}
	notNullConstraint := &ConstraintDef{
		Span: constraintSpan,
		Kind: ConstraintKindNotNull,
	}
	referencesConstraint := &ConstraintDef{
		Span:      constraintSpan,
		Kind:      ConstraintKindReferences,
		Reference: reference,
	}
	tableConstraint := &ConstraintDef{
		Span:    constraintSpan,
		Name:    constraintName,
		Kind:    ConstraintKindPrimaryKey,
		Columns: []*Identifier{columnName},
	}
	columnDef := &ColumnDef{
		Span:        columnDefSpan,
		Name:        columnName,
		Type:        typeName,
		Default:     stringLiteral,
		Constraints: []*ConstraintDef{notNullConstraint, referencesConstraint},
	}

	cases := []struct {
		name     string
		node     Node
		want     string
		wantSpan Span
		check    func(*testing.T, *statementRecordingVisitor, Node)
	}{
		{
			name: "select",
			node: &SelectStmt{
				Span:       selectSpan,
				Distinct:   true,
				SelectList: []*SelectItem{selectItem, starItem},
				From:       []*FromSource{fromSource},
				Where:      whereExpr,
				GroupBy:    []Node{columnName},
				Having:     whereExpr,
				OrderBy:    []*OrderByItem{orderBy},
				Limit:      integerLiteral,
			},
			want:     "select",
			wantSpan: selectSpan,
			check: func(t *testing.T, visitor *statementRecordingVisitor, node Node) {
				t.Helper()

				stmt := node.(*SelectStmt)
				if !stmt.Distinct {
					t.Fatal("Distinct = false, want true")
				}
				if len(stmt.SelectList) != 2 || stmt.SelectList[0] != selectItem || stmt.SelectList[1] != starItem {
					t.Fatalf("SelectList = %#v, want %#v", stmt.SelectList, []*SelectItem{selectItem, starItem})
				}
				if len(stmt.From) != 1 || stmt.From[0] != fromSource {
					t.Fatalf("From = %#v, want %#v", stmt.From, []*FromSource{fromSource})
				}
				if stmt.Where != whereExpr {
					t.Fatalf("Where = %#v, want %#v", stmt.Where, whereExpr)
				}
				if len(stmt.GroupBy) != 1 || stmt.GroupBy[0] != columnName {
					t.Fatalf("GroupBy = %#v, want %#v", stmt.GroupBy, []Node{columnName})
				}
				if stmt.Having != whereExpr {
					t.Fatalf("Having = %#v, want %#v", stmt.Having, whereExpr)
				}
				if len(stmt.OrderBy) != 1 || stmt.OrderBy[0] != orderBy {
					t.Fatalf("OrderBy = %#v, want %#v", stmt.OrderBy, []*OrderByItem{orderBy})
				}
				if stmt.Limit != integerLiteral {
					t.Fatalf("Limit = %#v, want %#v", stmt.Limit, integerLiteral)
				}
				if visitor.selectStmt != stmt {
					t.Fatalf("VisitSelectStmt node = %p, want %p", visitor.selectStmt, stmt)
				}
			},
		},
		{
			name:     "select item",
			node:     selectItem,
			want:     "select_item",
			wantSpan: selectItemSpan,
			check: func(t *testing.T, visitor *statementRecordingVisitor, node Node) {
				t.Helper()

				item := node.(*SelectItem)
				if item.Expr != columnName {
					t.Fatalf("Expr = %#v, want %#v", item.Expr, columnName)
				}
				if item.Alias != selectAlias {
					t.Fatalf("Alias = %#v, want %#v", item.Alias, selectAlias)
				}
				if visitor.selectItem != item {
					t.Fatalf("VisitSelectItem node = %p, want %p", visitor.selectItem, item)
				}
			},
		},
		{
			name:     "from source",
			node:     fromSource,
			want:     "from_source",
			wantSpan: fromSourceSpan,
			check: func(t *testing.T, visitor *statementRecordingVisitor, node Node) {
				t.Helper()

				source := node.(*FromSource)
				if source.Source != subquery {
					t.Fatalf("Source = %#v, want %#v", source.Source, subquery)
				}
				if source.Alias != fromAlias {
					t.Fatalf("Alias = %#v, want %#v", source.Alias, fromAlias)
				}
				if visitor.fromSource != source {
					t.Fatalf("VisitFromSource node = %p, want %p", visitor.fromSource, source)
				}
			},
		},
		{
			name:     "order by item",
			node:     orderBy,
			want:     "order_by",
			wantSpan: orderBySpan,
			check: func(t *testing.T, visitor *statementRecordingVisitor, node Node) {
				t.Helper()

				item := node.(*OrderByItem)
				if item.Expr != columnName {
					t.Fatalf("Expr = %#v, want %#v", item.Expr, columnName)
				}
				if item.Direction != "DESC" {
					t.Fatalf("Direction = %q, want %q", item.Direction, "DESC")
				}
				if visitor.orderByItem != item {
					t.Fatalf("VisitOrderByItem node = %p, want %p", visitor.orderByItem, item)
				}
			},
		},
		{
			name:     "insert values source",
			node:     valuesSource,
			want:     "insert_values_source",
			wantSpan: valuesSourceSpan,
			check: func(t *testing.T, visitor *statementRecordingVisitor, node Node) {
				t.Helper()

				source := node.(*InsertValuesSource)
				if len(source.Rows) != 1 || len(source.Rows[0]) != 2 {
					t.Fatalf("Rows = %#v, want one two-value row", source.Rows)
				}
				if visitor.insertValuesSource != source {
					t.Fatalf("VisitInsertValuesSource node = %p, want %p", visitor.insertValuesSource, source)
				}
			},
		},
		{
			name:     "insert query source",
			node:     querySource,
			want:     "insert_query_source",
			wantSpan: querySourceSpan,
			check: func(t *testing.T, visitor *statementRecordingVisitor, node Node) {
				t.Helper()

				source := node.(*InsertQuerySource)
				if source.Query != subquery {
					t.Fatalf("Query = %#v, want %#v", source.Query, subquery)
				}
				if visitor.insertQuerySource != source {
					t.Fatalf("VisitInsertQuerySource node = %p, want %p", visitor.insertQuerySource, source)
				}
			},
		},
		{
			name:     "insert default values source",
			node:     defaultValuesSource,
			want:     "insert_default_values_source",
			wantSpan: defaultValuesSpan,
			check: func(t *testing.T, visitor *statementRecordingVisitor, node Node) {
				t.Helper()

				source := node.(*InsertDefaultValuesSource)
				if visitor.insertDefaultValuesSource != source {
					t.Fatalf("VisitInsertDefaultValuesSource node = %p, want %p", visitor.insertDefaultValuesSource, source)
				}
			},
		},
		{
			name: "insert",
			node: &InsertStmt{
				Span:    insertSpan,
				Table:   qualifiedTable,
				Columns: []*Identifier{columnName},
				Source:  valuesSource,
			},
			want:     "insert",
			wantSpan: insertSpan,
			check: func(t *testing.T, visitor *statementRecordingVisitor, node Node) {
				t.Helper()

				stmt := node.(*InsertStmt)
				if stmt.Table != qualifiedTable {
					t.Fatalf("Table = %#v, want %#v", stmt.Table, qualifiedTable)
				}
				if len(stmt.Columns) != 1 || stmt.Columns[0] != columnName {
					t.Fatalf("Columns = %#v, want %#v", stmt.Columns, []*Identifier{columnName})
				}
				if stmt.Source != valuesSource {
					t.Fatalf("Source = %#v, want %#v", stmt.Source, valuesSource)
				}
				if visitor.insertStmt != stmt {
					t.Fatalf("VisitInsertStmt node = %p, want %p", visitor.insertStmt, stmt)
				}
			},
		},
		{
			name: "update assignment",
			node: updateAssignment,
			want: "update_assignment",
			wantSpan: NewSpan(
				columnName.Pos(),
				stringLiteral.End(),
			),
			check: func(t *testing.T, visitor *statementRecordingVisitor, node Node) {
				t.Helper()

				assignment := node.(*UpdateAssignment)
				if len(assignment.Columns) != 1 || assignment.Columns[0] != columnName {
					t.Fatalf("Columns = %#v, want %#v", assignment.Columns, []*Identifier{columnName})
				}
				if len(assignment.Values) != 1 || assignment.Values[0] != stringLiteral {
					t.Fatalf("Values = %#v, want %#v", assignment.Values, []Node{stringLiteral})
				}
				if visitor.updateAssignment != assignment {
					t.Fatalf("VisitUpdateAssignment node = %p, want %p", visitor.updateAssignment, assignment)
				}
			},
		},
		{
			name: "update",
			node: &UpdateStmt{
				Span:        updateSpan,
				Table:       qualifiedTable,
				Assignments: []*UpdateAssignment{updateAssignment},
				Where:       whereExpr,
			},
			want:     "update",
			wantSpan: updateSpan,
			check: func(t *testing.T, visitor *statementRecordingVisitor, node Node) {
				t.Helper()

				stmt := node.(*UpdateStmt)
				if stmt.Table != qualifiedTable {
					t.Fatalf("Table = %#v, want %#v", stmt.Table, qualifiedTable)
				}
				if len(stmt.Assignments) != 1 || stmt.Assignments[0] != updateAssignment {
					t.Fatalf("Assignments = %#v, want %#v", stmt.Assignments, []*UpdateAssignment{updateAssignment})
				}
				if stmt.Where != whereExpr {
					t.Fatalf("Where = %#v, want %#v", stmt.Where, whereExpr)
				}
				if visitor.updateStmt != stmt {
					t.Fatalf("VisitUpdateStmt node = %p, want %p", visitor.updateStmt, stmt)
				}
			},
		},
		{
			name: "delete",
			node: &DeleteStmt{
				Span:  deleteSpan,
				Table: qualifiedTable,
				Where: whereExpr,
			},
			want:     "delete",
			wantSpan: deleteSpan,
			check: func(t *testing.T, visitor *statementRecordingVisitor, node Node) {
				t.Helper()

				stmt := node.(*DeleteStmt)
				if stmt.Table != qualifiedTable {
					t.Fatalf("Table = %#v, want %#v", stmt.Table, qualifiedTable)
				}
				if stmt.Where != whereExpr {
					t.Fatalf("Where = %#v, want %#v", stmt.Where, whereExpr)
				}
				if visitor.deleteStmt != stmt {
					t.Fatalf("VisitDeleteStmt node = %p, want %p", visitor.deleteStmt, stmt)
				}
			},
		},
		{
			name:     "type name",
			node:     typeName,
			want:     "type_name",
			wantSpan: typeNameSpan,
			check: func(t *testing.T, visitor *statementRecordingVisitor, node Node) {
				t.Helper()

				name := node.(*TypeName)
				if name.Qualifier != typeQualifier {
					t.Fatalf("Qualifier = %#v, want %#v", name.Qualifier, typeQualifier)
				}
				if len(name.Names) != 2 || name.Names[0] != typeWordOne || name.Names[1] != typeWordTwo {
					t.Fatalf("Names = %#v, want %#v", name.Names, []*Identifier{typeWordOne, typeWordTwo})
				}
				if len(name.Args) != 1 || name.Args[0] != integerLiteral {
					t.Fatalf("Args = %#v, want %#v", name.Args, []Node{integerLiteral})
				}
				if visitor.typeName != name {
					t.Fatalf("VisitTypeName node = %p, want %p", visitor.typeName, name)
				}
			},
		},
		{
			name:     "reference spec",
			node:     reference,
			want:     "reference_spec",
			wantSpan: referenceSpan,
			check: func(t *testing.T, visitor *statementRecordingVisitor, node Node) {
				t.Helper()

				spec := node.(*ReferenceSpec)
				if spec.Table != qualifiedTable {
					t.Fatalf("Table = %#v, want %#v", spec.Table, qualifiedTable)
				}
				if len(spec.Columns) != 1 || spec.Columns[0] != referencedColumn {
					t.Fatalf("Columns = %#v, want %#v", spec.Columns, []*Identifier{referencedColumn})
				}
				if visitor.referenceSpec != spec {
					t.Fatalf("VisitReferenceSpec node = %p, want %p", visitor.referenceSpec, spec)
				}
			},
		},
		{
			name:     "constraint def",
			node:     referencesConstraint,
			want:     "constraint_def",
			wantSpan: constraintSpan,
			check: func(t *testing.T, visitor *statementRecordingVisitor, node Node) {
				t.Helper()

				constraint := node.(*ConstraintDef)
				if constraint.Kind != ConstraintKindReferences {
					t.Fatalf("Kind = %q, want %q", constraint.Kind, ConstraintKindReferences)
				}
				if constraint.Reference != reference {
					t.Fatalf("Reference = %#v, want %#v", constraint.Reference, reference)
				}
				if visitor.constraintDef != constraint {
					t.Fatalf("VisitConstraintDef node = %p, want %p", visitor.constraintDef, constraint)
				}
			},
		},
		{
			name:     "column def",
			node:     columnDef,
			want:     "column_def",
			wantSpan: columnDefSpan,
			check: func(t *testing.T, visitor *statementRecordingVisitor, node Node) {
				t.Helper()

				def := node.(*ColumnDef)
				if def.Name != columnName {
					t.Fatalf("Name = %#v, want %#v", def.Name, columnName)
				}
				if def.Type != typeName {
					t.Fatalf("Type = %#v, want %#v", def.Type, typeName)
				}
				if def.Default != stringLiteral {
					t.Fatalf("Default = %#v, want %#v", def.Default, stringLiteral)
				}
				if len(def.Constraints) != 2 || def.Constraints[0] != notNullConstraint || def.Constraints[1] != referencesConstraint {
					t.Fatalf("Constraints = %#v, want %#v", def.Constraints, []*ConstraintDef{notNullConstraint, referencesConstraint})
				}
				if visitor.columnDef != def {
					t.Fatalf("VisitColumnDef node = %p, want %p", visitor.columnDef, def)
				}
			},
		},
		{
			name: "create table",
			node: &CreateTableStmt{
				Span:        createTableSpan,
				Name:        qualifiedTable,
				Columns:     []*ColumnDef{columnDef},
				Constraints: []*ConstraintDef{tableConstraint},
			},
			want:     "create_table",
			wantSpan: createTableSpan,
			check: func(t *testing.T, visitor *statementRecordingVisitor, node Node) {
				t.Helper()

				stmt := node.(*CreateTableStmt)
				if stmt.Name != qualifiedTable {
					t.Fatalf("Name = %#v, want %#v", stmt.Name, qualifiedTable)
				}
				if len(stmt.Columns) != 1 || stmt.Columns[0] != columnDef {
					t.Fatalf("Columns = %#v, want %#v", stmt.Columns, []*ColumnDef{columnDef})
				}
				if len(stmt.Constraints) != 1 || stmt.Constraints[0] != tableConstraint {
					t.Fatalf("Constraints = %#v, want %#v", stmt.Constraints, []*ConstraintDef{tableConstraint})
				}
				if visitor.createTableStmt != stmt {
					t.Fatalf("VisitCreateTableStmt node = %p, want %p", visitor.createTableStmt, stmt)
				}
			},
		},
		{
			name: "drop table",
			node: &DropTableStmt{
				Span: dropTableSpan,
				Name: qualifiedTable,
			},
			want:     "drop_table",
			wantSpan: dropTableSpan,
			check: func(t *testing.T, visitor *statementRecordingVisitor, node Node) {
				t.Helper()

				stmt := node.(*DropTableStmt)
				if stmt.Name != qualifiedTable {
					t.Fatalf("Name = %#v, want %#v", stmt.Name, qualifiedTable)
				}
				if visitor.dropTableStmt != stmt {
					t.Fatalf("VisitDropTableStmt node = %p, want %p", visitor.dropTableStmt, stmt)
				}
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := tc.node.Pos(); got != tc.wantSpan.Pos() {
				t.Fatalf("Pos() = %#v, want %#v", got, tc.wantSpan.Pos())
			}

			if got := tc.node.End(); got != tc.wantSpan.End() {
				t.Fatalf("End() = %#v, want %#v", got, tc.wantSpan.End())
			}

			visitor := &statementRecordingVisitor{}
			if got := tc.node.Accept(visitor); got != tc.want {
				t.Fatalf("Accept() = %#v, want %#v", got, tc.want)
			}

			tc.check(t, visitor, tc.node)
		})
	}
}

type statementRecordingVisitor struct {
	noopVisitor

	selectStmt                *SelectStmt
	selectItem                *SelectItem
	fromSource                *FromSource
	orderByItem               *OrderByItem
	insertStmt                *InsertStmt
	insertValuesSource        *InsertValuesSource
	insertQuerySource         *InsertQuerySource
	insertDefaultValuesSource *InsertDefaultValuesSource
	updateAssignment          *UpdateAssignment
	updateStmt                *UpdateStmt
	deleteStmt                *DeleteStmt
	typeName                  *TypeName
	referenceSpec             *ReferenceSpec
	constraintDef             *ConstraintDef
	columnDef                 *ColumnDef
	createTableStmt           *CreateTableStmt
	dropTableStmt             *DropTableStmt
}

func (v *statementRecordingVisitor) VisitSelectStmt(stmt *SelectStmt) any {
	v.selectStmt = stmt
	return "select"
}

func (v *statementRecordingVisitor) VisitSelectItem(item *SelectItem) any {
	v.selectItem = item
	return "select_item"
}

func (v *statementRecordingVisitor) VisitFromSource(source *FromSource) any {
	v.fromSource = source
	return "from_source"
}

func (v *statementRecordingVisitor) VisitOrderByItem(item *OrderByItem) any {
	v.orderByItem = item
	return "order_by"
}

func (v *statementRecordingVisitor) VisitInsertStmt(stmt *InsertStmt) any {
	v.insertStmt = stmt
	return "insert"
}

func (v *statementRecordingVisitor) VisitInsertValuesSource(source *InsertValuesSource) any {
	v.insertValuesSource = source
	return "insert_values_source"
}

func (v *statementRecordingVisitor) VisitInsertQuerySource(source *InsertQuerySource) any {
	v.insertQuerySource = source
	return "insert_query_source"
}

func (v *statementRecordingVisitor) VisitInsertDefaultValuesSource(source *InsertDefaultValuesSource) any {
	v.insertDefaultValuesSource = source
	return "insert_default_values_source"
}

func (v *statementRecordingVisitor) VisitUpdateAssignment(assignment *UpdateAssignment) any {
	v.updateAssignment = assignment
	return "update_assignment"
}

func (v *statementRecordingVisitor) VisitUpdateStmt(stmt *UpdateStmt) any {
	v.updateStmt = stmt
	return "update"
}

func (v *statementRecordingVisitor) VisitDeleteStmt(stmt *DeleteStmt) any {
	v.deleteStmt = stmt
	return "delete"
}

func (v *statementRecordingVisitor) VisitTypeName(name *TypeName) any {
	v.typeName = name
	return "type_name"
}

func (v *statementRecordingVisitor) VisitReferenceSpec(spec *ReferenceSpec) any {
	v.referenceSpec = spec
	return "reference_spec"
}

func (v *statementRecordingVisitor) VisitConstraintDef(def *ConstraintDef) any {
	v.constraintDef = def
	return "constraint_def"
}

func (v *statementRecordingVisitor) VisitColumnDef(def *ColumnDef) any {
	v.columnDef = def
	return "column_def"
}

func (v *statementRecordingVisitor) VisitCreateTableStmt(stmt *CreateTableStmt) any {
	v.createTableStmt = stmt
	return "create_table"
}

func (v *statementRecordingVisitor) VisitDropTableStmt(stmt *DropTableStmt) any {
	v.dropTableStmt = stmt
	return "drop_table"
}
