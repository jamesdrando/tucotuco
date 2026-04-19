package ast

import (
	"strings"
	"testing"
)

func TestPrettyPrint(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		node Node
		want string
	}{
		{
			name: "nil",
			node: nil,
			want: "<nil>",
		},
		{
			name: "expression",
			node: &UnaryExpr{
				Operator: "NOT",
				Operand: &BinaryExpr{
					Operator: "=",
					Left:     qname("widgets", "price"),
					Right: &BinaryExpr{
						Operator: "+",
						Left: &FloatLiteral{
							Text: "3.14",
						},
						Right: &ParamLiteral{
							Text: "$1",
						},
					},
				},
			},
			want: trimPretty(`
				UnaryExpr
				  Operator: "NOT"
				  Operand:
				    BinaryExpr
				      Operator: "="
				      Left:
				        QualifiedName
				          Parts:
				            [0]:
				              Identifier(Name="widgets")
				            [1]:
				              Identifier(Name="price")
				      Right:
				        BinaryExpr
				          Operator: "+"
				          Left:
				            FloatLiteral(Text="3.14")
				          Right:
				            ParamLiteral(Text="$1")
			`),
		},
		{
			name: "select statement",
			node: &SelectStmt{
				Distinct: true,
				SelectList: []*SelectItem{
					{Expr: ident("id"), Alias: ident("widget_id")},
					{Expr: &Star{Qualifier: qname("widgets")}},
					{Expr: &BoolLiteral{Value: true}},
				},
				From: []*FromSource{
					{Source: qname("public", "widgets"), Alias: ident("w")},
				},
				Where: &BinaryExpr{
					Operator: "AND",
					Left: &UnaryExpr{
						Operator: "NOT",
						Operand:  qname("w", "deleted"),
					},
					Right: &BinaryExpr{
						Operator: "=",
						Left:     qname("w", "category_id"),
						Right:    &NullLiteral{},
					},
				},
				GroupBy: []Node{
					qname("w", "id"),
				},
				Having: &BinaryExpr{
					Operator: ">",
					Left:     qname("w", "id"),
					Right: &IntegerLiteral{
						Text: "0",
					},
				},
				OrderBy: []*OrderByItem{
					{
						Expr:      qname("w", "id"),
						Direction: "DESC",
					},
				},
				Limit: &IntegerLiteral{
					Text: "10",
				},
			},
			want: trimPretty(`
				SelectStmt
				  Distinct: true
				  SelectList:
				    [0]:
				      SelectItem
				        Expr:
				          Identifier(Name="id")
				        Alias:
				          Identifier(Name="widget_id")
				    [1]:
				      SelectItem
				        Expr:
				          Star
				            Qualifier:
				              QualifiedName
				                Parts:
				                  [0]:
				                    Identifier(Name="widgets")
				        Alias: <nil>
				    [2]:
				      SelectItem
				        Expr:
				          BoolLiteral(Value=true)
				        Alias: <nil>
				  From:
				    [0]:
				      FromSource
				        Source:
				          QualifiedName
				            Parts:
				              [0]:
				                Identifier(Name="public")
				              [1]:
				                Identifier(Name="widgets")
				        Alias:
				          Identifier(Name="w")
				  Where:
				    BinaryExpr
				      Operator: "AND"
				      Left:
				        UnaryExpr
				          Operator: "NOT"
				          Operand:
				            QualifiedName
				              Parts:
				                [0]:
				                  Identifier(Name="w")
				                [1]:
				                  Identifier(Name="deleted")
				      Right:
				        BinaryExpr
				          Operator: "="
				          Left:
				            QualifiedName
				              Parts:
				                [0]:
				                  Identifier(Name="w")
				                [1]:
				                  Identifier(Name="category_id")
				          Right:
				            NullLiteral
				  GroupBy:
				    [0]:
				      QualifiedName
				        Parts:
				          [0]:
				            Identifier(Name="w")
				          [1]:
				            Identifier(Name="id")
				  Having:
				    BinaryExpr
				      Operator: ">"
				      Left:
				        QualifiedName
				          Parts:
				            [0]:
				              Identifier(Name="w")
				            [1]:
				              Identifier(Name="id")
				      Right:
				        IntegerLiteral(Text="0")
				  OrderBy:
				    [0]:
				      OrderByItem
				        Expr:
				          QualifiedName
				            Parts:
				              [0]:
				                Identifier(Name="w")
				              [1]:
				                Identifier(Name="id")
				        Direction: "DESC"
				  Limit:
				    IntegerLiteral(Text="10")
			`),
		},
		{
			name: "dml script",
			node: &Script{
				Nodes: []Node{
					&InsertStmt{
						Table:   qname("public", "widgets"),
						Columns: []*Identifier{ident("id"), ident("status")},
						Source: &InsertValuesSource{
							Rows: [][]Node{
								{
									&IntegerLiteral{Text: "1"},
									&StringLiteral{Value: "new"},
								},
								{
									&IntegerLiteral{Text: "2"},
									&NullLiteral{},
								},
							},
						},
					},
					&InsertStmt{
						Table:   qname("archive", "widgets"),
						Columns: []*Identifier{ident("id")},
						Source: &InsertQuerySource{
							Query: &SelectStmt{
								SelectList: []*SelectItem{
									{Expr: &ParamLiteral{Text: "$1"}},
								},
							},
						},
					},
					&InsertStmt{
						Table:  qname("defaults", "widgets"),
						Source: &InsertDefaultValuesSource{},
					},
					&UpdateStmt{
						Table: qname("public", "widgets"),
						Assignments: []*UpdateAssignment{
							{
								Columns: []*Identifier{ident("id"), ident("status")},
								Values: []Node{
									&IntegerLiteral{Text: "1"},
									&StringLiteral{Value: "archived"},
								},
							},
						},
					},
					&DeleteStmt{
						Table: qname("public", "widgets"),
						Where: &BinaryExpr{
							Operator: "=",
							Left:     ident("id"),
							Right:    &IntegerLiteral{Text: "1"},
						},
					},
				},
			},
			want: trimPretty(`
				Script
				  Nodes:
				    [0]:
				      InsertStmt
				        Table:
				          QualifiedName
				            Parts:
				              [0]:
				                Identifier(Name="public")
				              [1]:
				                Identifier(Name="widgets")
				        Columns:
				          [0]:
				            Identifier(Name="id")
				          [1]:
				            Identifier(Name="status")
				        Source:
				          InsertValuesSource
				            Rows:
				              [0]:
				                [0]:
				                  IntegerLiteral(Text="1")
				                [1]:
				                  StringLiteral(Value="new")
				              [1]:
				                [0]:
				                  IntegerLiteral(Text="2")
				                [1]:
				                  NullLiteral
				    [1]:
				      InsertStmt
				        Table:
				          QualifiedName
				            Parts:
				              [0]:
				                Identifier(Name="archive")
				              [1]:
				                Identifier(Name="widgets")
				        Columns:
				          [0]:
				            Identifier(Name="id")
				        Source:
				          InsertQuerySource
				            Query:
				              SelectStmt
				                Distinct: false
				                SelectList:
				                  [0]:
				                    SelectItem
				                      Expr:
				                        ParamLiteral(Text="$1")
				                      Alias: <nil>
				                From: []
				                Where: <nil>
				                GroupBy: []
				                Having: <nil>
				                OrderBy: []
				                Limit: <nil>
				    [2]:
				      InsertStmt
				        Table:
				          QualifiedName
				            Parts:
				              [0]:
				                Identifier(Name="defaults")
				              [1]:
				                Identifier(Name="widgets")
				        Columns: []
				        Source:
				          InsertDefaultValuesSource
				    [3]:
				      UpdateStmt
				        Table:
				          QualifiedName
				            Parts:
				              [0]:
				                Identifier(Name="public")
				              [1]:
				                Identifier(Name="widgets")
				        Assignments:
				          [0]:
				            UpdateAssignment
				              Columns:
				                [0]:
				                  Identifier(Name="id")
				                [1]:
				                  Identifier(Name="status")
				              Values:
				                [0]:
				                  IntegerLiteral(Text="1")
				                [1]:
				                  StringLiteral(Value="archived")
				        Where: <nil>
				    [4]:
				      DeleteStmt
				        Table:
				          QualifiedName
				            Parts:
				              [0]:
				                Identifier(Name="public")
				              [1]:
				                Identifier(Name="widgets")
				        Where:
				          BinaryExpr
				            Operator: "="
				            Left:
				              Identifier(Name="id")
				            Right:
				              IntegerLiteral(Text="1")
			`),
		},
		{
			name: "ddl script",
			node: &Script{
				Nodes: []Node{
					&CreateTableStmt{
						Name: qname("public", "widgets"),
						Columns: []*ColumnDef{
							{
								Name: ident("id"),
								Type: &TypeName{
									Names: []*Identifier{ident("integer")},
								},
								Constraints: []*ConstraintDef{
									{Kind: ConstraintKindNotNull},
									{Kind: ConstraintKindPrimaryKey},
								},
							},
							{
								Name: ident("category_id"),
								Type: &TypeName{
									Qualifier: qname("pg_catalog"),
									Names:     []*Identifier{ident("character"), ident("varying")},
									Args:      []Node{&IntegerLiteral{Text: "10"}},
								},
								Default: &StringLiteral{Value: "new"},
								Constraints: []*ConstraintDef{
									{Kind: ConstraintKindNull},
									{
										Kind: ConstraintKindReferences,
										Reference: &ReferenceSpec{
											Table:   qname("public", "categories"),
											Columns: []*Identifier{ident("id")},
										},
									},
								},
							},
						},
						Constraints: []*ConstraintDef{
							{
								Name:    ident("widgets_pk"),
								Kind:    ConstraintKindPrimaryKey,
								Columns: []*Identifier{ident("id")},
							},
							{
								Kind: ConstraintKindCheck,
								Check: &BinaryExpr{
									Operator: ">",
									Left:     ident("id"),
									Right:    &IntegerLiteral{Text: "0"},
								},
							},
							{
								Kind:    ConstraintKindForeignKey,
								Columns: []*Identifier{ident("category_id")},
								Reference: &ReferenceSpec{
									Table:   qname("public", "categories"),
									Columns: []*Identifier{ident("id")},
								},
							},
						},
					},
					&DropTableStmt{
						Name: qname("archive", "widgets_old"),
					},
				},
			},
			want: trimPretty(`
				Script
				  Nodes:
				    [0]:
				      CreateTableStmt
				        Name:
				          QualifiedName
				            Parts:
				              [0]:
				                Identifier(Name="public")
				              [1]:
				                Identifier(Name="widgets")
				        Columns:
				          [0]:
				            ColumnDef
				              Name:
				                Identifier(Name="id")
				              Type:
				                TypeName
				                  Qualifier: <nil>
				                  Names:
				                    [0]:
				                      Identifier(Name="integer")
				                  Args: []
				              Default: <nil>
				              Constraints:
				                [0]:
				                  ConstraintDef
				                    Name: <nil>
				                    Kind: "NOT NULL"
				                    Columns: []
				                    Check: <nil>
				                    Reference: <nil>
				                [1]:
				                  ConstraintDef
				                    Name: <nil>
				                    Kind: "PRIMARY KEY"
				                    Columns: []
				                    Check: <nil>
				                    Reference: <nil>
				          [1]:
				            ColumnDef
				              Name:
				                Identifier(Name="category_id")
				              Type:
				                TypeName
				                  Qualifier:
				                    QualifiedName
				                      Parts:
				                        [0]:
				                          Identifier(Name="pg_catalog")
				                  Names:
				                    [0]:
				                      Identifier(Name="character")
				                    [1]:
				                      Identifier(Name="varying")
				                  Args:
				                    [0]:
				                      IntegerLiteral(Text="10")
				              Default:
				                StringLiteral(Value="new")
				              Constraints:
				                [0]:
				                  ConstraintDef
				                    Name: <nil>
				                    Kind: "NULL"
				                    Columns: []
				                    Check: <nil>
				                    Reference: <nil>
				                [1]:
				                  ConstraintDef
				                    Name: <nil>
				                    Kind: "REFERENCES"
				                    Columns: []
				                    Check: <nil>
				                    Reference:
				                      ReferenceSpec
				                        Table:
				                          QualifiedName
				                            Parts:
				                              [0]:
				                                Identifier(Name="public")
				                              [1]:
				                                Identifier(Name="categories")
				                        Columns:
				                          [0]:
				                            Identifier(Name="id")
				        Constraints:
				          [0]:
				            ConstraintDef
				              Name:
				                Identifier(Name="widgets_pk")
				              Kind: "PRIMARY KEY"
				              Columns:
				                [0]:
				                  Identifier(Name="id")
				              Check: <nil>
				              Reference: <nil>
				          [1]:
				            ConstraintDef
				              Name: <nil>
				              Kind: "CHECK"
				              Columns: []
				              Check:
				                BinaryExpr
				                  Operator: ">"
				                  Left:
				                    Identifier(Name="id")
				                  Right:
				                    IntegerLiteral(Text="0")
				              Reference: <nil>
				          [2]:
				            ConstraintDef
				              Name: <nil>
				              Kind: "FOREIGN KEY"
				              Columns:
				                [0]:
				                  Identifier(Name="category_id")
				              Check: <nil>
				              Reference:
				                ReferenceSpec
				                  Table:
				                    QualifiedName
				                      Parts:
				                        [0]:
				                          Identifier(Name="public")
				                        [1]:
				                          Identifier(Name="categories")
				                  Columns:
				                    [0]:
				                      Identifier(Name="id")
				    [1]:
				      DropTableStmt
				        Name:
				          QualifiedName
				            Parts:
				              [0]:
				                Identifier(Name="archive")
				              [1]:
				                Identifier(Name="widgets_old")
			`),
		},
	}

	for _, tc := range cases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := PrettyPrint(tc.node); got != tc.want {
				t.Fatalf("PrettyPrint() mismatch\ngot:\n%s\n\nwant:\n%s", got, tc.want)
			}
		})
	}
}

func ident(name string) *Identifier {
	return &Identifier{Name: name}
}

func qname(parts ...string) *QualifiedName {
	identifiers := make([]*Identifier, len(parts))
	for i, part := range parts {
		identifiers[i] = ident(part)
	}

	return &QualifiedName{Parts: identifiers}
}

func trimPretty(text string) string {
	lines := strings.Split(text, "\n")

	for len(lines) > 0 && strings.TrimSpace(lines[0]) == "" {
		lines = lines[1:]
	}

	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}

	if len(lines) == 0 {
		return ""
	}

	indent := -1
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		width := len(line) - len(strings.TrimLeft(line, " \t"))
		if indent == -1 || width < indent {
			indent = width
		}
	}

	if indent <= 0 {
		return strings.Join(lines, "\n")
	}

	for i, line := range lines {
		if strings.TrimSpace(line) == "" {
			lines[i] = ""
			continue
		}

		lines[i] = line[indent:]
	}

	return strings.Join(lines, "\n")
}
